import logging
import argparse
import time
from typing import List
import xml.etree.ElementTree as ET

import pandas as pd
from pydantic import ValidationError

from config import settings
from utils.logging_setup import setup_logging
from contracts.arxiv_contract import ArxivArticle
from ingestion.api_client import ArxivAPIClient
from utils.spark_utils import get_spark_session

setup_logging()
logger = logging.getLogger(__name__)

ATOM_NAMESPACE = "{http://www.w3.org/2005/Atom}"


def parse_xml_entry(entry) -> ArxivArticle:
    """Parses a single XML entry and validates it against the data contract."""
    raw_data = {
        "id": entry.find(f"{ATOM_NAMESPACE}id").text.split("/")[-1],
        "title": entry.find(f"{ATOM_NAMESPACE}title").text,
        "summary": entry.find(f"{ATOM_NAMESPACE}summary").text.strip(),
        "authors": [
            author.find(f"{ATOM_NAMESPACE}name").text
            for author in entry.findall(f"{ATOM_NAMESPACE}author")
        ],
        "categories": [
            (cat.get("term") or "").strip()
            for cat in entry.findall(f"{ATOM_NAMESPACE}category")
            if (cat.get("term") or "").strip() != ""
        ],
        "published_date": entry.find(f"{ATOM_NAMESPACE}published").text,
        "updated_date": entry.find(f"{ATOM_NAMESPACE}updated").text,
        "pdf_url": link.get("href")
        if (link := entry.find(f'{ATOM_NAMESPACE}link[@title="pdf"]')) is not None
        else "",
    }
    return ArxivArticle(**raw_data)


def create_bronze_table_if_needed(spark, table_fqn: str):
    spark.sql(f"USE CATALOG {settings.SPARK_CATALOG_NAME}")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {settings.SPARK_CATALOG_NAME}.{settings.BRONZE_SCHEMA}"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            id STRING,
            title STRING,
            summary STRING,
            authors ARRAY<STRING>,
            categories ARRAY<STRING>,
            published_date TIMESTAMP,
            updated_date TIMESTAMP,
            pdf_url STRING
        )
        USING delta
        """
    )


def run_ingestion(
    target_size_bytes: int | None = None,
    batch_size: int | None = None,
    api_sleep_seconds: int | None = None,
):
    """
    Orchestrates the data ingestion process and upserts into Bronze (Delta on UC).
    """
    api_client = ArxivAPIClient()

    all_articles: List[ArxivArticle] = []

    for category in settings.CATEGORIES_TO_FETCH:
        logger.info(f"--- Starting fetch for category: {category} ---")
        start_index = 0

        while True:
            try:
                content = api_client.fetch_batch(
                    category, start_index, (batch_size or settings.BATCH_SIZE)
                )
                root = ET.fromstring(content)
                entries = root.findall(f"{ATOM_NAMESPACE}entry")

                if not entries:
                    logger.info(f"No more articles for '{category}'. Moving to next.")
                    break

                for entry in entries:
                    try:
                        all_articles.append(parse_xml_entry(entry))
                    except ValidationError as e:
                        logger.warning(
                            f"Data contract validation failed, skipping article. Error: {e}"
                        )

                current_size = (
                    pd.DataFrame([m.model_dump() for m in all_articles])
                    .memory_usage(deep=True)
                    .sum()
                )
                logger.info(
                    f"Total articles: {len(all_articles)}. Estimated memory: {current_size / (1024 * 1024):.2f}MB"
                )

                if current_size >= (target_size_bytes or settings.TARGET_SIZE_BYTES):
                    logger.info("Target size reached. Stopping all fetches.")
                    break

                start_index += len(entries)
                time.sleep(api_sleep_seconds or settings.API_SLEEP_SECONDS)

            except Exception as e:
                logger.error(
                    f"A critical error occurred during batch fetch for '{category}'. Skipping to next category. Error: {e}"
                )
                break

        if "current_size" in locals() and current_size >= (
            target_size_bytes or settings.TARGET_SIZE_BYTES
        ):
            break

    if not all_articles:
        logger.warning("No data was ingested.")
        return

    logger.info("Ingestion complete. Preparing data for writing to Bronze table.")

    final_pd = (
        pd.DataFrame([m.model_dump() for m in all_articles])
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    for col in ("published_date", "updated_date"):
        if col in final_pd.columns:
            final_pd[col] = pd.to_datetime(final_pd[col], errors="coerce", utc=True)

    spark = get_spark_session("IngestionToBronze")
    bronze_tbl = f"{settings.SPARK_CATALOG_NAME}.{settings.BRONZE_SCHEMA}.arxiv_raw"

    create_bronze_table_if_needed(spark, bronze_tbl)

    df_spark = spark.createDataFrame(final_pd)
    df_spark.createOrReplaceTempView("arxiv_raw_batch")

    spark.sql(
        f"""
        MERGE INTO {bronze_tbl} AS t
        USING arxiv_raw_batch AS s
        ON t.id = s.id
        WHEN MATCHED AND s.updated_date > t.updated_date THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    written = df_spark.count()
    logger.info(f"Upserted ~{written} rows into {bronze_tbl}")


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-size-mb", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--api-sleep-seconds", type=int)
    args = parser.parse_args()

    run_ingestion(
        target_size_bytes=(args.target_size_mb * 1024 * 1024)
        if args.target_size_mb
        else None,
        batch_size=args.batch_size,
        api_sleep_seconds=args.api_sleep_seconds,
    )


if __name__ == "__main__":
    cli()
