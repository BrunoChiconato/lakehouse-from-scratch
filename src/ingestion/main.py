import logging
import time
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
from pydantic import ValidationError

from config import settings
from utils.logging_setup import setup_logging
from contracts.arxiv_contract import ArxivArticle
from ingestion.api_client import ArxivAPIClient
from ingestion.writer import S3Writer

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
            cat.get("term") for cat in entry.findall(f"{ATOM_NAMESPACE}category")
        ],
        "published_date": entry.find(f"{ATOM_NAMESPACE}published").text,
        "updated_date": entry.find(f"{ATOM_NAMESPACE}updated").text,
        "pdf_url": entry.find(f'{ATOM_NAMESPACE}link[@title="pdf"]').get("href") or "",
    }
    return ArxivArticle(**raw_data)


def run_ingestion():
    """
    Orchestrates the data ingestion process.
    """
    api_client = ArxivAPIClient()
    s3_writer = S3Writer(bucket_name=settings.S3_BUCKET)

    all_articles: List[ArxivArticle] = []

    for category in settings.CATEGORIES_TO_FETCH:
        logger.info(f"--- Starting fetch for category: {category} ---")
        start_index = 0

        while True:
            try:
                content = api_client.fetch_batch(
                    category, start_index, settings.BATCH_SIZE
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

                if current_size >= settings.TARGET_SIZE_BYTES:
                    logger.info("Target size reached. Stopping all fetches.")
                    break

                start_index += len(entries)
                time.sleep(settings.API_SLEEP_SECONDS)

            except Exception as e:
                logger.error(
                    f"A critical error occurred during batch fetch for '{category}'. Skipping to next category. Error: {e}"
                )
                break

        if "current_size" in locals() and current_size >= settings.TARGET_SIZE_BYTES:
            break

    if all_articles:
        logger.info("Ingestion complete. Preparing data for writing.")
        final_df = pd.DataFrame([m.model_dump() for m in all_articles]).drop_duplicates(
            subset=["id"]
        )
        logger.info(f"Final unique article count: {len(final_df)}")
        s3_writer.write_parquet(final_df, settings.BRONZE_OUTPUT_KEY)
    else:
        logger.warning("No data was ingested.")


if __name__ == "__main__":
    run_ingestion()
