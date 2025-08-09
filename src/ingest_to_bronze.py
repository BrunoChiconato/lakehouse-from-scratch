import os
import io
import logging
import time
from typing import List, Dict, Any
import xml.etree.ElementTree as ET

import boto3
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
ARXIV_BASE_URL = "http://export.arxiv.org/api/query"
ARXIV_SORT_BY = "submittedDate"
ARXIV_SORT_ORDER = "descending"

CATEGORIES_TO_FETCH = [
    "cs.AI",
    "cs.CL",
    "cs.CC",
    "cs.CE",
    "cs.CG",
    "cs.GT",
    "cs.CV",
    "cs.CY",
    "cs.CR",
    "cs.DS",
    "cs.DB",
    "cs.DL",
    "cs.DM",
    "cs.DC",
    "cs.ET",
    "cs.FL",
    "cs.GL",
    "cs.GR",
    "cs.AR",
    "cs.HC",
    "cs.IR",
    "cs.IT",
    "cs.LG",
    "cs.LO",
    "cs.MS",
    "cs.MA",
    "cs.MM",
    "cs.NI",
    "cs.NE",
    "cs.NA",
    "cs.OS",
    "cs.OH",
    "cs.PF",
    "cs.PL",
    "cs.RO",
    "cs.SI",
    "cs.SE",
    "cs.SD",
    "cs.SC",
    "cs.SY",
]

TARGET_SIZE_MB = 256
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
BATCH_SIZE = 1000
API_SLEEP_SECONDS = 3

BRONZE_OUTPUT_KEY = f"bronze/articles_parquet/data_all_cs_{TARGET_SIZE_MB}MB.parquet"

ATOM_NAMESPACE = "{http://www.w3.org/2005/Atom}"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_xml_entry(entry, namespace: str) -> Dict[str, Any]:
    """
    Helper function to parse a single XML <entry> element into a Python dictionary.
    Can raise exceptions if elements are not found or text is missing.
    """
    return {
        "id": entry.find(f"{namespace}id").text.split("/")[-1],
        "title": entry.find(f"{namespace}title").text,
        "summary": entry.find(f"{namespace}summary").text.strip(),
        "authors": [
            author.find(f"{namespace}name").text
            for author in entry.findall(f"{namespace}author")
        ],
        "categories": [
            cat.get("term") for cat in entry.findall(f"{namespace}category")
        ],
        "published_date": entry.find(f"{namespace}published").text,
        "updated_date": entry.find(f"{namespace}updated").text,
        "pdf_url": entry.find(f'{namespace}link[@title="pdf"]').get("href")
        if entry.find(f'{namespace}link[@title="pdf"]') is not None
        else "",
    }


def main() -> None:
    """
    Dynamically fetches data from the arXiv API by iterating through a list of
    categories, making separate, paginated requests for each. It continues until
    a target file size is reached or all articles are fetched. It is resilient
    to malformed XML entries from the API.
    """
    if not S3_BUCKET:
        logger.error("S3_BUCKET_NAME environment variable is not set. Aborting.")
        return

    s3_client = boto3.client("s3")
    logger.info(f"Target S3 Bucket: {S3_BUCKET}, Target Size: {TARGET_SIZE_MB}MB")

    all_articles_data: List[Dict] = []

    try:
        for category in CATEGORIES_TO_FETCH:
            logger.info(f"--- Starting fetch for category: {category} ---")
            start_index = 0

            while True:
                params = {
                    "search_query": f"cat:{category}",
                    "sortBy": ARXIV_SORT_BY,
                    "sortOrder": ARXIV_SORT_ORDER,
                    "start": start_index,
                    "max_results": BATCH_SIZE,
                }

                logger.info(
                    f"Fetching batch for '{category}', starting from index {start_index}..."
                )
                response = requests.get(ARXIV_BASE_URL, params=params)
                response.raise_for_status()

                try:
                    root = ET.fromstring(response.content)
                    entries = root.findall(f"{ATOM_NAMESPACE}entry")

                    if not entries:
                        logger.info(
                            f"No more articles found for category '{category}'. Moving to next category."
                        )
                        break

                    parsed_count_in_batch = 0
                    for entry in entries:
                        try:
                            article_data = parse_xml_entry(entry, ATOM_NAMESPACE)
                            all_articles_data.append(article_data)
                            parsed_count_in_batch += 1
                        except Exception as e_inner:
                            logger.warning(
                                f"Could not parse a single article entry, skipping it. Error: {e_inner}"
                            )

                    if parsed_count_in_batch > 0:
                        logger.info(
                            f"Successfully parsed {parsed_count_in_batch}/{len(entries)} articles from the batch."
                        )

                except ET.ParseError as e_outer:
                    logger.error(
                        f"Failed to parse entire XML batch due to a major formatting error. Skipping batch. Error: {e_outer}"
                    )
                    start_index += BATCH_SIZE
                    time.sleep(API_SLEEP_SECONDS)
                    continue

                current_size_bytes = (
                    pd.DataFrame(all_articles_data).memory_usage(deep=True).sum()
                )
                logger.info(
                    f"Total articles accumulated: {len(all_articles_data)}. "
                    f"Estimated memory usage: {current_size_bytes / (1024 * 1024):.2f}MB"
                )

                if current_size_bytes >= TARGET_SIZE_BYTES:
                    logger.info(
                        f"Target memory usage of ~{TARGET_SIZE_MB}MB reached. Stopping all fetches."
                    )
                    break

                start_index += len(entries)
                logger.info(f"Waiting for {API_SLEEP_SECONDS} seconds...")
                time.sleep(API_SLEEP_SECONDS)

            if (
                "current_size_bytes" in locals()
                and current_size_bytes >= TARGET_SIZE_BYTES
            ):
                break

        if all_articles_data:
            logger.info("All fetching complete. Converting final list to Parquet...")
            df = pd.DataFrame(all_articles_data).drop_duplicates(subset=["id"])
            logger.info(f"Final DataFrame has {len(df)} unique articles.")

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

            final_size_mb = parquet_buffer.tell() / (1024 * 1024)
            logger.info(
                f"Final Parquet buffer size: {final_size_mb:.2f}MB. Uploading to S3..."
            )

            parquet_buffer.seek(0)
            s3_client.put_object(
                Bucket=S3_BUCKET, Key=BRONZE_OUTPUT_KEY, Body=parquet_buffer.getvalue()
            )
            logger.info(f"Upload complete to s3://{S3_BUCKET}/{BRONZE_OUTPUT_KEY}")
        else:
            logger.warning("No data was collected.")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during the main process: {e}", exc_info=True
        )


if __name__ == "__main__":
    main()
