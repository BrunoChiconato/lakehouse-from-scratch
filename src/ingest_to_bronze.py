import os
import json
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Any

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()


S3_BUCKET = os.getenv("S3_BUCKET_NAME")
ARXIV_BASE_URL = "http://export.arxiv.org/api/query"
ARXIV_SEARCH_QUERY = os.getenv(
    "ARXIV_SEARCH_QUERY", "cat:cs.AI OR cat:cs.LG OR cat:cs.CL"
)
ARXIV_MAX_RESULTS = int(os.getenv("ARXIV_MAX_RESULTS", 150))
ARXIV_SORT_BY = "submittedDate"
ARXIV_SORT_ORDER = "descending"

ATOM_NAMESPACE = "{http://www.w3.org/2005/Atom}"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_xml_entry(entry: ET.Element, namespace: str) -> Dict[str, Any]:
    """
    Helper function to parse a single XML <entry> element into a Python dictionary.

    Args:
        entry: The XML element for an article.
        namespace: The Atom XML namespace.

    Returns:
        A dictionary containing the parsed article data.
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
    Main function to fetch data from the arXiv API and save it to the Bronze layer in S3.
    The process is idempotent at the file level; re-running will overwrite existing files.
    """
    if not S3_BUCKET:
        logger.error("Environment variable S3_BUCKET_NAME is not set. Aborting.")
        return

    s3_client = boto3.client("s3")
    logger.info(f"Connected to S3. Target bucket: {S3_BUCKET}")

    params = {
        "search_query": ARXIV_SEARCH_QUERY,
        "sortBy": ARXIV_SORT_BY,
        "sortOrder": ARXIV_SORT_ORDER,
        "start": 0,
        "max_results": ARXIV_MAX_RESULTS,
    }

    logger.info(f"Requesting {params['max_results']} articles from the arXiv API...")
    try:
        response = requests.get(ARXIV_BASE_URL, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        article_count = 0
        for entry in root.findall(f"{ATOM_NAMESPACE}entry"):
            article_data = parse_xml_entry(entry, ATOM_NAMESPACE)
            safe_id = article_data["id"].replace("/", "_")
            file_key = f"bronze/articles/{safe_id}.json"

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=json.dumps(article_data, indent=4, ensure_ascii=False),
            )
            article_count += 1

        logger.info(f"Extraction complete. Total of {article_count} articles saved.")

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request to arXiv API failed: {e}")
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML response: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()
