import os
import boto3
import requests
import json
import logging
import xml.etree.ElementTree as ET
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_entry(entry, namespace):
    """Helper function to parse a single XML entry into a dictionary."""
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


def main():
    """
    Main function to extract data directly from the arXiv API using requests.
    """
    load_dotenv()
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    if not s3_bucket:
        logging.error("S3_BUCKET_NAME not set.")
        return

    s3_client = boto3.client("s3")
    logging.info(f"Connected to S3, target bucket: {s3_bucket}")

    base_url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": "cat:cs.AI OR cat:cs.LG OR cat:cs.CL",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "start": 0,
        "max_results": 150,
    }

    logging.info(f"Requesting {params['max_results']} articles from arXiv API...")
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespace = "{http://www.w3.org/2005/Atom}"

        article_count = 0
        for entry in root.findall(f"{namespace}entry"):
            article_data = parse_entry(entry, namespace)
            safe_id = article_data["id"].replace("/", "_")
            file_key = f"bronze/articles/{safe_id}.json"

            s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_key,
                Body=json.dumps(article_data, indent=4, ensure_ascii=False),
            )
            article_count += 1

        logging.info(f"Extraction complete. Total of {article_count} articles saved.")

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request to arXiv API failed: {e}")
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML response: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
