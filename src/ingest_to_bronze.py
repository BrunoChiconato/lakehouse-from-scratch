import os
import boto3
import requests
import json
import logging
import xml.etree.ElementTree as ET
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


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_entry(entry, namespace):
    """Função auxiliar para converter uma entrada XML em um dicionário Python."""
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
    Função principal para extrair dados da API do arXiv e salvá-los na camada Bronze.
    """
    if not S3_BUCKET:
        logging.error("Variável de ambiente S3_BUCKET_NAME não definida.")
        return

    s3_client = boto3.client("s3")
    logging.info(f"Conectado ao S3. Bucket de destino: {S3_BUCKET}")

    params = {
        "search_query": ARXIV_SEARCH_QUERY,
        "sortBy": ARXIV_SORT_BY,
        "sortOrder": ARXIV_SORT_ORDER,
        "start": 0,
        "max_results": ARXIV_MAX_RESULTS,
    }

    logging.info(f"Requisitando {params['max_results']} artigos da API do arXiv...")
    try:
        response = requests.get(ARXIV_BASE_URL, params=params)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespace = "{http://www.w3.org/2005/Atom}"

        article_count = 0
        for entry in root.findall(f"{namespace}entry"):
            article_data = parse_entry(entry, namespace)
            safe_id = article_data["id"].replace("/", "_")
            file_key = f"bronze/articles/{safe_id}.json"

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=json.dumps(article_data, indent=4, ensure_ascii=False),
            )
            article_count += 1

        logging.info(f"Extração concluída. Total de {article_count} artigos salvos.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Requisição HTTP para a API do arXiv falhou: {e}")
    except ET.ParseError as e:
        logging.error(f"Falha ao processar o XML da resposta: {e}")
    except Exception as e:
        logging.error(f"Um erro inesperado ocorreu: {e}")


if __name__ == "__main__":
    main()
