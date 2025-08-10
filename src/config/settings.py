import os
from dotenv import load_dotenv

load_dotenv()

RUN_MODE = os.getenv("RUN_MODE", "production")

LOG_FORMAT = os.getenv("LOG_FORMAT", "text")

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

if RUN_MODE == "test":
    TARGET_SIZE_MB = 1
    print(f"INFO: Running in TEST mode. Target size set to {TARGET_SIZE_MB}MB.")
else:
    TARGET_SIZE_MB = int(os.getenv("TARGET_SIZE_MB", "128"))

TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
API_SLEEP_SECONDS = int(os.getenv("API_SLEEP_SECONDS", "3"))
BRONZE_OUTPUT_KEY = f"bronze/articles_parquet/data_all_cs_{TARGET_SIZE_MB}MB.parquet"

DEFAULT_WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/warehouse"
DEFAULT_CATALOG_NAME = "lakehouse_catalog"
SPARK_CATALOG_NAME = os.getenv("SPARK_CATALOG_NAME", DEFAULT_CATALOG_NAME)
SPARK_WAREHOUSE_PATH = os.getenv("SPARK_WAREHOUSE_PATH", DEFAULT_WAREHOUSE_PATH)
