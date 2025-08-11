import os

try:
    from dotenv import load_dotenv
except ImportError:

    def load_dotenv(*args, **kwargs):
        return False


load_dotenv()

RUN_MODE = os.getenv("RUN_MODE", "production")

LOG_FORMAT = os.getenv("LOG_FORMAT", "text")

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

UC_CATALOG = os.getenv("UC_CATALOG", "lakehouse")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "arxiv_db"
GOLD_SCHEMA = "analytics"

SPARK_CATALOG_NAME = UC_CATALOG

if RUN_MODE == "test":
    TARGET_SIZE_MB = 1
else:
    TARGET_SIZE_MB = int(os.getenv("TARGET_SIZE_MB", "128"))

TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
API_SLEEP_SECONDS = int(os.getenv("API_SLEEP_SECONDS", "3"))
