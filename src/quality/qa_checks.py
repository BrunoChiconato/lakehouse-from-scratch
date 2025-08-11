import sys
import logging
from pyspark.sql.functions import col, sum as Fsum

from utils.logging_setup import setup_logging
from utils.spark_utils import get_spark_session
from config import settings

setup_logging()
logger = logging.getLogger(__name__)

FACT_FQN = (
    f"{settings.SPARK_CATALOG_NAME}.{settings.GOLD_SCHEMA}.fact_publication_trends"
)
DIM_FQN = f"{settings.SPARK_CATALOG_NAME}.{settings.GOLD_SCHEMA}.dim_categories"
SILVER_FQN = f"{settings.SPARK_CATALOG_NAME}.{settings.SILVER_SCHEMA}.papers"


def fail(msg: str) -> None:
    logger.error(msg)
    sys.exit(1)


def _table_exists(spark, fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        try:
            return spark._jsparkSession.catalog().tableExists(fqn)  # type: ignore[attr-defined]
        except Exception:
            return False


def main() -> None:
    spark = get_spark_session("QAChecks")
    spark.sql(f"USE CATALOG {settings.SPARK_CATALOG_NAME}")

    for tbl in (DIM_FQN, SILVER_FQN, FACT_FQN):
        if not _table_exists(spark, tbl):  # type: ignore[attr-defined]
            fail(f"Tabela não encontrada: {tbl}")

    fact = spark.table(FACT_FQN)
    silver = spark.table(SILVER_FQN)

    if silver.limit(1).count() == 0:
        fail("Silver sem dados.")
    if fact.limit(1).count() == 0:
        fail("Fact (Gold) sem dados.")

    total = fact.count()
    nulls = fact.filter(col("category_name").isNull()).count()
    null_rate = (nulls / total) if total > 0 else 1.0
    logger.info(f"Fact rows={total}, category_name nulls={nulls} ({null_rate:.2%})")
    if null_rate > 0.05:
        fail(f"Muitas categorias sem nome na Fact (>{null_rate:.2%}).")

    by_year = fact.groupBy("publication_year").agg(Fsum("paper_count").alias("n"))
    if by_year.filter(col("n") <= 0).count() > 0:
        fail("Há anos com contagem de papers <= 0 na Fact.")

    logger.info("QA OK ✅")


if __name__ == "__main__":
    main()
