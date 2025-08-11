import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    year,
    to_date,
    expr,
    explode,
    split,
    trim,
    lower,
    regexp_replace,
    collect_list,
)

from config import settings
from utils.logging_setup import setup_logging
from utils.spark_utils import get_spark_session

setup_logging()
logger = logging.getLogger(__name__)


SILVER_TABLE_FQN = f"{settings.SPARK_CATALOG_NAME}.{settings.SILVER_SCHEMA}.papers"
BRONZE_TABLE_FQN = f"{settings.SPARK_CATALOG_NAME}.{settings.BRONZE_SCHEMA}.arxiv_raw"


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the database exists in the catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.SPARK_CATALOG_NAME}.{db_name}")


def create_table_if_not_exists(spark: SparkSession, table_name: str) -> None:
    """
    Creates the Silver Delta table with an explicit schema if it does not already exist.
    """
    logger.info(f"Ensuring table '{table_name}' exists with the correct schema.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id STRING,
        title STRING,
        summary STRING,
        authors ARRAY<STRING>,
        categories ARRAY<STRING>,
        published_date DATE,
        updated_date DATE,
        pdf_url STRING,
        publication_year INT
    )
    USING delta
    PARTITIONED BY (publication_year)
    """
    spark.sql(create_table_sql)


def transform_raw_data(spark: SparkSession, source_table: str) -> DataFrame:
    """
    Reads Bronze Delta table and applies a robust, multi-step transformation and cleaning process to standardize categories.
    """
    logger.info(f"Reading raw data from table {source_table}")
    df_raw = spark.sql(f"SELECT * FROM {source_table}")

    df_base = df_raw.select(
        col("id"),
        col("title"),
        col("summary"),
        col("authors"),
        col("categories"),
        to_date(col("published_date")).alias("published_date"),
        to_date(col("updated_date")).alias("updated_date"),
        col("pdf_url"),
    ).withColumn("publication_year", year(col("published_date")))

    df_exploded_outer = df_base.withColumn("category_raw", explode(col("categories")))

    df_exploded_inner = df_exploded_outer.withColumn(
        "category_split", split(col("category_raw"), ",")
    ).withColumn("category_single", explode(col("category_split")))

    df_cleaned = df_exploded_inner.withColumn(
        "category_cleaned",
        lower(
            trim(
                regexp_replace(
                    regexp_replace(col("category_single"), "\\s-.*", ""),
                    "\\s*\\(.*\\)",
                    "",
                )
            )
        ),
    )

    df_standardized = df_cleaned.withColumn(
        "category_standardized",
        expr(
            """
            CASE
                WHEN category_cleaned LIKE '%.%' THEN category_cleaned
                ELSE concat(category_cleaned, '.gen')
            END
        """
        ),
    ).filter(col("category_standardized") != ".gen")

    df_final_agg = df_standardized.groupBy(
        "id",
        "title",
        "summary",
        "authors",
        "published_date",
        "updated_date",
        "pdf_url",
        "publication_year",
    ).agg(collect_list("category_standardized").alias("categories"))

    logger.info("Raw data transformed successfully with robust category cleaning.")
    return df_final_agg


def upsert_to_silver(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    """
    Performs an idempotent MERGE (upsert) operation into the Silver Iceberg table.
    """
    logger.info(f"Executing MERGE into Silver table: {table_name}")
    df.createOrReplaceTempView("source_papers")

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING source_papers s
    ON t.id = s.id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """
    spark.sql(merge_sql)
    logger.info("MERGE process completed successfully.")


def main() -> None:
    """Main ETL job to process data from Bronze to the Silver layer."""
    spark = get_spark_session("BronzeToSilver")
    logger.info("Spark Session created successfully.")

    spark.sql(f"USE CATALOG {settings.SPARK_CATALOG_NAME}")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {settings.SPARK_CATALOG_NAME}.{settings.SILVER_SCHEMA}"
    )

    df_transformed = transform_raw_data(spark, BRONZE_TABLE_FQN)
    logger.info(f"Read and transformed {df_transformed.count()} records.")

    create_table_if_not_exists(spark, SILVER_TABLE_FQN)

    if settings.RUN_MODE == "test":
        logger.info("RUN_MODE=test: truncating Silver table before MERGE.")
        spark.sql(f"TRUNCATE TABLE {SILVER_TABLE_FQN}")

    upsert_to_silver(spark, df_transformed, SILVER_TABLE_FQN)


if __name__ == "__main__":
    main()
