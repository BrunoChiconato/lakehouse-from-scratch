import logging
import boto3
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

DB_NAME = "arxiv_db"
TABLE_NAME = "papers"
SILVER_TABLE_FQN = f"{settings.SPARK_CATALOG_NAME}.{DB_NAME}.{TABLE_NAME}"
BRONZE_PATH = f"s3a://{settings.S3_BUCKET}/bronze/articles_parquet/"


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the database exists in the catalog."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.SPARK_CATALOG_NAME}.{db_name}")


def create_table_if_not_exists(spark: SparkSession, table_name: str) -> None:
    """
    Creates the Silver Iceberg table with an explicit schema if it does not already exist.
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
    USING iceberg
    PARTITIONED BY (publication_year)
    """
    spark.sql(create_table_sql)


def transform_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Reads compacted Parquet data from the Bronze layer and applies a robust,
    multi-step transformation and cleaning process to standardize categories.
    """
    logger.info(f"Reading raw data from {path}")
    df_raw = spark.read.parquet(path)

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
    if not settings.S3_BUCKET:
        logger.error("S3_BUCKET_NAME environment variable not set. Aborting.")
        return

    spark = None
    try:
        spark = get_spark_session("BronzeToSilver")
        logger.info("Spark Session created successfully.")

        setup_database(spark, DB_NAME)
        logger.info(f"Database '{DB_NAME}' is ready.")

        df_transformed = transform_raw_data(spark, BRONZE_PATH)
        logger.info(f"Read and transformed {df_transformed.count()} records.")

        try:
            spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE_FQN}")
        except Exception:
            glue = boto3.client("glue")
            try:
                glue.delete_table(DatabaseName="arxiv_db", Name="papers")
            except glue.exceptions.EntityNotFoundException:
                pass

        create_table_if_not_exists(spark, SILVER_TABLE_FQN)

        if settings.RUN_MODE == "test":
            logger.info("RUN_MODE=test: truncating Silver table before MERGE.")
            spark.sql(f"TRUNCATE TABLE {SILVER_TABLE_FQN}")

        upsert_to_silver(spark, df_transformed, SILVER_TABLE_FQN)

    except Exception as e:
        logger.error(
            f"An error occurred during the Bronze-to-Silver job: {e}", exc_info=True
        )
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
