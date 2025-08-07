import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, to_date
from utils.spark_utils import get_spark_session


S3_BUCKET = os.getenv("S3_BUCKET_NAME")
CATALOG_NAME = os.getenv("SPARK_CATALOG_NAME", "lakehouse_catalog")
DB_NAME = "arxiv_db"
TABLE_NAME = "papers"
SILVER_TABLE_FQN = f"{CATALOG_NAME}.{DB_NAME}.{TABLE_NAME}"

BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/articles/"


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the database exists in the catalog."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{db_name}")


def transform_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Reads raw JSON data from the Bronze layer and applies transformations.
    This function defines the schema contract for the Silver layer.
    """
    df_raw = spark.read.option("multiline", "true").json(path)

    df_transformed = df_raw.select(
        col("id"),
        col("title"),
        col("summary"),
        col("authors"),
        col("categories"),
        to_date(col("published_date")).alias("published_date"),
        to_date(col("updated_date")).alias("updated_date"),
        col("pdf_url"),
    ).withColumn("publication_year", year(col("published_date")))

    return df_transformed


def upsert_to_silver(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    """
    Performs an idempotent MERGE (upsert) operation into the Silver Iceberg table.
    It creates or updates records based on the 'id' column.
    """
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


def main() -> None:
    """Main ETL job to process data from Bronze to the Silver layer."""
    if not S3_BUCKET:
        print("Error: S3_BUCKET_NAME environment variable not set. Aborting.")
        return

    spark = get_spark_session("BronzeToSilver")
    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    logger.info("Spark Session created successfully.")

    setup_database(spark, DB_NAME)
    logger.info(f"Database '{DB_NAME}' is ready.")

    df_transformed = transform_raw_data(spark, BRONZE_PATH)

    record_count = df_transformed.count()
    logger.info(f"Read and transformed {record_count} records from {BRONZE_PATH}")

    logger.info(f"Executing MERGE into Silver table: {SILVER_TABLE_FQN}")
    upsert_to_silver(spark, df_transformed, SILVER_TABLE_FQN)
    logger.info("MERGE process into Silver table completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
