import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_spark_session(
    aws_access_key: str, aws_secret_key: str, s3_bucket: str
) -> SparkSession:
    """Creates and configures the SparkSession to work with Iceberg on S3."""
    warehouse_path = f"s3a://{s3_bucket}/silver/"

    return (
        SparkSession.builder.appName("ArxivETLToIceberg")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,software.amazon.awssdk:bundle:2.17.257,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse_catalog.type", "hadoop")
        .config("spark.sql.catalog.lakehouse_catalog.warehouse", warehouse_path)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .getOrCreate()
    )


def main():
    """
    Main function for the ETL job.
    """
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket = os.getenv("S3_BUCKET_NAME")

    if not all([aws_key, aws_secret, s3_bucket]):
        logging.error(
            "AWS credentials or S3_BUCKET_NAME not found. Check your .env file."
        )
        return

    spark = get_spark_session(aws_key, aws_secret, s3_bucket)
    logging.info("Spark Session created successfully.")

    bronze_data_path = f"s3a://{s3_bucket}/bronze/articles/"
    df_raw = spark.read.option("multiline", "true").json(bronze_data_path)
    logging.info(f"Read {df_raw.count()} records from {bronze_data_path}")
    df_raw.printSchema()

    df_transformed = df_raw.select(
        col("id"),
        col("title"),
        col("summary"),
        col("authors"),
        col("categories"),
        to_date(col("published_date")).alias("published_date"),
        to_date(col("updated_date")).alias("updated_date"),
    ).withColumn("publication_year", year(col("published_date")))

    logging.info("Data transformed. Final schema:")
    df_transformed.printSchema()

    table_name = "lakehouse_catalog.arxiv_db.papers"
    logging.info(f"Writing data to Iceberg table: {table_name}")

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse_catalog.arxiv_db")

    df_transformed.write.mode("overwrite").saveAsTable(table_name)

    logging.info("Write to Iceberg table completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
