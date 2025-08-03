import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_spark_session(
    aws_access_key: str, aws_secret_key: str, s3_bucket: str
) -> SparkSession:
    """
    Creates and configures the SparkSession.
    - one for reading from Silver;
    - one for writing to Gold.
    """
    silver_path = f"s3a://{s3_bucket}/silver/"
    gold_path = f"s3a://{s3_bucket}/gold/"

    return (
        SparkSession.builder.appName("BuildGoldTables")
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
        .config("spark.sql.catalog.lakehouse_catalog.warehouse", silver_path)
        .config(
            "spark.sql.catalog.gold_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.gold_catalog.type", "hadoop")
        .config("spark.sql.catalog.gold_catalog.warehouse", gold_path)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .getOrCreate()
    )


def create_yearly_publication_stats(spark: SparkSession):
    """
    Creates a Gold table with publication counts per year and category.
    """
    logging.info("Building Gold table: yearly_publication_stats")

    papers_df = spark.table("lakehouse_catalog.arxiv_db.papers")

    stats_df = (
        papers_df.withColumn("category", explode(col("categories")))
        .groupBy("publication_year", "category")
        .agg(count("id").alias("paper_count"))
        .orderBy(col("publication_year").desc(), col("paper_count").desc())
    )

    table_name = "gold_catalog.analytics.yearly_publication_stats"
    spark.sql("CREATE DATABASE IF NOT EXISTS gold_catalog.analytics")

    stats_df.write.mode("overwrite").saveAsTable(table_name)
    logging.info(f"Successfully created Gold table: {table_name}")


def create_author_summary(spark: SparkSession):
    """
    Creates a Gold table summarizing each author's publication activity.
    """
    logging.info("Building Gold table: author_summary")

    papers_df = spark.table("lakehouse_catalog.arxiv_db.papers")

    author_df = (
        papers_df.withColumn("author_name", explode(col("authors")))
        .groupBy("author_name")
        .agg(count("id").alias("total_papers"))
        .orderBy(col("total_papers").desc())
    )

    table_name = "gold_catalog.entities.author_summary"
    spark.sql("CREATE DATABASE IF NOT EXISTS gold_catalog.entities")

    author_df.write.mode("overwrite").saveAsTable(table_name)
    logging.info(f"Successfully created Gold table: {table_name}")


def main():
    """Main function to build all Gold layer tables."""
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket = os.getenv("S3_BUCKET_NAME")

    if not all([aws_key, aws_secret, s3_bucket]):
        logging.error("AWS credentials or S3_BUCKET_NAME not found.")
        return

    spark = get_spark_session(aws_key, aws_secret, s3_bucket)

    create_yearly_publication_stats(spark)
    create_author_summary(spark)

    spark.stop()


if __name__ == "__main__":
    main()
