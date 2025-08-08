import os
from pyspark.sql import SparkSession


DEFAULT_WAREHOUSE_PATH = f"s3a://{os.getenv('S3_BUCKET_NAME')}/warehouse"
DEFAULT_CATALOG_NAME = "lakehouse_catalog"


def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured for Iceberg and S3.

    Configurations are dynamically sourced from environment variables to adhere
    to the 12-Factor App methodology, allowing the same application artifact
    to be used across different environments (dev, prod) without code changes.

    Args:
        app_name: The name for the Spark application.

    Returns:
        A configured SparkSession object.
    """
    catalog_name = os.getenv("SPARK_CATALOG_NAME", DEFAULT_CATALOG_NAME)
    warehouse_path = os.getenv("SPARK_WAREHOUSE_PATH", DEFAULT_WAREHOUSE_PATH)
    catalog_impl = os.getenv(
        "SPARK_CATALOG_IMPL", "org.apache.iceberg.spark.SparkCatalog"
    )
    catalog_type = os.getenv("SPARK_CATALOG_TYPE", "hadoop")
    s3_impl = os.getenv("SPARK_S3_IMPL", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", catalog_impl)
        .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config("spark.sql.defaultCatalog", catalog_name)
        .config("spark.hadoop.fs.s3a.impl", s3_impl)
    )

    return builder.getOrCreate()
