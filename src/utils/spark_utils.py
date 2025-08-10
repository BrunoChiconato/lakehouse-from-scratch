import os
from pyspark.sql import SparkSession
from config import settings


def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured for the Lakehouse environment.

    This utility centralizes Spark configuration, ensuring that all parts of the
    application (ETL jobs, tests) use a consistent setup.
    """
    packages = os.getenv("SPARK_PACKAGES")
    if not packages:
        raise ValueError(
            "SPARK_PACKAGES environment variable is not set. Please check your .env file."
        )

    spark_builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{settings.SPARK_CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{settings.SPARK_CATALOG_NAME}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{settings.SPARK_CATALOG_NAME}.warehouse",
            settings.SPARK_WAREHOUSE_PATH,
        )
        .config("spark.jars.packages", packages)
    )

    return spark_builder.getOrCreate()
