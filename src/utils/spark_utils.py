from pyspark.sql import SparkSession


def get_spark_session(app_name: str, warehouse_path: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured for Iceberg and S3.
    Packages are expected to be provided via spark-submit.
    """
    return (
        SparkSession.builder.appName(app_name)
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
        .getOrCreate()
    )
