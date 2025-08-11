from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Databricks: uses the active session if it exists (Jobs/Serverless),
    otherwise creates a new one with the given name.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
