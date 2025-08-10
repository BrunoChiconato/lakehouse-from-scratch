import pytest
from pyspark.sql import SparkSession
import os

from utils.spark_utils import get_spark_session
from config import settings


VALID_SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,"
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)


def test_get_spark_session_raises_error_if_packages_not_set(mocker):
    """
    Unit Test: Verifies that get_spark_session raises a ValueError if the
    SPARK_PACKAGES environment variable is not set.
    """
    mocker.patch.dict(os.environ, clear=True)

    with pytest.raises(ValueError) as excinfo:
        get_spark_session("TestApp")

    assert "SPARK_PACKAGES environment variable is not set" in str(excinfo.value)


def test_get_spark_session_success_and_config_check(mocker):
    """
    Integration-like Test: Verifies that get_spark_session successfully creates a
    SparkSession and applies all the correct configurations.
    """
    mocker.patch.dict(os.environ, {"SPARK_PACKAGES": VALID_SPARK_PACKAGES})

    session = None
    try:
        session = get_spark_session("MyTestApp")
        assert isinstance(session, SparkSession)

        conf = session.conf
        assert conf.get("spark.app.name") == "MyTestApp"
        assert "IcebergSparkSessionExtensions" in conf.get("spark.sql.extensions")
        assert (
            conf.get(f"spark.sql.catalog.{settings.SPARK_CATALOG_NAME}.catalog-impl")
            == "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        assert conf.get("spark.jars.packages") == VALID_SPARK_PACKAGES

    finally:
        if session:
            session.stop()
