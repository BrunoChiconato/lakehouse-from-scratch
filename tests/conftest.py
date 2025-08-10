import pytest
import os
import subprocess
from pyspark.sql import SparkSession

from utils.spark_utils import get_spark_session


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """
    pytest fixture to create a SparkSession for testing.
    """
    print("--- Setting up SparkSession for data quality tests ---")
    if "SPARK_MASTER_URL" not in os.environ:
        print("Starting Docker container for Spark...")
        subprocess.run(["make", "up"], check=True, capture_output=True)

    spark = get_spark_session("DataQualityTests")
    yield spark
    print("--- Tearing down SparkSession ---")
    spark.stop()


@pytest.fixture(scope="session")
def pipeline_data(spark_session):
    """
    Fixture to ensure the pipeline has been run and data is available.
    It runs the pipeline in 'test mode' for speed.
    """
    print(
        "--- Running the full data pipeline in TEST MODE to generate data for tests ---"
    )

    result = subprocess.run(
        ["make", "run_full_pipeline", "RUN_MODE=test"], capture_output=True, text=True
    )

    if result.returncode != 0:
        print("Pipeline execution failed!")
        print(result.stdout)
        print(result.stderr)
        pytest.fail(
            "Pipeline execution failed, aborting data quality tests.", pytrace=False
        )

    print("--- Pipeline execution complete. Data is ready. ---")
    return True
