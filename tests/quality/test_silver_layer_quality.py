import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, current_date, explode

from config import settings

pytestmark = pytest.mark.quality


@pytest.fixture(scope="module")
def silver_papers_df(spark_session: SparkSession, pipeline_data: bool) -> DataFrame:
    """
    Fixture to load the silver 'papers' table as a DataFrame.
    Depends on pipeline_data to ensure data exists.
    Caches the DataFrame for performance within the module.
    """
    assert pipeline_data is True, "Pipeline data fixture failed to run."
    table_fqn = f"{settings.SPARK_CATALOG_NAME}.arxiv_db.papers"
    print(f"Loading data from {table_fqn} for quality tests...")
    df = spark_session.table(table_fqn)
    df.cache()
    yield df
    df.unpersist()


def test_silver_id_is_unique(silver_papers_df: DataFrame):
    """
    Data Quality Test: Validates that the 'id' column is unique.
    """
    duplicate_ids_df = silver_papers_df.groupBy("id").count().filter("count > 1")
    assert duplicate_ids_df.count() == 0, "Found duplicate IDs in the silver table."


def test_silver_critical_columns_are_not_null(silver_papers_df: DataFrame):
    """
    Data Quality Test: Validates that critical columns do not contain null values.
    """
    critical_columns = ["id", "title", "published_date", "publication_year"]
    for column_name in critical_columns:
        null_count = silver_papers_df.filter(col(column_name).isNull()).count()
        assert null_count == 0, f"Found null values in critical column '{column_name}'."


def test_silver_publication_year_is_valid(
    silver_papers_df: DataFrame, spark_session: SparkSession
):
    """
    Data Quality Test: Validates that 'publication_year' is within a reasonable range.
    """
    current_year = (
        spark_session.createDataFrame([(1,)], ["dummy"])
        .select(year(current_date()))
        .first()[0]
    )

    invalid_years_df = silver_papers_df.filter(
        (col("publication_year") < 1991) | (col("publication_year") > current_year)
    )
    assert invalid_years_df.count() == 0, (
        "Found records with invalid publication years."
    )


def test_silver_categories_are_valid_format(silver_papers_df: DataFrame):
    """
    Data Quality Test: Validates that all categories follow a standard format.
    """
    categories_df = silver_papers_df.withColumn("category", explode(col("categories")))

    invalid_categories_df = categories_df.filter(
        ~col("category").rlike("^[a-z0-9-]+\\.")
    )

    count = invalid_categories_df.count()

    assert count == 0, "Found categories with an invalid format."
