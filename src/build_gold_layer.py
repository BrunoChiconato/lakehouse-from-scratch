import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, explode
from utils.spark_utils import get_spark_session


S3_BUCKET = os.getenv("S3_BUCKET_NAME")
CATALOG_NAME = os.getenv("SPARK_CATALOG_NAME", "lakehouse_catalog")

SILVER_DB = "arxiv_db"
SILVER_TABLE = "papers"
SILVER_TABLE_FQN = f"{CATALOG_NAME}.{SILVER_DB}.{SILVER_TABLE}"

GOLD_DB = "analytics"
YEARLY_STATS_TABLE = "yearly_publication_stats"
AUTHOR_SUMMARY_TABLE = "author_summary"
YEARLY_STATS_TABLE_FQN = f"{CATALOG_NAME}.{GOLD_DB}.{YEARLY_STATS_TABLE}"
AUTHOR_SUMMARY_TABLE_FQN = f"{CATALOG_NAME}.{GOLD_DB}.{AUTHOR_SUMMARY_TABLE}"


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the database exists in the catalog."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{db_name}")


def create_yearly_publication_stats(papers_df: DataFrame, table_name: str) -> None:
    """Creates a Gold table with publication counts per year and category."""
    stats_df = (
        papers_df.withColumn("category", explode(col("categories")))
        .groupBy("publication_year", "category")
        .agg(count("id").alias("paper_count"))
        .orderBy(col("publication_year").desc(), col("paper_count").desc())
    )

    (
        stats_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("publication_year")
        .saveAsTable(table_name)
    )


def create_author_summary(papers_df: DataFrame, table_name: str) -> None:
    """Creates a Gold table summarizing publications per author."""
    author_df = (
        papers_df.withColumn("author_name", explode(col("authors")))
        .groupBy("author_name")
        .agg(count("id").alias("total_papers"))
        .orderBy(col("total_papers").desc())
    )

    (
        author_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def main() -> None:
    """Main function to build all Gold layer aggregated tables."""
    if not S3_BUCKET:
        print("Error: S3_BUCKET_NAME environment variable not set. Aborting.")
        return

    spark = get_spark_session("SilverToGold")
    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    setup_database(spark, GOLD_DB)
    logger.info(f"Database '{GOLD_DB}' is ready.")

    logger.info(f"Reading data from Silver table: {SILVER_TABLE_FQN}")

    papers_df = spark.table(SILVER_TABLE_FQN).cache()
    logger.info("Silver table loaded and cached.")

    logger.info(f"Building Gold table: {YEARLY_STATS_TABLE_FQN}")
    create_yearly_publication_stats(papers_df, YEARLY_STATS_TABLE_FQN)
    logger.info("Successfully created yearly publication stats table.")

    logger.info(f"Building Gold table: {AUTHOR_SUMMARY_TABLE_FQN}")
    create_author_summary(papers_df, AUTHOR_SUMMARY_TABLE_FQN)
    logger.info("Successfully created author summary table.")

    spark.catalog.clearCache()
    spark.stop()


if __name__ == "__main__":
    main()
