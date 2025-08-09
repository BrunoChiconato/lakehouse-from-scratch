import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, explode
from utils.spark_utils import get_spark_session
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
CATALOG_NAME = os.getenv("SPARK_CATALOG_NAME", "lakehouse_catalog")

SILVER_DB = "arxiv_db"
SILVER_TABLE = "papers"
GOLD_DB = "analytics"
DIM_CATEGORIES_TABLE = "dim_categories"
FACT_PUBLICATION_TRENDS_TABLE = "fact_publication_trends"

SILVER_TABLE_FQN = f"{CATALOG_NAME}.{SILVER_DB}.{SILVER_TABLE}"
DIM_CATEGORIES_TABLE_FQN = f"{CATALOG_NAME}.{GOLD_DB}.{DIM_CATEGORIES_TABLE}"
FACT_PUBLICATION_TRENDS_TABLE_FQN = (
    f"{CATALOG_NAME}.{GOLD_DB}.{FACT_PUBLICATION_TRENDS_TABLE}"
)


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the database exists in the catalog."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{db_name}")
    logger.info(f"Database '{db_name}' is ready.")


def create_categories_dimension_table(
    spark: SparkSession, table_name_fqn: str
) -> DataFrame:
    """
    Creates and saves a dimension table to translate arXiv category codes
    into human-readable names.
    """
    logger.info("Creating category dimension table.")
    categories_data = [
        ("cs.AI", "Artificial Intelligence"),
        ("cs.CL", "Computation and Language"),
        ("cs.CC", "Computational Complexity"),
        ("cs.CE", "Computational Engineering, Finance, and Science"),
        ("cs.CG", "Computational Geometry"),
        ("cs.GT", "Computer Science and Game Theory"),
        ("cs.CV", "Computer Vision and Pattern Recognition"),
        ("cs.CY", "Computers and Society"),
        ("cs.CR", "Cryptography and Security"),
        ("cs.DS", "Data Structures and Algorithms"),
        ("cs.DB", "Databases"),
        ("cs.DL", "Digital Libraries"),
        ("cs.DM", "Discrete Mathematics"),
        ("cs.DC", "Distributed, Parallel, and Cluster Computing"),
        ("cs.ET", "Emerging Technologies"),
        ("cs.FL", "Formal Languages and Automata Theory"),
        ("cs.GL", "General Literature"),
        ("cs.GR", "Graphics"),
        ("cs.AR", "Hardware Architecture"),
        ("cs.HC", "Human-Computer Interaction"),
        ("cs.IR", "Information Retrieval"),
        ("cs.IT", "Information Theory"),
        ("cs.LG", "Machine Learning"),
        ("cs.LO", "Logic in Computer Science"),
        ("cs.MS", "Mathematical Software"),
        ("cs.MA", "Multiagent Systems"),
        ("cs.MM", "Multimedia"),
        ("cs.NI", "Networking and Internet Architecture"),
        ("cs.NE", "Neural and Evolutionary Computing"),
        ("cs.NA", "Numerical Analysis"),
        ("cs.OS", "Operating Systems"),
        ("cs.OH", "Other Computer Science"),
        ("cs.PF", "Performance"),
        ("cs.PL", "Programming Languages"),
        ("cs.RO", "Robotics"),
        ("cs.SI", "Social and Information Networks"),
        ("cs.SE", "Software Engineering"),
        ("cs.SD", "Sound"),
        ("cs.SC", "Symbolic Computation"),
        ("cs.SY", "Systems and Control"),
    ]

    columns = ["category_code", "category_name"]
    dim_df = spark.createDataFrame(categories_data, columns)

    logger.info(f"Saving dimension table to: {table_name_fqn}")
    (
        dim_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name_fqn)
    )
    return dim_df


def create_publication_trends_fact_table(
    papers_df: DataFrame, categories_df: DataFrame, table_name_fqn: str
) -> None:
    """
    Creates an aggregated fact table with publication counts per year and
    human-readable category name.
    """
    logger.info("Exploding paper categories for analysis.")
    papers_with_exploded_categories = papers_df.withColumn(
        "category_code", explode(col("categories"))
    )

    logger.info("Joining papers with category dimension table.")
    enriched_papers = papers_with_exploded_categories.join(
        categories_df, "category_code", "left"
    )

    logger.info("Aggregating data to create publication trends.")
    trends_df = (
        enriched_papers.groupBy("publication_year", "category_name")
        .agg(count("id").alias("paper_count"))
        .orderBy(col("publication_year").desc(), col("paper_count").desc())
    )

    logger.info(f"Saving aggregated fact table to: {table_name_fqn}")
    (
        trends_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("publication_year")
        .saveAsTable(table_name_fqn)
    )


def main() -> None:
    """Main function to build all Gold layer tables."""
    logger.info("Starting Gold layer creation process.")
    if not S3_BUCKET:
        logger.error("S3_BUCKET_NAME environment variable not set. Aborting.")
        return

    spark = None
    try:
        spark = get_spark_session("SilverToGold")
        logger.info("Spark session created successfully.")

        setup_database(spark, GOLD_DB)

        logger.info("Creating and saving the categories dimension table.")
        categories_df = create_categories_dimension_table(
            spark, DIM_CATEGORIES_TABLE_FQN
        )

        logger.info(f"Reading data from Silver table: {SILVER_TABLE_FQN}")
        papers_df = spark.table(SILVER_TABLE_FQN).cache()
        logger.info("Silver table loaded and cached.")

        logger.info(f"Building Gold fact table: {FACT_PUBLICATION_TRENDS_TABLE_FQN}")
        create_publication_trends_fact_table(
            papers_df, categories_df, FACT_PUBLICATION_TRENDS_TABLE_FQN
        )
        logger.info("Successfully created publication trends fact table.")

    except Exception as e:
        logger.error(f"An error occurred during the Gold layer job: {e}", exc_info=True)
    finally:
        if spark:
            if papers_df.is_cached:
                papers_df.unpersist()
                logger.info("Unpersisted Silver table DataFrame.")
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()
