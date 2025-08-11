import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, explode, broadcast, lower, coalesce

from config import settings
from utils.logging_setup import setup_logging
from utils.spark_utils import get_spark_session

setup_logging()
logger = logging.getLogger(__name__)

SILVER_DB = settings.SILVER_SCHEMA
SILVER_TABLE = "papers"
GOLD_DB = settings.GOLD_SCHEMA
DIM_CATEGORIES_TABLE = "dim_categories"
FACT_PUBLICATION_TRENDS_TABLE = "fact_publication_trends"

SILVER_TABLE_FQN = f"{settings.SPARK_CATALOG_NAME}.{SILVER_DB}.{SILVER_TABLE}"
DIM_CATEGORIES_TABLE_FQN = (
    f"{settings.SPARK_CATALOG_NAME}.{GOLD_DB}.{DIM_CATEGORIES_TABLE}"
)
FACT_PUBLICATION_TRENDS_TABLE_FQN = (
    f"{settings.SPARK_CATALOG_NAME}.{GOLD_DB}.{FACT_PUBLICATION_TRENDS_TABLE}"
)


def setup_database(spark: SparkSession, db_name: str) -> None:
    """Ensures the schema exists in the Unity Catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.SPARK_CATALOG_NAME}.{db_name}")
    logger.info(f"Database '{db_name}' is ready.")


def create_categories_dimension_table(
    spark: SparkSession, table_name_fqn: str
) -> DataFrame:
    """
    Creates and saves the categories dimension table (code -> readable name).
    """
    logger.info("Creating category dimension table.")

    categories_data = [(code, "") for code in settings.CATEGORIES_TO_FETCH]

    category_name_map = {
        "cs.AI": "Artificial Intelligence",
        "cs.CL": "Computation and Language",
        "cs.CC": "Computational Complexity",
        "cs.CE": "Computational Engineering, Finance, and Science",
        "cs.CG": "Computational Geometry",
        "cs.GT": "Computer Science and Game Theory",
        "cs.CV": "Computer Vision and Pattern Recognition",
        "cs.CY": "Computers and Society",
        "cs.CR": "Cryptography and Security",
        "cs.DS": "Data Structures and Algorithms",
        "cs.DB": "Databases",
        "cs.DL": "Digital Libraries",
        "cs.DM": "Discrete Mathematics",
        "cs.DC": "Distributed, Parallel, and Cluster Computing",
        "cs.ET": "Emerging Technologies",
        "cs.FL": "Formal Languages and Automata Theory",
        "cs.GL": "General Literature",
        "cs.GR": "Graphics",
        "cs.AR": "Hardware Architecture",
        "cs.HC": "Human-Computer Interaction",
        "cs.IR": "Information Retrieval",
        "cs.IT": "Information Theory",
        "cs.LG": "Machine Learning",
        "cs.LO": "Logic in Computer Science",
        "cs.MS": "Mathematical Software",
        "cs.MA": "Multiagent Systems",
        "cs.MM": "Multimedia",
        "cs.NI": "Networking and Internet Architecture",
        "cs.NE": "Neural and Evolutionary Computing",
        "cs.NA": "Numerical Analysis",
        "cs.OS": "Operating Systems",
        "cs.OH": "Other Computer Science",
        "cs.PF": "Performance",
        "cs.PL": "Programming Languages",
        "cs.RO": "Robotics",
        "cs.SI": "Social and Information Networks",
        "cs.SE": "Software Engineering",
        "cs.SD": "Sound",
        "cs.SC": "Symbolic Computation",
        "cs.SY": "Systems and Control",
    }

    categories_data_with_names = [
        (code, category_name_map.get(code, "Unknown")) for code, _ in categories_data
    ]

    columns = ["category_code", "category_name"]
    dim_df = spark.createDataFrame(categories_data_with_names, columns)

    logger.info(f"Saving dimension table to: {table_name_fqn}")
    dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_name_fqn
    )
    return dim_df


def compute_publication_trends_df(
    papers_df: DataFrame, categories_df: DataFrame
) -> DataFrame:
    """
    Returns the trends DataFrame (year x category) without saving it.
    - Case-insensitive join (normalizes codes to lowercase)
    - Ensures category_name is not null via coalesce
    """
    papers_with_exploded_categories = papers_df.withColumn(
        "category_code", explode(col("categories"))
    ).withColumn("category_code_lc", lower(col("category_code")))

    categories_df_norm = categories_df.withColumn(
        "category_code_lc", lower(col("category_code"))
    ).select("category_code_lc", "category_name")

    enriched_papers = papers_with_exploded_categories.join(
        broadcast(categories_df_norm), "category_code_lc", "left"
    ).withColumn("category_name", coalesce(col("category_name"), col("category_code")))

    trends_df = (
        enriched_papers.groupBy("publication_year", "category_name")
        .agg(count("id").alias("paper_count"))
        .orderBy(col("publication_year").desc(), col("paper_count").desc())
    )
    return trends_df


def create_publication_trends_fact_table(
    papers_df: DataFrame, categories_df: DataFrame, table_name_fqn: str
) -> None:
    """
    Creates and saves the fact table (Gold) partitioned by publication year.
    """
    logger.info("Aggregating data to create publication trends.")
    trends_df = compute_publication_trends_df(papers_df, categories_df)

    logger.info(f"Saving aggregated fact table to: {table_name_fqn}")
    (
        trends_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("publication_year")
        .saveAsTable(table_name_fqn)
    )


def main() -> None:
    """Gold job compatible with Serverless (no cache/persist)."""
    try:
        spark = get_spark_session("SilverToGold")
        spark.sql(f"USE CATALOG {settings.SPARK_CATALOG_NAME}")
        logger.info("Spark session created successfully.")

        setup_database(spark, GOLD_DB)

        logger.info("Creating and saving the categories dimension table.")
        categories_df = create_categories_dimension_table(
            spark, DIM_CATEGORIES_TABLE_FQN
        )

        logger.info(f"Reading data from Silver table: {SILVER_TABLE_FQN}")
        papers_df = spark.table(SILVER_TABLE_FQN)
        row_count = papers_df.count()
        logger.info(f"Silver table loaded with {row_count} records.")

        logger.info(f"Building Gold fact table: {FACT_PUBLICATION_TRENDS_TABLE_FQN}")
        create_publication_trends_fact_table(
            papers_df, categories_df, FACT_PUBLICATION_TRENDS_TABLE_FQN
        )
        logger.info("Successfully created publication trends fact table.")

    except Exception as e:
        logger.error(f"An error occurred during the Gold layer job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
