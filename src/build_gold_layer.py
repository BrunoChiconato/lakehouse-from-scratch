import os
import logging
from pyspark.sql.functions import col, count, explode
from utils.spark_utils import get_spark_session

SILVER_TABLE_NAME = "lakehouse_catalog.arxiv_db.papers"
YEARLY_STATS_TABLE = "lakehouse_catalog.analytics.yearly_publication_stats"
AUTHOR_SUMMARY_TABLE = "lakehouse_catalog.analytics.author_summary"
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/gold/"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_yearly_publication_stats(spark):
    """Cria uma tabela Gold com contagem de publicações por ano e categoria."""
    logging.info(f"Construindo tabela Gold: {YEARLY_STATS_TABLE}")

    papers_df = spark.table(SILVER_TABLE_NAME)

    stats_df = (
        papers_df.withColumn("category", explode(col("categories")))
        .groupBy("publication_year", "category")
        .agg(count("id").alias("paper_count"))
        .orderBy(col("publication_year").desc(), col("paper_count").desc())
    )

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {YEARLY_STATS_TABLE.split('.')[0]}.{YEARLY_STATS_TABLE.split('.')[1]}"
    )
    stats_df.write.mode("overwrite").partitionBy("publication_year").saveAsTable(
        YEARLY_STATS_TABLE
    )
    logging.info(f"Tabela Gold criada com sucesso: {YEARLY_STATS_TABLE}")


def create_author_summary(spark):
    """Cria uma tabela Gold com o resumo de publicações por autor."""
    logging.info(f"Construindo tabela Gold: {AUTHOR_SUMMARY_TABLE}")

    papers_df = spark.table(SILVER_TABLE_NAME)

    author_df = (
        papers_df.withColumn("author_name", explode(col("authors")))
        .groupBy("author_name")
        .agg(count("id").alias("total_papers"))
        .orderBy(col("total_papers").desc())
    )

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {AUTHOR_SUMMARY_TABLE.split('.')[0]}.{AUTHOR_SUMMARY_TABLE.split('.')[1]}"
    )
    author_df.write.mode("overwrite").saveAsTable(AUTHOR_SUMMARY_TABLE)
    logging.info(f"Tabela Gold criada com sucesso: {AUTHOR_SUMMARY_TABLE}")


def main():
    """Função principal para construir todas as tabelas da camada Gold."""
    if not S3_BUCKET:
        logging.error("S3_BUCKET_NAME não encontrada.")
        return

    spark = get_spark_session("SilverToGold", WAREHOUSE_PATH)

    create_yearly_publication_stats(spark)
    create_author_summary(spark)

    spark.stop()


if __name__ == "__main__":
    main()
