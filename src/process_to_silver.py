import os
import logging
from pyspark.sql.functions import col, year, to_date
from utils.spark_utils import get_spark_session


SILVER_TABLE_NAME = "lakehouse_catalog.arxiv_db.papers"
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/articles/"
SILVER_WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/silver/"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def transform_raw_data(spark, path):
    """Lê e transforma os dados brutos da camada Bronze."""
    df_raw = spark.read.option("multiline", "true").json(path)

    df_transformed = df_raw.select(
        col("id"),
        col("title"),
        col("summary"),
        col("authors"),
        col("categories"),
        to_date(col("published_date")).alias("published_date"),
        to_date(col("updated_date")).alias("updated_date"),
        col("pdf_url"),
    ).withColumn("publication_year", year(col("published_date")))

    return df_transformed


def upsert_to_silver(spark, df, table_name):
    """
    Realiza o MERGE (upsert) dos dados na tabela Silver do Iceberg.
    Cria a tabela se ela não existir.
    """
    df.createOrReplaceTempView("source_papers")

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING source_papers s
    ON t.id = s.id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """

    logging.info(f"Executando MERGE na tabela: {table_name}")
    spark.sql(merge_sql)


def main():
    """Main function for the Bronze to Silver ETL job."""
    if not S3_BUCKET:
        logging.error("S3_BUCKET_NAME não encontrada. Verifique seu arquivo .env.")
        return

    spark = get_spark_session("BronzeToSilver", SILVER_WAREHOUSE_PATH)
    logging.info("Spark Session criada com sucesso.")

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {SILVER_TABLE_NAME.split('.')[0]}.{SILVER_TABLE_NAME.split('.')[1]}"
    )

    df_transformed = transform_raw_data(spark, BRONZE_PATH)
    logging.info(
        f"Lidos e transformados {df_transformed.count()} registros de {BRONZE_PATH}"
    )

    upsert_to_silver(spark, df_transformed, SILVER_TABLE_NAME)

    logging.info("Processo de MERGE na tabela Silver concluído com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()
