from unittest.mock import MagicMock
import transformation.build_gold_layer as gold


def test_setup_database_emite_sql_corretamente(monkeypatch):
    spark = MagicMock()
    spark.sql = MagicMock()

    monkeypatch.setattr(gold.settings, "SPARK_CATALOG_NAME", "cat_test", raising=False)

    gold.setup_database(spark, "analytics_test")

    spark.sql.assert_called_once()
    sql = spark.sql.call_args.args[0]
    assert sql == "CREATE SCHEMA IF NOT EXISTS cat_test.analytics_test"


def test_create_categories_dimension_table_cria_df_e_grava(monkeypatch):
    monkeypatch.setattr(
        gold.settings, "CATEGORIES_TO_FETCH", ["cs.AI", "cs.CL", "cs.XY"], raising=False
    )

    spark = MagicMock()
    dim_df = MagicMock(name="DimDF")

    writer = MagicMock(name="Writer")
    writer.mode.return_value = writer
    writer.option.return_value = writer
    dim_df.write = writer

    spark.createDataFrame.return_value = dim_df

    table_fqn = "cat.analytics.dim_categories"
    out_df = gold.create_categories_dimension_table(spark, table_fqn)

    called_args, called_kwargs = spark.createDataFrame.call_args
    data_passado, colunas = called_args
    assert colunas == ["category_code", "category_name"]
    assert ("cs.AI", "Artificial Intelligence") in data_passado
    assert ("cs.CL", "Computation and Language") in data_passado
    assert ("cs.XY", "Unknown") in data_passado  # fallback

    writer.mode.assert_called_once_with("overwrite")
    writer.option.assert_any_call("overwriteSchema", "true")
    writer.saveAsTable.assert_called_once_with(table_fqn)

    assert out_df is dim_df


def test_create_publication_trends_fact_table_chama_compute_e_salva(monkeypatch):
    papers_df = MagicMock(name="papers_df")
    categories_df = MagicMock(name="categories_df")

    trends_df = MagicMock(name="trends_df")
    writer = MagicMock(name="Writer")
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    trends_df.write = writer

    compute_spy = MagicMock(return_value=trends_df)
    monkeypatch.setattr(gold, "compute_publication_trends_df", compute_spy)

    table_fqn = "cat.analytics.fact_publication_trends"
    gold.create_publication_trends_fact_table(papers_df, categories_df, table_fqn)

    compute_spy.assert_called_once_with(papers_df, categories_df)
    writer.mode.assert_called_once_with("overwrite")
    writer.option.assert_any_call("overwriteSchema", "true")
    writer.partitionBy.assert_called_once_with("publication_year")
    writer.saveAsTable.assert_called_once_with(table_fqn)


def test_main_feliz_caminho_orquestra_chamadas(monkeypatch):
    spark = MagicMock(name="Spark")
    spark.sql = MagicMock()
    papers_df = MagicMock(name="papers_df")
    papers_df.count.return_value = 42
    spark.table.return_value = papers_df

    get_spark_spy = MagicMock(return_value=spark)
    monkeypatch.setattr(gold, "get_spark_session", get_spark_spy)

    setup_db_spy = MagicMock()
    monkeypatch.setattr(gold, "setup_database", setup_db_spy)

    categories_df = MagicMock(name="categories_df")
    create_dim_spy = MagicMock(return_value=categories_df)
    monkeypatch.setattr(gold, "create_categories_dimension_table", create_dim_spy)

    create_fact_spy = MagicMock()
    monkeypatch.setattr(gold, "create_publication_trends_fact_table", create_fact_spy)

    catalog = gold.settings.SPARK_CATALOG_NAME

    gold.main()

    get_spark_spy.assert_called_once_with("SilverToGold")
    assert any(
        (isinstance(c.args[0], str) and f"USE CATALOG {catalog}" in c.args[0])
        for c in spark.sql.call_args_list
    )

    setup_db_spy.assert_called_once_with(spark, gold.GOLD_DB)
    create_dim_spy.assert_called_once_with(spark, gold.DIM_CATEGORIES_TABLE_FQN)
    spark.table.assert_called_once_with(gold.SILVER_TABLE_FQN)
    create_fact_spy.assert_called_once_with(
        papers_df, categories_df, gold.FACT_PUBLICATION_TRENDS_TABLE_FQN
    )
