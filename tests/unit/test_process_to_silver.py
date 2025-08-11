from unittest.mock import MagicMock, call

import transformation.process_to_silver as p2s


def _normalize_sql(s: str) -> str:
    return " ".join(s.split())


def test_setup_database_creates_database(monkeypatch):
    spark = MagicMock()
    spark.sql = MagicMock()

    monkeypatch.setattr(p2s.settings, "SPARK_CATALOG_NAME", "lakehouse", raising=True)

    p2s.setup_database(spark, "silver")

    spark.sql.assert_called_once_with("CREATE DATABASE IF NOT EXISTS lakehouse.silver")


def test_create_table_if_not_exists_executes_expected_ddl(monkeypatch):
    spark = MagicMock()
    spark.sql = MagicMock()

    table_fqn = "lakehouse.silver.papers"

    p2s.create_table_if_not_exists(spark, table_fqn)

    assert spark.sql.call_count == 1
    ddl = spark.sql.call_args[0][0]
    ddl_norm = _normalize_sql(ddl)

    assert "CREATE TABLE IF NOT EXISTS lakehouse.silver.papers" in ddl_norm
    assert "USING delta" in ddl_norm
    assert "PARTITIONED BY (publication_year)" in ddl_norm
    assert "id STRING" in ddl_norm
    assert "authors ARRAY<STRING>" in ddl_norm
    assert "categories ARRAY<STRING>" in ddl_norm
    assert "published_date DATE" in ddl_norm
    assert "updated_date DATE" in ddl_norm
    assert "publication_year INT" in ddl_norm


def test_upsert_to_silver_builds_merge_sql_and_executes(monkeypatch):
    spark = MagicMock()
    spark.sql = MagicMock()
    df = MagicMock()

    table_fqn = "lakehouse.silver.papers"

    p2s.upsert_to_silver(spark, df, table_fqn)

    df.createOrReplaceTempView.assert_called_once_with("source_papers")

    assert spark.sql.call_count == 1
    merge_sql = spark.sql.call_args[0][0]
    merge_norm = _normalize_sql(merge_sql)

    assert f"MERGE INTO {table_fqn} t" in merge_norm
    assert "USING source_papers s" in merge_norm
    assert "ON t.id = s.id" in merge_norm
    assert "WHEN MATCHED THEN UPDATE SET *" in merge_norm
    assert "WHEN NOT MATCHED THEN INSERT *" in merge_norm


def test_main_happy_path_with_run_mode_test(monkeypatch):
    monkeypatch.setattr(p2s.settings, "SPARK_CATALOG_NAME", "lakehouse", raising=True)
    monkeypatch.setattr(p2s.settings, "RUN_MODE", "test", raising=True)

    spark = MagicMock()
    spark.sql = MagicMock()
    monkeypatch.setattr(p2s, "get_spark_session", lambda app: spark, raising=True)

    df = MagicMock()
    df.count.return_value = 42
    tr_mock = MagicMock(return_value=df)
    monkeypatch.setattr(p2s, "transform_raw_data", tr_mock, raising=True)

    cti_mock = MagicMock()
    monkeypatch.setattr(p2s, "create_table_if_not_exists", cti_mock, raising=True)

    upsert_mock = MagicMock()
    monkeypatch.setattr(p2s, "upsert_to_silver", upsert_mock, raising=True)

    p2s.main()

    assert call("USE CATALOG lakehouse") in spark.sql.call_args_list

    expected_bronze = getattr(p2s, "BRONZE_TABLE_FQN", None) or getattr(
        p2s, "BRONZE_PATH", None
    )
    tr_mock.assert_called_once()
    args, _ = tr_mock.call_args
    assert args[0] is spark
    assert args[1] == expected_bronze

    expected_silver = p2s.SILVER_TABLE_FQN
    cti_mock.assert_called_once_with(spark, expected_silver)
    assert call(f"TRUNCATE TABLE {expected_silver}") in spark.sql.call_args_list
    upsert_mock.assert_called_once_with(spark, df, expected_silver)


def test_main_no_truncate_when_run_mode_not_test(monkeypatch):
    monkeypatch.setattr(p2s.settings, "SPARK_CATALOG_NAME", "lakehouse", raising=True)
    monkeypatch.setattr(p2s.settings, "SILVER_SCHEMA", "silver", raising=True)
    monkeypatch.setattr(p2s.settings, "BRONZE_SCHEMA", "bronze", raising=True)
    monkeypatch.setattr(p2s.settings, "RUN_MODE", "prod", raising=True)

    spark = MagicMock()
    spark.sql = MagicMock()

    monkeypatch.setattr(p2s, "get_spark_session", lambda app: spark, raising=True)

    df = MagicMock()
    df.count.return_value = 1
    monkeypatch.setattr(p2s, "transform_raw_data", lambda *_: df, raising=True)
    monkeypatch.setattr(p2s, "create_table_if_not_exists", MagicMock(), raising=True)
    monkeypatch.setattr(p2s, "upsert_to_silver", MagicMock(), raising=True)

    p2s.main()

    calls_sql = " ".join(c[0][0] for c in spark.sql.call_args_list)
    assert "TRUNCATE TABLE lakehouse.silver.papers" not in calls_sql
