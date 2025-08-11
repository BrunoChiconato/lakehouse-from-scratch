import sys
from unittest.mock import MagicMock

import ingestion.main as ingestion_main


def _make_atom_feed(entries_xml: str) -> bytes:
    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
        {entries_xml}
    </feed>
    """
    return feed.encode("utf-8")


def _make_single_entry(
    entry_id="http://arxiv.org/abs/1234.5678v1",
    title="Cool Paper",
    summary="A nice summary.",
    author="Jane Doe",
    category="cs.AI",
    published="2023-01-01T00:00:00Z",
    updated="2023-01-02T00:00:00Z",
    pdf="http://arxiv.org/pdf/1234.5678v1.pdf",
) -> str:
    return f"""
    <entry>
      <id>{entry_id}</id>
      <title>{title}</title>
      <summary>{summary}</summary>
      <author><name>{author}</name></author>
      <category term="{category}" />
      <published>{published}</published>
      <updated>{updated}</updated>
      <link title="pdf" href="{pdf}" />
    </entry>
    """


def test_create_bronze_table_if_needed_emits_expected_sql(monkeypatch):
    spark = MagicMock()
    spark.sql = MagicMock()

    monkeypatch.setattr(
        ingestion_main.settings, "SPARK_CATALOG_NAME", "test_catalog", raising=False
    )
    monkeypatch.setattr(
        ingestion_main.settings, "BRONZE_SCHEMA", "bronze_test", raising=False
    )

    table_fqn = "test_catalog.bronze_test.arxiv_raw"

    ingestion_main.create_bronze_table_if_needed(spark, table_fqn)

    assert spark.sql.call_count == 3

    sql_0 = spark.sql.call_args_list[0].args[0]
    sql_1 = spark.sql.call_args_list[1].args[0]
    sql_2 = spark.sql.call_args_list[2].args[0]

    assert "USE CATALOG test_catalog" in sql_0
    assert "CREATE SCHEMA IF NOT EXISTS test_catalog.bronze_test" in sql_1
    assert f"CREATE TABLE IF NOT EXISTS {table_fqn}" in sql_2
    assert "USING delta" in sql_2


def test_run_ingestion_happy_path_merges_into_bronze(monkeypatch, caplog):
    first_batch = _make_atom_feed(_make_single_entry())
    empty_batch = _make_atom_feed("")

    class FakeAPI:
        def __init__(self, *_, **__):
            self._calls = 0

        def fetch_batch(self, category, start, batch_size):
            self._calls += 1
            return first_batch if self._calls == 1 else empty_batch

    monkeypatch.setattr(ingestion_main, "ArxivAPIClient", FakeAPI)

    monkeypatch.setattr(
        ingestion_main.settings, "CATEGORIES_TO_FETCH", ["cs.AI"], raising=False
    )

    spark = MagicMock()
    spark.sql = MagicMock()
    df_spark = MagicMock()
    df_spark.count.return_value = 1
    df_spark.createOrReplaceTempView = MagicMock()
    spark.createDataFrame.return_value = df_spark

    monkeypatch.setattr(ingestion_main, "get_spark_session", lambda app: spark)

    monkeypatch.setattr(ingestion_main.time, "sleep", lambda *_: None)

    caplog.set_level("INFO")
    ingestion_main.run_ingestion(
        target_size_bytes=10**9,
        batch_size=100,
        api_sleep_seconds=0,
    )

    df_spark.createOrReplaceTempView.assert_called_once_with("arxiv_raw_batch")

    merge_calls = [
        c.args[0]
        for c in spark.sql.call_args_list
        if isinstance(c.args[0], str) and "MERGE INTO" in c.args[0]
    ]
    assert merge_calls, (
        "Esperava encontrar um MERGE INTO na lista de chamadas a spark.sql"
    )
    assert any("USING arxiv_raw_batch AS s" in m for m in merge_calls)


def test_run_ingestion_when_no_articles_exits_early(monkeypatch, caplog):
    empty_batch = _make_atom_feed("")

    class FakeAPI:
        def fetch_batch(self, *_, **__):
            return empty_batch

    monkeypatch.setattr(ingestion_main, "ArxivAPIClient", FakeAPI)
    monkeypatch.setattr(
        ingestion_main.settings, "CATEGORIES_TO_FETCH", ["cs.AI"], raising=False
    )
    get_spark_spy = MagicMock()
    monkeypatch.setattr(ingestion_main, "get_spark_session", get_spark_spy)

    caplog.set_level("INFO")
    ingestion_main.run_ingestion(
        target_size_bytes=1024, batch_size=5, api_sleep_seconds=0
    )

    assert "No data was ingested." in caplog.text
    get_spark_spy.assert_not_called()


def test_cli_parses_args_and_calls_run_ingestion(monkeypatch):
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--target-size-mb",
            "2",
            "--batch-size",
            "123",
            "--api-sleep-seconds",
            "1",
        ],
    )

    run_spy = MagicMock()
    monkeypatch.setattr(ingestion_main, "run_ingestion", run_spy)

    ingestion_main.cli()

    run_spy.assert_called_once()
    kwargs = run_spy.call_args.kwargs
    assert kwargs["target_size_bytes"] == 2 * 1024 * 1024
    assert kwargs["batch_size"] == 123
    assert kwargs["api_sleep_seconds"] == 1
