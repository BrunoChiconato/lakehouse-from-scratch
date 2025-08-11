from unittest.mock import MagicMock

import utils.spark_utils as spark_utils


def test_get_spark_session_returns_active_session(monkeypatch):
    """If an active Spark session exists, it should be reused instead of building a new one."""
    active = MagicMock(name="ActiveSparkSession")

    monkeypatch.setattr(
        spark_utils.SparkSession,
        "getActiveSession",
        staticmethod(lambda: active),
    )

    fake_builder = MagicMock(name="Builder")

    def _fail(*args, **kwargs):
        raise AssertionError("Builder should not be called when a session is active")

    fake_builder.appName.side_effect = _fail
    fake_builder.getOrCreate.side_effect = _fail
    monkeypatch.setattr(
        spark_utils.SparkSession,
        "builder",
        fake_builder,
        raising=False,
    )

    result = spark_utils.get_spark_session("ShouldNotMatter")
    assert result is active


def test_get_spark_session_builds_when_no_active(monkeypatch):
    """If an active Spark session exists, it should be reused instead of building a new one."""
    monkeypatch.setattr(
        spark_utils.SparkSession,
        "getActiveSession",
        staticmethod(lambda: None),
    )

    created = MagicMock(name="CreatedSparkSession")
    fake_builder = MagicMock(name="Builder")

    def _app_name(name):
        assert name == "MyApp"
        return fake_builder

    fake_builder.appName.side_effect = _app_name
    fake_builder.getOrCreate.return_value = created

    monkeypatch.setattr(
        spark_utils.SparkSession,
        "builder",
        fake_builder,
        raising=False,
    )

    result = spark_utils.get_spark_session("MyApp")
    assert result is created
    fake_builder.appName.assert_called_once()
    fake_builder.getOrCreate.assert_called_once()
