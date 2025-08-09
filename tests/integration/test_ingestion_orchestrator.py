from unittest.mock import patch

from ingestion import main as ingestion_orchestrator
from config import settings


FAKE_XML_CONTENT = b"""
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.0001v1</id>
    <title>A Mock Paper</title>
    <summary>A summary of the mock paper.</summary>
    <author><name>Dr. Mock</name></author>
    <category term="cs.TEST"/>
    <published>2024-01-01T10:00:00Z</published>
    <updated>2024-01-01T10:00:00Z</updated>
    <link href="http://example.com/mock.pdf" title="pdf"/>
  </entry>
</feed>
"""
EMPTY_XML_CONTENT = b"<feed></feed>"

INVALID_CONTRACT_XML = b"""
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.9999v1</id>
    <title>A Paper with an Empty Author List</title>
    <summary>Summary.</summary>
    <category term="cs.FAIL"/>
    <published>2024-01-01T10:00:00Z</published>
    <updated>2024-01-01T10:00:00Z</updated>
    <link href="http://example.com/mock.pdf" title="pdf"/>
  </entry>
</feed>
"""


@patch("ingestion.main.S3Writer")
@patch("ingestion.main.ArxivAPIClient")
def test_run_ingestion_happy_path(MockAPIClient, MockS3Writer, mocker):
    """Verifies the successful orchestration flow, focusing on pagination."""
    mocker.patch.object(settings, "TARGET_SIZE_BYTES", 999_999_999)
    mocker.patch.object(settings, "CATEGORIES_TO_FETCH", ["cs.TEST"])

    mock_api_instance = MockAPIClient.return_value
    mock_s3_instance = MockS3Writer.return_value

    mock_api_instance.fetch_batch.side_effect = [FAKE_XML_CONTENT, EMPTY_XML_CONTENT]

    ingestion_orchestrator.run_ingestion()

    assert mock_api_instance.fetch_batch.call_count == 2
    mock_s3_instance.write_parquet.assert_called_once()


@patch("ingestion.main.S3Writer")
@patch("ingestion.main.ArxivAPIClient")
def test_run_ingestion_stops_when_target_size_is_reached(
    MockAPIClient, MockS3Writer, mocker
):
    """Verifies the loop breaks correctly when target size is reached."""
    mocker.patch.object(settings, "TARGET_SIZE_BYTES", 100)
    mocker.patch.object(settings, "CATEGORIES_TO_FETCH", ["cs.TEST"])

    mock_api_instance = MockAPIClient.return_value
    mock_s3_instance = MockS3Writer.return_value

    mock_api_instance.fetch_batch.side_effect = [FAKE_XML_CONTENT, EMPTY_XML_CONTENT]

    ingestion_orchestrator.run_ingestion()

    mock_api_instance.fetch_batch.assert_called_once()
    mock_s3_instance.write_parquet.assert_called_once()


@patch("ingestion.main.S3Writer")
@patch("ingestion.main.ArxivAPIClient")
def test_run_ingestion_handles_api_failure_gracefully(
    MockAPIClient, MockS3Writer, mocker
):
    """
    Verifies that the orchestrator survives an API error and continues.
    THIS TEST COVERS THE 'except Exception' BLOCK.
    """
    mocker.patch.object(settings, "TARGET_SIZE_BYTES", 999_999_999)
    mocker.patch.object(settings, "CATEGORIES_TO_FETCH", ["cs.FAIL", "cs.SUCCESS"])

    mock_api_instance = MockAPIClient.return_value
    mock_s3_instance = MockS3Writer.return_value

    mock_api_instance.fetch_batch.side_effect = [
        Exception("Simulated API failure"),
        FAKE_XML_CONTENT,
        EMPTY_XML_CONTENT,
    ]

    ingestion_orchestrator.run_ingestion()

    assert mock_api_instance.fetch_batch.call_count == 3
    mock_s3_instance.write_parquet.assert_called_once()


@patch("ingestion.main.S3Writer")
@patch("ingestion.main.ArxivAPIClient")
def test_run_ingestion_handles_validation_error_gracefully(
    MockAPIClient, MockS3Writer, mocker
):
    """Verifies an article failing Pydantic validation is skipped."""
    mocker.patch.object(settings, "TARGET_SIZE_BYTES", 999_999_999)
    mocker.patch.object(settings, "CATEGORIES_TO_FETCH", ["cs.TEST"])

    mock_api_instance = MockAPIClient.return_value
    mock_s3_instance = MockS3Writer.return_value

    mock_api_instance.fetch_batch.side_effect = [
        INVALID_CONTRACT_XML,
        EMPTY_XML_CONTENT,
    ]

    ingestion_orchestrator.run_ingestion()

    assert mock_api_instance.fetch_batch.call_count == 2
    mock_s3_instance.write_parquet.assert_not_called()


@patch("ingestion.main.S3Writer")
@patch("ingestion.main.ArxivAPIClient")
def test_run_ingestion_does_not_write_if_no_data_is_fetched(
    MockAPIClient, MockS3Writer, mocker
):
    """Verifies the writer is not called if no articles are fetched."""
    mocker.patch.object(settings, "CATEGORIES_TO_FETCH", ["cs.EMPTY"])

    mock_api_instance = MockAPIClient.return_value
    mock_s3_instance = MockS3Writer.return_value

    mock_api_instance.fetch_batch.return_value = EMPTY_XML_CONTENT

    ingestion_orchestrator.run_ingestion()

    mock_s3_instance.write_parquet.assert_not_called()
