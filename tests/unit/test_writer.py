import pytest
import pandas as pd
from unittest.mock import MagicMock

from ingestion.writer import S3Writer


@pytest.fixture
def mock_boto3_client(mocker):
    """
    Fixture to mock the `boto3.client` function.

    This fixture patches the function and returns the mock object representing
    the function itself. The function is also configured to return another
    mock object, which represents the S3 client instance.
    """
    mock_factory = mocker.patch("boto3.client")

    mock_s3_instance = MagicMock()

    mock_factory.return_value = mock_s3_instance

    return mock_factory


def test_s3writer_initialization_success(mock_boto3_client):
    """
    Unit Test: Verifies that S3Writer initializes correctly by calling boto3.client.
    """
    writer = S3Writer(bucket_name="test-bucket")

    mock_boto3_client.assert_called_once_with("s3")
    assert writer.bucket_name == "test-bucket"


def test_s3writer_initialization_raises_error_if_no_bucket():
    """
    Unit Test: Verifies that S3Writer raises a ValueError if the bucket name is missing.
    (Este teste não precisa de mock e permanece o mesmo)
    """
    with pytest.raises(ValueError) as excinfo:
        S3Writer(bucket_name=None)

    assert "S3 bucket name must be provided" in str(excinfo.value)


def test_write_parquet_success(mock_boto3_client):
    """
    Unit Test: Verifies that write_parquet calls the S3 client's put_object correctly.
    """
    writer = S3Writer(bucket_name="test-bucket")

    test_data = {"id": [1, 2], "title": ["Test A", "Test B"]}
    test_df = pd.DataFrame(test_data)

    test_key = "bronze/test.parquet"

    writer.write_parquet(df=test_df, key=test_key)

    mock_s3_instance = mock_boto3_client.return_value

    mock_s3_instance.put_object.assert_called_once()

    call_args, call_kwargs = mock_s3_instance.put_object.call_args

    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == test_key
    assert isinstance(call_kwargs["Body"], bytes)
    assert len(call_kwargs["Body"]) > 0
