import io
import logging
import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class S3Writer:
    """A client to write data to Amazon S3."""

    def __init__(self, bucket_name: str):
        if not bucket_name:
            raise ValueError("S3 bucket name must be provided.")
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name

    def write_parquet(self, df: pd.DataFrame, key: str):
        """
        Converts a pandas DataFrame to Parquet in-memory and uploads it to S3.
        """
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        final_size_mb = parquet_buffer.tell() / (1024 * 1024)

        logger.info(
            f"Final Parquet buffer size: {final_size_mb:.2f}MB. Uploading to S3..."
        )

        parquet_buffer.seek(0)
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=key, Body=parquet_buffer.getvalue()
        )
        logger.info(f"Upload complete to s3://{self.bucket_name}/{key}")
