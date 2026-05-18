"""
Object-store client (S3 / MinIO).

Files are stored at:
  s3://<bucket>/<dataset_id>/<YYYY-MM-DDTHH-MM-SS>_<8-char-uuid>.parquet

This naming scheme:
  - Groups files by dataset under a folder per dataset_id
  - Sorts chronologically in S3/MinIO browser views
  - Avoids collisions between concurrent runs (uuid suffix)
"""

import logging
import uuid
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class StorageClient:
    def __init__(self, config: dict) -> None:
        """
        Expected config keys:
          endpoint_url  – full URL, e.g. http://minio:9000
          access_key
          secret_key
          bucket        – bucket must already exist (created by infra stack)
          region        – optional, default 'us-east-1'
          use_ssl       – optional bool, default False when endpoint_url is http
        """
        self._bucket = config["bucket"]

        self._client = boto3.client(
            "s3",
            endpoint_url=config["endpoint_url"],
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config.get("region", "us-east-1"),
            config=Config(signature_version="s3v4"),
        )
        logger.info(
            "Storage client initialised → %s / bucket=%s",
            config["endpoint_url"], self._bucket,
        )

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload_parquet(self, dataset_id: str, data: bytes) -> str:
        """
        Upload *data* (Parquet bytes) to the object store.

        Returns the object key of the uploaded file.
        """
        key = self._make_key(dataset_id)
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentLength=len(data),
                ContentType="application/octet-stream",
            )
            logger.info(
                "Uploaded %d bytes → s3://%s/%s", len(data), self._bucket, key
            )
        except ClientError as exc:
            logger.error("Upload failed for key '%s': %s", key, exc)
            raise
        return key

    # ------------------------------------------------------------------
    # Bucket management
    # ------------------------------------------------------------------

    def ensure_bucket(self) -> None:
        """Create the target bucket if it does not exist."""
        try:
            self._client.head_bucket(Bucket=self._bucket)
            logger.debug("Bucket '%s' already exists.", self._bucket)
        except ClientError as exc:
            error_code = int(exc.response["Error"]["Code"])
            if error_code == 404:
                self._client.create_bucket(Bucket=self._bucket)
                logger.info("Created bucket '%s'.", self._bucket)
            else:
                raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(dataset_id: str) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        uid = uuid.uuid4().hex[:8]
        return f"{dataset_id}/{ts}_{uid}.parquet"
