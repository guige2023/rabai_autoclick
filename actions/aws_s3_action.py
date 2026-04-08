"""AWS S3 integration for object storage operations.

Handles S3 operations including upload, download, copy,
bucket management, and presigned URLs.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError:
    boto3 = None
    ClientError = None
    BotoCoreError = Exception

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    """Configuration for AWS S3."""
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region_name: str = "us-east-1"
    endpoint_url: Optional[str] = None
    bucket: Optional[str] = None


@dataclass
class S3Object:
    """Represents an S3 object."""
    key: str
    size: int
    last_modified: datetime
    etag: str
    storage_class: str = "STANDARD"
    metadata: dict = field(default_factory=dict)


@dataclass
class S3UploadResult:
    """Result of an S3 upload."""
    success: bool
    key: str
    etag: Optional[str] = None
    version_id: Optional[str] = None
    error: Optional[str] = None


class S3APIError(Exception):
    """Raised when S3 operations fail."""
    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class S3Action:
    """AWS S3 client for object storage operations."""

    def __init__(self, config: S3Config):
        """Initialize S3 client with configuration.

        Args:
            config: S3Config with AWS credentials and settings

        Raises:
            ImportError: If boto3 is not installed
        """
        if boto3 is None:
            raise ImportError("boto3 required: pip install boto3")

        self.config = config
        self._client = None
        self._resource = None

    def _get_client(self):
        """Get or create S3 client."""
        if self._client is None:
            kwargs: dict[str, Any] = {
                "region_name": self.config.region_name
            }

            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url

            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            self._client = boto3.client("s3", **kwargs)

        return self._client

    def _get_resource(self):
        """Get or create S3 resource."""
        if self._resource is None:
            kwargs: dict[str, Any] = {
                "region_name": self.config.region_name
            }

            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url

            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            self._resource = boto3.resource("s3", **kwargs)

        return self._resource

    def upload_file(self, file_path: str, key: str,
                   bucket: Optional[str] = None,
                   extra_args: Optional[dict] = None) -> S3UploadResult:
        """Upload a file to S3.

        Args:
            file_path: Local file path
            key: S3 object key
            bucket: Target bucket (uses config default if None)
            extra_args: Extra arguments for upload (ACL, ContentType, etc.)

        Returns:
            S3UploadResult with upload status
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return S3UploadResult(success=False, key=key, error="No bucket specified")

        try:
            client = self._get_client()

            extra = extra_args or {}
            if "ContentType" not in extra:
                extra["ContentType"] = self._guess_content_type(file_path)

            client.upload_file(
                Filename=file_path,
                Bucket=bucket,
                Key=key,
                ExtraArgs=extra
            )

            return S3UploadResult(success=True, key=key)

        except (ClientError, BotoCoreError) as e:
            return S3UploadResult(success=False, key=key, error=str(e))

    def upload_bytes(self, data: bytes, key: str,
                   bucket: Optional[str] = None,
                   content_type: Optional[str] = None) -> S3UploadResult:
        """Upload bytes data to S3.

        Args:
            data: Bytes data to upload
            key: S3 object key
            bucket: Target bucket
            content_type: MIME type

        Returns:
            S3UploadResult with upload status
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return S3UploadResult(success=False, key=key, error="No bucket specified")

        try:
            client = self._get_client()

            extra: dict[str, Any] = {}
            if content_type:
                extra["ContentType"] = content_type

            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                **extra
            )

            return S3UploadResult(success=True, key=key)

        except (ClientError, BotoCoreError) as e:
            return S3UploadResult(success=False, key=key, error=str(e))

    def download_file(self, key: str, file_path: str,
                    bucket: Optional[str] = None) -> bool:
        """Download a file from S3.

        Args:
            key: S3 object key
            file_path: Local file path
            bucket: Source bucket

        Returns:
            True if successful
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return False

        try:
            client = self._get_client()
            client.download_file(Bucket=bucket, Key=key, Filename=file_path)
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Download failed: {e}")
            return False

    def download_bytes(self, key: str,
                     bucket: Optional[str] = None) -> Optional[bytes]:
        """Download object as bytes.

        Args:
            key: S3 object key
            bucket: Source bucket

        Returns:
            Object bytes or None if failed
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return None

        try:
            client = self._get_client()
            response = client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Download bytes failed: {e}")
            return None

    def copy_object(self, source_key: str, dest_key: str,
                  source_bucket: Optional[str] = None,
                  dest_bucket: Optional[str] = None) -> bool:
        """Copy an object within S3.

        Args:
            source_key: Source object key
            dest_key: Destination object key
            source_bucket: Source bucket
            dest_bucket: Destination bucket

        Returns:
            True if successful
        """
        src_bucket = source_bucket or self.config.bucket
        dst_bucket = dest_bucket or self.config.bucket or src_bucket

        if not src_bucket:
            return False

        try:
            client = self._get_client()
            copy_source = {"Bucket": src_bucket, "Key": source_key}

            client.copy(
                CopySource=copy_source,
                Bucket=dst_bucket,
                Key=dest_key
            )

            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Copy failed: {e}")
            return False

    def delete_object(self, key: str, bucket: Optional[str] = None) -> bool:
        """Delete an object from S3.

        Args:
            key: S3 object key
            bucket: Target bucket

        Returns:
            True if successful
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return False

        try:
            client = self._get_client()
            client.delete_object(Bucket=bucket, Key=key)
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Delete failed: {e}")
            return False

    def delete_objects(self, keys: list[str],
                      bucket: Optional[str] = None) -> int:
        """Delete multiple objects from S3.

        Args:
            keys: List of object keys
            bucket: Target bucket

        Returns:
            Number of deleted objects
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return 0

        try:
            client = self._get_client()

            objects = [{"Key": key} for key in keys]

            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects}
            )

            return len(response.get("Deleted", []))

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Batch delete failed: {e}")
            return 0

    def list_objects(self, prefix: str = "",
                    bucket: Optional[str] = None,
                    max_keys: int = 1000) -> list[S3Object]:
        """List objects in a bucket.

        Args:
            prefix: Key prefix filter
            bucket: Target bucket
            max_keys: Maximum number of keys to return

        Returns:
            List of S3Object
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return []

        try:
            client = self._get_client()
            response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )

            objects = []
            for obj in response.get("Contents", []):
                objects.append(S3Object(
                    key=obj["Key"],
                    size=obj["Size"],
                    last_modified=obj["LastModified"],
                    etag=obj["ETag"].strip('"'),
                    storage_class=obj.get("StorageClass", "STANDARD"),
                    metadata=obj.get("Metadata", {})
                ))

            return objects

        except (ClientError, BotoCoreError) as e:
            logger.error(f"List objects failed: {e}")
            return []

    def get_object(self, key: str,
                  bucket: Optional[str] = None) -> Optional[S3Object]:
        """Get metadata for an object.

        Args:
            key: S3 object key
            bucket: Target bucket

        Returns:
            S3Object or None if not found
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return None

        try:
            client = self._get_client()
            response = client.head_object(Bucket=bucket, Key=key)

            return S3Object(
                key=key,
                size=response["ContentLength"],
                last_modified=response["LastModified"],
                etag=response["ETag"].strip('"'),
                storage_class=response.get("StorageClass", "STANDARD"),
                metadata=response.get("Metadata", {})
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            logger.error(f"Get object failed: {e}")
            return None

    def generate_presigned_url(self, key: str,
                               bucket: Optional[str] = None,
                               expiration: int = 3600,
                               method: str = "GET") -> Optional[str]:
        """Generate a presigned URL for an object.

        Args:
            key: S3 object key
            bucket: Target bucket
            expiration: URL expiration time in seconds
            method: HTTP method (GET, PUT, DELETE)

        Returns:
            Presigned URL string or None if failed
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return None

        try:
            client = self._get_client()
            url = client.generate_presigned_url(
                ClientMethod=f"get_object" if method == "GET" else "put_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration
            )

            return url

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Generate presigned URL failed: {e}")
            return None

    def generate_presigned_post(self, key: str,
                              bucket: Optional[str] = None,
                              expiration: int = 3600) -> Optional[dict]:
        """Generate a presigned POST policy.

        Args:
            key: S3 object key
            bucket: Target bucket
            expiration: Policy expiration in seconds

        Returns:
            Dict with url and fields or None if failed
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return None

        try:
            client = self._get_client()
            response = client.generate_presigned_post(
                Bucket=bucket,
                Key=key,
                ExpiresIn=expiration
            )

            return response

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Generate presigned post failed: {e}")
            return None

    def create_bucket(self, bucket: Optional[str] = None,
                    region: Optional[str] = None) -> bool:
        """Create a new S3 bucket.

        Args:
            bucket: Bucket name
            region: AWS region

        Returns:
            True if created
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return False

        try:
            client = self._get_client()
            region = region or self.config.region_name

            if region == "us-east-1":
                client.create_bucket(Bucket=bucket)
            else:
                client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={
                        "LocationConstraint": region
                    }
                )

            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Create bucket failed: {e}")
            return False

    def delete_bucket(self, bucket: Optional[str] = None) -> bool:
        """Delete an S3 bucket.

        Args:
            bucket: Bucket name

        Returns:
            True if deleted
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return False

        try:
            client = self._get_client()
            client.delete_bucket(Bucket=bucket)
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Delete bucket failed: {e}")
            return False

    def bucket_exists(self, bucket: Optional[str] = None) -> bool:
        """Check if a bucket exists.

        Args:
            bucket: Bucket name

        Returns:
            True if exists
        """
        bucket = bucket or self.config.bucket

        if not bucket:
            return False

        try:
            client = self._get_client()
            client.head_bucket(Bucket=bucket)
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Bucket exists check failed: {e}")
            return False

    def _guess_content_type(self, file_path: str) -> str:
        """Guess content type from file extension."""
        import mimetypes

        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or "application/octet-stream"
