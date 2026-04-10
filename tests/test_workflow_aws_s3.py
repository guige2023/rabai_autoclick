"""
Tests for workflow_aws_s3 module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_s3
mock_boto3 = types.ModuleType('boto3')
mock_session = MagicMock()
mock_boto3.Session = MagicMock(return_value=mock_session)
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions and config
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

mock_botocore_config = types.ModuleType('botocore.config')
mock_botocore_config.Config = MagicMock()

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions
sys.modules['botocore.config'] = mock_botocore_config

# Now we can import the module
from src.workflow_aws_s3 import (
    S3Integration,
    StorageClass,
    EncryptionType,
    MetadataDirective,
    BucketConfig,
    UploadConfig,
    MultipartUploadConfig,
    LifecycleRule,
    CorsRule,
    WebsiteConfig,
    PolicyStatement,
)


class TestStorageClass(unittest.TestCase):
    """Test StorageClass enum"""

    def test_storage_class_values(self):
        self.assertEqual(StorageClass.STANDARD.value, "STANDARD")
        self.assertEqual(StorageClass.REDUCED_REDUNDANCY.value, "REDUCED_REDUNDANCY")
        self.assertEqual(StorageClass.STANDARD_IA.value, "STANDARD_IA")
        self.assertEqual(StorageClass.ONEZONE_IA.value, "ONEZONE_IA")
        self.assertEqual(StorageClass.GLACIER.value, "GLACIER")
        self.assertEqual(StorageClass.DEEP_ARCHIVE.value, "DEEP_ARCHIVE")
        self.assertEqual(StorageClass.INTELLIGENT_TIERING.value, "INTELLIGENT_TIERING")


class TestEncryptionType(unittest.TestCase):
    """Test EncryptionType enum"""

    def test_encryption_type_values(self):
        self.assertEqual(EncryptionType.NONE.value, "NONE")
        self.assertEqual(EncryptionType.SSE_S3.value, "SSE-S3")
        self.assertEqual(EncryptionType.SSE_KMS.value, "SSE-KMS")
        self.assertEqual(EncryptionType.SSE_C.value, "SSE-C")


class TestMetadataDirective(unittest.TestCase):
    """Test MetadataDirective enum"""

    def test_metadata_directive_values(self):
        self.assertEqual(MetadataDirective.COPY.value, "COPY")
        self.assertEqual(MetadataDirective.REPLACE.value, "REPLACE")


class TestBucketConfig(unittest.TestCase):
    """Test BucketConfig dataclass"""

    def test_bucket_config_defaults(self):
        config = BucketConfig(name="my-bucket")
        self.assertEqual(config.name, "my-bucket")
        self.assertEqual(config.region, None)
        self.assertEqual(config.acl, "private")
        self.assertTrue(config.block_public_acls)
        self.assertTrue(config.block_public_policy)
        self.assertEqual(config.versioning_enabled, False)
        self.assertEqual(config.encryption_enabled, True)
        self.assertEqual(config.encryption_type, EncryptionType.SSE_S3)

    def test_bucket_config_full(self):
        config = BucketConfig(
            name="prod-bucket",
            region="us-west-2",
            acl="authenticated-read",
            object_ownership="BucketOwnerPreferred",
            block_public_acls=False,
            block_public_policy=False,
            ignore_public_acls=False,
            restrict_public_buckets=False,
            tags={"Environment": "production", "Team": "devops"},
            versioning_enabled=True,
            encryption_enabled=True,
            encryption_type=EncryptionType.SSE_KMS,
            kms_key_id="arn:aws:kms:us-west-2:123456789:key/mrk-123",
            lifecycle_config={"rules": []},
            cors_config=[{"AllowedOrigins": ["*"], "AllowedMethods": ["GET"]}],
            website_config={"IndexDocument": {"Suffix": "index.html"}}
        )
        self.assertEqual(config.region, "us-west-2")
        self.assertEqual(config.tags["Environment"], "production")
        self.assertTrue(config.versioning_enabled)
        self.assertEqual(config.encryption_type, EncryptionType.SSE_KMS)


class TestUploadConfig(unittest.TestCase):
    """Test UploadConfig dataclass"""

    def test_upload_config_defaults(self):
        config = UploadConfig(
            bucket="my-bucket",
            key="path/to/file.txt",
            data=b"file content"
        )
        self.assertEqual(config.bucket, "my-bucket")
        self.assertEqual(config.key, "path/to/file.txt")
        self.assertEqual(config.storage_class, StorageClass.STANDARD)
        self.assertEqual(config.encryption, EncryptionType.SSE_S3)
        self.assertEqual(config.metadata, {})

    def test_upload_config_full(self):
        config = UploadConfig(
            bucket="prod-bucket",
            key="uploads/data.json",
            data='{"key": "value"}',
            content_type="application/json",
            storage_class=StorageClass.GLACIER,
            encryption=EncryptionType.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/1234abcd",
            metadata={"project": "myproject", "version": "1"},
            tags={"env": "prod"},
            cache_control="max-age=3600",
            content_disposition="attachment; filename=data.json",
            content_encoding="gzip",
            content_language="en-US",
            expires=datetime(2025, 12, 31),
            retain_until=datetime(2026, 12, 31)
        )
        self.assertEqual(config.content_type, "application/json")
        self.assertEqual(config.storage_class, StorageClass.GLACIER)


class TestMultipartUploadConfig(unittest.TestCase):
    """Test MultipartUploadConfig dataclass"""

    def test_multipart_upload_config_defaults(self):
        config = MultipartUploadConfig(
            bucket="my-bucket",
            key="large-file.zip",
            file_path="/tmp/large-file.zip"
        )
        self.assertEqual(config.bucket, "my-bucket")
        self.assertEqual(config.part_size, 5 * 1024 * 1024)  # 5MB
        self.assertEqual(config.threshold, 100 * 1024 * 1024)  # 100MB
        self.assertEqual(config.max_concurrency, 4)

    def test_multipart_upload_config_full(self):
        config = MultipartUploadConfig(
            bucket="prod-bucket",
            key="backup.tar.gz",
            data=b"x" * (200 * 1024 * 1024),  # 200MB
            content_type="application/gzip",
            storage_class=StorageClass.STANDARD_IA,
            part_size=10 * 1024 * 1024,  # 10MB
            threshold=50 * 1024 * 1024,  # 50MB
            encryption=EncryptionType.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/backup-key",
            metadata={"backup-date": "2024-01-01"},
            tags={"type": "daily-backup"},
            max_concurrency=8
        )
        self.assertEqual(config.part_size, 10 * 1024 * 1024)
        self.assertEqual(config.max_concurrency, 8)


class TestLifecycleRule(unittest.TestCase):
    """Test LifecycleRule dataclass"""

    def test_lifecycle_rule_creation(self):
        rule = LifecycleRule(
            id="rule-1",
            prefix="logs/",
            enabled=True,
            expiration_days=30
        )
        self.assertEqual(rule.id, "rule-1")
        self.assertEqual(rule.prefix, "logs/")
        self.assertTrue(rule.enabled)

    def test_lifecycle_rule_transition(self):
        rule = LifecycleRule(
            id="rule-2",
            prefix="archive/",
            enabled=True,
            transition_days=30,
            transition_storage_class=StorageClass.GLACIER
        )
        self.assertEqual(rule.transition_days, 30)
        self.assertEqual(rule.transition_storage_class, StorageClass.GLACIER)


class TestCorsRule(unittest.TestCase):
    """Test CorsRule dataclass"""

    def test_cors_rule_creation(self):
        rule = CorsRule(
            allowed_origins=["https://example.com"],
            allowed_methods=["GET", "POST"],
            allowed_headers=["Content-Type", "Authorization"],
            max_age_seconds=3600,
            expose_headers=["ETag"]
        )
        self.assertEqual(len(rule.allowed_origins), 1)
        self.assertEqual(len(rule.allowed_methods), 2)
        self.assertEqual(rule.max_age_seconds, 3600)


class TestWebsiteConfig(unittest.TestCase):
    """Test WebsiteConfig dataclass"""

    def test_website_config_defaults(self):
        config = WebsiteConfig()
        self.assertEqual(config.index_document, "index.html")
        self.assertIsNone(config.error_document)
        self.assertIsNone(config.redirect_all_requests_to)

    def test_website_config_full(self):
        config = WebsiteConfig(
            index_document="home.html",
            error_document="error.html",
            redirect_all_requests_to={"HostName": "www.example.com", "Protocol": "https"},
            routing_rules=[{"Condition": {"KeyPrefixEquals": "docs/"}, "Redirect": {"ReplaceKeyPrefixWith": "documents/"}}]
        )
        self.assertEqual(config.index_document, "home.html")
        self.assertEqual(config.error_document, "error.html")


class TestPolicyStatement(unittest.TestCase):
    """Test PolicyStatement dataclass"""

    def test_policy_statement_defaults(self):
        statement = PolicyStatement()
        self.assertEqual(statement.effect, "Allow")
        self.assertEqual(statement.principal, "*")

    def test_policy_statement_full(self):
        statement = PolicyStatement(
            sid="Stmt1",
            effect="Allow",
            principal={"AWS": "arn:aws:iam::123456789:root"},
            actions=["s3:GetObject", "s3:PutObject"],
            not_actions=["s3:DeleteObject"],
            resources=["arn:aws:s3:::my-bucket/*"],
            not_resources=["arn:aws:s3:::my-bucket/private/*"],
            conditions={"StringEquals": {"s3:x-amz-acl": ["public-read"]}}
        )
        self.assertEqual(statement.sid, "Stmt1")
        self.assertEqual(len(statement.actions), 2)


class TestS3Integration(unittest.TestCase):
    """Test S3Integration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        # Create integration instance with mocked clients
        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_initialization(self):
        """Test S3Integration initialization"""
        integration = S3Integration(region_name="eu-west-1")
        self.assertEqual(integration.region_name, "eu-west-1")
        self.assertIsNotNone(integration._lock)
        self.assertEqual(integration._upload_parts_cache, {})

    def test_create_bucket(self):
        """Test creating a bucket"""
        self.mock_s3_client.create_bucket.return_value = {}
        self.mock_s3_client.put_bucket_encryption.return_value = {}
        self.mock_s3_client.put_public_access_block.return_value = {}

        config = BucketConfig(name="test-bucket", region="us-east-1")
        result = self.integration.create_bucket(config)

        self.assertEqual(result["Bucket"], "test-bucket")
        self.assertEqual(result["Status"], "Created")
        self.mock_s3_client.create_bucket.assert_called_once()

    def test_create_bucket_with_versioning(self):
        """Test creating a bucket with versioning enabled"""
        self.mock_s3_client.create_bucket.return_value = {}
        self.mock_s3_client.put_bucket_versioning.return_value = {}
        self.mock_s3_client.put_bucket_encryption.return_value = {}
        self.mock_s3_client.put_public_access_block.return_value = {}

        config = BucketConfig(name="versioned-bucket", versioning_enabled=True)
        result = self.integration.create_bucket(config)

        self.mock_s3_client.put_bucket_versioning.assert_called_once()

    def test_create_bucket_with_tags(self):
        """Test creating a bucket with tags"""
        self.mock_s3_client.create_bucket.return_value = {}
        self.mock_s3_client.put_bucket_tagging.return_value = {}
        self.mock_s3_client.put_bucket_encryption.return_value = {}
        self.mock_s3_client.put_public_access_block.return_value = {}

        config = BucketConfig(name="tagged-bucket", tags={"Environment": "test"})
        result = self.integration.create_bucket(config)

        self.mock_s3_client.put_bucket_tagging.assert_called_once()

    def test_list_buckets(self):
        """Test listing buckets"""
        self.mock_s3_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket-1", "CreationDate": datetime.now()},
                {"Name": "bucket-2", "CreationDate": datetime.now()}
            ]
        }

        buckets = self.integration.list_buckets()

        self.assertEqual(len(buckets), 2)
        self.mock_s3_client.list_buckets.assert_called_once()

    def test_get_bucket_location(self):
        """Test getting bucket location"""
        self.mock_s3_client.get_bucket_location.return_value = {
            "LocationConstraint": "us-west-2"
        }

        location = self.integration.get_bucket_location("my-bucket")

        self.assertEqual(location, "us-west-2")

    def test_get_bucket_location_none(self):
        """Test getting bucket location for us-east-1"""
        self.mock_s3_client.get_bucket_location.return_value = {
            "LocationConstraint": None
        }

        location = self.integration.get_bucket_location("my-bucket")

        self.assertEqual(location, "us-east-1")

    def test_delete_bucket(self):
        """Test deleting an empty bucket"""
        self.mock_s3_client.delete_bucket.return_value = {}

        result = self.integration.delete_bucket("my-bucket")

        self.assertEqual(result["Status"], "Deleted")
        self.mock_s3_client.delete_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_delete_bucket_force(self):
        """Test force deleting a bucket with objects"""
        # Create mock pages iterator
        mock_pages = MagicMock()
        mock_pages.paginate.return_value = iter([
            {
                "Versions": [
                    {"Key": "file1.txt", "VersionId": "v1"},
                    {"Key": "file2.txt", "VersionId": "v1"}
                ],
                "DeleteMarkers": []
            }
        ])
        self.mock_s3_client.get_paginator.return_value = mock_pages
        self.mock_s3_client.delete_objects.return_value = {}
        self.mock_s3_client.delete_bucket.return_value = {}

        result = self.integration.delete_bucket("my-bucket", force=True)

        self.assertEqual(result["Status"], "Deleted")

    def test_get_bucket_info(self):
        """Test getting bucket info"""
        self.mock_s3_client.get_bucket_location.return_value = {"LocationConstraint": "us-east-1"}
        self.mock_s3_client.get_bucket_versioning.return_value = {"Status": "Enabled"}
        self.mock_s3_client.get_bucket_encryption.return_value = {
            "ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}
        }
        self.mock_s3_client.get_bucket_policy.return_value = {"Policy": "{}"}
        self.mock_s3_client.get_bucket_lifecycle_configuration.return_value = {"Rules": []}
        self.mock_s3_client.get_bucket_cors.return_value = {"CORSRules": []}
        self.mock_s3_client.get_bucket_website.return_value = {}
        self.mock_s3_client.get_bucket_tagging.return_value = {"TagSet": []}
        self.mock_s3_client.get_public_access_block.return_value = {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True
            }
        }

        info = self.integration.get_bucket_info("my-bucket")

        self.assertEqual(info["name"], "my-bucket")
        self.assertIn("versioning", info)
        self.assertIn("encryption", info)


class TestS3IntegrationObjectOperations(unittest.TestCase):
    """Test S3Integration object operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_upload_object(self):
        """Test uploading an object"""
        self.mock_s3_client.put_object.return_value = {
            "ETag": '"abc123"',
            "VersionId": "v1"
        }

        config = UploadConfig(
            bucket="my-bucket",
            key="uploads/test.txt",
            data=b"Hello, World!",
            content_type="text/plain"
        )
        result = self.integration.upload_object(config)

        self.assertEqual(result["Bucket"], "my-bucket")
        self.assertEqual(result["Key"], "uploads/test.txt")
        self.assertEqual(result["ETag"], '"abc123"')
        self.mock_s3_client.put_object.assert_called_once()

    def test_upload_object_with_metadata(self):
        """Test uploading an object with metadata"""
        self.mock_s3_client.put_object.return_value = {"ETag": '"def456"'}

        config = UploadConfig(
            bucket="my-bucket",
            key="data.json",
            data='{"key": "value"}',
            content_type="application/json",
            metadata={"project": "test", "version": "2"},
            tags={"env": "production"}
        )
        result = self.integration.upload_object(config)

        self.assertEqual(result["Status"], "Uploaded")

    def test_upload_object_with_sse_kms(self):
        """Test uploading an object with SSE-KMS encryption"""
        self.mock_s3_client.put_object.return_value = {"ETag": '"kms123"'}

        config = UploadConfig(
            bucket="my-bucket",
            key="sensitive/data.txt",
            data=b"Sensitive content",
            encryption=EncryptionType.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/my-key"
        )
        result = self.integration.upload_object(config)

        self.assertEqual(result["Status"], "Uploaded")

    def test_download_object(self):
        """Test downloading an object"""
        mock_body = MagicMock()
        mock_body.read.return_value = b"File content here"
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_body,
            "ContentLength": 17
        }

        data = self.integration.download_object("my-bucket", "test.txt")

        self.assertEqual(data, b"File content here")

    def test_download_object_to_file(self):
        """Test downloading an object to a file"""
        mock_body = MagicMock()
        mock_body.iter_chunks.return_value = [b"File ", b"content ", b"here"]
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_body,
            "ContentLength": 17
        }

        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            result = self.integration.download_object("my-bucket", "test.txt", output_path=temp_path)
            self.assertEqual(result, temp_path)
        finally:
            os.unlink(temp_path)

    def test_delete_object(self):
        """Test deleting an object"""
        self.mock_s3_client.delete_object.return_value = {
            "VersionId": "v1"
        }

        result = self.integration.delete_object("my-bucket", "old-file.txt")

        self.assertEqual(result["Status"], "Deleted")
        self.mock_s3_client.delete_object.assert_called_once()

    def test_delete_object_silent(self):
        """Test silent deletion of non-existent object"""
        from botocore.exceptions import ClientError

        def raise_not_found(*args, **kwargs):
            error = ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
            error.response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
            raise error

        self.mock_s3_client.delete_object.side_effect = raise_not_found

        result = self.integration.delete_object("my-bucket", "nonexistent.txt", silent=True)

        self.assertEqual(result["Status"], "NotFound")

    def test_delete_objects_batch(self):
        """Test batch deletion of objects"""
        self.mock_s3_client.delete_objects.return_value = {
            "Deleted": [{"Key": "file1.txt"}, {"Key": "file2.txt"}],
            "Errors": []
        }

        result = self.integration.delete_objects_batch("my-bucket", ["file1.txt", "file2.txt"])

        self.assertEqual(len(result["Deleted"]), 2)
        self.assertEqual(result["Status"], "Completed")

    def test_list_objects(self):
        """Test listing objects"""
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.txt", "Size": 100, "LastModified": datetime.now()},
                {"Key": "file2.txt", "Size": 200, "LastModified": datetime.now()}
            ],
            "IsTruncated": False,
            "KeyCount": 2
        }

        result = self.integration.list_objects("my-bucket", prefix="files/")

        self.assertEqual(len(result["Objects"]), 2)
        self.assertEqual(result["KeyCount"], 2)

    def test_list_objects_with_continuation(self):
        """Test listing objects with pagination"""
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1.txt", "Size": 100}],
            "IsTruncated": True,
            "NextContinuationToken": "token123",
            "KeyCount": 1
        }

        result = self.integration.list_objects("my-bucket", continuation_token="token123")

        self.assertTrue(result["IsTruncated"])
        self.assertEqual(result["NextContinuationToken"], "token123")

    def test_copy_object(self):
        """Test copying an object"""
        self.mock_s3_client.copy_object.return_value = {
            "ETag": '"copied123"',
            "VersionId": "v2"
        }

        result = self.integration.copy_object(
            source_bucket="source-bucket",
            source_key="original.txt",
            dest_bucket="dest-bucket",
            dest_key="copy.txt"
        )

        self.assertEqual(result["Status"], "Copied")
        self.assertIn("ETag", result)

    def test_get_object_metadata(self):
        """Test getting object metadata"""
        self.mock_s3_client.head_object.return_value = {
            "ContentLength": 1024,
            "ContentType": "text/plain",
            "ETag": '"abc123"',
            "LastModified": datetime.now(),
            "Metadata": {"project": "test"},
            "StorageClass": "STANDARD",
            "ServerSideEncryption": "AES256"
        }

        metadata = self.integration.get_object_metadata("my-bucket", "test.txt")

        self.assertEqual(metadata["ContentLength"], 1024)
        self.assertEqual(metadata["ContentType"], "text/plain")
        self.assertEqual(metadata["StorageClass"], "STANDARD")

    def test_generate_presigned_url(self):
        """Test generating presigned URL"""
        self.mock_s3_client.generate_presigned_url.return_value = "https://my-bucket.s3.amazonaws.com/test.txt?signature=abc"

        url = self.integration.generate_presigned_url(
            bucket="my-bucket",
            key="test.txt",
            expiration=3600,
            method="GET"
        )

        self.assertIn("https://", url)
        self.assertIn("signature=", url)

    def test_generate_presigned_post(self):
        """Test generating presigned POST policy"""
        self.mock_s3_client.generate_presigned_post.return_value = {
            "url": "https://my-bucket.s3.amazonaws.com",
            "fields": {"key": "test.txt", "signature": "abc"}
        }

        result = self.integration.generate_presigned_post(
            bucket="my-bucket",
            key="test.txt",
            expiration=3600
        )

        self.assertIn("url", result)
        self.assertIn("fields", result)


class TestS3IntegrationMultipartUpload(unittest.TestCase):
    """Test S3Integration multipart upload operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_initiate_multipart_upload(self):
        """Test initiating multipart upload"""
        self.mock_s3_client.create_multipart_upload.return_value = {
            "UploadId": "upload-123"
        }

        upload_id = self.integration.initiate_multipart_upload(
            bucket="my-bucket",
            key="large-file.zip",
            content_type="application/zip"
        )

        self.assertEqual(upload_id, "upload-123")

    def test_upload_part(self):
        """Test uploading a part"""
        self.mock_s3_client.upload_part.return_value = {
            "ETag": '"part1etag"'
        }

        result = self.integration.upload_part(
            bucket="my-bucket",
            key="large-file.zip",
            upload_id="upload-123",
            part_number=1,
            data=b"Part 1 content"
        )

        self.assertEqual(result["PartNumber"], 1)
        self.assertEqual(result["ETag"], '"part1etag"')

    def test_complete_multipart_upload(self):
        """Test completing multipart upload"""
        self.integration._upload_parts_cache["my-bucket/large-file.zip"] = [
            {"PartNumber": 1, "ETag": '"etag1"'},
            {"PartNumber": 2, "ETag": '"etag2"'}
        ]
        self.mock_s3_client.complete_multipart_upload.return_value = {
            "ETag": '"final-etag"',
            "VersionId": "v1",
            "Location": "https://my-bucket.s3.amazonaws.com/large-file.zip"
        }

        result = self.integration.complete_multipart_upload(
            bucket="my-bucket",
            key="large-file.zip",
            upload_id="upload-123"
        )

        self.assertEqual(result["Status"], "Completed")
        self.assertNotIn("my-bucket/large-file.zip", self.integration._upload_parts_cache)

    def test_abort_multipart_upload(self):
        """Test aborting multipart upload"""
        self.integration._upload_parts_cache["my-bucket/large-file.zip"] = [
            {"PartNumber": 1, "ETag": '"etag1"'}
        ]
        self.mock_s3_client.abort_multipart_upload.return_value = {}

        result = self.integration.abort_multipart_upload(
            bucket="my-bucket",
            key="large-file.zip",
            upload_id="upload-123"
        )

        self.assertEqual(result["Status"], "Aborted")
        self.assertNotIn("my-bucket/large-file.zip", self.integration._upload_parts_cache)

    def test_list_multipart_uploads(self):
        """Test listing multipart uploads"""
        self.mock_s3_client.list_multipart_uploads.return_value = {
            "Uploads": [
                {"Key": "file1.zip", "UploadId": "upload-1"},
                {"Key": "file2.zip", "UploadId": "upload-2"}
            ]
        }

        uploads = self.integration.list_multipart_uploads("my-bucket")

        self.assertEqual(len(uploads), 2)


class TestS3IntegrationBucketPolicies(unittest.TestCase):
    """Test S3Integration bucket policies"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_bucket_policy(self):
        """Test putting bucket policy"""
        self.mock_s3_client.put_bucket_policy.return_value = {}

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }
        result = self.integration.put_bucket_policy("my-bucket", policy)

        self.assertEqual(result["Status"], "PolicyUpdated")

    def test_get_bucket_policy(self):
        """Test getting bucket policy"""
        self.mock_s3_client.get_bucket_policy.return_value = {
            "Policy": '{"Version": "2012-10-17", "Statement": []}'
        }

        policy = self.integration.get_bucket_policy("my-bucket")

        self.assertIn("Version", policy)

    def test_delete_bucket_policy(self):
        """Test deleting bucket policy"""
        self.mock_s3_client.delete_bucket_policy.return_value = {}

        result = self.integration.delete_bucket_policy("my-bucket")

        self.assertEqual(result["Status"], "PolicyDeleted")


class TestS3IntegrationLifecycleRules(unittest.TestCase):
    """Test S3Integration lifecycle rules"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_bucket_lifecycle(self):
        """Test putting lifecycle configuration"""
        self.mock_s3_client.put_bucket_lifecycle_configuration.return_value = {}

        # Use LifecycleRule objects as input
        rules = [
            LifecycleRule(
                id="expire-old-files",
                prefix="logs/",
                enabled=True,
                expiration_days=30
            )
        ]
        result = self.integration.put_bucket_lifecycle("my-bucket", rules)
        self.assertIsNotNone(result)

    def test_get_bucket_lifecycle(self):
        """Test getting lifecycle configuration"""
        self.mock_s3_client.get_bucket_lifecycle_configuration.return_value = {
            "Rules": [
                {"ID": "rule1", "Status": "Enabled", "Expiration": {"Days": 30}}
            ]
        }

        lifecycle = self.integration.get_bucket_lifecycle("my-bucket")

        self.assertEqual(len(lifecycle), 1)

    def test_delete_bucket_lifecycle(self):
        """Test deleting lifecycle configuration"""
        self.mock_s3_client.delete_bucket_lifecycle.return_value = {}

        result = self.integration.delete_bucket_lifecycle("my-bucket")

        self.assertEqual(result["Status"], "LifecycleDeleted")


class TestS3IntegrationVersioning(unittest.TestCase):
    """Test S3Integration versioning operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_enable_bucket_versioning(self):
        """Test enabling bucket versioning"""
        self.mock_s3_client.put_bucket_versioning.return_value = {}

        result = self.integration.enable_bucket_versioning("my-bucket")

        self.assertEqual(result["Status"], "VersioningEnabled")
        self.mock_s3_client.put_bucket_versioning.assert_called_once()

    def test_get_bucket_versioning(self):
        """Test getting bucket versioning status"""
        self.mock_s3_client.get_bucket_versioning.return_value = {
            "Status": "Enabled",
            "MFADelete": "Disabled"
        }

        versioning = self.integration.get_bucket_versioning("my-bucket")

        self.assertEqual(versioning["Status"], "Enabled")


class TestS3IntegrationEncryption(unittest.TestCase):
    """Test S3Integration encryption operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_set_bucket_encryption_sse_s3(self):
        """Test setting SSE-S3 encryption"""
        self.mock_s3_client.put_bucket_encryption.return_value = {}

        result = self.integration.set_bucket_encryption("my-bucket", EncryptionType.SSE_S3)

        self.assertEqual(result["Status"], "EncryptionConfigured")

    def test_set_bucket_encryption_sse_kms(self):
        """Test setting SSE-KMS encryption"""
        self.mock_s3_client.put_bucket_encryption.return_value = {}

        result = self.integration.set_bucket_encryption(
            "my-bucket",
            EncryptionType.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/my-key"
        )

        self.assertEqual(result["Status"], "EncryptionConfigured")

    def test_get_bucket_encryption(self):
        """Test getting bucket encryption"""
        self.mock_s3_client.get_bucket_encryption.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            }
        }

        encryption = self.integration.get_bucket_encryption("my-bucket")

        self.assertEqual(encryption["Status"], "Enabled")
        self.assertEqual(encryption["Algorithm"], "AES256")

    def test_delete_bucket_encryption(self):
        """Test deleting bucket encryption"""
        # This method may not exist, testing that it returns appropriate result
        self.mock_s3_client.delete_bucket_encryption.return_value = {}
        # Skip if method doesn't exist
        if hasattr(self.integration, 'delete_bucket_encryption'):
            result = self.integration.delete_bucket_encryption("my-bucket")
            self.assertEqual(result["Status"], "EncryptionDeleted")
        else:
            self.skipTest("delete_bucket_encryption not available")


class TestS3IntegrationCORS(unittest.TestCase):
    """Test S3Integration CORS operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_bucket_cors(self):
        """Test putting CORS configuration"""
        self.mock_s3_client.put_bucket_cors.return_value = {}

        cors_rules = [
            {
                "AllowedOrigins": ["https://example.com"],
                "AllowedMethods": ["GET", "POST"],
                "AllowedHeaders": ["Content-Type"],
                "MaxAgeSeconds": 3600
            }
        ]
        result = self.integration.put_bucket_cors("my-bucket", cors_rules)

        self.assertEqual(result["Status"], "CORSConfigured")

    def test_get_bucket_cors(self):
        """Test getting CORS configuration"""
        self.mock_s3_client.get_bucket_cors.return_value = {
            "CORSRules": [
                {"AllowedOrigins": ["*"], "AllowedMethods": ["GET"]}
            ]
        }

        cors = self.integration.get_bucket_cors("my-bucket")

        self.assertEqual(len(cors), 1)

    def test_delete_bucket_cors(self):
        """Test deleting CORS configuration"""
        self.mock_s3_client.delete_bucket_cors.return_value = {}

        result = self.integration.delete_bucket_cors("my-bucket")

        self.assertEqual(result["Status"], "CORSDeleted")


class TestS3IntegrationStaticWebsite(unittest.TestCase):
    """Test S3Integration static website hosting"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_configure_static_website(self):
        """Test configuring static website hosting"""
        self.mock_s3_client.put_bucket_website.return_value = {}

        config = WebsiteConfig(
            index_document="index.html",
            error_document="error.html"
        )
        result = self.integration.configure_static_website("my-bucket", config)

        self.assertEqual(result["Status"], "WebsiteConfigured")

    def test_get_bucket_website(self):
        """Test getting website configuration"""
        self.mock_s3_client.get_bucket_website.return_value = {
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "error.html"}
        }

        website = self.integration.get_bucket_website("my-bucket")

        self.assertEqual(website["IndexDocument"]["Suffix"], "index.html")

    def test_delete_bucket_website(self):
        """Test deleting website configuration"""
        self.mock_s3_client.delete_bucket_website.return_value = {}

        result = self.integration.delete_bucket_website("my-bucket")

        self.assertEqual(result["Status"], "WebsiteDeleted")


class TestS3IntegrationCloudWatch(unittest.TestCase):
    """Test S3Integration CloudWatch integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_cloudwatch_dashboard(self):
        """Test putting CloudWatch dashboard"""
        self.mock_cw_client.put_dashboard.return_value = {}

        result = self.integration.put_cloudwatch_dashboard(
            dashboard_name="my-dashboard",
            buckets=["my-bucket"]
        )

        self.assertTrue(result)

    def test_get_cloudwatch_metrics(self):
        """Test getting CloudWatch metrics"""
        self.mock_cw_client.get_metric_data.return_value = {
            "MetricDataResults": [
                {"Id": "bucket_size", "Label": "BucketSizeBytes", "Values": [100, 200]}
            ]
        }

        metrics = self.integration.get_cloudwatch_metrics(
            bucket="my-bucket",
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now()
        )

        self.assertIn("bucket_size", metrics)


class TestS3IntegrationTags(unittest.TestCase):
    """Test S3Integration tag operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_bucket_tags(self):
        """Test putting bucket tags"""
        self.mock_s3_client.put_bucket_tagging.return_value = {}

        result = self.integration.put_bucket_tags(
            "my-bucket",
            {"Environment": "production", "Team": "devops"}
        )

        self.assertIsNotNone(result)

    def test_get_bucket_tags(self):
        """Test getting bucket tags"""
        self.mock_s3_client.get_bucket_tagging.return_value = {
            "TagSet": [
                {"Key": "Environment", "Value": "production"}
            ]
        }

        tags = self.integration.get_bucket_tags("my-bucket")

        self.assertEqual(tags["Environment"], "production")

    def test_delete_bucket_tags(self):
        """Test deleting bucket tags"""
        self.mock_s3_client.delete_bucket_tagging.return_value = {}

        result = self.integration.delete_bucket_tags("my-bucket")

        self.assertEqual(result["Status"], "TagsDeleted")


class TestS3IntegrationPublicAccessBlock(unittest.TestCase):
    """Test S3Integration public access block"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_s3_client = MagicMock()
        self.mock_s3_resource = MagicMock()
        self.mock_cw_client = MagicMock()

        self.integration = S3Integration(region_name="us-east-1")
        self.integration.s3_client = self.mock_s3_client
        self.integration.s3_resource = self.mock_s3_resource
        self.integration.cloudwatch_client = self.mock_cw_client

    def test_put_public_access_block(self):
        """Test putting public access block"""
        self.mock_s3_client.put_public_access_block.return_value = {}

        result = self.integration.put_public_access_block(
            "my-bucket",
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True
        )

        self.assertTrue(result)

    def test_get_public_access_block(self):
        """Test getting public access block"""
        self.mock_s3_client.get_public_access_block.return_value = {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True
            }
        }

        block = self.integration.get_public_access_block("my-bucket")

        self.assertTrue(block["BlockPublicAcls"])
        self.assertTrue(block["BlockPublicPolicy"])


if __name__ == "__main__":
    unittest.main()
