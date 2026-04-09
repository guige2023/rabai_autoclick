"""GCS action module for RabAI AutoClick.

Provides Google Cloud Storage operations:
- GCSUploader: Upload files to GCS buckets
- GCSDownloader: Download files from GCS buckets
- GCSBucketManager: Manage GCS buckets
- GCSObjectManager: Manage GCS objects
- GCSSignedURLGenerator: Generate signed URLs for GCS objects
"""

from __future__ import annotations

import json
import sys
import os
import base64
import hashlib
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    from google.cloud import storage
    from google.oauth2 import service_account
    GOOGLE_CLOUD_AVAILABLE = True
except ImportError:
    GOOGLE_CLOUD_AVAILABLE = False


class GCSUploaderAction(BaseAction):
    """Upload files to GCS buckets."""
    action_type = "gcs_uploader"
    display_name = "GCS上传"
    description = "上传文件到GCS存储桶"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not GOOGLE_CLOUD_AVAILABLE:
            return ActionResult(success=False, message="google-cloud-storage not installed: pip install google-cloud-storage")

        try:
            bucket_name = params.get("bucket_name", "")
            source_file = params.get("source_file", "")
            destination_blob = params.get("destination_blob", "")
            project_id = params.get("project_id", "")
            credentials_path = params.get("credentials_path", "")
            content_type = params.get("content_type", "application/octet-stream")
            cache_control = params.get("cache_control", None)
            metadata = params.get("metadata", {})

            if not bucket_name or not source_file:
                return ActionResult(success=False, message="bucket_name and source_file required")
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"File not found: {source_file}")

            client = self._get_client(project_id, credentials_path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob)

            blob.content_type = content_type
            if cache_control:
                blob.cache_control = cache_control

            generation_match_precondition = None
            if params.get("if_generation_match"):
                generation_match_precondition = params.get("if_generation_match")

            blob.upload_from_filename(source_file, if_generation_match=generation_match_precondition)

            file_size = os.path.getsize(source_file)
            gcs_uri = f"gs://{bucket_name}/{destination_blob}"

            return ActionResult(
                success=True,
                message=f"Uploaded to gs://{bucket_name}/{destination_blob}",
                data={"gcs_uri": gcs_uri, "bucket": bucket_name, "blob": destination_blob, "size": file_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Upload error: {str(e)}")

    def _get_client(self, project_id: str, credentials_path: str):
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return storage.Client(project=project_id, credentials=credentials)
        return storage.Client(project=project_id)


class GCSDownloaderAction(BaseAction):
    """Download files from GCS buckets."""
    action_type = "gcs_downloader"
    display_name = "GCS下载"
    description = "从GCS存储桶下载文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not GOOGLE_CLOUD_AVAILABLE:
            return ActionResult(success=False, message="google-cloud-storage not installed: pip install google-cloud-storage")

        try:
            bucket_name = params.get("bucket_name", "")
            source_blob = params.get("source_blob", "")
            destination_file = params.get("destination_file", "")
            project_id = params.get("project_id", "")
            credentials_path = params.get("credentials_path", "")

            if not bucket_name or not source_blob:
                return ActionResult(success=False, message="bucket_name and source_blob required")
            if not destination_file:
                destination_file = os.path.basename(source_blob)

            client = self._get_client(project_id, credentials_path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(source_blob)

            if not blob.exists():
                return ActionResult(success=False, message=f"Blob not found: gs://{bucket_name}/{source_blob}")

            os.makedirs(os.path.dirname(destination_file), exist_ok=True)
            blob.download_to_filename(destination_file)

            downloaded_size = os.path.getsize(destination_file)

            return ActionResult(
                success=True,
                message=f"Downloaded gs://{bucket_name}/{source_blob}",
                data={"destination_file": destination_file, "size": downloaded_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Download error: {str(e)}")

    def _get_client(self, project_id: str, credentials_path: str):
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return storage.Client(project=project_id, credentials=credentials)
        return storage.Client(project=project_id)


class GCSBucketManagerAction(BaseAction):
    """Manage GCS buckets."""
    action_type = "gcs_bucket_manager"
    display_name = "GCS Bucket管理"
    description = "管理GCS存储桶"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not GOOGLE_CLOUD_AVAILABLE:
            return ActionResult(success=False, message="google-cloud-storage not installed: pip install google-cloud-storage")

        try:
            operation = params.get("operation", "list")
            project_id = params.get("project_id", "")
            credentials_path = params.get("credentials_path", "")
            bucket_name = params.get("bucket_name", "")
            location = params.get("location", "US")
            storage_class = params.get("storage_class", "STANDARD")

            client = self._get_client(project_id, credentials_path)

            if operation == "list":
                buckets = list(client.list_buckets())
                return ActionResult(
                    success=True,
                    message=f"{len(buckets)} buckets",
                    data={"buckets": [{"name": b.name, "location": b.location, "storage_class": b.storage_class} for b in buckets]}
                )

            elif operation == "create":
                if not bucket_name:
                    return ActionResult(success=False, message="bucket_name required for create")

                bucket = client.bucket(bucket_name)
                bucket.location = location
                bucket.storage_class = storage_class
                bucket.create()

                return ActionResult(success=True, message=f"Created bucket: gs://{bucket_name}")

            elif operation == "delete":
                if not bucket_name:
                    return ActionResult(success=False, message="bucket_name required for delete")

                bucket = client.bucket(bucket_name)
                bucket.delete(force=params.get("force", False))

                return ActionResult(success=True, message=f"Deleted bucket: gs://{bucket_name}")

            elif operation == "get":
                if not bucket_name:
                    return ActionResult(success=False, message="bucket_name required for get")

                bucket = client.bucket(bucket_name)
                bucket.reload()

                return ActionResult(
                    success=True,
                    message=f"Bucket: gs://{bucket_name}",
                    data={
                        "name": bucket.name,
                        "location": bucket.location,
                        "storage_class": bucket.storage_class,
                        "versioning_enabled": bucket.versioning_enabled,
                        "acl": list(bucket.acl),
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _get_client(self, project_id: str, credentials_path: str):
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return storage.Client(project=project_id, credentials=credentials)
        return storage.Client(project=project_id)


class GCSObjectManagerAction(BaseAction):
    """Manage GCS objects."""
    action_type = "gcs_object_manager"
    display_name = "GCS对象管理"
    description = "管理GCS对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not GOOGLE_CLOUD_AVAILABLE:
            return ActionResult(success=False, message="google-cloud-storage not installed: pip install google-cloud-storage")

        try:
            operation = params.get("operation", "list")
            bucket_name = params.get("bucket_name", "")
            prefix = params.get("prefix", "")
            project_id = params.get("project_id", "")
            credentials_path = params.get("credentials_path", "")

            client = self._get_client(project_id, credentials_path)
            bucket = client.bucket(bucket_name)

            if operation == "list":
                blobs = list(bucket.list_blobs(prefix=prefix))
                return ActionResult(
                    success=True,
                    message=f"{len(blobs)} objects",
                    data={"objects": [{"name": b.name, "size": b.size, "updated": b.updated.isoformat()} for b in blobs]}
                )

            elif operation == "delete":
                blob_name = params.get("blob_name", "")
                if not blob_name:
                    return ActionResult(success=False, message="blob_name required for delete")

                blob = bucket.blob(blob_name)
                blob.delete()
                return ActionResult(success=True, message=f"Deleted: gs://{bucket_name}/{blob_name}")

            elif operation == "copy":
                source_blob = params.get("source_blob", "")
                dest_bucket = params.get("dest_bucket", "")
                dest_blob = params.get("dest_blob", "")

                if not source_blob or not dest_bucket or not dest_blob:
                    return ActionResult(success=False, message="source_blob, dest_bucket, dest_blob required")

                source = bucket.blob(source_blob)
                dest = client.bucket(dest_bucket).blob(dest_blob)
                dest.rewrite(source)

                return ActionResult(success=True, message=f"Copied to gs://{dest_bucket}/{dest_blob}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _get_client(self, project_id: str, credentials_path: str):
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return storage.Client(project=project_id, credentials=credentials)
        return storage.Client(project=project_id)


class GCSSignedURLGeneratorAction(BaseAction):
    """Generate signed URLs for GCS objects."""
    action_type = "gcs_signed_url"
    display_name = "GCS签名URL"
    description = "为GCS对象生成签名URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not GOOGLE_CLOUD_AVAILABLE:
            return ActionResult(success=False, message="google-cloud-storage not installed: pip install google-cloud-storage")

        try:
            bucket_name = params.get("bucket_name", "")
            blob_name = params.get("blob_name", "")
            project_id = params.get("project_id", "")
            credentials_path = params.get("credentials_path", "")
            expiration_minutes = params.get("expiration_minutes", 60)
            action = params.get("action", "READ")

            if not bucket_name or not blob_name:
                return ActionResult(success=False, message="bucket_name and blob_name required")

            client = self._get_client(project_id, credentials_path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=expiration_minutes),
                method="GET" if action == "READ" else "PUT",
            )

            return ActionResult(
                success=True,
                message=f"Generated signed URL (expires in {expiration_minutes} min)",
                data={"signed_url": url, "expires_in_minutes": expiration_minutes}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _get_client(self, project_id: str, credentials_path: str):
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return storage.Client(project=project_id, credentials=credentials)
        return storage.Client(project=project_id)
