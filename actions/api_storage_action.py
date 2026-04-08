"""API storage action module for RabAI AutoClick.

Provides cloud storage operations:
- StorageUploadAction: Upload to storage
- StorageDownloadAction: Download from storage
- StorageDeleteAction: Delete from storage
- StorageListAction: List storage objects
- StoragePresignAction: Generate presigned URL
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StorageUploadAction(BaseAction):
    """Upload data to cloud storage."""
    action_type = "storage_upload"
    display_name = "上传存储"
    description = "上传数据到云存储"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bucket = params.get("bucket", "")
            key = params.get("key", "")
            data = params.get("data", b"")
            content_type = params.get("content_type", "application/octet-stream")

            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key are required")

            if isinstance(data, str):
                data = data.encode("utf-8")

            object_id = hashlib.md5(f"{bucket}/{key}".encode()).hexdigest()[:12]
            size = len(data)

            if not hasattr(context, "storage_objects"):
                context.storage_objects = {}
            context.storage_objects[object_id] = {
                "object_id": object_id,
                "bucket": bucket,
                "key": key,
                "size": size,
                "content_type": content_type,
                "uploaded_at": time.time(),
                "etag": hashlib.md5(data).hexdigest(),
            }

            return ActionResult(
                success=True,
                data={"object_id": object_id, "bucket": bucket, "key": key, "size": size},
                message=f"Uploaded {key} to bucket {bucket}: {size} bytes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Storage upload failed: {e}")


class StorageDownloadAction(BaseAction):
    """Download from cloud storage."""
    action_type = "storage_download"
    display_name = "下载存储"
    description = "从云存储下载"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bucket = params.get("bucket", "")
            key = params.get("key", "")
            version = params.get("version", "")

            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key are required")

            objects = getattr(context, "storage_objects", {})
            obj = next((o for o in objects.values() if o["bucket"] == bucket and o["key"] == key), None)

            if not obj:
                return ActionResult(success=False, message=f"Object {bucket}/{key} not found")

            return ActionResult(
                success=True,
                data={"bucket": bucket, "key": key, "size": obj["size"], "etag": obj["etag"]},
                message=f"Downloaded {key} from {bucket}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Storage download failed: {e}")


class StorageDeleteAction(BaseAction):
    """Delete from cloud storage."""
    action_type = "storage_delete"
    display_name = "删除存储"
    description = "从云存储删除"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bucket = params.get("bucket", "")
            key = params.get("key", "")

            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key are required")

            objects = getattr(context, "storage_objects", {})
            obj_to_delete = next((o for o in objects.values() if o["bucket"] == bucket and o["key"] == key), None)

            if obj_to_delete:
                del objects[obj_to_delete["object_id"]]

            return ActionResult(
                success=True,
                data={"bucket": bucket, "key": key, "deleted": obj_to_delete is not None},
                message=f"Deleted {key} from {bucket}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Storage delete failed: {e}")


class StorageListAction(BaseAction):
    """List storage objects."""
    action_type = "storage_list"
    display_name = "列出存储"
    description = "列出云存储对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bucket = params.get("bucket", "")
            prefix = params.get("prefix", "")
            max_keys = params.get("max_keys", 100)

            if not bucket:
                return ActionResult(success=False, message="bucket is required")

            objects = getattr(context, "storage_objects", {})
            matching = [o for o in objects.values() if o["bucket"] == bucket and (not prefix or o["key"].startswith(prefix))]
            matching = matching[:max_keys]

            return ActionResult(
                success=True,
                data={"bucket": bucket, "objects": [{"key": o["key"], "size": o["size"]} for o in matching], "count": len(matching)},
                message=f"Listed {len(matching)} objects in bucket {bucket}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Storage list failed: {e}")


class StoragePresignAction(BaseAction):
    """Generate presigned URL."""
    action_type = "storage_presign"
    display_name = "预签名URL"
    description = "生成云存储预签名URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bucket = params.get("bucket", "")
            key = params.get("key", "")
            expires_in = params.get("expires_in", 3600)

            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key are required")

            object_id = hashlib.md5(f"{bucket}/{key}".encode()).hexdigest()[:12]
            url = f"https://{bucket}.storage.example.com/{key}?signature={object_id}&expires={int(time.time()) + expires_in}"

            return ActionResult(
                success=True,
                data={"url": url, "bucket": bucket, "key": key, "expires_in": expires_in},
                message=f"Presigned URL for {key}: expires in {expires_in}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Storage presign failed: {e}")
