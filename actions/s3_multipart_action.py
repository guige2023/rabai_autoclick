"""S3 multipart upload action module for RabAI AutoClick.

Provides S3 multipart upload operations:
- MultipartUploader: Upload large files in parts
- MultipartStatusChecker: Check upload status
- MultipartAborter: Abort incomplete uploads
- MultipartComplete: Complete multipart uploads
- PartTracker: Track uploaded parts
"""

from __future__ import annotations

import json
import sys
import os
import hashlib
import boto3
from botocore.exceptions import ClientError
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class MultipartUploaderAction(BaseAction):
    """Upload large files using S3 multipart."""
    action_type = "s3_multipart_uploader"
    display_name = "S3分段上传"
    description = "S3大文件分段上传"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed")

        try:
            bucket = params.get("bucket", "")
            key = params.get("key", "")
            file_path = params.get("file_path", "")
            part_size = params.get("part_size", 5 * 1024 * 1024)
            region = params.get("region", "us-east-1")
            num_threads = params.get("num_threads", 4)

            if not bucket or not key or not file_path:
                return ActionResult(success=False, message="bucket, key, and file_path required")
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")

            client = boto3.client("s3", region_name=region)
            file_size = os.path.getsize(file_path)

            mpu = client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = mpu["UploadId"]

            parts = []
            with open(file_path, "rb") as f:
                part_number = 1
                offset = 0
                while offset < file_size:
                    chunk_size = min(part_size, file_size - offset)
                    chunk = f.read(chunk_size)

                    part_hash = hashlib.md5(chunk).hexdigest()
                    response = client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                    offset += chunk_size
                    part_number += 1

            response = client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            return ActionResult(
                success=True,
                message=f"Multipart upload complete: {key}",
                data={"bucket": bucket, "key": key, "parts": len(parts), "location": response.get("Location")}
            )

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class MultipartAbortedListAction(BaseAction):
    """List and abort incomplete multipart uploads."""
    action_type = "s3_multipart_abort"
    display_name = "S3上传中止"
    description = "列出并中止未完成的分段上传"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed")

        try:
            bucket = params.get("bucket", "")
            region = params.get("region", "us-east-1")
            abort_all = params.get("abort_all", False)
            older_than_days = params.get("older_than_days", 7)
            prefix = params.get("prefix", "")

            if not bucket:
                return ActionResult(success=False, message="bucket required")

            client = boto3.client("s3", region_name=region)

            paginator = client.get_paginator("list_multipart_uploads")
            uploads = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for upload in page.get("Uploads", []):
                    uploads.append({
                        "key": upload["Key"],
                        "upload_id": upload["UploadId"],
                        "initiated": upload["Initiated"],
                    })

            aborted = []
            for upload in uploads:
                if abort_all or params.get("upload_id"):
                    if params.get("upload_id") == upload["upload_id"] or abort_all:
                        client.abort_multipart_upload(
                            Bucket=bucket,
                            Key=upload["key"],
                            UploadId=upload["upload_id"],
                        )
                        aborted.append(upload["key"])

            return ActionResult(
                success=True,
                message=f"Aborted {len(aborted)} uploads",
                data={"aborted": aborted, "total_found": len(uploads)}
            )

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
