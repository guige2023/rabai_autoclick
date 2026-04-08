"""AWS S3 action module for RabAI AutoClick.

Provides Amazon S3 operations for object storage.
"""

import sys
import os
import hashlib
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AWSS3Action(BaseAction):
    """Amazon S3 object storage operations.
    
    Supports uploading, downloading, listing, copying, and
    managing S3 objects with presigned URLs and metadata.
    """
    action_type = "aws_s3"
    display_name = "AWS S3存储"
    description = "Amazon S3对象存储操作"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_boto3(self):
        """Import boto3."""
        try:
            import boto3
            return boto3
        except ImportError:
            return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'upload', 'download', 'list', 'delete', 'copy', 'presign', 'metadata'
                - bucket: S3 bucket name
                - key: S3 object key
                - source_file: Local file to upload
                - dest_file: Local path to download to
                - data: Data content to upload (bytes or str)
                - region: AWS region (default from env)
                - expires_in: Presigned URL expiry in seconds
                - prefix: Filter by prefix (for list)
        
        Returns:
            ActionResult with operation result.
        """
        boto3 = self._get_boto3()
        if boto3 is None:
            return ActionResult(
                success=False,
                message="Requires boto3. Install: pip install boto3"
            )
        
        command = params.get('command', 'list')
        bucket = params.get('bucket')
        key = params.get('key')
        source_file = params.get('source_file')
        dest_file = params.get('dest_file')
        data = params.get('data')
        region = params.get('region', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
        expires_in = params.get('expires_in', 3600)
        prefix = params.get('prefix', '')
        
        try:
            s3 = boto3.client('s3', region_name=region)
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create S3 client: {e}")
        
        if command == 'upload':
            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key required for upload")
            return self._s3_upload(s3, bucket, key, source_file, data)
        
        if command == 'download':
            if not bucket or not key or not dest_file:
                return ActionResult(success=False, message="bucket, key, and dest_file required for download")
            return self._s3_download(s3, bucket, key, dest_file)
        
        if command == 'list':
            if not bucket:
                return ActionResult(success=False, message="bucket required for list")
            return self._s3_list(s3, bucket, prefix)
        
        if command == 'delete':
            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key required for delete")
            return self._s3_delete(s3, bucket, key)
        
        if command == 'copy':
            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key required for copy")
            return self._s3_copy(s3, bucket, key, params.get('dest_bucket', bucket), params.get('dest_key', key))
        
        if command == 'presign':
            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key required for presign")
            return self._s3_presign(s3, bucket, key, expires_in)
        
        if command == 'metadata':
            if not bucket or not key:
                return ActionResult(success=False, message="bucket and key required for metadata")
            return self._s3_metadata(s3, bucket, key)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _s3_upload(self, s3: Any, bucket: str, key: str, source_file: Optional[str], data: Any) -> ActionResult:
        """Upload file or data to S3."""
        try:
            if source_file:
                with open(source_file, 'rb') as f:
                    content = f.read()
            elif data is not None:
                content = data.encode('utf-8') if isinstance(data, str) else data
            else:
                return ActionResult(success=False, message="source_file or data required for upload")
            
            extra = {}
            if isinstance(data, str):
                extra['ContentType'] = 'text/plain'
            
            s3.put_object(Bucket=bucket, Key=key, Body=content, **extra)
            return ActionResult(
                success=True,
                message=f"Uploaded to s3://{bucket}/{key} ({len(content)} bytes)",
                data={'bucket': bucket, 'key': key, 'size': len(content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to upload: {e}")
    
    def _s3_download(self, s3: Any, bucket: str, key: str, dest_file: str) -> ActionResult:
        """Download S3 object to local file."""
        try:
            s3.download_file(bucket, key, dest_file)
            size = os.path.getsize(dest_file)
            return ActionResult(
                success=True,
                message=f"Downloaded s3://{bucket}/{key} to {dest_file} ({size} bytes)",
                data={'bucket': bucket, 'key': key, 'dest_file': dest_file, 'size': size}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to download: {e}")
    
    def _s3_list(self, s3: Any, bucket: str, prefix: str) -> ActionResult:
        """List objects in S3 bucket."""
        try:
            kwargs: Dict[str, Any] = {'Bucket': bucket}
            if prefix:
                kwargs['Prefix'] = prefix
            
            response = s3.list_objects_v2(**kwargs)
            objects = response.get('Contents', [])
            
            return ActionResult(
                success=True,
                message=f"Listed {len(objects)} objects in {bucket}",
                data={
                    'objects': [{'key': obj['Key'], 'size': obj['Size'], 'last_modified': str(obj['LastModified'])} for obj in objects],
                    'count': len(objects)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to list: {e}")
    
    def _s3_delete(self, s3: Any, bucket: str, key: str) -> ActionResult:
        """Delete S3 object."""
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            return ActionResult(success=True, message=f"Deleted s3://{bucket}/{key}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to delete: {e}")
    
    def _s3_copy(self, s3: Any, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str) -> ActionResult:
        """Copy S3 object."""
        try:
            copy_source = {'Bucket': src_bucket, 'Key': src_key}
            s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            return ActionResult(
                success=True,
                message=f"Copied s3://{src_bucket}/{src_key} -> s3://{dest_bucket}/{dest_key}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to copy: {e}")
    
    def _s3_presign(self, s3: Any, bucket: str, key: str, expires_in: int) -> ActionResult:
        """Generate presigned URL for S3 object."""
        try:
            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expires_in
            )
            return ActionResult(
                success=True,
                message=f"Presigned URL (expires in {expires_in}s)",
                data={'url': url, 'expires_in': expires_in}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to generate presigned URL: {e}")
    
    def _s3_metadata(self, s3: Any, bucket: str, key: str) -> ActionResult:
        """Get S3 object metadata."""
        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            return ActionResult(
                success=True,
                message=f"Metadata for s3://{bucket}/{key}",
                data={
                    'content_length': response.get('ContentLength'),
                    'content_type': response.get('ContentType'),
                    'last_modified': str(response.get('LastModified')),
                    'etag': response.get('ETag'),
                    'metadata': response.get('Metadata', {})
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get metadata: {e}")
