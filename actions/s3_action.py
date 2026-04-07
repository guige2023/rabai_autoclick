"""S3 storage action module for RabAI AutoClick.

Provides S3 operations:
- S3UploadAction: Upload file to S3
- S3DownloadAction: Download file from S3
- S3ListAction: List S3 objects
- S3DeleteAction: Delete S3 object
- S3PresignAction: Generate presigned URL
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class S3UploadAction(BaseAction):
    """Upload file to S3."""
    action_type = "s3_upload"
    display_name = "S3上传"
    description = "上传文件到S3"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 upload."""
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        file_path = params.get('file_path', '')
        content = params.get('content', None)
        content_type = params.get('content_type', 'application/octet-stream')
        acl = params.get('acl', 'private')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        endpoint = params.get('endpoint', None)  # for S3-compatible services
        output_var = params.get('output_var', 's3_upload_result')

        if not bucket or not key:
            return ActionResult(success=False, message="bucket and key are required")
        if not file_path and content is None:
            return ActionResult(success=False, message="file_path or content is required")

        try:
            import boto3
            from botocore.config import Config

            resolved_bucket = context.resolve_value(bucket) if context else bucket
            resolved_key = context.resolve_value(key) if context else key
            resolved_file = context.resolve_value(file_path) if context else file_path
            resolved_content = context.resolve_value(content) if context else content
            resolved_region = context.resolve_value(region) if context else region
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_acl = context.resolve_value(acl) if context else acl

            extra_args = {'ContentType': content_type, 'ACL': resolved_acl}

            config = Config(region_name=resolved_region)
            client_kwargs = {
                'service_name': 's3',
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'config': config,
            }
            if resolved_endpoint:
                client_kwargs['endpoint_url'] = resolved_endpoint

            s3 = boto3.client(**client_kwargs)

            if resolved_file:
                s3.upload_file(resolved_file, resolved_bucket, resolved_key, ExtraArgs=extra_args)
                result = {'bucket': resolved_bucket, 'key': resolved_key, 'source': resolved_file}
            else:
                if isinstance(resolved_content, str):
                    resolved_content = resolved_content.encode('utf-8')
                s3.put_object(Bucket=resolved_bucket, Key=resolved_key, Body=resolved_content, **extra_args)
                result = {'bucket': resolved_bucket, 'key': resolved_key, 'content': len(resolved_content)}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Uploaded to s3://{resolved_bucket}/{resolved_key}", data=result)
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed. Run: pip install boto3")
        except Exception as e:
            return ActionResult(success=False, message=f"S3 upload error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'file_path': '', 'content': None, 'content_type': 'application/octet-stream',
            'acl': 'private', 'access_key': '', 'secret_key': '', 'region': 'us-east-1',
            'endpoint': None, 'output_var': 's3_upload_result'
        }


class S3DownloadAction(BaseAction):
    """Download file from S3."""
    action_type = "s3_download"
    display_name = "S3下载"
    description = "从S3下载文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 download."""
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        output_path = params.get('output_path', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        endpoint = params.get('endpoint', None)
        output_var = params.get('output_var', 's3_download_result')

        if not bucket or not key:
            return ActionResult(success=False, message="bucket and key are required")

        try:
            import boto3
            from botocore.config import Config

            resolved_bucket = context.resolve_value(bucket) if context else bucket
            resolved_key = context.resolve_value(key) if context else key
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_region = context.resolve_value(region) if context else region
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint

            config = Config(region_name=resolved_region)
            client_kwargs = {
                'service_name': 's3',
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'config': config,
            }
            if resolved_endpoint:
                client_kwargs['endpoint_url'] = resolved_endpoint

            s3 = boto3.client(**client_kwargs)

            if resolved_output:
                _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
                s3.download_file(resolved_bucket, resolved_key, resolved_output)
                result = {'output_path': resolved_output, 'bucket': resolved_bucket, 'key': resolved_key}
            else:
                obj = s3.get_object(Bucket=resolved_bucket, Key=resolved_key)
                data = obj['Body'].read()
                result = {'data': data, 'bucket': resolved_bucket, 'key': resolved_key, 'size': len(data)}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Downloaded s3://{resolved_bucket}/{resolved_key}", data=result)
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"S3 download error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_path': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1',
            'endpoint': None, 'output_var': 's3_download_result'
        }


class S3ListAction(BaseAction):
    """List S3 objects."""
    action_type = "s3_list"
    display_name = "S3列表"
    description = "列出S3对象"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 list."""
        bucket = params.get('bucket', '')
        prefix = params.get('prefix', '')
        max_keys = params.get('max_keys', 100)
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        endpoint = params.get('endpoint', None)
        output_var = params.get('output_var', 's3_list_result')

        if not bucket:
            return ActionResult(success=False, message="bucket is required")

        try:
            import boto3
            from botocore.config import Config

            resolved_bucket = context.resolve_value(bucket) if context else bucket
            resolved_prefix = context.resolve_value(prefix) if context else prefix
            resolved_max = context.resolve_value(max_keys) if context else max_keys
            resolved_region = context.resolve_value(region) if context else region
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint

            config = Config(region_name=resolved_region)
            client_kwargs = {
                'service_name': 's3',
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'config': config,
            }
            if resolved_endpoint:
                client_kwargs['endpoint_url'] = resolved_endpoint

            s3 = boto3.client(**client_kwargs)

            kwargs = {'Bucket': resolved_bucket, 'MaxKeys': int(resolved_max)}
            if resolved_prefix:
                kwargs['Prefix'] = resolved_prefix

            response = s3.list_objects_v2(**kwargs)
            objects = response.get('Contents', [])
            result = {
                'objects': [{'key': o['Key'], 'size': o['Size'], 'last_modified': str(o.get('LastModified', ''))} for o in objects],
                'count': len(objects),
                'bucket': resolved_bucket,
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Listed {len(objects)} objects", data=result)
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"S3 list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['bucket']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'prefix': '', 'max_keys': 100, 'access_key': '', 'secret_key': '', 'region': 'us-east-1',
            'endpoint': None, 'output_var': 's3_list_result'
        }


class S3DeleteAction(BaseAction):
    """Delete S3 object."""
    action_type = "s3_delete"
    display_name = "S3删除"
    description = "删除S3对象"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 delete."""
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        endpoint = params.get('endpoint', None)
        output_var = params.get('output_var', 's3_delete_result')

        if not bucket or not key:
            return ActionResult(success=False, message="bucket and key are required")

        try:
            import boto3
            from botocore.config import Config

            resolved_bucket = context.resolve_value(bucket) if context else bucket
            resolved_key = context.resolve_value(key) if context else key
            resolved_region = context.resolve_value(region) if context else region
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint

            config = Config(region_name=resolved_region)
            client_kwargs = {
                'service_name': 's3',
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'config': config,
            }
            if resolved_endpoint:
                client_kwargs['endpoint_url'] = resolved_endpoint

            s3 = boto3.client(**client_kwargs)
            s3.delete_object(Bucket=resolved_bucket, Key=resolved_key)

            result = {'deleted': True, 'bucket': resolved_bucket, 'key': resolved_key}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Deleted s3://{resolved_bucket}/{resolved_key}", data=result)
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"S3 delete error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'access_key': '', 'secret_key': '', 'region': 'us-east-1',
            'endpoint': None, 'output_var': 's3_delete_result'
        }


class S3PresignAction(BaseAction):
    """Generate S3 presigned URL."""
    action_type = "s3_presign"
    display_name = "S3预签名URL"
    description = "生成S3预签名URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 presign."""
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        expiration = params.get('expiration', 3600)
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        endpoint = params.get('endpoint', None)
        output_var = params.get('output_var', 's3_presign_result')

        if not bucket or not key:
            return ActionResult(success=False, message="bucket and key are required")

        try:
            import boto3
            from botocore.config import Config

            resolved_bucket = context.resolve_value(bucket) if context else bucket
            resolved_key = context.resolve_value(key) if context else key
            resolved_exp = context.resolve_value(expiration) if context else expiration
            resolved_region = context.resolve_value(region) if context else region
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint

            config = Config(region_name=resolved_region)
            client_kwargs = {
                'service_name': 's3',
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'config': config,
            }
            if resolved_endpoint:
                client_kwargs['endpoint_url'] = resolved_endpoint

            s3 = boto3.client(**client_kwargs)
            url = s3.generate_presigned_url('get_object', Params={'Bucket': resolved_bucket, 'Key': resolved_key}, ExpiresIn=int(resolved_exp))

            result = {'url': url, 'bucket': resolved_bucket, 'key': resolved_key, 'expires_in': resolved_exp}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Presigned URL generated (expires in {resolved_exp}s)", data=result)
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"S3 presign error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'expiration': 3600, 'access_key': '', 'secret_key': '', 'region': 'us-east-1',
            'endpoint': None, 'output_var': 's3_presign_result'
        }
