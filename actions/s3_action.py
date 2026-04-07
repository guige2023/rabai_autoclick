"""S3 action module for RabAI AutoClick.

Provides S3/cloud storage operations:
- S3UploadAction: Upload file to S3
- S3DownloadAction: Download file from S3
- S3ListAction: List S3 objects
- S3DeleteAction: Delete S3 object
- S3ExistsAction: Check if S3 object exists
- S3CopyAction: Copy S3 object
- S3MetadataAction: Get S3 object metadata
- S3GenerateUrlAction: Generate presigned URL
"""

import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class S3UploadAction(BaseAction):
    """Upload file to S3."""
    action_type = "s3_upload"
    display_name = "S3上传"
    description = "上传文件到S3"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upload.

        Args:
            context: Execution context.
            params: Dict with file_path, bucket, key, endpoint, access_key, secret_key, region.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        content_type = params.get('content_type', 'application/octet-stream')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_path = context.resolve_value(file_path)
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)
            resolved_ct = context.resolve_value(content_type)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            s3.upload_file(
                resolved_path,
                resolved_bucket,
                resolved_key,
                ExtraArgs={'ContentType': resolved_ct}
            )

            return ActionResult(
                success=True,
                message=f"已上传: {resolved_key} -> s3://{resolved_bucket}/{resolved_key}",
                data={'bucket': resolved_bucket, 'key': resolved_key}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="boto3未安装: pip install boto3"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3上传失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1', 'content_type': 'application/octet-stream'}


class S3DownloadAction(BaseAction):
    """Download file from S3."""
    action_type = "s3_download"
    display_name = "S3下载"
    description = "从S3下载文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute download.

        Args:
            context: Execution context.
            params: Dict with bucket, key, output_path, endpoint, access_key, secret_key, region.

        Returns:
            ActionResult indicating success.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        output_path = params.get('output_path', '')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')

        valid, msg = self.validate_type(bucket, str, 'bucket')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_output = context.resolve_value(output_path) if output_path else os.path.basename(resolved_key)
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            s3.download_file(resolved_bucket, resolved_key, resolved_output)

            return ActionResult(
                success=True,
                message=f"已下载: s3://{resolved_bucket}/{resolved_key} -> {resolved_output}",
                data={'output_path': resolved_output, 'bucket': resolved_bucket, 'key': resolved_key}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="boto3未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3下载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1'}


class S3ListAction(BaseAction):
    """List S3 objects."""
    action_type = "s3_list"
    display_name = "S3列表"
    description = "列出S3桶中的对象"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with bucket, prefix, endpoint, access_key, secret_key, region, output_var.

        Returns:
            ActionResult with object list.
        """
        bucket = params.get('bucket', '')
        prefix = params.get('prefix', '')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        output_var = params.get('output_var', 's3_objects')

        valid, msg = self.validate_type(bucket, str, 'bucket')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_bucket = context.resolve_value(bucket)
            resolved_prefix = context.resolve_value(prefix) if prefix else ''
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            kwargs2 = {'Bucket': resolved_bucket}
            if resolved_prefix:
                kwargs2['Prefix'] = resolved_prefix

            response = s3.list_objects_v2(**kwargs2)

            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat() if obj.get('LastModified') else None
                })

            context.set(output_var, objects)

            return ActionResult(
                success=True,
                message=f"S3对象: {len(objects)} 个",
                data={'count': len(objects), 'objects': objects, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="boto3未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'prefix': '', 'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1', 'output_var': 's3_objects'}


class S3DeleteAction(BaseAction):
    """Delete S3 object."""
    action_type = "s3_delete"
    display_name = "S3删除"
    description = "删除S3对象"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with bucket, key, endpoint, access_key, secret_key, region.

        Returns:
            ActionResult indicating success.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            s3.delete_object(Bucket=resolved_bucket, Key=resolved_key)

            return ActionResult(
                success=True,
                message=f"已删除: s3://{resolved_bucket}/{resolved_key}",
                data={'bucket': resolved_bucket, 'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1'}


class S3ExistsAction(BaseAction):
    """Check if S3 object exists."""
    action_type = "s3_exists"
    display_name = "S3检查存在"
    description = "检查S3对象是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exists.

        Args:
            context: Execution context.
            params: Dict with bucket, key, endpoint, access_key, secret_key, region, output_var.

        Returns:
            ActionResult with exists flag.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        output_var = params.get('output_var', 's3_exists')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            try:
                s3.head_object(Bucket=resolved_bucket, Key=resolved_key)
                exists = True
            except:
                exists = False

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"s3://{resolved_bucket}/{resolved_key} {'存在' if exists else '不存在'}",
                data={'exists': exists, 'bucket': resolved_bucket, 'key': resolved_key, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1', 'output_var': 's3_exists'}


class S3GenerateUrlAction(BaseAction):
    """Generate presigned URL."""
    action_type = "s3_generate_url"
    display_name = "S3生成URL"
    description = "生成S3对象预签名URL"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate URL.

        Args:
            context: Execution context.
            params: Dict with bucket, key, expires_in, endpoint, access_key, secret_key, region, output_var.

        Returns:
            ActionResult with URL.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        expires_in = params.get('expires_in', 3600)
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        output_var = params.get('output_var', 's3_url')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import boto3

            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_expiry = context.resolve_value(expires_in)
            resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
            resolved_ak = context.resolve_value(access_key) if access_key else None
            resolved_sk = context.resolve_value(secret_key) if secret_key else None
            resolved_region = context.resolve_value(region)

            kwargs = {'RegionName': resolved_region}
            if resolved_endpoint:
                kwargs['endpoint_url'] = resolved_endpoint
            if resolved_ak:
                kwargs['aws_access_key_id'] = resolved_ak
            if resolved_sk:
                kwargs['aws_secret_access_key'] = resolved_sk

            s3 = boto3.client('s3', **kwargs)

            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': resolved_bucket, 'Key': resolved_key},
                ExpiresIn=int(resolved_expiry)
            )

            context.set(output_var, url)

            return ActionResult(
                success=True,
                message=f"预签名URL已生成 (有效期 {resolved_expiry}s)",
                data={'url': url, 'expires_in': resolved_expiry, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"S3生成URL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expires_in': 3600, 'endpoint': '', 'access_key': '', 'secret_key': '', 'region': 'us-east-1', 'output_var': 's3_url'}
