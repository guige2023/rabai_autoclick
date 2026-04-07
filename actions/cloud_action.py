"""Cloud action module for RabAI AutoClick.

Provides cloud storage operations:
- CloudUploadAction: Upload file to cloud storage
- CloudDownloadAction: Download file from cloud storage
- CloudListAction: List cloud storage contents
- CloudDeleteAction: Delete file from cloud storage
- CloudExistsAction: Check if file exists
- CloudSignUrlAction: Generate signed URL
- CloudCopyAction: Copy file within cloud storage
- CloudMetadataAction: Get file metadata
"""

import os
import json
import hashlib
import subprocess
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CloudUploadAction(BaseAction):
    """Upload file to cloud storage."""
    action_type = "cloud_upload"
    display_name = "上传到云存储"
    description = "上传文件到云存储"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upload.

        Args:
            context: Execution context.
            params: Dict with file_path, bucket, key, provider.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_provider = context.resolve_value(provider)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            if resolved_provider == 'local':
                # Local file copy to bucket directory
                import shutil
                bucket_dir = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}")
                os.makedirs(bucket_dir, exist_ok=True)
                dest = os.path.join(bucket_dir, resolved_key)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.copy2(resolved_path, dest)
                url = dest

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    s3.upload_file(resolved_path, resolved_bucket, resolved_key)
                    url = f"{resolved_endpoint}/{resolved_bucket}/{resolved_key}"
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装: pip install boto3"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            return ActionResult(
                success=True,
                message=f"已上传: {resolved_key}",
                data={'key': resolved_key, 'url': url}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"上传失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudDownloadAction(BaseAction):
    """Download file from cloud storage."""
    action_type = "cloud_download"
    display_name = "从云存储下载"
    description = "从云存储下载文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute download.

        Args:
            context: Execution context.
            params: Dict with bucket, key, output_path, provider.

        Returns:
            ActionResult indicating success.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        output_path = params.get('output_path', '')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(bucket, str, 'bucket')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_output = context.resolve_value(output_path) if output_path else os.path.basename(resolved_key)
            resolved_provider = context.resolve_value(provider)

            if resolved_provider == 'local':
                source = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_key}")
                if not os.path.exists(source):
                    return ActionResult(
                        success=False,
                        message=f"云端文件不存在: {resolved_key}"
                    )
                import shutil
                shutil.copy2(source, resolved_output)

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    s3.download_file(resolved_bucket, resolved_key, resolved_output)
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            return ActionResult(
                success=True,
                message=f"已下载: {resolved_key} -> {resolved_output}",
                data={'output_path': resolved_output}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"下载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudListAction(BaseAction):
    """List cloud storage contents."""
    action_type = "cloud_list"
    display_name = "列出云存储"
    description = "列出云存储桶中的文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with bucket, prefix, output_var, provider.

        Returns:
            ActionResult with file list.
        """
        bucket = params.get('bucket', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'cloud_files')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_prefix = context.resolve_value(prefix) if prefix else ''
            resolved_provider = context.resolve_value(provider)

            files = []

            if resolved_provider == 'local':
                bucket_dir = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}")
                if not os.path.exists(bucket_dir):
                    return ActionResult(
                        success=True,
                        message=f"存储桶为空",
                        data={'files': [], 'count': 0, 'output_var': output_var}
                    )

                prefix_path = os.path.join(bucket_dir, resolved_prefix)
                for root, dirs, filenames in os.walk(prefix_path):
                    for filename in filenames:
                        full_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(full_path, bucket_dir)
                        files.append({
                            'key': rel_path,
                            'size': os.path.getsize(full_path),
                            'modified': os.path.getmtime(full_path)
                        })

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    kwargs = {'Bucket': resolved_bucket}
                    if resolved_prefix:
                        kwargs['Prefix'] = resolved_prefix

                    response = s3.list_objects_v2(**kwargs)
                    for obj in response.get('Contents', []):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'modified': obj['LastModified'].timestamp()
                        })
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            context.set(output_var, files)

            return ActionResult(
                success=True,
                message=f"列出 {len(files)} 个文件",
                data={'count': len(files), 'files': files, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'prefix': '', 'output_var': 'cloud_files', 'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudDeleteAction(BaseAction):
    """Delete file from cloud storage."""
    action_type = "cloud_delete"
    display_name = "删除云端文件"
    description = "从云存储删除文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with bucket, key, provider.

        Returns:
            ActionResult indicating success.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_provider = context.resolve_value(provider)

            if resolved_provider == 'local':
                file_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_key}")
                if not os.path.exists(file_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_key}"
                    )
                os.remove(file_path)

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    s3.delete_object(Bucket=resolved_bucket, Key=resolved_key)
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            return ActionResult(
                success=True,
                message=f"已删除: {resolved_key}",
                data={'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudExistsAction(BaseAction):
    """Check if file exists in cloud."""
    action_type = "cloud_exists"
    display_name = "检查云端文件"
    description = "检查云存储中文件是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exists check.

        Args:
            context: Execution context.
            params: Dict with bucket, key, output_var, provider.

        Returns:
            ActionResult with exists flag.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'cloud_exists')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_provider = context.resolve_value(provider)

            exists = False

            if resolved_provider == 'local':
                file_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_key}")
                exists = os.path.exists(file_path)

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    try:
                        s3.head_object(Bucket=resolved_bucket, Key=resolved_key)
                        exists = True
                    except:
                        exists = False
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"{resolved_key} 存在: {exists}",
                data={'exists': exists, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cloud_exists', 'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudSignUrlAction(BaseAction):
    """Generate signed URL for cloud object."""
    action_type = "cloud_sign_url"
    display_name = "生成签名URL"
    description = "为云存储对象生成签名URL"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sign URL.

        Args:
            context: Execution context.
            params: Dict with bucket, key, expires_in, output_var, provider.

        Returns:
            ActionResult with signed URL.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        expires_in = params.get('expires_in', 3600)
        output_var = params.get('output_var', 'signed_url')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_expiry = context.resolve_value(expires_in)
            resolved_provider = context.resolve_value(provider)

            if resolved_provider == 'local':
                file_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_key}")
                if not os.path.exists(file_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_key}"
                    )
                signed_url = f"file://{file_path}"

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    signed_url = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': resolved_bucket, 'Key': resolved_key},
                        ExpiresIn=int(resolved_expiry)
                    )
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            context.set(output_var, signed_url)

            return ActionResult(
                success=True,
                message=f"签名URL已生成 (有效期 {resolved_expiry}s)",
                data={'url': signed_url, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成签名URL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expires_in': 3600, 'output_var': 'signed_url', 'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudCopyAction(BaseAction):
    """Copy file within cloud storage."""
    action_type = "cloud_copy"
    display_name = "云端复制"
    description = "在云存储中复制文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with bucket, source_key, dest_key, provider.

        Returns:
            ActionResult indicating success.
        """
        bucket = params.get('bucket', '')
        source_key = params.get('source_key', '')
        dest_key = params.get('dest_key', '')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(source_key, str, 'source_key')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dest_key, str, 'dest_key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_src = context.resolve_value(source_key)
            resolved_dst = context.resolve_value(dest_key)
            resolved_provider = context.resolve_value(provider)

            if resolved_provider == 'local':
                import shutil
                src_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_src}")
                dst_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_dst}")

                if not os.path.exists(src_path):
                    return ActionResult(
                        success=False,
                        message=f"源文件不存在: {resolved_src}"
                    )

                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy2(src_path, dst_path)

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    s3.copy_object(
                        Bucket=resolved_bucket,
                        CopySource={'Bucket': resolved_bucket, 'Key': resolved_src},
                        Key=resolved_dst
                    )
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            return ActionResult(
                success=True,
                message=f"已复制: {resolved_src} -> {resolved_dst}",
                data={'source': resolved_src, 'dest': resolved_dst}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'source_key', 'dest_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}


class CloudMetadataAction(BaseAction):
    """Get file metadata from cloud storage."""
    action_type = "cloud_metadata"
    display_name = "获取云端元数据"
    description = "获取云存储文件的元数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metadata get.

        Args:
            context: Execution context.
            params: Dict with bucket, key, output_var, provider.

        Returns:
            ActionResult with metadata.
        """
        bucket = params.get('bucket', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'cloud_metadata')
        provider = params.get('provider', 'local')
        endpoint = params.get('endpoint', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_bucket = context.resolve_value(bucket)
            resolved_key = context.resolve_value(key)
            resolved_provider = context.resolve_value(provider)

            metadata = {}

            if resolved_provider == 'local':
                file_path = os.path.expanduser(f"~/.cloud_storage/{resolved_bucket}/{resolved_key}")
                if not os.path.exists(file_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_key}"
                    )
                stat = os.stat(file_path)
                metadata = {
                    'size': stat.st_size,
                    'modified': stat.st_mtime,
                    'created': stat.st_ctime,
                    'key': resolved_key
                }

            elif resolved_provider in ('minio', 's3'):
                try:
                    import boto3
                    from datetime import datetime as dt
                    resolved_endpoint = context.resolve_value(endpoint) if endpoint else None
                    resolved_ak = context.resolve_value(access_key) if access_key else None
                    resolved_sk = context.resolve_value(secret_key) if secret_key else None

                    s3 = boto3.client(
                        's3',
                        endpoint_url=resolved_endpoint,
                        aws_access_key_id=resolved_ak,
                        aws_secret_access_key=resolved_sk
                    )
                    resp = s3.head_object(Bucket=resolved_bucket, Key=resolved_key)
                    metadata = {
                        'size': resp.get('ContentLength', 0),
                        'modified': resp.get('LastModified', '').timestamp() if resp.get('LastModified') else 0,
                        'content_type': resp.get('ContentType', ''),
                        'etag': resp.get('ETag', ''),
                        'key': resolved_key
                    }
                except ImportError:
                    return ActionResult(
                        success=False,
                        message="boto3未安装"
                    )

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的云提供商: {resolved_provider}"
                )

            context.set(output_var, metadata)

            return ActionResult(
                success=True,
                message=f"获取元数据: {metadata.get('size', 0)} bytes",
                data=metadata
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取元数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bucket', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cloud_metadata', 'provider': 'local', 'endpoint': '', 'access_key': '', 'secret_key': ''}
