"""
AWS S3 storage operations actions.
"""
from __future__ import annotations

import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, List


def create_s3_client(
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: str = 'us-east-1',
    endpoint_url: Optional[str] = None
):
    """
    Create an S3 client.

    Args:
        aws_access_key_id: AWS access key.
        aws_secret_access_key: AWS secret key.
        region_name: AWS region.
        endpoint_url: Custom endpoint URL (for MinIO, LocalStack, etc.).

    Returns:
        S3 client.
    """
    kwargs: Dict[str, Any] = {
        'region_name': region_name,
    }

    if aws_access_key_id and aws_secret_access_key:
        kwargs['aws_access_key_id'] = aws_access_key_id
        kwargs['aws_secret_access_key'] = aws_secret_access_key

    if endpoint_url:
        kwargs['endpoint_url'] = endpoint_url

    return boto3.client('s3', **kwargs)


def list_buckets(s3_client) -> List[str]:
    """
    List all S3 buckets.

    Args:
        s3_client: S3 client.

    Returns:
        List of bucket names.
    """
    try:
        response = s3_client.list_buckets()
        return [bucket['Name'] for bucket in response.get('Buckets', [])]
    except ClientError as e:
        raise RuntimeError(f"Failed to list buckets: {e}")


def list_objects(
    s3_client,
    bucket: str,
    prefix: str = '',
    max_keys: int = 1000
) -> List[Dict[str, Any]]:
    """
    List objects in a bucket.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        prefix: Object prefix filter.
        max_keys: Maximum keys to return.

    Returns:
        List of object information.
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys
        )

        objects = []
        for obj in response.get('Contents', []):
            objects.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
                'etag': obj['ETag'].strip('"'),
            })

        return objects
    except ClientError as e:
        raise RuntimeError(f"Failed to list objects: {e}")


def upload_file(
    s3_client,
    file_path: str,
    bucket: str,
    key: str,
    extra_args: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Upload a file to S3.

    Args:
        s3_client: S3 client.
        file_path: Local file path.
        bucket: Target bucket.
        key: Object key.
        extra_args: Extra arguments (ContentType, ACL, etc.).

    Returns:
        Upload result.
    """
    try:
        extra = extra_args or {}

        s3_client.upload_file(
            file_path,
            bucket,
            key,
            ExtraArgs=extra
        )

        return {
            'success': True,
            'bucket': bucket,
            'key': key,
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def download_file(
    s3_client,
    bucket: str,
    key: str,
    file_path: str
) -> Dict[str, Any]:
    """
    Download a file from S3.

    Args:
        s3_client: S3 client.
        bucket: Source bucket.
        key: Object key.
        file_path: Local destination path.

    Returns:
        Download result.
    """
    try:
        import os
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

        s3_client.download_file(bucket, key, file_path)

        return {
            'success': True,
            'bucket': bucket,
            'key': key,
            'file_path': file_path,
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def delete_object(
    s3_client,
    bucket: str,
    key: str
) -> Dict[str, Any]:
    """
    Delete an object from S3.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        key: Object key.

    Returns:
        Deletion result.
    """
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)

        return {
            'success': True,
            'bucket': bucket,
            'key': key,
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def delete_objects_batch(
    s3_client,
    bucket: str,
    keys: List[str]
) -> Dict[str, Any]:
    """
    Delete multiple objects from S3.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        keys: List of object keys.

    Returns:
        Deletion result.
    """
    try:
        objects = [{'Key': key} for key in keys]

        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': objects}
        )

        deleted = response.get('Deleted', [])
        errors = response.get('Errors', [])

        return {
            'success': len(errors) == 0,
            'deleted': len(deleted),
            'errors': len(errors),
            'error_details': errors,
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def copy_object(
    s3_client,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str
) -> Dict[str, Any]:
    """
    Copy an object within S3.

    Args:
        s3_client: S3 client.
        source_bucket: Source bucket.
        source_key: Source object key.
        dest_bucket: Destination bucket.
        dest_key: Destination object key.

    Returns:
        Copy result.
    """
    try:
        copy_source = {'Bucket': source_bucket, 'Key': source_key}

        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=dest_key
        )

        return {
            'success': True,
            'source': f'{source_bucket}/{source_key}',
            'destination': f'{dest_bucket}/{dest_key}',
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def get_object_metadata(
    s3_client,
    bucket: str,
    key: str
) -> Optional[Dict[str, Any]]:
    """
    Get metadata for an S3 object.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        key: Object key.

    Returns:
        Object metadata or None.
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)

        return {
            'key': key,
            'size': response.get('ContentLength'),
            'content_type': response.get('ContentType'),
            'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
            'etag': response.get('ETag', '').strip('"'),
            'metadata': response.get('Metadata', {}),
        }
    except ClientError:
        return None


def generate_presigned_url(
    s3_client,
    bucket: str,
    key: str,
    expiration: int = 3600
) -> Optional[str]:
    """
    Generate a presigned URL for an object.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        key: Object key.
        expiration: URL expiration in seconds.

    Returns:
        Presigned URL or None.
    """
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except ClientError:
        return None


def create_bucket(
    s3_client,
    bucket: str,
    region: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create an S3 bucket.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.
        region: AWS region.

    Returns:
        Creation result.
    """
    try:
        kwargs: Dict[str, Any] = {'Bucket': bucket}

        if region:
            kwargs['CreateBucketConfiguration'] = {
                'LocationConstraint': region
            }

        s3_client.create_bucket(**kwargs)

        return {
            'success': True,
            'bucket': bucket,
        }
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def bucket_exists(s3_client, bucket: str) -> bool:
    """
    Check if a bucket exists.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.

    Returns:
        True if bucket exists.
    """
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def sync_directory(
    s3_client,
    local_path: str,
    bucket: str,
    prefix: str = ''
) -> Dict[str, Any]:
    """
    Sync a local directory to S3.

    Args:
        s3_client: S3 client.
        local_path: Local directory path.
        bucket: Target bucket.
        prefix: S3 key prefix.

    Returns:
        Sync result.
    """
    import os

    uploaded = 0
    errors = 0

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)

            if prefix:
                key = f'{prefix}/{relative_path}'
            else:
                key = relative_path

            result = upload_file(s3_client, local_file, bucket, key)

            if result['success']:
                uploaded += 1
            else:
                errors += 1

    return {
        'success': errors == 0,
        'uploaded': uploaded,
        'errors': errors,
    }


def empty_bucket(s3_client, bucket: str) -> Dict[str, Any]:
    """
    Delete all objects from a bucket.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.

    Returns:
        Empty result.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)

        objects = response.get('Contents', [])

        if not objects:
            return {'success': True, 'deleted': 0}

        result = delete_objects_batch(
            s3_client,
            bucket,
            [obj['Key'] for obj in objects]
        )

        return result
    except ClientError as e:
        return {
            'success': False,
            'error': str(e),
        }


def get_bucket_size(s3_client, bucket: str) -> Dict[str, Any]:
    """
    Calculate total size of objects in a bucket.

    Args:
        s3_client: S3 client.
        bucket: Bucket name.

    Returns:
        Size information.
    """
    total_size = 0
    total_objects = 0

    paginator = s3_client.get_paginator('list_objects_v2')

    try:
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
                total_objects += 1

        return {
            'bucket': bucket,
            'total_bytes': total_size,
            'total_mb': round(total_size / (1024 * 1024), 2),
            'total_gb': round(total_size / (1024 * 1024 * 1024), 4),
            'object_count': total_objects,
        }
    except ClientError as e:
        return {
            'error': str(e),
        }
