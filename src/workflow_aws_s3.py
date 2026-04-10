"""
AWS S3 Storage Integration Module for Workflow System

Implements an S3Integration class with:
1. Bucket management: Create/manage S3 buckets
2. Object operations: Upload/download/delete objects
3. Multipart upload: Large file uploads
4. Bucket policies: Manage bucket policies
5. Lifecycle rules: Configure lifecycle policies
6. Versioning: Enable bucket versioning
7. Encryption: Configure encryption
8. CORS: Configure CORS rules
9. Static website: Configure static website hosting
10. CloudWatch integration: Metrics and monitoring

Commit: 'feat(aws-s3): add AWS S3 integration with bucket management, object operations, multipart upload, policies, lifecycle, versioning, encryption, CORS, static website, CloudWatch'
"""

import uuid
import json
import threading
import time
import logging
import os
import mimetypes
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, BinaryIO
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import hashlib
import math

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    from botocore.config import Config as BotoConfig
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None
    BotoConfig = None


logger = logging.getLogger(__name__)


class StorageClass(Enum):
    """S3 storage classes."""
    STANDARD = "STANDARD"
    REDUCED_REDUNDANCY = "REDUCED_REDUNDANCY"
    STANDARD_IA = "STANDARD_IA"
    ONEZONE_IA = "ONEZONE_IA"
    GLACIER = "GLACIER"
    DEEP_ARCHIVE = "DEEP_ARCHIVE"
    INTELLIGENT_TIERING = "INTELLIGENT_TIERING"


class EncryptionType(Enum):
    """S3 encryption types."""
    NONE = "NONE"
    SSE_S3 = "SSE-S3"
    SSE_KMS = "SSE-KMS"
    SSE_C = "SSE-C"


class MetadataDirective(Enum):
    """Metadata directives for copy operations."""
    COPY = "COPY"
    REPLACE = "REPLACE"


@dataclass
class BucketConfig:
    """Configuration for an S3 bucket."""
    name: str
    region: Optional[str] = None
    acl: str = "private"
    object_ownership: str = "BucketOwnerPreferred"
    block_public_acls: bool = True
    block_public_policy: bool = True
    ignore_public_acls: bool = True
    restrict_public_buckets: bool = True
    tags: Dict[str, str] = field(default_factory=dict)
    lifecycle_config: Optional[Dict[str, Any]] = None
    versioning_enabled: bool = False
    encryption_enabled: bool = True
    encryption_type: EncryptionType = EncryptionType.SSE_S3
    kms_key_id: Optional[str] = None
    cors_config: Optional[List[Dict[str, Any]]] = None
    website_config: Optional[Dict[str, Any]] = None


@dataclass
class UploadConfig:
    """Configuration for uploading an object."""
    bucket: str
    key: str
    data: Union[bytes, str, BinaryIO]
    content_type: Optional[str] = None
    storage_class: StorageClass = StorageClass.STANDARD
    encryption: EncryptionType = EncryptionType.SSE_S3
    kms_key_id: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    cache_control: Optional[str] = None
    content_disposition: Optional[str] = None
    content_encoding: Optional[str] = None
    content_language: Optional[str] = None
    expires: Optional[datetime] = None
    retain_until: Optional[datetime] = None


@dataclass
class MultipartUploadConfig:
    """Configuration for multipart upload."""
    bucket: str
    key: str
    file_path: Optional[str] = None
    data: Optional[bytes] = None
    content_type: Optional[str] = None
    storage_class: StorageClass = StorageClass.STANDARD
    part_size: int = 5 * 1024 * 1024  # 5MB minimum
    threshold: int = 100 * 1024 * 1024  # 100MB
    encryption: EncryptionType = EncryptionType.SSE_S3
    kms_key_id: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    max_concurrency: int = 4


@dataclass
class LifecycleRule:
    """Lifecycle rule configuration."""
    id: str
    prefix: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    expiration_days: Optional[int] = None
    expiration_date: Optional[datetime] = None
    transition_days: Optional[int] = None
    transition_date: Optional[datetime] = None
    transition_storage_class: Optional[StorageClass] = None
    noncurrent_expiration_days: Optional[int] = None
    noncurrent_transition_days: Optional[int] = None
    noncurrent_transition_storage_class: Optional[StorageClass] = None
    abort_incomplete_days: Optional[int] = None


@dataclass
class CorsRule:
    """CORS rule configuration."""
    allowed_origins: List[str]
    allowed_methods: List[str]
    allowed_headers: List[str] = field(default_factory=list)
    max_age_seconds: int = 3600
    expose_headers: List[str] = field(default_factory=list)


@dataclass
class WebsiteConfig:
    """Static website hosting configuration."""
    index_document: str = "index.html"
    error_document: Optional[str] = None
    redirect_all_requests_to: Optional[Dict[str, str]] = None
    routing_rules: Optional[List[Dict[str, Any]]] = None


@dataclass
class PolicyStatement:
    """IAM policy statement."""
    sid: Optional[str] = None
    effect: str = "Allow"
    principal: Union[str, Dict, List] = "*"
    actions: List[str] = field(default_factory=list)
    not_actions: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)
    not_resources: List[str] = field(default_factory=list)
    conditions: Dict[str, Any] = field(default_factory=dict)


class S3Integration:
    """
    AWS S3 Storage Integration class.
    
    Provides comprehensive S3 operations including bucket management,
    object operations, multipart uploads, policies, lifecycle rules,
    versioning, encryption, CORS, static website hosting, and CloudWatch integration.
    """
    
    def __init__(
        self,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        config: Optional[BotoConfig] = None
    ):
        """
        Initialize S3Integration.
        
        Args:
            region_name: AWS region
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            endpoint_url: Custom endpoint URL (for S3-compatible services)
            config: Botocore configuration
        """
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for S3 integration. "
                "Install it with: pip install boto3"
            )
        
        self.region_name = region_name or os.environ.get("AWS_REGION", "us-east-1")
        self.endpoint_url = endpoint_url
        
        session_kwargs = {
            "region_name": self.region_name
        }
        
        if profile_name:
            session_kwargs["profile_name"] = profile_name
        elif aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                session_kwargs["aws_session_token"] = aws_session_token
        
        self.session = boto3.Session(**session_kwargs)
        
        client_kwargs = {"region_name": self.region_name}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if config:
            client_kwargs["config"] = config
        
        self.s3_client = self.session.client("s3", **client_kwargs)
        self.s3_resource = self.session.resource("s3", **client_kwargs)
        self.cloudwatch_client = self.session.client("cloudwatch", **client_kwargs)
        
        self._upload_parts_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()
    
    # =========================================================================
    # Bucket Management
    # =========================================================================
    
    def create_bucket(
        self,
        config: BucketConfig,
        enable_accelerate: bool = False
    ) -> Dict[str, Any]:
        """
        Create an S3 bucket with configuration.
        
        Args:
            config: Bucket configuration
            enable_accelerate: Enable S3 Transfer Acceleration
            
        Returns:
            Bucket creation response
        """
        try:
            create_params = {
                "Bucket": config.name
            }
            
            if config.region and config.region != "us-east-1":
                create_params["CreateBucketConfiguration"] = {
                    "LocationConstraint": config.region
                }
            
            if config.acl not in ("private", "BucketOwnerPreferred"):
                create_params["ObjectOwnership"] = config.object_ownership
            
            self.s3_client.create_bucket(**create_params)
            
            if enable_accelerate:
                self.s3_client.put_bucket_accelerate_configuration(
                    Bucket=config.name,
                    AccelerateConfiguration={"Status": "Enabled"}
                )
            
            if config.encryption_enabled:
                self.set_bucket_encryption(
                    config.name,
                    config.encryption_type,
                    config.kms_key_id
                )
            
            if config.versioning_enabled:
                self.enable_bucket_versioning(config.name)
            
            if config.tags:
                self.put_bucket_tags(config.name, config.tags)
            
            if config.lifecycle_config:
                self.put_bucket_lifecycle(config.name, config.lifecycle_config)
            
            if config.cors_config:
                self.put_bucket_cors(config.name, config.cors_config)
            
            if config.website_config:
                self.configure_static_website(config.name, config.website_config)
            
            public_access_block = {
                "BlockPublicAcls": config.block_public_acls,
                "IgnorePublicAcls": config.ignore_public_acls,
                "BlockPublicPolicy": config.block_public_policy,
                "RestrictPublicBuckets": config.restrict_public_buckets
            }
            self.s3_client.put_public_access_block(
                Bucket=config.name,
                PublicAccessBlockConfiguration=public_access_block
            )
            
            logger.info(f"Created bucket: {config.name}")
            return {"Bucket": config.name, "Status": "Created"}
            
        except ClientError as e:
            logger.error(f"Failed to create bucket {config.name}: {e}")
            raise
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """
        List all S3 buckets.
        
        Returns:
            List of bucket information
        """
        try:
            response = self.s3_client.list_buckets()
            return response.get("Buckets", [])
        except ClientError as e:
            logger.error(f"Failed to list buckets: {e}")
            raise
    
    def get_bucket_location(self, bucket: str) -> str:
        """
        Get bucket region.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Region name
        """
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket)
            return response.get("LocationConstraint") or "us-east-1"
        except ClientError as e:
            logger.error(f"Failed to get bucket location: {e}")
            raise
    
    def delete_bucket(self, bucket: str, force: bool = False) -> Dict[str, Any]:
        """
        Delete an S3 bucket.
        
        Args:
            bucket: Bucket name
            force: Force delete even if bucket has objects
            
        Returns:
            Deletion response
        """
        try:
            if force:
                self._empty_bucket(bucket)
            
            self.s3_client.delete_bucket(Bucket=bucket)
            logger.info(f"Deleted bucket: {bucket}")
            return {"Bucket": bucket, "Status": "Deleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete bucket {bucket}: {e}")
            raise
    
    def _empty_bucket(self, bucket: str) -> None:
        """Empty all objects from a bucket."""
        try:
            paginator = self.s3_client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=bucket):
                delete_markers = page.get("DeleteMarkers", [])
                versions = page.get("Versions", [])
                
                objects_to_delete = []
                for obj in delete_markers + versions:
                    objects_to_delete.append({
                        "Key": obj["Key"],
                        "VersionId": obj["VersionId"]
                    })
                
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": objects_to_delete}
                    )
        except ClientError as e:
            logger.error(f"Failed to empty bucket {bucket}: {e}")
            raise
    
    def get_bucket_info(self, bucket: str) -> Dict[str, Any]:
        """
        Get bucket information and configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Bucket information dictionary
        """
        try:
            info = {
                "name": bucket,
                "region": self.get_bucket_location(bucket)
            }
            
            info["versioning"] = self.get_bucket_versioning(bucket)
            info["encryption"] = self.get_bucket_encryption(bucket)
            info["policy"] = self.get_bucket_policy(bucket)
            info["lifecycle"] = self.get_bucket_lifecycle(bucket)
            info["cors"] = self.get_bucket_cors(bucket)
            info["website"] = self.get_bucket_website(bucket)
            info["tags"] = self.get_bucket_tags(bucket)
            info["public_access_block"] = self.get_public_access_block(bucket)
            
            return info
            
        except ClientError as e:
            logger.error(f"Failed to get bucket info: {e}")
            raise
    
    # =========================================================================
    # Object Operations
    # =========================================================================
    
    def upload_object(self, config: UploadConfig) -> Dict[str, Any]:
        """
        Upload an object to S3.
        
        Args:
            config: Upload configuration
            
        Returns:
            Upload response with ETag and version ID
        """
        try:
            extra_args = {}
            
            if config.content_type:
                extra_args["ContentType"] = config.content_type
            elif config.key:
                guessed_type, _ = mimetypes.guess_type(config.key)
                if guessed_type:
                    extra_args["ContentType"] = guessed_type
            
            if config.metadata:
                extra_args["Metadata"] = config.metadata
            
            if config.tags:
                extra_args["Tagging"] = "&".join(
                    f"{k}={v}" for k, v in config.tags.items()
                )
            
            if config.cache_control:
                extra_args["CacheControl"] = config.cache_control
            
            if config.content_disposition:
                extra_args["ContentDisposition"] = config.content_disposition
            
            if config.content_encoding:
                extra_args["ContentEncoding"] = config.content_encoding
            
            if config.content_language:
                extra_args["ContentLanguage"] = config.content_language
            
            if config.expires:
                extra_args["Expires"] = config.expires
            
            if config.storage_class != StorageClass.STANDARD:
                extra_args["StorageClass"] = config.storage_class.value
            
            if config.encryption != EncryptionType.NONE:
                if config.encryption == EncryptionType.SSE_S3:
                    extra_args["ServerSideEncryption"] = "AES256"
                elif config.encryption == EncryptionType.SSE_KMS:
                    extra_args["ServerSideEncryption"] = "aws:kms"
                    if config.kms_key_id:
                        extra_args["SSEKMSKeyId"] = config.kms_key_id
            
            if isinstance(config.data, (str, bytes)):
                if isinstance(config.data, str):
                    data = config.data.encode("utf-8")
                else:
                    data = config.data
            else:
                data = config.data
            
            response = self.s3_client.put_object(
                Bucket=config.bucket,
                Key=config.key,
                Body=data,
                **extra_args
            )
            
            logger.debug(f"Uploaded object: {config.bucket}/{config.key}")
            return {
                "Bucket": config.bucket,
                "Key": config.key,
                "ETag": response.get("ETag"),
                "VersionId": response.get("VersionId"),
                "Status": "Uploaded"
            }
            
        except ClientError as e:
            logger.error(f"Failed to upload {config.key}: {e}")
            raise
    
    def download_object(
        self,
        bucket: str,
        key: str,
        output_path: Optional[str] = None,
        version_id: Optional[str] = None
    ) -> Union[bytes, str]:
        """
        Download an object from S3.
        
        Args:
            bucket: Bucket name
            key: Object key
            output_path: Local file path to save (optional)
            version_id: Specific version to download
            
        Returns:
            Object data as bytes or file path if output_path specified
        """
        try:
            params = {"Bucket": bucket, "Key": key}
            if version_id:
                params["VersionId"] = version_id
            
            response = self.s3_client.get_object(**params)
            
            if output_path:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "wb") as f:
                    for chunk in response["Body"].iter_chunks():
                        f.write(chunk)
                logger.debug(f"Downloaded object to: {output_path}")
                return output_path
            else:
                data = response["Body"].read()
                logger.debug(f"Downloaded object: {bucket}/{key}")
                return data
                
        except ClientError as e:
            logger.error(f"Failed to download {bucket}/{key}: {e}")
            raise
    
    def delete_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
        silent: bool = False
    ) -> Dict[str, Any]:
        """
        Delete an object from S3.
        
        Args:
            bucket: Bucket name
            key: Object key
            version_id: Specific version to delete
            silent: Suppress error if object doesn't exist
            
        Returns:
            Deletion response
        """
        try:
            params = {"Bucket": bucket, "Key": key}
            if version_id:
                params["VersionId"] = version_id
            
            response = self.s3_client.delete_object(**params)
            
            if not silent:
                logger.info(f"Deleted object: {bucket}/{key}")
            
            return {
                "Bucket": bucket,
                "Key": key,
                "VersionId": response.get("VersionId"),
                "DeleteMarker": response.get("DeleteMarker"),
                "Status": "Deleted"
            }
            
        except ClientError as e:
            if silent and e.response["Error"]["Code"] == "NoSuchKey":
                return {"Bucket": bucket, "Key": key, "Status": "NotFound"}
            logger.error(f"Failed to delete {bucket}/{key}: {e}")
            raise
    
    def delete_objects_batch(
        self,
        bucket: str,
        keys: List[str],
        quiet: bool = True
    ) -> Dict[str, Any]:
        """
        Delete multiple objects in a batch.
        
        Args:
            bucket: Bucket name
            keys: List of object keys
            quiet: Use quiet mode
            
        Returns:
            Deletion results
        """
        try:
            objects = [{"Key": key} for key in keys]
            
            response = self.s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects, "Quiet": quiet}
            )
            
            return {
                "Deleted": response.get("Deleted", []),
                "Errors": response.get("Errors", []),
                "Status": "Completed"
            }
            
        except ClientError as e:
            logger.error(f"Failed to batch delete objects: {e}")
            raise
    
    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
        include_versions: bool = False
    ) -> Dict[str, Any]:
        """
        List objects in a bucket.
        
        Args:
            bucket: Bucket name
            prefix: Filter by prefix
            max_keys: Maximum number of keys to return
            continuation_token: Token for pagination
            include_versions: Include version IDs
            
        Returns:
            List of objects and pagination info
        """
        try:
            params = {
                "Bucket": bucket,
                "Prefix": prefix,
                "MaxKeys": max_keys
            }
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            
            if include_versions:
                response = self.s3_client.list_object_versions(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=max_keys,
                    **({"VersionIdMarker": continuation_token} if continuation_token else {})
                )
                objects = response.get("Versions", [])
                delete_markers = response.get("DeleteMarkers", [])
                objects.extend(delete_markers)
            else:
                response = self.s3_client.list_objects_v2(**params)
                objects = response.get("Contents", [])
            
            return {
                "Objects": objects,
                "IsTruncated": response.get("IsTruncated", False),
                "NextContinuationToken": response.get("NextContinuationToken"),
                "KeyCount": response.get("KeyCount", 0),
                "MaxKeys": response.get("MaxKeys", max_keys)
            }
            
        except ClientError as e:
            logger.error(f"Failed to list objects in {bucket}: {e}")
            raise
    
    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        metadata: Optional[Dict[str, str]] = None,
        metadata_directive: MetadataDirective = MetadataDirective.COPY,
        storage_class: StorageClass = StorageClass.STANDARD,
        encryption: EncryptionType = EncryptionType.SSE_S3,
        kms_key_id: Optional[str] = None,
        source_version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Copy an object within or between buckets.
        
        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key
            metadata: New metadata (if replacing)
            metadata_directive: COPY or REPLACE metadata
            storage_class: Storage class for destination
            encryption: Encryption type
            kms_key_id: KMS key ID (for SSE-KMS)
            source_version_id: Source version ID
            
        Returns:
            Copy response with ETag and version ID
        """
        try:
            copy_source = {
                "Bucket": source_bucket,
                "Key": source_key
            }
            if source_version_id:
                copy_source["VersionId"] = source_version_id
            
            extra_args = {
                "MetadataDirective": metadata_directive.value,
                "StorageClass": storage_class.value
            }
            
            if metadata and metadata_directive == MetadataDirective.REPLACE:
                extra_args["Metadata"] = metadata
            
            if encryption != EncryptionType.NONE:
                if encryption == EncryptionType.SSE_S3:
                    extra_args["ServerSideEncryption"] = "AES256"
                elif encryption == EncryptionType.SSE_KMS:
                    extra_args["ServerSideEncryption"] = "aws:kms"
                    if kms_key_id:
                        extra_args["SSEKMSKeyId"] = kms_key_id
            
            response = self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                **extra_args
            )
            
            logger.debug(f"Copied {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")
            return {
                "Source": f"{source_bucket}/{source_key}",
                "Destination": f"{dest_bucket}/{dest_key}",
                "ETag": response.get("ETag"),
                "VersionId": response.get("VersionId"),
                "Status": "Copied"
            }
            
        except ClientError as e:
            logger.error(f"Failed to copy {source_bucket}/{source_key}: {e}")
            raise
    
    def get_object_metadata(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get object metadata without downloading.
        
        Args:
            bucket: Bucket name
            key: Object key
            version_id: Specific version ID
            
        Returns:
            Object metadata
        """
        try:
            params = {"Bucket": bucket, "Key": key}
            if version_id:
                params["VersionId"] = version_id
            
            response = self.s3_client.head_object(**params)
            
            metadata = {
                "ContentLength": response.get("ContentLength"),
                "ContentType": response.get("ContentType"),
                "ETag": response.get("ETag"),
                "LastModified": response.get("LastModified"),
                "Metadata": response.get("Metadata", {}),
                "StorageClass": response.get("StorageClass"),
                "VersionId": response.get("VersionId"),
                "Encryption": response.get("ServerSideEncryption"),
                "Tags": {}
            }
            
            if "x-amz-tagging-count" in response:
                metadata["TagCount"] = int(response["x-amz-tagging-count"])
            
            return metadata
            
        except ClientError as e:
            logger.error(f"Failed to get metadata for {bucket}/{key}: {e}")
            raise
    
    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        expiration: int = 3600,
        method: str = "GET",
        version_id: Optional[str] = None
    ) -> str:
        """
        Generate a presigned URL for object access.
        
        Args:
            bucket: Bucket name
            key: Object key
            expiration: URL expiration time in seconds
            method: HTTP method (GET, PUT, DELETE)
            version_id: Specific version ID
            
        Returns:
            Presigned URL
        """
        try:
            params = {"Bucket": bucket, "Key": key}
            if version_id:
                params["VersionId"] = version_id
            
            url = self.s3_client.generate_presigned_url(
                method,
                Params=params,
                ExpiresIn=expiration
            )
            
            return url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise
    
    def generate_presigned_post(
        self,
        bucket: str,
        key: str,
        expiration: int = 3600,
        conditions: Optional[List] = None,
        fields: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Generate a presigned POST policy for browser uploads.
        
        Args:
            bucket: Bucket name
            key: Object key pattern (can include ${filename})
            expiration: Policy expiration in seconds
            conditions: Additional conditions
            fields: Fixed form fields
            
        Returns:
            Dictionary with url and fields
        """
        try:
            conditions = conditions or []
            fields = fields or {}
            
            policy = {
                "expiration": (datetime.utcnow() + timedelta(seconds=expiration)).isoformat()
            }
            
            all_conditions = [
                {"bucket": bucket},
                {"key": key}
            ]
            for cond in conditions:
                all_conditions.append(cond)
            
            if fields:
                all_conditions.append(fields)
            
            response = self.s3_client.generate_presigned_post(
                Bucket=bucket,
                Key=key,
                Fields=fields,
                Conditions=all_conditions,
                ExpiresIn=expiration
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned POST: {e}")
            raise
    
    # =========================================================================
    # Multipart Upload
    # =========================================================================
    
    def initiate_multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Initiate a multipart upload.
        
        Args:
            bucket: Bucket name
            key: Object key
            content_type: Content type
            metadata: Object metadata
            
        Returns:
            Upload ID
        """
        try:
            params = {"Bucket": bucket, "Key": key}
            if content_type:
                params["ContentType"] = content_type
            if metadata:
                params["Metadata"] = metadata
            
            response = self.s3_client.create_multipart_upload(**params)
            upload_id = response["UploadId"]
            
            cache_key = f"{bucket}/{key}"
            self._upload_parts_cache[cache_key] = []
            
            logger.debug(f"Initiated multipart upload: {cache_key}, ID: {upload_id}")
            return upload_id
            
        except ClientError as e:
            logger.error(f"Failed to initiate multipart upload: {e}")
            raise
    
    def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        data: Union[bytes, BinaryIO]
    ) -> Dict[str, Any]:
        """
        Upload a single part in multipart upload.
        
        Args:
            bucket: Bucket name
            key: Object key
            upload_id: Upload ID
            part_number: Part number (1-10000)
            data: Part data
            
        Returns:
            ETag for the uploaded part
        """
        try:
            if isinstance(data, bytes):
                data_stream = data
            else:
                data_stream = data.read()
            
            response = self.s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data_stream
            )
            
            etag = response["ETag"]
            
            cache_key = f"{bucket}/{key}"
            if cache_key not in self._upload_parts_cache:
                self._upload_parts_cache[cache_key] = []
            self._upload_parts_cache[cache_key].append({
                "PartNumber": part_number,
                "ETag": etag
            })
            
            logger.debug(f"Uploaded part {part_number} for {bucket}/{key}")
            return {"PartNumber": part_number, "ETag": etag}
            
        except ClientError as e:
            logger.error(f"Failed to upload part {part_number}: {e}")
            raise
    
    def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str
    ) -> Dict[str, Any]:
        """
        Complete a multipart upload.
        
        Args:
            bucket: Bucket name
            key: Object key
            upload_id: Upload ID
            
        Returns:
            Completion response
        """
        try:
            cache_key = f"{bucket}/{key}"
            parts = self._upload_parts_cache.get(cache_key, [])
            
            if not parts:
                raise ValueError(f"No parts found for upload {upload_id}")
            
            parts_sorted = sorted(parts, key=lambda x: x["PartNumber"])
            
            response = self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts_sorted}
            )
            
            if cache_key in self._upload_parts_cache:
                del self._upload_parts_cache[cache_key]
            
            logger.info(f"Completed multipart upload: {bucket}/{key}")
            return {
                "Bucket": bucket,
                "Key": key,
                "ETag": response.get("ETag"),
                "VersionId": response.get("VersionId"),
                "Location": response.get("Location"),
                "Status": "Completed"
            }
            
        except ClientError as e:
            logger.error(f"Failed to complete multipart upload: {e}")
            raise
    
    def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str
    ) -> Dict[str, Any]:
        """
        Abort a multipart upload.
        
        Args:
            bucket: Bucket name
            key: Object key
            upload_id: Upload ID
            
        Returns:
            Abort response
        """
        try:
            self.s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            
            cache_key = f"{bucket}/{key}"
            if cache_key in self._upload_parts_cache:
                del self._upload_parts_cache[cache_key]
            
            logger.info(f"Aborted multipart upload: {bucket}/{key}")
            return {
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "Status": "Aborted"
            }
            
        except ClientError as e:
            logger.error(f"Failed to abort multipart upload: {e}")
            raise
    
    def list_multipart_uploads(
        self,
        bucket: str,
        prefix: str = ""
    ) -> List[Dict[str, Any]]:
        """
        List in-progress multipart uploads.
        
        Args:
            bucket: Bucket name
            prefix: Filter by prefix
            
        Returns:
            List of multipart uploads
        """
        try:
            response = self.s3_client.list_multipart_uploads(
                Bucket=bucket,
                Prefix=prefix
            )
            
            uploads = response.get("Uploads", [])
            return uploads
            
        except ClientError as e:
            logger.error(f"Failed to list multipart uploads: {e}")
            raise
    
    def multipart_upload_file(self, config: MultipartUploadConfig) -> Dict[str, Any]:
        """
        Upload a large file using multipart upload.
        
        Args:
            config: Multipart upload configuration
            
        Returns:
            Upload response
        """
        try:
            if config.file_path:
                file_size = os.path.getsize(config.file_path)
                
                if file_size < config.threshold:
                    with open(config.file_path, "rb") as f:
                        data = f.read()
                    upload_config = UploadConfig(
                        bucket=config.bucket,
                        key=config.key,
                        data=data,
                        content_type=config.content_type,
                        storage_class=config.storage_class,
                        encryption=config.encryption,
                        kms_key_id=config.kms_key_id,
                        metadata=config.metadata,
                        tags=config.tags
                    )
                    return self.upload_object(upload_config)
                
                upload_id = self.initiate_multipart_upload(
                    config.bucket,
                    config.key,
                    config.content_type,
                    config.metadata
                )
                
                parts = []
                part_number = 1
                
                with open(config.file_path, "rb") as f:
                    while True:
                        chunk = f.read(config.part_size)
                        if not chunk:
                            break
                        
                        result = self.upload_part(
                            config.bucket,
                            config.key,
                            upload_id,
                            part_number,
                            chunk
                        )
                        parts.append(result)
                        part_number += 1
                
                parts_sorted = sorted(parts, key=lambda x: x["PartNumber"])
                
                response = self.s3_client.complete_multipart_upload(
                    Bucket=config.bucket,
                    Key=config.key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts_sorted}
                )
                
                logger.info(f"Multipart upload completed: {config.bucket}/{config.key}")
                return {
                    "Bucket": config.bucket,
                    "Key": config.key,
                    "ETag": response.get("ETag"),
                    "VersionId": response.get("VersionId"),
                    "Status": "Completed"
                }
            elif config.data:
                upload_id = self.initiate_multipart_upload(
                    config.bucket,
                    config.key,
                    config.content_type,
                    config.metadata
                )
                
                data_len = len(config.data)
                parts = []
                part_number = 1
                offset = 0
                
                while offset < data_len:
                    chunk = config.data[offset:offset + config.part_size]
                    result = self.upload_part(
                        config.bucket,
                        config.key,
                        upload_id,
                        part_number,
                        chunk
                    )
                    parts.append(result)
                    part_number += 1
                    offset += config.part_size
                
                parts_sorted = sorted(parts, key=lambda x: x["PartNumber"])
                
                response = self.s3_client.complete_multipart_upload(
                    Bucket=config.bucket,
                    Key=config.key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts_sorted}
                )
                
                return {
                    "Bucket": config.bucket,
                    "Key": config.key,
                    "ETag": response.get("ETag"),
                    "VersionId": response.get("VersionId"),
                    "Status": "Completed"
                }
            else:
                raise ValueError("Either file_path or data must be provided")
            
        except ClientError as e:
            logger.error(f"Multipart upload failed: {e}")
            raise
    
    # =========================================================================
    # Bucket Policies
    # =========================================================================
    
    def put_bucket_policy(
        self,
        bucket: str,
        policy: Union[Dict, str],
        validate: bool = True
    ) -> Dict[str, Any]:
        """
        Set or update bucket policy.
        
        Args:
            bucket: Bucket name
            policy: Policy document (dict or JSON string)
            validate: Validate policy before applying
            
        Returns:
            Response
        """
        try:
            if isinstance(policy, dict):
                policy_str = json.dumps(policy)
            else:
                policy_str = policy
                if validate:
                    json.loads(policy_str)
            
            self.s3_client.put_bucket_policy(
                Bucket=bucket,
                Policy=policy_str
            )
            
            logger.info(f"Updated policy for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "PolicyUpdated"}
            
        except ClientError as e:
            logger.error(f"Failed to put bucket policy: {e}")
            raise
    
    def get_bucket_policy(self, bucket: str) -> Dict[str, Any]:
        """
        Get bucket policy.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Policy document
        """
        try:
            response = self.s3_client.get_bucket_policy(Bucket=bucket)
            policy_str = response.get("Policy", "{}")
            return json.loads(policy_str)
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
                return {}
            logger.error(f"Failed to get bucket policy: {e}")
            raise
    
    def delete_bucket_policy(self, bucket: str) -> Dict[str, Any]:
        """
        Delete bucket policy.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_bucket_policy(Bucket=bucket)
            logger.info(f"Deleted policy for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "PolicyDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete bucket policy: {e}")
            raise
    
    def create_policy_statement(
        self,
        effect: str = "Allow",
        principal: Union[str, Dict, List] = "*",
        actions: Optional[List[str]] = None,
        not_actions: Optional[List[str]] = None,
        resources: Optional[List[str]] = None,
        not_resources: Optional[List[str]] = None,
        conditions: Optional[Dict[str, Any]] = None,
        sid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an IAM policy statement.
        
        Args:
            effect: Allow or Deny
            principal: Who the policy applies to
            actions: Allowed actions
            not_actions: Actions to exclude
            resources: Resource ARNs
            not_resources: Resources to exclude
            conditions: Condition expressions
            sid: Statement ID
            
        Returns:
            Policy statement
        """
        statement = {"Effect": effect}
        
        if sid:
            statement["Sid"] = sid
        
        if isinstance(principal, str):
            statement["Principal"] = principal
        else:
            statement["Principal"] = principal
        
        if actions:
            statement["Action"] = actions
        
        if not_actions:
            statement["NotAction"] = not_actions
        
        if resources:
            statement["Resource"] = resources
        
        if not_resources:
            statement["NotResource"] = not_resources
        
        if conditions:
            statement["Condition"] = conditions
        
        return statement
    
    # =========================================================================
    # Lifecycle Rules
    # =========================================================================
    
    def put_bucket_lifecycle(
        self,
        bucket: str,
        rules: Union[List[LifecycleRule], List[Dict]]
    ) -> Dict[str, Any]:
        """
        Configure bucket lifecycle rules.
        
        Args:
            bucket: Bucket name
            rules: Lifecycle rules
            
        Returns:
            Response
        """
        try:
            lifecycle_config = {"Rules": []}
            
            for rule in rules:
                if isinstance(rule, dict):
                    rule_dict = rule.copy()
                else:
                    rule_dict = self._lifecycle_rule_to_dict(rule)
                
                lifecycle_config["Rules"].append(rule_dict)
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration=lifecycle_config
            )
            
            logger.info(f"Updated lifecycle for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "LifecycleUpdated"}
            
        except ClientError as e:
            logger.error(f"Failed to put lifecycle: {e}")
            raise
    
    def _lifecycle_rule_to_dict(self, rule: LifecycleRule) -> Dict[str, Any]:
        """Convert LifecycleRule to dictionary."""
        rule_dict = {
            "ID": rule.id,
            "Status": "Enabled" if rule.enabled else "Disabled",
            "Filter": {}
        }
        
        if rule.prefix:
            rule_dict["Filter"]["Prefix"] = rule.prefix
        elif rule.tags:
            rule_dict["Filter"]["Tag"] = rule.tags
        
        if rule.expiration_days:
            rule_dict["Expiration"] = {"Days": rule.expiration_days}
        elif rule.expiration_date:
            rule_dict["Expiration"] = {"Date": rule.expiration_date.isoformat()}
        
        if rule.transition_days:
            transition = {"Days": rule.transition_days}
            if rule.transition_storage_class:
                transition["StorageClass"] = rule.transition_storage_class.value
            rule_dict["Transitions"] = [transition]
        elif rule.transition_date:
            transition = {"Date": rule.transition_date.isoformat()}
            if rule.transition_storage_class:
                transition["StorageClass"] = rule.transition_storage_class.value
            rule_dict["Transitions"] = [transition]
        
        if rule.noncurrent_expiration_days:
            rule_dict["NoncurrentVersionExpiration"] = {
                "NoncurrentDays": rule.noncurrent_expiration_days
            }
        
        if rule.noncurrent_transition_days:
            nc_transition = {"NoncurrentDays": rule.noncurrent_transition_days}
            if rule.noncurrent_transition_storage_class:
                nc_transition["StorageClass"] = rule.noncurrent_transition_storage_class.value
            rule_dict["NoncurrentVersionTransitions"] = [nc_transition]
        
        if rule.abort_incomplete_days:
            rule_dict["AbortIncompleteMultipartUpload"] = {
                "DaysAfterInitiation": rule.abort_incomplete_days
            }
        
        return rule_dict
    
    def get_bucket_lifecycle(self, bucket: str) -> List[Dict[str, Any]]:
        """
        Get bucket lifecycle rules.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Lifecycle rules
        """
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
            return response.get("Rules", [])
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                return []
            logger.error(f"Failed to get lifecycle: {e}")
            raise
    
    def delete_bucket_lifecycle(self, bucket: str) -> Dict[str, Any]:
        """
        Delete bucket lifecycle configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_bucket_lifecycle(Bucket=bucket)
            logger.info(f"Deleted lifecycle for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "LifecycleDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete lifecycle: {e}")
            raise
    
    # =========================================================================
    # Versioning
    # =========================================================================
    
    def enable_bucket_versioning(self, bucket: str) -> Dict[str, Any]:
        """
        Enable bucket versioning.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={"Status": "Enabled"}
            )
            
            logger.info(f"Enabled versioning for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "VersioningEnabled"}
            
        except ClientError as e:
            logger.error(f"Failed to enable versioning: {e}")
            raise
    
    def suspend_bucket_versioning(self, bucket: str) -> Dict[str, Any]:
        """
        Suspend bucket versioning.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={"Status": "Suspended"}
            )
            
            logger.info(f"Suspended versioning for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "VersioningSuspended"}
            
        except ClientError as e:
            logger.error(f"Failed to suspend versioning: {e}")
            raise
    
    def get_bucket_versioning(self, bucket: str) -> Dict[str, Any]:
        """
        Get bucket versioning configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Versioning configuration
        """
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket)
            return {
                "Status": response.get("Status"),
                "MFADelete": response.get("MFADelete")
            }
            
        except ClientError as e:
            logger.error(f"Failed to get versioning: {e}")
            raise
    
    def list_object_versions(
        self,
        bucket: str,
        prefix: str = ""
    ) -> List[Dict[str, Any]]:
        """
        List all object versions in bucket.
        
        Args:
            bucket: Bucket name
            prefix: Filter by prefix
            
        Returns:
            List of versions
        """
        try:
            paginator = self.s3_client.get_paginator("list_object_versions")
            versions = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                page_versions = page.get("Versions", [])
                versions.extend(page_versions)
            
            return versions
            
        except ClientError as e:
            logger.error(f"Failed to list versions: {e}")
            raise
    
    # =========================================================================
    # Encryption
    # =========================================================================
    
    def set_bucket_encryption(
        self,
        bucket: str,
        encryption_type: EncryptionType,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Configure bucket encryption.
        
        Args:
            bucket: Bucket name
            encryption_type: Encryption type
            kms_key_id: KMS key ID (for SSE-KMS)
            
        Returns:
            Response
        """
        try:
            if encryption_type == EncryptionType.NONE:
                self.s3_client.delete_bucket_encryption(Bucket=bucket)
                return {"Bucket": bucket, "Status": "EncryptionDisabled"}
            
            if encryption_type == EncryptionType.SSE_S3:
                config = {
                    "ServerSideEncryptionConfiguration": {
                        "Rules": [
                            {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                        ]
                    }
                }
            elif encryption_type == EncryptionType.SSE_KMS:
                rule = {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"}
                }
                if kms_key_id:
                    rule["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"] = kms_key_id
                config = {
                    "ServerSideEncryptionConfiguration": {
                        "Rules": [rule]
                    }
                }
            else:
                raise ValueError(f"Unsupported encryption type: {encryption_type}")
            
            self.s3_client.put_bucket_encryption(
                Bucket=bucket,
                ServerSideEncryptionConfiguration=config["ServerSideEncryptionConfiguration"]
            )
            
            logger.info(f"Set encryption for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "EncryptionConfigured"}
            
        except ClientError as e:
            logger.error(f"Failed to set encryption: {e}")
            raise
    
    def get_bucket_encryption(self, bucket: str) -> Dict[str, Any]:
        """
        Get bucket encryption configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Encryption configuration
        """
        try:
            response = self.s3_client.get_bucket_encryption(Bucket=bucket)
            rules = response.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
            
            if not rules:
                return {"Status": "Disabled"}
            
            default_rule = rules[0].get("ApplyServerSideEncryptionByDefault", {})
            return {
                "Status": "Enabled",
                "Algorithm": default_rule.get("SSEAlgorithm"),
                "KMSKeyId": default_rule.get("KMSMasterKeyID")
            }
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ServerSideEncryptionConfigurationNotFoundError":
                return {"Status": "Disabled"}
            logger.error(f"Failed to get encryption: {e}")
            raise
    
    # =========================================================================
    # CORS
    # =========================================================================
    
    def put_bucket_cors(
        self,
        bucket: str,
        cors_rules: Union[List[CorsRule], List[Dict]]
    ) -> Dict[str, Any]:
        """
        Configure bucket CORS rules.
        
        Args:
            bucket: Bucket name
            cors_rules: CORS rules
            
        Returns:
            Response
        """
        try:
            cors_configuration = {"CORSRules": []}
            
            for rule in cors_rules:
                if isinstance(rule, dict):
                    rule_dict = rule.copy()
                else:
                    rule_dict = {
                        "AllowedOrigins": rule.allowed_origins,
                        "AllowedMethods": rule.allowed_methods,
                        "AllowedHeaders": rule.allowed_headers,
                        "MaxAgeSeconds": rule.max_age_seconds,
                        "ExposeHeaders": rule.expose_headers
                    }
                
                cors_configuration["CORSRules"].append(rule_dict)
            
            self.s3_client.put_bucket_cors(
                Bucket=bucket,
                CORSConfiguration=cors_configuration
            )
            
            logger.info(f"Set CORS for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "CORSConfigured"}
            
        except ClientError as e:
            logger.error(f"Failed to set CORS: {e}")
            raise
    
    def get_bucket_cors(self, bucket: str) -> List[Dict[str, Any]]:
        """
        Get bucket CORS configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            CORS rules
        """
        try:
            response = self.s3_client.get_bucket_cors(Bucket=bucket)
            return response.get("CORSRules", [])
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchCORSConfiguration":
                return []
            logger.error(f"Failed to get CORS: {e}")
            raise
    
    def delete_bucket_cors(self, bucket: str) -> Dict[str, Any]:
        """
        Delete bucket CORS configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_bucket_cors(Bucket=bucket)
            logger.info(f"Deleted CORS for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "CORSDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete CORS: {e}")
            raise
    
    # =========================================================================
    # Static Website Hosting
    # =========================================================================
    
    def configure_static_website(
        self,
        bucket: str,
        config: Union[WebsiteConfig, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Configure static website hosting.
        
        Args:
            bucket: Bucket name
            config: Website configuration
            
        Returns:
            Response
        """
        try:
            if isinstance(config, dict):
                website_config = config
            else:
                website_config = {
                    "IndexDocument": {"Suffix": config.index_document}
                }
                if config.error_document:
                    website_config["ErrorDocument"] = {"Key": config.error_document}
                if config.redirect_all_requests_to:
                    website_config["RedirectAllRequestsTo"] = config.redirect_all_requests_to
                if config.routing_rules:
                    website_config["RoutingRules"] = config.routing_rules
            
            self.s3_client.put_bucket_website(
                Bucket=bucket,
                WebsiteConfiguration=website_config
            )
            
            logger.info(f"Configured website hosting for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "WebsiteConfigured"}
            
        except ClientError as e:
            logger.error(f"Failed to configure website: {e}")
            raise
    
    def get_bucket_website(self, bucket: str) -> Dict[str, Any]:
        """
        Get bucket website configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Website configuration
        """
        try:
            response = self.s3_client.get_bucket_website(Bucket=bucket)
            return response
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchWebsiteConfiguration":
                return {}
            logger.error(f"Failed to get website config: {e}")
            raise
    
    def delete_bucket_website(self, bucket: str) -> Dict[str, Any]:
        """
        Delete bucket website configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_bucket_website(Bucket=bucket)
            logger.info(f"Deleted website config for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "WebsiteDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete website config: {e}")
            raise
    
    def get_bucket_website_endpoint(self, bucket: str) -> str:
        """
        Get static website hosting endpoint.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Website endpoint URL
        """
        region = self.get_bucket_location(bucket)
        
        if region == "us-east-1":
            return f"http://{bucket}.s3-website.us-east-1.amazonaws.com"
        else:
            return f"http://{bucket}.s3-website.{region}.amazonaws.com"
    
    # =========================================================================
    # Public Access Block
    # =========================================================================
    
    def put_public_access_block(
        self,
        bucket: str,
        block_public_acls: bool = True,
        ignore_public_acls: bool = True,
        block_public_policy: bool = True,
        restrict_public_buckets: bool = True
    ) -> Dict[str, Any]:
        """
        Configure public access block.
        
        Args:
            bucket: Bucket name
            block_public_acls: Block public ACLs
            ignore_public_acls: Ignore public ACLs
            block_public_policy: Block public policies
            restrict_public_buckets: Restrict public buckets
            
        Returns:
            Response
        """
        try:
            self.s3_client.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": block_public_acls,
                    "IgnorePublicAcls": ignore_public_acls,
                    "BlockPublicPolicy": block_public_policy,
                    "RestrictPublicBuckets": restrict_public_buckets
                }
            )
            
            logger.info(f"Set public access block for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "PublicAccessBlockConfigured"}
            
        except ClientError as e:
            logger.error(f"Failed to set public access block: {e}")
            raise
    
    def get_public_access_block(self, bucket: str) -> Dict[str, Any]:
        """
        Get public access block configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Public access block configuration
        """
        try:
            response = self.s3_client.get_public_access_block(Bucket=bucket)
            return response.get("PublicAccessBlockConfiguration", {})
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration":
                return {}
            logger.error(f"Failed to get public access block: {e}")
            raise
    
    def delete_public_access_block(self, bucket: str) -> Dict[str, Any]:
        """
        Delete public access block.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_public_access_block(Bucket=bucket)
            logger.info(f"Deleted public access block for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "PublicAccessBlockDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete public access block: {e}")
            raise
    
    # =========================================================================
    # Tags
    # =========================================================================
    
    def put_bucket_tags(self, bucket: str, tags: Dict[str, str]) -> Dict[str, Any]:
        """
        Set bucket tags.
        
        Args:
            bucket: Bucket name
            tags: Tag key-value pairs
            
        Returns:
            Response
        """
        try:
            tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            self.s3_client.put_bucket_tagging(
                Bucket=bucket,
                Tagging={"TagSet": tag_set}
            )
            
            logger.info(f"Set tags for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "TagsSet"}
            
        except ClientError as e:
            logger.error(f"Failed to set tags: {e}")
            raise
    
    def get_bucket_tags(self, bucket: str) -> Dict[str, str]:
        """
        Get bucket tags.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Tag dictionary
        """
        try:
            response = self.s3_client.get_bucket_tagging(Bucket=bucket)
            tags = {}
            for tag in response.get("TagSet", []):
                tags[tag["Key"]] = tag["Value"]
            return tags
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchTagSet":
                return {}
            logger.error(f"Failed to get tags: {e}")
            raise
    
    def delete_bucket_tags(self, bucket: str) -> Dict[str, Any]:
        """
        Delete all bucket tags.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.delete_bucket_tagging(Bucket=bucket)
            logger.info(f"Deleted tags for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "TagsDeleted"}
            
        except ClientError as e:
            logger.error(f"Failed to delete tags: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def put_bucket_metrics(
        self,
        bucket: str,
        metrics: List[str] = None
    ) -> Dict[str, Any]:
        """
        Configure bucket metrics configuration for CloudWatch.
        
        Args:
            bucket: Bucket name
            metrics: List of metrics (e.g., ['BucketSizeBytes', 'ObjectCount'])
            
        Returns:
            Response
        """
        try:
            if metrics is None:
                metrics = ["BucketSizeBytes", "ObjectCount"]
            
            for metric in metrics:
                self.s3_client.put_bucket_metrics_configuration(
                    Bucket=bucket,
                    Id=metric,
                    MetricsConfiguration={
                        "Metrics": [
                            {"Key": "StorageType", "Value": "Standard"}
                        ] if metric == "BucketSizeBytes" else []
                    }
                )
            
            logger.info(f"Configured CloudWatch metrics for bucket: {bucket}")
            return {"Bucket": bucket, "Metrics": metrics, "Status": "MetricsConfigured"}
            
        except ClientError as e:
            logger.error(f"Failed to configure metrics: {e}")
            raise
    
    def get_bucket_metrics(self, bucket: str) -> List[str]:
        """
        Get bucket metrics configuration.
        
        Args:
            bucket: Bucket name
            
        Returns:
            List of configured metrics
        """
        try:
            response = self.s3_client.list_bucket_metrics_configurations(Bucket=bucket)
            configs = response.get("MetricsConfigurationList", [])
            return [c.get("Id") for c in configs if c.get("Id")]
            
        except ClientError as e:
            logger.error(f"Failed to get metrics: {e}")
            raise
    
    def get_cloudwatch_metrics(
        self,
        bucket: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 86400
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a bucket.
        
        Args:
            bucket: Bucket name
            start_time: Start time
            end_time: End time
            period: Aggregation period in seconds
            
        Returns:
            CloudWatch metrics data
        """
        try:
            namespace = "AWS/S3"
            stats = ["Average", "Maximum", "Minimum", "SampleCount"]
            
            metrics_data = {}
            
            metric_queries = [
                {
                    "Id": "bucket_size",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": namespace,
                            "MetricName": "BucketSizeBytes",
                            "Dimensions": [
                                {"Name": "BucketName", "Value": bucket},
                                {"Name": "StorageType", "Value": "StandardStorage"}
                            ]
                        },
                        "Period": period,
                        "Stat": "Average"
                    }
                },
                {
                    "Id": "object_count",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": namespace,
                            "MetricName": "NumberOfObjects",
                            "Dimensions": [
                                {"Name": "BucketName", "Value": bucket},
                                {"Name": "StorageType", "Value": "AllStorageTypes"}
                            ]
                        },
                        "Period": period,
                        "Stat": "Average"
                    }
                }
            ]
            
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time
            )
            
            for result in response.get("MetricDataResults", []):
                metrics_data[result["Id"]] = {
                    "Label": result["Label"],
                    "Values": result["Values"]
                }
            
            return metrics_data
            
        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            raise
    
    def put_cloudwatch_dashboard(
        self,
        dashboard_name: str,
        buckets: List[str]
    ) -> Dict[str, Any]:
        """
        Create/update CloudWatch dashboard for S3 buckets.
        
        Args:
            dashboard_name: Dashboard name
            buckets: List of bucket names
            
        Returns:
            Response
        """
        try:
            widget_body = {
                "widgets": []
            }
            
            for bucket in buckets:
                widget = {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/S3", "BucketSizeBytes", "BucketName", bucket, "StorageType", "StandardStorage"],
                            [".", "NumberOfObjects", ".", ".", ".", "AllStorageTypes"]
                        ],
                        "period": 86400,
                        "stat": "Average",
                        "region": self.region_name,
                        "title": f"{bucket} Metrics"
                    }
                }
                widget_body["widgets"].append(widget)
            
            body_json = json.dumps(widget_body)
            
            self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=body_json
            )
            
            logger.info(f"Created CloudWatch dashboard: {dashboard_name}")
            return {"DashboardName": dashboard_name, "Status": "DashboardCreated"}
            
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def enable_request_metrics(self, bucket: str) -> Dict[str, Any]:
        """
        Enable request metrics (CloudWatch RequestMetrics) for bucket.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Response
        """
        try:
            self.s3_client.put_bucket_metrics_configuration(
                Bucket=bucket,
                Id="AllRequests",
                MetricsConfiguration={
                    "Metrics": []
                }
            )
            
            logger.info(f"Enabled request metrics for bucket: {bucket}")
            return {"Bucket": bucket, "Status": "RequestMetricsEnabled"}
            
        except ClientError as e:
            logger.error(f"Failed to enable request metrics: {e}")
            raise
    
    def get_request_metrics(
        self,
        bucket: str,
        start_time: datetime,
        end_time: datetime,
        filter_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch request metrics for a bucket.
        
        Args:
            bucket: Bucket name
            start_time: Start time
            end_time: End time
            filter_name: Optional filter name
            
        Returns:
            Request metrics data
        """
        try:
            metrics = [
                ("4xxErrors", "Sum"),
                ("5xxErrors", "Sum"),
                ("GetRequests", "Sum"),
                ("PutRequests", "Sum"),
                ("DeleteRequests", "Sum"),
                ("HeadRequests", "Sum"),
                ("PostRequests", "Sum"),
                ("ListRequests", "Sum"),
                ("BytesDownloaded", "Sum"),
                ("BytesUploaded", "Sum"),
                ("TotalRequestLatency", "Average")
            ]
            
            namespace = "AWS/S3"
            results = {}
            
            for metric_name, stat in metrics:
                try:
                    dimensions = [{"Name": "BucketName", "Value": bucket}]
                    if filter_name:
                        dimensions.append({"Name": "FilterId", "Value": filter_name})
                    
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace=namespace,
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,
                        Statistics=[stat]
                    )
                    
                    results[metric_name] = {
                        "stat": stat,
                        "data_points": response.get("Datapoints", [])
                    }
                except ClientError:
                    results[metric_name] = {"stat": stat, "data_points": []}
            
            return results
            
        except ClientError as e:
            logger.error(f"Failed to get request metrics: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def sync_directory(
        self,
        bucket: str,
        prefix: str,
        local_path: str,
        direction: str = "upload",
        delete: bool = False,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Sync a local directory with S3 prefix.
        
        Args:
            bucket: Bucket name
            prefix: S3 key prefix
            local_path: Local directory path
            direction: 'upload' or 'download'
            delete: Delete files that don't exist locally/in S3
            exclude_patterns: Patterns to exclude
            include_patterns: Patterns to include
            
        Returns:
            Sync results
        """
        try:
            if direction == "upload":
                return self._sync_upload(bucket, prefix, local_path, delete, exclude_patterns, include_patterns)
            else:
                return self._sync_download(bucket, prefix, local_path, delete, exclude_patterns, include_patterns)
                
        except ClientError as e:
            logger.error(f"Sync failed: {e}")
            raise
    
    def _sync_upload(
        self,
        bucket: str,
        prefix: str,
        local_path: str,
        delete: bool,
        exclude_patterns: Optional[List[str]],
        include_patterns: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Upload directory to S3."""
        uploaded = []
        deleted = []
        
        local_files = {}
        for root, _, files in os.walk(local_path):
            for filename in files:
                filepath = os.path.join(root, filename)
                rel_path = os.path.relpath(filepath, local_path)
                s3_key = f"{prefix}/{rel_path}".replace("\\", "/")
                local_files[s3_key] = filepath
        
        s3_objects = self.list_objects(bucket, prefix)
        s3_keys = {obj["Key"] for obj in s3_objects["Objects"]}
        
        for s3_key, filepath in local_files.items():
            if s3_key not in s3_keys:
                self.upload_object(UploadConfig(
                    bucket=bucket,
                    key=s3_key,
                    data=open(filepath, "rb").read()
                ))
                uploaded.append(s3_key)
        
        if delete:
            for s3_key in s3_keys:
                if s3_key.startswith(prefix) and s3_key not in local_files:
                    self.delete_object(bucket, s3_key, silent=True)
                    deleted.append(s3_key)
        
        return {"Uploaded": uploaded, "Deleted": deleted, "Status": "SyncCompleted"}
    
    def _sync_download(
        self,
        bucket: str,
        prefix: str,
        local_path: str,
        delete: bool,
        exclude_patterns: Optional[List[str]],
        include_patterns: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Download S3 prefix to local directory."""
        downloaded = []
        deleted = []
        
        s3_objects = self.list_objects(bucket, prefix)
        s3_files = {}
        for obj in s3_objects["Objects"]:
            key = obj["Key"]
            local_rel = key[len(prefix):].lstrip("/")
            if local_rel:
                s3_files[key] = local_rel
        
        os.makedirs(local_path, exist_ok=True)
        
        for s3_key, rel_path in s3_files.items():
            filepath = os.path.join(local_path, rel_path)
            if not os.path.exists(filepath):
                self.download_object(bucket, s3_key, filepath)
                downloaded.append(s3_key)
        
        if delete:
            for root, _, files in os.walk(local_path):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    rel_path = os.path.relpath(filepath, local_path)
                    s3_key = f"{prefix}/{rel_path}".replace("\\", "/")
                    if s3_key not in s3_files:
                        os.remove(filepath)
                        deleted.append(s3_key)
        
        return {"Downloaded": downloaded, "Deleted": deleted, "Status": "SyncCompleted"}
    
    def calculate_bucket_size(self, bucket: str) -> Dict[str, Any]:
        """
        Calculate total size of all objects in bucket.
        
        Args:
            bucket: Bucket name
            
        Returns:
            Size statistics
        """
        try:
            total_size = 0
            total_count = 0
            storage_by_type = defaultdict(int)
            
            paginator = self.s3_client.get_paginator("list_objects_v2")
            
            for page in paginator.paginate(Bucket=bucket):
                objects = page.get("Contents", [])
                for obj in objects:
                    size = obj.get("Size", 0)
                    total_size += size
                    total_count += 1
                    
                    storage_class = obj.get("StorageClass", "STANDARD")
                    storage_by_type[storage_class] += size
            
            return {
                "Bucket": bucket,
                "TotalSizeBytes": total_size,
                "TotalSizeMB": round(total_size / (1024 * 1024), 2),
                "TotalSizeGB": round(total_size / (1024 * 1024 * 1024), 2),
                "ObjectCount": total_count,
                "StorageByType": dict(storage_by_type)
            }
            
        except ClientError as e:
            logger.error(f"Failed to calculate bucket size: {e}")
            raise
    
    def get_bucket_usage_report(self, buckets: List[str]) -> Dict[str, Any]:
        """
        Get usage report for multiple buckets.
        
        Args:
            buckets: List of bucket names
            
        Returns:
            Usage report
        """
        report = {
            "ReportTimestamp": datetime.utcnow().isoformat(),
            "Buckets": []
        }
        
        for bucket in buckets:
            try:
                info = self.get_bucket_info(bucket)
                size_info = self.calculate_bucket_size(bucket)
                
                bucket_report = {
                    "Name": bucket,
                    "Region": info.get("region"),
                    "SizeBytes": size_info["TotalSizeBytes"],
                    "SizeGB": size_info["TotalSizeGB"],
                    "ObjectCount": size_info["ObjectCount"],
                    "Versioning": info.get("versioning", {}).get("Status"),
                    "Encryption": info.get("encryption", {}).get("Status"),
                    "StorageClasses": size_info["StorageByType"]
                }
                report["Buckets"].append(bucket_report)
                
            except ClientError as e:
                logger.warning(f"Failed to get info for bucket {bucket}: {e}")
                report["Buckets"].append({
                    "Name": bucket,
                    "Error": str(e)
                })
        
        total_size = sum(
            b.get("SizeBytes", 0) for b in report["Buckets"]
            if "SizeBytes" in b
        )
        report["TotalSizeBytes"] = total_size
        report["TotalSizeGB"] = round(total_size / (1024 ** 3), 2)
        report["TotalBuckets"] = len(report["Buckets"])
        
        return report
