"""
AWS CloudFront CDN Integration Module for Workflow System

Implements a CloudFrontIntegration class with:
1. Distribution management: Create/manage CloudFront distributions
2. Origin management: Manage origins
3. Behavior management: Manage cache behaviors
4. Invalidations: Create/invalidate cache
5. SSL/TLS: Manage SSL certificates
6. Lambda@Edge: Lambda@Edge functions
7. Signed URLs: Generate signed URLs
8. Geo restriction: Configure geo restriction
9. Access logs: Configure access logs
10. CloudWatch integration: Monitoring and metrics

Commit: 'feat(aws-cloudfront): add AWS CloudFront CDN with distribution management, origins, behaviors, invalidations, SSL/TLS, Lambda@Edge, signed URLs, geo restriction, access logs, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import hmac
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Union, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class PriceClass(Enum):
    """CloudFront price classes."""
    PRICE_CLASS_ALL = "PriceClass_All"
    PRICE_CLASS_100 = "PriceClass_100"
    PRICE_CLASS_200 = "PriceClass_200"


class ViewerProtocolPolicy(Enum):
    """Viewer protocol policies."""
    HTTP_ONLY = "http-only"
    HTTPS_ONLY = "https-only"
    REDIRECT_TO_HTTPS = "redirect-to-https"


class CachePolicy(Enum):
    """Cache policy IDs (AWS managed)."""
    ELIMINATE_PARAMETERS = "c基本化"  # Not used
    IMAGE_OPTIMIZATION = "cz7ur4j2"  # Not used
    MANAGEABLE_CACHE = "c83n9j0v"  # Not used


class GeoRestrictionType(Enum):
    """Geo restriction types."""
    NONE = "none"
    WHITELIST = "whitelist"
    BLACKLIST = "blacklist"


class HttpVersion(Enum):
    """Supported HTTP versions."""
    HTTP1_1 = "http1.1"
    HTTP2 = "http2"
    HTTP2_AND_3 = "http2and3"


class DefaultCacheBehaviorTarget(Enum):
    """Default cache behavior target origin."""
    ORIGIN = "origin"


@dataclass
class OriginConfig:
    """CloudFront origin configuration."""
    origin_id: str
    domain_name: str
    origin_path: str = ""
    custom_headers: Dict[str, str] = field(default_factory=dict)
    connection_attempts: int = 3
    connection_timeout: int = 10
    read_timeout: int = 30
    keepalive_timeout: int = 5
    origin_shield: Optional[str] = None
    origin_ssl_protocols: List[str] = field(default_factory=lambda: ["TLSv1.2"])
    origin_protocol_policy: str = "match-viewer"
    http_port: int = 80
    https_port: int = 443


@dataclass
class CacheBehaviorConfig:
    """Cache behavior configuration."""
    path_pattern: str = "/*"
    target_origin_id: str = ""
    viewer_protocol_policy: str = "https-only"
    allowed_methods: List[str] = field(default_factory=lambda: ["GET", "HEAD"])
    cached_methods: List[str] = field(default_factory=lambda: ["GET", "HEAD"])
    cache_policy_id: Optional[str] = None
    origin_request_policy_id: Optional[str] = None
    response_headers_policy_id: Optional[str] = None
    lambda_function_associations: List[Dict] = field(default_factory=list)
    min_ttl: int = 0
    default_ttl: int = 86400
    max_ttl: int = 31536000
    compress: bool = True
    smooth_streaming: bool = False
    trusted_key_groups: List[str] = field(default_factory=list)
    trusted_signers: List[str] = field(default_factory=list)


@dataclass
class DistributionConfig:
    """CloudFront distribution configuration."""
    origin_id: str
    domain_name: str
    caller_reference: str = field(default_factory=lambda: str(uuid.uuid4()))
    comment: str = ""
    enabled: bool = True
    price_class: str = "PriceClass_All"
    aliases: List[str] = field(default_factory=list)
    ssl_certificate: str = "cloudfront.default"
    min_protocol_version: str = "TLSv1.2_2021"
    supported_http_versions: List[str] = field(default_factory=lambda: ["http1.1", "http2", "http2and3"])
    default_root_object: str = "index.html"
    logs_bucket: Optional[str] = None
    logs_prefix: str = "cloudfront"
    logs_include_cookies: bool = False
    geo_restriction_type: str = "none"
    geo_restriction_locations: List[str] = field(default_factory=list)
    default_cache_behavior: Optional[CacheBehaviorConfig] = None
    additional_cache_behaviors: List[CacheBehaviorConfig] = field(default_factory=list)


@dataclass
class InvalidationResult:
    """Result of a cache invalidation."""
    invalidation_id: str
    status: str
    create_time: datetime
    caller_reference: str
    paths: List[str]


@dataclass
class SignedUrlResult:
    """Result of signed URL generation."""
    url: str
    expires: datetime
    policy: str


class CloudFrontIntegration:
    """
    AWS CloudFront CDN integration for workflow automation.
    
    Provides comprehensive CloudFront management including:
    - Distribution creation and management
    - Origin and cache behavior configuration
    - Cache invalidation
    - SSL/TLS certificate management
    - Lambda@Edge function associations
    - Signed URL generation
    - Geo restriction configuration
    - Access logs setup
    - CloudWatch monitoring
    
    Example:
        cf = CloudFrontIntegration(region="us-east-1")
        
        # Create distribution
        dist = cf.create_distribution(
            origin_id="my-origin",
            domain_name="my-source.example.com",
            comment="My CDN distribution"
        )
        
        # Generate signed URL
        signed = cf.generate_signed_url(
            distribution_url="https://d123.cloudfront.net/files/doc.pdf",
            key_pair_id="K123456789",
            private_key_path="/path/to/key.pem",
            expires=3600
        )
        
        # Invalidate cache
        result = cf.create_invalidation(
            distribution_id="E12ABC",
            paths=["/images/*", "/css/*"]
        )
    """
    
    _client_lock = threading.Lock()
    _clients: Dict[str, Any] = {}
    
    def __init__(
        self,
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        config_overrides: Optional[Dict] = None
    ):
        """
        Initialize CloudFront integration.
        
        Args:
            region: AWS region (default: us-east-1 for CloudFront)
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            config_overrides: Boto configuration overrides
        """
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.config_overrides = config_overrides or {}
        
        self._distribution_cache: Dict[str, Dict] = {}
        self._origin_cache: Dict[str, Dict] = {}
        self._invalidation_cache: Dict[str, InvalidationResult] = {}
        
        if BOTO3_AVAILABLE:
            self._init_client()
        else:
            logger.warning("boto3 not available. CloudFront operations will fail.")
    
    def _init_client(self) -> None:
        """Initialize boto3 CloudFront client."""
        with self._client_lock:
            cache_key = f"{self.region}:{self.profile_name}"
            if cache_key not in self._clients:
                session_kwargs = {}
                if self.profile_name:
                    session_kwargs["profile_name"] = self.profile_name
                elif self.aws_access_key_id and self.aws_secret_access_key:
                    session_kwargs["aws_access_key_id"] = self.aws_access_key_id
                    session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
                    if self.aws_session_token:
                        session_kwargs["aws_session_token"] = self.aws_session_token
                
                session = boto3.Session(**session_kwargs)
                
                client_kwargs = {"region_name": self.region}
                client_kwargs.update(self.config_overrides)
                
                self._clients[cache_key] = session.client("cloudfront", **client_kwargs)
                logger.info(f"Initialized CloudFront client for region {self.region}")
    
    @property
    def client(self) -> Any:
        """Get the CloudFront client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudFront operations")
        cache_key = f"{self.region}:{self.profile_name}"
        if cache_key not in self._clients:
            self._init_client()
        return self._clients[cache_key]
    
    def _wrap_boto_errors(self, operation: str):
        """Decorator to wrap boto3 errors."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "Unknown")
                    logger.error(f"CloudFront {operation} failed: {error_code} - {e}")
                    raise
                except BotoCoreError as e:
                    logger.error(f"CloudFront {operation} failed: {e}")
                    raise
            return wrapper
        return decorator
    
    # =========================================================================
    # DISTRIBUTION MANAGEMENT
    # =========================================================================
    
    def create_distribution(
        self,
        origin_id: str,
        domain_name: str,
        comment: str = "",
        enabled: bool = True,
        price_class: str = "PriceClass_All",
        aliases: Optional[List[str]] = None,
        ssl_certificate: Optional[str] = None,
        default_root_object: str = "index.html",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a new CloudFront distribution.
        
        Args:
            origin_id: Unique identifier for the origin
            domain_name: Domain name of the origin (e.g., 'my-source.example.com')
            comment: Optional comment for the distribution
            enabled: Whether the distribution is enabled
            price_class: Price class for the distribution
            aliases: Optional list of alternate CNAMEs
            ssl_certificate: SSL certificate ARN or cloudfront.default
            default_root_object: Default object to serve from root URL
            **kwargs: Additional distribution configuration
            
        Returns:
            Dictionary with distribution details including DistributionConfig
        """
        if not BOTO3_AVAILABLE:
            return self._mock_distribution_response(origin_id, domain_name, comment, enabled)
        
        caller_reference = str(uuid.uuid4())
        
        origin_config = {
            "Id": origin_id,
            "DomainName": domain_name,
            "OriginPath": kwargs.get("origin_path", ""),
            "CustomHeaders": {
                "Quantity": len(kwargs.get("custom_headers", {})),
                "Items": [
                    {"HeaderName": k, "HeaderValue": v}
                    for k, v in kwargs.get("custom_headers", {}).items()
                ]
            } if kwargs.get("custom_headers") else {"Quantity": 0, "Items": []},
            "ConnectionAttempts": kwargs.get("connection_attempts", 3),
            "ConnectionTimeout": kwargs.get("connection_timeout", 10),
        }
        
        default_cache_behavior = {
            "TargetOriginId": origin_id,
            "ViewerProtocolPolicy": kwargs.get("viewer_protocol_policy", "https-only"),
            "AllowedMethods": {
                "Quantity": 2,
                "Items": ["GET", "HEAD"],
                "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}
            },
            "Compress": kwargs.get("compress", True),
            "CachePolicyId": kwargs.get("cache_policy_id", "Managed-Default"),
            "MinTTL": kwargs.get("min_ttl", 0),
            "DefaultTTL": kwargs.get("default_ttl", 86400),
            "MaxTTL": kwargs.get("max_ttl", 31536000),
        }
        
        aliases_config = {"Quantity": 0, "Items": []}
        if aliases:
            aliases_config = {"Quantity": len(aliases), "Items": aliases}
        
        ssl_config = {
            "CloudFrontDefaultCertificate": True,
            "MinimumProtocolVersion": "TLSv1.2_2021"
        }
        if ssl_certificate and ssl_certificate != "cloudfront.default":
            ssl_config = {
                "ACMCertificateArn": ssl_certificate,
                "SSLSupportMethod": "sni-only",
                "MinimumProtocolVersion": kwargs.get("min_protocol_version", "TLSv1.2_2021")
            }
        
        geo_config = {"RestrictionType": "none", "Quantity": 0, "Items": []}
        geo_restriction_type = kwargs.get("geo_restriction_type", "none")
        geo_restriction_locations = kwargs.get("geo_restriction_locations", [])
        if geo_restriction_type != "none" and geo_restriction_locations:
            geo_config = {
                "RestrictionType": geo_restriction_type,
                "Quantity": len(geo_restriction_locations),
                "Items": geo_restriction_locations
            }
        
        logging_config = None
        logs_bucket = kwargs.get("logs_bucket")
        if logs_bucket:
            logging_config = {
                "Enabled": True,
                "Bucket": logs_bucket,
                "Prefix": kwargs.get("logs_prefix", "cloudfront"),
                "IncludeCookies": kwargs.get("logs_include_cookies", False)
            }
        
        distribution_config = {
            "CallerReference": caller_reference,
            "Comment": comment,
            "Enabled": enabled,
            "PriceClass": price_class,
            "Aliases": aliases_config,
            "DefaultRootObject": default_root_object,
            "Origins": {"Quantity": 1, "Items": [origin_config]},
            "DefaultCacheBehavior": default_cache_behavior,
            "CacheBehaviors": {"Quantity": 0, "Items": []},
            "CustomErrorResponses": {"Quantity": 0, "Items": []},
            "Logging": logging_config,
            "ViewerCertificate": ssl_config,
            "GeoRestriction": geo_config,
            "HttpVersion": kwargs.get("http_version", "http2and3"),
            "IsIPV6Enabled": kwargs.get("ipv6_enabled", True),
        }
        
        try:
            response = self.client.create_distribution(
                DistributionConfig=distribution_config
            )
            
            result = response.get("Distribution", {})
            self._distribution_cache[result.get("Id", origin_id)] = result
            
            logger.info(f"Created CloudFront distribution: {result.get('Id')}")
            return {
                "id": result.get("Id"),
                "arn": result.get("ARN"),
                "status": result.get("Status"),
                "domain_name": result.get("DomainName"),
                "caller_reference": caller_reference,
                "config": result.get("DistributionConfig", {}),
                "created": True
            }
        except ClientError as e:
            logger.error(f"Failed to create distribution: {e}")
            raise
    
    def get_distribution(
        self,
        distribution_id: str,
        refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about a CloudFront distribution.
        
        Args:
            distribution_id: The distribution ID
            refresh: Force refresh from AWS
            
        Returns:
            Distribution details or None if not found
        """
        if not BOTO3_AVAILABLE:
            return self._get_mock_distribution(distribution_id)
        
        if distribution_id in self._distribution_cache and not refresh:
            return self._distribution_cache[distribution_id]
        
        try:
            response = self.client.get_distribution(Id=distribution_id)
            result = response.get("Distribution", {})
            self._distribution_cache[distribution_id] = result
            return result
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchDistribution":
                logger.warning(f"Distribution not found: {distribution_id}")
                return None
            raise
    
    def list_distributions(
        self,
        marker: Optional[str] = None,
        max_items: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List all CloudFront distributions.
        
        Args:
            marker: Pagination marker
            max_items: Maximum number of items to return
            
        Returns:
            List of distribution summaries
        """
        if not BOTO3_AVAILABLE:
            return []
        
        distributions = []
        params = {"MaxItems": str(max_items)}
        if marker:
            params["Marker"] = marker
        
        try:
            response = self.client.list_distributions(**params)
            items = response.get("DistributionList", {}).get("Items", [])
            distributions.extend(items)
            
            if response.get("DistributionList", {}).get("IsTruncated"):
                next_marker = response.get("DistributionList", {}).get("NextMarker")
                distributions.extend(self.list_distributions(
                    marker=next_marker,
                    max_items=max_items
                ))
            
            return distributions
        except ClientError as e:
            logger.error(f"Failed to list distributions: {e}")
            raise
    
    def update_distribution(
        self,
        distribution_id: str,
        etag: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a CloudFront distribution configuration.
        
        Args:
            distribution_id: The distribution ID
            etag: The ETag for the current distribution
            updates: Dictionary of configuration updates
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"id": distribution_id, "updated": True, "mock": True}
        
        try:
            response = self.client.get_distribution(Id=distribution_id)
            config = response.get("Distribution", {}).get("DistributionConfig", {})
            
            for key, value in updates.items():
                if hasattr(config, key) or key in config:
                    config[key] = value
            
            response = self.client.update_distribution(
                Id=distribution_id,
                DistributionConfig=config,
                IfMatch=etag
            )
            
            result = response.get("Distribution", {})
            self._distribution_cache[distribution_id] = result
            
            logger.info(f"Updated distribution: {distribution_id}")
            return {
                "id": result.get("Id"),
                "status": result.get("Status"),
                "domain_name": result.get("DomainName"),
                "updated": True
            }
        except ClientError as e:
            logger.error(f"Failed to update distribution {distribution_id}: {e}")
            raise
    
    def delete_distribution(
        self,
        distribution_id: str,
        etag: str
    ) -> bool:
        """
        Delete a CloudFront distribution.
        
        Args:
            distribution_id: The distribution ID
            etag: The ETag for the distribution
            
        Returns:
            True if deleted successfully
        """
        if not BOTO3_AVAILABLE:
            return True
        
        try:
            self.client.delete_distribution(Id=distribution_id, IfMatch=etag)
            
            if distribution_id in self._distribution_cache:
                del self._distribution_cache[distribution_id]
            
            logger.info(f"Deleted distribution: {distribution_id}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete distribution {distribution_id}: {e}")
            raise
    
    def enable_distribution(self, distribution_id: str) -> Dict[str, Any]:
        """Enable a CloudFront distribution."""
        if not BOTO3_AVAILABLE:
            return {"id": distribution_id, "enabled": True}
        
        response = self.client.get_distribution(Id=distribution_id)
        dist = response.get("Distribution", {})
        etag = response.get("ETag")
        
        config = dist.get("DistributionConfig", {})
        config["Enabled"] = True
        
        update_response = self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=etag
        )
        
        return {
            "id": distribution_id,
            "enabled": True,
            "status": update_response.get("Distribution", {}).get("Status")
        }
    
    def disable_distribution(self, distribution_id: str) -> Dict[str, Any]:
        """Disable a CloudFront distribution."""
        if not BOTO3_AVAILABLE:
            return {"id": distribution_id, "enabled": False}
        
        response = self.client.get_distribution(Id=distribution_id)
        dist = response.get("Distribution", {})
        etag = response.get("ETag")
        
        config = dist.get("DistributionConfig", {})
        config["Enabled"] = False
        
        update_response = self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=etag
        )
        
        return {
            "id": distribution_id,
            "enabled": False,
            "status": update_response.get("Distribution", {}).get("Status")
        }
    
    # =========================================================================
    # ORIGIN MANAGEMENT
    # =========================================================================
    
    def add_origin(
        self,
        distribution_id: str,
        origin_id: str,
        domain_name: str,
        etag: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Add an origin to an existing distribution.
        
        Args:
            distribution_id: The distribution ID
            origin_id: Unique identifier for the origin
            domain_name: Domain name of the origin
            etag: The ETag for the current distribution
            **kwargs: Additional origin configuration
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "added_origin": origin_id}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        etag = response.get("ETag")
        
        origins = config.get("Origins", {"Quantity": 0, "Items": []})
        origin_list = origins.get("Items", [])
        
        new_origin = {
            "Id": origin_id,
            "DomainName": domain_name,
            "OriginPath": kwargs.get("origin_path", ""),
            "CustomHeaders": {
                "Quantity": len(kwargs.get("custom_headers", {})),
                "Items": [
                    {"HeaderName": k, "HeaderValue": v}
                    for k, v in kwargs.get("custom_headers", {}).items()
                ]
            } if kwargs.get("custom_headers") else {"Quantity": 0, "Items": []},
            "ConnectionAttempts": kwargs.get("connection_attempts", 3),
            "ConnectionTimeout": kwargs.get("connection_timeout", 10),
        }
        
        origin_list.append(new_origin)
        origins["Quantity"] = len(origin_list)
        origins["Items"] = origin_list
        config["Origins"] = origins
        
        update_response = self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=etag
        )
        
        logger.info(f"Added origin {origin_id} to distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "added_origin": origin_id,
            "domain_name": domain_name
        }
    
    def remove_origin(
        self,
        distribution_id: str,
        origin_id: str,
        etag: str
    ) -> Dict[str, Any]:
        """
        Remove an origin from a distribution.
        
        Args:
            distribution_id: The distribution ID
            origin_id: The origin ID to remove
            etag: The ETag for the current distribution
            
        Returns:
            Result of the removal operation
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "removed_origin": origin_id}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        origins = config.get("Origins", {"Quantity": 0, "Items": []})
        origin_list = origins.get("Items", [])
        
        origin_list = [o for o in origin_list if o.get("Id") != origin_id]
        
        origins["Quantity"] = len(origin_list)
        origins["Items"] = origin_list
        config["Origins"] = origins
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Removed origin {origin_id} from distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "removed_origin": origin_id
        }
    
    def get_origin(
        self,
        distribution_id: str,
        origin_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get details of a specific origin.
        
        Args:
            distribution_id: The distribution ID
            origin_id: The origin ID
            
        Returns:
            Origin details or None if not found
        """
        distribution = self.get_distribution(distribution_id, refresh=True)
        if not distribution:
            return None
        
        origins = distribution.get("DistributionConfig", {}).get("Origins", {}).get("Items", [])
        for origin in origins:
            if origin.get("Id") == origin_id:
                return origin
        
        return None
    
    # =========================================================================
    # CACHE BEHAVIOR MANAGEMENT
    # =========================================================================
    
    def add_cache_behavior(
        self,
        distribution_id: str,
        behavior: CacheBehaviorConfig,
        etag: str
    ) -> Dict[str, Any]:
        """
        Add a cache behavior to a distribution.
        
        Args:
            distribution_id: The distribution ID
            behavior: CacheBehaviorConfig object
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "added_behavior": behavior.path_pattern}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        behavior_dict = {
            "PathPattern": behavior.path_pattern,
            "TargetOriginId": behavior.target_origin_id,
            "ViewerProtocolPolicy": behavior.viewer_protocol_policy,
            "AllowedMethods": {
                "Quantity": len(behavior.allowed_methods),
                "Items": behavior.allowed_methods,
                "CachedMethods": {
                    "Quantity": len(behavior.cached_methods),
                    "Items": behavior.cached_methods
                }
            },
            "Compress": behavior.compress,
            "CachePolicyId": behavior.cache_policy_id or "Managed-Default",
            "MinTTL": behavior.min_ttl,
            "DefaultTTL": behavior.default_ttl,
            "MaxTTL": behavior.max_ttl,
        }
        
        if behavior.lambda_function_associations:
            behavior_dict["LambdaFunctionAssociations"] = {
                "Quantity": len(behavior.lambda_function_associations),
                "Items": behavior.lambda_function_associations
            }
        
        behaviors = config.get("CacheBehaviors", {"Quantity": 0, "Items": []})
        behavior_list = behaviors.get("Items", [])
        behavior_list.append(behavior_dict)
        behaviors["Quantity"] = len(behavior_list)
        behaviors["Items"] = behavior_list
        config["CacheBehaviors"] = behaviors
        
        update_response = self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Added cache behavior {behavior.path_pattern} to distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "added_behavior": behavior.path_pattern
        }
    
    def update_cache_behavior(
        self,
        distribution_id: str,
        path_pattern: str,
        updates: Dict[str, Any],
        etag: str
    ) -> Dict[str, Any]:
        """
        Update a cache behavior configuration.
        
        Args:
            distribution_id: The distribution ID
            path_pattern: The path pattern of the behavior to update
            updates: Dictionary of updates to apply
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "updated_behavior": path_pattern}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        behaviors = config.get("CacheBehaviors", {"Quantity": 0, "Items": []})
        behavior_list = behaviors.get("Items", [])
        
        for behavior in behavior_list:
            if behavior.get("PathPattern") == path_pattern:
                behavior.update(updates)
                break
        
        behaviors["Items"] = behavior_list
        config["CacheBehaviors"] = behaviors
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Updated cache behavior {path_pattern} in distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "updated_behavior": path_pattern
        }
    
    def remove_cache_behavior(
        self,
        distribution_id: str,
        path_pattern: str,
        etag: str
    ) -> Dict[str, Any]:
        """
        Remove a cache behavior from a distribution.
        
        Args:
            distribution_id: The distribution ID
            path_pattern: The path pattern of the behavior to remove
            etag: The ETag for the current distribution
            
        Returns:
            Result of the removal operation
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "removed_behavior": path_pattern}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        behaviors = config.get("CacheBehaviors", {"Quantity": 0, "Items": []})
        behavior_list = behaviors.get("Items", [])
        behavior_list = [b for b in behavior_list if b.get("PathPattern") != path_pattern]
        
        behaviors["Quantity"] = len(behavior_list)
        behaviors["Items"] = behavior_list
        config["CacheBehaviors"] = behaviors
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Removed cache behavior {path_pattern} from distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "removed_behavior": path_pattern
        }
    
    def set_default_cache_behavior(
        self,
        distribution_id: str,
        behavior: CacheBehaviorConfig,
        etag: str
    ) -> Dict[str, Any]:
        """
        Set the default cache behavior for a distribution.
        
        Args:
            distribution_id: The distribution ID
            behavior: CacheBehaviorConfig object
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "default_behavior_updated": True}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        default_behavior = {
            "TargetOriginId": behavior.target_origin_id,
            "ViewerProtocolPolicy": behavior.viewer_protocol_policy,
            "AllowedMethods": {
                "Quantity": len(behavior.allowed_methods),
                "Items": behavior.allowed_methods,
                "CachedMethods": {
                    "Quantity": len(behavior.cached_methods),
                    "Items": behavior.cached_methods
                }
            },
            "Compress": behavior.compress,
            "CachePolicyId": behavior.cache_policy_id or "Managed-Default",
            "MinTTL": behavior.min_ttl,
            "DefaultTTL": behavior.default_ttl,
            "MaxTTL": behavior.max_ttl,
        }
        
        config["DefaultCacheBehavior"] = default_behavior
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Set default cache behavior for distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "default_behavior_updated": True
        }
    
    # =========================================================================
    # INVALIDATIONS
    # =========================================================================
    
    def create_invalidation(
        self,
        distribution_id: str,
        paths: List[str],
        caller_reference: Optional[str] = None
    ) -> InvalidationResult:
        """
        Create a cache invalidation request.
        
        Args:
            distribution_id: The distribution ID
            paths: List of paths to invalidate (e.g., ['/images/*', '/css/*'])
            caller_reference: Optional unique reference string
            
        Returns:
            InvalidationResult object with invalidation details
        """
        if not BOTO3_AVAILABLE:
            invalidation_id = f"I{str(uuid.uuid4())[:8].upper()}"
            return InvalidationResult(
                invalidation_id=invalidation_id,
                status="Completed",
                create_time=datetime.utcnow(),
                caller_reference=caller_reference or str(uuid.uuid4()),
                paths=paths
            )
        
        if not caller_reference:
            caller_reference = str(uuid.uuid4())
        
        try:
            response = self.client.create_invalidation(
                DistributionId=distribution_id,
                InvalidationBatch={
                    "Paths": {
                        "Quantity": len(paths),
                        "Items": paths
                    },
                    "CallerReference": caller_reference
                }
            )
            
            invalidation = response.get("Invalidation", {})
            result = InvalidationResult(
                invalidation_id=invalidation.get("Id"),
                status=invalidation.get("Status"),
                create_time=invalidation.get("CreateTime"),
                caller_reference=invalidation.get("CallerReference"),
                paths=paths
            )
            
            self._invalidation_cache[result.invalidation_id] = result
            logger.info(f"Created invalidation {result.invalidation_id} for distribution {distribution_id}")
            return result
            
        except ClientError as e:
            logger.error(f"Failed to create invalidation: {e}")
            raise
    
    def get_invalidation(
        self,
        distribution_id: str,
        invalidation_id: str
    ) -> Optional[InvalidationResult]:
        """
        Get the status of an invalidation.
        
        Args:
            distribution_id: The distribution ID
            invalidation_id: The invalidation ID
            
        Returns:
            InvalidationResult or None if not found
        """
        if invalidation_id in self._invalidation_cache:
            inv = self._invalidation_cache[invalidation_id]
            if inv.status != "Completed":
                return inv
        
        if not BOTO3_AVAILABLE:
            return InvalidationResult(
                invalidation_id=invalidation_id,
                status="Completed",
                create_time=datetime.utcnow(),
                caller_reference="mock",
                paths=[]
            )
        
        try:
            response = self.client.get_invalidation(
                DistributionId=distribution_id,
                Id=invalidation_id
            )
            
            inv = response.get("Invalidation", {})
            result = InvalidationResult(
                invalidation_id=inv.get("Id"),
                status=inv.get("Status"),
                create_time=inv.get("CreateTime"),
                caller_reference=inv.get("CallerReference"),
                paths=[p for p in inv.get("InvalidationBatch", {}).get("Paths", {}).get("Items", [])]
            )
            
            self._invalidation_cache[result.invalidation_id] = result
            return result
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchInvalidation":
                return None
            raise
    
    def list_invalidations(
        self,
        distribution_id: str,
        max_items: int = 100
    ) -> List[InvalidationResult]:
        """
        List all invalidations for a distribution.
        
        Args:
            distribution_id: The distribution ID
            max_items: Maximum number of items to return
            
        Returns:
            List of InvalidationResult objects
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.client.list_invalidations(
                DistributionId=distribution_id,
                MaxItems=str(max_items)
            )
            
            items = response.get("InvalidationList", {}).get("Items", [])
            results = []
            
            for inv in items:
                results.append(InvalidationResult(
                    invalidation_id=inv.get("Id"),
                    status=inv.get("Status"),
                    create_time=inv.get("CreateTime"),
                    caller_reference=inv.get("CallerReference"),
                    paths=[]
                ))
            
            return results
            
        except ClientError as e:
            logger.error(f"Failed to list invalidations: {e}")
            raise
    
    def wait_for_invalidation(
        self,
        distribution_id: str,
        invalidation_id: str,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> InvalidationResult:
        """
        Wait for an invalidation to complete.
        
        Args:
            distribution_id: The distribution ID
            invalidation_id: The invalidation ID
            timeout: Maximum seconds to wait
            poll_interval: Seconds between status checks
            
        Returns:
            Completed InvalidationResult
            
        Raises:
            TimeoutError: If invalidation doesn't complete within timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = self.get_invalidation(distribution_id, invalidation_id)
            if result and result.status == "Completed":
                return result
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Invalidation {invalidation_id} did not complete within {timeout} seconds")
    
    # =========================================================================
    # SSL/TLS CERTIFICATE MANAGEMENT
    # =========================================================================
    
    def attach_ssl_certificate(
        self,
        distribution_id: str,
        certificate_arn: str,
        etag: str,
        min_protocol_version: str = "TLSv1.2_2021",
        ssl_support_method: str = "sni-only"
    ) -> Dict[str, Any]:
        """
        Attach an SSL certificate to a distribution.
        
        Args:
            distribution_id: The distribution ID
            certificate_arn: ACM certificate ARN
            etag: The ETag for the current distribution
            min_protocol_version: Minimum TLS protocol version
            ssl_support_method: SSL support method ('sni-only' or 'vip')
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "certificate_arn": certificate_arn,
                "attached": True
            }
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["ViewerCertificate"] = {
            "ACMCertificateArn": certificate_arn,
            "SSLSupportMethod": ssl_support_method,
            "MinimumProtocolVersion": min_protocol_version,
            "Certificate": certificate_arn,
            "CertificateSource": "acm"
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Attached SSL certificate to distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "certificate_arn": certificate_arn,
            "attached": True
        }
    
    def use_cloudfront_default_certificate(
        self,
        distribution_id: str,
        etag: str,
        min_protocol_version: str = "TLSv1.2_2021"
    ) -> Dict[str, Any]:
        """
        Use CloudFront's default SSL certificate.
        
        Args:
            distribution_id: The distribution ID
            etag: The ETag for the current distribution
            min_protocol_version: Minimum TLS protocol version
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "using_default_cert": True}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["ViewerCertificate"] = {
            "CloudFrontDefaultCertificate": True,
            "MinimumProtocolVersion": min_protocol_version,
            "CertificateSource": "cloudfront"
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Set default SSL certificate for distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "using_default_cert": True
        }
    
    def list_ssl_certificates(self) -> List[Dict[str, Any]]:
        """
        List SSL certificates available for CloudFront.
        Note: This requires ACM or IAM access.
        
        Returns:
            List of certificate summaries
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.client.list_distributions()
            distributions = response.get("DistributionList", {}).get("Items", [])
            
            certs = []
            seen_arns = set()
            
            for dist in distributions:
                vc = dist.get("DistributionConfig", {}).get("ViewerCertificate", {})
                arn = vc.get("ACMCertificateArn")
                if arn and arn not in seen_arns:
                    seen_arns.add(arn)
                    certs.append({
                        "arn": arn,
                        "certificate_source": vc.get("CertificateSource"),
                        "minimum_protocol": vc.get("MinimumProtocolVersion"),
                        "ssl_support_method": vc.get("SSLSupportMethod")
                    })
            
            return certs
        except ClientError as e:
            logger.error(f"Failed to list SSL certificates: {e}")
            raise
    
    # =========================================================================
    # LAMBDA@EDGE
    # =========================================================================
    
    def add_lambda_association(
        self,
        distribution_id: str,
        behavior_path_pattern: str,
        lambda_arn: str,
        event_type: str = "viewer-request",
        include_body: bool = False,
        etag: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a Lambda@Edge function association to a cache behavior.
        
        Args:
            distribution_id: The distribution ID
            behavior_path_pattern: Path pattern of the cache behavior
            lambda_arn: Lambda function ARN (include version)
            event_type: Event type ('viewer-request', 'viewer-response', 
                       'origin-request', 'origin-response')
            include_body: Whether to include body in request
            etag: The ETag for the current distribution (auto-retrieved if None)
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "lambda_arn": lambda_arn,
                "event_type": event_type,
                "added": True
            }
        
        if etag is None:
            response = self.client.get_distribution(Id=distribution_id)
            etag = response.get("ETag")
            config = response.get("Distribution", {}).get("DistributionConfig", {})
        else:
            response = self.client.get_distribution(Id=distribution_id)
            config = response.get("Distribution", {}).get("DistributionConfig", {})
        
        lambda_assoc = {
            "LambdaFunctionARN": lambda_arn,
            "EventType": event_type,
            "IncludeBody": include_body
        }
        
        if behavior_path_pattern == "":
            default_behavior = config.get("DefaultCacheBehavior", {})
            lfa = default_behavior.get("LambdaFunctionAssociations", {"Quantity": 0, "Items": []})
            items = lfa.get("Items", [])
            items.append(lambda_assoc)
            lfa["Quantity"] = len(items)
            lfa["Items"] = items
            default_behavior["LambdaFunctionAssociations"] = lfa
            config["DefaultCacheBehavior"] = default_behavior
        else:
            behaviors = config.get("CacheBehaviors", {"Quantity": 0, "Items": []})
            for behavior in behaviors.get("Items", []):
                if behavior.get("PathPattern") == behavior_path_pattern:
                    lfa = behavior.get("LambdaFunctionAssociations", {"Quantity": 0, "Items": []})
                    items = lfa.get("Items", [])
                    items.append(lambda_assoc)
                    lfa["Quantity"] = len(items)
                    lfa["Items"] = items
                    behavior["LambdaFunctionAssociations"] = lfa
                    break
            
            config["CacheBehaviors"] = behaviors
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=etag
        )
        
        logger.info(f"Added Lambda@Edge {lambda_arn} to {event_type} for {behavior_path_pattern}")
        return {
            "distribution_id": distribution_id,
            "lambda_arn": lambda_arn,
            "event_type": event_type,
            "added": True
        }
    
    def remove_lambda_association(
        self,
        distribution_id: str,
        behavior_path_pattern: str,
        lambda_arn: str,
        event_type: str,
        etag: str
    ) -> Dict[str, Any]:
        """
        Remove a Lambda@Edge function association.
        
        Args:
            distribution_id: The distribution ID
            behavior_path_pattern: Path pattern of the cache behavior
            lambda_arn: Lambda function ARN
            event_type: Event type
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "lambda_arn": lambda_arn,
                "removed": True
            }
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        def filter_lambda(items):
            return [
                i for i in items 
                if not (i.get("LambdaFunctionARN") == lambda_arn and i.get("EventType") == event_type)
            ]
        
        if behavior_path_pattern == "":
            default_behavior = config.get("DefaultCacheBehavior", {})
            lfa = default_behavior.get("LambdaFunctionAssociations", {"Quantity": 0, "Items": []})
            items = filter_lambda(lfa.get("Items", []))
            lfa["Quantity"] = len(items)
            lfa["Items"] = items
            default_behavior["LambdaFunctionAssociations"] = lfa
            config["DefaultCacheBehavior"] = default_behavior
        else:
            behaviors = config.get("CacheBehaviors", {"Quantity": 0, "Items": []})
            for behavior in behaviors.get("Items", []):
                if behavior.get("PathPattern") == behavior_path_pattern:
                    lfa = behavior.get("LambdaFunctionAssociations", {"Quantity": 0, "Items": []})
                    items = filter_lambda(lfa.get("Items", []))
                    lfa["Quantity"] = len(items)
                    lfa["Items"] = items
                    behavior["LambdaFunctionAssociations"] = lfa
                    break
            
            config["CacheBehaviors"] = behaviors
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Removed Lambda@Edge {lambda_arn} from {behavior_path_pattern}")
        return {
            "distribution_id": distribution_id,
            "lambda_arn": lambda_arn,
            "removed": True
        }
    
    def list_lambda_associations(
        self,
        distribution_id: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        List all Lambda@Edge associations for a distribution.
        
        Args:
            distribution_id: The distribution ID
            
        Returns:
            Dictionary with 'default_cache_behavior' and 'cache_behaviors' keys
        """
        if not BOTO3_AVAILABLE:
            return {"default_cache_behavior": [], "cache_behaviors": []}
        
        distribution = self.get_distribution(distribution_id, refresh=True)
        if not distribution:
            return {"default_cache_behavior": [], "cache_behaviors": []}
        
        config = distribution.get("DistributionConfig", {})
        
        result = {
            "default_cache_behavior": [],
            "cache_behaviors": []
        }
        
        default_behavior = config.get("DefaultCacheBehavior", {})
        lfa = default_behavior.get("LambdaFunctionAssociations", {})
        result["default_cache_behavior"] = lfa.get("Items", [])
        
        behaviors = config.get("CacheBehaviors", {}).get("Items", [])
        for behavior in behaviors:
            lfa = behavior.get("LambdaFunctionAssociations", {})
            result["cache_behaviors"].append({
                "path_pattern": behavior.get("PathPattern"),
                "associations": lfa.get("Items", [])
            })
        
        return result
    
    # =========================================================================
    # SIGNED URLs
    # =========================================================================
    
    def generate_signed_url(
        self,
        distribution_url: str,
        key_pair_id: str,
        private_key_path: str,
        expires: Optional[int] = None,
        date_less_than: Optional[datetime] = None,
        date_greater_than: Optional[datetime] = None,
        ip_address: Optional[str] = None,
        policy: Optional[str] = None
    ) -> SignedUrlResult:
        """
        Generate a signed URL for CloudFront content.
        
        Args:
            distribution_url: The full URL of the content
            key_pair_id: CloudFront key pair ID
            private_key_path: Path to the private key file
            expires: Seconds until expiration (from now)
            date_less_than: Expiration datetime (alternative to expires)
            date_greater_than: Start datetime for the URL validity
            ip_address: IP address to restrict access to
            policy: Custom policy JSON string (overrides other options)
            
        Returns:
            SignedUrlResult with URL and expiration
            
        Note:
            Requires a valid CloudFront key pair configured in AWS.
        """
        if expires:
            date_less_than = datetime.utcnow() + timedelta(seconds=expires)
        
        if policy:
            policy = policy.replace("'", '"').replace(" ", "")
            policy_hash = hashlib.sha256(policy.encode()).hexdigest()
            signature = self._sign_policy(policy, private_key_path)
            encoded_policy = base64.urlsafe_b64encode(policy.encode()).rstrip(b"=").decode()
            
            url = f"{distribution_url}"
            separator = "&" if "?" in distribution_url else "?"
            
            params = [
                f"Policy={encoded_policy}",
                f"Signature={signature}",
                f"Key-Pair-Id={key_pair_id}"
            ]
            
            url = f"{url}{separator}{'&'.join(params)}"
            
            return SignedUrlResult(
                url=url,
                expires=date_less_than or datetime.utcnow() + timedelta(hours=1),
                policy=policy
            )
        else:
            if not date_less_than:
                date_less_than = datetime.utcnow() + timedelta(hours=1)
            
            policy_dict = {
                "Statement": [
                    {
                        "Resource": distribution_url.split("?")[0],
                        "Condition": {
                            "DateLessThan": {
                                "AWS:EpochTime": int(date_less_than.timestamp())
                            }
                        }
                    }
                ]
            }
            
            if date_greater_than:
                policy_dict["Statement"][0]["Condition"]["DateGreaterThan"] = {
                    "AWS:EpochTime": int(date_greater_than.timestamp())
                }
            
            if ip_address:
                policy_dict["Statement"][0]["Condition"]["IpAddress"] = {
                    "AWS:SourceIp": f"{ip_address}/32"
                }
            
            policy_json = json.dumps(policy_dict, separators=(",", ":"))
            policy_hash = hashlib.sha256(policy_json.encode()).hexdigest()
            signature = self._sign_policy(policy_json, private_key_path)
            encoded_policy = base64.urlsafe_b64encode(policy_json.encode()).rstrip(b"=").decode()
            
            url = distribution_url
            separator = "&" if "?" in distribution_url else "?"
            
            params = [
                f"Policy={encoded_policy}",
                f"Signature={signature}",
                f"Key-Pair-Id={key_pair_id}"
            ]
            
            url = f"{url}{separator}{'&'.join(params)}"
            
            return SignedUrlResult(
                url=url,
                expires=date_less_than,
                policy=policy_json
            )
    
    def _sign_policy(self, policy: str, private_key_path: str) -> str:
        """Sign a policy string with the private key."""
        try:
            with open(private_key_path, "rb") as key_file:
                private_key = key_file.read()
        except FileNotFoundError:
            if not BOTO3_AVAILABLE:
                return "mock_signature"
            raise
        
        import cryptography.hazmat.primitives.serialization as ser
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        
        try:
            from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
        except ImportError:
            decode_dss_signature = None
        
        policy_bytes = policy.encode()
        
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        key = load_pem_private_key(private_key, password=None, backend=default_backend())
        
        signature = key.sign(policy_bytes, padding.PKCS1v15(), hashes.SHA1())
        
        return base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    
    def generate_presigned_url(
        self,
        distribution_domain: str,
        key_pair_id: str,
        private_key_path: str,
        expires: int = 3600,
        policy_path: Optional[str] = None
    ) -> str:
        """
        Generate a simpler signed URL using canned policy.
        
        Args:
            distribution_domain: CloudFront distribution domain (e.g., 'd123.cloudfront.net')
            key_pair_id: CloudFront key pair ID
            private_key_path: Path to the private key file
            expires: Seconds until expiration
            policy_path: Optional specific path (defaults to '/*')
            
        Returns:
            Signed URL string
        """
        path = policy_path or "/*"
        url = f"https://{distribution_domain}/{path.lstrip('/')}"
        
        result = self.generate_signed_url(
            distribution_url=url,
            key_pair_id=key_pair_id,
            private_key_path=private_key_path,
            expires=expires
        )
        
        return result.url
    
    def get_signing_key(
        self,
        key_pair_id: str,
        private_key_path: str,
        key_group_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get information about a signing key.
        
        Args:
            key_pair_id: CloudFront key pair ID
            private_key_path: Path to the private key file
            key_group_id: Optional key group ID
            
        Returns:
            Dictionary with key information
        """
        try:
            with open(private_key_path, "rb") as f:
                key_content = f.read()
            
            key_hash = hashlib.sha256(key_content).hexdigest()[:16]
            
            return {
                "key_pair_id": key_pair_id,
                "key_group_id": key_group_id,
                "fingerprint": key_hash,
                "algorithm": "RSA-SHA1",
                "valid": True
            }
        except FileNotFoundError:
            return {
                "key_pair_id": key_pair_id,
                "valid": False,
                "error": f"Private key not found at {private_key_path}"
            }
    
    # =========================================================================
    # GEO RESTRICTION
    # =========================================================================
    
    def configure_geo_restriction(
        self,
        distribution_id: str,
        restriction_type: str,
        locations: List[str],
        etag: str
    ) -> Dict[str, Any]:
        """
        Configure geo restriction for a distribution.
        
        Args:
            distribution_id: The distribution ID
            restriction_type: 'whitelist' or 'blacklist'
            locations: List of country codes (ISO 3166-1 alpha-2)
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "restriction_type": restriction_type,
                "locations_count": len(locations)
            }
        
        if restriction_type not in ("whitelist", "blacklist"):
            raise ValueError("restriction_type must be 'whitelist' or 'blacklist'")
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["GeoRestriction"] = {
            "RestrictionType": restriction_type,
            "Quantity": len(locations),
            "Items": locations
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Configured geo restriction {restriction_type} for {len(locations)} locations")
        return {
            "distribution_id": distribution_id,
            "restriction_type": restriction_type,
            "locations_count": len(locations),
            "locations": locations
        }
    
    def disable_geo_restriction(
        self,
        distribution_id: str,
        etag: str
    ) -> Dict[str, Any]:
        """
        Disable geo restriction for a distribution.
        
        Args:
            distribution_id: The distribution ID
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "geo_restriction_disabled": True}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["GeoRestriction"] = {
            "RestrictionType": "none",
            "Quantity": 0,
            "Items": []
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Disabled geo restriction for distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "geo_restriction_disabled": True
        }
    
    def get_geo_restriction(self, distribution_id: str) -> Dict[str, Any]:
        """
        Get geo restriction configuration for a distribution.
        
        Args:
            distribution_id: The distribution ID
            
        Returns:
            Geo restriction configuration
        """
        distribution = self.get_distribution(distribution_id)
        if not distribution:
            return {}
        
        geo = distribution.get("DistributionConfig", {}).get("GeoRestriction", {})
        return {
            "restriction_type": geo.get("RestrictionType", "none"),
            "quantity": geo.get("Quantity", 0),
            "locations": geo.get("Items", [])
        }
    
    # =========================================================================
    # ACCESS LOGS
    # =========================================================================
    
    def enable_access_logs(
        self,
        distribution_id: str,
        s3_bucket: str,
        etag: str,
        prefix: str = "cloudfront",
        include_cookies: bool = False
    ) -> Dict[str, Any]:
        """
        Enable access logging for a distribution.
        
        Args:
            distribution_id: The distribution ID
            s3_bucket: S3 bucket for logs (e.g., 'my-logs.s3.amazonaws.com')
            etag: The ETag for the current distribution
            prefix: Prefix for log files
            include_cookies: Whether to include cookies in logs
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "logs_bucket": s3_bucket,
                "logs_enabled": True
            }
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["Logging"] = {
            "Enabled": True,
            "Bucket": s3_bucket,
            "Prefix": prefix,
            "IncludeCookies": include_cookies
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Enabled access logs for distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "logs_bucket": s3_bucket,
            "logs_enabled": True
        }
    
    def disable_access_logs(
        self,
        distribution_id: str,
        etag: str
    ) -> Dict[str, Any]:
        """
        Disable access logging for a distribution.
        
        Args:
            distribution_id: The distribution ID
            etag: The ETag for the current distribution
            
        Returns:
            Updated distribution details
        """
        if not BOTO3_AVAILABLE:
            return {"distribution_id": distribution_id, "logs_enabled": False}
        
        response = self.client.get_distribution(Id=distribution_id)
        config = response.get("Distribution", {}).get("DistributionConfig", {})
        current_etag = response.get("ETag")
        
        config["Logging"] = {
            "Enabled": False
        }
        
        self.client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=current_etag
        )
        
        logger.info(f"Disabled access logs for distribution {distribution_id}")
        return {
            "distribution_id": distribution_id,
            "logs_enabled": False
        }
    
    def get_access_logs_config(self, distribution_id: str) -> Dict[str, Any]:
        """
        Get access logging configuration for a distribution.
        
        Args:
            distribution_id: The distribution ID
            
        Returns:
            Access logs configuration
        """
        distribution = self.get_distribution(distribution_id)
        if not distribution:
            return {}
        
        logging_config = distribution.get("DistributionConfig", {}).get("Logging", {})
        return {
            "enabled": logging_config.get("Enabled", False),
            "bucket": logging_config.get("Bucket"),
            "prefix": logging_config.get("Prefix"),
            "include_cookies": logging_config.get("IncludeCookies", False)
        }
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def get_distribution_metrics(
        self,
        distribution_id: str,
        metric_names: Optional[List[str]] = None,
        period: int = 3600,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a distribution.
        
        Args:
            distribution_id: The distribution ID
            metric_names: List of metric names (default: all common metrics)
            period: Metric period in seconds
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dictionary of metric data
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "metrics": {},
                "mock": True
            }
        
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        default_metrics = [
            "Requests",
            "BytesDownloaded",
            "BytesUploaded",
            "TotalErrorRate",
            "4xxErrorRate",
            "5xxErrorRate"
        ]
        
        metrics_to_fetch = metric_names or default_metrics
        
        cloudwatch = boto3.client("cloudwatch", region_name=self.region)
        
        namespace = "AWS/CloudFront"
        metric_data = []
        
        for metric in metrics_to_fetch:
            metric_data.append({
                "Id": metric.lower().replace(" ", "_"),
                "MetricStat": {
                    "Metric": {
                        "Namespace": namespace,
                        "MetricName": metric,
                        "Dimensions": [
                            {
                                "Name": "DistributionId",
                                "Value": distribution_id
                            }
                        ]
                    },
                    "Period": period,
                    "Stat": "Sum"
                }
            })
        
        try:
            response = cloudwatch.get_metric_data(
                MetricDataQueries=metric_data,
                StartTime=start_time,
                EndTime=end_time
            )
            
            results = {}
            for result in response.get("MetricDataResults", []):
                results[result.get("Label")] = {
                    "timestamps": result.get("Timestamps", []),
                    "values": result.get("Values", [])
                }
            
            return {
                "distribution_id": distribution_id,
                "metrics": results,
                "period": period,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            raise
    
    def get_realtime_metrics(
        self,
        distribution_id: str
    ) -> Dict[str, Any]:
        """
        Get real-time metrics for a distribution.
        
        Args:
            distribution_id: The distribution ID
            
        Returns:
            Current real-time statistics
        """
        if not BOTO3_AVAILABLE:
            return {
                "distribution_id": distribution_id,
                "realtime_metrics": {},
                "mock": True
            }
        
        try:
            response = self.client.get_distribution_realtime_metrics(Id=distribution_id)
            return {
                "distribution_id": distribution_id,
                "realtime_metrics": response.get("RealtimeMetrics", {}),
                "fetched_at": datetime.utcnow().isoformat()
            }
        except ClientError as e:
            logger.error(f"Failed to get real-time metrics: {e}")
            return {
                "distribution_id": distribution_id,
                "error": str(e),
                "realtime_metrics": {}
            }
    
    def create_alarm(
        self,
        alarm_name: str,
        distribution_id: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Sum"
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for a CloudFront distribution.
        
        Args:
            alarm_name: Name of the alarm
            distribution_id: The distribution ID
            metric_name: Name of the metric
            threshold: Threshold value for the alarm
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic type
            
        Returns:
            Created alarm configuration
        """
        if not BOTO3_AVAILABLE:
            return {
                "alarm_name": alarm_name,
                "distribution_id": distribution_id,
                "created": True,
                "mock": True
            }
        
        cloudwatch = boto3.client("cloudwatch", region_name=self.region)
        
        alarm_config = {
            "AlarmName": alarm_name,
            "AlarmDescription": f"CloudFront {metric_name} alarm for {distribution_id}",
            "Namespace": "AWS/CloudFront",
            "MetricName": metric_name,
            "Dimensions": [
                {
                    "Name": "DistributionId",
                    "Value": distribution_id
                }
            ],
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "Statistic": statistic,
            "TreatMissingData": "notBreaching"
        }
        
        try:
            cloudwatch.put_metric_alarm(**alarm_config)
            
            logger.info(f"Created CloudWatch alarm {alarm_name}")
            return {
                "alarm_name": alarm_name,
                "distribution_id": distribution_id,
                "metric_name": metric_name,
                "threshold": threshold,
                "created": True
            }
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def list_alarms(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms for CloudFront.
        
        Args:
            prefix: Filter alarms by name prefix
            
        Returns:
            List of alarm configurations
        """
        if not BOTO3_AVAILABLE:
            return []
        
        cloudwatch = boto3.client("cloudwatch", region_name=self.region)
        
        try:
            response = cloudwatch.describe_alarms(
                AlarmNamePrefix=prefix,
                MetricAlarms=True
            )
            
            cloudfront_alarms = []
            for alarm in response.get("MetricAlarms", []):
                if alarm.get("Namespace") == "AWS/CloudFront":
                    cloudfront_alarms.append({
                        "alarm_name": alarm.get("AlarmName"),
                        "metric_name": alarm.get("MetricName"),
                        "threshold": alarm.get("Threshold"),
                        "comparison_operator": alarm.get("ComparisonOperator"),
                        "evaluation_periods": alarm.get("EvaluationPeriods"),
                        "period": alarm.get("Period"),
                        "state": alarm.get("StateValue"),
                        "dimensions": alarm.get("Dimensions", [])
                    })
            
            return cloudfront_alarms
        except ClientError as e:
            logger.error(f"Failed to list alarms: {e}")
            raise
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_distribution_config_etag(self, distribution_id: str) -> Tuple[str, str]:
        """
        Get the current ETag and configuration for a distribution.
        
        Args:
            distribution_id: The distribution ID
            
        Returns:
            Tuple of (etag, config)
        """
        if not BOTO3_AVAILABLE:
            return ("mock-etag", {})
        
        response = self.client.get_distribution(Id=distribution_id)
        return response.get("ETag"), response.get("Distribution", {}).get("DistributionConfig", {})
    
    def wait_for_deployment(
        self,
        distribution_id: str,
        timeout: int = 1800,
        poll_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for a distribution deployment to complete.
        
        Args:
            distribution_id: The distribution ID
            timeout: Maximum seconds to wait
            poll_interval: Seconds between status checks
            
        Returns:
            Final distribution status
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            distribution = self.get_distribution(distribution_id, refresh=True)
            if distribution:
                status = distribution.get("Status", "")
                if status == "Deployed":
                    return {
                        "distribution_id": distribution_id,
                        "status": status,
                        "deployed": True,
                        "wait_time": time.time() - start_time
                    }
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Distribution {distribution_id} deployment did not complete within {timeout} seconds")
    
    def _mock_distribution_response(
        self,
        origin_id: str,
        domain_name: str,
        comment: str,
        enabled: bool
    ) -> Dict[str, Any]:
        """Generate mock distribution response for testing without boto3."""
        dist_id = f"E{str(uuid.uuid4())[:8].upper()}"
        return {
            "id": dist_id,
            "arn": f"arn:aws:cloudfront::123456789:distribution/{dist_id}",
            "status": "InProgress",
            "domain_name": f"{dist_id.lower()}.cloudfront.net",
            "caller_reference": str(uuid.uuid4()),
            "config": {
                "Comment": comment,
                "Enabled": enabled,
                "Origins": {
                    "Quantity": 1,
                    "Items": [{
                        "Id": origin_id,
                        "DomainName": domain_name
                    }]
                }
            },
            "created": True,
            "mock": True
        }
    
    def _get_mock_distribution(self, distribution_id: str) -> Dict[str, Any]:
        """Get mock distribution for testing without boto3."""
        return {
            "Id": distribution_id,
            "ARN": f"arn:aws:cloudfront::123456789:distribution/{distribution_id}",
            "Status": "Deployed",
            "DomainName": f"{distribution_id.lower()}.cloudfront.net",
            "DistributionConfig": {
                "Enabled": True,
                "Comment": "mock",
                "Origins": {
                    "Quantity": 1,
                    "Items": [{
                        "Id": "mock-origin",
                        "DomainName": "mock-origin.example.com"
                    }]
                },
                "DefaultCacheBehavior": {
                    "TargetOriginId": "mock-origin"
                }
            }
        }
    
    def validate_distribution_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate distribution configuration.
        
        Args:
            config: Distribution configuration dictionary
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if "DomainName" not in config and not BOTO3_AVAILABLE:
            errors.append("DomainName is required")
        
        if "Enabled" in config and not isinstance(config.get("Enabled"), bool):
            errors.append("Enabled must be a boolean")
        
        if "Origins" in config:
            origins = config.get("Origins", {})
            if "Quantity" not in origins or not origins.get("Items"):
                errors.append("At least one origin is required")
        
        return errors
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._distribution_cache.clear()
        self._origin_cache.clear()
        self._invalidation_cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get integration statistics."""
        return {
            "cached_distributions": len(self._distribution_cache),
            "cached_origins": len(self._origin_cache),
            "cached_invalidations": len(self._invalidation_cache),
            "boto3_available": BOTO3_AVAILABLE,
            "region": self.region
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the CloudFront integration.
        
        Returns:
            Health status information
        """
        return {
            "service": "CloudFront",
            "status": "available" if BOTO3_AVAILABLE else "degraded",
            "boto3_available": BOTO3_AVAILABLE,
            "region": self.region
        }
    
    def __repr__(self) -> str:
        return f"CloudFrontIntegration(region='{self.region}')"
