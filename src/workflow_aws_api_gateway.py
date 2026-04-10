"""
AWS API Gateway Integration Module for Workflow System

Implements an APIGatewayIntegration class with:
1. REST API management: Create/manage REST APIs
2. HTTP API management: Create/manage HTTP APIs
3. WebSocket API: Manage WebSocket APIs
4. Resource and method: Create resources and methods
5. Integration: Lambda, HTTP, AWS service integrations
6. Stage management: Deploy and manage stages
7. Custom domains: Manage custom domains
8. API keys: Manage API keys
9. Usage plans: Manage usage plans
10. CloudWatch integration: Logging and metrics

Commit: 'feat(aws-api-gateway): add AWS API Gateway integration with REST/HTTP/WebSocket APIs, resources, methods, integrations, stages, custom domains, API keys, usage plans, CloudWatch'
"""

import uuid
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import hashlib

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None


logger = logging.getLogger(__name__)


class APIType(Enum):
    """API Gateway types."""
    REST = "REST"
    HTTP = "HTTP"
    WEBSOCKET = "WEBSOCKET"


class MethodType(Enum):
    """HTTP method types."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"
    ANY = "ANY"


class IntegrationType(Enum):
    """Integration types."""
    LAMBDA = "AWS"
    HTTP = "HTTP"
    HTTP_PROXY = "HTTP_PROXY"
    AWS_PROXY = "AWS_PROXY"
    MOCK = "MOCK"
    HTTP_API = "HTTP_API"
    AWS_PROXY_V2 = "AWS_PROXY_V2"


class AuthorizationType(Enum):
    """Authorization types."""
    NONE = "NONE"
    IAM = "AWS_IAM"
    RESOURCE_POLICY = "RESOURCE_POLICY"
    CUSTOM = "CUSTOM"
    COGNITO_USER_POOLS = "COGNITO_USER_POOLS"


@dataclass
class RESTAPIConfig:
    """Configuration for a REST API."""
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    binary_media_types: List[str] = field(default_factory=list)
    minimum_compression_size: Optional[int] = None
    api_key_source: str = "HEADER"
    endpoint_configuration: Dict[str, List[str]] = field(default_factory=lambda: {"types": ["REGIONAL"]})
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class HTTPAPIConfig:
    """Configuration for an HTTP API."""
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    protocols: List[str] = field(default_factory=lambda: ["HTTP", "HTTPS"])
    route_key: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class WebSocketAPIConfig:
    """Configuration for a WebSocket API."""
    name: str
    description: Optional[str] = None
    route_selection_expression: str = "$request.body.action"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ResourceConfig:
    """Configuration for an API resource."""
    path: str
    parent_id: Optional[str] = None
    methods: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class MethodConfig:
    """Configuration for a method."""
    http_method: MethodType
    authorization_type: AuthorizationType = AuthorizationType.NONE
    api_key_required: bool = False
    request_parameters: Dict[str, bool] = field(default_factory=dict)
    request_models: Dict[str, str] = field(default_factory=dict)
    method_handlers: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IntegrationConfig:
    """Configuration for an integration."""
    integration_type: IntegrationType
    uri: Optional[str] = None
    integration_http_method: Optional[str] = None
    passthrough_behavior: str = "WHEN_NO_MATCH"
    content_handling: Optional[str] = None
    timeout_milliseconds: int = 29000
    cache_namespace: Optional[str] = None
    cache_parameters: Dict[str, str] = field(default_factory=dict)
    tls_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageConfig:
    """Configuration for an API stage."""
    stage_name: str
    description: Optional[str] = None
    deployment_id: Optional[str] = None
    variables: Dict[str, str] = field(default_factory=dict)
    access_log_settings: Dict[str, Any] = field(default_factory=dict)
    canary_settings: Dict[str, Any] = field(default_factory=dict)
    tracing_enabled: bool = False
    metrics_enabled: bool = True
    throttling_burst_limit: Optional[int] = None
    throttling_rate_limit: Optional[int] = None


@dataclass
class CustomDomainConfig:
    """Configuration for a custom domain."""
    domain_name: str
    certificate_arn: str
    security_policy: str = "TLS_1_2"
    endpoint_type: str = "REGIONAL"
    base_path_mapping: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class APIKeyConfig:
    """Configuration for an API key."""
    name: str
    description: Optional[str] = None
    enabled: bool = True
    generate_distinct_id: bool = True
    value: Optional[str] = None


@dataclass
class UsagePlanConfig:
    """Configuration for a usage plan."""
    name: str
    description: Optional[str] = None
    quota: Dict[str, Any] = field(default_factory=dict)
    throttle: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class CloudWatchConfig:
    """Configuration for CloudWatch logging and metrics."""
    log_level: str = "INFO"
    log_format: str = "JSON"
    detailed_metrics: bool = True
    data_trace: bool = False
    logging_level: str = "OFF"
    metrics_enabled: bool = True
    cache_log_enabled: bool = True


class APIGatewayIntegration:
    """
    AWS API Gateway Integration for workflow automation.
    
    Supports REST APIs, HTTP APIs, WebSocket APIs, and provides:
    - API creation and management
    - Resource and method configuration
    - Lambda, HTTP, and AWS service integrations
    - Stage deployment and management
    - Custom domains and API keys
    - Usage plans and CloudWatch integration
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        role_arn: Optional[str] = None,
        external_id: Optional[str] = None
    ):
        """
        Initialize the API Gateway integration.
        
        Args:
            region: AWS region
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            role_arn: Role ARN to assume
            external_id: External ID for role assumption
        """
        self.region = region
        self.role_arn = role_arn
        self.external_id = external_id
        self._clients = {}
        self._lock = threading.RLock()
        self._apis = {}
        self._resources = {}
        self._stages = {}
        self._custom_domains = {}
        self._api_keys = {}
        self._usage_plans = {}
        self._cloudwatch_configs = {}
        
        if BOTO3_AVAILABLE:
            self._init_clients(
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token
            )
    
    def _init_clients(
        self,
        aws_access_key_id: Optional[str],
        aws_secret_access_key: Optional[str],
        aws_session_token: Optional[str]
    ) -> None:
        """Initialize boto3 clients."""
        session_kwargs = {
            "region_name": self.region
        }
        
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                session_kwargs["aws_session_token"] = aws_session_token
        
        if self.role_arn:
            sts_client = boto3.Session(**session_kwargs).client("sts")
            assume_role_kwargs = {
                "RoleArn": self.role_arn,
                "RoleSessionName": f"api_gateway_session_{uuid.uuid4().hex[:8]}"
            }
            if self.external_id:
                assume_role_kwargs["ExternalId"] = self.external_id
            
            credentials = sts_client.assume_role(**assume_role_kwargs)["Credentials"]
            session_kwargs["aws_access_key_id"] = credentials["AccessKeyId"]
            session_kwargs["aws_secret_access_key"] = credentials["SecretAccessKey"]
            session_kwargs["aws_session_token"] = credentials["SessionToken"]
        
        self._session = boto3.Session(**session_kwargs)
        
        self._clients["apigateway"] = self._session.client("apigateway")
        self._clients["apigatewayv2"] = self._session.client("apigatewayv2")
        self._clients["logs"] = self._session.client("logs")
        self._clients["cloudwatch"] = self._session.client("cloudwatch")
        self._clients["lambda"] = self._session.client("lambda")
        self._clients["route53"] = self._session.client("route53")
        self._clients["acm"] = self._session.client("acm")
    
    def _get_client(self, service_name: str):
        """Get a boto3 client."""
        with self._lock:
            if service_name not in self._clients:
                if not BOTO3_AVAILABLE:
                    raise RuntimeError("boto3 is not available")
                self._clients[service_name] = self._session.client(service_name)
            return self._clients[service_name]
    
    def _generate_id(self, prefix: str = "") -> str:
        """Generate a unique ID."""
        return f"{prefix}{uuid.uuid4().hex[:8]}"
    
    # ==================== REST API Management ====================
    
    def create_rest_api(
        self,
        config: RESTAPIConfig
    ) -> Dict[str, Any]:
        """
        Create a REST API.
        
        Args:
            config: REST API configuration
            
        Returns:
            Created API details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                api_id = self._generate_id("rest-")
                api = {
                    "id": api_id,
                    "name": config.name,
                    "description": config.description,
                    "version": config.version,
                    "binaryMediaTypes": config.binary_media_types,
                    "minimumCompressionSize": config.minimum_compression_size,
                    "apiKeySource": config.api_key_source,
                    "endpointConfiguration": config.endpoint_configuration,
                    "createdTimestamp": datetime.utcnow().isoformat()
                }
                self._apis[api_id] = api
                logger.info(f"Created REST API: {config.name} ({api_id})")
                return api
            
            client = self._get_client("apigateway")
            params = {
                "name": config.name
            }
            if config.description:
                params["description"] = config.description
            if config.version:
                params["version"] = config.version
            if config.binary_media_types:
                params["binaryMediaTypes"] = config.binary_media_types
            if config.minimum_compression_size is not None:
                params["minimumCompressionSize"] = config.minimum_compression_size
            if config.api_key_source:
                params["apiKeySource"] = config.api_key_source
            if config.endpoint_configuration:
                params["endpointConfiguration"] = config.endpoint_configuration
            
            response = client.create_rest_api(**params)
            api_id = response["id"]
            
            if config.tags:
                client.tag_resource(
                    resourceArn=self._get_rest_api_arn(api_id),
                    tags=config.tags
                )
            
            api = {
                "id": api_id,
                "name": response["name"],
                "description": response.get("description"),
                "version": response.get("version"),
                "binaryMediaTypes": response.get("binaryMediaTypes", []),
                "minimumCompressionSize": response.get("minimumCompressionSize"),
                "apiKeySource": response.get("apiKeySource"),
                "endpointConfiguration": response.get("endpointConfiguration"),
                "createdTimestamp": response.get("createdTimestamp").isoformat()
                    if hasattr(response.get("createdTimestamp"), "isoformat")
                    else str(response.get("createdTimestamp"))
            }
            self._apis[api_id] = api
            logger.info(f"Created REST API: {config.name} ({api_id})")
            return api
    
    def get_rest_api(self, api_id: str) -> Dict[str, Any]:
        """
        Get a REST API by ID.
        
        Args:
            api_id: API ID
            
        Returns:
            API details
        """
        with self._lock:
            if api_id in self._apis:
                return self._apis[api_id]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"REST API not found: {api_id}")
            
            client = self._get_client("apigateway")
            response = client.get_rest_api(restApiId=api_id)
            api = {
                "id": response["id"],
                "name": response["name"],
                "description": response.get("description"),
                "version": response.get("version"),
                "binaryMediaTypes": response.get("binaryMediaTypes", []),
                "minimumCompressionSize": response.get("minimumCompressionSize"),
                "apiKeySource": response.get("apiKeySource"),
                "endpointConfiguration": response.get("endpointConfiguration")
            }
            self._apis[api_id] = api
            return api
    
    def list_rest_apis(
        self,
        limit: int = 100,
        position: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List REST APIs.
        
        Args:
            limit: Maximum number of APIs to return
            position: Pagination token
            
        Returns:
            List of APIs and pagination info
        """
        if not BOTO3_AVAILABLE:
            return {"items": list(self._apis.values()), "position": None}
        
        client = self._get_client("apigateway")
        params = {"limit": limit}
        if position:
            params["position"] = position
        
        response = client.get_rest_apis(**params)
        return {
            "items": response.get("items", []),
            "position": response.get("position")
        }
    
    def update_rest_api(
        self,
        api_id: str,
        patch_operations: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Update a REST API.
        
        Args:
            api_id: API ID
            patch_operations: List of patch operations
            
        Returns:
            Updated API details
        """
        if not BOTO3_AVAILABLE:
            if api_id in self._apis:
                self._apis[api_id].update({"patched": True})
                return self._apis[api_id]
            raise ValueError(f"REST API not found: {api_id}")
        
        client = self._get_client("apigateway")
        response = client.update_rest_api(
            restApiId=api_id,
            patchOperations=patch_operations
        )
        api = {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description")
        }
        self._apis[api_id] = api
        return api
    
    def delete_rest_api(self, api_id: str) -> bool:
        """
        Delete a REST API.
        
        Args:
            api_id: API ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_id in self._apis:
                    del self._apis[api_id]
                    logger.info(f"Deleted REST API: {api_id}")
                    return True
                return False
            
            client = self._get_client("apigateway")
            client.delete_rest_api(restApiId=api_id)
            
            if api_id in self._apis:
                del self._apis[api_id]
            
            logger.info(f"Deleted REST API: {api_id}")
            return True
    
    # ==================== HTTP API Management ====================
    
    def create_http_api(
        self,
        config: HTTPAPIConfig
    ) -> Dict[str, Any]:
        """
        Create an HTTP API.
        
        Args:
            config: HTTP API configuration
            
        Returns:
            Created API details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                api_id = self._generate_id("http-")
                api = {
                    "apiId": api_id,
                    "name": config.name,
                    "description": config.description,
                    "version": config.version,
                    "protocols": config.protocols,
                    "createdAt": datetime.utcnow().isoformat()
                }
                self._apis[f"http-{api_id}"] = api
                logger.info(f"Created HTTP API: {config.name} ({api_id})")
                return api
            
            client = self._get_client("apigatewayv2")
            params = {
                "name": config.name,
                "protocols": config.protocols
            }
            if config.description:
                params["description"] = config.description
            if config.version:
                params["version"] = config.version
            
            response = client.create_api(**params)
            api_id = response["ApiId"]
            
            if config.tags:
                client.tag_resource(
                    ResourceArn=self._get_http_api_arn(api_id),
                    Tags=config.tags
                )
            
            api = {
                "apiId": api_id,
                "name": response["Name"],
                "description": response.get("Description"),
                "version": response.get("Version"),
                "protocols": response.get("Protocols", []),
                "apiEndpoint": response.get("ApiEndpoint"),
                "createdAt": response.get("CreatedDate").isoformat()
                    if hasattr(response.get("CreatedDate"), "isoformat")
                    else str(response.get("CreatedDate"))
            }
            self._apis[f"http-{api_id}"] = api
            logger.info(f"Created HTTP API: {config.name} ({api_id})")
            return api
    
    def get_http_api(self, api_id: str) -> Dict[str, Any]:
        """
        Get an HTTP API by ID.
        
        Args:
            api_id: API ID
            
        Returns:
            API details
        """
        if not BOTO3_AVAILABLE:
            key = f"http-{api_id}"
            if key in self._apis:
                return self._apis[key]
            raise ValueError(f"HTTP API not found: {api_id}")
        
        client = self._get_client("apigatewayv2")
        response = client.get_api(ApiId=api_id)
        return {
            "apiId": response["ApiId"],
            "name": response["Name"],
            "description": response.get("Description"),
            "version": response.get("Version"),
            "protocols": response.get("Protocols", []),
            "apiEndpoint": response.get("ApiEndpoint")
        }
    
    def list_http_apis(
        self,
        max_results: int = 100
    ) -> Dict[str, Any]:
        """
        List HTTP APIs.
        
        Args:
            max_results: Maximum number of APIs to return
            
        Returns:
            List of APIs
        """
        if not BOTO3_AVAILABLE:
            return {"Items": [v for k, v in self._apis.items() if k.startswith("http-")]}
        
        client = self._get_client("apigatewayv2")
        response = client.get_apis(MaxResults=str(max_results))
        return {"Items": response.get("Items", [])}
    
    def delete_http_api(self, api_id: str) -> bool:
        """
        Delete an HTTP API.
        
        Args:
            api_id: API ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                key = f"http-{api_id}"
                if key in self._apis:
                    del self._apis[key]
                    logger.info(f"Deleted HTTP API: {api_id}")
                    return True
                return False
            
            client = self._get_client("apigatewayv2")
            client.delete_api(ApiId=api_id)
            
            key = f"http-{api_id}"
            if key in self._apis:
                del self._apis[key]
            
            logger.info(f"Deleted HTTP API: {api_id}")
            return True
    
    # ==================== WebSocket API Management ====================
    
    def create_websocket_api(
        self,
        config: WebSocketAPIConfig
    ) -> Dict[str, Any]:
        """
        Create a WebSocket API.
        
        Args:
            config: WebSocket API configuration
            
        Returns:
            Created API details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                api_id = self._generate_id("ws-")
                api = {
                    "apiId": api_id,
                    "name": config.name,
                    "description": config.description,
                    "routeSelectionExpression": config.route_selection_expression,
                    "createdDate": datetime.utcnow().isoformat()
                }
                self._apis[f"ws-{api_id}"] = api
                logger.info(f"Created WebSocket API: {config.name} ({api_id})")
                return api
            
            client = self._get_client("apigatewayv2")
            params = {
                "name": config.name,
                "protocol": "WEBSOCKET",
                "routeSelectionExpression": config.route_selection_expression
            }
            if config.description:
                params["description"] = config.description
            
            response = client.create_api(**params)
            api_id = response["ApiId"]
            
            if config.tags:
                client.tag_resource(
                    ResourceArn=self._get_websocket_api_arn(api_id),
                    Tags=config.tags
                )
            
            api = {
                "apiId": api_id,
                "name": response["Name"],
                "description": response.get("Description"),
                "routeSelectionExpression": response.get("RouteSelectionExpression"),
                "apiEndpoint": response.get("ApiEndpoint"),
                "createdDate": response.get("CreatedDate").isoformat()
                    if hasattr(response.get("CreatedDate"), "isoformat")
                    else str(response.get("CreatedDate"))
            }
            self._apis[f"ws-{api_id}"] = api
            logger.info(f"Created WebSocket API: {config.name} ({api_id})")
            return api
    
    def add_websocket_route(
        self,
        api_id: str,
        route_key: str,
        integration_uri: Optional[str] = None,
        authorization_type: str = "NONE"
    ) -> Dict[str, Any]:
        """
        Add a route to a WebSocket API.
        
        Args:
            api_id: API ID
            route_key: Route key (e.g., "$connect", "$disconnect", "message")
            integration_uri: Integration URI (e.g., Lambda ARN)
            authorization_type: Authorization type
            
        Returns:
            Route details
        """
        if not BOTO3_AVAILABLE:
            return {
                "routeKey": route_key,
                "apiId": api_id,
                "integrationId": self._generate_id("int-")
            }
        
        client = self._get_client("apigatewayv2")
        
        integration_params = {
            "ApiId": api_id,
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": integration_uri,
            "PayloadFormatVersion": "1.0"
        }
        
        if integration_uri:
            integration_response = client.create_integration(**integration_params)
            integration_id = integration_response["IntegrationId"]
        else:
            integration_id = None
        
        route_params = {
            "ApiId": api_id,
            "RouteKey": route_key
        }
        
        if integration_id:
            route_params["Target"] = f"integrations/{integration_id}"
        
        if authorization_type != "NONE":
            route_params["AuthorizationType"] = authorization_type
        
        response = client.create_route(**route_params)
        
        return {
            "routeKey": response["RouteKey"],
            "apiId": api_id,
            "integrationId": integration_id,
            "routeId": response.get("RouteId")
        }
    
    # ==================== Resource Management ====================
    
    def create_resource(
        self,
        api_id: str,
        path_part: str,
        parent_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a resource in a REST API.
        
        Args:
            api_id: API ID
            path_part: Path part for the resource
            parent_id: Parent resource ID
            
        Returns:
            Created resource details
        """
        if not BOTO3_AVAILABLE:
            resource_id = self._generate_id("res-")
            resource = {
                "id": resource_id,
                "apiId": api_id,
                "pathPart": path_part,
                "parentId": parent_id,
                "path": f"/{path_part}"
            }
            self._resources[f"{api_id}/{resource_id}"] = resource
            return resource
        
        client = self._get_client("apigateway")
        
        if parent_id is None:
            rest_api = self.get_rest_api(api_id)
            parent_id = rest_api.get("rootResourceId", "")
        
        response = client.create_resource(
            restApiId=api_id,
            parentId=parent_id,
            pathPart=path_part
        )
        
        resource_id = response["id"]
        resource = {
            "id": resource_id,
            "apiId": api_id,
            "pathPart": response["pathPart"],
            "parentId": response.get("parentId"),
            "path": response["path"]
        }
        self._resources[f"{api_id}/{resource_id}"] = resource
        logger.info(f"Created resource: {path_part} in API {api_id}")
        return resource
    
    def get_resource(self, api_id: str, resource_id: str) -> Dict[str, Any]:
        """
        Get a resource by ID.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            
        Returns:
            Resource details
        """
        key = f"{api_id}/{resource_id}"
        if key in self._resources:
            return self._resources[key]
        
        if not BOTO3_AVAILABLE:
            raise ValueError(f"Resource not found: {resource_id}")
        
        client = self._get_client("apigateway")
        response = client.get_resource(restApiId=api_id, resourceId=resource_id)
        return {
            "id": response["id"],
            "apiId": api_id,
            "pathPart": response["pathPart"],
            "parentId": response.get("parentId"),
            "path": response["path"]
        }
    
    def list_resources(
        self,
        api_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List resources in a REST API.
        
        Args:
            api_id: API ID
            limit: Maximum number of resources to return
            
        Returns:
            List of resources
        """
        if not BOTO3_AVAILABLE:
            return [v for k, v in self._resources.items() if k.startswith(f"{api_id}/")]
        
        client = self._get_client("apigateway")
        response = client.get_resources(restApiId=api_id, limit=limit)
        return response.get("items", [])
    
    # ==================== Method Management ====================
    
    def create_method(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        authorization_type: str = "NONE",
        api_key_required: bool = False,
        request_parameters: Optional[Dict[str, bool]] = None,
        request_models: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a method for a resource.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            http_method: HTTP method (GET, POST, etc.)
            authorization_type: Authorization type
            api_key_required: Whether API key is required
            request_parameters: Request parameters
            request_models: Request models
            
        Returns:
            Created method details
        """
        if not BOTO3_AVAILABLE:
            return {
                "resourceId": resource_id,
                "httpMethod": http_method,
                "authorizationType": authorization_type,
                "apiKeyRequired": api_key_required,
                "methodId": self._generate_id("mth-")
            }
        
        client = self._get_client("apigateway")
        params = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method.upper(),
            "authorizationType": authorization_type
        }
        
        if api_key_required:
            params["apiKeyRequired"] = api_key_required
        
        if request_parameters:
            params["requestParameters"] = request_parameters
        
        if request_models:
            params["requestModels"] = request_models
        
        response = client.put_method(**params)
        
        logger.info(f"Created {http_method} method on resource {resource_id} in API {api_id}")
        return {
            "resourceId": resource_id,
            "httpMethod": response["httpMethod"],
            "authorizationType": response["authorizationType"],
            "apiKeyRequired": response.get("apiKeyRequired", False),
            "methodId": response.get("id")
        }
    
    def create_method_response(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        status_codes: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a method response.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            http_method: HTTP method
            status_codes: Response status codes configuration
            
        Returns:
            Method response details
        """
        if not BOTO3_AVAILABLE:
            return {
                "resourceId": resource_id,
                "httpMethod": http_method,
                "statusCodes": status_codes or {}
            }
        
        client = self._get_client("apigateway")
        params = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method.upper()
        }
        
        if status_codes:
            params["responseParameters"] = {
                f"method.response.header.{k}": v.get("headerCustom", True)
                for k, v in status_codes.items()
            }
            params["responseModels"] = {
                v.get("model", "Empty"): k
                for k, v in status_codes.items()
            }
        
        response = client.put_method_response(**params)
        
        return {
            "resourceId": resource_id,
            "httpMethod": response["httpMethod"],
            "statusCode": response.get("statusCode")
        }
    
    # ==================== Integration Management ====================
    
    def create_integration(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        integration_type: str,
        uri: Optional[str] = None,
        integration_http_method: Optional[str] = None,
        passthrough_behavior: str = "WHEN_NO_MATCH",
        content_handling: Optional[str] = None,
        timeout_milliseconds: int = 29000
    ) -> Dict[str, Any]:
        """
        Create an integration for a method.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            http_method: HTTP method
            integration_type: Integration type (LAMBDA, HTTP, AWS, etc.)
            uri: Integration URI
            integration_http_method: Integration HTTP method (for Lambda/ECS)
            passthrough_behavior: Passthrough behavior
            content_handling: Content handling strategy
            timeout_milliseconds: Integration timeout
            
        Returns:
            Created integration details
        """
        if not BOTO3_AVAILABLE:
            return {
                "resourceId": resource_id,
                "httpMethod": http_method,
                "integrationType": integration_type,
                "uri": uri,
                "integrationId": self._generate_id("int-")
            }
        
        client = self._get_client("apigateway")
        params = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method.upper(),
            "type": integration_type.upper()
        }
        
        if uri:
            params["uri"] = uri
        
        if integration_http_method:
            params["integrationHttpMethod"] = integration_http_method
        
        if passthrough_behavior:
            params["passthroughBehavior"] = passthrough_behavior
        
        if content_handling:
            params["contentHandling"] = content_handling
        
        if timeout_milliseconds:
            params["timeoutInMillis"] = timeout_milliseconds
        
        response = client.put_integration(**params)
        
        logger.info(f"Created {integration_type} integration for {http_method} on resource {resource_id}")
        return {
            "resourceId": resource_id,
            "httpMethod": http_method,
            "integrationType": response["type"],
            "integrationId": response.get("id"),
            "uri": response.get("uri"),
            "integrationHttpMethod": response.get("integrationHttpMethod")
        }
    
    def create_integration_response(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        status_code: str = "200",
        selection_pattern: Optional[str] = None,
        response_parameters: Optional[Dict[str, str]] = None,
        response_templates: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create an integration response.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            http_method: HTTP method
            status_code: Response status code
            selection_pattern: Selection pattern for regex matching
            response_parameters: Response parameter mappings
            response_templates: Response templates
            
        Returns:
            Integration response details
        """
        if not BOTO3_AVAILABLE:
            return {
                "resourceId": resource_id,
                "httpMethod": http_method,
                "statusCode": status_code
            }
        
        client = self._get_client("apigateway")
        params = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method.upper(),
            "statusCode": status_code
        }
        
        if selection_pattern:
            params["selectionPattern"] = selection_pattern
        
        if response_parameters:
            params["responseParameters"] = response_parameters
        
        if response_templates:
            params["responseTemplates"] = response_templates
        
        response = client.put_integration_response(**params)
        
        return {
            "resourceId": resource_id,
            "httpMethod": http_method,
            "statusCode": response.get("statusCode")
        }
    
    # ==================== HTTP API Integration (v2) ====================
    
    def create_http_api_integration(
        self,
        api_id: str,
        integration_type: str,
        integration_uri: Optional[str] = None,
        integration_method: str = "GET",
        payload_format_version: str = "2.0",
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an integration for an HTTP API (v2).
        
        Args:
            api_id: API ID
            integration_type: Integration type (AWS_PROXY, HTTP_PROXY, etc.)
            integration_uri: Integration URI
            integration_method: Integration method
            payload_format_version: Payload format version
            description: Integration description
            
        Returns:
            Created integration details
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "integrationType": integration_type,
                "integrationUri": integration_uri,
                "integrationId": self._generate_id("int-")
            }
        
        client = self._get_client("apigatewayv2")
        params = {
            "ApiId": api_id,
            "IntegrationType": integration_type,
            "PayloadFormatVersion": payload_format_version
        }
        
        if integration_uri:
            params["IntegrationUri"] = integration_uri
        
        if integration_method:
            params["IntegrationMethod"] = integration_method
        
        if description:
            params["Description"] = description
        
        response = client.create_integration(**params)
        
        return {
            "apiId": api_id,
            "integrationType": response["IntegrationType"],
            "integrationId": response.get("IntegrationId"),
            "integrationUri": response.get("IntegrationUri"),
            "integrationMethod": response.get("IntegrationMethod")
        }
    
    def create_http_api_route(
        self,
        api_id: str,
        route_key: str,
        target: str,
        authorization_type: str = "NONE"
    ) -> Dict[str, Any]:
        """
        Create a route for an HTTP API.
        
        Args:
            api_id: API ID
            route_key: Route key (e.g., "GET /items")
            target: Target integration ID
            authorization_type: Authorization type
            
        Returns:
            Created route details
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "routeKey": route_key,
                "routeId": self._generate_id("rt-")
            }
        
        client = self._get_client("apigatewayv2")
        response = client.create_route(
            ApiId=api_id,
            RouteKey=route_key,
            Target=target,
            AuthorizationType=authorization_type
        )
        
        return {
            "apiId": api_id,
            "routeKey": response["RouteKey"],
            "routeId": response.get("RouteId")
        }
    
    # ==================== Lambda Integration ====================
    
    def create_lambda_integration(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        function_name: str,
        alias_or_version: Optional[str] = None,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a Lambda proxy integration.
        
        Args:
            api_id: API ID
            resource_id: Resource ID
            http_method: HTTP method
            function_name: Lambda function name
            alias_or_version: Lambda alias or version
            region: AWS region (defaults to self.region)
            
        Returns:
            Created integration details
        """
        region = region or self.region
        lambda_uri = f"arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/arn:aws:lambda:{region}:"
        lambda_uri += f"{self._get_account_id() if BOTO3_AVAILABLE else '123456789'}:function:{function_name}"
        
        if alias_or_version:
            lambda_uri += f":{alias_or_version}"
        lambda_uri += "/invocations"
        
        return self.create_integration(
            api_id=api_id,
            resource_id=resource_id,
            http_method=http_method,
            integration_type="AWS_PROXY",
            uri=lambda_uri,
            integration_http_method="POST"
        )
    
    def add_lambda_permission(
        self,
        api_id: str,
        function_name: str,
        source_arn: Optional[str] = None,
        statement_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add Lambda permission for API Gateway invocation.
        
        Args:
            api_id: API ID
            function_name: Lambda function name
            source_arn: Source ARN (optional)
            statement_id: Statement ID (optional)
            
        Returns:
            Permission addition result
        """
        if not BOTO3_AVAILABLE:
            return {
                "function_name": function_name,
                "api_id": api_id,
                "StatementId": statement_id or self._generate_id("stmt-")
            }
        
        lambda_client = self._get_client("lambda")
        
        if source_arn is None:
            source_arn = f"arn:aws:execute-api:{self.region}:{self._get_account_id()}:{api_id}/*/*"
        
        params = {
            "FunctionName": function_name,
            "StatementId": statement_id or f"api-gateway-{api_id}",
            "Action": "lambda:InvokeFunction",
            "Principal": "apigateway.amazonaws.com",
            "SourceArn": source_arn
        }
        
        try:
            response = lambda_client.add_permission(**params)
            logger.info(f"Added Lambda permission for API Gateway: {api_id}")
            return {"StatementId": response.get("Statement")}
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                logger.warning(f"Lambda permission already exists for: {function_name}")
                return {"StatementId": params["StatementId"], "AlreadyExists": True}
            raise
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        sts_client = self._get_client("sts") if BOTO3_AVAILABLE else None
        if sts_client:
            return sts_client.get_caller_identity()["Account"]
        return "123456789"
    
    # ==================== Stage Management ====================
    
    def create_stage(
        self,
        api_id: str,
        config: StageConfig,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Create a stage for an API.
        
        Args:
            api_id: API ID
            config: Stage configuration
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Created stage details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                stage = {
                    "apiId": api_id,
                    "stageName": config.stage_name,
                    "description": config.description,
                    "deploymentId": config.deployment_id,
                    "variables": config.variables,
                    "createdAt": datetime.utcnow().isoformat()
                }
                self._stages[f"{api_id}/{config.stage_name}"] = stage
                logger.info(f"Created stage: {config.stage_name} for API {api_id}")
                return stage
            
            if is_http_api:
                client = self._get_client("apigatewayv2")
                params = {
                    "ApiId": api_id,
                    "StageName": config.stage_name
                }
                if config.description:
                    params["Description"] = config.description
                if config.deployment_id:
                    params["DeploymentId"] = config.deployment_id
                if config.variables:
                    params["StageVariables"] = config.variables
                if config.access_log_settings:
                    params["AccessLogSettings"] = config.access_log_settings
                
                response = client.create_stage(**params)
                
                stage = {
                    "apiId": api_id,
                    "stageName": response["StageName"],
                    "description": response.get("Description"),
                    "deploymentId": response.get("DeploymentId"),
                    "stageVariables": response.get("StageVariables", {}),
                    "createdAt": response.get("CreatedDate").isoformat()
                        if hasattr(response.get("CreatedDate"), "isoformat")
                        else str(response.get("CreatedDate"))
                }
            else:
                client = self._get_client("apigateway")
                params = {
                    "restApiId": api_id,
                    "stageName": config.stage_name
                }
                if config.description:
                    params["description"] = config.description
                if config.deployment_id:
                    params["deploymentId"] = config.deployment_id
                if config.variables:
                    params["variables"] = config.variables
                if config.access_log_settings:
                    params["accessLogSettings"] = config.access_log_settings
                if config.canary_settings:
                    params["canarySettings"] = config.canary_settings
                if config.tracing_enabled:
                    params["tracingEnabled"] = config.tracing_enabled
                
                response = client.create_stage(**params)
                
                stage = {
                    "apiId": api_id,
                    "stageName": response["stageName"],
                    "description": response.get("description"),
                    "deploymentId": response.get("deploymentId"),
                    "variables": response.get("variables", {}),
                    "createdAt": response.get("createdDate").isoformat()
                        if hasattr(response.get("createdDate"), "isoformat")
                        else str(response.get("createdDate"))
                }
            
            self._stages[f"{api_id}/{config.stage_name}"] = stage
            logger.info(f"Created stage: {config.stage_name} for API {api_id}")
            return stage
    
    def get_stage(
        self,
        api_id: str,
        stage_name: str,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Get a stage by name.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Stage details
        """
        if not BOTO3_AVAILABLE:
            key = f"{api_id}/{stage_name}"
            if key in self._stages:
                return self._stages[key]
            raise ValueError(f"Stage not found: {stage_name}")
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            response = client.get_stage(ApiId=api_id, StageName=stage_name)
            return {
                "apiId": api_id,
                "stageName": response["StageName"],
                "description": response.get("Description"),
                "deploymentId": response.get("DeploymentId"),
                "stageVariables": response.get("StageVariables", {}),
                "apiEndpoint": response.get("ApiEndpoint")
            }
        else:
            client = self._get_client("apigateway")
            response = client.get_stage(restApiId=api_id, stageName=stage_name)
            return {
                "apiId": api_id,
                "stageName": response["stageName"],
                "description": response.get("description"),
                "deploymentId": response.get("deploymentId"),
                "variables": response.get("variables", {}),
                "methodSettings": response.get("methodSettings", {})
            }
    
    def list_stages(
        self,
        api_id: str,
        is_http_api: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List stages for an API.
        
        Args:
            api_id: API ID
            is_http_api: Whether this is an HTTP API
            
        Returns:
            List of stages
        """
        if not BOTO3_AVAILABLE:
            return [v for k, v in self._stages.items() if k.startswith(f"{api_id}/")]
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            response = client.get_stages(ApiId=api_id)
            return response.get("Items", [])
        else:
            client = self._get_client("apigateway")
            response = client.get_stages(restApiId=api_id)
            return response.get("item", [])
    
    def update_stage(
        self,
        api_id: str,
        stage_name: str,
        patch_operations: List[Dict[str, str]],
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Update a stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            patch_operations: List of patch operations
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Updated stage details
        """
        if not BOTO3_AVAILABLE:
            key = f"{api_id}/{stage_name}"
            if key in self._stages:
                self._stages[key].update({"patched": True})
                return self._stages[key]
            raise ValueError(f"Stage not found: {stage_name}")
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            response = client.update_stage(
                ApiId=api_id,
                StageName=stage_name,
                PatchOperations=patch_operations
            )
            return {
                "apiId": api_id,
                "stageName": response["StageName"],
                "description": response.get("Description")
            }
        else:
            client = self._get_client("apigateway")
            response = client.update_stage(
                restApiId=api_id,
                stageName=stage_name,
                patchOperations=patch_operations
            )
            return {
                "apiId": api_id,
                "stageName": response["stageName"],
                "description": response.get("description")
            }
    
    def delete_stage(
        self,
        api_id: str,
        stage_name: str,
        is_http_api: bool = False
    ) -> bool:
        """
        Delete a stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            is_http_api: Whether this is an HTTP API
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                key = f"{api_id}/{stage_name}"
                if key in self._stages:
                    del self._stages[key]
                    logger.info(f"Deleted stage: {stage_name} from API {api_id}")
                    return True
                return False
            
            if is_http_api:
                client = self._get_client("apigatewayv2")
                client.delete_stage(ApiId=api_id, StageName=stage_name)
            else:
                client = self._get_client("apigateway")
                client.delete_stage(restApiId=api_id, stageName=stage_name)
            
            key = f"{api_id}/{stage_name}"
            if key in self._stages:
                del self._stages[key]
            
            logger.info(f"Deleted stage: {stage_name} from API {api_id}")
            return True
    
    # ==================== Deployment Management ====================
    
    def create_deployment(
        self,
        api_id: str,
        stage_name: Optional[str] = None,
        stage_description: Optional[str] = None,
        description: Optional[str] = None,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Create a deployment for an API.
        
        Args:
            api_id: API ID
            stage_name: Stage name to deploy to
            stage_description: Stage description
            description: Deployment description
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Created deployment details
        """
        if not BOTO3_AVAILABLE:
            deployment_id = self._generate_id("deploy-")
            return {
                "apiId": api_id,
                "deploymentId": deployment_id,
                "description": description,
                "createdAt": datetime.utcnow().isoformat()
            }
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            params = {"ApiId": api_id}
            if description:
                params["Description"] = description
            
            response = client.create_deployment(**params)
            deployment_id = response.get("DeploymentId")
            
            if stage_name:
                stage_config = StageConfig(
                    stage_name=stage_name,
                    description=stage_description,
                    deployment_id=deployment_id
                )
                self.create_stage(api_id, stage_config, is_http_api=True)
            
            return {
                "apiId": api_id,
                "deploymentId": deployment_id,
                "description": response.get("Description"),
                "createdAt": response.get("CreatedDate").isoformat()
                    if hasattr(response.get("CreatedDate"), "isoformat")
                    else str(response.get("CreatedDate"))
            }
        else:
            client = self._get_client("apigateway")
            params = {"restApiId": api_id}
            if description:
                params["description"] = description
            
            response = client.create_deployment(**params)
            deployment_id = response.get("id")
            
            if stage_name:
                stage_config = StageConfig(
                    stage_name=stage_name,
                    description=stage_description,
                    deployment_id=deployment_id
                )
                self.create_stage(api_id, stage_config, is_http_api=False)
            
            return {
                "apiId": api_id,
                "deploymentId": deployment_id,
                "description": response.get("description"),
                "createdAt": response.get("createdDate").isoformat()
                    if hasattr(response.get("createdDate"), "isoformat")
                    else str(response.get("createdDate"))
            }
    
    # ==================== Custom Domain Management ====================
    
    def create_custom_domain(
        self,
        config: CustomDomainConfig,
        api_id: Optional[str] = None,
        stage: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a custom domain.
        
        Args:
            config: Custom domain configuration
            api_id: API ID to associate
            stage: Stage name to associate
            
        Returns:
            Created domain details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                domain = {
                    "domainName": config.domain_name,
                    "certificateArn": config.certificate_arn,
                    "securityPolicy": config.security_policy,
                    "endpointConfiguration": {"types": [config.endpoint_type]},
                    "domainId": self._generate_id("domain-")
                }
                self._custom_domains[config.domain_name] = domain
                logger.info(f"Created custom domain: {config.domain_name}")
                return domain
            
            client = self._get_client("apigateway")
            
            try:
                response = client.create_domain_name(
                    domainName=config.domain_name,
                    certificateArn=config.certificate_arn,
                    securityPolicy=config.security_policy,
                    endpointConfiguration={
                        "types": [config.endpoint_type]
                    }
                )
                
                domain = {
                    "domainName": response["domainName"],
                    "certificateArn": response.get("certificateArn"),
                    "securityPolicy": response.get("securityPolicy"),
                    "endpointConfiguration": response.get("endpointConfiguration"),
                    "domainId": response.get("domainName")
                }
                self._custom_domains[config.domain_name] = domain
                
                if api_id and stage:
                    client.create_base_path_mapping(
                        domainName=config.domain_name,
                        restApiId=api_id,
                        stage=stage
                    )
                
                logger.info(f"Created custom domain: {config.domain_name}")
                return domain
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConflictException":
                    logger.warning(f"Custom domain already exists: {config.domain_name}")
                    return self._custom_domains.get(config.domain_name, {
                        "domainName": config.domain_name
                    })
                raise
    
    def get_custom_domain(self, domain_name: str) -> Dict[str, Any]:
        """
        Get a custom domain.
        
        Args:
            domain_name: Domain name
            
        Returns:
            Domain details
        """
        if domain_name in self._custom_domains:
            return self._custom_domains[domain_name]
        
        if not BOTO3_AVAILABLE:
            raise ValueError(f"Custom domain not found: {domain_name}")
        
        client = self._get_client("apigateway")
        response = client.get_domain_name(domainName=domain_name)
        return {
            "domainName": response["domainName"],
            "certificateArn": response.get("certificateArn"),
            "securityPolicy": response.get("securityPolicy"),
            "endpointConfiguration": response.get("endpointConfiguration")
        }
    
    def create_base_path_mapping(
        self,
        domain_name: str,
        api_id: str,
        stage: str,
        base_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a base path mapping.
        
        Args:
            domain_name: Domain name
            api_id: API ID
            stage: Stage name
            base_path: Base path
            
        Returns:
            Created mapping details
        """
        if not BOTO3_AVAILABLE:
            return {
                "domainName": domain_name,
                "restApiId": api_id,
                "stage": stage,
                "basePath": base_path or "(none)"
            }
        
        client = self._get_client("apigateway")
        params = {
            "domainName": domain_name,
            "restApiId": api_id,
            "stage": stage
        }
        if base_path:
            params["basePath"] = base_path
        
        response = client.create_base_path_mapping(**params)
        return {
            "domainName": response["domainName"],
            "restApiId": response["restApiId"],
            "stage": response["stage"],
            "basePath": response.get("basePath", "(none)")
        }
    
    def delete_custom_domain(self, domain_name: str) -> bool:
        """
        Delete a custom domain.
        
        Args:
            domain_name: Domain name
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if domain_name in self._custom_domains:
                    del self._custom_domains[domain_name]
                    logger.info(f"Deleted custom domain: {domain_name}")
                    return True
                return False
            
            client = self._get_client("apigateway")
            try:
                client.delete_domain_name(domainName=domain_name)
                if domain_name in self._custom_domains:
                    del self._custom_domains[domain_name]
                logger.info(f"Deleted custom domain: {domain_name}")
                return True
            except ClientError:
                return False
    
    # ==================== API Key Management ====================
    
    def create_api_key(
        self,
        config: APIKeyConfig
    ) -> Dict[str, Any]:
        """
        Create an API key.
        
        Args:
            config: API key configuration
            
        Returns:
            Created API key details
        """
        if not BOTO3_AVAILABLE:
            api_key = {
                "id": self._generate_id("key-"),
                "name": config.name,
                "description": config.description,
                "enabled": config.enabled,
                "value": config.value or self._generate_id(""),
                "createdAt": datetime.utcnow().isoformat()
            }
            self._api_keys[api_key["id"]] = api_key
            logger.info(f"Created API key: {config.name}")
            return api_key
        
        client = self._get_client("apigateway")
        params = {
            "name": config.name,
            "enabled": config.enabled
        }
        
        if config.description:
            params["description"] = config.description
        
        if config.value:
            params["value"] = config.value
        
        if not config.generate_distinct_id:
            params["generateDistinctId"] = config.generate_distinct_id
        
        response = client.create_api_key(**params)
        
        api_key = {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description"),
            "enabled": response["enabled"],
            "value": response.get("value"),
            "createdDate": response.get("createdDate").isoformat()
                if hasattr(response.get("createdDate"), "isoformat")
                else str(response.get("createdDate"))
        }
        self._api_keys[api_key["id"]] = api_key
        logger.info(f"Created API key: {config.name}")
        return api_key
    
    def get_api_key(self, api_key_id: str) -> Dict[str, Any]:
        """
        Get an API key.
        
        Args:
            api_key_id: API key ID
            
        Returns:
            API key details
        """
        if api_key_id in self._api_keys:
            return self._api_keys[api_key_id]
        
        if not BOTO3_AVAILABLE:
            raise ValueError(f"API key not found: {api_key_id}")
        
        client = self._get_client("apigateway")
        response = client.get_api_key(apiKeyId=api_key_id)
        return {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description"),
            "enabled": response["enabled"],
            "value": response.get("value")
        }
    
    def list_api_keys(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List API keys.
        
        Args:
            limit: Maximum number of keys to return
            
        Returns:
            List of API keys
        """
        if not BOTO3_AVAILABLE:
            return list(self._api_keys.values())
        
        client = self._get_client("apigateway")
        response = client.get_api_keys(limit=limit)
        return response.get("items", [])
    
    def update_api_key(
        self,
        api_key_id: str,
        patch_operations: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Update an API key.
        
        Args:
            api_key_id: API key ID
            patch_operations: List of patch operations
            
        Returns:
            Updated API key details
        """
        if not BOTO3_AVAILABLE:
            if api_key_id in self._api_keys:
                self._api_keys[api_key_id].update({"patched": True})
                return self._api_keys[api_key_id]
            raise ValueError(f"API key not found: {api_key_id}")
        
        client = self._get_client("apigateway")
        response = client.update_api_key(
            apiKeyId=api_key_id,
            patchOperations=patch_operations
        )
        return {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description"),
            "enabled": response["enabled"]
        }
    
    def delete_api_key(self, api_key_id: str) -> bool:
        """
        Delete an API key.
        
        Args:
            api_key_id: API key ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_key_id in self._api_keys:
                    del self._api_keys[api_key_id]
                    logger.info(f"Deleted API key: {api_key_id}")
                    return True
                return False
            
            client = self._get_client("apigateway")
            try:
                client.delete_api_key(apiKeyId=api_key_id)
                if api_key_id in self._api_keys:
                    del self._api_keys[api_key_id]
                logger.info(f"Deleted API key: {api_key_id}")
                return True
            except ClientError:
                return False
    
    # ==================== Usage Plan Management ====================
    
    def create_usage_plan(
        self,
        config: UsagePlanConfig
    ) -> Dict[str, Any]:
        """
        Create a usage plan.
        
        Args:
            config: Usage plan configuration
            
        Returns:
            Created usage plan details
        """
        if not BOTO3_AVAILABLE:
            usage_plan = {
                "id": self._generate_id("up-"),
                "name": config.name,
                "description": config.description,
                "quota": config.quota,
                "throttle": config.throttle,
                "createdAt": datetime.utcnow().isoformat()
            }
            self._usage_plans[usage_plan["id"]] = usage_plan
            logger.info(f"Created usage plan: {config.name}")
            return usage_plan
        
        client = self._get_client("apigateway")
        params = {"name": config.name}
        
        if config.description:
            params["description"] = config.description
        if config.quota:
            params["quota"] = config.quota
        if config.throttle:
            params["throttle"] = config.throttle
        
        response = client.create_usage_plan(**params)
        
        usage_plan = {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description"),
            "quota": response.get("quota"),
            "throttle": response.get("throttle"),
            "apiStages": response.get("apiStages", [])
        }
        self._usage_plans[usage_plan["id"]] = usage_plan
        logger.info(f"Created usage plan: {config.name}")
        return usage_plan
    
    def get_usage_plan(self, usage_plan_id: str) -> Dict[str, Any]:
        """
        Get a usage plan.
        
        Args:
            usage_plan_id: Usage plan ID
            
        Returns:
            Usage plan details
        """
        if usage_plan_id in self._usage_plans:
            return self._usage_plans[usage_plan_id]
        
        if not BOTO3_AVAILABLE:
            raise ValueError(f"Usage plan not found: {usage_plan_id}")
        
        client = self._get_client("apigateway")
        response = client.get_usage_plan(usagePlanId=usage_plan_id)
        return {
            "id": response["id"],
            "name": response["name"],
            "description": response.get("description"),
            "quota": response.get("quota"),
            "throttle": response.get("throttle"),
            "apiStages": response.get("apiStages", [])
        }
    
    def list_usage_plans(self) -> List[Dict[str, Any]]:
        """
        List usage plans.
        
        Returns:
            List of usage plans
        """
        if not BOTO3_AVAILABLE:
            return list(self._usage_plans.values())
        
        client = self._get_client("apigateway")
        response = client.get_usage_plans()
        return response.get("items", [])
    
    def create_usage_plan_key(
        self,
        usage_plan_id: str,
        key_id: str,
        key_type: str = "API_KEY"
    ) -> Dict[str, Any]:
        """
        Create a usage plan key (associate API key with usage plan).
        
        Args:
            usage_plan_id: Usage plan ID
            key_id: API key ID
            key_type: Key type
            
        Returns:
            Created usage plan key details
        """
        if not BOTO3_AVAILABLE:
            return {
                "id": key_id,
                "type": key_type,
                "value": "mock_value"
            }
        
        client = self._get_client("apigateway")
        response = client.create_usage_plan_key(
            usagePlanId=usage_plan_id,
            keyId=key_id,
            keyType=key_type
        )
        return {
            "id": response["id"],
            "type": response["type"],
            "value": response.get("value")
        }
    
    def delete_usage_plan(self, usage_plan_id: str) -> bool:
        """
        Delete a usage plan.
        
        Args:
            usage_plan_id: Usage plan ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if usage_plan_id in self._usage_plans:
                    del self._usage_plans[usage_plan_id]
                    logger.info(f"Deleted usage plan: {usage_plan_id}")
                    return True
                return False
            
            client = self._get_client("apigateway")
            try:
                client.delete_usage_plan(usagePlanId=usage_plan_id)
                if usage_plan_id in self._usage_plans:
                    del self._usage_plans[usage_plan_id]
                logger.info(f"Deleted usage plan: {usage_plan_id}")
                return True
            except ClientError:
                return False
    
    # ==================== CloudWatch Integration ====================
    
    def configure_access_logging(
        self,
        api_id: str,
        stage_name: str,
        log_group_arn: str,
        execution_logging: bool = True,
        access_log_format: Optional[str] = None,
        detailed_debugging: bool = False,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Configure access logging for an API stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            log_group_arn: CloudWatch log group ARN
            execution_logging: Enable execution logging
            access_log_format: Access log format (CLF, JSON, XML, or custom)
            detailed_debugging: Enable detailed debugging
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Logging configuration result
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "loggingEnabled": True,
                "logGroupArn": log_group_arn
            }
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            patch_operations = [
                {"op": "replace", "path": "/AccessLogSettings/DeploymentType", "value": "HTTP_API"},
                {"op": "replace", "path": "/AccessLogSettings/LogGroupArn", "value": log_group_arn}
            ]
            response = client.update_stage(
                ApiId=api_id,
                StageName=stage_name,
                PatchOperations=patch_operations
            )
        else:
            client = self._get_client("apigateway")
            
            logging_level = "INFO" if execution_logging else "OFF"
            
            patch_operations = [
                {"op": "replace", "path": "/logging/loglevel", "value": logging_level},
                {"op": "replace", "path": "/logging/dataTrace", "value": str(detailed_debugging).lower()},
                {"op": "replace", "path": "/metrics/enabled", "value": str(execution_logging).lower()}
            ]
            
            if access_log_format:
                patch_operations.append(
                    {"op": "replace", "path": "/logging/accessLogFormat", "value": access_log_format}
                )
            
            response = client.update_stage(
                restApiId=api_id,
                stageName=stage_name,
                patchOperations=patch_operations
            )
            
            try:
                client.update_stage(
                    restApiId=api_id,
                    stageName=stage_name,
                    patchOperations=[
                        {"op": "replace", "path": "/*/*/logging/loglevel", "value": logging_level}
                    ]
                )
            except ClientError:
                pass
        
        logger.info(f"Configured access logging for {api_id}/{stage_name}")
        return {
            "apiId": api_id,
            "stageName": stage_name,
            "loggingEnabled": execution_logging,
            "logGroupArn": log_group_arn
        }
    
    def create_log_group(
        self,
        log_group_name: str,
        retention_days: Optional[int] = None,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch log group.
        
        Args:
            log_group_name: Log group name
            retention_days: Log retention days
            kms_key_id: KMS key ID for encryption
            
        Returns:
            Created log group details
        """
        if not BOTO3_AVAILABLE:
            return {
                "logGroupName": log_group_name,
                "retentionDays": retention_days,
                "created": True
            }
        
        logs_client = self._get_client("logs")
        params = {"logGroupName": log_group_name}
        
        if retention_days:
            params["retentionInDays"] = retention_days
        
        if kms_key_id:
            params["kmsKeyId"] = kms_key_id
        
        try:
            logs_client.create_log_group(**params)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            logger.warning(f"Log group already exists: {log_group_name}")
        
        return {
            "logGroupName": log_group_name,
            "retentionDays": retention_days,
            "kmsKeyId": kms_key_id
        }
    
    def set_throttling(
        self,
        api_id: str,
        stage_name: str,
        burst_limit: int,
        rate_limit: float,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Set throttling limits for a stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            burst_limit: Burst limit (requests per second)
            rate_limit: Rate limit (requests per second)
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Throttling configuration result
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "burstLimit": burst_limit,
                "rateLimit": rate_limit
            }
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            patch_operations = [
                {"op": "replace", "path": "/Throttling/BurstLimit", "value": str(burst_limit)},
                {"op": "replace", "path": "/Throttling/RateLimit", "value": str(rate_limit)}
            ]
            client.update_stage(
                ApiId=api_id,
                StageName=stage_name,
                PatchOperations=patch_operations
            )
        else:
            client = self._get_client("apigateway")
            patch_operations = [
                {"op": "replace", "path": "/*/*/throttling/burstlimit", "value": str(burst_limit)},
                {"op": "replace", "path": "/*/*/throttling/ratelimit", "value": str(rate_limit)}
            ]
            client.update_stage(
                restApiId=api_id,
                stageName=stage_name,
                patchOperations=patch_operations
            )
        
        logger.info(f"Set throttling for {api_id}/{stage_name}: burst={burst_limit}, rate={rate_limit}")
        return {
            "apiId": api_id,
            "stageName": stage_name,
            "burstLimit": burst_limit,
            "rateLimit": rate_limit
        }
    
    def enable_cloudwatch_metrics(
        self,
        api_id: str,
        stage_name: str,
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Enable CloudWatch metrics for a stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Metrics configuration result
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "metricsEnabled": True
            }
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            client.update_stage(
                ApiId=api_id,
                StageName=stage_name,
                PatchOperations=[
                    {"op": "replace", "path": "/Metrics/Enabled", "value": "true"}
                ]
            )
        else:
            client = self._get_client("apigateway")
            client.update_stage(
                restApiId=api_id,
                stageName=stage_name,
                patchOperations=[
                    {"op": "replace", "path": "/*/*/metrics/enabled", "value": "true"}
                ]
            )
        
        logger.info(f"Enabled CloudWatch metrics for {api_id}/{stage_name}")
        return {
            "apiId": api_id,
            "stageName": stage_name,
            "metricsEnabled": True
        }
    
    def get_api_metrics(
        self,
        api_id: str,
        stage_name: str,
        metric_names: List[str],
        period: int = 60,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for an API.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            metric_names: List of metric names to retrieve
            period: Period in seconds
            start_time: Start time
            end_time: End time
            
        Returns:
            CloudWatch metrics data
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "metrics": {m: [] for m in metric_names}
            }
        
        cloudwatch_client = self._get_client("cloudwatch")
        
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        metric_data = []
        for metric_name in metric_names:
            metric_data.append({
                "Id": metric_name.lower().replace(".", "_"),
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/ApiGateway",
                        "MetricName": metric_name,
                        "Dimensions": [
                            {"Name": "ApiName", "Value": api_id},
                            {"Name": "Stage", "Value": stage_name}
                        ]
                    },
                    "Period": period,
                    "Stat": "Sum"
                }
            })
        
        try:
            response = cloudwatch_client.get_metric_data(
                MetricData=metric_data,
                StartTime=start_time,
                EndTime=end_time
            )
            
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "metrics": {
                    result["Id"]: result.get("Values", [])
                    for result in response.get("MetricDataResults", [])
                },
                "timestamps": metric_data[0].get("MetricStat", {}).get("Metric", {}).get("Dimensions", [])
            }
        except ClientError as e:
            logger.error(f"Error getting metrics: {e}")
            return {
                "apiId": api_id,
                "stageName": stage_name,
                "error": str(e)
            }
    
    def create_dashboard(self, dashboard_name: str) -> Dict[str, Any]:
        """
        Create a CloudWatch dashboard for API Gateway metrics.
        
        Args:
            dashboard_name: Dashboard name
            
        Returns:
            Created dashboard details
        """
        if not BOTO3_AVAILABLE:
            return {
                "dashboardName": dashboard_name,
                "created": True
            }
        
        cloudwatch_client = self._get_client("cloudwatch")
        
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/ApiGateway", "Count", {"stat": "Sum"}],
                            [".", "Latency", {"stat": "Average"}]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": self.region,
                        "title": "API Gateway Overview"
                    }
                }
            ]
        }
        
        try:
            cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )
            return {
                "dashboardName": dashboard_name,
                "created": True
            }
        except ClientError:
            return {
                "dashboardName": dashboard_name,
                "created": False
            }
    
    # ==================== ARN Helpers ====================
    
    def _get_rest_api_arn(self, api_id: str) -> str:
        """Get ARN for a REST API."""
        account_id = self._get_account_id() if BOTO3_AVAILABLE else "123456789"
        return f"arn:aws:apigateway:{self.region}:{account_id}:/restapis/{api_id}"
    
    def _get_http_api_arn(self, api_id: str) -> str:
        """Get ARN for an HTTP API."""
        account_id = self._get_account_id() if BOTO3_AVAILABLE else "123456789"
        return f"arn:aws:apigateway:{self.region}:{account_id}:/apis/{api_id}"
    
    def _get_websocket_api_arn(self, api_id: str) -> str:
        """Get ARN for a WebSocket API."""
        return self._get_http_api_arn(api_id)
    
    # ==================== Utility Methods ====================
    
    def export_api(
        self,
        api_id: str,
        export_type: str = "swagger",
        is_http_api: bool = False
    ) -> Dict[str, Any]:
        """
        Export an API definition.
        
        Args:
            api_id: API ID
            export_type: Export type (swagger, oas30)
            is_http_api: Whether this is an HTTP API
            
        Returns:
            Exported API definition
        """
        if not BOTO3_AVAILABLE:
            return {
                "apiId": api_id,
                "exportType": export_type,
                "data": {}
            }
        
        if is_http_api:
            client = self._get_client("apigatewayv2")
            response = client.export_api(
                ApiId=api_id,
                Specification="OAS30",
                OutputType="YAML"
            )
            return {
                "apiId": api_id,
                "exportType": "oas30",
                "data": response.get("body", {}).read().decode() if hasattr(response.get("body"), "read") else str(response.get("body", ""))
            }
        else:
            client = self._get_client("apigateway")
            response = client.export_rest_api(
                restApiId=api_id,
                exportType=export_type,
                mimeType="application/json"
            )
            return {
                "apiId": api_id,
                "exportType": export_type,
                "data": response.get("body", {}).read().decode() if hasattr(response.get("body"), "read") else str(response.get("body", ""))
            }
    
    def import_api(
        self,
        body: str,
        mode: str = "overwrite",
        fail_on_warnings: bool = False
    ) -> Dict[str, Any]:
        """
        Import an API from definition.
        
        Args:
            body: API definition body
            mode: Import mode (overwrite, merge)
            fail_on_warnings: Fail on warnings
            
        Returns:
            Imported API details
        """
        if not BOTO3_AVAILABLE:
            api_id = self._generate_id("imported-")
            return {
                "apiId": api_id,
                "imported": True,
                "warnings": []
            }
        
        client = self._get_client("apigateway")
        response = client.import_rest_api(
            body=body,
            mode=mode,
            failOnWarnings=fail_on_warnings
        )
        
        return {
            "apiId": response["id"],
            "name": response["name"],
            "imported": True,
            "warnings": response.get("warnings", [])
        }
    
    def get_api_endpoint(
        self,
        api_id: str,
        stage_name: str,
        is_http_api: bool = False
    ) -> str:
        """
        Get the API endpoint URL.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            is_http_api: Whether this is an HTTP API
            
        Returns:
            API endpoint URL
        """
        if is_http_api:
            api = self.get_http_api(api_id)
            return f"{api.get('apiEndpoint', '')}/{stage_name}"
        else:
            return f"https://{api_id}.execute-api.{self.region}.amazonaws.com/{stage_name}"
    
    def flush_authorizer_cache(
        self,
        api_id: str,
        authorizer_id: str
    ) -> bool:
        """
        Flush the cache for an authorizer.
        
        Args:
            api_id: API ID
            authorizer_id: Authorizer ID
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            return True
        
        client = self._get_client("apigateway")
        try:
            client.flush_authorizer_cache(
                restApiId=api_id,
                authorizerId=authorizer_id
            )
            return True
        except ClientError:
            return False
    
    def flush_stage_cache(
        self,
        api_id: str,
        stage_name: str
    ) -> bool:
        """
        Flush the cache for a stage.
        
        Args:
            api_id: API ID
            stage_name: Stage name
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            return True
        
        client = self._get_client("apigateway")
        try:
            client.flush_stage_cache(
                restApiId=api_id,
                stageName=stage_name
            )
            return True
        except ClientError:
            return False
    
    def close(self) -> None:
        """Close all client connections."""
        with self._lock:
            self._clients.clear()
            logger.info("Closed API Gateway integration")
