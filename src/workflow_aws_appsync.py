"""
AWS AppSync Integration Module for Workflow System

Implements an AppSyncIntegration class with:
1. GraphQL API management: Create/manage GraphQL APIs
2. Schema management: Manage GraphQL schemas
3. Resolvers: Create/manage resolvers
4. Data sources: Create/manage data sources
5. Functions: AppSync functions
6. API keys: Manage API keys
7. Cognito auth: Cognito user pool integration
8. Lambda authorizers: Lambda authorization
9. WebSocket subscriptions: Real-time subscriptions
10. CloudWatch integration: API metrics and logging

Commit: 'feat(aws-appsync): add AWS AppSync with GraphQL API management, schema, resolvers, data sources, functions, API keys, Cognito auth, Lambda authorizers, subscriptions, CloudWatch'
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


class APIAuthType(Enum):
    """AppSync API authentication types."""
    API_KEY = "API_KEY"
    IAM = "IAM"
    Cognito_USER_POOL = "AMAZON_COGNITO_USER_POOLS"
    OPENID_CONNECT = "OPENID_CONNECT"
    Lambda_AUTH = "AWS_LAMBDA"


class AuthorizationMode(Enum):
    """Authorization modes for AppSync."""
    API_KEY = "API_KEY"
    IAM = "IAM"
    AMAZON_COGNITO_USER_POOLS = "AMAZON_COGNITO_USER_POOLS"
    OPENID_CONNECT = "OPENID_CONNECT"
    AWS_LAMBDA = "AWS_LAMBDA"


class DataSourceType(Enum):
    """AppSync data source types."""
    NONE = "NONE"
    AWS_LAMBDA = "AWS_LAMBDA"
    AMAZON_DYNAMODB = "AMAZON_DYNAMODB"
    AMAZON_ELASTICSEARCH = "AMAZON_ELASTICSEARCH"
    AMAZON_OPENSEARCH = "AMAZON_OPENSEARCH"
    AMAZON_COGNITO_USER_POOLS = "AMAZON_COGNITO_USER_POOLS"
    HTTP = "HTTP"
    RELATIONAL_DATABASE = "RELATIONAL_DATABASE"
    AMAZON_EVENTBRIDGE = "AMAZON_EVENTBRIDGE"


class RuntimeType(Enum):
    """AppSync resolver runtime types."""
    APPSYNC_JS = "APPSYNC_JS"


@dataclass
class GraphQLAPIConfig:
    """Configuration for a GraphQL API."""
    name: str
    auth_type: APIAuthType = APIAuthType.API_KEY
    description: Optional[str] = None
    log_level: str = "ALL"
    additional_auth_types: List[AuthorizationMode] = field(default_factory=list)
    user_pool_config: Optional[Dict[str, Any]] = None
    open_id_connect_config: Optional[Dict[str, Any]] = None
    lambda_authorizer_config: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    enhancedMonitoringConfig: Optional[Dict[str, Any]] = None
    metrics_enabled: bool = True


@dataclass
class SchemaConfig:
    """Configuration for a GraphQL schema."""
    definition: str
    description: Optional[str] = None


@dataclass
class ResolverConfig:
    """Configuration for a resolver."""
    type_name: str
    field_name: str
    data_source_name: str
    request_template: str
    response_template: str
    runtime: Optional[Dict[str, Any]] = None
    sync_config: Optional[Dict[str, Any]] = None
    caching_config: Optional[Dict[str, Any]] = None


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""
    name: str
    data_source_type: DataSourceType
    description: Optional[str] = None
    dynamodb_config: Optional[Dict[str, Any]] = None
    lambda_config: Optional[Dict[str, Any]] = None
    elasticsearch_config: Optional[Dict[str, Any]] = None
    opensearch_config: Optional[Dict[str, Any]] = None
    http_config: Optional[Dict[str, Any]] = None
    relational_database_config: Optional[Dict[str, Any]] = None
    eventbridge_config: Optional[Dict[str, Any]] = None
    service_role_arn: Optional[str] = None


@dataclass
class FunctionConfig:
    """Configuration for an AppSync function."""
    name: str
    data_source_name: str
    request_mapping_template: str
    response_mapping_template: str
    runtime: Optional[Dict[str, Any]] = None
    sync_config: Optional[Dict[str, Any]] = None
    max_batch_size: Optional[int] = None
    description: Optional[str] = None


@dataclass
class APIKeyConfig:
    """Configuration for an API key."""
    description: Optional[str] = None
    expires: Optional[int] = None


@dataclass
class CognitoAuthConfig:
    """Configuration for Cognito user pool authentication."""
    user_pool_id: str
    aws_region: str
    default_action: str = "ALLOW"
    cognito_groups_config: Optional[Dict[str, Any]] = None


@dataclass
class LambdaAuthorizerConfig:
    """Configuration for Lambda authorization."""
    authorizer_uri: str
    lambda_arity: int = 1
    lambda_version: str = "VARIABLE"
    refresh_cluster_auth_config: Optional[Dict[str, Any]] = None


@dataclass
class SubscriptionConfig:
    """Configuration for WebSocket subscriptions."""
    mutation_field: str
    topic: Optional[str] = None
    filter_scope: Optional[str] = None


@dataclass
class CloudWatchConfig:
    """Configuration for CloudWatch logging and metrics."""
    log_level: str = "ALL"
    field_log_level: str = "ALL"
    metrics_enabled: bool = True
    detailed_metrics: bool = True


class AppSyncIntegration:
    """
    AWS AppSync Integration for workflow automation.
    
    Supports GraphQL API management and provides:
    - GraphQL API creation and management
    - Schema management
    - Resolver creation and management
    - Data source configuration
    - AppSync functions
    - API key management
    - Cognito user pool integration
    - Lambda authorizers
    - WebSocket subscriptions
    - CloudWatch integration
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
        Initialize the AppSync integration.
        
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
        self._graphql_apis = {}
        self._schemas = {}
        self._resolvers = defaultdict(dict)
        self._data_sources = defaultdict(dict)
        self._functions = defaultdict(dict)
        self._api_keys = defaultdict(dict)
        self._cognito_configs = {}
        self._lambda_authorizers = {}
        self._subscriptions = defaultdict(list)
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
                "RoleSessionName": f"appsync_session_{uuid.uuid4().hex[:8]}"
            }
            if self.external_id:
                assume_role_kwargs["ExternalId"] = self.external_id
            
            credentials = sts_client.assume_role(**assume_role_kwargs)["Credentials"]
            session_kwargs["aws_access_key_id"] = credentials["AccessKeyId"]
            session_kwargs["aws_secret_access_key"] = credentials["SecretAccessKey"]
            session_kwargs["aws_session_token"] = credentials["SessionToken"]
        
        self._session = boto3.Session(**session_kwargs)
        
        self._clients["appsync"] = self._session.client("appsync")
        self._clients["logs"] = self._session.client("logs")
        self._clients["cloudwatch"] = self._session.client("cloudwatch")
        self._clients["lambda"] = self._session.client("lambda")
        self._clients["cognito-idp"] = self._session.client("cognito-idp")
        self._clients["dynamodb"] = self._session.client("dynamodb")
        self._clients["events"] = self._session.client("events")
    
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
    
    # ==================== GraphQL API Management ====================
    
    def create_graphql_api(
        self,
        config: GraphQLAPIConfig
    ) -> Dict[str, Any]:
        """
        Create a GraphQL API.
        
        Args:
            config: GraphQL API configuration
            
        Returns:
            Created API details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                api_id = self._generate_id("graphql-")
                api = {
                    "id": api_id,
                    "name": config.name,
                    "authenticationType": config.auth_type.value,
                    "description": config.description,
                    "logLevel": config.log_level,
                    "additionalAuthenticationProviders": [
                        {"authenticationType": auth.value}
                        for auth in config.additional_auth_types
                    ] if config.additional_auth_types else None,
                    "userPoolConfig": config.user_pool_config,
                    "openIdConnectConfig": config.open_id_connect_config,
                    "lambdaAuthorizerConfig": config.lambda_authorizer_config,
                    "tags": config.tags,
                    "xrayEnabled": False,
                    "createdAt": datetime.utcnow().isoformat(),
                    "updatedAt": datetime.utcnow().isoformat()
                }
                if config.enhancedMonitoring_config:
                    api["enhancedMonitoringConfig"] = config.enhancedMonitoring_config
                self._graphql_apis[api_id] = api
                logger.info(f"Created GraphQL API: {config.name} ({api_id})")
                return api
            
            client = self._get_client("appsync")
            
            create_kwargs = {
                "name": config.name,
                "authenticationType": config.auth_type.value,
            }
            
            if config.description:
                create_kwargs["description"] = config.description
            
            if config.additional_auth_types:
                providers = []
                for auth in config.additional_auth_types:
                    provider = {"authenticationType": auth.value}
                    if auth == AuthorizationMode.AMAZON_COGNITO_USER_POOLS and config.user_pool_config:
                        provider["cognitoConfig"] = config.user_pool_config
                    elif auth == AuthorizationMode.OPENID_CONNECT and config.open_id_connect_config:
                        provider["openIdConnectConfig"] = config.open_id_connect_config
                    elif auth == AuthorizationMode.AWS_LAMBDA and config.lambda_authorizer_config:
                        provider["lambdaConfig"] = config.lambda_authorizer_config
                    providers.append(provider)
                create_kwargs["additionalAuthenticationProviders"] = providers
            
            if config.user_pool_config and config.auth_type == APIAuthType.Cognito_USER_POOL:
                create_kwargs["userPoolConfig"] = config.user_pool_config
            
            if config.open_id_connect_config and config.auth_type == APIAuthType.OPENID_CONNECT:
                create_kwargs["openIdConnectConfig"] = config.open_id_connect_config
            
            if config.lambda_authorizer_config and config.auth_type == APIAuthType.Lambda_AUTH:
                create_kwargs["lambdaAuthorizerConfig"] = config.lambda_authorizer_config
            
            if config.tags:
                create_kwargs["tags"] = config.tags
            
            try:
                response = client.create_graphql_api(**create_kwargs)
                api = response["graphqlApi"]
                api_id = api["apiId"]
                self._graphql_apis[api_id] = api
                logger.info(f"Created GraphQL API: {config.name} ({api_id})")
                return api
            except ClientError as e:
                logger.error(f"Failed to create GraphQL API: {e}")
                raise
    
    def get_graphql_api(self, api_id: str) -> Dict[str, Any]:
        """
        Get GraphQL API details.
        
        Args:
            api_id: API ID
            
        Returns:
            API details
        """
        with self._lock:
            if api_id in self._graphql_apis:
                return self._graphql_apis[api_id]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"GraphQL API not found: {api_id}")
            
            client = self._get_client("appsync")
            try:
                response = client.get_graphql_api(apiId=api_id)
                api = response["graphqlApi"]
                self._graphql_apis[api_id] = api
                return api
            except ClientError as e:
                logger.error(f"Failed to get GraphQL API: {e}")
                raise
    
    def list_graphql_apis(self) -> List[Dict[str, Any]]:
        """
        List all GraphQL APIs.
        
        Returns:
            List of GraphQL APIs
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return list(self._graphql_apis.values())
            
            client = self._get_client("appsync")
            apis = []
            paginator = client.get_paginator("list_graphql_apis")
            
            for page in paginator.paginate():
                apis.extend(page.get("graphqlApis", []))
            
            return apis
    
    def update_graphql_api(
        self,
        api_id: str,
        config: GraphQLAPIConfig
    ) -> Dict[str, Any]:
        """
        Update a GraphQL API.
        
        Args:
            api_id: API ID
            config: Updated configuration
            
        Returns:
            Updated API details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_id not in self._graphql_apis:
                    raise ValueError(f"GraphQL API not found: {api_id}")
                api = self._graphql_apis[api_id]
                api.update({
                    "name": config.name,
                    "authenticationType": config.auth_type.value,
                    "description": config.description,
                    "logLevel": config.log_level,
                    "updatedAt": datetime.utcnow().isoformat()
                })
                logger.info(f"Updated GraphQL API: {api_id}")
                return api
            
            client = self._get_client("appsync")
            
            update_kwargs = {"apiId": api_id}
            
            if config.description is not None:
                update_kwargs["description"] = config.description
            
            if config.log_level:
                update_kwargs["logConfig"] = {"fieldLogLevel": config.log_level, "logsRegion": self.region}
            
            if config.user_pool_config:
                update_kwargs["userPoolConfig"] = config.user_pool_config
            
            if config.open_id_connect_config:
                update_kwargs["openIdConnectConfig"] = config.open_id_connect_config
            
            if config.lambda_authorizer_config:
                update_kwargs["lambdaAuthorizerConfig"] = config.lambda_authorizer_config
            
            try:
                response = client.update_graphql_api(**update_kwargs)
                api = response["graphqlApi"]
                self._graphql_apis[api_id] = api
                logger.info(f"Updated GraphQL API: {api_id}")
                return api
            except ClientError as e:
                logger.error(f"Failed to update GraphQL API: {e}")
                raise
    
    def delete_graphql_api(self, api_id: str) -> bool:
        """
        Delete a GraphQL API.
        
        Args:
            api_id: API ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_id in self._graphql_apis:
                    del self._graphql_apis[api_id]
                    logger.info(f"Deleted GraphQL API: {api_id}")
                    return True
                return False
            
            client = self._get_client("appsync")
            try:
                client.delete_graphql_api(apiId=api_id)
                if api_id in self._graphql_apis:
                    del self._graphql_apis[api_id]
                logger.info(f"Deleted GraphQL API: {api_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to delete GraphQL API: {e}")
                raise
    
    # ==================== Schema Management ====================
    
    def create_schema(
        self,
        api_id: str,
        schema_definition: str
    ) -> Dict[str, Any]:
        """
        Create a GraphQL schema.
        
        Args:
            api_id: API ID
            schema_definition: GraphQL schema SDL definition
            
        Returns:
            Schema details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                schema = {
                    "apiId": api_id,
                    "definition": schema_definition,
                    "status": "ACTIVE",
                    "createdAt": datetime.utcnow().isoformat()
                }
                self._schemas[api_id] = schema
                logger.info(f"Created schema for API: {api_id}")
                return schema
            
            client = self._get_client("appsync")
            
            try:
                response = client.start_schema_creation(
                    apiId=api_id,
                    definition=schema_definition.encode("utf-8")
                )
                
                status = response.get("status")
                while status == "PROCESSING":
                    time.sleep(1)
                    try:
                        response = client.get_schema_creation_status(apiId=api_id)
                        status = response.get("status")
                    except ClientError:
                        break
                
                if status == "SUCCESS":
                    schema = {
                        "apiId": api_id,
                        "definition": schema_definition,
                        "status": "ACTIVE"
                    }
                    self._schemas[api_id] = schema
                    logger.info(f"Created schema for API: {api_id}")
                    return schema
                else:
                    details = response.get("details", "Unknown error")
                    raise RuntimeError(f"Schema creation failed: {details}")
                    
            except ClientError as e:
                logger.error(f"Failed to create schema: {e}")
                raise
    
    def get_schema(self, api_id: str) -> Dict[str, Any]:
        """
        Get GraphQL schema.
        
        Args:
            api_id: API ID
            
        Returns:
            Schema details
        """
        with self._lock:
            if api_id in self._schemas:
                return self._schemas[api_id]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"Schema not found for API: {api_id}")
            
            client = self._get_client("appsync")
            try:
                response = client.get_schema(apiId=api_id)
                schema = {
                    "apiId": api_id,
                    "definition": response.get("schema", "").decode("utf-8") if isinstance(response.get("schema"), bytes) else response.get("schema", ""),
                    "status": "ACTIVE"
                }
                self._schemas[api_id] = schema
                return schema
            except ClientError as e:
                logger.error(f"Failed to get schema: {e}")
                raise
    
    def update_schema(
        self,
        api_id: str,
        schema_definition: str
    ) -> Dict[str, Any]:
        """
        Update GraphQL schema.
        
        Args:
            api_id: API ID
            schema_definition: New GraphQL schema SDL definition
            
        Returns:
            Updated schema details
        """
        return self.create_schema(api_id, schema_definition)
    
    def validate_schema(self, api_id: str) -> Dict[str, Any]:
        """
        Validate GraphQL schema.
        
        Args:
            api_id: API ID
            
        Returns:
            Validation result
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return {
                    "apiId": api_id,
                    "status": "VALID",
                    "messages": []
                }
            
            client = self._get_client("appsync")
            try:
                response = client.get_introspection_schema(apiId=api_id)
                schema_content = response.get("schema", "").decode("utf-8") if isinstance(response.get("schema"), bytes) else response.get("schema", "")
                
                return {
                    "apiId": api_id,
                    "status": "VALID",
                    "messages": [],
                    "typesCount": schema_content.count("type")
                }
            except ClientError as e:
                logger.error(f"Failed to validate schema: {e}")
                return {
                    "apiId": api_id,
                    "status": "INVALID",
                    "messages": [str(e)]
                }
    
    # ==================== Data Source Management ====================
    
    def create_data_source(
        self,
        api_id: str,
        config: DataSourceConfig
    ) -> Dict[str, Any]:
        """
        Create a data source.
        
        Args:
            api_id: API ID
            config: Data source configuration
            
        Returns:
            Created data source details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                ds_id = self._generate_id("ds-")
                data_source = {
                    "dataSourceId": ds_id,
                    "name": config.name,
                    "type": config.data_source_type.value,
                    "description": config.description,
                    "dynamodbConfig": config.dynamodb_config,
                    "lambdaConfig": config.lambda_config,
                    "elasticsearchConfig": config.elasticsearch_config,
                    "openSearchConfig": config.opensearch_config,
                    "httpConfig": config.http_config,
                    "relationalDatabaseConfig": config.relational_database_config,
                    "eventBridgeConfig": config.eventbridge_config,
                    "serviceRoleArn": config.service_role_arn,
                    "apiId": api_id
                }
                self._data_sources[api_id][ds_id] = data_source
                logger.info(f"Created data source: {config.name} ({ds_id}) for API: {api_id}")
                return data_source
            
            client = self._get_client("appsync")
            
            create_kwargs = {
                "apiId": api_id,
                "name": config.name,
                "type": config.data_source_type.value
            }
            
            if config.description:
                create_kwargs["description"] = config.description
            
            if config.dynamodb_config and config.data_source_type == DataSourceType.AMAZON_DYNAMODB:
                create_kwargs["dynamodbConfig"] = config.dynamodb_config
            
            if config.lambda_config and config.data_source_type == DataSourceType.AWS_LAMBDA:
                create_kwargs["lambdaConfig"] = config.lambda_config
            
            if config.elasticsearch_config and config.data_source_type == DataSourceType.AMAZON_ELASTICSEARCH:
                create_kwargs["elasticsearchConfig"] = config.elasticsearch_config
            
            if config.opensearch_config and config.data_source_type == DataSourceType.AMAZON_OPENSEARCH:
                create_kwargs["openSearchConfig"] = config.opensearch_config
            
            if config.http_config and config.data_source_type == DataSourceType.HTTP:
                create_kwargs["httpConfig"] = config.http_config
            
            if config.relational_database_config and config.data_source_type == DataSourceType.RELATIONAL_DATABASE:
                create_kwargs["relationalDatabaseConfig"] = config.relational_database_config
            
            if config.eventbridge_config and config.data_source_type == DataSourceType.AMAZON_EVENTBRIDGE:
                create_kwargs["eventBridgeConfig"] = config.eventbridge_config
            
            if config.service_role_arn:
                create_kwargs["serviceRoleArn"] = config.service_role_arn
            
            try:
                response = client.create_data_source(**create_kwargs)
                data_source = response["dataSource"]
                ds_id = data_source["dataSourceId"]
                self._data_sources[api_id][ds_id] = data_source
                logger.info(f"Created data source: {config.name} ({ds_id}) for API: {api_id}")
                return data_source
            except ClientError as e:
                logger.error(f"Failed to create data source: {e}")
                raise
    
    def get_data_source(self, api_id: str, data_source_id: str) -> Dict[str, Any]:
        """
        Get data source details.
        
        Args:
            api_id: API ID
            data_source_id: Data source ID
            
        Returns:
            Data source details
        """
        with self._lock:
            if data_source_id in self._data_sources.get(api_id, {}):
                return self._data_sources[api_id][data_source_id]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"Data source not found: {data_source_id}")
            
            client = self._get_client("appsync")
            try:
                response = client.get_data_source(apiId=api_id, dataSourceId=data_source_id)
                data_source = response["dataSource"]
                self._data_sources[api_id][data_source_id] = data_source
                return data_source
            except ClientError as e:
                logger.error(f"Failed to get data source: {e}")
                raise
    
    def list_data_sources(self, api_id: str) -> List[Dict[str, Any]]:
        """
        List all data sources for an API.
        
        Args:
            api_id: API ID
            
        Returns:
            List of data sources
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return list(self._data_sources.get(api_id, {}).values())
            
            client = self._get_client("appsync")
            data_sources = []
            paginator = client.get_paginator("list_data_sources")
            
            for page in paginator.paginate(apiId=api_id):
                data_sources.extend(page.get("dataSources", []))
            
            return data_sources
    
    def update_data_source(
        self,
        api_id: str,
        data_source_id: str,
        config: DataSourceConfig
    ) -> Dict[str, Any]:
        """
        Update a data source.
        
        Args:
            api_id: API ID
            data_source_id: Data source ID
            config: Updated configuration
            
        Returns:
            Updated data source details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if data_source_id not in self._data_sources.get(api_id, {}):
                    raise ValueError(f"Data source not found: {data_source_id}")
                data_source = self._data_sources[api_id][data_source_id]
                data_source.update({
                    "name": config.name,
                    "type": config.data_source_type.value,
                    "description": config.description
                })
                logger.info(f"Updated data source: {data_source_id}")
                return data_source
            
            client = self._get_client("appsync")
            
            update_kwargs = {
                "apiId": api_id,
                "dataSourceId": data_source_id,
                "name": config.name,
                "type": config.data_source_type.value
            }
            
            if config.description:
                update_kwargs["description"] = config.description
            
            if config.dynamodb_config:
                update_kwargs["dynamodbConfig"] = config.dynamodb_config
            
            if config.lambda_config:
                update_kwargs["lambdaConfig"] = config.lambda_config
            
            if config.elasticsearch_config:
                update_kwargs["elasticsearchConfig"] = config.elasticsearch_config
            
            if config.opensearch_config:
                update_kwargs["openSearchConfig"] = config.opensearch_config
            
            if config.http_config:
                update_kwargs["httpConfig"] = config.http_config
            
            if config.relational_database_config:
                update_kwargs["relationalDatabaseConfig"] = config.relational_database_config
            
            if config.eventbridge_config:
                update_kwargs["eventBridgeConfig"] = config.eventbridge_config
            
            if config.service_role_arn:
                update_kwargs["serviceRoleArn"] = config.service_role_arn
            
            try:
                response = client.update_data_source(**update_kwargs)
                data_source = response["dataSource"]
                self._data_sources[api_id][data_source_id] = data_source
                logger.info(f"Updated data source: {data_source_id}")
                return data_source
            except ClientError as e:
                logger.error(f"Failed to update data source: {e}")
                raise
    
    def delete_data_source(self, api_id: str, data_source_id: str) -> bool:
        """
        Delete a data source.
        
        Args:
            api_id: API ID
            data_source_id: Data source ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if data_source_id in self._data_sources.get(api_id, {}):
                    del self._data_sources[api_id][data_source_id]
                    logger.info(f"Deleted data source: {data_source_id}")
                    return True
                return False
            
            client = self._get_client("appsync")
            try:
                client.delete_data_source(apiId=api_id, dataSourceId=data_source_id)
                if data_source_id in self._data_sources.get(api_id, {}):
                    del self._data_sources[api_id][data_source_id]
                logger.info(f"Deleted data source: {data_source_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to delete data source: {e}")
                raise
    
    # ==================== Resolver Management ====================
    
    def create_resolver(
        self,
        api_id: str,
        config: ResolverConfig
    ) -> Dict[str, Any]:
        """
        Create a resolver.
        
        Args:
            api_id: API ID
            config: Resolver configuration
            
        Returns:
            Created resolver details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                resolver_id = self._generate_id("res-")
                resolver = {
                    "resolverId": resolver_id,
                    "typeName": config.type_name,
                    "fieldName": config.field_name,
                    "dataSourceName": config.data_source_name,
                    "requestMappingTemplate": config.request_template,
                    "responseMappingTemplate": config.response_template,
                    "runtime": config.runtime,
                    "syncConfig": config.sync_config,
                    "cachingConfig": config.caching_config,
                    "apiId": api_id
                }
                self._resolvers[api_id][f"{config.type_name}.{config.field_name}"] = resolver
                logger.info(f"Created resolver: {config.type_name}.{config.field_name} for API: {api_id}")
                return resolver
            
            client = self._get_client("appsync")
            
            create_kwargs = {
                "apiId": api_id,
                "typeName": config.type_name,
                "fieldName": config.field_name,
                "dataSourceName": config.data_source_name,
                "requestMappingTemplate": config.request_template,
                "responseMappingTemplate": config.response_template
            }
            
            if config.runtime:
                create_kwargs["runtime"] = config.runtime
            
            if config.sync_config:
                create_kwargs["syncConfig"] = config.sync_config
            
            if config.caching_config:
                create_kwargs["cachingConfig"] = config.caching_config
            
            try:
                response = client.create_resolver(**create_kwargs)
                resolver = response["resolver"]
                resolver_id = resolver["resolverId"]
                self._resolvers[api_id][f"{config.type_name}.{config.field_name}"] = resolver
                logger.info(f"Created resolver: {config.type_name}.{config.field_name} for API: {api_id}")
                return resolver
            except ClientError as e:
                logger.error(f"Failed to create resolver: {e}")
                raise
    
    def get_resolver(
        self,
        api_id: str,
        type_name: str,
        field_name: str
    ) -> Dict[str, Any]:
        """
        Get resolver details.
        
        Args:
            api_id: API ID
            type_name: GraphQL type name
            field_name: GraphQL field name
            
        Returns:
            Resolver details
        """
        with self._lock:
            key = f"{type_name}.{field_name}"
            if key in self._resolvers.get(api_id, {}):
                return self._resolvers[api_id][key]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"Resolver not found: {key}")
            
            client = self._get_client("appsync")
            try:
                response = client.get_resolver(
                    apiId=api_id,
                    typeName=type_name,
                    fieldName=field_name
                )
                resolver = response["resolver"]
                self._resolvers[api_id][key] = resolver
                return resolver
            except ClientError as e:
                logger.error(f"Failed to get resolver: {e}")
                raise
    
    def list_resolvers(self, api_id: str) -> List[Dict[str, Any]]:
        """
        List all resolvers for an API.
        
        Args:
            api_id: API ID
            
        Returns:
            List of resolvers
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return list(self._resolvers.get(api_id, {}).values())
            
            client = self._get_client("appsync")
            resolvers = []
            paginator = client.get_paginator("list_resolvers")
            
            for page in paginator.paginate(apiId=api_id):
                resolvers.extend(page.get("resolvers", []))
            
            return resolvers
    
    def list_resolvers_by_type(
        self,
        api_id: str,
        type_name: str
    ) -> List[Dict[str, Any]]:
        """
        List resolvers for a specific type.
        
        Args:
            api_id: API ID
            type_name: GraphQL type name
            
        Returns:
            List of resolvers for the type
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return [
                    r for k, r in self._resolvers.get(api_id, {}).items()
                    if k.startswith(f"{type_name}.")
                ]
            
            client = self._get_client("appsync")
            resolvers = []
            paginator = client.get_paginator("list_resolvers_by_type")
            
            for page in paginator.paginate(apiId=api_id, typeName=type_name):
                resolvers.extend(page.get("resolvers", []))
            
            return resolvers
    
    def update_resolver(
        self,
        api_id: str,
        type_name: str,
        field_name: str,
        config: ResolverConfig
    ) -> Dict[str, Any]:
        """
        Update a resolver.
        
        Args:
            api_id: API ID
            type_name: GraphQL type name
            field_name: GraphQL field name
            config: Updated configuration
            
        Returns:
            Updated resolver details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                key = f"{type_name}.{field_name}"
                if key not in self._resolvers.get(api_id, {}):
                    raise ValueError(f"Resolver not found: {key}")
                resolver = self._resolvers[api_id][key]
                resolver.update({
                    "dataSourceName": config.data_source_name,
                    "requestMappingTemplate": config.request_template,
                    "responseMappingTemplate": config.response_template
                })
                logger.info(f"Updated resolver: {key}")
                return resolver
            
            client = self._get_client("appsync")
            
            update_kwargs = {
                "apiId": api_id,
                "typeName": type_name,
                "fieldName": field_name,
                "dataSourceName": config.data_source_name,
                "requestMappingTemplate": config.request_template,
                "responseMappingTemplate": config.response_template
            }
            
            if config.runtime:
                update_kwargs["runtime"] = config.runtime
            
            if config.sync_config:
                update_kwargs["syncConfig"] = config.sync_config
            
            if config.caching_config:
                update_kwargs["cachingConfig"] = config.caching_config
            
            try:
                response = client.update_resolver(**update_kwargs)
                resolver = response["resolver"]
                self._resolvers[api_id][f"{type_name}.{field_name}"] = resolver
                logger.info(f"Updated resolver: {type_name}.{field_name}")
                return resolver
            except ClientError as e:
                logger.error(f"Failed to update resolver: {e}")
                raise
    
    def delete_resolver(
        self,
        api_id: str,
        type_name: str,
        field_name: str
    ) -> bool:
        """
        Delete a resolver.
        
        Args:
            api_id: API ID
            type_name: GraphQL type name
            field_name: GraphQL field name
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            key = f"{type_name}.{field_name}"
            if not BOTO3_AVAILABLE:
                if key in self._resolvers.get(api_id, {}):
                    del self._resolvers[api_id][key]
                    logger.info(f"Deleted resolver: {key}")
                    return True
                return False
            
            client = self._get_client("appsync")
            try:
                client.delete_resolver(
                    apiId=api_id,
                    typeName=type_name,
                    fieldName=field_name
                )
                if key in self._resolvers.get(api_id, {}):
                    del self._resolvers[api_id][key]
                logger.info(f"Deleted resolver: {key}")
                return True
            except ClientError as e:
                logger.error(f"Failed to delete resolver: {e}")
                raise
    
    # ==================== AppSync Functions ====================
    
    def create_function(
        self,
        api_id: str,
        config: FunctionConfig
    ) -> Dict[str, Any]:
        """
        Create an AppSync function.
        
        Args:
            api_id: API ID
            config: Function configuration
            
        Returns:
            Created function details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                func_id = self._generate_id("func-")
                function = {
                    "functionId": func_id,
                    "name": config.name,
                    "dataSourceName": config.data_source_name,
                    "requestMappingTemplate": config.request_mapping_template,
                    "responseMappingTemplate": config.response_mapping_template,
                    "runtime": config.runtime,
                    "syncConfig": config.sync_config,
                    "maxBatchSize": config.max_batch_size,
                    "description": config.description,
                    "apiId": api_id
                }
                self._functions[api_id][func_id] = function
                logger.info(f"Created function: {config.name} ({func_id}) for API: {api_id}")
                return function
            
            client = self._get_client("appsync")
            
            create_kwargs = {
                "apiId": api_id,
                "name": config.name,
                "dataSourceName": config.data_source_name,
                "requestMappingTemplate": config.request_mapping_template,
                "responseMappingTemplate": config.response_mapping_template
            }
            
            if config.runtime:
                create_kwargs["runtime"] = config.runtime
            
            if config.sync_config:
                create_kwargs["syncConfig"] = config.sync_config
            
            if config.max_batch_size:
                create_kwargs["maxBatchSize"] = config.max_batch_size
            
            if config.description:
                create_kwargs["description"] = config.description
            
            try:
                response = client.create_function(**create_kwargs)
                function = response["functionConfiguration"]
                func_id = function["functionId"]
                self._functions[api_id][func_id] = function
                logger.info(f"Created function: {config.name} ({func_id}) for API: {api_id}")
                return function
            except ClientError as e:
                logger.error(f"Failed to create function: {e}")
                raise
    
    def get_function(self, api_id: str, function_id: str) -> Dict[str, Any]:
        """
        Get function details.
        
        Args:
            api_id: API ID
            function_id: Function ID
            
        Returns:
            Function details
        """
        with self._lock:
            if function_id in self._functions.get(api_id, {}):
                return self._functions[api_id][function_id]
            
            if not BOTO3_AVAILABLE:
                raise ValueError(f"Function not found: {function_id}")
            
            client = self._get_client("appsync")
            try:
                response = client.get_function(apiId=api_id, functionId=function_id)
                function = response["functionConfiguration"]
                self._functions[api_id][function_id] = function
                return function
            except ClientError as e:
                logger.error(f"Failed to get function: {e}")
                raise
    
    def list_functions(self, api_id: str) -> List[Dict[str, Any]]:
        """
        List all functions for an API.
        
        Args:
            api_id: API ID
            
        Returns:
            List of functions
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return list(self._functions.get(api_id, {}).values())
            
            client = self._get_client("appsync")
            functions = []
            paginator = client.get_paginator("list_functions")
            
            for page in paginator.paginate(apiId=api_id):
                functions.extend(page.get("functions", []))
            
            return functions
    
    def update_function(
        self,
        api_id: str,
        function_id: str,
        config: FunctionConfig
    ) -> Dict[str, Any]:
        """
        Update an AppSync function.
        
        Args:
            api_id: API ID
            function_id: Function ID
            config: Updated configuration
            
        Returns:
            Updated function details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if function_id not in self._functions.get(api_id, {}):
                    raise ValueError(f"Function not found: {function_id}")
                function = self._functions[api_id][function_id]
                function.update({
                    "name": config.name,
                    "dataSourceName": config.data_source_name,
                    "requestMappingTemplate": config.request_mapping_template,
                    "responseMappingTemplate": config.response_mapping_template
                })
                logger.info(f"Updated function: {function_id}")
                return function
            
            client = self._get_client("appsync")
            
            update_kwargs = {
                "apiId": api_id,
                "functionId": function_id,
                "name": config.name,
                "dataSourceName": config.data_source_name,
                "requestMappingTemplate": config.request_mapping_template,
                "responseMappingTemplate": config.response_mapping_template
            }
            
            if config.runtime:
                update_kwargs["runtime"] = config.runtime
            
            if config.sync_config:
                update_kwargs["syncConfig"] = config.sync_config
            
            if config.max_batch_size:
                update_kwargs["maxBatchSize"] = config.max_batch_size
            
            if config.description:
                update_kwargs["description"] = config.description
            
            try:
                response = client.update_function(**update_kwargs)
                function = response["functionConfiguration"]
                self._functions[api_id][function_id] = function
                logger.info(f"Updated function: {function_id}")
                return function
            except ClientError as e:
                logger.error(f"Failed to update function: {e}")
                raise
    
    def delete_function(self, api_id: str, function_id: str) -> bool:
        """
        Delete an AppSync function.
        
        Args:
            api_id: API ID
            function_id: Function ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if function_id in self._functions.get(api_id, {}):
                    del self._functions[api_id][function_id]
                    logger.info(f"Deleted function: {function_id}")
                    return True
                return False
            
            client = self._get_client("appsync")
            try:
                client.delete_function(apiId=api_id, functionId=function_id)
                if function_id in self._functions.get(api_id, {}):
                    del self._functions[api_id][function_id]
                logger.info(f"Deleted function: {function_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to delete function: {e}")
                raise
    
    # ==================== API Key Management ====================
    
    def create_api_key(
        self,
        api_id: str,
        config: Optional[APIKeyConfig] = None
    ) -> Dict[str, Any]:
        """
        Create an API key.
        
        Args:
            api_id: API ID
            config: API key configuration
            
        Returns:
            Created API key details
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                key_id = self._generate_id("key-")
                expires = None
                if config and config.expires:
                    expires = int(time.time()) + config.expires
                api_key = {
                    "id": key_id,
                    "apiKey": f"daa-{uuid.uuid4().hex}",
                    "description": config.description if config else None,
                    "expires": expires,
                    "apiId": api_id,
                    "createdAt": datetime.utcnow().isoformat()
                }
                self._api_keys[api_id][key_id] = api_key
                logger.info(f"Created API key: {key_id} for API: {api_id}")
                return api_key
            
            client = self._get_client("appsync")
            
            create_kwargs = {"apiId": api_id}
            
            if config:
                if config.description:
                    create_kwargs["description"] = config.description
                if config.expires:
                    expires_time = datetime.utcnow() + timedelta(days=config.expires)
                    create_kwargs["expires"] = int(expires_time.timestamp())
            
            try:
                response = client.create_api_key(**create_kwargs)
                api_key = response["apiKey"]
                key_id = api_key["id"]
                self._api_keys[api_id][key_id] = api_key
                logger.info(f"Created API key: {key_id} for API: {api_id}")
                return api_key
            except ClientError as e:
                logger.error(f"Failed to create API key: {e}")
                raise
    
    def list_api_keys(self, api_id: str) -> List[Dict[str, Any]]:
        """
        List API keys for an API.
        
        Args:
            api_id: API ID
            
        Returns:
            List of API keys
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return list(self._api_keys.get(api_id, {}).values())
            
            client = self._get_client("appsync")
            api_keys = []
            paginator = client.get_paginator("list_api_keys")
            
            for page in paginator.paginate(apiId=api_id):
                api_keys.extend(page.get("apiKeys", []))
            
            return api_keys
    
    def delete_api_key(self, api_id: str, api_key_id: str) -> bool:
        """
        Delete an API key.
        
        Args:
            api_id: API ID
            api_key_id: API key ID
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_key_id in self._api_keys.get(api_id, {}):
                    del self._api_keys[api_id][api_key_id]
                    logger.info(f"Deleted API key: {api_key_id}")
                    return True
                return False
            
            client = self._get_client("appsync")
            try:
                client.delete_api_key(apiId=api_id, id=api_key_id)
                if api_key_id in self._api_keys.get(api_id, {}):
                    del self._api_keys[api_id][api_key_id]
                logger.info(f"Deleted API key: {api_key_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to delete API key: {e}")
                raise
    
    # ==================== Cognito Authentication ====================
    
    def configure_cognito_auth(
        self,
        api_id: str,
        config: CognitoAuthConfig
    ) -> Dict[str, Any]:
        """
        Configure Cognito user pool authentication.
        
        Args:
            api_id: API ID
            config: Cognito authentication configuration
            
        Returns:
            Updated API details
        """
        with self._lock:
            cognito_config = {
                "userPoolId": config.user_pool_id,
                "awsRegion": config.aws_region,
                "defaultAction": config.default_action
            }
            
            if config.cognito_groups_config:
                cognito_config["cognitoGroupsConfig"] = config.cognito_groups_config
            
            self._cognito_configs[api_id] = cognito_config
            
            if not BOTO3_AVAILABLE:
                logger.info(f"Configured Cognito auth for API: {api_id}")
                return cognito_config
            
            client = self._get_client("appsync")
            
            try:
                response = client.update_graphql_api(
                    apiId=api_id,
                    authenticationType=APIAuthType.Cognito_USER_POOL.value,
                    userPoolConfig=cognito_config
                )
                api = response["graphqlApi"]
                self._graphql_apis[api_id] = api
                logger.info(f"Configured Cognito auth for API: {api_id}")
                return api
            except ClientError as e:
                logger.error(f"Failed to configure Cognito auth: {e}")
                raise
    
    def get_cognito_auth_config(self, api_id: str) -> Optional[Dict[str, Any]]:
        """
        Get Cognito authentication configuration.
        
        Args:
            api_id: API ID
            
        Returns:
            Cognito configuration or None
        """
        with self._lock:
            if api_id in self._cognito_configs:
                return self._cognito_configs[api_id]
            
            if not BOTO3_AVAILABLE:
                return None
            
            try:
                api = self.get_graphql_api(api_id)
                return api.get("userPoolConfig")
            except ClientError:
                return None
    
    # ==================== Lambda Authorizers ====================
    
    def configure_lambda_authorizer(
        self,
        api_id: str,
        config: LambdaAuthorizerConfig
    ) -> Dict[str, Any]:
        """
        Configure Lambda authorization.
        
        Args:
            api_id: API ID
            config: Lambda authorizer configuration
            
        Returns:
            Updated API details
        """
        with self._lock:
            lambda_config = {
                "authorizerUri": config.authorizer_uri,
                "authorizerCredentials": f"arn:aws:lambda:{self.region}:*:function:appsync-authorizer",
                "lambdaArity": config.lambda_arity,
                "lambdaVersion": config.lambda_version
            }
            
            if config.refresh_cluster_auth_config:
                lambda_config["refreshClusterAuthConfig"] = config.refresh_cluster_auth_config
            
            self._lambda_authorizers[api_id] = lambda_config
            
            if not BOTO3_AVAILABLE:
                logger.info(f"Configured Lambda authorizer for API: {api_id}")
                return lambda_config
            
            client = self._get_client("appsync")
            
            try:
                response = client.update_graphql_api(
                    apiId=api_id,
                    authenticationType=APIAuthType.Lambda_AUTH.value,
                    lambdaAuthorizerConfig=lambda_config
                )
                api = response["graphqlApi"]
                self._graphql_apis[api_id] = api
                logger.info(f"Configured Lambda authorizer for API: {api_id}")
                return api
            except ClientError as e:
                logger.error(f"Failed to configure Lambda authorizer: {e}")
                raise
    
    def get_lambda_authorizer_config(self, api_id: str) -> Optional[Dict[str, Any]]:
        """
        Get Lambda authorizer configuration.
        
        Args:
            api_id: API ID
            
        Returns:
            Lambda authorizer configuration or None
        """
        with self._lock:
            if api_id in self._lambda_authorizers:
                return self._lambda_authorizers[api_id]
            
            if not BOTO3_AVAILABLE:
                return None
            
            try:
                api = self.get_graphql_api(api_id)
                return api.get("lambdaAuthorizerConfig")
            except ClientError:
                return None
    
    # ==================== WebSocket Subscriptions ====================
    
    def configure_subscription(
        self,
        api_id: str,
        config: SubscriptionConfig
    ) -> Dict[str, Any]:
        """
        Configure a subscription for real-time updates.
        
        Args:
            api_id: API ID
            config: Subscription configuration
            
        Returns:
            Subscription configuration
        """
        with self._lock:
            subscription = {
                "mutationField": config.mutation_field,
                "topic": config.topic or f"topic/{config.mutation_field}",
                "filterScope": config.filter_scope
            }
            self._subscriptions[api_id].append(subscription)
            logger.info(f"Configured subscription for field: {config.mutation_field} on API: {api_id}")
            return subscription
    
    def list_subscriptions(self, api_id: str) -> List[Dict[str, Any]]:
        """
        List all subscriptions for an API.
        
        Args:
            api_id: API ID
            
        Returns:
            List of subscription configurations
        """
        with self._lock:
            return list(self._subscriptions.get(api_id, []))
    
    def delete_subscription(
        self,
        api_id: str,
        mutation_field: str
    ) -> bool:
        """
        Delete a subscription.
        
        Args:
            api_id: API ID
            mutation_field: Mutation field name
            
        Returns:
            True if deleted successfully
        """
        with self._lock:
            subscriptions = self._subscriptions.get(api_id, [])
            for i, sub in enumerate(subscriptions):
                if sub["mutationField"] == mutation_field:
                    subscriptions.pop(i)
                    logger.info(f"Deleted subscription for field: {mutation_field} on API: {api_id}")
                    return True
            return False
    
    # ==================== CloudWatch Integration ====================
    
    def configure_cloudwatch_logging(
        self,
        api_id: str,
        config: CloudWatchConfig
    ) -> Dict[str, Any]:
        """
        Configure CloudWatch logging and metrics.
        
        Args:
            api_id: API ID
            config: CloudWatch configuration
            
        Returns:
            Updated configuration
        """
        with self._lock:
            cw_config = {
                "logLevel": config.log_level,
                "fieldLogLevel": config.field_log_level,
                "cloudWatchLogsRoleArn": f"arn:aws:iam::*:role/service-role/appsync-cloudwatch-logs-role",
                "metricsEnabled": config.metrics_enabled
            }
            self._cloudwatch_configs[api_id] = cw_config
            
            if not BOTO3_AVAILABLE:
                logger.info(f"Configured CloudWatch logging for API: {api_id}")
                return cw_config
            
            client = self._get_client("appsync")
            
            try:
                response = client.update_graphql_api(
                    apiId=api_id,
                    logConfig={
                        "fieldLogLevel": config.field_log_level,
                        "logsRegion": self.region
                    }
                )
                api = response["graphqlApi"]
                self._graphql_apis[api_id] = api
                logger.info(f"Configured CloudWatch logging for API: {api_id}")
                return api
            except ClientError as e:
                logger.error(f"Failed to configure CloudWatch logging: {e}")
                raise
    
    def get_cloudwatch_config(self, api_id: str) -> Optional[Dict[str, Any]]:
        """
        Get CloudWatch configuration.
        
        Args:
            api_id: API ID
            
        Returns:
            CloudWatch configuration or None
        """
        with self._lock:
            if api_id in self._cloudwatch_configs:
                return self._cloudwatch_configs[api_id]
            
            if not BOTO3_AVAILABLE:
                return None
            
            try:
                api = self.get_graphql_api(api_id)
                return api.get("logConfig")
            except ClientError:
                return None
    
    def get_api_metrics(
        self,
        api_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 3600
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for an API.
        
        Args:
            api_id: API ID
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            
        Returns:
            CloudWatch metrics data
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return {
                    "apiId": api_id,
                    "metrics": [],
                    "message": "CloudWatch metrics not available without boto3"
                }
            
            if not start_time:
                start_time = datetime.utcnow() - timedelta(hours=1)
            if not end_time:
                end_time = datetime.utcnow()
            
            cloudwatch = self._get_client("cloudwatch")
            
            try:
                api = self.get_graphql_api(api_id)
                api_name = api.get("name", api_id)
                
                metric_data = cloudwatch.get_metric_data(
                    MetricDataQueries=[
                        {
                            "Id": "req_count",
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": "AWS/AppSync",
                                    "MetricName": "Requests",
                                    "Dimensions": [
                                        {"Name": "GraphQLAPIId", "Value": api_id}
                                    ]
                                },
                                "Period": period,
                                "Stat": "Sum"
                            }
                        },
                        {
                            "Id": "latency",
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": "AWS/AppSync",
                                    "MetricName": "Latency",
                                    "Dimensions": [
                                        {"Name": "GraphQLAPIId", "Value": api_id}
                                    ]
                                },
                                "Period": period,
                                "Stat": "Average"
                            }
                        },
                        {
                            "Id": "err_count",
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": "AWS/AppSync",
                                    "MetricName": "Errors",
                                    "Dimensions": [
                                        {"Name": "GraphQLAPIId", "Value": api_id}
                                    ]
                                },
                                "Period": period,
                                "Stat": "Sum"
                            }
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time
                )
                
                return {
                    "apiId": api_id,
                    "apiName": api_name,
                    "metricData": metric_data.get("MetricDataResults", []),
                    "startTime": start_time.isoformat(),
                    "endTime": end_time.isoformat()
                }
            except ClientError as e:
                logger.error(f"Failed to get API metrics: {e}")
                return {
                    "apiId": api_id,
                    "error": str(e)
                }
    
    def create_log_subscription(
        self,
        api_id: str,
        log_group_name: str,
        filter_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch log subscription.
        
        Args:
            api_id: API ID
            log_group_name: CloudWatch log group name
            filter_pattern: Optional filter pattern
            
        Returns:
            Subscription configuration
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                subscription = {
                    "apiId": api_id,
                    "logGroupName": log_group_name,
                    "filterPattern": filter_pattern,
                    "subscriptionId": self._generate_id("sub-")
                }
                logger.info(f"Created log subscription for API: {api_id}")
                return subscription
            
            logs = self._get_client("logs")
            
            try:
                subscription = logs.subscribe(
                    logGroupIdentifier=log_group_name,
                    filterPattern=filter_pattern
                )
                
                return {
                    "apiId": api_id,
                    "logGroupName": log_group_name,
                    "filterPattern": filter_pattern,
                    "subscriptionId": subscription.get("subscriptionId")
                }
            except ClientError as e:
                logger.error(f"Failed to create log subscription: {e}")
                raise
    
    # ==================== Tags Management ====================
    
    def tag_resource(
        self,
        api_id: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Tag an AppSync resource.
        
        Args:
            api_id: API ID
            tags: Tags to apply
            
        Returns:
            True if successful
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                if api_id in self._graphql_apis:
                    self._graphql_apis[api_id]["tags"] = tags
                logger.info(f"Tagged resource: {api_id}")
                return True
            
            client = self._get_client("appsync")
            
            try:
                client.tag_resource(resourceArn=f"arn:aws:appsync:{self.region}:*:graphqlapis/{api_id}", tags=tags)
                logger.info(f"Tagged resource: {api_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to tag resource: {e}")
                raise
    
    def untag_resource(
        self,
        api_id: str,
        tag_keys: List[str]
    ) -> bool:
        """
        Remove tags from an AppSync resource.
        
        Args:
            api_id: API ID
            tag_keys: Tag keys to remove
            
        Returns:
            True if successful
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                logger.info(f"Untagged resource: {api_id}")
                return True
            
            client = self._get_client("appsync")
            
            try:
                client.untag_resource(
                    resourceArn=f"arn:aws:appsync:{self.region}:*:graphqlapis/{api_id}",
                    tagKeys=tag_keys
                )
                logger.info(f"Untagged resource: {api_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to untag resource: {e}")
                raise
    
    def list_tags_for_resource(self, api_id: str) -> Dict[str, str]:
        """
        List tags for an AppSync resource.
        
        Args:
            api_id: API ID
            
        Returns:
            Dictionary of tags
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                api = self._graphql_apis.get(api_id, {})
                return api.get("tags", {})
            
            client = self._get_client("appsync")
            
            try:
                response = client.list_tags_for_resource(
                    resourceArn=f"arn:aws:appsync:{self.region}:*:graphqlapis/{api_id}"
                )
                return response.get("tags", {})
            except ClientError as e:
                logger.error(f"Failed to list tags: {e}")
                raise
    
    # ==================== Utility Methods ====================
    
    def get_api_info(self, api_id: str) -> Dict[str, Any]:
        """
        Get comprehensive API information.
        
        Args:
            api_id: API ID
            
        Returns:
            Comprehensive API information
        """
        with self._lock:
            api = self.get_graphql_api(api_id)
            
            info = {
                "api": api,
                "dataSources": self.list_data_sources(api_id),
                "resolvers": self.list_resolvers(api_id),
                "functions": self.list_functions(api_id),
                "apiKeys": self.list_api_keys(api_id),
                "subscriptions": self.list_subscriptions(api_id)
            }
            
            cognito_config = self.get_cognito_auth_config(api_id)
            if cognito_config:
                info["cognitoAuth"] = cognito_config
            
            lambda_config = self.get_lambda_authorizer_config(api_id)
            if lambda_config:
                info["lambdaAuthorizer"] = lambda_config
            
            cw_config = self.get_cloudwatch_config(api_id)
            if cw_config:
                info["cloudwatch"] = cw_config
            
            return info
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of AppSync integration.
        
        Returns:
            Health status
        """
        with self._lock:
            status = {
                "service": "AppSync",
                "region": self.region,
                "boto3_available": BOTO3_AVAILABLE,
                "apis_managed": len(self._graphql_apis),
                "data_sources_managed": sum(len(ds) for ds in self._data_sources.values()),
                "resolvers_managed": sum(len(r) for r in self._resolvers.values()),
                "functions_managed": sum(len(f) for f in self._functions.values()),
                "healthy": True
            }
            
            if BOTO3_AVAILABLE:
                try:
                    client = self._get_client("appsync")
                    client.list_graphql_apis()
                except Exception as e:
                    status["healthy"] = False
                    status["error"] = str(e)
            
            return status
