"""
AWS Lambda Serverless Integration Module for Workflow System

Implements a LambdaIntegration class with:
1. Function management: Create/manage Lambda functions
2. Function invocation: Invoke functions
3. Layer management: Manage Lambda layers
4. Alias management: Create/manage aliases
5. Version management: Publish/manage versions
6. Event source mapping: Connect to event sources
7. Concurrency management: Configure reserved/provisioned concurrency
8. Layer sharing: Share layers across accounts
9. CloudWatch integration: Logs and metrics
10. SAM support: SAM template support

Commit: 'feat(aws-lambda): add AWS Lambda integration with function management, invocation, layers, aliases, versions, event sources, concurrency, CloudWatch, SAM support'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import zipfile
import io
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os

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


class Runtime(Enum):
    """Lambda runtime environments."""
    PYTHON38 = "python3.8"
    PYTHON39 = "python3.9"
    PYTHON310 = "python3.10"
    PYTHON311 = "python3.11"
    PYTHON312 = "python3.12"
    NODEJS14 = "nodejs14.x"
    NODEJS16 = "nodejs16.x"
    NODEJS18 = "nodejs18.x"
    NODEJS20 = "nodejs20.x"
    JAVA8 = "java8"
    JAVA8_AL2 = "java8.al2"
    JAVA11 = "java11"
    JAVA17 = "java17"
    JAVA21 = "java21"
    DOTNET6 = "dotnet6"
    DOTNET7 = "dotnet7"
    DOTNET8 = "dotnet8"
    GO1X = "go1.x"
    RUBY27 = "ruby2.7"
    RUBY32 = "ruby3.2"
    PROVIDED = "provided"
    PROVIDED_AL2 = "provided.al2"
    PROVIDED_AL2023 = "provided.al2023"


class InvocationType(Enum):
    """Lambda invocation types."""
    REQUEST_RESPONSE = "RequestResponse"
    EVENT = "Event"
    DRY_RUN = "DryRun"


class LogType(Enum):
    """CloudWatch log types."""
    NONE = "None"
    TAIL = "Tail"


class Architecture(Enum):
    """Lambda architectures."""
    X86_64 = "x86_64"
    ARM64 = "arm64"


class EventSourceType(Enum):
    """Event source types for Lambda."""
    SQS = "sqs"
    SNS = "sns"
    KINESIS = "kinesis"
    DYNAMODB = "dynamodb"
    S3 = "s3"
    API_GATEWAY = "apigateway"
    SCHEDULED = "scheduled"
    CLOUDWATCH_EVENTS = "cloudwatch_events"
    SQS_FIFO = "sqs_fifo"
    MSK = "msk"
    KAFKA = "kafka"
    SELF_MANAGED_KAFKA = "self_managed_kafka"


class ConcurrencyType(Enum):
    """Concurrency configuration types."""
    RESERVED = "reserved"
    PROVISIONED = "provisioned"


@dataclass
class LambdaConfig:
    """Configuration for Lambda connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    config: Optional[Any] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class FunctionConfig:
    """Configuration for creating a Lambda function."""
    function_name: str
    runtime: Union[Runtime, str]
    handler: str
    code: Union[str, bytes, Dict[str, Any]]
    role_arn: str
    description: str = ""
    timeout: int = 3
    memory_size: int = 128
    publish: bool = False
    zip_file_path: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    s3_object_version: Optional[str] = None
    environment_variables: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    layers: List[str] = field(default_factory=list)
    dead_letter_config: Optional[Dict[str, str]] = None
    tracing_config: Optional[Dict[str, str]] = None
    security_group_ids: List[str] = field(default_factory=list)
    subnet_ids: List[str] = field(default_factory=list)
    runtime_version_config: Optional[Dict[str, str]] = None
    architectures: List[Architecture] = field(default_factory=lambda: [Architecture.X86_64])
    ephemeral_storage: Optional[Dict[str, int]] = None
    snap_start: bool = False
    file_system_configs: List[Dict[str, str]] = field(default_factory=list)
    image_uri: Optional[str] = None
    package_type: str = "Zip"


@dataclass
class FunctionInfo:
    """Information about a Lambda function."""
    function_name: str
    function_arn: str
    runtime: str
    handler: str
    code_size: int
    description: str
    timeout: int
    memory_size: int
    last_modified: Optional[str] = None
    version: Optional[str] = None
    architectures: List[str] = field(default_factory=list)
    state: Optional[str] = None
    state_reason: Optional[str] = None
    environment: Optional[Dict[str, Any]] = None
    layers: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    kms_key_arn: Optional[str] = None
    master_arn: Optional[str] = None
    revision_id: Optional[str] = None


@dataclass
class LayerConfig:
    """Configuration for creating a Lambda layer."""
    layer_name: str
    description: str = ""
    code: Union[str, bytes, Dict[str, Any]]
    compatible_runtimes: List[Union[Runtime, str]] = field(default_factory=list)
    license_info: str = ""
    zip_file_path: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    s3_object_version: Optional[str] = None


@dataclass
class LayerInfo:
    """Information about a Lambda layer."""
    layer_name: str
    layer_arn: str
    version: int
    description: str
    created_date: str
    compatible_runtimes: List[str] = field(default_factory=list)
    license_info: str = ""
    code_sha256: Optional[str] = None
    code_size: int = 0


@dataclass
class AliasConfig:
    """Configuration for creating an alias."""
    name: str
    function_name: str
    function_version: str
    description: str = ""
    routing_config: Optional[Dict[str, Any]] = None


@dataclass
class AliasInfo:
    """Information about an alias."""
    name: str
    alias_arn: str
    function_name: str
    function_version: str
    description: str
    routing_config: Dict[str, Any] = field(default_factory=dict)
    revision_id: Optional[str] = None


@dataclass
class VersionInfo:
    """Information about a function version."""
    version: str
    function_name: str
    function_arn: str
    code_sha256: str
    code_size: int
    description: str
    last_modified: str
    revision_id: Optional[str] = None


@dataclass
class EventSourceMappingConfig:
    """Configuration for creating an event source mapping."""
    event_source_arn: str
    function_name: str
    enabled: bool = True
    batch_size: int = 100
    start_position: str = "LATEST"
    destination_config: Optional[Dict[str, Any]] = None
    filter_criteria: Optional[Dict[str, Any]] = None
    parallelization_factor: Optional[int] = None
    tumbling_window_in_seconds: Optional[int] = None
    maximum_record_age_in_seconds: Optional[int] = None
    bisect_batch_on_function_error: bool = False
    maximum_retry_attempts: Optional[int] = None
    source_access_configurations: List[Dict[str, str]] = field(default_factory=list)
    self_managed_event_source: Optional[Dict[str, Any]] = None
    topics: List[str] = field(default_factory=list)
    queues: List[str] = field(default_factory=list)


@dataclass
class EventSourceMappingInfo:
    """Information about an event source mapping."""
    uuid: str
    event_source_arn: str
    function_arn: str
    last_modified: Optional[str] = None
    last_processing_result: Optional[str] = None
    state: Optional[str] = None
    state_transition_reason: Optional[str] = None


@dataclass
class ConcurrencyConfig:
    """Configuration for reserved or provisioned concurrency."""
    function_name: str
    reserved_concurrent_executions: Optional[int] = None
    provisioned_concurrent_executions: Optional[int] = None
    qualifier: str = "$LATEST"


@dataclass
class SAMTemplate:
    """SAM template representation."""
    AWSTemplateFormatVersion: str = "2010-09-09"
    Transform: str = "AWS::Serverless-2016-10-31"
    Description: str = ""
    Globals: Dict[str, Any] = field(default_factory=dict)
    Parameters: Dict[str, Any] = field(default_factory=dict)
    Resources: Dict[str, Any] = field(default_factory=dict)
    Outputs: Dict[str, Any] = field(default_factory=dict)
    Metadata: Dict[str, Any] = field(default_factory=dict)
    Conditions: Dict[str, Any] = field(default_factory=dict)
    Mappings: Dict[str, Any] = field(default_factory=dict)


class LambdaIntegration:
    """
    AWS Lambda integration class for serverless operations.
    
    Supports:
    - Function creation, update, delete, and management
    - Synchronous and asynchronous function invocation
    - Layer management for reusable components
    - Alias management for version routing
    - Version publishing and management
    - Event source mapping (SQS, Kinesis, DynamoDB, etc.)
    - Reserved and provisioned concurrency
    - Cross-account layer sharing
    - CloudWatch Logs and Metrics integration
    - AWS SAM template support
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        lambda_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None,
        iam_client: Optional[Any] = None,
        logs_client: Optional[Any] = None,
        s3_client: Optional[Any] = None
    ):
        """
        Initialize Lambda integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: Lambda endpoint URL (for testing with LocalStack, etc.)
            lambda_client: Pre-configured Lambda client (overrides boto3 creation)
            cloudwatch_client: Pre-configured CloudWatch client for metrics
            iam_client: Pre-configured IAM client for role management
            logs_client: Pre-configured CloudWatch Logs client
            s3_client: Pre-configured S3 client for code storage
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for Lambda integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        
        if lambda_client:
            self.lambda_client = lambda_client
        else:
            session_kwargs = {"region_name": region_name}
            if aws_access_key_id and aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key
            self.lambda_client = boto3.client("lambda", endpoint_url=endpoint_url, **session_kwargs)
        
        if cloudwatch_client:
            self.cloudwatch_client = cloudwatch_client
        else:
            self.cloudwatch_client = boto3.client("cloudwatch", region_name=region_name)
        
        if iam_client:
            self.iam_client = iam_client
        else:
            self.iam_client = boto3.client("iam", region_name=region_name)
        
        if logs_client:
            self.logs_client = logs_client
        else:
            self.logs_client = boto3.client("logs", region_name=region_name)
        
        if s3_client:
            self.s3_client = s3_client
        else:
            self.s3_client = boto3.client("s3", region_name=region_name)
        
        self._function_cache: Dict[str, FunctionInfo] = {}
        self._layer_cache: Dict[str, LayerInfo] = {}
        self._lock = threading.Lock()
    
    def _get_sha256(self, data: bytes) -> str:
        """Calculate SHA256 hash of data."""
        return hashlib.sha256(data).hexdigest()
    
    def _prepare_code_package(self, code: Union[str, bytes, Dict[str, Any]], zip_file_path: Optional[str] = None) -> Dict[str, Any]:
        """Prepare code package for Lambda function."""
        if isinstance(code, dict):
            return code
        elif isinstance(code, str) and os.path.isfile(code):
            with open(code, "rb") as f:
                code_bytes = f.read()
            return {"ZipFile": code_bytes}
        elif isinstance(code, bytes):
            return {"ZipFile": code}
        elif zip_file_path and os.path.isfile(zip_file_path):
            with open(zip_file_path, "rb") as f:
                code_bytes = f.read()
            return {"ZipFile": code_bytes}
        else:
            raise ValueError("Code must be a valid file path, bytes, or S3 location dict")
    
    # =========================================================================
    # Function Management
    # =========================================================================
    
    def create_function(self, config: FunctionConfig) -> FunctionInfo:
        """
        Create a new Lambda function.
        
        Args:
            config: Function configuration
            
        Returns:
            FunctionInfo object with created function details
        """
        kwargs = {
            "FunctionName": config.function_name,
            "Runtime": config.runtime.value if isinstance(config.runtime, Runtime) else config.runtime,
            "Handler": config.handler,
            "Role": config.role_arn,
            "Description": config.description,
            "Timeout": config.timeout,
            "MemorySize": config.memory_size,
            "Publish": config.publish,
            "Tags": config.tags,
        }
        
        code_package = self._prepare_code_package(config.code, config.zip_file_path)
        if "ZipFile" in code_package:
            kwargs["ZipFile"] = code_package["ZipFile"]
        elif "S3Bucket" in code_package:
            kwargs.update(code_package)
        
        if config.environment_variables:
            kwargs["Environment"] = {"Variables": config.environment_variables}
        
        if config.layers:
            kwargs["Layers"] = config.layers
        
        if config.dead_letter_config:
            kwargs["DeadLetterConfig"] = config.dead_letter_config
        
        if config.tracing_config:
            kwargs["TracingConfig"] = config.tracing_config
        
        if config.architectures:
            kwargs["Architectures"] = [a.value if isinstance(a, Architecture) else a for a in config.architectures]
        
        if config.ephemeral_storage:
            kwargs["EphemeralStorage"] = config.ephemeral_storage
        
        if config.file_system_configs:
            kwargs["FileSystemConfigs"] = config.file_system_configs
        
        if config.image_uri:
            kwargs["ImageUri"] = config.image_uri
            kwargs["PackageType"] = "Image"
        
        try:
            response = self.lambda_client.create_function(**kwargs)
            function_info = self._parse_function_info(response)
            with self._lock:
                self._function_cache[config.function_name] = function_info
            return function_info
        except ClientError as e:
            logger.error(f"Failed to create function {config.function_name}: {e}")
            raise
    
    def get_function(self, function_name: str, qualifier: Optional[str] = None) -> Optional[FunctionInfo]:
        """
        Get information about a Lambda function.
        
        Args:
            function_name: Name or ARN of the function
            qualifier: Version, alias, or $LATEST
            
        Returns:
            FunctionInfo object or None if not found
        """
        with self._lock:
            cache_key = f"{function_name}:{qualifier}"
            if cache_key in self._function_cache:
                return self._function_cache[cache_key]
        
        kwargs = {"FunctionName": function_name}
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            response = self.lambda_client.get_function(**kwargs)
            function_info = self._parse_function_info(response.get("Configuration", {}))
            function_info.code_size = response.get("Code", {}).get("Size", 0)
            function_info.location = response.get("Code", {}).get("Location")
            function_info.tags = response.get("Tags", {})
            
            with self._lock:
                self._function_cache[cache_key] = function_info
            return function_info
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def update_function_code(self, function_name: str, code: Union[str, bytes, Dict[str, Any]], 
                             zip_file_path: Optional[str] = None, 
                             revision_id: Optional[str] = None) -> FunctionInfo:
        """
        Update the code of an existing Lambda function.
        
        Args:
            function_name: Name of the function
            code: New code (file path, bytes, or S3 location dict)
            zip_file_path: Path to zip file
            revision_id: Expected revision ID for conditional update
            
        Returns:
            Updated FunctionInfo object
        """
        kwargs = {"FunctionName": function_name}
        code_package = self._prepare_code_package(code, zip_file_path)
        kwargs.update(code_package)
        if revision_id:
            kwargs["RevisionId"] = revision_id
        
        try:
            response = self.lambda_client.update_function_code(**kwargs)
            function_info = self._parse_function_info(response)
            with self._lock:
                self._function_cache[function_name] = function_info
            return function_info
        except ClientError as e:
            logger.error(f"Failed to update function code for {function_name}: {e}")
            raise
    
    def update_function_configuration(self, function_name: str, 
                                       timeout: Optional[int] = None,
                                       memory_size: Optional[int] = None,
                                       environment_variables: Optional[Dict[str, str]] = None,
                                       runtime: Optional[Union[Runtime, str]] = None,
                                       handler: Optional[str] = None,
                                       description: Optional[str] = None,
                                       layers: Optional[List[str]] = None,
                                       revision_id: Optional[str] = None) -> FunctionInfo:
        """
        Update function configuration.
        
        Args:
            function_name: Name of the function
            timeout: New timeout value
            memory_size: New memory size
            environment_variables: New environment variables
            runtime: New runtime
            handler: New handler
            description: New description
            layers: New layers list
            revision_id: Expected revision ID for conditional update
            
        Returns:
            Updated FunctionInfo object
        """
        kwargs = {"FunctionName": function_name}
        
        if timeout is not None:
            kwargs["Timeout"] = timeout
        if memory_size is not None:
            kwargs["MemorySize"] = memory_size
        if environment_variables is not None:
            kwargs["Environment"] = {"Variables": environment_variables}
        if runtime is not None:
            kwargs["Runtime"] = runtime.value if isinstance(runtime, Runtime) else runtime
        if handler is not None:
            kwargs["Handler"] = handler
        if description is not None:
            kwargs["Description"] = description
        if layers is not None:
            kwargs["Layers"] = layers
        if revision_id:
            kwargs["RevisionId"] = revision_id
        
        try:
            response = self.lambda_client.update_function_configuration(**kwargs)
            function_info = self._parse_function_info(response)
            with self._lock:
                self._function_cache[function_name] = function_info
            return function_info
        except ClientError as e:
            logger.error(f"Failed to update function configuration for {function_name}: {e}")
            raise
    
    def delete_function(self, function_name: str, qualifier: Optional[str] = None) -> bool:
        """
        Delete a Lambda function.
        
        Args:
            function_name: Name of the function
            qualifier: Version or alias to delete (deletes entire function if None)
            
        Returns:
            True if deleted successfully
        """
        kwargs = {"FunctionName": function_name}
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            self.lambda_client.delete_function(**kwargs)
            with self._lock:
                self._function_cache.pop(function_name, None)
                for key in list(self._function_cache.keys()):
                    if key.startswith(f"{function_name}:"):
                        self._function_cache.pop(key, None)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete function {function_name}: {e}")
            raise
    
    def list_functions(self, marker: Optional[str] = None, max_items: int = 100,
                        function_version: str = "ALL") -> Dict[str, Any]:
        """
        List Lambda functions.
        
        Args:
            marker: Pagination token
            max_items: Maximum number of functions to return
            function_version: Include versions or just $LATEST
            
        Returns:
            Dict with 'functions' list and 'next_marker'
        """
        kwargs = {"MaxItems": max_items, "FunctionVersion": function_version}
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_functions(**kwargs)
            functions = [self._parse_function_info(f) for f in response.get("Functions", [])]
            return {
                "functions": functions,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list functions: {e}")
            raise
    
    def invoke_function(self, function_name: str, payload: Union[str, Dict, Any] = None,
                        invocation_type: InvocationType = InvocationType.REQUEST_RESPONSE,
                        log_type: LogType = LogType.NONE,
                        client_context: Optional[Dict] = None,
                        qualifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Invoke a Lambda function.
        
        Args:
            function_name: Name or ARN of the function
            payload: Event data to pass to the function
            invocation_type: REQUEST_RESPONSE, EVENT, or DRY_RUN
            log_type: NONE or TAIL for CloudWatch logs
            client_context: Custom context to pass to the function
            qualifier: Version or alias
            
        Returns:
            Dict with 'status_code', 'payload', 'logs', etc.
        """
        kwargs = {
            "FunctionName": function_name,
            "InvocationType": invocation_type.value,
            "LogType": log_type.value
        }
        
        if payload:
            if isinstance(payload, dict):
                kwargs["Payload"] = json.dumps(payload)
            else:
                kwargs["Payload"] = payload
        
        if client_context:
            kwargs["ClientContext"] = base64.b64encode(json.dumps(client_context).encode()).decode()
        
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            response = self.lambda_client.invoke(**kwargs)
            result = {
                "status_code": response.get("StatusCode"),
                "function_error": response.get("FunctionError"),
                "log_result": response.get("LogResult"),
            }
            
            if response.get("Payload"):
                result["payload"] = json.loads(response["Payload"].read().decode())
            
            return result
        except ClientError as e:
            logger.error(f"Failed to invoke function {function_name}: {e}")
            raise
    
    def invoke_async(self, function_name: str, payload: Union[str, Dict, Any] = None,
                     qualifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Invoke a Lambda function asynchronously.
        
        Args:
            function_name: Name or ARN of the function
            payload: Event data to pass to the function
            qualifier: Version or alias
            
        Returns:
            Dict with 'request_id' and 'status_code'
        """
        return self.invoke_function(
            function_name=function_name,
            payload=payload,
            invocation_type=InvocationType.EVENT,
            qualifier=qualifier
        )
    
    def invoke_function_url(self, function_name: str, payload: Union[str, Dict, Any] = None,
                           http_method: str = "POST") -> Dict[str, Any]:
        """
        Invoke a function URL.
        
        Args:
            function_name: Name or ARN of the function
            payload: Event data to pass to the function
            http_method: HTTP method to use
            
        Returns:
            Dict with 'status_code', 'headers', 'payload', etc.
        """
        if not function_name.startswith("https://"):
            function_name = f"https://{function_name}"
        
        kwargs = {"FunctionName": function_name, "HttpMethod": http_method}
        
        if payload:
            if isinstance(payload, dict):
                kwargs["Payload"] = json.dumps(payload)
            else:
                kwargs["Payload"] = payload
        
        try:
            response = self.lambda_client.invoke_with_function_url(**kwargs)
            result = {
                "status_code": response.get("StatusCode"),
                "headers": dict(response.get("Headers", {})),
            }
            
            if response.get("Body"):
                result["body"] = json.loads(response["Body"].read().decode())
            
            return result
        except ClientError as e:
            logger.error(f"Failed to invoke function URL {function_name}: {e}")
            raise
    
    def _parse_function_info(self, response: Dict[str, Any]) -> FunctionInfo:
        """Parse function info from Lambda response."""
        return FunctionInfo(
            function_name=response.get("FunctionName", ""),
            function_arn=response.get("FunctionArn", ""),
            runtime=response.get("Runtime", ""),
            handler=response.get("Handler", ""),
            code_size=response.get("CodeSize", 0),
            description=response.get("Description", ""),
            timeout=response.get("Timeout", 3),
            memory_size=response.get("MemorySize", 128),
            last_modified=response.get("LastModified"),
            version=response.get("Version"),
            architectures=response.get("Architectures", []),
            state=response.get("State"),
            state_reason=response.get("StateReason"),
            environment=response.get("Environment"),
            layers=response.get("Layers", []),
            tags=response.get("Tags", {}),
            kms_key_arn=response.get("KMSKeyArn"),
            master_arn=response.get("MasterArn"),
            revision_id=response.get("RevisionId")
        )
    
    # =========================================================================
    # Layer Management
    # =========================================================================
    
    def create_layer(self, config: LayerConfig) -> LayerInfo:
        """
        Create a new Lambda layer.
        
        Args:
            config: Layer configuration
            
        Returns:
            LayerInfo object with created layer details
        """
        kwargs = {
            "LayerName": config.layer_name,
            "Description": config.description,
            "LicenseInfo": config.license_info,
        }
        
        if isinstance(config.code, dict):
            kwargs.update(config.code)
        elif isinstance(config.code, str) and os.path.isfile(config.code):
            with open(config.code, "rb") as f:
                kwargs["ZipFile"] = f.read()
        elif isinstance(config.code, bytes):
            kwargs["ZipFile"] = config.code
        elif config.zip_file_path and os.path.isfile(config.zip_file_path):
            with open(config.zip_file_path, "rb") as f:
                kwargs["ZipFile"] = f.read()
        
        if config.compatible_runtimes:
            kwargs["CompatibleRuntimes"] = [
                r.value if isinstance(r, Runtime) else r 
                for r in config.compatible_runtimes
            ]
        
        try:
            response = self.lambda_client.publish_layer_version(**kwargs)
            layer_info = self._parse_layer_info(response)
            with self._lock:
                self._layer_cache[config.layer_name] = layer_info
            return layer_info
        except ClientError as e:
            logger.error(f"Failed to create layer {config.layer_name}: {e}")
            raise
    
    def get_layer_info(self, layer_name: str, version: int) -> Optional[LayerInfo]:
        """
        Get information about a layer version.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            
        Returns:
            LayerInfo object or None if not found
        """
        try:
            response = self.lambda_client.get_layer_version(
                LayerName=layer_name,
                VersionNumber=version
            )
            return self._parse_layer_info(response)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def list_layers(self, compatible_runtime: Optional[Union[Runtime, str]] = None,
                    marker: Optional[str] = None, max_items: int = 100) -> Dict[str, Any]:
        """
        List available Lambda layers.
        
        Args:
            compatible_runtime: Filter by compatible runtime
            marker: Pagination token
            max_items: Maximum number of layers to return
            
        Returns:
            Dict with 'layers' list and 'next_marker'
        """
        kwargs = {"MaxItems": max_items}
        if compatible_runtime:
            kwargs["CompatibleRuntime"] = (
                compatible_runtime.value if isinstance(compatible_runtime, Runtime) 
                else compatible_runtime
            )
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_layers(**kwargs)
            layers = [self._parse_layer_info(l) for l in response.get("Layers", [])]
            return {
                "layers": layers,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list layers: {e}")
            raise
    
    def list_layer_versions(self, layer_name: str, marker: Optional[str] = None,
                            max_items: int = 100) -> Dict[str, Any]:
        """
        List all versions of a layer.
        
        Args:
            layer_name: Name of the layer
            marker: Pagination token
            max_items: Maximum number of versions to return
            
        Returns:
            Dict with 'versions' list and 'next_marker'
        """
        kwargs = {"LayerName": layer_name, "MaxItems": max_items}
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_layer_versions(**kwargs)
            versions = [self._parse_layer_info(v) for v in response.get("LayerVersions", [])]
            return {
                "versions": versions,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list layer versions for {layer_name}: {e}")
            raise
    
    def delete_layer_version(self, layer_name: str, version: int) -> bool:
        """
        Delete a layer version.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            
        Returns:
            True if deleted successfully
        """
        try:
            self.lambda_client.delete_layer_version(
                LayerName=layer_name,
                VersionNumber=version
            )
            with self._lock:
                self._layer_cache.pop(f"{layer_name}:{version}", None)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete layer version {layer_name}:{version}: {e}")
            raise
    
    def add_layer_to_function(self, function_name: str, layer_name: str, 
                              layer_version: int, qualifier: Optional[str] = None) -> FunctionInfo:
        """
        Add a layer to a function.
        
        Args:
            function_name: Name of the function
            layer_name: Name of the layer
            layer_version: Layer version number
            qualifier: Function qualifier
            
        Returns:
            Updated FunctionInfo object
        """
        layer_arn = f"arn:aws:lambda:{self.region_name}:*:layer:{layer_name}:{layer_version}"
        
        try:
            current_info = self.get_function(function_name, qualifier)
            if not current_info:
                raise ValueError(f"Function {function_name} not found")
            
            current_layers = current_info.layers or []
            new_layer_arn = {"Arn": layer_arn}
            
            existing_arns = [l.get("Arn") for l in current_layers]
            if layer_arn not in existing_arns:
                current_layers.append(new_layer_arn)
            
            layers = [l.get("Arn") for l in current_layers]
            return self.update_function_configuration(function_name, layers=layers)
        except ClientError as e:
            logger.error(f"Failed to add layer to function {function_name}: {e}")
            raise
    
    def remove_layer_from_function(self, function_name: str, layer_name: str,
                                    layer_version: int, qualifier: Optional[str] = None) -> FunctionInfo:
        """
        Remove a layer from a function.
        
        Args:
            function_name: Name of the function
            layer_name: Name of the layer
            layer_version: Layer version number
            qualifier: Function qualifier
            
        Returns:
            Updated FunctionInfo object
        """
        layer_arn = f"arn:aws:lambda:{self.region_name}:*:layer:{layer_name}:{layer_version}"
        
        try:
            current_info = self.get_function(function_name, qualifier)
            if not current_info:
                raise ValueError(f"Function {function_name} not found")
            
            current_layers = current_info.layers or []
            updated_layers = [l for l in current_layers if l.get("Arn") != layer_arn]
            
            layers = [l.get("Arn") for l in updated_layers]
            return self.update_function_configuration(function_name, layers=layers)
        except ClientError as e:
            logger.error(f"Failed to remove layer from function {function_name}: {e}")
            raise
    
    def _parse_layer_info(self, response: Dict[str, Any]) -> LayerInfo:
        """Parse layer info from Lambda response."""
        return LayerInfo(
            layer_name=response.get("LayerName", ""),
            layer_arn=response.get("LayerVersionArn", ""),
            version=response.get("Version", 1),
            description=response.get("Description", ""),
            created_date=response.get("CreatedDate", ""),
            compatible_runtimes=response.get("CompatibleRuntimes", []),
            license_info=response.get("LicenseInfo", ""),
            code_sha256=response.get("CodeSha256"),
            code_size=response.get("CodeSize", 0)
        )
    
    # =========================================================================
    # Alias Management
    # =========================================================================
    
    def create_alias(self, config: AliasConfig) -> AliasInfo:
        """
        Create an alias for a function version.
        
        Args:
            config: Alias configuration
            
        Returns:
            AliasInfo object with created alias details
        """
        kwargs = {
            "Name": config.name,
            "FunctionName": config.function_name,
            "FunctionVersion": config.function_version,
            "Description": config.description
        }
        
        if config.routing_config:
            kwargs["RoutingConfig"] = config.routing_config
        
        try:
            response = self.lambda_client.create_alias(**kwargs)
            return self._parse_alias_info(response)
        except ClientError as e:
            logger.error(f"Failed to create alias {config.name}: {e}")
            raise
    
    def get_alias(self, function_name: str, name: str) -> Optional[AliasInfo]:
        """
        Get information about an alias.
        
        Args:
            function_name: Name of the function
            name: Alias name
            
        Returns:
            AliasInfo object or None if not found
        """
        try:
            response = self.lambda_client.get_alias(
                FunctionName=function_name,
                Name=name
            )
            return self._parse_alias_info(response)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def update_alias(self, function_name: str, name: str,
                    function_version: Optional[str] = None,
                    description: Optional[str] = None,
                    routing_config: Optional[Dict[str, Any]] = None,
                    revision_id: Optional[str] = None) -> AliasInfo:
        """
        Update an alias.
        
        Args:
            function_name: Name of the function
            name: Alias name
            function_version: New function version
            description: New description
            routing_config: New routing configuration
            revision_id: Expected revision ID for conditional update
            
        Returns:
            Updated AliasInfo object
        """
        kwargs = {"FunctionName": function_name, "Name": name}
        
        if function_version is not None:
            kwargs["FunctionVersion"] = function_version
        if description is not None:
            kwargs["Description"] = description
        if routing_config is not None:
            kwargs["RoutingConfig"] = routing_config
        if revision_id:
            kwargs["RevisionId"] = revision_id
        
        try:
            response = self.lambda_client.update_alias(**kwargs)
            return self._parse_alias_info(response)
        except ClientError as e:
            logger.error(f"Failed to update alias {name}: {e}")
            raise
    
    def delete_alias(self, function_name: str, name: str) -> bool:
        """
        Delete an alias.
        
        Args:
            function_name: Name of the function
            name: Alias name
            
        Returns:
            True if deleted successfully
        """
        try:
            self.lambda_client.delete_alias(
                FunctionName=function_name,
                Name=name
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete alias {name}: {e}")
            raise
    
    def list_aliases(self, function_name: str, marker: Optional[str] = None,
                     max_items: int = 100) -> Dict[str, Any]:
        """
        List aliases for a function.
        
        Args:
            function_name: Name of the function
            marker: Pagination token
            max_items: Maximum number of aliases to return
            
        Returns:
            Dict with 'aliases' list and 'next_marker'
        """
        kwargs = {"FunctionName": function_name, "MaxItems": max_items}
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_aliases(**kwargs)
            aliases = [self._parse_alias_info(a) for a in response.get("Aliases", [])]
            return {
                "aliases": aliases,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list aliases for {function_name}: {e}")
            raise
    
    def _parse_alias_info(self, response: Dict[str, Any]) -> AliasInfo:
        """Parse alias info from Lambda response."""
        return AliasInfo(
            name=response.get("Name", ""),
            alias_arn=response.get("AliasArn", ""),
            function_name=response.get("FunctionName", ""),
            function_version=response.get("FunctionVersion", ""),
            description=response.get("Description", ""),
            routing_config=response.get("RoutingConfig", {}),
            revision_id=response.get("RevisionId")
        )
    
    # =========================================================================
    # Version Management
    # =========================================================================
    
    def publish_version(self, function_name: str, description: str = "",
                       code_sha256: Optional[str] = None,
                       revision_id: Optional[str] = None) -> VersionInfo:
        """
        Publish a new version of a function.
        
        Args:
            function_name: Name of the function
            description: Description for the version
            code_sha256: Expected code SHA256 for conditional publish
            revision_id: Expected revision ID for conditional update
            
        Returns:
            VersionInfo object with published version details
        """
        kwargs = {"FunctionName": function_name, "Description": description}
        if code_sha256:
            kwargs["CodeSha256"] = code_sha256
        if revision_id:
            kwargs["RevisionId"] = revision_id
        
        try:
            response = self.lambda_client.publish_version(**kwargs)
            return self._parse_version_info(response)
        except ClientError as e:
            logger.error(f"Failed to publish version for {function_name}: {e}")
            raise
    
    def get_version(self, function_name: str, version: str) -> Optional[VersionInfo]:
        """
        Get information about a function version.
        
        Args:
            function_name: Name of the function
            version: Version identifier
            
        Returns:
            VersionInfo object or None if not found
        """
        try:
            response = self.lambda_client.get_function(
                FunctionName=function_name,
                Qualifier=version
            )
            config = response.get("Configuration", {})
            return VersionInfo(
                version=config.get("Version", ""),
                function_name=config.get("FunctionName", ""),
                function_arn=config.get("FunctionArn", ""),
                code_sha256=response.get("Code", {}).get("CodeSha256", ""),
                code_size=response.get("Code", {}).get("Size", 0),
                description=config.get("Description", ""),
                last_modified=config.get("LastModified", ""),
                revision_id=config.get("RevisionId")
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def list_versions(self, function_name: str, marker: Optional[str] = None,
                      max_items: int = 100) -> Dict[str, Any]:
        """
        List all versions of a function.
        
        Args:
            function_name: Name of the function
            marker: Pagination token
            max_items: Maximum number of versions to return
            
        Returns:
            Dict with 'versions' list and 'next_marker'
        """
        kwargs = {"FunctionName": function_name, "MaxItems": max_items}
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_versions_by_function(**kwargs)
            versions = [self._parse_version_info(v) for v in response.get("Versions", [])]
            return {
                "versions": versions,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list versions for {function_name}: {e}")
            raise
    
    def _parse_version_info(self, response: Dict[str, Any]) -> VersionInfo:
        """Parse version info from Lambda response."""
        return VersionInfo(
            version=response.get("Version", ""),
            function_name=response.get("FunctionName", ""),
            function_arn=response.get("FunctionArn", ""),
            code_sha256=response.get("CodeSha256", ""),
            code_size=response.get("CodeSize", 0),
            description=response.get("Description", ""),
            last_modified=response.get("LastModified", ""),
            revision_id=response.get("RevisionId")
        )
    
    # =========================================================================
    # Event Source Mapping
    # =========================================================================
    
    def create_event_source_mapping(self, config: EventSourceMappingConfig) -> EventSourceMappingInfo:
        """
        Create an event source mapping to connect Lambda to event sources.
        
        Args:
            config: Event source mapping configuration
            
        Returns:
            EventSourceMappingInfo object with created mapping details
        """
        kwargs = {
            "EventSourceArn": config.event_source_arn,
            "FunctionName": config.function_name,
            "Enabled": config.enabled,
            "BatchSize": config.batch_size,
            "StartingPosition": config.start_position
        }
        
        if config.destination_config:
            kwargs["DestinationConfig"] = config.destination_config
        if config.filter_criteria:
            kwargs["FilterCriteria"] = config.filter_criteria
        if config.parallelization_factor is not None:
            kwargs["ParallelizationFactor"] = config.parallelization_factor
        if config.tumbling_window_in_seconds is not None:
            kwargs["TumblingWindowInSeconds"] = config.tumbling_window_in_seconds
        if config.maximum_record_age_in_seconds is not None:
            kwargs["MaximumRecordAgeInSeconds"] = config.maximum_record_age_in_seconds
        if config.maximum_retry_attempts is not None:
            kwargs["MaximumRetryAttempts"] = config.maximum_retry_attempts
        if config.source_access_configurations:
            kwargs["SourceAccessConfigurations"] = config.source_access_configurations
        if config.self_managed_event_source:
            kwargs["SelfManagedEventSource"] = config.self_managed_event_source
        if config.topics:
            kwargs["Topics"] = config.topics
        if config.queues:
            kwargs["Queues"] = config.queues
        
        try:
            response = self.lambda_client.create_event_source_mapping(**kwargs)
            return self._parse_event_source_mapping_info(response)
        except ClientError as e:
            logger.error(f"Failed to create event source mapping: {e}")
            raise
    
    def get_event_source_mapping(self, uuid: str) -> Optional[EventSourceMappingInfo]:
        """
        Get information about an event source mapping.
        
        Args:
            uuid: UUID of the event source mapping
            
        Returns:
            EventSourceMappingInfo object or None if not found
        """
        try:
            response = self.lambda_client.get_event_source_mapping(UUID=uuid)
            return self._parse_event_source_mapping_info(response)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def update_event_source_mapping(self, uuid: str,
                                    function_name: Optional[str] = None,
                                    enabled: Optional[bool] = None,
                                    batch_size: Optional[int] = None,
                                    destination_config: Optional[Dict[str, Any]] = None,
                                    filter_criteria: Optional[Dict[str, Any]] = None,
                                    parallelization_factor: Optional[int] = None,
                                    tumbling_window_in_seconds: Optional[int] = None,
                                    maximum_record_age_in_seconds: Optional[int] = None,
                                    maximum_retry_attempts: Optional[int] = None,
                                    source_access_configurations: Optional[List[Dict[str, str]]] = None) -> EventSourceMappingInfo:
        """
        Update an event source mapping.
        
        Args:
            uuid: UUID of the event source mapping
            function_name: New function name or ARN
            enabled: Enable or disable the mapping
            batch_size: New batch size
            destination_config: New destination configuration
            filter_criteria: New filter criteria
            parallelization_factor: New parallelization factor
            tumbling_window_in_seconds: New tumbling window
            maximum_record_age_in_seconds: New maximum record age
            maximum_retry_attempts: New maximum retry attempts
            source_access_configurations: New source access configurations
            
        Returns:
            Updated EventSourceMappingInfo object
        """
        kwargs = {"UUID": uuid}
        
        if function_name is not None:
            kwargs["FunctionName"] = function_name
        if enabled is not None:
            kwargs["Enabled"] = enabled
        if batch_size is not None:
            kwargs["BatchSize"] = batch_size
        if destination_config is not None:
            kwargs["DestinationConfig"] = destination_config
        if filter_criteria is not None:
            kwargs["FilterCriteria"] = filter_criteria
        if parallelization_factor is not None:
            kwargs["ParallelizationFactor"] = parallelization_factor
        if tumbling_window_in_seconds is not None:
            kwargs["TumblingWindowInSeconds"] = tumbling_window_in_seconds
        if maximum_record_age_in_seconds is not None:
            kwargs["MaximumRecordAgeInSeconds"] = maximum_record_age_in_seconds
        if maximum_retry_attempts is not None:
            kwargs["MaximumRetryAttempts"] = maximum_retry_attempts
        if source_access_configurations is not None:
            kwargs["SourceAccessConfigurations"] = source_access_configurations
        
        try:
            response = self.lambda_client.update_event_source_mapping(**kwargs)
            return self._parse_event_source_mapping_info(response)
        except ClientError as e:
            logger.error(f"Failed to update event source mapping {uuid}: {e}")
            raise
    
    def delete_event_source_mapping(self, uuid: str) -> bool:
        """
        Delete an event source mapping.
        
        Args:
            uuid: UUID of the event source mapping
            
        Returns:
            True if deleted successfully
        """
        try:
            self.lambda_client.delete_event_source_mapping(UUID=uuid)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete event source mapping {uuid}: {e}")
            raise
    
    def list_event_source_mappings(self, event_source_arn: Optional[str] = None,
                                   function_name: Optional[str] = None,
                                   marker: Optional[str] = None,
                                   max_items: int = 100) -> Dict[str, Any]:
        """
        List event source mappings.
        
        Args:
            event_source_arn: Filter by event source ARN
            function_name: Filter by function name
            marker: Pagination token
            max_items: Maximum number of mappings to return
            
        Returns:
            Dict with 'mappings' list and 'next_marker'
        """
        kwargs = {"MaxItems": max_items}
        if event_source_arn:
            kwargs["EventSourceArn"] = event_source_arn
        if function_name:
            kwargs["FunctionName"] = function_name
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_event_source_mappings(**kwargs)
            mappings = [self._parse_event_source_mapping_info(m) for m in response.get("EventSourceMappings", [])]
            return {
                "mappings": mappings,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list event source mappings: {e}")
            raise
    
    def _parse_event_source_mapping_info(self, response: Dict[str, Any]) -> EventSourceMappingInfo:
        """Parse event source mapping info from Lambda response."""
        return EventSourceMappingInfo(
            uuid=response.get("UUID", ""),
            event_source_arn=response.get("EventSourceArn", ""),
            function_arn=response.get("FunctionArn", ""),
            last_modified=response.get("LastModified"),
            last_processing_result=response.get("LastProcessingResult"),
            state=response.get("State"),
            state_transition_reason=response.get("StateTransitionReason")
        )
    
    # =========================================================================
    # Concurrency Management
    # =========================================================================
    
    def put_reserved_concurrency(self, function_name: str, 
                                  reserved_concurrent_executions: int) -> Dict[str, Any]:
        """
        Configure reserved concurrency for a function.
        
        Args:
            function_name: Name of the function
            reserved_concurrent_executions: Reserved concurrent executions (0 to disable)
            
        Returns:
            Dict with configuration details
        """
        kwargs = {
            "FunctionName": function_name,
            "ReservedConcurrentExecutions": reserved_concurrent_executions
        }
        
        try:
            response = self.lambda_client.put_reserved_concurrency(**kwargs)
            return {
                "function_name": response.get("FunctionName"),
                "reserved_concurrent_executions": response.get("ReservedConcurrentExecutions"),
                "revision_id": response.get("RevisionId")
            }
        except ClientError as e:
            logger.error(f"Failed to put reserved concurrency for {function_name}: {e}")
            raise
    
    def get_reserved_concurrency(self, function_name: str) -> Optional[Dict[str, Any]]:
        """
        Get reserved concurrency configuration for a function.
        
        Args:
            function_name: Name of the function
            
        Returns:
            Dict with 'reserved_concurrent_executions' or None if not configured
        """
        try:
            response = self.lambda_client.get_function_concurrency(
                FunctionName=function_name
            )
            return {
                "reserved_concurrent_executions": response.get("ReservedConcurrentExecutions"),
                "function_name": response.get("FunctionName")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def delete_reserved_concurrency(self, function_name: str) -> bool:
        """
        Remove reserved concurrency configuration.
        
        Args:
            function_name: Name of the function
            
        Returns:
            True if removed successfully
        """
        try:
            self.lambda_client.delete_reserved_concurrency(FunctionName=function_name)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete reserved concurrency for {function_name}: {e}")
            raise
    
    def put_provisioned_concurrency_config(self, function_name: str, qualifier: str,
                                            provisioned_concurrent_executions: int) -> Dict[str, Any]:
        """
        Configure provisioned concurrency for a function alias or version.
        
        Args:
            function_name: Name of the function
            qualifier: Version or alias qualifier
            provisioned_concurrent_executions: Provisioned concurrent executions
            
        Returns:
            Dict with configuration details
        """
        kwargs = {
            "FunctionName": function_name,
            "Qualifier": qualifier,
            "ProvisionedConcurrentExecutions": provisioned_concurrent_executions
        }
        
        try:
            response = self.lambda_client.put_provisioned_concurrency_config(**kwargs)
            return {
                "function_name": response.get("FunctionName"),
                "qualifier": response.get("Qualifier"),
                "provisioned_concurrent_executions": response.get("ProvisionedConcurrentExecutions"),
                "status": response.get("Status"),
                "last_modified": response.get("LastModified")
            }
        except ClientError as e:
            logger.error(f"Failed to put provisioned concurrency for {function_name}: {e}")
            raise
    
    def get_provisioned_concurrency_config(self, function_name: str, 
                                            qualifier: str) -> Optional[Dict[str, Any]]:
        """
        Get provisioned concurrency configuration.
        
        Args:
            function_name: Name of the function
            qualifier: Version or alias qualifier
            
        Returns:
            Dict with configuration details or None if not configured
        """
        try:
            response = self.lambda_client.get_provisioned_concurrency_config(
                FunctionName=function_name,
                Qualifier=qualifier
            )
            return {
                "provisioned_concurrent_executions": response.get("ProvisionedConcurrentExecutions"),
                "status": response.get("Status"),
                "last_modified": response.get("LastModified")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "ProvisionedConcurrencyConfigNotFoundException":
                return None
            raise
    
    def delete_provisioned_concurrency_config(self, function_name: str,
                                               qualifier: str) -> bool:
        """
        Delete provisioned concurrency configuration.
        
        Args:
            function_name: Name of the function
            qualifier: Version or alias qualifier
            
        Returns:
            True if deleted successfully
        """
        try:
            self.lambda_client.delete_provisioned_concurrency_config(
                FunctionName=function_name,
                Qualifier=qualifier
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete provisioned concurrency for {function_name}: {e}")
            raise
    
    def list_provisioned_concurrency_configs(self, function_name: str,
                                              marker: Optional[str] = None,
                                              max_items: int = 100) -> Dict[str, Any]:
        """
        List provisioned concurrency configurations for a function.
        
        Args:
            function_name: Name of the function
            marker: Pagination token
            max_items: Maximum number of configs to return
            
        Returns:
            Dict with 'configs' list and 'next_marker'
        """
        kwargs = {"FunctionName": function_name, "MaxItems": max_items}
        if marker:
            kwargs["Marker"] = marker
        
        try:
            response = self.lambda_client.list_provisioned_concurrency_configs(**kwargs)
            configs = []
            for item in response.get("ProvisionedConcurrencyConfigs", []):
                configs.append({
                    "function_name": item.get("FunctionName"),
                    "qualifier": item.get("Qualifier"),
                    "provisioned_concurrent_executions": item.get("ProvisionedConcurrentExecutions"),
                    "status": item.get("Status"),
                    "last_modified": item.get("LastModified")
                })
            return {
                "configs": configs,
                "next_marker": response.get("NextMarker")
            }
        except ClientError as e:
            logger.error(f"Failed to list provisioned concurrency configs: {e}")
            raise
    
    # =========================================================================
    # Layer Sharing
    # =========================================================================
    
    def get_layer_version_policy(self, layer_name: str, version: int) -> Optional[Dict[str, Any]]:
        """
        Get the resource-based policy for a layer version.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            
        Returns:
            Dict with policy details or None
        """
        try:
            response = self.lambda_client.get_layer_version_policy(
                LayerName=layer_name,
                VersionNumber=version
            )
            return {
                "policy": response.get("Policy"),
                "revision_id": response.get("RevisionId")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def add_layer_version_permission(self, layer_name: str, version: int,
                                     statement_id: str, action: str,
                                     principal: str, source_arn: Optional[str] = None,
                                     source_account: Optional[str] = None,
                                     organization_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Add permission to a layer version for sharing across accounts.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            statement_id: Unique statement identifier
            action: Action to allow (e.g., "lambda:GetLayerVersion")
            principal: AWS principal to allow (e.g., "*" or account ID)
            source_arn: Optional source ARN for condition
            source_account: Optional source account for condition
            organization_id: Optional organization ID for condition
            
        Returns:
            Dict with policy details
        """
        kwargs = {
            "LayerName": layer_name,
            "VersionNumber": version,
            "StatementId": statement_id,
            "Action": action,
            "Principal": principal
        }
        
        if source_arn:
            kwargs["SourceArn"] = source_arn
        if source_account:
            kwargs["SourceAccount"] = source_account
        if organization_id:
            kwargs["OrganizationId"] = organization_id
        
        try:
            response = self.lambda_client.add_layer_version_permission(**kwargs)
            return {
                "statement": response.get("Statement"),
                "revision_id": response.get("RevisionId")
            }
        except ClientError as e:
            logger.error(f"Failed to add layer version permission: {e}")
            raise
    
    def remove_layer_version_permission(self, layer_name: str, version: int,
                                        statement_id: str,
                                        revision_id: Optional[str] = None) -> bool:
        """
        Remove permission from a layer version.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            statement_id: Statement identifier to remove
            revision_id: Expected revision ID for conditional delete
            
        Returns:
            True if removed successfully
        """
        kwargs = {
            "LayerName": layer_name,
            "VersionNumber": version,
            "StatementId": statement_id
        }
        if revision_id:
            kwargs["RevisionId"] = revision_id
        
        try:
            self.lambda_client.remove_layer_version_permission(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to remove layer version permission: {e}")
            raise
    
    def grant_layer_version_access(self, layer_name: str, version: int,
                                   account_id: str) -> bool:
        """
        Grant another account access to use a layer version.
        
        Args:
            layer_name: Name of the layer
            version: Layer version number
            account_id: AWS account ID to grant access
            
        Returns:
            True if granted successfully
        """
        statement_id = f"share-{account_id}-{version}"
        try:
            self.add_layer_version_permission(
                layer_name=layer_name,
                version=version,
                statement_id=statement_id,
                action="lambda:GetLayerVersion",
                principal=account_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to grant layer version access: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def get_function_metrics(self, function_name: str, start_time: datetime,
                             end_time: datetime, period: int = 60,
                             statistics: List[str] = None) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a Lambda function.
        
        Args:
            function_name: Name of the function
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            statistics: List of statistics (Sum, Average, Maximum, Minimum)
            
        Returns:
            Dict with metric data points
        """
        if statistics is None:
            statistics = ["Sum", "Average", "Maximum"]
        
        namespace = "AWS/Lambda"
        metric_names = [
            "Invocations", "Errors", "Throttles", "Duration",
            "ConcurrentExecutions", "ProvisionedConcurrencyInvocations",
            "ProvisionedConcurrencySpilloverInvocations", "ProvisionedConcurrencyUtilization"
        ]
        
        results = {}
        
        try:
            for metric_name in metric_names:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=statistics,
                    Dimensions=[{"Name": "FunctionName", "Value": function_name}]
                )
                results[metric_name] = {
                    "label": response.get("Label"),
                    "datapoints": sorted(
                        response.get("Datapoints", []),
                        key=lambda x: x.get("Timestamp", datetime.min)
                    )
                }
            return results
        except ClientError as e:
            logger.error(f"Failed to get function metrics for {function_name}: {e}")
            raise
    
    def get_invocation_metrics(self, function_name: str, start_time: datetime,
                                end_time: datetime, period: int = 60) -> Dict[str, Any]:
        """
        Get invocation-specific CloudWatch metrics.
        
        Args:
            function_name: Name of the function
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            
        Returns:
            Dict with invocation metrics
        """
        return self.get_function_metrics(
            function_name=function_name,
            start_time=start_time,
            end_time=end_time,
            period=period,
            statistics=["Sum", "Average", "Maximum", "Minimum"]
        )
    
    def get_duration_metrics(self, function_name: str, start_time: datetime,
                             end_time: datetime, period: int = 60) -> Dict[str, Any]:
        """
        Get duration-specific CloudWatch metrics.
        
        Args:
            function_name: Name of the function
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            
        Returns:
            Dict with duration metrics
        """
        return self.get_function_metrics(
            function_name=function_name,
            start_time=start_time,
            end_time=end_time,
            period=period,
            statistics=["Average", "Maximum", "Minimum", "Sum"]
        )
    
    def get_error_metrics(self, function_name: str, start_time: datetime,
                          end_time: datetime, period: int = 60) -> Dict[str, Any]:
        """
        Get error-specific CloudWatch metrics.
        
        Args:
            function_name: Name of the function
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            
        Returns:
            Dict with error metrics
        """
        return self.get_function_metrics(
            function_name=function_name,
            start_time=start_time,
            end_time=end_time,
            period=period,
            statistics=["Sum", "Average"]
        )
    
    def get_logs(self, function_name: str, start_time: Optional[datetime] = None,
                 end_time: Optional[datetime] = None,
                 filter_pattern: Optional[str] = None,
                 limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get CloudWatch logs for a Lambda function.
        
        Args:
            function_name: Name of the function
            start_time: Start time for logs (defaults to 1 hour ago)
            end_time: End time for logs (defaults to now)
            filter_pattern: CloudWatch filter pattern
            limit: Maximum number of log events to return
            
        Returns:
            List of log events
        """
        log_group_name = f"/aws/lambda/{function_name}"
        
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.now()
        
        kwargs = {
            "logGroupName": log_group_name,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": limit
        }
        
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern
        
        try:
            response = self.logs_client.filter_log_events(**kwargs)
            events = []
            for event in response.get("logEvents", []):
                events.append({
                    "id": event.get("id"),
                    "timestamp": event.get("timestamp"),
                    "message": event.get("message"),
                    "ingestion_time": event.get("ingestionTime")
                })
            return events
        except ClientError as e:
            logger.error(f"Failed to get logs for {function_name}: {e}")
            raise
    
    def get_function_insights(self, function_name: str, start_time: datetime,
                              end_time: datetime) -> Dict[str, Any]:
        """
        Get CloudWatch Lambda Insights metrics.
        
        Args:
            function_name: Name of the function
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dict with Lambda Insights data
        """
        namespace = "AWS/Lambda"
        metrics = [
            "Duration", "Errors", "Invocations", "Throttles",
            "ConcurrentExecutions"
        ]
        
        insights = {}
        
        try:
            for metric in metrics:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,
                    Statistics=["Average", "Maximum", "Minimum"]
                )
                insights[metric] = response.get("Datapoints", [])
            return insights
        except ClientError as e:
            logger.error(f"Failed to get function insights: {e}")
            raise
    
    def create_log_subscription(self, function_name: str, log_group_name: str,
                                 filter_name: str, filter_pattern: str) -> bool:
        """
        Create a log subscription filter for a Lambda function.
        
        Args:
            function_name: Name of the function
            log_group_name: CloudWatch log group name
            filter_name: Name of the filter
            filter_pattern: CloudWatch filter pattern
            
        Returns:
            True if created successfully
        """
        try:
            self.logs_client.put_subscription_filter(
                logGroupName=log_group_name,
                filterName=filter_name,
                filterPattern=filter_pattern,
                destinationArn=f"arn:aws:lambda:{self.region_name}:*:function:{function_name}"
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to create log subscription: {e}")
            raise
    
    # =========================================================================
    # SAM Template Support
    # =========================================================================
    
    def create_sam_template(self, template: Optional[SAMTemplate] = None) -> SAMTemplate:
        """
        Create a new SAM template structure.
        
        Args:
            template: Optional SAM template with initial configuration
            
        Returns:
            SAMTemplate object
        """
        if template is None:
            return SAMTemplate()
        return template
    
    def add_sam_function(self, template: SAMTemplate, resource_name: str,
                         function_config: Dict[str, Any]) -> SAMTemplate:
        """
        Add a Lambda function to a SAM template.
        
        Args:
            template: SAM template to modify
            resource_name: Logical ID for the function resource
            function_config: Function configuration (runtime, handler, code_uri, etc.)
            
        Returns:
            Updated SAMTemplate object
        """
        function_resource = {
            "Type": "AWS::Serverless::Function",
            "Properties": {
                "FunctionName": function_config.get("function_name"),
                "Runtime": function_config.get("runtime"),
                "Handler": function_config.get("handler"),
                "CodeUri": function_config.get("code_uri", "."),
            }
        }
        
        optional_props = [
            "Description", "Timeout", "MemorySize", "Role", "Policies",
            "Environment", "VpcConfig", "Events", "Layers", "DeadLetterQueue",
            "Tracing", "KmsKeyArn", "Tags", "AutoPublishAlias", "ImageUri",
            "PackageType", "Architectures"
        ]
        
        for prop in optional_props:
            if prop in function_config:
                function_resource["Properties"][prop] = function_config[prop]
        
        template.Resources[resource_name] = function_resource
        return template
    
    def add_sam_api(self, template: SAMTemplate, resource_name: str,
                    api_config: Dict[str, Any]) -> SAMTemplate:
        """
        Add an API Gateway to a SAM template.
        
        Args:
            template: SAM template to modify
            resource_name: Logical ID for the API resource
            api_config: API configuration
            
        Returns:
            Updated SAMTemplate object
        """
        api_resource = {
            "Type": "AWS::Serverless::Api",
            "Properties": {
                "StageName": api_config.get("stage_name", "Prod"),
            }
        }
        
        if "definition_uri" in api_config:
            api_resource["Properties"]["DefinitionUri"] = api_config["definition_uri"]
        if "binary_media_types" in api_config:
            api_resource["Properties"]["BinaryMediaTypes"] = api_config["binary_media_types"]
        if "minimum_compression_size" in api_config:
            api_resource["Properties"]["MinimumCompressionSize"] = api_config["minimum_compression_size"]
        
        template.Resources[resource_name] = api_resource
        return template
    
    def add_sam_layer(self, template: SAMTemplate, resource_name: str,
                      layer_config: Dict[str, Any]) -> SAMTemplate:
        """
        Add a Lambda layer to a SAM template.
        
        Args:
            template: SAM template to modify
            resource_name: Logical ID for the layer resource
            layer_config: Layer configuration
            
        Returns:
            Updated SAMTemplate object
        """
        layer_resource = {
            "Type": "AWS::Serverless::LayerVersion",
            "Properties": {
                "LayerName": layer_config.get("layer_name"),
                "ContentUri": layer_config.get("content_uri", "."),
                "CompatibleRuntimes": layer_config.get("compatible_runtimes", []),
            }
        }
        
        if "description" in layer_config:
            layer_resource["Properties"]["Description"] = layer_config["description"]
        if "license_info" in layer_config:
            layer_resource["Properties"]["LicenseInfo"] = layer_config["license_info"]
        
        template.Resources[resource_name] = layer_resource
        return template
    
    def add_sam_output(self, template: SAMTemplate, output_name: str,
                        output_config: Dict[str, Any]) -> SAMTemplate:
        """
        Add an output to a SAM template.
        
        Args:
            template: SAM template to modify
            output_name: Logical ID for the output
            output_config: Output configuration (Description, Value, Export)
            
        Returns:
            Updated SAMTemplate object
        """
        output = {
            "Description": output_config.get("description", ""),
            "Value": output_config.get("value"),
        }
        
        if "export" in output_config:
            output["Export"] = {"Name": output_config["export"]}
        
        template.Outputs[output_name] = output
        return template
    
    def add_sam_global(self, template: SAMTemplate, 
                       global_config: Dict[str, Any]) -> SAMTemplate:
        """
        Add global configuration to a SAM template.
        
        Args:
            template: SAM template to modify
            global_config: Global configuration (function timeout, memory, etc.)
            
        Returns:
            Updated SAMTemplate object
        """
        template.Globals.update(global_config)
        return template
    
    def render_sam_template(self, template: SAMTemplate) -> str:
        """
        Render a SAM template to YAML.
        
        Args:
            template: SAM template to render
            
        Returns:
            YAML string representation
        """
        try:
            import yaml
            
            template_dict = {
                "AWSTemplateFormatVersion": template.AWSTemplateFormatVersion,
                "Transform": template.Transform,
            }
            
            if template.Description:
                template_dict["Description"] = template.Description
            if template.Globals:
                template_dict["Globals"] = template.Globals
            if template.Metadata:
                template_dict["Metadata"] = template.Metadata
            if template.Parameters:
                template_dict["Parameters"] = template.Parameters
            if template.Conditions:
                template_dict["Conditions"] = template.Conditions
            if template.Mappings:
                template_dict["Mappings"] = template.Mappings
            if template.Resources:
                template_dict["Resources"] = template.Resources
            if template.Outputs:
                template_dict["Outputs"] = template.Outputs
            
            return yaml.dump(template_dict, default_flow_style=False, sort_keys=False)
        except ImportError:
            return json.dumps(template_dict, indent=2)
    
    def save_sam_template(self, template: SAMTemplate, file_path: str) -> bool:
        """
        Save a SAM template to a file.
        
        Args:
            template: SAM template to save
            file_path: Path to save the template file
            
        Returns:
            True if saved successfully
        """
        try:
            rendered = self.render_sam_template(template)
            with open(file_path, "w") as f:
                f.write(rendered)
            return True
        except Exception as e:
            logger.error(f"Failed to save SAM template: {e}")
            raise
    
    def load_sam_template(self, file_path: str) -> SAMTemplate:
        """
        Load a SAM template from a file.
        
        Args:
            file_path: Path to the template file
            
        Returns:
            SAMTemplate object
        """
        try:
            import yaml
            
            with open(file_path, "r") as f:
                data = yaml.safe_load(f)
            
            template = SAMTemplate()
            if data:
                template.AWSTemplateFormatVersion = data.get("AWSTemplateFormatVersion", "2010-09-09")
                template.Transform = data.get("Transform", "AWS::Serverless-2016-10-31")
                template.Description = data.get("Description", "")
                template.Globals = data.get("Globals", {})
                template.Metadata = data.get("Metadata", {})
                template.Parameters = data.get("Parameters", {})
                template.Conditions = data.get("Conditions", {})
                template.Mappings = data.get("Mappings", {})
                template.Resources = data.get("Resources", {})
                template.Outputs = data.get("Outputs", {})
            
            return template
        except Exception as e:
            logger.error(f"Failed to load SAM template: {e}")
            raise
    
    def validate_sam_template(self, file_path: str) -> Dict[str, Any]:
        """
        Validate a SAM template using CloudFormation.
        
        Args:
            file_path: Path to the template file
            
        Returns:
            Dict with validation result
        """
        try:
            cf_client = boto3.client("cloudformation", region_name=self.region_name)
            
            with open(file_path, "r") as f:
                template_body = f.read()
            
            response = cf_client.validate_template(TemplateBody=template_body)
            return {
                "valid": True,
                "description": response.get("Description", ""),
                "parameters": response.get("Parameters", []),
                "capabilities": response.get("Capabilities", []),
                "capabilities_reason": response.get("CapabilitiesReason", "")
            }
        except ClientError as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def deploy_sam_template(self, file_path: str, stack_name: str,
                            capabilities: List[str] = None,
                            parameter_overrides: Dict[str, str] = None,
                            tags: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Deploy a SAM template using CloudFormation.
        
        Args:
            file_path: Path to the SAM template file
            stack_name: Name of the CloudFormation stack
            capabilities: CloudFormation capabilities (CAPABILITY_IAM, etc.)
            parameter_overrides: Parameter overrides
            tags: Stack tags
            
        Returns:
            Dict with deployment result
        """
        if capabilities is None:
            capabilities = ["CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND"]
        
        try:
            cf_client = boto3.client("cloudformation", region_name=self.region_name)
            
            with open(file_path, "r") as f:
                template_body = f.read()
            
            kwargs = {
                "StackName": stack_name,
                "TemplateBody": template_body,
                "Capabilities": capabilities
            }
            
            if parameter_overrides:
                parameters = [
                    {"ParameterKey": k, "ParameterValue": v}
                    for k, v in parameter_overrides.items()
                ]
                kwargs["Parameters"] = parameters
            
            if tags:
                stack_tags = [{"Key": k, "Value": v} for k, v in tags.items()]
                kwargs["Tags"] = stack_tags
            
            response = cf_client.create_stack(**kwargs)
            return {
                "stack_id": response.get("StackId"),
                "status": "CREATE_IN_PROGRESS"
            }
        except ClientError as e:
            logger.error(f"Failed to deploy SAM template: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_account_settings(self) -> Dict[str, Any]:
        """
        Get Lambda account settings.
        
        Returns:
            Dict with account settings
        """
        try:
            response = self.lambda_client.get_account_settings()
            return {
                "reserved_concurrent_executions": response.get("AccountLimit", {}).get("ReservedConcurrentExecutions"),
                "limit": response.get("AccountLimit", {}).get("FunctionCount"),
                "code_size_limit": response.get("AccountLimit", {}).get("CodeSizeUnzipped"),
                "total_code_size": response.get("AccountUsage", {}).get("TotalCodeSize"),
                "function_count": response.get("AccountUsage", {}).get("FunctionCount")
            }
        except ClientError as e:
            logger.error(f"Failed to get account settings: {e}")
            raise
    
    def tag_function(self, function_name: str, tags: Dict[str, str]) -> bool:
        """
        Tag a Lambda function.
        
        Args:
            function_name: Name of the function
            tags: Tags to apply
            
        Returns:
            True if successful
        """
        try:
            self.lambda_client.tag_resource(
                Resource=f"arn:aws:lambda:{self.region_name}:*:function:{function_name}",
                Tags=tags
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to tag function {function_name}: {e}")
            raise
    
    def untag_function(self, function_name: str, tag_keys: List[str]) -> bool:
        """
        Remove tags from a Lambda function.
        
        Args:
            function_name: Name of the function
            tag_keys: Tag keys to remove
            
        Returns:
            True if successful
        """
        try:
            self.lambda_client.untag_resource(
                Resource=f"arn:aws:lambda:{self.region_name}:*:function:{function_name}",
                TagKeys=tag_keys
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to untag function {function_name}: {e}")
            raise
    
    def wait_for_function_active(self, function_name: str, timeout: int = 60,
                                  poll_interval: int = 5) -> bool:
        """
        Wait for a function to become active.
        
        Args:
            function_name: Name of the function
            timeout: Maximum time to wait in seconds
            poll_interval: Time between polls in seconds
            
        Returns:
            True if function became active, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            info = self.get_function(function_name)
            if info and info.state == "Active":
                return True
            time.sleep(poll_interval)
        return False
    
    def wait_for_event_source_mapping_active(self, uuid: str, timeout: int = 60,
                                              poll_interval: int = 5) -> bool:
        """
        Wait for an event source mapping to become active.
        
        Args:
            uuid: UUID of the event source mapping
            timeout: Maximum time to wait in seconds
            poll_interval: Time between polls in seconds
            
        Returns:
            True if mapping became active, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            info = self.get_event_source_mapping(uuid)
            if info and info.state == "Enabled":
                return True
            time.sleep(poll_interval)
        return False
    
    def create_zip_package(self, directory_path: str, output_path: str,
                           include_files: List[str] = None) -> str:
        """
        Create a ZIP package from a directory.
        
        Args:
            directory_path: Path to directory to zip
            output_path: Path to output zip file
            include_files: Optional list of files to include (relative paths)
            
        Returns:
            Path to created zip file
        """
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            if include_files:
                for file_path in include_files:
                    full_path = os.path.join(directory_path, file_path)
                    if os.path.isfile(full_path):
                        zipf.write(full_path, file_path)
            else:
                for root, dirs, files in os.walk(directory_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, directory_path)
                        zipf.write(file_path, arcname)
        
        return output_path
    
    def extract_zip_package(self, zip_path: str, output_directory: str) -> List[str]:
        """
        Extract a ZIP package to a directory.
        
        Args:
            zip_path: Path to zip file
            output_directory: Directory to extract to
            
        Returns:
            List of extracted file paths
        """
        os.makedirs(output_directory, exist_ok=True)
        extracted = []
        
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            zipf.extractall(output_directory)
            extracted = [os.path.join(output_directory, name) for name in zipf.namelist()]
        
        return extracted
    
    def get_function_url_config(self, function_name: str, 
                                 qualifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a function URL.
        
        Args:
            function_name: Name of the function
            qualifier: Function qualifier
            
        Returns:
            Dict with URL configuration or None
        """
        kwargs = {"FunctionName": function_name}
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            response = self.lambda_client.get_function_url_config(**kwargs)
            return {
                "function_url": response.get("FunctionUrl"),
                "function_arn": response.get("FunctionArn"),
                "auth_type": response.get("AuthType"),
                "cors": response.get("Cors"),
                "creation_time": response.get("CreationTime"),
                "last_modified_time": response.get("LastModifiedTime")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
    
    def create_function_url(self, function_name: str, auth_type: str = "AWS_IAM",
                             cors_config: Optional[Dict[str, Any]] = None,
                             qualifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a function URL for a Lambda function.
        
        Args:
            function_name: Name of the function
            auth_type: Auth type (AWS_IAM or NONE)
            cors_config: CORS configuration
            qualifier: Function qualifier
            
        Returns:
            Dict with created URL configuration
        """
        kwargs = {
            "FunctionName": function_name,
            "AuthType": auth_type
        }
        
        if cors_config:
            kwargs["Cors"] = cors_config
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            response = self.lambda_client.create_function_url_config(**kwargs)
            return {
                "function_url": response.get("FunctionUrl"),
                "function_arn": response.get("FunctionArn"),
                "auth_type": response.get("AuthType"),
                "cors": response.get("Cors"),
                "creation_time": response.get("CreationTime")
            }
        except ClientError as e:
            logger.error(f"Failed to create function URL: {e}")
            raise
    
    def delete_function_url(self, function_name: str, qualifier: Optional[str] = None) -> bool:
        """
        Delete a function URL.
        
        Args:
            function_name: Name of the function
            qualifier: Function qualifier
            
        Returns:
            True if deleted successfully
        """
        kwargs = {"FunctionName": function_name}
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            self.lambda_client.delete_function_url_config(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete function URL: {e}")
            raise
    
    def add_function_url_permission(self, function_name: str, statement_id: str,
                                     action: str, principal: str,
                                     qualifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Add permission for accessing a function URL.
        
        Args:
            function_name: Name of the function
            statement_id: Unique statement identifier
            action: Action to allow
            principal: Principal to allow
            qualifier: Function qualifier
            
        Returns:
            Dict with permission details
        """
        kwargs = {
            "FunctionName": function_name,
            "StatementId": statement_id,
            "Action": action,
            "Principal": principal,
            "FunctionUrlAuthType": "AWS_IAM"
        }
        if qualifier:
            kwargs["Qualifier"] = qualifier
        
        try:
            response = self.lambda_client.add_permission(**kwargs)
            return {
                "statement": response.get("Statement"),
                "revision_id": response.get("RevisionId")
            }
        except ClientError as e:
            logger.error(f"Failed to add function URL permission: {e}")
            raise
