"""
AWS SageMaker ML Integration Module for Workflow System

Implements a SageMakerMLIntegration class with:
1. Model management: Create/manage ML models
2. Endpoint configuration: Manage endpoint configs
3. Endpoints: Create/update/delete endpoints
4. Training jobs: Manage training jobs
5. Processing jobs: Manage processing jobs
6. Notebook instances: Manage notebook instances
7. Hyperparameter tuning: Hyperparameter tuning jobs
8. Model registry: Model registry management
9. Pipelines: SageMaker Pipelines
10. CloudWatch integration: Training and inference metrics

Commit: 'feat(aws-sagemaker): add Amazon SageMaker with model management, endpoint configuration, training jobs, processing jobs, notebook instances, hyperparameter tuning, model registry, pipelines, CloudWatch'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

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


class SageMakerResourceState(Enum):
    """SageMaker resource states."""
    CREATE_IN_PROGRESS = "CreateInProgress"
    CREATE_COMPLETE = "CreateComplete"
    CREATE_FAILED = "CreateFailed"
    DELETE_IN_PROGRESS = "DeleteInProgress"
    DELETE_COMPLETE = "DeleteComplete"
    DELETE_FAILED = "DeleteFailed"
    UPDATE_IN_PROGRESS = "UpdateInProgress"
    UPDATE_COMPLETE = "UpdateComplete"
    UPDATING = "Updating"
    IN_SERVICE = "InService"
    FAILED = "Failed"
    PENDING = "Pending"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    STARTING = "Starting"


class TrainingJobStatus(Enum):
    """Training job statuses."""
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    STOPPED = "Stopped"
    STOPPING = "Stopping"
    STARTING = "Starting"


class ProcessingJobStatus(Enum):
    """Processing job statuses."""
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    STOPPED = "Stopped"


class TuningJobStatus(Enum):
    """Hyperparameter tuning job statuses."""
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    STOPPED = "Stopped"
    PENDING = "Pending"


class PipelineExecutionStatus(Enum):
    """SageMaker Pipeline execution statuses."""
    EXECUTING = "Executing"
    EXECUTION_COMPLETED = "ExecutionCompleted"
    EXECUTION_FAILED = "ExecutionFailed"
    EXECUTION_STOPPED = "ExecutionStopped"
    EXECUTION_CANCELLED = "ExecutionCancelled"


class InstanceType(Enum):
    """Common SageMaker instance types."""
    ML_M4_XLARGE = "ml.m4.xlarge"
    ML_M4_2XLARGE = "ml.m4.2xlarge"
    ML_M4_4XLARGE = "ml.m4.4xlarge"
    ML_M5_LARGE = "ml.m5.large"
    ML_M5_XLARGE = "ml.m5.xlarge"
    ML_M5_2XLARGE = "ml.m5.2xlarge"
    ML_C4_XLARGE = "ml.c4.xlarge"
    ML_C4_2XLARGE = "ml.c4.2xlarge"
    ML_C5_XLARGE = "ml.c5.xlarge"
    ML_C5_2XLARGE = "ml.c5.2xlarge"
    ML_P2_XLARGE = "ml.p2.xlarge"
    ML_P2_8XLARGE = "ml.p2.8xlarge"
    ML_P2_16XLARGE = "ml.p2.16xlarge"
    ML_P3_2XLARGE = "ml.p3.2xlarge"
    ML_P3_8XLARGE = "ml.p3.8xlarge"
    ML_P3_16XLARGE = "ml.p3.16xlarge"
    ML_G4DN_XLARGE = "ml.g4dn.xlarge"
    ML_G4DN_2XLARGE = "ml.g4dn.2xlarge"
    ML_G4DN_4XLARGE = "ml.g4dn.4xlarge"
    ML_G4DN_8XLARGE = "ml.g4dn.8xlarge"
    ML_G4DN_12XLARGE = "ml.g4dn.12xlarge"
    ML_G4DN_16XLARGE = "ml.g4dn.16xlarge"


@dataclass
class SageMakerConfig:
    """Configuration for SageMaker connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class ModelConfig:
    """Configuration for creating an ML model."""
    model_name: str
    primary_container_image: Optional[str] = None
    model_url: Optional[str] = None
    entry_point: Optional[str] = None
    source_dir: Optional[str] = None
    execution_role_arn: str = ""
    enable_network_isolation: bool = False
    model_vpc_config: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    storage_entry_point: Optional[str] = None
    code_entry_point: Optional[str] = None
    inference_code: Optional[str] = None
    inference_entry_point: Optional[str] = None


@dataclass
class EndpointConfigConfig:
    """Configuration for creating an endpoint configuration."""
    endpoint_config_name: str
    production_variants: List[Dict[str, Any]] = field(default_factory=list)
    data_capture_config: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    kms_key_arn: Optional[str] = None


@dataclass
class EndpointConfig:
    """Configuration for creating/updating an endpoint."""
    endpoint_name: str
    endpoint_config_name: str
    tags: Dict[str, str] = field(default_factory=dict)
    retain_all_variant_properties: bool = False
    exclude_retained_variant_properties: Optional[List[Dict[str, Any]]] = None


@dataclass
class TrainingJobConfig:
    """Configuration for creating a training job."""
    training_job_name: str
    algorithm_specification: Dict[str, Any]
    output_data_config: Dict[str, Any]
    resource_config: Dict[str, Any]
    input_data_config: Optional[List[Dict[str, Any]]] = None
    hyper_parameters: Optional[Dict[str, Any]] = None
    stopping_condition: Optional[Dict[str, Any]] = None
    enable_network_isolation: bool = False
    enable_encrypted_container_logs: bool = False
    metric_definitions: Optional[List[Dict[str, Any]]] = None
    checkpoint_config: Optional[Dict[str, Any]] = None
    tensor_board_output_config: Optional[Dict[str, Any]] = None
    debugger_hook_config: Optional[Dict[str, Any]] = None
    debugger_rule_configurations: Optional[List[Dict[str, Any]]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    warm_pool_status: Optional[Dict[str, Any]] = None


@dataclass
class ProcessingJobConfig:
    """Configuration for creating a processing job."""
    processing_job_name: str
    processing_inputs: Optional[List[Dict[str, Any]]] = None
    processing_output_config: Optional[Dict[str, Any]] = None
    processing_resources: Dict[str, Any] = field(default_factory=dict)
    processing_image_uri: Optional[str] = None
    processing_app_specification: Dict[str, Any] = field(default_factory=dict)
    network_config: Optional[Dict[str, Any]] = None
    stopping_condition: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class NotebookInstanceConfig:
    """Configuration for creating a notebook instance."""
    notebook_instance_name: str
    instance_type: str = "ml.t3.medium"
    role_arn: str = ""
    subnet_id: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    kms_key_id: Optional[str] = None
    lifecycle_config_name: Optional[str] = None
    default_code_repository: Optional[str] = None
    additional_code_repositories: List[str] = field(default_factory=list)
    root_access: str = "Enabled"
    platform_identifier: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class HyperparameterTuningJobConfig:
    """Configuration for creating a hyperparameter tuning job."""
    tuning_job_name: str
    hyperparameter_ranges: Dict[str, Any]
    training_job_definition: Dict[str, Any]
    tuning_strategy: str = "Bayesian"
    warm_start_config: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ModelRegistryConfig:
    """Configuration for model registry."""
    model_package_group_name: str
    model_package_description: Optional[str] = None
    model_approval_status: Optional[str] = None
    metadata_properties: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Configuration for SageMaker Pipelines."""
    pipeline_name: str
    pipeline_definition: Optional[Dict[str, Any]] = None
    pipeline_definition_s3_location: Optional[Dict[str, Any]] = None
    pipeline_description: Optional[str] = None
    role_arn: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class CloudWatchMetricsConfig:
    """Configuration for CloudWatch metrics integration."""
    namespace: str = "SageMaker"
    training_metrics: List[Dict[str, str]] = field(default_factory=list)
    validation_metrics: List[Dict[str, str]] = field(default_factory=list)
    custom_metrics: List[Dict[str, Any]] = field(default_factory=list)


class SageMakerMLIntegration:
    """
    AWS SageMaker ML Integration.
    
    Provides comprehensive Amazon SageMaker management including:
    - ML model creation and management
    - Endpoint configuration management
    - Real-time inference endpoints
    - Training job management
    - Processing job management
    - Notebook instance lifecycle management
    - Hyperparameter tuning jobs
    - Model registry and model versioning
    - SageMaker Pipelines workflow management
    - CloudWatch metrics integration
    """
    
    def __init__(self, config: Optional[SageMakerConfig] = None):
        """Initialize the SageMaker integration.
        
        Args:
            config: SageMaker configuration containing AWS credentials and region.
        """
        self.config = config or SageMakerConfig()
        self._client = None
        self._resource_groups_client = None
        self._cloudwatch_client = None
        self._lock = threading.Lock()
        self._resource_cache: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._operation_counters: Dict[str, int] = defaultdict(int)
        
    @property
    def client(self):
        """Get or create the SageMaker boto3 client (lazy initialization)."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    if not BOTO3_AVAILABLE:
                        raise ImportError("boto3 is required for SageMaker integration. Install with: pip install boto3")
                    
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    else:
                        if self.config.aws_access_key_id:
                            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                        if self.config.aws_session_token:
                            kwargs["aws_session_token"] = self.config.aws_session_token
                    
                    self._client = boto3.client("sagemaker", **kwargs)
        return self._client
    
    @property
    def cloudwatch_client(self):
        """Get or create the CloudWatch boto3 client."""
        if self._cloudwatch_client is None:
            with self._lock:
                if self._cloudwatch_client is None:
                    if not BOTO3_AVAILABLE:
                        raise ImportError("boto3 is required for CloudWatch integration.")
                    
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    else:
                        if self.config.aws_access_key_id:
                            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    
                    self._cloudwatch_client = boto3.client("cloudwatch", **kwargs)
        return self._cloudwatch_client

    # =========================================================================
    # Model Management
    # =========================================================================
    
    def create_model(self, config: ModelConfig) -> Dict[str, Any]:
        """Create an ML model in SageMaker.
        
        Args:
            config: Model configuration including name, container image, and role.
            
        Returns:
            Dictionary containing the created model information.
        """
        try:
            primary_container = {}
            if config.primary_container_image:
                primary_container["Image"] = config.primary_container_image
            if config.model_url:
                primary_container["ModelDataUrl"] = config.model_url
            if config.inference_entry_point:
                primary_container["InferenceSpecification"] = {
                    "EntryPoint": config.inference_entry_point,
                    "SourceDir": config.source_dir
                }
            
            create_model_kwargs = {
                "ModelName": config.model_name,
                "ExecutionRoleArn": config.execution_role_arn,
                "PrimaryContainer": primary_container if primary_container else None,
                "EnableNetworkIsolation": config.enable_network_isolation,
            }
            
            if config.model_vpc_config:
                create_model_kwargs["VpcConfig"] = config.model_vpc_config
            
            if config.tags:
                create_model_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            if config.entry_point and config.source_dir:
                create_model_kwargs["ContainerConfig"] = {
                    "EntryPoint": config.entry_point,
                    "SourceDir": config.source_dir
                }
            
            response = self.client.create_model(**{k: v for k, v in create_model_kwargs.items() if v is not None})
            
            self._resource_cache["models"][config.model_name] = {
                "name": config.model_name,
                "status": SageMakerResourceState.CREATE_COMPLETE.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created model: {config.model_name}")
            return {"status": "success", "model": config.model_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create model {config.model_name}: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_model(self, model_name: str) -> Dict[str, Any]:
        """Get detailed information about a model.
        
        Args:
            model_name: Name of the model to describe.
            
        Returns:
            Dictionary containing model details.
        """
        try:
            response = self.client.describe_model(ModelName=model_name)
            return {"status": "success", "model": response}
        except ClientError as e:
            logger.error(f"Failed to describe model {model_name}: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_models(self, prefix: Optional[str] = None, sort_by: str = "Name", 
                    sort_order: str = "Ascending") -> Dict[str, Any]:
        """List all models in SageMaker.
        
        Args:
            prefix: Filter models by name prefix.
            sort_by: Field to sort by (Name, CreationTime).
            sort_order: Sort order (Ascending, Descending).
            
        Returns:
            Dictionary containing list of models.
        """
        try:
            kwargs = {"SortBy": sort_by, "SortOrder": sort_order}
            if prefix:
                kwargs["NameContains"] = prefix
            
            response = self.client.list_models(**kwargs)
            models = response.get("Models", [])
            
            return {
                "status": "success",
                "models": models,
                "count": len(models)
            }
        except ClientError as e:
            logger.error(f"Failed to list models: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_model(self, model_name: str) -> Dict[str, Any]:
        """Delete an ML model from SageMaker.
        
        Args:
            model_name: Name of the model to delete.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.delete_model(ModelName=model_name)
            
            if model_name in self._resource_cache["models"]:
                del self._resource_cache["models"][model_name]
            
            logger.info(f"Deleted model: {model_name}")
            return {"status": "success", "model": model_name}
        except ClientError as e:
            logger.error(f"Failed to delete model {model_name}: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # Endpoint Configuration Management
    # =========================================================================
    
    def create_endpoint_config(self, config: EndpointConfigConfig) -> Dict[str, Any]:
        """Create an endpoint configuration.
        
        Args:
            config: Endpoint configuration including name and production variants.
            
        Returns:
            Dictionary containing the created endpoint configuration.
        """
        try:
            create_kwargs = {
                "EndpointConfigName": config.endpoint_config_name,
                "ProductionVariants": config.production_variants,
            }
            
            if config.data_capture_config:
                create_kwargs["DataCaptureConfig"] = config.data_capture_config
            
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            if config.kms_key_arn:
                create_kwargs["KmsKey"] = config.kms_key_arn
            
            response = self.client.create_endpoint_config(**create_kwargs)
            
            self._resource_cache["endpoint_configs"][config.endpoint_config_name] = {
                "name": config.endpoint_config_name,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created endpoint config: {config.endpoint_config_name}")
            return {"status": "success", "endpoint_config": config.endpoint_config_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create endpoint config: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_endpoint_config(self, endpoint_config_name: str) -> Dict[str, Any]:
        """Get details of an endpoint configuration.
        
        Args:
            endpoint_config_name: Name of the endpoint configuration.
            
        Returns:
            Dictionary containing endpoint configuration details.
        """
        try:
            response = self.client.describe_endpoint_config(EndpointConfigName=endpoint_config_name)
            return {"status": "success", "endpoint_config": response}
        except ClientError as e:
            logger.error(f"Failed to describe endpoint config: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_endpoint_configs(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """List all endpoint configurations.
        
        Args:
            prefix: Filter by name prefix.
            
        Returns:
            Dictionary containing list of endpoint configurations.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            
            response = self.client.list_endpoint_configs(**kwargs)
            configs = response.get("EndpointConfigs", [])
            
            return {
                "status": "success",
                "endpoint_configs": configs,
                "count": len(configs)
            }
        except ClientError as e:
            logger.error(f"Failed to list endpoint configs: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_endpoint_config(self, endpoint_config_name: str) -> Dict[str, Any]:
        """Delete an endpoint configuration.
        
        Args:
            endpoint_config_name: Name of the endpoint configuration to delete.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
            
            if endpoint_config_name in self._resource_cache["endpoint_configs"]:
                del self._resource_cache["endpoint_configs"][endpoint_config_name]
            
            logger.info(f"Deleted endpoint config: {endpoint_config_name}")
            return {"status": "success", "endpoint_config": endpoint_config_name}
        except ClientError as e:
            logger.error(f"Failed to delete endpoint config: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # Endpoint Management
    # =========================================================================
    
    def create_endpoint(self, config: EndpointConfig) -> Dict[str, Any]:
        """Create a SageMaker endpoint.
        
        Args:
            config: Endpoint configuration including name and endpoint config.
            
        Returns:
            Dictionary containing the created endpoint information.
        """
        try:
            create_kwargs = {
                "EndpointName": config.endpoint_name,
                "EndpointConfigName": config.endpoint_config_name,
            }
            
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_endpoint(**create_kwargs)
            
            self._resource_cache["endpoints"][config.endpoint_name] = {
                "name": config.endpoint_name,
                "config_name": config.endpoint_config_name,
                "status": SageMakerResourceState.CREATE_IN_PROGRESS.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created endpoint: {config.endpoint_name}")
            return {"status": "success", "endpoint": config.endpoint_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create endpoint: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_endpoint(self, endpoint_name: str) -> Dict[str, Any]:
        """Get details of a SageMaker endpoint.
        
        Args:
            endpoint_name: Name of the endpoint.
            
        Returns:
            Dictionary containing endpoint details.
        """
        try:
            response = self.client.describe_endpoint(EndpointName=endpoint_name)
            return {"status": "success", "endpoint": response}
        except ClientError as e:
            logger.error(f"Failed to describe endpoint: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_endpoints(self, prefix: Optional[str] = None, 
                       status_equals: Optional[str] = None) -> Dict[str, Any]:
        """List all SageMaker endpoints.
        
        Args:
            prefix: Filter by name prefix.
            status_equals: Filter by status.
            
        Returns:
            Dictionary containing list of endpoints.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            if status_equals:
                kwargs["StatusEquals"] = status_equals
            
            response = self.client.list_endpoints(**kwargs)
            endpoints = response.get("Endpoints", [])
            
            return {
                "status": "success",
                "endpoints": endpoints,
                "count": len(endpoints)
            }
        except ClientError as e:
            logger.error(f"Failed to list endpoints: {e}")
            return {"status": "error", "error": str(e)}
    
    def update_endpoint(self, endpoint_name: str, endpoint_config_name: str) -> Dict[str, Any]:
        """Update a SageMaker endpoint with a new configuration.
        
        Args:
            endpoint_name: Name of the endpoint to update.
            endpoint_config_name: New endpoint configuration name.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            response = self.client.update_endpoint(
                EndpointName=endpoint_name,
                EndpointConfigName=endpoint_config_name
            )
            
            self._resource_cache["endpoints"][endpoint_name]["status"] = SageMakerResourceState.UPDATE_IN_PROGRESS.value
            self._resource_cache["endpoints"][endpoint_name]["config_name"] = endpoint_config_name
            
            logger.info(f"Updated endpoint: {endpoint_name}")
            return {"status": "success", "endpoint": endpoint_name, "response": response}
        except ClientError as e:
            logger.error(f"Failed to update endpoint: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_endpoint(self, endpoint_name: str) -> Dict[str, Any]:
        """Delete a SageMaker endpoint.
        
        Args:
            endpoint_name: Name of the endpoint to delete.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.delete_endpoint(EndpointName=endpoint_name)
            
            if endpoint_name in self._resource_cache["endpoints"]:
                del self._resource_cache["endpoints"][endpoint_name]
            
            logger.info(f"Deleted endpoint: {endpoint_name}")
            return {"status": "success", "endpoint": endpoint_name}
        except ClientError as e:
            logger.error(f"Failed to delete endpoint: {e}")
            return {"status": "error", "error": str(e)}
    
    def wait_for_endpoint_in_service(self, endpoint_name: str, timeout: int = 600) -> Dict[str, Any]:
        """Wait for an endpoint to be in service.
        
        Args:
            endpoint_name: Name of the endpoint.
            timeout: Maximum wait time in seconds.
            
        Returns:
            Dictionary containing endpoint details or timeout status.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.describe_endpoint(endpoint_name)
            if status["status"] == "error":
                return status
            
            endpoint_info = status["endpoint"]
            current_status = endpoint_info.get("EndpointStatus")
            
            if current_status == "InService":
                return {"status": "success", "endpoint": endpoint_info}
            elif current_status in ["Failed", "Deleted"]:
                return {"status": "error", "error": f"Endpoint in {current_status} state"}
            
            time.sleep(30)
        
        return {"status": "error", "error": "Timeout waiting for endpoint to be in service"}

    # =========================================================================
    # Training Jobs
    # =========================================================================
    
    def create_training_job(self, config: TrainingJobConfig) -> Dict[str, Any]:
        """Create a SageMaker training job.
        
        Args:
            config: Training job configuration.
            
        Returns:
            Dictionary containing the created training job information.
        """
        try:
            create_kwargs = {
                "TrainingJobName": config.training_job_name,
                "AlgorithmSpecification": config.algorithm_specification,
                "OutputDataConfig": config.output_data_config,
                "ResourceConfig": config.resource_config,
            }
            
            if config.input_data_config:
                create_kwargs["InputDataConfig"] = config.input_data_config
            if config.hyper_parameters:
                create_kwargs["HyperParameters"] = config.hyper_parameters
            if config.stopping_condition:
                create_kwargs["StoppingCondition"] = config.stopping_condition
            if config.enable_network_isolation:
                create_kwargs["EnableNetworkIsolation"] = config.enable_network_isolation
            if config.enable_encrypted_container_logs:
                create_kwargs["EnableEncryptedContainerLogs"] = config.enable_encrypted_container_logs
            if config.metric_definitions:
                create_kwargs["MetricDefinitions"] = config.metric_definitions
            if config.checkpoint_config:
                create_kwargs["CheckpointConfig"] = config.checkpoint_config
            if config.tensor_board_output_config:
                create_kwargs["TensorBoardOutputConfig"] = config.tensor_board_output_config
            if config.debugger_hook_config:
                create_kwargs["DebugHookConfig"] = config.debugger_hook_config
            if config.debugger_rule_configurations:
                create_kwargs["DebuggerRuleConfigurations"] = config.debugger_rule_configurations
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            if config.warm_pool_status:
                create_kwargs["WarmPoolStatus"] = config.warm_pool_status
            
            response = self.client.create_training_job(**create_kwargs)
            
            self._resource_cache["training_jobs"][config.training_job_name] = {
                "name": config.training_job_name,
                "status": TrainingJobStatus.IN_PROGRESS.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created training job: {config.training_job_name}")
            return {"status": "success", "training_job": config.training_job_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create training job: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_training_job(self, training_job_name: str) -> Dict[str, Any]:
        """Get details of a training job.
        
        Args:
            training_job_name: Name of the training job.
            
        Returns:
            Dictionary containing training job details.
        """
        try:
            response = self.client.describe_training_job(TrainingJobName=training_job_name)
            return {"status": "success", "training_job": response}
        except ClientError as e:
            logger.error(f"Failed to describe training job: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_training_jobs(self, prefix: Optional[str] = None,
                          status_equals: Optional[str] = None) -> Dict[str, Any]:
        """List all training jobs.
        
        Args:
            prefix: Filter by name prefix.
            status_equals: Filter by job status.
            
        Returns:
            Dictionary containing list of training jobs.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            if status_equals:
                kwargs["StatusEquals"] = status_equals
            
            response = self.client.list_training_jobs(**kwargs)
            jobs = response.get("TrainingJobSummaries", [])
            
            return {
                "status": "success",
                "training_jobs": jobs,
                "count": len(jobs)
            }
        except ClientError as e:
            logger.error(f"Failed to list training jobs: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_training_job(self, training_job_name: str) -> Dict[str, Any]:
        """Stop a running training job.
        
        Args:
            training_job_name: Name of the training job to stop.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.stop_training_job(TrainingJobName=training_job_name)
            
            if training_job_name in self._resource_cache["training_jobs"]:
                self._resource_cache["training_jobs"][training_job_name]["status"] = TrainingJobStatus.STOPPED.value
            
            logger.info(f"Stopped training job: {training_job_name}")
            return {"status": "success", "training_job": training_job_name}
        except ClientError as e:
            logger.error(f"Failed to stop training job: {e}")
            return {"status": "error", "error": str(e)}
    
    def wait_for_training_job_complete(self, training_job_name: str, timeout: int = 3600) -> Dict[str, Any]:
        """Wait for a training job to complete.
        
        Args:
            training_job_name: Name of the training job.
            timeout: Maximum wait time in seconds.
            
        Returns:
            Dictionary containing training job final status.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.describe_training_job(training_job_name)
            if status["status"] == "error":
                return status
            
            job_info = status["training_job"]
            current_status = job_info.get("TrainingJobStatus")
            
            if current_status in ["Completed", "Stopped"]:
                return {"status": "success", "training_job": job_info}
            elif current_status == "Failed":
                failure_reason = job_info.get("FailureReason", "Unknown")
                return {"status": "error", "error": f"Training failed: {failure_reason}"}
            
            time.sleep(30)
        
        return {"status": "error", "error": "Timeout waiting for training job to complete"}

    # =========================================================================
    # Processing Jobs
    # =========================================================================
    
    def create_processing_job(self, config: ProcessingJobConfig) -> Dict[str, Any]:
        """Create a SageMaker processing job.
        
        Args:
            config: Processing job configuration.
            
        Returns:
            Dictionary containing the created processing job information.
        """
        try:
            create_kwargs = {
                "ProcessingJobName": config.processing_job_name,
                "ProcessingResources": config.processing_resources,
                "ProcessingAppSpecification": config.processing_app_specification,
            }
            
            if config.processing_inputs:
                create_kwargs["ProcessingInputs"] = config.processing_inputs
            if config.processing_output_config:
                create_kwargs["ProcessingOutputConfig"] = config.processing_output_config
            if config.processing_image_uri:
                create_kwargs["ProcessingImageUri"] = config.processing_image_uri
            if config.network_config:
                create_kwargs["NetworkConfig"] = config.network_config
            if config.stopping_condition:
                create_kwargs["StoppingCondition"] = config.stopping_condition
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_processing_job(**create_kwargs)
            
            self._resource_cache["processing_jobs"][config.processing_job_name] = {
                "name": config.processing_job_name,
                "status": ProcessingJobStatus.IN_PROGRESS.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created processing job: {config.processing_job_name}")
            return {"status": "success", "processing_job": config.processing_job_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create processing job: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_processing_job(self, processing_job_name: str) -> Dict[str, Any]:
        """Get details of a processing job.
        
        Args:
            processing_job_name: Name of the processing job.
            
        Returns:
            Dictionary containing processing job details.
        """
        try:
            response = self.client.describe_processing_job(ProcessingJobName=processing_job_name)
            return {"status": "success", "processing_job": response}
        except ClientError as e:
            logger.error(f"Failed to describe processing job: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_processing_jobs(self, prefix: Optional[str] = None,
                             status_equals: Optional[str] = None) -> Dict[str, Any]:
        """List all processing jobs.
        
        Args:
            prefix: Filter by name prefix.
            status_equals: Filter by job status.
            
        Returns:
            Dictionary containing list of processing jobs.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            if status_equals:
                kwargs["StatusEquals"] = status_equals
            
            response = self.client.list_processing_jobs(**kwargs)
            jobs = response.get("ProcessingJobSummaries", [])
            
            return {
                "status": "success",
                "processing_jobs": jobs,
                "count": len(jobs)
            }
        except ClientError as e:
            logger.error(f"Failed to list processing jobs: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_processing_job(self, processing_job_name: str) -> Dict[str, Any]:
        """Stop a running processing job.
        
        Args:
            processing_job_name: Name of the processing job to stop.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.stop_processing_job(ProcessingJobName=processing_job_name)
            
            if processing_job_name in self._resource_cache["processing_jobs"]:
                self._resource_cache["processing_jobs"][processing_job_name]["status"] = ProcessingJobStatus.STOPPED.value
            
            logger.info(f"Stopped processing job: {processing_job_name}")
            return {"status": "success", "processing_job": processing_job_name}
        except ClientError as e:
            logger.error(f"Failed to stop processing job: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # Notebook Instances
    # =========================================================================
    
    def create_notebook_instance(self, config: NotebookInstanceConfig) -> Dict[str, Any]:
        """Create a SageMaker notebook instance.
        
        Args:
            config: Notebook instance configuration.
            
        Returns:
            Dictionary containing the created notebook instance information.
        """
        try:
            create_kwargs = {
                "NotebookInstanceName": config.notebook_instance_name,
                "InstanceType": config.instance_type,
                "RoleArn": config.role_arn,
            }
            
            if config.subnet_id:
                create_kwargs["SubnetId"] = config.subnet_id
            if config.security_group_ids:
                create_kwargs["SecurityGroupIds"] = config.security_group_ids
            if config.kms_key_id:
                create_kwargs["KmsKeyId"] = config.kms_key_id
            if config.lifecycle_config_name:
                create_kwargs["LifecycleConfigName"] = config.lifecycle_config_name
            if config.default_code_repository:
                create_kwargs["DefaultCodeRepository"] = config.default_code_repository
            if config.additional_code_repositories:
                create_kwargs["AdditionalCodeRepositories"] = config.additional_code_repositories
            if config.root_access:
                create_kwargs["RootAccess"] = config.root_access
            if config.platform_identifier:
                create_kwargs["PlatformIdentifier"] = config.platform_identifier
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_notebook_instance(**create_kwargs)
            
            self._resource_cache["notebook_instances"][config.notebook_instance_name] = {
                "name": config.notebook_instance_name,
                "status": SageMakerResourceState.CREATE_IN_PROGRESS.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created notebook instance: {config.notebook_instance_name}")
            return {"status": "success", "notebook_instance": config.notebook_instance_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create notebook instance: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_notebook_instance(self, notebook_instance_name: str) -> Dict[str, Any]:
        """Get details of a notebook instance.
        
        Args:
            notebook_instance_name: Name of the notebook instance.
            
        Returns:
            Dictionary containing notebook instance details.
        """
        try:
            response = self.client.describe_notebook_instance(NotebookInstanceName=notebook_instance_name)
            return {"status": "success", "notebook_instance": response}
        except ClientError as e:
            logger.error(f"Failed to describe notebook instance: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_notebook_instances(self, prefix: Optional[str] = None,
                                status_equals: Optional[str] = None) -> Dict[str, Any]:
        """List all notebook instances.
        
        Args:
            prefix: Filter by name prefix.
            status_equals: Filter by instance status.
            
        Returns:
            Dictionary containing list of notebook instances.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            if status_equals:
                kwargs["StatusEquals"] = status_equals
            
            response = self.client.list_notebook_instances(**kwargs)
            instances = response.get("NotebookInstances", [])
            
            return {
                "status": "success",
                "notebook_instances": instances,
                "count": len(instances)
            }
        except ClientError as e:
            logger.error(f"Failed to list notebook instances: {e}")
            return {"status": "error", "error": str(e)}
    
    def start_notebook_instance(self, notebook_instance_name: str) -> Dict[str, Any]:
        """Start a stopped notebook instance.
        
        Args:
            notebook_instance_name: Name of the notebook instance.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.start_notebook_instance(NotebookInstanceName=notebook_instance_name)
            
            if notebook_instance_name in self._resource_cache["notebook_instances"]:
                self._resource_cache["notebook_instances"][notebook_instance_name]["status"] = SageMakerResourceState.STARTING.value
            
            logger.info(f"Started notebook instance: {notebook_instance_name}")
            return {"status": "success", "notebook_instance": notebook_instance_name}
        except ClientError as e:
            logger.error(f"Failed to start notebook instance: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_notebook_instance(self, notebook_instance_name: str) -> Dict[str, Any]:
        """Stop a running notebook instance.
        
        Args:
            notebook_instance_name: Name of the notebook instance.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.stop_notebook_instance(NotebookInstanceName=notebook_instance_name)
            
            if notebook_instance_name in self._resource_cache["notebook_instances"]:
                self._resource_cache["notebook_instances"][notebook_instance_name]["status"] = SageMakerResourceState.STOPPING.value
            
            logger.info(f"Stopped notebook instance: {notebook_instance_name}")
            return {"status": "success", "notebook_instance": notebook_instance_name}
        except ClientError as e:
            logger.error(f"Failed to stop notebook instance: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_notebook_instance(self, notebook_instance_name: str) -> Dict[str, Any]:
        """Delete a notebook instance.
        
        Args:
            notebook_instance_name: Name of the notebook instance to delete.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.delete_notebook_instance(NotebookInstanceName=notebook_instance_name)
            
            if notebook_instance_name in self._resource_cache["notebook_instances"]:
                del self._resource_cache["notebook_instances"][notebook_instance_name]
            
            logger.info(f"Deleted notebook instance: {notebook_instance_name}")
            return {"status": "success", "notebook_instance": notebook_instance_name}
        except ClientError as e:
            logger.error(f"Failed to delete notebook instance: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # Hyperparameter Tuning Jobs
    # =========================================================================
    
    def create_hyperparameter_tuning_job(self, config: HyperparameterTuningJobConfig) -> Dict[str, Any]:
        """Create a hyperparameter tuning job.
        
        Args:
            config: Hyperparameter tuning job configuration.
            
        Returns:
            Dictionary containing the created tuning job information.
        """
        try:
            create_kwargs = {
                "HyperParameterTuningJobName": config.tuning_job_name,
                "HyperParameterRanges": config.hyperparameter_ranges,
                "TrainingJobDefinition": config.training_job_definition,
                "HyperParameterTuningJobConfig": {
                    "Strategy": config.tuning_strategy,
                },
            }
            
            if config.warm_start_config:
                create_kwargs["WarmStartConfig"] = config.warm_start_config
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_hyper_parameter_tuning_job(**create_kwargs)
            
            self._resource_cache["tuning_jobs"][config.tuning_job_name] = {
                "name": config.tuning_job_name,
                "status": TuningJobStatus.IN_PROGRESS.value,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created hyperparameter tuning job: {config.tuning_job_name}")
            return {"status": "success", "tuning_job": config.tuning_job_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create hyperparameter tuning job: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_hyperparameter_tuning_job(self, tuning_job_name: str) -> Dict[str, Any]:
        """Get details of a hyperparameter tuning job.
        
        Args:
            tuning_job_name: Name of the tuning job.
            
        Returns:
            Dictionary containing tuning job details.
        """
        try:
            response = self.client.describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName=tuning_job_name
            )
            return {"status": "success", "tuning_job": response}
        except ClientError as e:
            logger.error(f"Failed to describe tuning job: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_hyperparameter_tuning_jobs(self, prefix: Optional[str] = None,
                                        status_equals: Optional[str] = None) -> Dict[str, Any]:
        """List all hyperparameter tuning jobs.
        
        Args:
            prefix: Filter by name prefix.
            status_equals: Filter by job status.
            
        Returns:
            Dictionary containing list of tuning jobs.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            if status_equals:
                kwargs["StatusEquals"] = status_equals
            
            response = self.client.list_hyper_parameter_tuning_jobs(**kwargs)
            jobs = response.get("HyperParameterTuningJobSummaries", [])
            
            return {
                "status": "success",
                "tuning_jobs": jobs,
                "count": len(jobs)
            }
        except ClientError as e:
            logger.error(f"Failed to list tuning jobs: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_hyperparameter_tuning_job(self, tuning_job_name: str) -> Dict[str, Any]:
        """Stop a hyperparameter tuning job.
        
        Args:
            tuning_job_name: Name of the tuning job to stop.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.stop_hyper_parameter_tuning_job(
                HyperParameterTuningJobName=tuning_job_name
            )
            
            if tuning_job_name in self._resource_cache["tuning_jobs"]:
                self._resource_cache["tuning_jobs"][tuning_job_name]["status"] = TuningJobStatus.STOPPED.value
            
            logger.info(f"Stopped hyperparameter tuning job: {tuning_job_name}")
            return {"status": "success", "tuning_job": tuning_job_name}
        except ClientError as e:
            logger.error(f"Failed to stop tuning job: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # Model Registry
    # =========================================================================
    
    def create_model_package_group(self, config: ModelRegistryConfig) -> Dict[str, Any]:
        """Create a model package group for model registry.
        
        Args:
            config: Model package group configuration.
            
        Returns:
            Dictionary containing the created model package group information.
        """
        try:
            create_kwargs = {
                "ModelPackageGroupName": config.model_package_group_name,
            }
            
            if config.model_package_description:
                create_kwargs["ModelPackageGroupDescription"] = config.model_package_description
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_model_package_group(**create_kwargs)
            
            self._resource_cache["model_groups"][config.model_package_group_name] = {
                "name": config.model_package_group_name,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created model package group: {config.model_package_group_name}")
            return {"status": "success", "model_package_group": config.model_package_group_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create model package group: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_model_package_group(self, model_package_group_name: str) -> Dict[str, Any]:
        """Get details of a model package group.
        
        Args:
            model_package_group_name: Name of the model package group.
            
        Returns:
            Dictionary containing model package group details.
        """
        try:
            response = self.client.describe_model_package_group(
                ModelPackageGroupName=model_package_group_name
            )
            return {"status": "success", "model_package_group": response}
        except ClientError as e:
            logger.error(f"Failed to describe model package group: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_model_package_groups(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """List all model package groups.
        
        Args:
            prefix: Filter by name prefix.
            
        Returns:
            Dictionary containing list of model package groups.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["NameContains"] = prefix
            
            response = self.client.list_model_package_groups(**kwargs)
            groups = response.get("ModelPackageGroupSummaryList", [])
            
            return {
                "status": "success",
                "model_package_groups": groups,
                "count": len(groups)
            }
        except ClientError as e:
            logger.error(f"Failed to list model package groups: {e}")
            return {"status": "error", "error": str(e)}
    
    def register_model_version(self, model_package_group_name: str,
                               model_image_uri: str, model_data_url: str,
                               approval_status: Optional[str] = None,
                               metadata_properties: Optional[Dict[str, Any]] = None,
                               tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Register a new model version in the model registry.
        
        Args:
            model_package_group_name: Name of the model package group.
            model_image_uri: URI of the model Docker image.
            model_data_url: S3 URL of the model artifacts.
            approval_status: Approval status (Approved, PendingApproval, Rejected).
            metadata_properties: Metadata properties for the model.
            tags: Tags for the model version.
            
        Returns:
            Dictionary containing the registered model version information.
        """
        try:
            register_kwargs = {
                "ModelPackageGroupName": model_package_group_name,
                "InferenceSpecification": {
                    "Containers": [
                        {
                            "Image": model_image_uri,
                            "ModelDataUrl": model_data_url
                        }
                    ]
                },
            }
            
            if approval_status:
                register_kwargs["ModelApprovalStatus"] = approval_status
            
            if metadata_properties:
                register_kwargs["MetadataProperties"] = metadata_properties
            
            if tags:
                register_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.client.create_model_package(**register_kwargs)
            
            logger.info(f"Registered model version in group: {model_package_group_name}")
            return {"status": "success", "model_package": response}
            
        except ClientError as e:
            logger.error(f"Failed to register model version: {e}")
            return {"status": "error", "error": str(e)}
    
    def update_model_package_approval_status(self, model_package_arn: str,
                                              approval_status: str) -> Dict[str, Any]:
        """Update the approval status of a model package.
        
        Args:
            model_package_arn: ARN of the model package.
            approval_status: New approval status.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.update_model_package(
                ModelPackageArn=model_package_arn,
                ModelApprovalStatus=approval_status
            )
            
            logger.info(f"Updated model package approval status: {model_package_arn}")
            return {"status": "success", "model_package_arn": model_package_arn}
        except ClientError as e:
            logger.error(f"Failed to update model package approval status: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # SageMaker Pipelines
    # =========================================================================
    
    def create_pipeline(self, config: PipelineConfig) -> Dict[str, Any]:
        """Create a SageMaker Pipeline.
        
        Args:
            config: Pipeline configuration.
            
        Returns:
            Dictionary containing the created pipeline information.
        """
        try:
            create_kwargs = {
                "PipelineName": config.pipeline_name,
                "RoleArn": config.role_arn,
            }
            
            if config.pipeline_definition:
                create_kwargs["PipelineDefinition"] = config.pipeline_definition
            if config.pipeline_definition_s3_location:
                create_kwargs["PipelineDefinitionS3Location"] = config.pipeline_definition_s3_location
            if config.pipeline_description:
                create_kwargs["PipelineDescription"] = config.pipeline_description
            if config.tags:
                create_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.client.create_pipeline(**create_kwargs)
            
            self._resource_cache["pipelines"][config.pipeline_name] = {
                "name": config.pipeline_name,
                "created_at": datetime.now().isoformat(),
                "response": response
            }
            
            logger.info(f"Created pipeline: {config.pipeline_name}")
            return {"status": "success", "pipeline": config.pipeline_name, "response": response}
            
        except ClientError as e:
            logger.error(f"Failed to create pipeline: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Get details of a SageMaker Pipeline.
        
        Args:
            pipeline_name: Name of the pipeline.
            
        Returns:
            Dictionary containing pipeline details.
        """
        try:
            response = self.client.describe_pipeline(PipelineName=pipeline_name)
            return {"status": "success", "pipeline": response}
        except ClientError as e:
            logger.error(f"Failed to describe pipeline: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_pipelines(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """List all SageMaker Pipelines.
        
        Args:
            prefix: Filter by name prefix.
            
        Returns:
            Dictionary containing list of pipelines.
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["PipelineNamePrefix"] = prefix
            
            response = self.client.list_pipelines(**kwargs)
            pipelines = response.get("PipelineSummaries", [])
            
            return {
                "status": "success",
                "pipelines": pipelines,
                "count": len(pipelines)
            }
        except ClientError as e:
            logger.error(f"Failed to list pipelines: {e}")
            return {"status": "error", "error": str(e)}
    
    def start_pipeline_execution(self, pipeline_name: str,
                                  pipeline_parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Start a SageMaker Pipeline execution.
        
        Args:
            pipeline_name: Name of the pipeline.
            pipeline_parameters: Parameters for the pipeline execution.
            
        Returns:
            Dictionary containing the pipeline execution information.
        """
        try:
            start_kwargs = {"PipelineName": pipeline_name}
            
            if pipeline_parameters:
                start_kwargs["PipelineParameters"] = pipeline_parameters
            
            response = self.client.start_pipeline_execution(**start_kwargs)
            
            logger.info(f"Started pipeline execution: {pipeline_name}")
            return {"status": "success", "pipeline_execution": response}
            
        except ClientError as e:
            logger.error(f"Failed to start pipeline execution: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_pipeline_execution(self, pipeline_execution_arn: str) -> Dict[str, Any]:
        """Get details of a pipeline execution.
        
        Args:
            pipeline_execution_arn: ARN of the pipeline execution.
            
        Returns:
            Dictionary containing pipeline execution details.
        """
        try:
            response = self.client.describe_pipeline_execution(
                PipelineExecutionArn=pipeline_execution_arn
            )
            return {"status": "success", "pipeline_execution": response}
        except ClientError as e:
            logger.error(f"Failed to describe pipeline execution: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_pipeline_execution(self, pipeline_execution_arn: str) -> Dict[str, Any]:
        """Stop a pipeline execution.
        
        Args:
            pipeline_execution_arn: ARN of the pipeline execution to stop.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.stop_pipeline_execution(
                PipelineExecutionArn=pipeline_execution_arn
            )
            
            logger.info(f"Stopped pipeline execution: {pipeline_execution_arn}")
            return {"status": "success", "pipeline_execution_arn": pipeline_execution_arn}
        except ClientError as e:
            logger.error(f"Failed to stop pipeline execution: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Delete a SageMaker Pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to delete.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.client.delete_pipeline(PipelineName=pipeline_name)
            
            if pipeline_name in self._resource_cache["pipelines"]:
                del self._resource_cache["pipelines"][pipeline_name]
            
            logger.info(f"Deleted pipeline: {pipeline_name}")
            return {"status": "success", "pipeline": pipeline_name}
        except ClientError as e:
            logger.error(f"Failed to delete pipeline: {e}")
            return {"status": "error", "error": str(e)}

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def put_metric_data(self, namespace: str, metric_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Publish custom metrics to CloudWatch.
        
        Args:
            namespace: CloudWatch namespace for the metrics.
            metric_data: List of metric data dictionaries.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            
            logger.info(f"Published {len(metric_data)} metrics to CloudWatch namespace: {namespace}")
            return {"status": "success", "count": len(metric_data)}
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_metric_statistics(self, namespace: str, metric_name: str,
                              dimensions: Optional[List[Dict[str, str]]] = None,
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None,
                              period: int = 60,
                              statistics: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get metric statistics from CloudWatch.
        
        Args:
            namespace: CloudWatch namespace.
            metric_name: Name of the metric.
            dimensions: List of dimension filters.
            start_time: Start time for the query.
            end_time: End time for the query.
            period: Period in seconds for the metrics.
            statistics: List of statistics to retrieve.
            
        Returns:
            Dictionary containing metric statistics.
        """
        try:
            kwargs = {
                "Namespace": namespace,
                "MetricName": metric_name,
                "Period": period,
            }
            
            if dimensions:
                kwargs["Dimensions"] = dimensions
            if start_time:
                kwargs["StartTime"] = start_time
            if end_time:
                kwargs["EndTime"] = end_time
            if statistics:
                kwargs["Statistics"] = statistics
            else:
                kwargs["Statistics"] = ["Average", "Sum", "SampleCount"]
            
            response = self.cloudwatch_client.get_metric_statistics(**kwargs)
            
            return {
                "status": "success",
                "label": response.get("Label"),
                "datapoints": response.get("Datapoints", [])
            }
        except ClientError as e:
            logger.error(f"Failed to get metric statistics: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_metrics(self, namespace: Optional[str] = None,
                     metric_name: Optional[str] = None,
                     dimensions: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """List metrics from CloudWatch.
        
        Args:
            namespace: Filter by namespace.
            metric_name: Filter by metric name.
            dimensions: Filter by dimensions.
            
        Returns:
            Dictionary containing list of metrics.
        """
        try:
            kwargs = {}
            if namespace:
                kwargs["Namespace"] = namespace
            if metric_name:
                kwargs["MetricName"] = metric_name
            if dimensions:
                kwargs["Dimensions"] = dimensions
            
            response = self.cloudwatch_client.list_metrics(**kwargs)
            metrics = response.get("Metrics", [])
            
            return {
                "status": "success",
                "metrics": metrics,
                "count": len(metrics)
            }
        except ClientError as e:
            logger.error(f"Failed to list metrics: {e}")
            return {"status": "error", "error": str(e)}
    
    def create_cloudwatch_alarm(self, alarm_name: str, metric_name: str,
                                namespace: str, threshold: float,
                                comparison_operator: str = "GreaterThanThreshold",
                                period: int = 60, evaluation_periods: int = 1,
                                statistic: str = "Average",
                                alarm_description: Optional[str] = None,
                                alarm_actions: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create a CloudWatch alarm for SageMaker metrics.
        
        Args:
            alarm_name: Name of the alarm.
            metric_name: Name of the metric to alarm on.
            namespace: Namespace of the metric.
            threshold: Threshold value for the alarm.
            comparison_operator: Comparison operator.
            period: Period in seconds.
            evaluation_periods: Number of evaluation periods.
            statistic: Statistic to use.
            alarm_description: Description of the alarm.
            alarm_actions: List of ARNs for alarm actions.
            
        Returns:
            Dictionary containing operation status.
        """
        try:
            alarm_kwargs = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": namespace,
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "Period": period,
                "EvaluationPeriods": evaluation_periods,
                "Statistic": statistic,
            }
            
            if alarm_description:
                alarm_kwargs["AlarmDescription"] = alarm_description
            if alarm_actions:
                alarm_kwargs["AlarmActions"] = alarm_actions
            
            self.cloudwatch_client.put_metric_alarm(**alarm_kwargs)
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return {"status": "success", "alarm_name": alarm_name}
        except ClientError as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            return {"status": "error", "error": str(e)}
    
    def record_training_metrics(self, training_job_name: str,
                                 metrics: Dict[str, float],
                                 timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """Record training job metrics to CloudWatch.
        
        Args:
            training_job_name: Name of the training job.
            metrics: Dictionary of metric names to values.
            timestamp: Timestamp for the metrics (defaults to now).
            
        Returns:
            Dictionary containing operation status.
        """
        metric_data = []
        ts = timestamp or datetime.now()
        
        for name, value in metrics.items():
            metric_data.append({
                "MetricName": name,
                "Timestamp": ts,
                "Value": value,
                "Dimensions": [
                    {"Name": "TrainingJobName", "Value": training_job_name}
                ]
            })
        
        return self.put_metric_data("SageMaker/Training", metric_data)
    
    def record_inference_metrics(self, endpoint_name: str,
                                  metrics: Dict[str, float],
                                  timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """Record inference endpoint metrics to CloudWatch.
        
        Args:
            endpoint_name: Name of the endpoint.
            metrics: Dictionary of metric names to values.
            timestamp: Timestamp for the metrics (defaults to now).
            
        Returns:
            Dictionary containing operation status.
        """
        metric_data = []
        ts = timestamp or datetime.now()
        
        for name, value in metrics.items():
            metric_data.append({
                "MetricName": name,
                "Timestamp": ts,
                "Value": value,
                "Dimensions": [
                    {"Name": "EndpointName", "Value": endpoint_name}
                ]
            })
        
        return self.put_metric_data("SageMaker/Inference", metric_data)

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_resource_cache(self) -> Dict[str, Dict[str, Any]]:
        """Get the cached resource information.
        
        Returns:
            Dictionary containing cached resources by type.
        """
        return dict(self._resource_cache)
    
    def clear_resource_cache(self) -> None:
        """Clear all cached resource information."""
        self._resource_cache.clear()
        logger.info("Cleared resource cache")
    
    def get_operation_stats(self) -> Dict[str, int]:
        """Get statistics about operations performed.
        
        Returns:
            Dictionary containing operation counts.
        """
        return dict(self._operation_counters)
    
    def _increment_operation_counter(self, operation: str) -> None:
        """Increment the counter for a specific operation type."""
        self._operation_counters[operation] += 1
    
    def health_check(self) -> Dict[str, Any]:
        """Perform a health check on the SageMaker integration.
        
        Returns:
            Dictionary containing health status of the integration.
        """
        try:
            if not BOTO3_AVAILABLE:
                return {
                    "status": "unhealthy",
                    "error": "boto3 not available",
                    "boto3_available": False
                }
            
            regions = self.client.list_training_jobs(MaxResults=1)
            
            return {
                "status": "healthy",
                "region": self.config.region_name,
                "boto3_available": True,
                "cached_resources": len(self._resource_cache)
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "boto3_available": BOTO3_AVAILABLE
            }
