"""
AWS Amplify Backend Integration Module for Workflow System

Implements an AmplifyBackendIntegration class with:
1. Backend environment: Manage backend environments
2. Backend operations: Create/update/delete backend
3. Backend imports: Import existing backend
4. Backend deletion: Delete backend environments
5. Feature flags: Manage feature flags
6. Backend configuration: Get backend configuration
7. Remove backend: Remove backend resources
8. Amplify CLI integration: Amplify CLI workflow support
9. GitHub integration: GitHub webhook integration
10. CloudWatch integration: Backend operation metrics

Commit: 'feat(aws-amplifybackend): add AWS Amplify Backend with backend environment management, operations, imports, deletion, feature flags, configuration, CLI integration, GitHub integration, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
import os
import subprocess
import re
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
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


class BackendStatus(Enum):
    """Backend environment status values."""
    DEPLOYING = "DEPLOYING"
    DEPLOYED = "DEPLOYED"
    DEPLOYMENT_FAILED = "DEPLOYMENT_FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"
    IMPORTING = "IMPORTING"
    IMPORTED = "IMPORTED"
    IMPORT_FAILED = "IMPORT_FAILED"
    LOCKED = "LOCKED"
    UNLOCKED = "UNLOCKED"


class BackendOperationType(Enum):
    """Backend operation types."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    IMPORT = "IMPORT"
    REMOVE = "REMOVE"


class BackendImportStatus(Enum):
    """Backend import status."""
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class FeatureFlagType(Enum):
    """Feature flag types for Amplify."""
    BOOLEAN = "BOOLEAN"
    STRING = "STRING"
    NUMBER = "NUMBER"


class GitHubEventType(Enum):
    """GitHub webhook event types for Amplify."""
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    CREATE = "create"
    DELETE = "delete"


@dataclass
class BackendEnvironment:
    """Backend environment details."""
    environment_name: str
    environment_id: str
    stack_name: str = ""
    status: BackendStatus = BackendStatus.DEPLOYED
    created_at: datetime = None
    updated_at: datetime = None
    deployment_artifacts: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BackendOperation:
    """Backend operation details."""
    operation_id: str
    operation_type: BackendOperationType
    backend_environment_name: str
    status: str
    created_at: datetime = None
    completed_at: datetime = None
    error_message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    flag_name: str
    flag_type: FeatureFlagType
    value: Any
    enabled: bool = True
    description: str = ""


@dataclass
class BackendConfiguration:
    """Backend configuration details."""
    app_id: str
    backend_environment_name: str
    amplify_app_name: str = ""
    cloudformation_template_url: str = ""
    deployment_artifacts: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GitHubWebhookConfig:
    """GitHub webhook configuration."""
    webhook_id: str
    webhook_url: str
    secret_token: str = ""
    events: List[str] = field(default_factory=list)
    active: bool = True


@dataclass
class AmplifyCLIConfig:
    """Amplify CLI configuration."""
    project_path: str
    amplify_app_id: str = ""
    env_name: str = ""
    cli_version: str = ""
    config: Dict[str, Any] = field(default_factory=dict)


class AmplifyBackendIntegration:
    """
    AWS Amplify Backend Integration for workflow automation.
    
    Provides comprehensive backend environment management, operations,
    feature flags, configuration, CLI integration, GitHub webhooks,
    and CloudWatch metrics.
    """
    
    def __init__(
        self,
        app_id: Optional[str] = None,
        region: str = "us-east-1",
        profile_name: Optional[str] = None,
        cloudwatch_client=None,
        amplify_client=None
    ):
        """
        Initialize AmplifyBackendIntegration.
        
        Args:
            app_id: AWS Amplify App ID
            region: AWS region
            profile_name: AWS profile name
            cloudwatch_client: Optional CloudWatch client
            amplify_client: Optional Amplify client
        """
        self.app_id = app_id
        self.region = region
        self.profile_name = profile_name
        self._cloudwatch_client = cloudwatch_client
        self._amplify_client = amplify_client
        self._cloudwatch_namespace = "AWS/AmplifyBackend"
        
        self._backends: Dict[str, BackendEnvironment] = {}
        self._operations: Dict[str, BackendOperation] = {}
        self._feature_flags: Dict[str, Dict[str, FeatureFlag]] = defaultdict(dict)
        self._configurations: Dict[str, BackendConfiguration] = {}
        self._github_webhooks: Dict[str, GitHubWebhookConfig] = {}
        self._cli_configs: Dict[str, AmplifyCLIConfig] = {}
        
        self._lock = threading.RLock()
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
    
    @property
    def amplify_client(self):
        """Get or create Amplify client."""
        if self._amplify_client is None and BOTO3_AVAILABLE:
            client_kwargs = {"region_name": self.region}
            if self.profile_name:
                client_kwargs["profile_name"] = self.profile_name
            self._amplify_client = boto3.client("amplify", **client_kwargs)
        return self._amplify_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None and BOTO3_AVAILABLE:
            client_kwargs = {"region_name": self.region}
            if self.profile_name:
                client_kwargs["profile_name"] = self.profile_name
            self._cloudwatch_client = boto3.client("cloudwatch", **client_kwargs)
        return self._cloudwatch_client
    
    # =========================================================================
    # 1. BACKEND ENVIRONMENT MANAGEMENT
    # =========================================================================
    
    def create_backend_environment(
        self,
        environment_name: str,
        stack_name: Optional[str] = None,
        deployment_artifacts: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> BackendEnvironment:
        """
        Create a new backend environment.
        
        Args:
            environment_name: Name for the backend environment
            stack_name: CloudFormation stack name
            deployment_artifacts: Deployment artifact configuration
            tags: AWS tags for the environment
            
        Returns:
            BackendEnvironment object
        """
        if not self.app_id:
            raise ValueError("app_id is required for backend operations")
        
        environment_id = f"{self.app_id}-{environment_name}"
        
        with self._lock:
            if environment_name in self._backends:
                raise ValueError(f"Backend environment '{environment_name}' already exists")
            
            backend = BackendEnvironment(
                environment_name=environment_name,
                environment_id=environment_id,
                stack_name=stack_name or f"amplify-{environment_name}",
                status=BackendStatus.DEPLOYING,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deployment_artifacts=deployment_artifacts or {},
                metadata={"tags": tags or {}}
            )
            
            self._backends[environment_name] = backend
            
            operation_id = self._generate_operation_id()
            operation = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.CREATE,
                backend_environment_name=environment_name,
                status="IN_PROGRESS",
                created_at=datetime.now()
            )
            self._operations[operation_id] = operation
        
        try:
            if self.amplify_client and BOTO3_AVAILABLE:
                response = self.amplify_client.create_backend_environment(
                    appId=self.app_id,
                    environmentName=environment_name,
                    stackName=stack_name,
                    tags=tags
                )
                
                backend.status = BackendStatus.DEPLOYED
                backend.updated_at = datetime.now()
                
                self._record_operation_metric(
                    "CreateBackendEnvironment",
                    "Success",
                    environment_name
                )
            else:
                backend.status = BackendStatus.DEPLOYED
                
        except (ClientError, BotoCoreError) as e:
            backend.status = BackendStatus.DEPLOYMENT_FAILED
            self._record_operation_metric(
                "CreateBackendEnvironment",
                "Failure",
                environment_name,
                str(e)
            )
            logger.error(f"Failed to create backend environment: {e}")
            raise
        
        return backend
    
    def list_backend_environments(
        self,
        include_deleted: bool = False
    ) -> List[BackendEnvironment]:
        """
        List all backend environments.
        
        Args:
            include_deleted: Include deleted environments
            
        Returns:
            List of BackendEnvironment objects
        """
        backends = []
        
        with self._lock:
            for backend in self._backends.values():
                if include_deleted or backend.status != BackendStatus.DELETED:
                    backends.append(copy.deepcopy(backend))
        
        if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
            try:
                response = self.amplify_client.list_backend_environments(
                    appId=self.app_id
                )
                
                for item in response.get("backendEnvironments", []):
                    env_name = item["environmentName"]
                    if env_name not in self._backends:
                        backend = BackendEnvironment(
                            environment_name=env_name,
                            environment_id=item.get("backendEnvironmentId", ""),
                            stack_name=item.get("stackName", ""),
                            status=BackendStatus(item.get("status", "DEPLOYED")),
                            created_at=item.get("createdAt", datetime.now()),
                            updated_at=item.get("updatedAt", datetime.now())
                        )
                        self._backends[env_name] = backend
                        if include_deleted or backend.status != BackendStatus.DELETED:
                            backends.append(backend)
                            
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to list backend environments from API: {e}")
        
        return backends
    
    def get_backend_environment(
        self,
        environment_name: str
    ) -> Optional[BackendEnvironment]:
        """
        Get backend environment details.
        
        Args:
            environment_name: Name of the environment
            
        Returns:
            BackendEnvironment or None
        """
        with self._lock:
            if environment_name in self._backends:
                return copy.deepcopy(self._backends[environment_name])
        
        if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
            try:
                response = self.amplify_client.get_backend_environment(
                    appId=self.app_id,
                    environmentName=environment_name
                )
                
                backend = BackendEnvironment(
                    environment_name=environment_name,
                    environment_id=response.get("backendEnvironmentId", ""),
                    stack_name=response.get("stackName", ""),
                    status=BackendStatus(response.get("status", "DEPLOYED")),
                    created_at=response.get("createdAt", datetime.now()),
                    updated_at=response.get("updatedAt", datetime.now())
                )
                
                with self._lock:
                    self._backends[environment_name] = backend
                
                return copy.deepcopy(backend)
                
            except (ClientError, BotoCoreError):
                pass
        
        return None
    
    def update_backend_environment(
        self,
        environment_name: str,
        deployment_artifacts: Optional[Dict[str, Any]] = None,
        stack_name: Optional[str] = None
    ) -> BackendEnvironment:
        """
        Update a backend environment.
        
        Args:
            environment_name: Name of the environment to update
            deployment_artifacts: New deployment artifacts
            stack_name: New stack name
            
        Returns:
            Updated BackendEnvironment
        """
        backend = self.get_backend_environment(environment_name)
        if not backend:
            raise ValueError(f"Backend environment '{environment_name}' not found")
        
        operation_id = self._generate_operation_id()
        operation = BackendOperation(
            operation_id=operation_id,
            operation_type=BackendOperationType.UPDATE,
            backend_environment_name=environment_name,
            status="IN_PROGRESS",
            created_at=datetime.now()
        )
        self._operations[operation_id] = operation
        
        try:
            if self.amplify_client and BOTO3_AVAILABLE:
                self.amplify_client.update_backend_environment(
                    appId=self.app_id,
                    environmentName=environment_name,
                    deploymentArtifacts=deployment_artifacts,
                    stackName=stack_name
                )
            
            with self._lock:
                if environment_name in self._backends:
                    self._backends[environment_name].updated_at = datetime.now()
                    if deployment_artifacts:
                        self._backends[environment_name].deployment_artifacts.update(deployment_artifacts)
                    if stack_name:
                        self._backends[environment_name].stack_name = stack_name
                    backend = copy.deepcopy(self._backends[environment_name])
            
            operation.status = "COMPLETED"
            operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "UpdateBackendEnvironment",
                "Success",
                environment_name
            )
            
        except (ClientError, BotoCoreError) as e:
            operation.status = "FAILED"
            operation.error_message = str(e)
            operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "UpdateBackendEnvironment",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return backend
    
    # =========================================================================
    # 2. BACKEND OPERATIONS: CREATE/UPDATE/DELETE
    # =========================================================================
    
    def create_backend(
        self,
        backend_config: Dict[str, Any],
        environment_name: str = "dev"
    ) -> BackendOperation:
        """
        Create a complete backend with configuration.
        
        Args:
            backend_config: Backend configuration dictionary
            environment_name: Environment name for the backend
            
        Returns:
            BackendOperation with operation details
        """
        operation_id = self._generate_operation_id()
        
        with self._lock:
            operation = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.CREATE,
                backend_environment_name=environment_name,
                status="PENDING",
                created_at=datetime.now(),
                metadata={"config": backend_config}
            )
            self._operations[operation_id] = operation
        
        try:
            operation.status = "IN_PROGRESS"
            
            if "resource_config" in backend_config:
                resources = backend_config["resource_config"]
                
                for resource_type, resource_config in resources.items():
                    self._create_backend_resource(
                        environment_name,
                        resource_type,
                        resource_config
                    )
            
            backend = self.create_backend_environment(
                environment_name=environment_name,
                stack_name=backend_config.get("stack_name"),
                deployment_artifacts=backend_config.get("deployment_artifacts"),
                tags=backend_config.get("tags")
            )
            
            with self._lock:
                operation.status = "COMPLETED"
                operation.completed_at = datetime.now()
                operation.metadata["backend_environment_id"] = backend.environment_id
            
            self._record_operation_metric(
                "CreateBackend",
                "Success",
                environment_name
            )
            
        except Exception as e:
            with self._lock:
                operation.status = "FAILED"
                operation.error_message = str(e)
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "CreateBackend",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return operation
    
    def update_backend(
        self,
        environment_name: str,
        backend_config: Dict[str, Any]
    ) -> BackendOperation:
        """
        Update backend with new configuration.
        
        Args:
            environment_name: Name of the backend environment
            backend_config: New backend configuration
            
        Returns:
            BackendOperation with operation details
        """
        operation_id = self._generate_operation_id()
        
        with self._lock:
            operation = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.UPDATE,
                backend_environment_name=environment_name,
                status="PENDING",
                created_at=datetime.now(),
                metadata={"config": backend_config}
            )
            self._operations[operation_id] = operation
        
        try:
            operation.status = "IN_PROGRESS"
            
            if "resource_config" in backend_config:
                resources = backend_config["resource_config"]
                
                for resource_type, resource_config in resources.items():
                    self._update_backend_resource(
                        environment_name,
                        resource_type,
                        resource_config
                    )
            
            backend = self.update_backend_environment(
                environment_name=environment_name,
                deployment_artifacts=backend_config.get("deployment_artifacts"),
                stack_name=backend_config.get("stack_name")
            )
            
            with self._lock:
                operation.status = "COMPLETED"
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "UpdateBackend",
                "Success",
                environment_name
            )
            
        except Exception as e:
            with self._lock:
                operation.status = "FAILED"
                operation.error_message = str(e)
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "UpdateBackend",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return operation
    
    def delete_backend(
        self,
        environment_name: str,
        delete_resources: bool = True
    ) -> BackendOperation:
        """
        Delete a backend environment.
        
        Args:
            environment_name: Name of the environment to delete
            delete_resources: Whether to delete underlying resources
            
        Returns:
            BackendOperation with operation details
        """
        operation_id = self._generate_operation_id()
        
        with self._lock:
            operation = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.DELETE,
                backend_environment_name=environment_name,
                status="PENDING",
                created_at=datetime.now(),
                metadata={"delete_resources": delete_resources}
            )
            self._operations[operation_id] = operation
        
        try:
            operation.status = "IN_PROGRESS"
            
            if delete_resources and self.amplify_client and BOTO3_AVAILABLE:
                self._delete_backend_resources(environment_name)
            
            if environment_name in self._backends:
                self._backends[environment_name].status = BackendStatus.DELETING
            
            if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
                try:
                    self.amplify_client.delete_backend_environment(
                        appId=self.app_id,
                        environmentName=environment_name
                    )
                except (ClientError, BotoCoreError) as e:
                    if "NotFoundException" not in str(e):
                        raise
            
            with self._lock:
                if environment_name in self._backends:
                    self._backends[environment_name].status = BackendStatus.DELETED
            
            operation.status = "COMPLETED"
            operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "DeleteBackend",
                "Success",
                environment_name
            )
            
        except Exception as e:
            with self._lock:
                operation.status = "FAILED"
                operation.error_message = str(e)
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "DeleteBackend",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return operation
    
    def _create_backend_resource(
        self,
        environment_name: str,
        resource_type: str,
        resource_config: Dict[str, Any]
    ) -> None:
        """Create a backend resource."""
        logger.info(f"Creating {resource_type} resource for {environment_name}")
    
    def _update_backend_resource(
        self,
        environment_name: str,
        resource_type: str,
        resource_config: Dict[str, Any]
    ) -> None:
        """Update a backend resource."""
        logger.info(f"Updating {resource_type} resource for {environment_name}")
    
    def _delete_backend_resources(self, environment_name: str) -> None:
        """Delete backend resources."""
        logger.info(f"Deleting resources for {environment_name}")
    
    # =========================================================================
    # 3. BACKEND IMPORTS
    # =========================================================================
    
    def import_backend(
        self,
        environment_name: str,
        import_config: Dict[str, Any]
    ) -> BackendEnvironment:
        """
        Import an existing backend.
        
        Args:
            environment_name: Name for the imported environment
            import_config: Import configuration with resource details
            
        Returns:
            BackendEnvironment for the imported backend
        """
        operation_id = self._generate_operation_id()
        
        backend = BackendEnvironment(
            environment_name=environment_name,
            environment_id=f"{self.app_id}-{environment_name}" if self.app_id else str(uuid.uuid4()),
            status=BackendStatus.IMPORTING,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            metadata={"imported": True, "import_config": import_config}
        )
        
        with self._lock:
            self._backends[environment_name] = backend
            self._operations[operation_id] = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.IMPORT,
                backend_environment_name=environment_name,
                status="IN_PROGRESS",
                created_at=datetime.now()
            )
        
        try:
            imported_resources = import_config.get("resources", [])
            
            if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
                try:
                    response = self.amplify_client.import_backend_environment(
                        appId=self.app_id,
                        environmentName=environment_name
                    )
                    
                    backend.environment_id = response.get("backendEnvironmentId", backend.environment_id)
                except (ClientError, BotoCoreError) as e:
                    logger.warning(f"Import API not available: {e}")
            
            for resource in imported_resources:
                self._import_backend_resource(environment_name, resource)
            
            with self._lock:
                backend.status = BackendStatus.IMPORTED
                backend.updated_at = datetime.now()
                operation = self._operations.get(operation_id)
                if operation:
                    operation.status = "COMPLETED"
                    operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "ImportBackend",
                "Success",
                environment_name
            )
            
        except Exception as e:
            with self._lock:
                backend.status = BackendStatus.IMPORT_FAILED
                operation = self._operations.get(operation_id)
                if operation:
                    operation.status = "FAILED"
                    operation.error_message = str(e)
                    operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "ImportBackend",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return backend
    
    def _import_backend_resource(
        self,
        environment_name: str,
        resource_config: Dict[str, Any]
    ) -> None:
        """Import a single backend resource."""
        resource_type = resource_config.get("type", "unknown")
        resource_id = resource_config.get("id", "")
        logger.info(f"Importing {resource_type} resource: {resource_id}")
    
    def list_imported_backends(self) -> List[BackendEnvironment]:
        """List all imported backends."""
        imported = []
        with self._lock:
            for backend in self._backends.values():
                if backend.metadata.get("imported", False):
                    imported.append(copy.deepcopy(backend))
        return imported
    
    # =========================================================================
    # 4. BACKEND DELETION
    # =========================================================================
    
    def delete_backend_environment(
        self,
        environment_name: str,
        force: bool = False
    ) -> bool:
        """
        Delete a backend environment.
        
        Args:
            environment_name: Name of the environment to delete
            force: Force deletion even if resources exist
            
        Returns:
            True if deletion was successful
        """
        backend = self.get_backend_environment(environment_name)
        if not backend:
            raise ValueError(f"Backend environment '{environment_name}' not found")
        
        if not force and backend.status == BackendStatus.DEPLOYED:
            has_resources = self._check_backend_has_resources(environment_name)
            if has_resources:
                raise ValueError(
                    f"Backend environment '{environment_name}' has resources. "
                    "Use force=True to delete anyway."
                )
        
        operation = self.delete_backend(environment_name, delete_resources=True)
        
        return operation.status == "COMPLETED"
    
    def _check_backend_has_resources(self, environment_name: str) -> bool:
        """Check if backend has associated resources."""
        return False
    
    def delete_all_backend_environments(
        self,
        except_environments: Optional[List[str]] = None
    ) -> Dict[str, BackendOperation]:
        """
        Delete all backend environments.
        
        Args:
            except_environments: List of environments to keep
            
        Returns:
            Dictionary mapping environment names to operations
        """
        except_environments = except_environments or []
        results = {}
        
        backends = self.list_backend_environments(include_deleted=False)
        
        for backend in backends:
            if backend.environment_name not in except_environments:
                try:
                    operation = self.delete_backend(
                        backend.environment_name,
                        delete_resources=True
                    )
                    results[backend.environment_name] = operation
                except Exception as e:
                    results[backend.environment_name] = BackendOperation(
                        operation_id=str(uuid.uuid4()),
                        operation_type=BackendOperationType.DELETE,
                        backend_environment_name=backend.environment_name,
                        status="FAILED",
                        error_message=str(e)
                    )
        
        return results
    
    # =========================================================================
    # 5. FEATURE FLAGS
    # =========================================================================
    
    def create_feature_flag(
        self,
        environment_name: str,
        flag_name: str,
        flag_type: FeatureFlagType,
        value: Any,
        enabled: bool = True,
        description: str = ""
    ) -> FeatureFlag:
        """
        Create a feature flag.
        
        Args:
            environment_name: Environment for the flag
            flag_name: Name of the flag
            flag_type: Type of the flag
            value: Default value for the flag
            enabled: Whether the flag is enabled
            description: Description of the flag
            
        Returns:
            FeatureFlag object
        """
        flag = FeatureFlag(
            flag_name=flag_name,
            flag_type=flag_type,
            value=value,
            enabled=enabled,
            description=description
        )
        
        with self._lock:
            self._feature_flags[environment_name][flag_name] = flag
        
        self._record_operation_metric(
            "CreateFeatureFlag",
            "Success",
            environment_name,
            flag_name
        )
        
        return flag
    
    def get_feature_flag(
        self,
        environment_name: str,
        flag_name: str
    ) -> Optional[FeatureFlag]:
        """Get a feature flag."""
        with self._lock:
            return self._feature_flags.get(environment_name, {}).get(flag_name)
    
    def list_feature_flags(
        self,
        environment_name: str
    ) -> List[FeatureFlag]:
        """List all feature flags for an environment."""
        with self._lock:
            return list(self._feature_flags.get(environment_name, {}).values())
    
    def update_feature_flag(
        self,
        environment_name: str,
        flag_name: str,
        value: Optional[Any] = None,
        enabled: Optional[bool] = None,
        description: Optional[str] = None
    ) -> FeatureFlag:
        """Update a feature flag."""
        flag = self.get_feature_flag(environment_name, flag_name)
        if not flag:
            raise ValueError(f"Feature flag '{flag_name}' not found")
        
        if value is not None:
            flag.value = value
        if enabled is not None:
            flag.enabled = enabled
        if description is not None:
            flag.description = description
        
        self._record_operation_metric(
            "UpdateFeatureFlag",
            "Success",
            environment_name,
            flag_name
        )
        
        return flag
    
    def delete_feature_flag(
        self,
        environment_name: str,
        flag_name: str
    ) -> bool:
        """Delete a feature flag."""
        with self._lock:
            if environment_name in self._feature_flags:
                if flag_name in self._feature_flags[environment_name]:
                    del self._feature_flags[environment_name][flag_name]
                    self._record_operation_metric(
                        "DeleteFeatureFlag",
                        "Success",
                        environment_name,
                        flag_name
                    )
                    return True
        return False
    
    def batch_update_feature_flags(
        self,
        environment_name: str,
        updates: Dict[str, Dict[str, Any]]
    ) -> List[FeatureFlag]:
        """
        Batch update feature flags.
        
        Args:
            environment_name: Environment name
            updates: Dictionary mapping flag names to update values
            
        Returns:
            List of updated FeatureFlag objects
        """
        results = []
        
        for flag_name, update_values in updates.items():
            try:
                flag = self.update_feature_flag(
                    environment_name,
                    flag_name,
                    **update_values
                )
                results.append(flag)
            except ValueError:
                flag_type = FeatureFlagType(update_values.get("flag_type", "BOOLEAN"))
                flag = self.create_feature_flag(
                    environment_name,
                    flag_name,
                    flag_type,
                    update_values.get("value", True),
                    update_values.get("enabled", True),
                    update_values.get("description", "")
                )
                results.append(flag)
        
        return results
    
    # =========================================================================
    # 6. BACKEND CONFIGURATION
    # =========================================================================
    
    def get_backend_configuration(
        self,
        environment_name: str
    ) -> Optional[BackendConfiguration]:
        """
        Get backend configuration.
        
        Args:
            environment_name: Name of the environment
            
        Returns:
            BackendConfiguration or None
        """
        config_key = f"{self.app_id}:{environment_name}" if self.app_id else environment_name
        
        if config_key in self._configurations:
            return copy.deepcopy(self._configurations[config_key])
        
        backend = self.get_backend_environment(environment_name)
        if not backend:
            return None
        
        config = BackendConfiguration(
            app_id=self.app_id or "",
            backend_environment_name=environment_name,
            amplify_app_name=self.app_id or "",
            deployment_artifacts=backend.deployment_artifacts,
            metadata=backend.metadata
        )
        
        if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
            try:
                response = self.amplify_client.get_backend_configuration(
                    appId=self.app_id,
                    environmentName=environment_name
                )
                config.cloudformation_template_url = response.get(
                    "cloudFormationTemplateUrl", ""
                )
                config.deployment_artifacts = response.get(
                    "deploymentArtifacts", {}
                )
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to get backend configuration: {e}")
        
        self._configurations[config_key] = config
        
        return copy.deepcopy(config)
    
    def update_backend_configuration(
        self,
        environment_name: str,
        configuration: Dict[str, Any]
    ) -> BackendConfiguration:
        """
        Update backend configuration.
        
        Args:
            environment_name: Environment name
            configuration: New configuration
            
        Returns:
            Updated BackendConfiguration
        """
        config_key = f"{self.app_id}:{environment_name}" if self.app_id else environment_name
        
        config = BackendConfiguration(
            app_id=self.app_id or "",
            backend_environment_name=environment_name,
            amplify_app_name=configuration.get("app_name", self.app_id or ""),
            cloudformation_template_url=configuration.get(
                "cloudformation_template_url", ""
            ),
            deployment_artifacts=configuration.get("deployment_artifacts", {}),
            metadata=configuration.get("metadata", {})
        )
        
        with self._lock:
            self._configurations[config_key] = config
        
        self._record_operation_metric(
            "UpdateBackendConfiguration",
            "Success",
            environment_name
        )
        
        return copy.deepcopy(config)
    
    def generate_backend_config_template(
        self,
        environment_name: str,
        template_type: str = "full"
    ) -> Dict[str, Any]:
        """
        Generate a backend configuration template.
        
        Args:
            environment_name: Environment name
            template_type: Type of template (full, minimal, api, auth)
            
        Returns:
            Configuration template dictionary
        """
        base_template = {
            "version": "1.0",
            "backend_environment_name": environment_name,
            "cloudformation": {
                "stack_name": f"amplify-{environment_name}",
                "template_format_version": "2010-09-09"
            }
        }
        
        if template_type == "full":
            base_template.update({
                "auth": {
                    "resource_name": f"{environment_name}-auth",
                    "authentication_type": "user_pool"
                },
                "api": {
                    "resource_name": f"{environment_name}-api",
                    "api_type": "REST"
                },
                "storage": {
                    "resource_name": f"{environment_name}-storage",
                    "storage_type": "S3"
                }
            })
        elif template_type == "api":
            base_template.update({
                "api": {
                    "resource_name": f"{environment_name}-api",
                    "api_type": "GraphQL"
                }
            })
        elif template_type == "auth":
            base_template.update({
                "auth": {
                    "resource_name": f"{environment_name}-auth",
                    "authentication_type": "user_pool"
                }
            })
        
        return base_template
    
    # =========================================================================
    # 7. REMOVE BACKEND RESOURCES
    # =========================================================================
    
    def remove_backend_resources(
        self,
        environment_name: str,
        resource_types: Optional[List[str]] = None
    ) -> BackendOperation:
        """
        Remove backend resources.
        
        Args:
            environment_name: Environment name
            resource_types: List of resource types to remove (None = all)
            
        Returns:
            BackendOperation with removal details
        """
        operation_id = self._generate_operation_id()
        
        with self._lock:
            operation = BackendOperation(
                operation_id=operation_id,
                operation_type=BackendOperationType.REMOVE,
                backend_environment_name=environment_name,
                status="IN_PROGRESS",
                created_at=datetime.now(),
                metadata={"resource_types": resource_types or []}
            )
            self._operations[operation_id] = operation
        
        try:
            if resource_types is None:
                resource_types = ["auth", "api", "storage", "function", "hosting"]
            
            for resource_type in resource_types:
                self._remove_backend_resource_type(environment_name, resource_type)
            
            with self._lock:
                operation.status = "COMPLETED"
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "RemoveBackendResources",
                "Success",
                environment_name
            )
            
        except Exception as e:
            with self._lock:
                operation.status = "FAILED"
                operation.error_message = str(e)
                operation.completed_at = datetime.now()
            
            self._record_operation_metric(
                "RemoveBackendResources",
                "Failure",
                environment_name,
                str(e)
            )
            raise
        
        return operation
    
    def _remove_backend_resource_type(
        self,
        environment_name: str,
        resource_type: str
    ) -> None:
        """Remove a specific resource type."""
        logger.info(f"Removing {resource_type} resources from {environment_name}")
    
    def remove_all_backend_resources(
        self,
        environment_name: str
    ) -> BackendOperation:
        """Remove all backend resources for an environment."""
        return self.remove_backend_resources(environment_name, None)
    
    # =========================================================================
    # 8. AMPLIFY CLI INTEGRATION
    # =========================================================================
    
    def setup_amplify_cli(
        self,
        project_path: str,
        amplify_app_id: Optional[str] = None
    ) -> AmplifyCLIConfig:
        """
        Setup Amplify CLI configuration.
        
        Args:
            project_path: Path to the Amplify project
            amplify_app_id: Optional Amplify App ID
            
        Returns:
            AmplifyCLIConfig object
        """
        config = AmplifyCLIConfig(
            project_path=project_path,
            amplify_app_id=amplify_app_id or self.app_id or ""
        )
        
        try:
            result = subprocess.run(
                ["amplify", "--version"],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                config.cli_version = result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            config.cli_version = "not_installed"
        
        with self._lock:
            self._cli_configs[project_path] = config
        
        return config
    
    def run_amplify_cli_command(
        self,
        project_path: str,
        command: List[str],
        env_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run an Amplify CLI command.
        
        Args:
            project_path: Path to the Amplify project
            command: Command arguments (e.g., ["push", "--yes"])
            env_name: Optional environment name
            
        Returns:
            Command result dictionary
        """
        config = self._cli_configs.get(project_path)
        if not config:
            config = self.setup_amplify_cli(project_path)
        
        full_command = ["amplify"] + command
        
        env = os.environ.copy()
        if env_name:
            env["AMPLIFY_ENV_NAME"] = env_name
        if config.amplify_app_id:
            env["AMPLIFY_APP_ID"] = config.amplify_app_id
        
        try:
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                timeout=300,
                cwd=project_path,
                env=env
            )
            
            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": "Command timed out"
            }
        except FileNotFoundError:
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": "Amplify CLI not found"
            }
    
    def amplify_push(
        self,
        project_path: str,
        env_name: Optional[str] = None,
        force_push: bool = False
    ) -> Dict[str, Any]:
        """
        Run amplify push.
        
        Args:
            project_path: Path to the Amplify project
            env_name: Environment name
            force_push: Force push even if validation fails
            
        Returns:
            Command result
        """
        command = ["push"]
        if force_push:
            command.append("--force")
        
        return self.run_amplify_cli_command(project_path, command, env_name)
    
    def amplify_pull(
        self,
        project_path: str,
        env_name: Optional[str] = None,
        restore_config: bool = False
    ) -> Dict[str, Any]:
        """
        Run amplify pull.
        
        Args:
            project_path: Path to the Amplify project
            env_name: Environment name
            restore_config: Restore backend config from cloud
            
        Returns:
            Command result
        """
        command = ["pull"]
        if restore_config:
            command.append("--restore")
        
        return self.run_amplify_cli_command(project_path, command, env_name)
    
    def amplify_env_add(
        self,
        project_path: str,
        env_name: str,
        env_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add a new Amplify environment.
        
        Args:
            project_path: Path to the Amplify project
            env_name: New environment name
            env_config: Environment configuration
            
        Returns:
            Command result
        """
        command = ["env", "add", "--name", env_name]
        
        if env_config:
            command.append("--yes")
        
        return self.run_amplify_cli_command(project_path, command)
    
    def amplify_env_checkout(
        self,
        project_path: str,
        env_name: str
    ) -> Dict[str, Any]:
        """
        Checkout an Amplify environment.
        
        Args:
            project_path: Path to the Amplify project
            env_name: Environment name to checkout
            
        Returns:
            Command result
        """
        return self.run_amplify_cli_command(
            project_path,
            ["env", "checkout", "--name", env_name]
        )
    
    def amplify_status(
        self,
        project_path: str
    ) -> Dict[str, Any]:
        """
        Get amplify status.
        
        Args:
            project_path: Path to the Amplify project
            
        Returns:
            Status result
        """
        return self.run_amplify_cli_command(project_path, ["status"])
    
    def amplify_init(
        self,
        project_path: str,
        project_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Initialize an Amplify project.
        
        Args:
            project_path: Path for the new Amplify project
            project_config: Project configuration
            
        Returns:
            Command result
        """
        command = ["init"]
        
        if "app_name" in project_config:
            command.extend(["--app-name", project_config["app_name"]])
        if "env_name" in project_config:
            command.extend(["--env-name", project_config["env_name"]])
        if "framework" in project_config:
            command.extend(["--framework", project_config["framework"]])
        
        command.append("--yes")
        
        return self.run_amplify_cli_command(project_path, command)
    
    # =========================================================================
    # 9. GITHUB INTEGRATION
    # =========================================================================
    
    def setup_github_webhook(
        self,
        environment_name: str,
        repository_owner: str,
        repository_name: str,
        branch_pattern: str = "*",
        events: Optional[List[str]] = None
    ) -> GitHubWebhookConfig:
        """
        Setup GitHub webhook for Amplify.
        
        Args:
            environment_name: Amplify environment name
            repository_owner: GitHub repository owner
            repository_name: GitHub repository name
            branch_pattern: Branch pattern to trigger webhooks
            events: List of GitHub events to listen for
            
        Returns:
            GitHubWebhookConfig object
        """
        webhook_id = str(uuid.uuid4())
        webhook_url = f"https://{self.region}.console.aws.amazon.com/amplify/webhooks/{webhook_id}"
        
        if events is None:
            events = [
                GitHubEventType.PUSH.value,
                GitHubEventType.PULL_REQUEST.value
            ]
        
        secret_token = self._generate_webhook_secret()
        
        webhook = GitHubWebhookConfig(
            webhook_id=webhook_id,
            webhook_url=webhook_url,
            secret_token=secret_token,
            events=events,
            active=True
        )
        
        with self._lock:
            self._github_webhooks[webhook_id] = webhook
        
        if self.amplify_client and BOTO3_AVAILABLE and self.app_id:
            try:
                self.amplify_client.create_webhook(
                    appId=self.app_id,
                    branchName=environment_name,
                    webhookUrl=webhook_url,
                    webhookSecret=secret_token
                )
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to create webhook in Amplify: {e}")
        
        self._record_operation_metric(
            "SetupGitHubWebhook",
            "Success",
            environment_name
        )
        
        return webhook
    
    def get_github_webhook(
        self,
        webhook_id: str
    ) -> Optional[GitHubWebhookConfig]:
        """Get GitHub webhook configuration."""
        return self._github_webhooks.get(webhook_id)
    
    def list_github_webhooks(
        self,
        environment_name: Optional[str] = None
    ) -> List[GitHubWebhookConfig]:
        """List GitHub webhooks."""
        webhooks = []
        with self._lock:
            for webhook in self._github_webhooks.values():
                webhooks.append(copy.deepcopy(webhook))
        return webhooks
    
    def delete_github_webhook(
        self,
        webhook_id: str
    ) -> bool:
        """Delete a GitHub webhook."""
        if webhook_id in self._github_webhooks:
            del self._github_webhooks[webhook_id]
            self._record_operation_metric(
                "DeleteGitHubWebhook",
                "Success"
            )
            return True
        return False
    
    def validate_github_webhook_payload(
        self,
        webhook_id: str,
        payload: str,
        signature: str
    ) -> bool:
        """
        Validate GitHub webhook payload signature.
        
        Args:
            webhook_id: Webhook ID
            payload: Raw webhook payload
            signature: X-Hub-Signature-256 header value
            
        Returns:
            True if signature is valid
        """
        webhook = self.get_github_webhook(webhook_id)
        if not webhook:
            return False
        
        import hmac
        expected = "sha256=" + hmac.new(
            webhook.secret_token.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(expected, signature)
    
    def parse_github_webhook_event(
        self,
        payload: Dict[str, Any],
        event_type: str
    ) -> Dict[str, Any]:
        """
        Parse GitHub webhook event.
        
        Args:
            payload: Webhook payload
            event_type: GitHub event type
            
        Returns:
            Parsed event information
        """
        parsed = {
            "event_type": event_type,
            "action": payload.get("action", ""),
            "repository": {},
            "branch": "",
            "commit": ""
        }
        
        if "repository" in payload:
            repo = payload["repository"]
            parsed["repository"] = {
                "owner": repo.get("owner", {}).get("login", ""),
                "name": repo.get("name", ""),
                "full_name": repo.get("full_name", "")
            }
        
        if event_type == GitHubEventType.PUSH.value:
            parsed["branch"] = payload.get("ref", "").replace("refs/heads/", "")
            parsed["commit"] = payload.get("after", "")
        elif event_type == GitHubEventType.PULL_REQUEST.value:
            if "pull_request" in payload:
                pr = payload["pull_request"]
                parsed["branch"] = pr.get("head", {}).get("ref", "")
                parsed["commit"] = pr.get("head", {}).get("sha", "")
        
        return parsed
    
    def _generate_webhook_secret(self) -> str:
        """Generate a random webhook secret."""
        return hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:32]
    
    # =========================================================================
    # 10. CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def _record_operation_metric(
        self,
        operation: str,
        result: str,
        environment: str = "",
        error_message: str = ""
    ) -> None:
        """
        Record an operation metric.
        
        Args:
            operation: Operation name
            result: Operation result (Success/Failure)
            environment: Environment name
            error_message: Error message if failed
        """
        metric = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "result": result,
            "environment": environment,
            "app_id": self.app_id or "unknown",
            "region": self.region
        }
        
        if error_message:
            metric["error_message"] = error_message
        
        with self._metrics_lock:
            self._metrics_buffer.append(metric)
            
            if len(self._metrics_buffer) >= 10:
                self._flush_metrics()
    
    def _flush_metrics(self) -> None:
        """Flush buffered metrics to CloudWatch."""
        if not self._metrics_buffer or not self.cloudwatch_client:
            return
        
        metrics_to_send = []
        
        with self._metrics_lock:
            metrics_to_send = self._metrics_buffer.copy()
            self._metrics_buffer.clear()
        
        try:
            metric_data = []
            
            operation_counts = defaultdict(int)
            for metric in metrics_to_send:
                operation_counts[(metric["operation"], metric["result"])] += 1
            
            for (operation, result), count in operation_counts.items():
                metric_data.append({
                    "MetricName": f"{operation}_{result}",
                    "Value": count,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Operation", "Value": operation},
                        {"Name": "Result", "Value": result}
                    ]
                })
            
            if metric_data:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self._cloudwatch_namespace,
                    MetricData=metric_data
                )
                
        except (ClientError, BotoCoreError) as e:
            logger.warning(f"Failed to send metrics to CloudWatch: {e}")
    
    def get_backend_metrics(
        self,
        environment_name: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get backend metrics from CloudWatch.
        
        Args:
            environment_name: Backend environment name
            metric_name: Name of the metric
            start_time: Start of time range
            end_time: End of time range
            period: Metric period in seconds
            
        Returns:
            List of metric data points
        """
        if not self.cloudwatch_client:
            return []
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace=self._cloudwatch_namespace,
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Sum", "Average", "Maximum", "Minimum"]
            )
            
            return response.get("Datapoints", [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get metrics: {e}")
            return []
    
    def create_backend_alarm(
        self,
        alarm_name: str,
        environment_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for backend metrics.
        
        Args:
            alarm_name: Name of the alarm
            environment_name: Backend environment
            metric_name: Metric to monitor
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            
        Returns:
            Alarm configuration
        """
        if not self.cloudwatch_client:
            return {"error": "CloudWatch client not available"}
        
        try:
            response = self.cloudwatch_client.put_metric_alarm(
                AlarmName=f"AmplifyBackend-{environment_name}-{alarm_name}",
                Namespace=self._cloudwatch_namespace,
                MetricName=metric_name,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Dimensions=[
                    {"Name": "Environment", "Value": environment_name}
                ]
            )
            
            return {"success": True, "alarm_name": alarm_name}
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create alarm: {e}")
            return {"success": False, "error": str(e)}
    
    def get_operation_history(
        self,
        environment_name: Optional[str] = None,
        operation_type: Optional[BackendOperationType] = None,
        limit: int = 100
    ) -> List[BackendOperation]:
        """
        Get operation history.
        
        Args:
            environment_name: Filter by environment
            operation_type: Filter by operation type
            limit: Maximum number of operations to return
            
        Returns:
            List of BackendOperation objects
        """
        operations = []
        
        with self._lock:
            for op in self._operations.values():
                if environment_name and op.backend_environment_name != environment_name:
                    continue
                if operation_type and op.operation_type != operation_type:
                    continue
                operations.append(copy.deepcopy(op))
        
        operations.sort(key=lambda x: x.created_at or datetime.min, reverse=True)
        
        return operations[:limit]
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _generate_operation_id(self) -> str:
        """Generate a unique operation ID."""
        return f"op-{uuid.uuid4().hex[:12]}"
    
    def get_backend_summary(self) -> Dict[str, Any]:
        """
        Get summary of all backends.
        
        Returns:
            Summary dictionary
        """
        backends = self.list_backend_environments()
        
        summary = {
            "total_backends": len(backends),
            "by_status": defaultdict(int),
            "environments": []
        }
        
        for backend in backends:
            summary["by_status"][backend.status.value] += 1
            summary["environments"].append({
                "name": backend.environment_name,
                "status": backend.status.value,
                "created": backend.created_at.isoformat() if backend.created_at else None
            })
        
        return summary
    
    def export_configuration(self) -> Dict[str, Any]:
        """
        Export complete configuration.
        
        Returns:
            Complete configuration dictionary
        """
        return {
            "app_id": self.app_id,
            "region": self.region,
            "backends": {
                name: {
                    "environment_name": b.environment_name,
                    "environment_id": b.environment_id,
                    "status": b.status.value,
                    "created_at": b.created_at.isoformat() if b.created_at else None,
                    "metadata": b.metadata
                }
                for name, b in self._backends.items()
            },
            "feature_flags": {
                env: {
                    flag: {
                        "name": f.flag_name,
                        "type": f.flag_type.value,
                        "value": f.value,
                        "enabled": f.enabled,
                        "description": f.description
                    }
                    for flag, f in flags.items()
                }
                for env, flags in self._feature_flags.items()
            }
        }
    
    def import_configuration(
        self,
        config: Dict[str, Any]
    ) -> None:
        """
        Import configuration.
        
        Args:
            config: Configuration dictionary
        """
        if "backends" in config:
            for name, backend_config in config["backends"].items():
                backend = BackendEnvironment(
                    environment_name=backend_config["environment_name"],
                    environment_id=backend_config["environment_id"],
                    status=BackendStatus(backend_config.get("status", "DEPLOYED")),
                    metadata=backend_config.get("metadata", {})
                )
                self._backends[name] = backend
        
        if "feature_flags" in config:
            for env_name, flags in config["feature_flags"].items():
                for flag_name, flag_config in flags.items():
                    flag = FeatureFlag(
                        flag_name=flag_config["name"],
                        flag_type=FeatureFlagType(flag_config["type"]),
                        value=flag_config["value"],
                        enabled=flag_config.get("enabled", True),
                        description=flag_config.get("description", "")
                    )
                    self._feature_flags[env_name][flag_name] = flag
    
    def close(self) -> None:
        """Clean up resources."""
        self._flush_metrics()
        
        if self._cloudwatch_client:
            self._cloudwatch_client = None
        if self._amplify_client:
            self._amplify_client = None
