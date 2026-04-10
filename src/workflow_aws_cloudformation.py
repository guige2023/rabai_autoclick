"""
AWS CloudFormation Integration Module for Workflow System

Implements a CloudFormationIntegration class with:
1. Stack management: Create/manage CloudFormation stacks
2. Stack sets: Manage stack sets
3. Drift detection: Detect configuration drift
4. Change sets: Preview stack changes
5. Resource providers: Custom resource providers
6. Stack policies: Configure stack policies
7. Outputs: Manage stack outputs
8. Imports/Exports: Cross-stack references
9. Template management: Validate/estimate costs
10. CloudWatch integration: Stack events and alarms

Commit: 'feat(aws-cloudformation): add AWS CloudFormation with stack management, stack sets, drift detection, change sets, resource providers, stack policies, outputs, imports/exports, templates, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
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


class StackStatus(Enum):
    """CloudFormation stack status values."""
    CREATE_IN_PROGRESS = "CREATE_IN_PROGRESS"
    CREATE_FAILED = "CREATE_FAILED"
    CREATE_COMPLETE = "CREATE_COMPLETE"
    ROLLBACK_IN_PROGRESS = "ROLLBACK_IN_PROGRESS"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"
    ROLLBACK_COMPLETE = "ROLLBACK_COMPLETE"
    DELETE_IN_PROGRESS = "DELETE_IN_PROGRESS"
    DELETE_FAILED = "DELETE_FAILED"
    DELETE_COMPLETE = "DELETE_COMPLETE"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"
    UPDATE_COMPLETE = "UPDATE_COMPLETE"
    UPDATE_FAILED = "UPDATE_FAILED"
    UPDATE_ROLLBACK_IN_PROGRESS = "UPDATE_ROLLBACK_IN_PROGRESS"
    UPDATE_ROLLBACK_FAILED = "UPDATE_ROLLBACK_FAILED"
    UPDATE_ROLLBACK_COMPLETE = "UPDATE_ROLLBACK_COMPLETE"
    REVIEW_IN_PROGRESS = "REVIEW_IN_PROGRESS"
    IMPORT_IN_PROGRESS = "IMPORT_IN_PROGRESS"
    IMPORT_COMPLETE = "IMPORT_COMPLETE"
    IMPORT_ROLLBACK_IN_PROGRESS = "IMPORT_ROLLBACK_IN_PROGRESS"
    IMPORT_ROLLBACK_FAILED = "IMPORT_ROLLBACK_FAILED"
    IMPORT_ROLLBACK_COMPLETE = "IMPORT_ROLLBACK_COMPLETE"


class DriftDetectionStatus(Enum):
    """Stack drift detection status."""
    DETECTION_IN_PROGRESS = "DETECTION_IN_PROGRESS"
    DETECTION_FAILED = "DETECTION_FAILED"
    DETECTION_COMPLETE = "DETECTION_COMPLETE"
    DRIFTED = "DRIFTED"
    IN_SYNC = "IN_SYNC"


class ResourceSignalStatus(Enum):
    """Resource signal status for wait conditions."""
    SIGNALS_RECEIVED = "SIGNALS_RECEIVED"
    SIGNALS_NOT_RECEIVED = "SIGNALS_NOT_RECEIVED"


class ChangeSetType(Enum):
    """Change set types."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    IMPORT = "IMPORT"


class ChangeSetStatus(Enum):
    """Change set status values."""
    CREATE_PENDING = "CREATE_PENDING"
    CREATE_IN_PROGRESS = "CREATE_IN_PROGRESS"
    CREATE_COMPLETE = "CREATE_COMPLETE"
    DELETE_COMPLETE = "DELETE_COMPLETE"
    DELETE_FAILED = "DELETE_FAILED"
    FAILED = "FAILED"


class StackSetOperationStatus(Enum):
    """Stack set operation status."""
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class PermissionModel(Enum):
    """Stack set permission models."""
    SERVICE_MANAGED = "SERVICE_MANAGED"
    SELF_MANAGED = "SELF_MANAGED"


class Capability(Enum):
    """CloudFormation capabilities."""
    CAPABILITY_IAM = "CAPABILITY_IAM"
    CAPABILITY_NAMED_IAM = "CAPABILITY_NAMED_IAM"
    CAPABILITY_AUTO_EXPAND = "CAPABILITY_AUTO_EXPAND"


class OnFailure(Enum):
    """Stack creation on failure behavior."""
    ROLLBACK = "ROLLBACK"
    DELETE = "DELETE"
    DO_NOTHING = "DO_NOTHING"


@dataclass
class CloudFormationConfig:
    """Configuration for CloudFormation connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class StackConfig:
    """Configuration for creating a CloudFormation stack."""
    stack_name: str
    template_body: Optional[str] = None
    template_url: Optional[str] = None
    parameters: Dict[str, str] = field(default_factory=dict)
    disable_rollback: bool = False
    on_failure: Optional[Union[OnFailure, str]] = None
    notification_arns: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    timeout_in_minutes: Optional[int] = None
    capabilities: List[Union[Capability, str]] = field(default_factory=list)
    resource_types: List[str] = field(default_factory=list)
    role_arn: Optional[str] = None
    rollback_configuration: Optional[Dict[str, Any]] = None


@dataclass
class StackInfo:
    """Information about a CloudFormation stack."""
    stack_id: str
    stack_name: str
    stack_status: str
    stack_status_reason: Optional[str] = None
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    enable_termination_protection: bool = False
    drift_info: Optional[Dict[str, Any]] = None
    parent_id: Optional[str] = None
    root_id: Optional[str] = None
    creation_time: Optional[str] = None
    last_updated_time: Optional[str] = None
    notification_arns: List[str] = field(default_factory=list)
    timeout_in_minutes: Optional[int] = None
    role_arn: Optional[str] = None


@dataclass
class ChangeSetInfo:
    """Information about a change set."""
    change_set_id: str
    change_set_name: str
    stack_id: str
    stack_name: str
    change_set_type: str
    status: str
    status_reason: Optional[str] = None
    execution_status: str = "UNAVAILABLE"
    changes: List[Dict[str, Any]] = field(default_factory=list)
    notifications: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    template_body: Optional[str] = None


@dataclass
class StackResourceInfo:
    """Information about a stack resource."""
    logical_resource_id: str
    physical_resource_id: Optional[str] = None
    resource_type: str = ""
    last_updated_timestamp: Optional[str] = None
    resource_status: Optional[str] = None
    status_reason: Optional[str] = None
    drift_information: Optional[Dict[str, Any]] = None


@dataclass
class StackEventInfo:
    """Information about a stack event."""
    event_id: str
    stack_name: str
    stack_id: str
    logical_resource_id: str
    physical_resource_id: Optional[str] = None
    resource_type: str = ""
    timestamp: Optional[str] = None
    resource_status: Optional[str] = None
    status_reason: Optional[str] = None
    resource_properties: Optional[str] = None


@dataclass
class DriftDetectionResult:
    """Result of drift detection."""
    stack_drift_status: str
    last_check_timestamp: Optional[str] = None
    total_checks: int = 0
    drifted_stacks: int = 0
    in_sync_stacks: int = 0
    check_results: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StackSetConfig:
    """Configuration for creating a stack set."""
    stack_set_name: str
    description: str = ""
    template_body: Optional[str] = None
    template_url: Optional[str] = None
    parameters: Dict[str, str] = field(default_factory=dict)
    permission_model: Union[PermissionModel, str] = PermissionModel.SELF_MANAGED
    administration_role_arn: Optional[str] = None
    execution_role_name: str = "AWSCloudFormationStackSetExecutionRole"
    auto_deployment: Optional[Dict[str, Any]] = None
    capabilities: List[Union[Capability, str]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    stack_instances_group: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StackSetInfo:
    """Information about a stack set."""
    stack_set_id: str
    stack_set_name: str
    description: str = ""
    status: str = ""
    template_body: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    permission_model: Optional[str] = None
    administration_role_arn: Optional[str] = None
    execution_role_name: Optional[str] = None
    auto_deployment: Optional[Dict[str, Any]] = None


@dataclass
class StackPolicyConfig:
    """Configuration for stack policy."""
    stack_name: str
    stack_policy_body: Optional[str] = None
    stack_policy_url: Optional[str] = None
    stack_policy_during_update_body: Optional[str] = None
    stack_policy_during_update_url: Optional[str] = None


@dataclass
class TemplateEstimate:
    """Template cost estimate."""
    template_body: Optional[str] = None
    template_url: Optional[str] = None
    resources: List[Dict[str, Any]] = field(default_factory=list)
    total_estimated_cost: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


@dataclass
class CustomResourceConfig:
    """Configuration for custom resource provider."""
    type_name: str
    schema: Dict[str, Any]
    logging_config: Optional[Dict[str, str]] = None
    execution_role_arn: Optional[str] = None


@dataclass
class ExportInfo:
    """Information about a stack export."""
    exporting_stack_id: str
    exporting_stack_name: str
    name: str
    value: str
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ImportInfo:
    """Information about an import operation."""
    import_id: str
    import_status: str
    parent_stacks: List[str] = field(default_factory=list)
    imported_resources: int = 0


class CloudFormationIntegration:
    """
    AWS CloudFormation integration class for infrastructure as code.
    
    Supports:
    - Stack creation, update, deletion, and management
    - Stack set management for multi-account/multi-region deployments
    - Drift detection to identify out-of-band changes
    - Change sets for previewing stack modifications
    - Custom resource providers for extending CloudFormation
    - Stack policies for protection
    - Cross-stack imports and exports
    - Template validation and cost estimation
    - CloudWatch integration for monitoring
    
    Attributes:
        region_name: AWS region name
        profile_name: AWS profile name (optional)
        endpoint_url: Custom endpoint URL (optional)
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize CloudFormation integration.
        
        Args:
            region_name: AWS region for CloudFormation operations
            profile_name: AWS credentials profile name
            endpoint_url: Custom CloudFormation endpoint URL
        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self._clients = {}
        self._resources = {}
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize boto3 clients for CloudFormation services."""
        try:
            session_kwargs = {"region_name": self.region_name}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # CloudFormation client
            cf_kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                cf_kwargs["endpoint_url"] = self.endpoint_url
            self._clients["cloudformation"] = session.client("cloudformation", **cf_kwargs)
            
            # CloudWatch client for events and alarms
            self._clients["cloudwatch"] = session.client("cloudwatch", region_name=self.region_name)
            
            # CloudWatch Logs client
            self._clients["logs"] = session.client("logs", region_name=self.region_name)
            
            # CloudWatch Events/EventBridge client
            self._clients["events"] = session.client("events", region_name=self.region_name)
            
            # S3 client for templates
            self._clients["s3"] = session.client("s3", region_name=self.region_name)
            
            # IAM client for roles
            self._clients["iam"] = session.client("iam", region_name=self.region_name)
            
            logger.info(f"CloudFormation clients initialized for region {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize CloudFormation clients: {e}")
    
    @property
    def cloudformation(self):
        """Get CloudFormation client."""
        return self._clients.get("cloudformation")
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        return self._clients.get("cloudwatch")
    
    @property
    def logs(self):
        """Get CloudWatch Logs client."""
        return self._clients.get("logs")
    
    @property
    def events(self):
        """Get CloudWatch Events client."""
        return self._clients.get("events")
    
    # =========================================================================
    # STACK MANAGEMENT
    # =========================================================================
    
    def create_stack(self, config: StackConfig) -> StackInfo:
        """
        Create a new CloudFormation stack.
        
        Args:
            config: StackConfig with stack settings
        
        Returns:
            StackInfo object with created stack details
        
        Example:
            >>> config = StackConfig(
            ...     stack_name="my-stack",
            ...     template_body='{"AWSTemplateFormatVersion":"2010-09-09"}',
            ...     parameters={"Param1": "Value1"},
            ...     tags={"Environment": "Production"}
            ... )
            >>> stack = self.create_stack(config)
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackName": config.stack_name}
        
        if config.template_body:
            kwargs["TemplateBody"] = config.template_body
        if config.template_url:
            kwargs["TemplateURL"] = config.template_url
        
        if config.parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in config.parameters.items()
            ]
        
        if config.disable_rollback:
            kwargs["DisableRollback"] = True
        
        if config.on_failure:
            kwargs["OnFailure"] = config.on_failure.value if isinstance(config.on_failure, OnFailure) else config.on_failure
        
        if config.notification_arns:
            kwargs["NotificationARNs"] = config.notification_arns
        
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
        
        if config.timeout_in_minutes:
            kwargs["TimeoutInMinutes"] = config.timeout_in_minutes
        
        if config.capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in config.capabilities
            ]
        
        if config.resource_types:
            kwargs["ResourceTypes"] = config.resource_types
        
        if config.role_arn:
            kwargs["RoleARN"] = config.role_arn
        
        if config.rollback_configuration:
            kwargs["RollbackConfiguration"] = config.rollback_configuration
        
        try:
            response = self.cloudformation.create_stack(**kwargs)
            stack_id = response["StackId"]
            
            # Wait for stack creation to complete
            stack_info = self._wait_for_stack(stack_id)
            logger.info(f"Created stack: {config.stack_name}")
            return stack_info
        except ClientError as e:
            logger.error(f"Failed to create stack {config.stack_name}: {e}")
            raise
    
    def update_stack(
        self,
        stack_name: str,
        template_body: Optional[str] = None,
        template_url: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        capabilities: Optional[List[Union[Capability, str]]] = None,
        role_arn: Optional[str] = None,
        stack_policy_during_update_body: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> StackInfo:
        """
        Update an existing CloudFormation stack.
        
        Args:
            stack_name: Name or stack ID
            template_body: New template body
            template_url: New template URL
            parameters: Parameter overrides
            capabilities: IAM capability flags
            role_arn: Execution role ARN
            stack_policy_during_update_body: Temporary stack policy during update
            tags: New tags
        
        Returns:
            StackInfo object with updated stack details
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackName": stack_name}
        
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        if parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameters.items()
            ]
        
        if capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in capabilities
            ]
        
        if role_arn:
            kwargs["RoleARN"] = role_arn
        
        if stack_policy_during_update_body:
            kwargs["StackPolicyDuringUpdateBody"] = stack_policy_during_update_body
        
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        try:
            response = self.cloudformation.update_stack(**kwargs)
            stack_id = response["StackId"]
            
            stack_info = self._wait_for_stack(stack_id)
            logger.info(f"Updated stack: {stack_name}")
            return stack_info
        except ClientError as e:
            if "No updates are to be performed" in str(e):
                return self.get_stack(stack_name)
            logger.error(f"Failed to update stack {stack_name}: {e}")
            raise
    
    def delete_stack(self, stack_name: str, retain_resources: List[str] = None) -> bool:
        """
        Delete a CloudFormation stack.
        
        Args:
            stack_name: Name or stack ID
            retain_resources: Resources to retain (don't delete)
        
        Returns:
            True if deletion successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackName": stack_name}
        
        if retain_resources:
            kwargs["RetainResources"] = retain_resources
        
        try:
            self.cloudformation.delete_stack(**kwargs)
            
            # Wait for deletion to complete
            self._wait_for_stack_deletion(stack_name)
            logger.info(f"Deleted stack: {stack_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete stack {stack_name}: {e}")
            raise
    
    def get_stack(self, stack_name: str) -> Optional[StackInfo]:
        """
        Get information about a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            StackInfo object or None if not found
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.describe_stacks(StackName=stack_name)
            stacks = response.get("Stacks", [])
            
            if not stacks:
                return None
            
            return self._parse_stack_info(stacks[0])
        except ClientError as e:
            if "does not exist" in str(e):
                return None
            logger.error(f"Failed to get stack {stack_name}: {e}")
            raise
    
    def list_stacks(
        self,
        status_filter: Optional[List[StackStatus]] = None
    ) -> List[StackInfo]:
        """
        List CloudFormation stacks.
        
        Args:
            status_filter: Filter by stack statuses
        
        Returns:
            List of StackInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if status_filter:
            kwargs["StackStatusFilter"] = [s.value for s in status_filter]
        
        all_stacks = []
        try:
            paginator = self.cloudformation.get_paginator("list_stacks")
            for page in paginator.paginate(**kwargs):
                for stack_data in page.get("StackSummaries", []):
                    if "ParentId" not in stack_data:  # Skip nested stacks
                        info = StackInfo(
                            stack_id=stack_data["StackId"],
                            stack_name=stack_data["StackName"],
                            stack_status=stack_data["StackStatus"],
                            stack_status_reason=stack_data.get("StackStatusReason")
                        )
                        all_stacks.append(info)
            
            return all_stacks
        except ClientError as e:
            logger.error(f"Failed to list stacks: {e}")
            raise
    
    def describe_stack_events(
        self,
        stack_name: str,
        limit: int = 100
    ) -> List[StackEventInfo]:
        """
        Get stack events.
        
        Args:
            stack_name: Name or stack ID
            limit: Maximum number of events
        
        Returns:
            List of StackEventInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            kwargs = {"StackName": stack_name}
            events = []
            
            paginator = self.cloudformation.get_paginator("describe_stack_events")
            for page in paginator.paginate(**kwargs):
                for event in page.get("StackEvents", []):
                    events.append(StackEventInfo(
                        event_id=event["EventId"],
                        stack_name=event["StackName"],
                        stack_id=event["StackId"],
                        logical_resource_id=event["LogicalResourceId"],
                        physical_resource_id=event.get("PhysicalResourceId"),
                        resource_type=event["ResourceType"],
                        timestamp=event.get("Timestamp"),
                        resource_status=event.get("ResourceStatus"),
                        status_reason=event.get("ResourceStatusReason"),
                        resource_properties=event.get("ResourceProperties")
                    ))
                    if len(events) >= limit:
                        break
                if len(events) >= limit:
                    break
            
            return events
        except ClientError as e:
            logger.error(f"Failed to describe stack events: {e}")
            raise
    
    def list_stack_resources(self, stack_name: str) -> List[StackResourceInfo]:
        """
        List resources in a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            List of StackResourceInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            resources = []
            paginator = self.cloudformation.get_paginator("list_stack_resources")
            for page in paginator.paginate(StackName=stack_name):
                for resource in page.get("StackResourceSummaries", []):
                    resources.append(StackResourceInfo(
                        logical_resource_id=resource["LogicalResourceId"],
                        physical_resource_id=resource.get("PhysicalResourceId"),
                        resource_type=resource["ResourceType"],
                        last_updated_timestamp=resource.get("LastUpdatedTimestamp"),
                        resource_status=resource.get("ResourceStatus"),
                        status_reason=resource.get("ResourceStatusReason"),
                        drift_information=resource.get("DriftInformation")
                    ))
            
            return resources
        except ClientError as e:
            logger.error(f"Failed to list stack resources: {e}")
            raise
    
    def _wait_for_stack(self, stack_id: str, timeout: int = 1800) -> StackInfo:
        """Wait for stack operation to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = self.cloudformation.describe_stacks(StackName=stack_id)
            stacks = response.get("Stacks", [])
            
            if stacks:
                stack_data = stacks[0]
                status = stack_data["StackStatus"]
                
                if status.endswith("_COMPLETE"):
                    return self._parse_stack_info(stack_data)
                elif status.endswith("_FAILED") or status.endswith("_ROLLBACK"):
                    raise RuntimeError(f"Stack operation failed: {status}")
            
            time.sleep(5)
        
        raise TimeoutError(f"Timeout waiting for stack {stack_id}")
    
    def _wait_for_stack_deletion(self, stack_name: str, timeout: int = 1800):
        """Wait for stack deletion to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = self.cloudformation.describe_stacks(StackName=stack_name)
                stacks = response.get("Stacks", [])
                
                if not stacks:
                    return
                
                status = stacks[0]["StackStatus"]
                if status == "DELETE_FAILED":
                    raise RuntimeError("Stack deletion failed")
            except ClientError as e:
                if "does not exist" in str(e):
                    return
            
            time.sleep(5)
        
        raise TimeoutError(f"Timeout waiting for stack deletion: {stack_name}")
    
    def _parse_stack_info(self, stack_data: Dict[str, Any]) -> StackInfo:
        """Parse stack data into StackInfo object."""
        return StackInfo(
            stack_id=stack_data["StackId"],
            stack_name=stack_data["StackName"],
            stack_status=stack_data["StackStatus"],
            stack_status_reason=stack_data.get("StackStatusReason"),
            description=stack_data.get("Description"),
            parameters=stack_data.get("Parameters", []),
            outputs=stack_data.get("Outputs", []),
            tags={t["Key"]: t["Value"] for t in stack_data.get("Tags", [])},
            enable_termination_protection=stack_data.get("EnableTerminationProtection", False),
            drift_info=stack_data.get("DriftInformation"),
            parent_id=stack_data.get("ParentId"),
            root_id=stack_data.get("RootId"),
            creation_time=stack_data.get("CreationTime"),
            last_updated_time=stack_data.get("LastUpdatedTime"),
            notification_arns=stack_data.get("NotificationARNs", []),
            timeout_in_minutes=stack_data.get("TimeoutInMinutes"),
            role_arn=stack_data.get("RoleARN")
        )
    
    # =========================================================================
    # STACK SETS
    # =========================================================================
    
    def create_stack_set(self, config: StackSetConfig) -> StackSetInfo:
        """
        Create a CloudFormation stack set.
        
        Args:
            config: StackSetConfig with stack set settings
        
        Returns:
            StackSetInfo object with created stack set details
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "StackSetName": config.stack_set_name,
            "Description": config.description
        }
        
        if config.template_body:
            kwargs["TemplateBody"] = config.template_body
        if config.template_url:
            kwargs["TemplateURL"] = config.template_url
        
        if config.parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in config.parameters.items()
            ]
        
        permission_model = config.permission_model.value if isinstance(config.permission_model, PermissionModel) else config.permission_model
        kwargs["PermissionModel"] = permission_model
        
        if config.administration_role_arn:
            kwargs["AdministrationRoleARN"] = config.administration_role_arn
        
        if config.execution_role_name:
            kwargs["ExecutionRoleName"] = config.execution_role_name
        
        if config.auto_deployment:
            kwargs["AutoDeployment"] = config.auto_deployment
        
        if config.capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in config.capabilities
            ]
        
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
        
        try:
            response = self.cloudformation.create_stack_set(**kwargs)
            stack_set_id = response["StackSetId"]
            
            # Get full details
            info = self.get_stack_set(config.stack_set_name)
            logger.info(f"Created stack set: {config.stack_set_name}")
            return info
        except ClientError as e:
            logger.error(f"Failed to create stack set {config.stack_set_name}: {e}")
            raise
    
    def update_stack_set(
        self,
        stack_set_name: str,
        template_body: Optional[str] = None,
        template_url: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        capabilities: Optional[List[Union[Capability, str]]] = None,
        execution_role_name: Optional[str] = None,
        administration_role_arn: Optional[str] = None
    ) -> StackSetInfo:
        """
        Update a CloudFormation stack set.
        
        Args:
            stack_set_name: Name or stack set ID
            template_body: New template body
            template_url: New template URL
            parameters: Parameter overrides
            capabilities: IAM capability flags
            execution_role_name: Execution role name
            administration_role_arn: Administration role ARN
        
        Returns:
            StackSetInfo object with updated stack set details
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackSetName": stack_set_name}
        
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        if parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameters.items()
            ]
        
        if capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in capabilities
            ]
        
        if execution_role_name:
            kwargs["ExecutionRoleName"] = execution_role_name
        
        if administration_role_arn:
            kwargs["AdministrationRoleARN"] = administration_role_arn
        
        try:
            self.cloudformation.update_stack_set(**kwargs)
            
            info = self.get_stack_set(stack_set_name)
            logger.info(f"Updated stack set: {stack_set_name}")
            return info
        except ClientError as e:
            logger.error(f"Failed to update stack set {stack_set_name}: {e}")
            raise
    
    def delete_stack_set(self, stack_set_name: str) -> bool:
        """
        Delete a CloudFormation stack set.
        
        Args:
            stack_set_name: Name or stack set ID
        
        Returns:
            True if deletion successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            self.cloudformation.delete_stack_set(StackSetName=stack_set_name)
            logger.info(f"Deleted stack set: {stack_set_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete stack set {stack_set_name}: {e}")
            raise
    
    def get_stack_set(self, stack_set_name: str) -> Optional[StackSetInfo]:
        """
        Get information about a stack set.
        
        Args:
            stack_set_name: Name or stack set ID
        
        Returns:
            StackSetInfo object or None if not found
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.describe_stack_set(StackSetName=stack_set_name)
            stack_set = response.get("StackSet", {})
            
            return StackSetInfo(
                stack_set_id=stack_set["StackSetId"],
                stack_set_name=stack_set["StackSetName"],
                description=stack_set.get("Description", ""),
                status=stack_set.get("StackSetStatus", ""),
                template_body=stack_set.get("TemplateBody"),
                parameters=stack_set.get("Parameters", []),
                capabilities=stack_set.get("Capabilities", []),
                tags={t["Key"]: t["Value"] for t in stack_set.get("Tags", [])},
                permission_model=stack_set.get("PermissionModel"),
                administration_role_arn=stack_set.get("AdministrationRoleARN"),
                execution_role_name=stack_set.get("ExecutionRoleName"),
                auto_deployment=stack_set.get("AutoDeployment")
            )
        except ClientError as e:
            if "does not exist" in str(e):
                return None
            logger.error(f"Failed to get stack set {stack_set_name}: {e}")
            raise
    
    def list_stack_sets(self, status_filter: str = None) -> List[StackSetInfo]:
        """
        List CloudFormation stack sets.
        
        Args:
            status_filter: Filter by status (ACTIVE, DELETED)
        
        Returns:
            List of StackSetInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if status_filter:
            kwargs["Status"] = status_filter
        
        all_stack_sets = []
        try:
            paginator = self.cloudformation.get_paginator("list_stack_sets")
            for page in paginator.paginate(**kwargs):
                for ss in page.get("Summaries", []):
                    all_stack_sets.append(StackSetInfo(
                        stack_set_id=ss["StackSetId"],
                        stack_set_name=ss["StackSetName"],
                        description=ss.get("Description", ""),
                        status=ss.get("Status", "")
                    ))
            
            return all_stack_sets
        except ClientError as e:
            logger.error(f"Failed to list stack sets: {e}")
            raise
    
    def create_stack_instances(
        self,
        stack_set_name: str,
        accounts: List[str],
        regions: List[str],
        parameter_overrides: Dict[str, str] = None,
        operation_preferences: Dict[str, Any] = None
    ) -> str:
        """
        Create stack instances in target accounts and regions.
        
        Args:
            stack_set_name: Name or stack set ID
            accounts: Target AWS account IDs
            regions: Target AWS regions
            parameter_overrides: Parameter overrides
            operation_preferences: Operation preferences
        
        Returns:
            Operation ID
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "StackSetName": stack_set_name,
            "Accounts": accounts,
            "Regions": regions
        }
        
        if parameter_overrides:
            kwargs["ParameterOverrides"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameter_overrides.items()
            ]
        
        if operation_preferences:
            kwargs["OperationPreferences"] = operation_preferences
        
        try:
            response = self.cloudformation.create_stack_instances(**kwargs)
            operation_id = response["OperationId"]
            logger.info(f"Created stack instances for {stack_set_name}")
            return operation_id
        except ClientError as e:
            logger.error(f"Failed to create stack instances: {e}")
            raise
    
    def delete_stack_instances(
        self,
        stack_set_name: str,
        accounts: List[str],
        regions: List[str],
        operation_preferences: Dict[str, Any] = None,
        retain_stacks: bool = False
    ) -> str:
        """
        Delete stack instances from target accounts and regions.
        
        Args:
            stack_set_name: Name or stack set ID
            accounts: Target AWS account IDs
            regions: Target AWS regions
            operation_preferences: Operation preferences
            retain_stacks: Whether to retain stacks
        
        Returns:
            Operation ID
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "StackSetName": stack_set_name,
            "Accounts": accounts,
            "Regions": regions,
            "RetainStacks": retain_stacks
        }
        
        if operation_preferences:
            kwargs["OperationPreferences"] = operation_preferences
        
        try:
            response = self.cloudformation.delete_stack_instances(**kwargs)
            operation_id = response["OperationId"]
            logger.info(f"Deleted stack instances from {stack_set_name}")
            return operation_id
        except ClientError as e:
            logger.error(f"Failed to delete stack instances: {e}")
            raise
    
    def list_stack_instances(
        self,
        stack_set_name: str,
        filters: Dict[str, str] = None
    ) -> List[Dict[str, Any]]:
        """
        List stack instances for a stack set.
        
        Args:
            stack_set_name: Name or stack set ID
            filters: Filters for filtering results
        
        Returns:
            List of stack instance summaries
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackSetName": stack_set_name}
        if filters:
            kwargs["Filters"] = filters
        
        instances = []
        try:
            paginator = self.cloudformation.get_paginator("list_stack_instances")
            for page in paginator.paginate(**kwargs):
                instances.extend(page.get("Summaries", []))
            
            return instances
        except ClientError as e:
            logger.error(f"Failed to list stack instances: {e}")
            raise
    
    # =========================================================================
    # DRIFT DETECTION
    # =========================================================================
    
    def detect_drift(self, stack_name: str) -> str:
        """
        Initiate drift detection on a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            Drift detection ID
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.detect_stack_drift(StackName=stack_name)
            drift_id = response["StackDriftDetectionId"]
            logger.info(f"Initiated drift detection for stack: {stack_name}")
            return drift_id
        except ClientError as e:
            logger.error(f"Failed to detect drift for {stack_name}: {e}")
            raise
    
    def detect_drift_stack_set(self, stack_set_name: str) -> str:
        """
        Initiate drift detection on a stack set.
        
        Args:
            stack_set_name: Name or stack set ID
        
        Returns:
            Operation ID
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.detect_stack_set_drift(StackSetName=stack_set_name)
            operation_id = response["OperationId"]
            logger.info(f"Initiated drift detection for stack set: {stack_set_name}")
            return operation_id
        except ClientError as e:
            logger.error(f"Failed to detect drift for stack set {stack_set_name}: {e}")
            raise
    
    def get_drift_detection_status(self, stack_name: str) -> Dict[str, Any]:
        """
        Get drift detection status for a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            Drift detection status details
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.describe_stack_drift_detection_status(StackName=stack_name)
            return response
        except ClientError as e:
            logger.error(f"Failed to get drift detection status: {e}")
            raise
    
    def get_resource_drift(self, stack_name: str, logical_resource_id: str) -> Dict[str, Any]:
        """
        Get drift information for a specific resource.
        
        Args:
            stack_name: Name or stack ID
            logical_resource_id: Logical resource ID
        
        Returns:
            Resource drift information
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.describe_stack_resource_drift(
                StackName=stack_name,
                LogicalResourceId=logical_resource_id
            )
            return response.get("StackResourceDrift", {})
        except ClientError as e:
            logger.error(f"Failed to get resource drift: {e}")
            raise
    
    def list_drifted_stacks(self) -> List[StackInfo]:
        """
        List all stacks with drift detected.
        
        Returns:
            List of StackInfo objects with drift
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        all_stacks = []
        try:
            paginator = self.cloudformation.get_paginator("list_stacks")
            for page in paginator.paginate():
                for stack_summary in page.get("StackSummaries", []):
                    if stack_summary["StackStatus"] not in ["DELETE_COMPLETE", "IMPORT_ROLLBACK_COMPLETE"]:
                        drift_info = self.cloudformation.describe_stacks(
                            StackName=stack_summary["StackId"]
                        ).get("Stacks", [{}])[0].get("DriftInformation", {})
                        
                        if drift_info.get("StackDriftStatus") == "DRIFTED":
                            all_stacks.append(StackInfo(
                                stack_id=stack_summary["StackId"],
                                stack_name=stack_summary["StackName"],
                                stack_status=stack_summary["StackStatus"],
                                drift_info=drift_info
                            ))
            
            return all_stacks
        except ClientError as e:
            logger.error(f"Failed to list drifted stacks: {e}")
            raise
    
    # =========================================================================
    # CHANGE SETS
    # =========================================================================
    
    def create_change_set(
        self,
        stack_name: str,
        change_set_name: str,
        template_body: Optional[str] = None,
        template_url: Optional[str] = None,
        parameters: Dict[str, str] = None,
        change_set_type: Union[ChangeSetType, str] = ChangeSetType.UPDATE,
        capabilities: List[Union[Capability, str]] = None,
        description: str = None
    ) -> ChangeSetInfo:
        """
        Create a change set for previewing stack changes.
        
        Args:
            stack_name: Name or stack ID for the target stack
            change_set_name: Name for the change set
            template_body: Template body
            template_url: Template URL
            parameters: Parameter overrides
            change_set_type: CREATE, UPDATE, or IMPORT
            capabilities: IAM capability flags
            description: Change set description
        
        Returns:
            ChangeSetInfo object with change set details
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "StackName": stack_name,
            "ChangeSetName": change_set_name
        }
        
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        if parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameters.items()
            ]
        
        change_type = change_set_type.value if isinstance(change_set_type, ChangeSetType) else change_set_type
        kwargs["ChangeSetType"] = change_type
        
        if capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in capabilities
            ]
        
        if description:
            kwargs["Description"] = description
        
        try:
            response = self.cloudformation.create_change_set(**kwargs)
            
            # Wait for change set creation
            cs_info = self._wait_for_change_set(response["Id"])
            logger.info(f"Created change set: {change_set_name}")
            return cs_info
        except ClientError as e:
            logger.error(f"Failed to create change set {change_set_name}: {e}")
            raise
    
    def _wait_for_change_set(self, change_set_id: str, timeout: int = 600) -> ChangeSetInfo:
        """Wait for change set creation to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = self.cloudformation.describe_change_set(ChangeSetName=change_set_id)
            status = response["Status"]
            
            if status in [ChangeSetStatus.CREATE_COMPLETE.value, ChangeSetStatus.FAILED.value]:
                return ChangeSetInfo(
                    change_set_id=change_set_id,
                    change_set_name=response["ChangeSetName"],
                    stack_id=response["StackId"],
                    stack_name=response["StackName"],
                    change_set_type=response.get("ChangeSetType", "UPDATE"),
                    status=status,
                    status_reason=response.get("StatusReason"),
                    execution_status=response.get("ExecutionStatus", "UNAVAILABLE"),
                    changes=response.get("Changes", []),
                    notifications=response.get("Notifications", []),
                    capabilities=response.get("Capabilities", []),
                    parameters=response.get("Parameters", []),
                    template_body=response.get("TemplateBody")
                )
            
            time.sleep(3)
        
        raise TimeoutError(f"Timeout waiting for change set {change_set_id}")
    
    def describe_change_set(self, change_set_name: str) -> Optional[ChangeSetInfo]:
        """
        Get information about a change set.
        
        Args:
            change_set_name: Name or change set ID
        
        Returns:
            ChangeSetInfo object or None if not found
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.describe_change_set(ChangeSetName=change_set_name)
            
            return ChangeSetInfo(
                change_set_id=response["ChangeSetId"],
                change_set_name=response["ChangeSetName"],
                stack_id=response["StackId"],
                stack_name=response["StackName"],
                change_set_type=response.get("ChangeSetType", "UPDATE"),
                status=response["Status"],
                status_reason=response.get("StatusReason"),
                execution_status=response.get("ExecutionStatus", "UNAVAILABLE"),
                changes=response.get("Changes", []),
                notifications=response.get("Notifications", []),
                capabilities=response.get("Capabilities", []),
                parameters=response.get("Parameters", []),
                template_body=response.get("TemplateBody")
            )
        except ClientError as e:
            if "does not exist" in str(e):
                return None
            logger.error(f"Failed to describe change set {change_set_name}: {e}")
            raise
    
    def execute_change_set(self, change_set_name: str) -> bool:
        """
        Execute a change set.
        
        Args:
            change_set_name: Name or change set ID
        
        Returns:
            True if execution successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            self.cloudformation.execute_change_set(ChangeSetName=change_set_name)
            logger.info(f"Executed change set: {change_set_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to execute change set {change_set_name}: {e}")
            raise
    
    def delete_change_set(self, change_set_name: str) -> bool:
        """
        Delete a change set.
        
        Args:
            change_set_name: Name or change set ID
        
        Returns:
            True if deletion successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            self.cloudformation.delete_change_set(ChangeSetName=change_set_name)
            logger.info(f"Deleted change set: {change_set_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete change set {change_set_name}: {e}")
            raise
    
    def list_change_sets(self, stack_name: str) -> List[ChangeSetInfo]:
        """
        List change sets for a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            List of ChangeSetInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        change_sets = []
        try:
            paginator = self.cloudformation.get_paginator("list_change_sets")
            for page in paginator.paginate(StackName=stack_name):
                for cs in page.get("Summaries", []):
                    change_sets.append(ChangeSetInfo(
                        change_set_id=cs["ChangeSetId"],
                        change_set_name=cs["ChangeSetName"],
                        stack_id=cs["StackId"],
                        stack_name=cs["StackName"],
                        change_set_type=cs.get("ChangeSetType", "UPDATE"),
                        status=cs["Status"],
                        status_reason=cs.get("StatusReason"),
                        execution_status=cs.get("ExecutionStatus", "UNAVAILABLE")
                    ))
            
            return change_sets
        except ClientError as e:
            logger.error(f"Failed to list change sets for {stack_name}: {e}")
            raise
    
    # =========================================================================
    # RESOURCE PROVIDERS
    # =========================================================================
    
    def register_type(
        self,
        type_name: str,
        schema: Dict[str, Any],
        logging_config: Dict[str, str] = None,
        execution_role_arn: str = None
    ) -> str:
        """
        Register a resource type for custom resource provider.
        
        Args:
            type_name: Resource type name (e.g., Custom::MyResource)
            schema: Resource schema
            logging_config: Logging configuration
            execution_role_arn: Execution role ARN
        
        Returns:
            Registration token
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "TypeName": type_name,
            "Schema": schema
        }
        
        if logging_config:
            kwargs["LoggingConfig"] = logging_config
        
        if execution_role_arn:
            kwargs["ExecutionRoleArn"] = execution_role_arn
        
        try:
            response = self.cloudformation.register_type(**kwargs)
            logger.info(f"Registered resource type: {type_name}")
            return response["RegistrationToken"]
        except ClientError as e:
            logger.error(f"Failed to register type {type_name}: {e}")
            raise
    
    def deregister_type(self, type_name: str, version: str = None) -> bool:
        """
        Deregister a resource type.
        
        Args:
            type_name: Resource type name
            version: Specific version to deregister (None for all)
        
        Returns:
            True if deregistration successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"TypeName": type_name}
        if version:
            kwargs["TypeVersionArn"] = version
        
        try:
            self.cloudformation.deregister_type(**kwargs)
            logger.info(f"Deregistered resource type: {type_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to deregister type {type_name}: {e}")
            raise
    
    def describe_type(
        self,
        type_name: str,
        type_: str = "RESOURCE",
        version_id: str = None
    ) -> Dict[str, Any]:
        """
        Get information about a registered type.
        
        Args:
            type_name: Type name
            type_: RESOURCE, MODULE, or HOOK
            version_id: Specific version ARN
        
        Returns:
            Type information
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "TypeName": type_name,
            "Type": type_
        }
        if version_id:
            kwargs["VersionArn"] = version_id
        
        try:
            return self.cloudformation.describe_type(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to describe type {type_name}: {e}")
            raise
    
    def list_registered_types(
        self,
        type_name_prefix: str = None,
        type_: str = "RESOURCE",
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List registered resource types.
        
        Args:
            type_name_prefix: Filter by type name prefix
            type_: RESOURCE, MODULE, or HOOK
            max_results: Maximum results
        
        Returns:
            List of registered types
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"Type": type_, "MaxResults": max_results}
        if type_name_prefix:
            kwargs["TypeNamePrefix"] = type_name_prefix
        
        types = []
        try:
            paginator = self.cloudformation.get_paginator("list_types")
            for page in paginator.paginate(**kwargs):
                types.extend(page.get("TypeSummaries", []))
            
            return types
        except ClientError as e:
            logger.error(f"Failed to list registered types: {e}")
            raise
    
    # =========================================================================
    # STACK POLICIES
    # =========================================================================
    
    def set_stack_policy(
        self,
        stack_name: str,
        stack_policy_body: str = None,
        stack_policy_url: str = None
    ) -> bool:
        """
        Set a stack policy.
        
        Args:
            stack_name: Name or stack ID
            stack_policy_body: Stack policy body
            stack_policy_url: Stack policy URL
        
        Returns:
            True if successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {"StackName": stack_name}
        if stack_policy_body:
            kwargs["StackPolicyBody"] = stack_policy_body
        if stack_policy_url:
            kwargs["StackPolicyURL"] = stack_policy_url
        
        try:
            self.cloudformation.set_stack_policy(**kwargs)
            logger.info(f"Set stack policy for: {stack_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to set stack policy: {e}")
            raise
    
    def get_stack_policy(self, stack_name: str) -> Optional[str]:
        """
        Get stack policy for a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            Stack policy body or None
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.get_stack_policy(StackName=stack_name)
            return response.get("StackPolicyBody")
        except ClientError as e:
            logger.error(f"Failed to get stack policy: {e}")
            raise
    
    def create_stack_policy_body(self, statements: List[Dict[str, Any]]) -> str:
        """
        Create a stack policy body from statements.
        
        Args:
            statements: Policy statements
        
        Returns:
            Stack policy JSON body
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": statements
        }
        return json.dumps(policy)
    
    def create_protection_statement(
        self,
        effect: str = "Allow",
        principals: List[str] = None,
        actions: List[str] = None,
        not_actions: List[str] = None,
        resources: List[str] = None,
        condition: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Create a stack protection statement.
        
        Args:
            effect: Allow or Deny
            principals: AWS principals
            actions: Allowed/denied actions
            not_actions: Excluded actions
            resources: Affected resources
            condition: Condition expression
        
        Returns:
            Policy statement
        """
        statement = {"Effect": effect}
        
        if principals:
            statement["Principal"] = principals if len(principals) > 1 else principals[0]
        
        if actions:
            statement["Action"] = actions if len(actions) > 1 else actions[0]
        
        if not_actions:
            statement["NotAction"] = not_actions if len(not_actions) > 1 else not_actions[0]
        
        if resources:
            statement["Resource"] = resources if len(resources) > 1 else resources[0]
        
        if condition:
            statement["Condition"] = condition
        
        return statement
    
    # =========================================================================
    # OUTPUTS
    # =========================================================================
    
    def get_stack_outputs(self, stack_name: str) -> Dict[str, str]:
        """
        Get stack outputs as a dictionary.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            Dictionary mapping output keys to values
        """
        stack_info = self.get_stack(stack_name)
        if not stack_info:
            return {}
        
        return {output["OutputKey"]: output["OutputValue"] for output in stack_info.outputs}
    
    def get_output_value(self, stack_name: str, output_key: str) -> Optional[str]:
        """
        Get a specific output value from a stack.
        
        Args:
            stack_name: Name or stack ID
            output_key: Output key
        
        Returns:
            Output value or None
        """
        outputs = self.get_stack_outputs(stack_name)
        return outputs.get(output_key)
    
    def list_exports(self, prefix: str = None) -> List[ExportInfo]:
        """
        List stack exports.
        
        Args:
            prefix: Filter by export name prefix
        
        Returns:
            List of ExportInfo objects
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if prefix:
            kwargs["Prefix"] = prefix
        
        exports = []
        try:
            paginator = self.cloudformation.get_paginator("list_exports")
            for page in paginator.paginate(**kwargs):
                for exp in page.get("Exports", []):
                    exports.append(ExportInfo(
                        exporting_stack_id=exp["ExportingStackId"],
                        exporting_stack_name=exp.get("ExportingStackName", ""),
                        name=exp["Name"],
                        value=exp["Value"],
                        tags={}
                    ))
            
            return exports
        except ClientError as e:
            logger.error(f"Failed to list exports: {e}")
            raise
    
    # =========================================================================
    # IMPORTS/EXPORTS (Cross-stack references)
    # =========================================================================
    
    def import_stacks(
        self,
        stack_name: str,
        template_body: str,
        parameters: Dict[str, str],
        resources_to_import: List[Dict[str, str]],
        capabilities: List[Union[Capability, str]] = None
    ) -> StackInfo:
        """
        Import resources into a new stack.
        
        Args:
            stack_name: Name for the new stack
            template_body: Template body
            parameters: Stack parameters
            resources_to_import: Resources to import [{LogicalId, ResourceType, Identifier}]
            capabilities: IAM capability flags
        
        Returns:
            StackInfo object
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {
            "StackName": stack_name,
            "TemplateBody": template_body,
            "Parameters": [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameters.items()
            ],
            "ResourcesToImport": resources_to_import
        }
        
        if capabilities:
            kwargs["Capabilities"] = [
                c.value if isinstance(c, Capability) else c
                for c in capabilities
            ]
        
        try:
            self.cloudformation.create_change_set(**kwargs)
            
            # Wait for import to complete
            response = self.cloudformation.describe_stacks(StackName=stack_name)
            stack_data = response["Stacks"][0]
            
            if stack_data["StackStatus"] == "IMPORT_COMPLETE":
                return self._parse_stack_info(stack_data)
            else:
                raise RuntimeError(f"Import failed with status: {stack_data['StackStatus']}")
        except ClientError as e:
            logger.error(f"Failed to import stack {stack_name}: {e}")
            raise
    
    def create_export(self, stack_name: str, export_name: str, export_value: str) -> bool:
        """
        Create an export using Fn::Sub in stack outputs.
        
        Args:
            stack_name: Name of the stack with the export
            export_name: Name for the export
            export_value: Value to export
        
        Returns:
            True if successful
        """
        pass  # Exports are created via stack outputs
    
    def get_export(self, export_name: str) -> Optional[ExportInfo]:
        """
        Get an export by name.
        
        Args:
            export_name: Export name
        
        Returns:
            ExportInfo or None
        """
        exports = self.list_exports()
        for exp in exports:
            if exp.name == export_name:
                return exp
        return None
    
    # =========================================================================
    # TEMPLATE MANAGEMENT
    # =========================================================================
    
    def validate_template(
        self,
        template_body: str = None,
        template_url: str = None
    ) -> Dict[str, Any]:
        """
        Validate a CloudFormation template.
        
        Args:
            template_body: Template body
            template_url: Template URL
        
        Returns:
            Validation result
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        try:
            return self.cloudformation.validate_template(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to validate template: {e}")
            raise
    
    def estimate_template_cost(
        self,
        template_body: str = None,
        template_url: str = None,
        parameters: Dict[str, str] = None
    ) -> TemplateEstimate:
        """
        Estimate cost for a template.
        
        Args:
            template_body: Template body
            template_url: Template URL
            parameters: Template parameters
        
        Returns:
            TemplateEstimate object
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        if parameters:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in parameters.items()
            ]
        
        try:
            response = self.cloudformation.estimate_template_cost(**kwargs)
            
            return TemplateEstimate(
                template_body=template_body,
                template_url=template_url,
                total_estimated_cost=response.get("Url")
            )
        except ClientError as e:
            logger.error(f"Failed to estimate template cost: {e}")
            raise
    
    def get_template(self, stack_name: str, stage: str = "Original") -> Optional[str]:
        """
        Get stack template.
        
        Args:
            stack_name: Name or stack ID
            stage: Stages: Original, Processed, or Definition
    
        Returns:
            Template body
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            response = self.cloudformation.get_template(
                StackName=stack_name,
                TemplateStage=stage
            )
            return response.get("TemplateBody")
        except ClientError as e:
            logger.error(f"Failed to get template for {stack_name}: {e}")
            raise
    
    def get_template_summary(
        self,
        template_body: str = None,
        template_url: str = None
    ) -> Dict[str, Any]:
        """
        Get template summary with resource types.
        
        Args:
            template_body: Template body
            template_url: Template URL
        
        Returns:
            Template summary
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        kwargs = {}
        if template_body:
            kwargs["TemplateBody"] = template_body
        if template_url:
            kwargs["TemplateURL"] = template_url
        
        try:
            return self.cloudformation.get_template_summary(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to get template summary: {e}")
            raise
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def create_stack_alarm(
        self,
        stack_name: str,
        alarm_name: str,
        metric_name: str,
        namespace: str = "AWS/CloudFormation",
        threshold: float = 1.0,
        comparison_operator: str = "LessThanThreshold",
        period: int = 60,
        evaluation_periods: int = 1,
        statistic: str = "Average",
        alarm_actions: List[str] = None,
        treat_missing_data: str = "missing"
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for stack events.
        
        Args:
            stack_name: Name or stack ID
            alarm_name: Alarm name
            metric_name: Metric name
            namespace: Metric namespace
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods
            statistic: Statistic type
            alarm_actions: Alarm action ARNs
            treat_missing_data: How to treat missing data
        
        Returns:
            CloudWatch response
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        dimensions = [
            {"Name": "StackName", "Value": stack_name}
        ]
        
        kwargs = {
            "AlarmName": alarm_name,
            "MetricName": metric_name,
            "Namespace": namespace,
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "Period": period,
            "EvaluationPeriods": evaluation_periods,
            "Statistic": statistic,
            "TreatMissingData": treat_missing_data,
            "Dimensions": dimensions
        }
        
        if alarm_actions:
            kwargs["AlarmActions"] = alarm_actions
        
        try:
            response = self.cloudwatch.put_metric_alarm(**kwargs)
            logger.info(f"Created alarm {alarm_name} for stack {stack_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def create_stack_events_rule(
        self,
        stack_name: str,
        rule_name: str,
        target_arn: str = None,
        event_pattern: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Create CloudWatch Events rule for stack events.
        
        Args:
            stack_name: Stack name to create event rule for
            rule_name: Rule name
            target_arn: Target ARN (e.g., SNS topic)
            event_pattern: Custom event pattern
        
        Returns:
            CloudWatch Events response
        """
        if not self.events:
            raise RuntimeError("CloudWatch Events client not initialized")
        
        if event_pattern is None:
            event_pattern = {
                "source": ["aws.cloudformation"],
                "detail": {
                    "stack-name": [stack_name]
                }
            }
        
        kwargs = {
            "Name": rule_name,
            "EventPattern": json.dumps(event_pattern),
            "State": "ENABLED"
        }
        
        try:
            response = self.events.put_rule(**kwargs)
            
            if target_arn:
                self.events.put_targets(
                    Rule=rule_name,
                    Targets=[{"Id": "1", "Arn": target_arn}]
                )
            
            logger.info(f"Created CloudWatch Events rule {rule_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create events rule: {e}")
            raise
    
    def create_stack_log_group(
        self,
        stack_name: str,
        retention_days: int = 30
    ) -> str:
        """
        Create CloudWatch log group for stack.
        
        Args:
            stack_name: Stack name
            retention_days: Log retention days
        
        Returns:
            Log group name
        """
        if not self.logs:
            raise RuntimeError("CloudWatch Logs client not initialized")
        
        log_group_name = f"/aws/cloudformation/stack/{stack_name}"
        
        try:
            self.logs.create_log_group(logGroupName=log_group_name)
            self.logs.put_retention_policy(
                logGroupName=log_group_name,
                RetentionInDays=retention_days
            )
            logger.info(f"Created log group for stack {stack_name}")
            return log_group_name
        except ClientError as e:
            if "ResourceAlreadyExists" in str(e):
                return log_group_name
            logger.error(f"Failed to create log group: {e}")
            raise
    
    def get_stack_metrics(
        self,
        stack_name: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a stack.
        
        Args:
            stack_name: Stack name
            start_time: Start time
            end_time: End time
        
        Returns:
            Metric data results
        """
        if not self.cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")
        
        metric_queries = [
            {
                "Id": "create_count",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/CloudFormation",
                        "MetricName": "CreateStackSuccess",
                        "Dimensions": [{"Name": "StackName", "Value": stack_name}]
                    },
                    "Period": 60,
                    "Stat": "Sum"
                }
            },
            {
                "Id": "delete_count",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/CloudFormation",
                        "MetricName": "DeleteStackSuccess",
                        "Dimensions": [{"Name": "StackName", "Value": stack_name}]
                    },
                    "Period": 60,
                    "Stat": "Sum"
                }
            },
            {
                "Id": "update_count",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/CloudFormation",
                        "MetricName": "UpdateStackSuccess",
                        "Dimensions": [{"Name": "StackName", "Value": stack_name}]
                    },
                    "Period": 60,
                    "Stat": "Sum"
                }
            }
        ]
        
        try:
            return self.cloudwatch.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time.isoformat(),
                EndTime=end_time.isoformat()
            )
        except ClientError as e:
            logger.error(f"Failed to get stack metrics: {e}")
            raise
    
    def enable_stack_termination_protection(self, stack_name: str) -> bool:
        """
        Enable termination protection for a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            True if successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            self.cloudformation.update_termination_protection(
                StackName=stack_name,
                EnableTerminationProtection=True
            )
            logger.info(f"Enabled termination protection for {stack_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to enable termination protection: {e}")
            raise
    
    def disable_stack_termination_protection(self, stack_name: str) -> bool:
        """
        Disable termination protection for a stack.
        
        Args:
            stack_name: Name or stack ID
        
        Returns:
            True if successful
        """
        if not self.cloudformation:
            raise RuntimeError("CloudFormation client not initialized")
        
        try:
            self.cloudformation.update_termination_protection(
                StackName=stack_name,
                EnableTerminationProtection=False
            )
            logger.info(f"Disabled termination protection for {stack_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to disable termination protection: {e}")
            raise
