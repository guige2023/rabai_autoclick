"""
AWS CodePipeline Integration Module for Workflow System

Implements a CodePipelineIntegration class with:
1. Pipeline management: Create/manage pipelines
2. Stage management: Add/manage stages
3. Action management: Configure actions
4. Execution: Start/examine executions
5. Approval: Manual approval actions
6. Webhook: GitHub webhooks
7. Encryption: Configure encryption
8. Artifact store: Manage artifact storage
9. CloudWatch integration: Events integration
10. Custom actions: Custom action types

Commit: 'feat(aws-codepipeline): add AWS CodePipeline integration with pipeline management, stages, actions, executions, approvals, webhooks, encryption, artifact store, CloudWatch, custom actions'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
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


class PipelineStatus(Enum):
    """CodePipeline status states."""
    CREATING = "Creating"
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    DELETED = "Deleted"


class StageStatus(Enum):
    """Stage execution status."""
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


class ActionStatus(Enum):
    """Action execution status."""
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    PENDING = "Pending"


class ActionOwner(Enum):
    """Action owner types."""
    AWS = "AWS"
    THIRD_PARTY = "ThirdParty"
    CUSTOM = "Custom"


class ActionCategory(Enum):
    """Action categories."""
    SOURCE = "Source"
    BUILD = "Build"
    DEPLOY = "Deploy"
    TEST = "Test"
    INVOKE = "Invoke"
    APPROVAL = "Approval"
    QUALITY = "Quality"
    MONITORING = "Monitoring"


class StageTransitionType(Enum):
    """Stage transition types."""
    ENABLE = "Enable"
    DISABLE = "Disable"


class WebhookFilterType(Enum):
    """Webhook filter types."""
    REGEX = "Regex"
    JSON_PATH = "JsonPath"
    X_PATH = "XPath"


class EncryptionStatus(Enum):
    """Encryption status."""
    ENABLED = "Enabled"
    DISABLED = "Disabled"
    UNKNOWN = "Unknown"


@dataclass
class Pipeline:
    """Represents a CodePipeline."""
    name: str
    role_arn: str
    artifact_store: Dict[str, Any]
    stages: List[Dict[str, Any]] = field(default_factory=list)
    version: int = 1
    status: PipelineStatus = PipelineStatus.CREATING
    created: Optional[datetime] = None
    updated: Optional[datetime] = None


@dataclass
class Stage:
    """Represents a pipeline stage."""
    name: str
    actions: List[Dict[str, Any]] = field(default_factory=list)
    status: StageStatus = StageStatus.IN_PROGRESS
    enabled: bool = True


@dataclass
class Action:
    """Represents a pipeline action."""
    name: str
    category: ActionCategory
    owner: ActionOwner
    provider: str
    version: str = "1"
    configuration: Dict[str, Any] = field(default_factory=dict)
    input_artifacts: List[str] = field(default_factory=list)
    output_artifacts: List[str] = field(default_factory=list)
    role_arn: Optional[str] = None
    status: ActionStatus = ActionStatus.PENDING


@dataclass
class Execution:
    """Represents a pipeline execution."""
    pipeline_execution_id: str
    pipeline_name: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    stages: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ApprovalDetails:
    """Manual approval action details."""
    approval_id: str
    approved: bool = False
    approver_name: Optional[str] = None
    comment: Optional[str] = None
    approved_at: Optional[datetime] = None


@dataclass
class WebhookDefinition:
    """GitHub webhook definition."""
    url: str
    secret: Optional[str] = None
    filters: List[Dict[str, Any]] = field(default_factory=list)
    enabled: bool = True


@dataclass
class ArtifactStoreConfig:
    """Artifact store configuration."""
    type: str = "S3"
    location: str = ""
    encryption_key: Optional[str] = None


@dataclass
class CustomActionType:
    """Custom action type definition."""
    category: ActionCategory
    provider: str
    version: str
    settings: Dict[str, Any] = field(default_factory=dict)
    configuration_properties: List[Dict[str, Any]] = field(default_factory=list)


class CodePipelineIntegration:
    """
    AWS CodePipeline Integration for workflow automation.

    Provides comprehensive management of AWS CodePipeline resources including:
    - Pipeline lifecycle management (create, update, delete, start)
    - Stage configuration and transitions
    - Action management with various providers
    - Execution monitoring and control
    - Manual approval workflows
    - GitHub webhook integration
    - Encryption configuration
    - Artifact store management
    - CloudWatch Events integration
    - Custom action types
    """

    def __init__(
        self,
        region: str = "us-east-1",
        profile: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize CodePipeline integration.

        Args:
            region: AWS region
            profile: AWS profile name
            config: Additional boto3 configuration
        """
        self.region = region
        self.profile = profile
        self.config = config or {}

        self._client = None
        self._resource_group_cache = {}
        self._lock = threading.RLock()

        if BOTO3_AVAILABLE:
            self._initialize_client()

    def _initialize_client(self):
        """Initialize boto3 client with configuration."""
        session_kwargs = {"region_name": self.region}
        if self.profile:
            session_kwargs["profile_name"] = self.profile

        session = boto3.Session(**session_kwargs)

        client_kwargs = {}
        if self.config:
            client_kwargs.update(self.config)

        self._client = session.client("codepipeline", **client_kwargs)
        self._s3_client = session.client("s3")
        self._iam_client = session.client("iam")
        self._events_client = session.client("events")
        self._cw_client = session.client("cloudwatch")

    @property
    def client(self):
        """Get boto3 client."""
        return self._client

    def _ensure_client(self):
        """Ensure boto3 client is initialized."""
        if not self._client:
            self._initialize_client()

    def _generate_id(self) -> str:
        """Generate unique ID."""
        return str(uuid.uuid4())

    def _format_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Format API response."""
        return {
            "success": True,
            "data": response,
            "timestamp": datetime.utcnow().isoformat()
        }

    def _handle_error(self, error: Exception) -> Dict[str, Any]:
        """Handle boto3 errors."""
        logger.error(f"CodePipeline error: {error}")
        return {
            "success": False,
            "error": str(error),
            "error_type": type(error).__name__
        }

    # =========================================================================
    # PIPELINE MANAGEMENT
    # =========================================================================

    def create_pipeline(
        self,
        name: str,
        role_arn: str,
        artifact_store: ArtifactStoreConfig,
        stages: List[Dict[str, Any]],
        tags: Optional[List[Dict[str, str]]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new CodePipeline.

        Args:
            name: Pipeline name
            role_arn: IAM role ARN for pipeline execution
            artifact_store: Artifact store configuration
            stages: List of stage configurations
            tags: Optional tags
            kwargs: Additional parameters

        Returns:
            Response with pipeline details
        """
        try:
            self._ensure_client()

            pipeline_definition = {
                "name": name,
                "roleArn": role_arn,
                "artifactStore": {
                    "type": artifact_store.type,
                    "location": artifact_store.location
                },
                "stages": stages
            }

            if artifact_store.encryption_key:
                pipeline_definition["artifactStore"]["encryptionKey"] = {
                    "id": artifact_store.encryption_key,
                    "type": "KMS"
                }

            params = {"pipeline": pipeline_definition}

            if tags:
                params["tags"] = tags

            response = self._client.create_pipeline(**params)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "tags": response.get("tags", [])
            })

        except Exception as e:
            return self._handle_error(e)

    def get_pipeline(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get pipeline details.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Pipeline details
        """
        try:
            self._ensure_client()
            response = self._client.get_pipeline(name=name)
            return self._format_response(response.get("pipeline", {}))

        except Exception as e:
            return self._handle_error(e)

    def list_pipelines(
        self,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List all pipelines.

        Args:
            kwargs: Additional parameters

        Returns:
            List of pipelines
        """
        try:
            self._ensure_client()
            response = self._client.list_pipelines()
            return self._format_response({
                "pipelines": response.get("pipelines", [])
            })

        except Exception as e:
            return self._handle_error(e)

    def update_pipeline(
        self,
        pipeline: Dict[str, Any],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update an existing pipeline.

        Args:
            pipeline: Updated pipeline definition
            kwargs: Additional parameters

        Returns:
            Updated pipeline details
        """
        try:
            self._ensure_client()
            response = self._client.update_pipeline(pipeline=pipeline)
            return self._format_response(response.get("pipeline", {}))

        except Exception as e:
            return self._handle_error(e)

    def delete_pipeline(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Delete a pipeline.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Deletion confirmation
        """
        try:
            self._ensure_client()
            self._client.delete_pipeline(name=name)
            return self._format_response({
                "message": f"Pipeline '{name}' deleted successfully"
            })

        except Exception as e:
            return self._handle_error(e)

    def disable_pipeline(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Disable a pipeline.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Disabled confirmation
        """
        try:
            self._ensure_client()
            self._client.disable_stage_transition(
                pipelineName=name,
                stageName="Source",
                transitionType="Inbound"
            )
            return self._format_response({
                "message": f"Pipeline '{name}' disabled successfully"
            })

        except Exception as e:
            return self._handle_error(e)

    def enable_pipeline(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enable a pipeline.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Enabled confirmation
        """
        try:
            self._ensure_client()
            self._client.enable_stage_transition(
                pipelineName=name,
                stageName="Source",
                transitionType="Inbound"
            )
            return self._format_response({
                "message": f"Pipeline '{name}' enabled successfully"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # STAGE MANAGEMENT
    # =========================================================================

    def add_stage(
        self,
        pipeline_name: str,
        stage: Dict[str, Any],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add a new stage to a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            stage: Stage configuration
            kwargs: Additional parameters

        Returns:
            Updated pipeline details
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            pipeline_definition = pipeline["pipeline"]

            stage_name = stage.get("name")
            existing_names = [s["name"] for s in pipeline_definition["stages"]]
            if stage_name in existing_names:
                raise ValueError(f"Stage '{stage_name}' already exists")

            pipeline_definition["stages"].append(stage)

            response = self._client.update_pipeline(pipeline=pipeline_definition)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "added_stage": stage_name
            })

        except Exception as e:
            return self._handle_error(e)

    def update_stage(
        self,
        pipeline_name: str,
        stage_name: str,
        stage: Dict[str, Any],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update a stage in a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            stage_name: Current stage name
            stage: Updated stage configuration
            kwargs: Additional parameters

        Returns:
            Updated pipeline details
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            pipeline_definition = pipeline["pipeline"]

            stage_names = [s["name"] for s in pipeline_definition["stages"]]
            if stage_name not in stage_names:
                raise ValueError(f"Stage '{stage_name}' not found")

            index = stage_names.index(stage_name)
            pipeline_definition["stages"][index] = stage

            response = self._client.update_pipeline(pipeline=pipeline_definition)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "updated_stage": stage_name
            })

        except Exception as e:
            return self._handle_error(e)

    def delete_stage(
        self,
        pipeline_name: str,
        stage_name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Delete a stage from a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            stage_name: Stage name to delete
            kwargs: Additional parameters

        Returns:
            Updated pipeline details
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            pipeline_definition = pipeline["pipeline"]

            stage_names = [s["name"] for s in pipeline_definition["stages"]]
            if stage_name not in stage_names:
                raise ValueError(f"Stage '{stage_name}' not found")

            pipeline_definition["stages"] = [
                s for s in pipeline_definition["stages"]
                if s["name"] != stage_name
            ]

            response = self._client.update_pipeline(pipeline=pipeline_definition)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "deleted_stage": stage_name
            })

        except Exception as e:
            return self._handle_error(e)

    def disable_stage_transition(
        self,
        pipeline_name: str,
        stage_name: str,
        transition_type: StageTransitionType = StageTransitionType.DISABLE,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Disable transition into a stage.

        Args:
            pipeline_name: Name of the pipeline
            stage_name: Stage name
            transition_type: Transition type (Inbound/Outbound)
            kwargs: Additional parameters

        Returns:
            Disabled confirmation
        """
        try:
            self._ensure_client()
            self._client.disable_stage_transition(
                pipelineName=pipeline_name,
                stageName=stage_name,
                transitionType=transition_type.value
            )
            return self._format_response({
                "message": f"Transition disabled for stage '{stage_name}'"
            })

        except Exception as e:
            return self._handle_error(e)

    def enable_stage_transition(
        self,
        pipeline_name: str,
        stage_name: str,
        transition_type: StageTransitionType = StageTransitionType.ENABLE,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enable transition into a stage.

        Args:
            pipeline_name: Name of the pipeline
            stage_name: Stage name
            transition_type: Transition type (Inbound/Outbound)
            kwargs: Additional parameters

        Returns:
            Enabled confirmation
        """
        try:
            self._ensure_client()
            self._client.enable_stage_transition(
                pipelineName=pipeline_name,
                stageName=stage_name,
                transitionType=transition_type.value
            )
            return self._format_response({
                "message": f"Transition enabled for stage '{stage_name}'"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # ACTION MANAGEMENT
    # =========================================================================

    def create_action(
        self,
        name: str,
        category: ActionCategory,
        owner: ActionOwner,
        provider: str,
        version: str = "1",
        configuration: Optional[Dict[str, Any]] = None,
        input_artifacts: Optional[List[str]] = None,
        output_artifacts: Optional[List[str]] = None,
        role_arn: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Action:
        """
        Create an action configuration.

        Args:
            name: Action name
            category: Action category
            owner: Action owner
            provider: Action provider
            version: Action version
            configuration: Provider-specific configuration
            input_artifacts: Input artifact names
            output_artifacts: Output artifact names
            role_arn: Optional role ARN

        Returns:
            Action configuration
        """
        action = Action(
            name=name,
            category=category,
            owner=owner,
            provider=provider,
            version=version,
            configuration=configuration or {},
            input_artifacts=input_artifacts or [],
            output_artifacts=output_artifacts or [],
            role_arn=role_arn
        )
        return action

    def configure_source_action(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        output_artifacts: List[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure a source action.

        Args:
            name: Action name
            provider: Source provider (e.g., CodeCommit, GitHub, S3)
            configuration: Provider-specific configuration
            output_artifacts: Output artifact names
            kwargs: Additional parameters

        Returns:
            Source action configuration
        """
        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.SOURCE.value,
                "owner": ActionOwner.AWS.value if provider in ["CodeCommit", "S3", "ECR"] else ActionOwner.THIRD_PARTY.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": configuration,
            "outputArtifacts": [{"name": a} for a in (output_artifacts or [])]
        }

    def configure_build_action(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        input_artifacts: Optional[List[str]] = None,
        output_artifacts: Optional[List[str]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure a build action.

        Args:
            name: Action name
            provider: Build provider (e.g., CodeBuild, Jenkins)
            configuration: Provider-specific configuration
            input_artifacts: Input artifact names
            output_artifacts: Output artifact names
            kwargs: Additional parameters

        Returns:
            Build action configuration
        """
        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.BUILD.value,
                "owner": ActionOwner.AWS.value if provider == "CodeBuild" else ActionOwner.THIRD_PARTY.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": configuration,
            "inputArtifacts": [{"name": a} for a in (input_artifacts or [])],
            "outputArtifacts": [{"name": a} for a in (output_artifacts or [])]
        }

    def configure_deploy_action(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        input_artifacts: Optional[List[str]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure a deploy action.

        Args:
            name: Action name
            provider: Deploy provider (e.g., CloudFormation, CodeDeploy, ECS, ElasticBeanstalk)
            configuration: Provider-specific configuration
            input_artifacts: Input artifact names
            kwargs: Additional parameters

        Returns:
            Deploy action configuration
        """
        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.DEPLOY.value,
                "owner": ActionOwner.AWS.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": configuration,
            "inputArtifacts": [{"name": a} for a in (input_artifacts or [])]
        }

    def configure_test_action(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        input_artifacts: Optional[List[str]] = None,
        output_artifacts: Optional[List[str]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure a test action.

        Args:
            name: Action name
            provider: Test provider (e.g., CodeBuild, Bugsnag)
            configuration: Provider-specific configuration
            input_artifacts: Input artifact names
            output_artifacts: Output artifact names
            kwargs: Additional parameters

        Returns:
            Test action configuration
        """
        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.TEST.value,
                "owner": ActionOwner.AWS.value if provider == "CodeBuild" else ActionOwner.THIRD_PARTY.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": configuration,
            "inputArtifacts": [{"name": a} for a in (input_artifacts or [])],
            "outputArtifacts": [{"name": a} for a in (output_artifacts or [])]
        }

    def configure_invoke_action(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        input_artifacts: Optional[List[str]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure an invoke action.

        Args:
            name: Action name
            provider: Invoke provider (e.g., Lambda, StepFunctions)
            configuration: Provider-specific configuration
            input_artifacts: Input artifact names
            kwargs: Additional parameters

        Returns:
            Invoke action configuration
        """
        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.INVOKE.value,
                "owner": ActionOwner.AWS.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": configuration,
            "inputArtifacts": [{"name": a} for a in (input_artifacts or [])]
        }

    # =========================================================================
    # APPROVAL ACTIONS
    # =========================================================================

    def configure_approval_action(
        self,
        name: str,
        provider: str = "Manual",
        configuration: Optional[Dict[str, Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure a manual approval action.

        Args:
            name: Action name
            provider: Approval provider (default: Manual)
            configuration: Approval configuration (e.g., CustomData, ExternalEntityLink)
            kwargs: Additional parameters

        Returns:
            Approval action configuration
        """
        config = configuration or {}
        approval_config = {
            "CustomData": config.get("custom_data", "Approval required"),
            "ExternalEntityLink": config.get("external_link", ""),
            "Approvers": config.get("approvers", [])
        }

        return {
            "name": name,
            "actionTypeId": {
                "category": ActionCategory.APPROVAL.value,
                "owner": ActionOwner.AWS.value,
                "provider": provider,
                "version": "1"
            },
            "configuration": approval_config
        }

    def get_approval_details(
        self,
        pipeline_name: str,
        stage_name: str,
        action_name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get approval action details.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name
            action_name: Action name
            kwargs: Additional parameters

        Returns:
            Approval details
        """
        try:
            self._ensure_client()
            response = self._client.get_approval_details(
                pipelineName=pipeline_name,
                stageName=stage_name,
                actionName=action_name
            )
            return self._format_response(response)

        except Exception as e:
            return self._handle_error(e)

    def put_approval_result(
        self,
        pipeline_name: str,
        stage_name: str,
        action_name: str,
        result: Dict[str, Any],
        token: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Submit approval result.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name
            action_name: Action name
            result: Approval result (approved: bool, summary: str)
            token: Approval token
            kwargs: Additional parameters

        Returns:
            Approval result confirmation
        """
        try:
            self._ensure_client()
            response = self._client.put_approval_result(
                pipelineName=pipeline_name,
                stageName=stage_name,
                actionName=action_name,
                result=result,
                token=token
            )
            return self._format_response(response)

        except Exception as e:
            return self._handle_error(e)

    def approve_execution(
        self,
        pipeline_name: str,
        stage_name: str,
        action_name: str,
        execution_id: str,
        comment: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Approve a pipeline execution.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name
            action_name: Action name
            execution_id: Execution ID
            comment: Optional approval comment
            kwargs: Additional parameters

        Returns:
            Approval confirmation
        """
        try:
            self._ensure_client()

            self._client.put_approval_result(
                pipelineName=pipeline_name,
                stageName=stage_name,
                actionName=action_name,
                result={
                    "approved": True,
                    "summary": comment or "Approved via API"
                },
                token=execution_id
            )

            return self._format_response({
                "message": "Execution approved",
                "pipeline": pipeline_name,
                "stage": stage_name,
                "action": action_name
            })

        except Exception as e:
            return self._handle_error(e)

    def reject_approval(
        self,
        pipeline_name: str,
        stage_name: str,
        action_name: str,
        execution_id: str,
        comment: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Reject a pipeline approval.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name
            action_name: Action name
            execution_id: Execution ID
            comment: Optional rejection comment
            kwargs: Additional parameters

        Returns:
            Rejection confirmation
        """
        try:
            self._ensure_client()

            self._client.put_approval_result(
                pipelineName=pipeline_name,
                stageName=stage_name,
                actionName=action_name,
                result={
                    "approved": False,
                    "summary": comment or "Rejected via API"
                },
                token=execution_id
            )

            return self._format_response({
                "message": "Execution rejected",
                "pipeline": pipeline_name,
                "stage": stage_name,
                "action": action_name
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # WEBHOOK MANAGEMENT (GitHub)
    # =========================================================================

    def create_github_webhook(
        self,
        pipeline_name: str,
        webhook_url: str,
        secret: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a GitHub webhook for a pipeline.

        Args:
            pipeline_name: Pipeline name
            webhook_url: Webhook URL
            secret: Webhook secret for validation
            filters: List of webhook filters
            kwargs: Additional parameters

        Returns:
            Webhook configuration
        """
        try:
            self._ensure_client()

            webhook_definition = {
                "url": webhook_url,
                "secret": secret,
                "filters": filters or [
                    {
                        "jsonPath": "$.ref",
                        "matchEquals": "refs/heads/{Branch}"
                    }
                ]
            }

            response = self._client.put_webhook(
                pipelineName=pipeline_name,
                webhook=webhook_definition
            )

            return self._format_response({
                "webhook": response.get("webhook", {})
            })

        except Exception as e:
            return self._handle_error(e)

    def register_github_webhook(
        self,
        pipeline_name: str,
        git_hub_configuration: Dict[str, Any],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Register a GitHub webhook for a pipeline source action.

        Args:
            pipeline_name: Pipeline name
            git_hub_configuration: GitHub configuration
            kwargs: Additional parameters

        Returns:
            Registration confirmation
        """
        try:
            self._ensure_client()

            self._client.put_webhook(
                pipelineName=pipeline_name,
                webhook={
                    "name": f"{pipeline_name}-webhook",
                    "configuration": git_hub_configuration,
                    "rules": [
                        {
                            "jsonPath": "$.ref",
                            "matchEquals": "refs/heads/{Branch}"
                        }
                    ],
                    "filters": [
                        {
                            "jsonPath": "$.action",
                            "matchEquals": "push"
                        }
                    ]
                }
            )

            return self._format_response({
                "message": f"GitHub webhook registered for pipeline '{pipeline_name}'"
            })

        except Exception as e:
            return self._handle_error(e)

    def delete_webhook(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Delete a webhook.

        Args:
            name: Webhook name
            kwargs: Additional parameters

        Returns:
            Deletion confirmation
        """
        try:
            self._ensure_client()
            self._client.delete_webhook(name=name)
            return self._format_response({
                "message": f"Webhook '{name}' deleted successfully"
            })

        except Exception as e:
            return self._handle_error(e)

    def list_webhooks(
        self,
        pipeline_name: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List webhooks.

        Args:
            pipeline_name: Optional pipeline name filter
            kwargs: Additional parameters

        Returns:
            List of webhooks
        """
        try:
            self._ensure_client()

            if pipeline_name:
                response = self._client.list_webhooks(
                    pipelineName=pipeline_name
                )
            else:
                response = self._client.list_webhooks()

            return self._format_response({
                "webhooks": response.get("webhooks", [])
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # ENCRYPTION
    # =========================================================================

    def configure_encryption(
        self,
        pipeline_name: str,
        encryption_key_id: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure encryption for a pipeline's artifact store.

        Args:
            pipeline_name: Pipeline name
            encryption_key_id: KMS encryption key ID
            kwargs: Additional parameters

        Returns:
            Updated pipeline with encryption
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            pipeline_definition = pipeline["pipeline"]

            pipeline_definition["artifactStore"]["encryptionKey"] = {
                "id": encryption_key_id,
                "type": "KMS"
            }

            response = self._client.update_pipeline(pipeline=pipeline_definition)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "encryption_key": encryption_key_id
            })

        except Exception as e:
            return self._handle_error(e)

    def get_encryption_status(
        self,
        pipeline_name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get encryption status for a pipeline.

        Args:
            pipeline_name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Encryption status
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            artifact_store = pipeline["pipeline"].get("artifactStore", {})

            encryption_key = artifact_store.get("encryptionKey")
            status = EncryptionStatus.ENABLED if encryption_key else EncryptionStatus.DISABLED

            return self._format_response({
                "pipeline": pipeline_name,
                "encryption_status": status.value,
                "encryption_key": encryption_key
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # ARTIFACT STORE MANAGEMENT
    # =========================================================================

    def configure_artifact_store(
        self,
        pipeline_name: str,
        store_type: str,
        location: str,
        encryption_key_id: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure artifact store for a pipeline.

        Args:
            pipeline_name: Pipeline name
            store_type: Store type (S3)
            location: Bucket location
            encryption_key_id: Optional KMS key ID
            kwargs: Additional parameters

        Returns:
            Updated pipeline with artifact store
        """
        try:
            self._ensure_client()

            pipeline = self._client.get_pipeline(name=pipeline_name)
            pipeline_definition = pipeline["pipeline"]

            pipeline_definition["artifactStore"] = {
                "type": store_type,
                "location": location
            }

            if encryption_key_id:
                pipeline_definition["artifactStore"]["encryptionKey"] = {
                    "id": encryption_key_id,
                    "type": "KMS"
                }

            response = self._client.update_pipeline(pipeline=pipeline_definition)

            return self._format_response({
                "pipeline": response.get("pipeline", {}),
                "artifact_store": pipeline_definition["artifactStore"]
            })

        except Exception as e:
            return self._handle_error(e)

    def create_artifact_bucket(
        self,
        bucket_name: str,
        region: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create an S3 bucket for pipeline artifacts.

        Args:
            bucket_name: Bucket name
            region: Optional region (defaults to instance region)
            kwargs: Additional parameters

        Returns:
            Bucket creation confirmation
        """
        try:
            self._ensure_client()

            config = {}
            if region and region != self.region:
                config["LocationConstraint"] = region

            if config:
                self._s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration=config
                )
            else:
                self._s3_client.create_bucket(Bucket=bucket_name)

            return self._format_response({
                "bucket": bucket_name,
                "region": region or self.region
            })

        except Exception as e:
            return self._handle_error(e)

    def configure_bucket_policy(
        self,
        bucket_name: str,
        pipeline_role_arn: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Configure bucket policy for pipeline artifacts.

        Args:
            bucket_name: Bucket name
            pipeline_role_arn: Pipeline IAM role ARN
            kwargs: Additional parameters

        Returns:
            Policy configuration confirmation
        """
        try:
            self._ensure_client()

            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowPipelineRole",
                        "Effect": "Allow",
                        "Principal": {"AWS": pipeline_role_arn},
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:GetObjectVersion"
                        ],
                        "Resource": f"arn:aws:s3:::{bucket_name}/*"
                    }
                ]
            }

            self._s3_client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(policy)
            )

            return self._format_response({
                "bucket": bucket_name,
                "policy": "configured"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # EXECUTION MANAGEMENT
    # =========================================================================

    def start_pipeline(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Start a pipeline execution.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Execution details
        """
        try:
            self._ensure_client()
            response = self._client.start_pipeline_execution(name=name)
            return self._format_response({
                "pipeline_execution_id": response.get("pipelineExecutionId"),
                "pipeline_name": name
            })

        except Exception as e:
            return self._handle_error(e)

    def stop_pipeline(
        self,
        name: str,
        execution_id: str,
        abandon: bool = False,
        reason: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Stop a pipeline execution.

        Args:
            name: Pipeline name
            execution_id: Execution ID
            abandon: Whether to abandon execution
            reason: Stop reason
            kwargs: Additional parameters

        Returns:
            Stop confirmation
        """
        try:
            self._ensure_client()

            params = {
                "pipelineName": name,
                "pipelineExecutionId": execution_id
            }

            if abandon:
                params["abandon"] = True
            if reason:
                params["reason"] = reason

            self._client.stop_pipeline_execution(**params)

            return self._format_response({
                "message": "Pipeline execution stopped",
                "pipeline_name": name,
                "execution_id": execution_id,
                "abandoned": abandon
            })

        except Exception as e:
            return self._handle_error(e)

    def get_execution(
        self,
        pipeline_name: str,
        execution_id: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get execution details.

        Args:
            pipeline_name: Pipeline name
            execution_id: Execution ID
            kwargs: Additional parameters

        Returns:
            Execution details
        """
        try:
            self._ensure_client()
            response = self._client.get_pipeline_execution(
                pipelineName=pipeline_name,
                pipelineExecutionId=execution_id
            )
            return self._format_response(response.get("pipelineExecution", {}))

        except Exception as e:
            return self._handle_error(e)

    def list_executions(
        self,
        pipeline_name: str,
        max_results: int = 100,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List pipeline executions.

        Args:
            pipeline_name: Pipeline name
            max_results: Maximum results to return
            kwargs: Additional parameters

        Returns:
            List of executions
        """
        try:
            self._ensure_client()
            response = self._client.list_pipeline_executions(
                pipelineName=pipeline_name,
                maxResults=max_results
            )
            return self._format_response({
                "pipeline_executions": response.get("pipelineExecutions", [])
            })

        except Exception as e:
            return self._handle_error(e)

    def get_execution_history(
        self,
        pipeline_name: str,
        execution_id: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get execution history.

        Args:
            pipeline_name: Pipeline name
            execution_id: Execution ID
            kwargs: Additional parameters

        Returns:
            Execution history
        """
        try:
            self._ensure_client()
            response = self._client.get_pipeline_execution(
                pipelineName=pipeline_name,
                pipelineExecutionId=execution_id
            )
            return self._format_response(response)

        except Exception as e:
            return self._handle_error(e)

    def poll_for_approval(
        self,
        pipeline_name: str,
        stage_name: str,
        action_name: str,
        timeout_seconds: int = 300,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Poll for manual approval.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name
            action_name: Action name
            timeout_seconds: Polling timeout
            kwargs: Additional parameters

        Returns:
            Approval token if approved
        """
        try:
            self._ensure_client()

            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                try:
                    response = self._client.get_pipeline_state(
                        name=pipeline_name
                    )

                    for stage in response.get("stageStates", []):
                        if stage["stageName"] == stage_name:
                            for action in stage.get("actionStates", []):
                                if action["actionName"] == action_name:
                                    latest_execution = action.get("latestExecution", {})
                                    if latest_execution.get("status") == "InProgress":
                                        token = latest_execution.get("token")
                                        if token:
                                            return self._format_response({
                                                "token": token,
                                                "status": "pending"
                                            })
                                    elif latest_execution.get("status") == "Approved":
                                        return self._format_response({
                                            "status": "approved"
                                        })
                                    elif latest_execution.get("status") == "Rejected":
                                        return self._format_response({
                                            "status": "rejected"
                                        })
                except ClientError as e:
                    if e.response["Error"]["Code"] != "ExecutionNotFoundException":
                        raise

                time.sleep(5)

            return self._format_response({
                "status": "timeout",
                "message": "Approval polling timed out"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================

    def create_cloudwatch_event_rule(
        self,
        name: str,
        pipeline_name: str,
        description: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create CloudWatch Event rule for pipeline.

        Args:
            name: Rule name
            pipeline_name: Pipeline name
            description: Rule description
            kwargs: Additional parameters

        Returns:
            Event rule configuration
        """
        try:
            self._ensure_client()

            rule_config = {
                "Name": name,
                "EventPattern": json.dumps({
                    "source": ["aws.codepipeline"],
                    "detail-type": ["CodePipeline Pipeline Execution State Change"],
                    "detail": {
                        "pipeline": [pipeline_name]
                    }
                })
            }

            if description:
                rule_config["Description"] = description

            self._events_client.put_rule(**rule_config)

            return self._format_response({
                "rule_name": name,
                "pipeline": pipeline_name,
                "event_pattern": rule_config["EventPattern"]
            })

        except Exception as e:
            return self._handle_error(e)

    def add_cloudwatch_event_target(
        self,
        rule_name: str,
        target_arn: str,
        target_id: Optional[str] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add target to CloudWatch Event rule.

        Args:
            rule_name: Rule name
            target_arn: Target ARN (e.g., SNS topic ARN)
            target_id: Target ID
            kwargs: Additional parameters

        Returns:
            Target configuration
        """
        try:
            self._ensure_client()

            self._events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        "Id": target_id or self._generate_id(),
                        "Arn": target_arn
                    }
                ]
            )

            return self._format_response({
                "rule": rule_name,
                "target_arn": target_arn
            })

        except Exception as e:
            return self._handle_error(e)

    def create_cloudwatch_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        pipeline_name: str,
        threshold: float = 1.0,
        comparison_operator: str = "LessThanThreshold",
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create CloudWatch alarm for pipeline metrics.

        Args:
            alarm_name: Alarm name
            metric_name: Metric name
            namespace: Metric namespace
            pipeline_name: Pipeline name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            kwargs: Additional parameters

        Returns:
            Alarm configuration
        """
        try:
            self._ensure_client()

            self._cw_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace=namespace,
                Statistic="Sum",
                Period=300,
                EvaluationPeriods=1,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                Dimensions=[
                    {
                        "Name": "PipelineName",
                        "Value": pipeline_name
                    }
                ]
            )

            return self._format_response({
                "alarm_name": alarm_name,
                "metric_name": metric_name,
                "pipeline": pipeline_name
            })

        except Exception as e:
            return self._handle_error(e)

    def enable_cloudwatch_events(
        self,
        pipeline_name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enable CloudWatch Events for a pipeline.

        Args:
            pipeline_name: Pipeline name
            kwargs: Additional parameters

        Returns:
            CloudWatch events enabled confirmation
        """
        try:
            self._ensure_client()

            self._client.put_pipeline_stages(
                pipelineName=pipeline_name,
                stageName="Source",
                pipelineStage={
                    "name": "Source",
                    "actions": []
                }
            )

            return self._format_response({
                "message": f"CloudWatch Events enabled for pipeline '{pipeline_name}'"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # CUSTOM ACTIONS
    # =========================================================================

    def create_custom_action_type(
        self,
        category: ActionCategory,
        provider: str,
        version: str,
        settings: Optional[Dict[str, Any]] = None,
        configuration_properties: Optional[List[Dict[str, Any]]] = None,
        input_artifacts: int = 0,
        output_artifacts: int = 0,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a custom action type.

        Args:
            category: Action category
            provider: Provider name
            version: Action version
            settings: Action settings
            configuration_properties: Configuration properties
            input_artifacts: Number of input artifacts
            output_artifacts: Number of output artifacts
            kwargs: Additional parameters

        Returns:
            Custom action type configuration
        """
        try:
            self._ensure_client()

            action_type_config = {
                "category": category.value,
                "owner": ActionOwner.CUSTOM.value,
                "provider": provider,
                "version": version,
                "settings": settings or {},
                "configurationProperties": configuration_properties or [],
                "inputArtifactDetails": {
                    "count": input_artifacts,
                    "name": "InputArtifact"
                },
                "outputArtifactDetails": {
                    "count": output_artifacts,
                    "name": "OutputArtifact"
                }
            }

            response = self._client.create_custom_action_type(**action_type_config)

            return self._format_response({
                "action_type": response.get("actionType", {})
            })

        except Exception as e:
            return self._handle_error(e)

    def get_custom_action_type(
        self,
        category: ActionCategory,
        provider: str,
        version: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get custom action type details.

        Args:
            category: Action category
            provider: Provider name
            version: Action version
            kwargs: Additional parameters

        Returns:
            Custom action type details
        """
        try:
            self._ensure_client()
            response = self._client.get_custom_action_type(
                category=category.value,
                provider=provider,
                version=version
            )
            return self._format_response(response.get("actionType", {}))

        except Exception as e:
            return self._handle_error(e)

    def list_custom_action_types(
        self,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List custom action types.

        Args:
            kwargs: Additional parameters

        Returns:
            List of custom action types
        """
        try:
            self._ensure_client()
            response = self._client.list_action_types(
                actionOwnerFilter=ActionOwner.CUSTOM.value
            )
            return self._format_response({
                "action_types": response.get("actionTypes", [])
            })

        except Exception as e:
            return self._handle_error(e)

    def delete_custom_action_type(
        self,
        category: ActionCategory,
        provider: str,
        version: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Delete a custom action type.

        Args:
            category: Action category
            provider: Provider name
            version: Action version
            kwargs: Additional parameters

        Returns:
            Deletion confirmation
        """
        try:
            self._ensure_client()

            self._client.delete_custom_action_type(
                category=category.value,
                provider=provider,
                version=version
            )

            return self._format_response({
                "message": f"Custom action type '{provider}' deleted"
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # PIPELINE STATE
    # =========================================================================

    def get_pipeline_state(
        self,
        name: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get pipeline state.

        Args:
            name: Pipeline name
            kwargs: Additional parameters

        Returns:
            Pipeline state details
        """
        try:
            self._ensure_client()
            response = self._client.get_pipeline_state(name=name)
            return self._format_response(response)

        except Exception as e:
            return self._handle_error(e)

    def list_tags_for_resource(
        self,
        resource_arn: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List tags for a resource.

        Args:
            resource_arn: Resource ARN
            kwargs: Additional parameters

        Returns:
            Resource tags
        """
        try:
            self._ensure_client()
            response = self._client.list_tags_for_resource(resourceArn=resource_arn)
            return self._format_response({
                "tags": response.get("tags", [])
            })

        except Exception as e:
            return self._handle_error(e)

    def tag_resource(
        self,
        resource_arn: str,
        tags: List[Dict[str, str]],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Tag a resource.

        Args:
            resource_arn: Resource ARN
            tags: List of tag key-value pairs
            kwargs: Additional parameters

        Returns:
            Tagging confirmation
        """
        try:
            self._ensure_client()
            self._client.tag_resource(resourceArn=resource_arn, tags=tags)
            return self._format_response({
                "message": "Tags added successfully",
                "resource_arn": resource_arn
            })

        except Exception as e:
            return self._handle_error(e)

    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: List[str],
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Remove tags from a resource.

        Args:
            resource_arn: Resource ARN
            tag_keys: List of tag keys to remove
            kwargs: Additional parameters

        Returns:
            Untagging confirmation
        """
        try:
            self._ensure_client()
            self._client.untag_resource(resourceArn=resource_arn, tagKeys=tag_keys)
            return self._format_response({
                "message": "Tags removed successfully",
                "resource_arn": resource_arn
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # PIPELINE EXECUTION HELPERS
    # =========================================================================

    def retry_stage(
        self,
        pipeline_name: str,
        stage_name: str,
        execution_id: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Retry a failed stage.

        Args:
            pipeline_name: Pipeline name
            stage_name: Stage name to retry
            execution_id: Execution ID
            kwargs: Additional parameters

        Returns:
            Retry confirmation
        """
        try:
            self._ensure_client()
            self._client.retry_stage(
                pipelineName=pipeline_name,
                stageName=stage_name,
                pipelineExecutionId=execution_id
            )
            return self._format_response({
                "message": f"Stage '{stage_name}' retry initiated",
                "pipeline": pipeline_name,
                "stage": stage_name
            })

        except Exception as e:
            return self._handle_error(e)

    def get_action_type(
        self,
        category: ActionCategory,
        owner: ActionOwner,
        provider: str,
        version: str,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get action type details.

        Args:
            category: Action category
            owner: Action owner
            provider: Provider name
            version: Action version
            kwargs: Additional parameters

        Returns:
            Action type details
        """
        try:
            self._ensure_client()
            response = self._client.get_action_type(
                actionTypeId={
                    "category": category.value,
                    "owner": owner.value,
                    "provider": provider,
                    "version": version
                }
            )
            return self._format_response(response)

        except Exception as e:
            return self._handle_error(e)

    def list_action_types(
        self,
        owner_filter: Optional[ActionOwner] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List action types.

        Args:
            owner_filter: Filter by owner
            kwargs: Additional parameters

        Returns:
            List of action types
        """
        try:
            self._ensure_client()

            params = {}
            if owner_filter:
                params["actionOwnerFilter"] = owner_filter.value

            response = self._client.list_action_types(**params)

            return self._format_response({
                "action_types": response.get("actionTypes", [])
            })

        except Exception as e:
            return self._handle_error(e)

    # =========================================================================
    # BUILDER METHODS
    # =========================================================================

    def build_source_stage(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        output_artifact: str = "SourceArtifact",
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a source stage configuration.

        Args:
            name: Stage name
            provider: Source provider
            configuration: Provider configuration
            output_artifact: Output artifact name
            kwargs: Additional parameters

        Returns:
            Stage configuration
        """
        return {
            "name": name,
            "actions": [
                {
                    "name": f"{name}Action",
                    "actionTypeId": {
                        "category": ActionCategory.SOURCE.value,
                        "owner": ActionOwner.AWS.value if provider in ["CodeCommit", "S3", "ECR"] else ActionOwner.THIRD_PARTY.value,
                        "provider": provider,
                        "version": "1"
                    },
                    "configuration": configuration,
                    "outputArtifacts": [{"name": output_artifact}]
                }
            ]
        }

    def build_build_stage(
        self,
        name: str,
        provider: str,
        project_name: str,
        input_artifact: str = "SourceArtifact",
        output_artifact: str = "BuildArtifact",
        configuration: Optional[Dict[str, Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a build stage configuration.

        Args:
            name: Stage name
            provider: Build provider
            project_name: Build project name
            input_artifact: Input artifact name
            output_artifact: Output artifact name
            configuration: Additional configuration
            kwargs: Additional parameters

        Returns:
            Stage configuration
        """
        config = configuration or {}
        config["ProjectName"] = project_name

        return {
            "name": name,
            "actions": [
                {
                    "name": f"{name}Action",
                    "actionTypeId": {
                        "category": ActionCategory.BUILD.value,
                        "owner": ActionOwner.AWS.value if provider == "CodeBuild" else ActionOwner.THIRD_PARTY.value,
                        "provider": provider,
                        "version": "1"
                    },
                    "configuration": config,
                    "inputArtifacts": [{"name": input_artifact}],
                    "outputArtifacts": [{"name": output_artifact}]
                }
            ]
        }

    def build_deploy_stage(
        self,
        name: str,
        provider: str,
        configuration: Dict[str, Any],
        input_artifact: str = "BuildArtifact",
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a deploy stage configuration.

        Args:
            name: Stage name
            provider: Deploy provider
            configuration: Deploy configuration
            input_artifact: Input artifact name
            kwargs: Additional parameters

        Returns:
            Stage configuration
        """
        return {
            "name": name,
            "actions": [
                {
                    "name": f"{name}Action",
                    "actionTypeId": {
                        "category": ActionCategory.DEPLOY.value,
                        "owner": ActionOwner.AWS.value,
                        "provider": provider,
                        "version": "1"
                    },
                    "configuration": configuration,
                    "inputArtifacts": [{"name": input_artifact}]
                }
            ]
        }

    def build_approval_stage(
        self,
        name: str,
        custom_data: Optional[str] = None,
        approvers: Optional[List[str]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build an approval stage configuration.

        Args:
            name: Stage name
            custom_data: Custom data message
            approvers: List of approvers
            kwargs: Additional parameters

        Returns:
            Stage configuration
        """
        config = {
            "CustomData": custom_data or "Approval required before deployment"
        }
        if approvers:
            config["Approvers"] = approvers

        return {
            "name": name,
            "actions": [
                {
                    "name": f"{name}Action",
                    "actionTypeId": {
                        "category": ActionCategory.APPROVAL.value,
                        "owner": ActionOwner.AWS.value,
                        "provider": "Manual",
                        "version": "1"
                    },
                    "configuration": config
                }
            ]
        }

    def build_pipeline(
        self,
        name: str,
        role_arn: str,
        stages: List[Dict[str, Any]],
        artifact_store: ArtifactStoreConfig,
        tags: Optional[List[Dict[str, str]]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build and create a complete pipeline.

        Args:
            name: Pipeline name
            role_arn: IAM role ARN
            stages: List of stage configurations
            artifact_store: Artifact store configuration
            tags: Optional tags
            kwargs: Additional parameters

        Returns:
            Created pipeline details
        """
        return self.create_pipeline(
            name=name,
            role_arn=role_arn,
            artifact_store=artifact_store,
            stages=stages,
            tags=tags
        )
