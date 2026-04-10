"""
AWS CodeDeploy Serverless Integration Module for Workflow System

Implements a CodeDeployIntegration class with:
1. Application management: Create/manage applications
2. Deployment group: Create/manage deployment groups
3. Deployment management: Create/manage deployments
4. Deployment config: Configure deployment configurations
5. Rollback: Configure automatic rollback
6. EC2/on-prem: Support for EC2/on-premise deployments
7. ECS: ECS deployment support
8. Lambda: Lambda deployment support
9. AppSpec files: AppSpec file management
10. CloudWatch integration: Deployment events

Commit: 'feat(aws-codedeploy): add AWS CodeDeploy integration with application management, deployment groups, deployments, configurations, rollback, EC2, ECS, Lambda, AppSpec, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
import os
import yaml
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


class ComputePlatform(Enum):
    """CodeDeploy compute platform."""
    EC2 = "EC2"
    LAMBDA = "Lambda"
    ECS = "ECS"


class DeploymentStatus(Enum):
    """Deployment status values."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"
    READY = "READY"


class DeploymentOption(Enum):
    """Deployment option types."""
    WITH_TRAFFIC = "WITH_TRAFFIC"
    WITHOUT_TRAFFIC = "WITHOUT_TRAFFIC"


class DeploymentType(Enum):
    """Deployment type."""
    IN_PLACE = "IN_PLACE"
    BLUE_GREEN = "BLUE_GREEN"


class TrafficRoutingType(Enum):
    """Traffic routing configuration type."""
    ALL_AT_ONCE = "AllAtOnce"
    TIME_BASED_CANARY = "TimeBasedCanary"
    TIME_BASED_LINEAR = "TimeBasedLinear"


class DeploymentReadyOption(Enum):
    """Deployment ready option for blue/green."""
    NONE = "NONE"
    AUTO_BEFORE_ALARM = "AutoBeforeAlarm"


class TerminationOption(Enum):
    """Termination option for blue/green deployments."""
    TERMINATION = "TERMINATION"
    KEEP_ALIVE = "KEEP_ALIVE"


class RevisionLocationType(Enum):
    """Revision location type."""
    S3 = "S3"
    GITHUB = "GitHub"
    STRING = "String"
    APPSPEC_CONTENT = "AppSpecContent"


class InstanceStatus(Enum):
    """Instance deployment status."""
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    SKIPPED = "Skipped"
    UNKNOWN = "Unknown"


class ErrorCode(Enum):
    """CodeDeploy error codes."""
    SUCCESS = "Success"
    DEPLOYMENT_MISSING_APP_SPEC = "DeploymentMissingAppSpec"
    DEPLOYMENT_GROUP_MISSING = "DeploymentGroupMissing"
    DEPLOYMENT_IN_PROGRESS = "DeploymentInProgress"
    DEPLOYMENT_STOPPED = "DeploymentStopped"
    DEPLOYMENT_FAILED = "DeploymentFailed"
    DEPLOYMENT_SUCCEEDED = "DeploymentSucceeded"
    INVALID_REVISION = "InvalidRevision"
    INVALID_ROLE = "InvalidRole"
    INVALID_DEPLOYMENT_CONFIG = "InvalidDeploymentConfig"
    INVALID_SERVICE_ROLE = "InvalidServiceRole"


class AlarmStatus(Enum):
    """Alarm status for deployments."""
    ALARM_OK = "AlarmOk"
    ALARM_INSUFFICIENT_DATA = "AlarmInsufficientData"
    ALARM_CONFIGURATION_ERROR = "AlarmConfigurationError"


@dataclass
class CodeDeployConfig:
    """Configuration for CodeDeploy connection."""
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
class S3Location:
    """S3 revision location configuration."""
    bucket: str
    key: str
    bundle_type: str = "zip"  # zip, tar, tgz
    version: Optional[str] = None
    e_tag: Optional[str] = None


@dataclass
class GitHubLocation:
    """GitHub revision location configuration."""
    repository: str
    commit_id: str
    branch: Optional[str] = None
    git_hub_location: Optional[str] = None


@dataclass
class RevisionLocation:
    """Revision location for deployment."""
    revision_type: Union[RevisionLocationType, str]
    s3_location: Optional[S3Location] = None
    git_hub_location: Optional[GitHubLocation] = None
    app_spec_content: Optional[str] = None


@dataclass
class AlarmConfig:
    """Alarm configuration for deployment."""
    alarms: List[Dict[str, Any]] = field(default_factory=list)
    enabled: bool = True
    ignore_poll_alarm_failure: bool = False


@dataclass
class AutoRollbackConfig:
    """Automatic rollback configuration."""
    enabled: bool = False
    events: List[str] = field(default_factory=lambda: ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_REQUEST"])


@dataclass
class DeploymentStyle:
    """Deployment style configuration."""
    deployment_type: Union[DeploymentType, str] = DeploymentType.IN_PLACE
    deployment_option: Union[DeploymentOption, str] = DeploymentOption.WITH_TRAFFIC


@dataclass
class BlueGreenDeploymentConfig:
    """Blue/green deployment configuration."""
    terminate_blue_instances_on_deployment_success: Optional[Dict[str, Any]] = None
    deployment_ready_option: Optional[Union[DeploymentReadyOption, str]] = None
    green_fleet_provisioning_option: Optional[Dict[str, Any]] = None


@dataclass
class EC2TagFilter:
    """EC2 tag filter for deployment group."""
    key: Optional[str] = None
    value: Optional[str] = None
    type: str = "KEY_AND_VALUE"  # KEY_ONLY, VALUE_ONLY, KEY_AND_VALUE


@dataclass
class OnPremisesTagFilter:
    """On-premises tag filter for deployment group."""
    key: Optional[str] = None
    value: Optional[str] = None
    type: str = "KEY_AND_VALUE"


@dataclass
class EC2TargetGroupInfo:
    """EC2 target group info for ALB."""
    name: Optional[str] = None


@dataclass
class LoadBalancerInfo:
    """Load balancer info for deployment group."""
    elb_infos: List[Dict[str, Any]] = field(default_factory=list)
    target_group_infos: List[Dict[str, Any]] = field(default_factory=list)
    target_groups: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DeploymentGroupConfig:
    """Configuration for creating a deployment group."""
    deployment_group_name: str
    application_name: str
    deployment_style: Optional[DeploymentStyle] = None
    deployment_config_name: Optional[str] = None
    ec2_tag_filters: List[EC2TagFilter] = field(default_factory=list)
    on_premises_instance_tag_filters: List[OnPremisesTagFilter] = field(default_factory=list)
    auto_rollback_config: Optional[AutoRollbackConfig] = None
    alarm_config: Optional[AlarmConfig] = None
    blue_green_deployment_config: Optional[BlueGreenDeploymentConfig] = None
    load_balancer_info: Optional[LoadBalancerInfo] = None
    service_role_arn: Optional[str] = None
    trigger_config: Optional[List[Dict[str, Any]]] = None
    auto_scaling_groups: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DeploymentConfigInfo:
    """Information about a deployment configuration."""
    deployment_config_name: str
    deployment_config_arn: str
    created: str
    minimum_hosts: int = 0
    maximum_hosts: int = 0


@dataclass
class ApplicationInfo:
    """Information about a CodeDeploy application."""
    application_id: str
    application_name: str
    compute_platform: Optional[str] = None
    linked_to_github: bool = False
    github_account_name: Optional[str] = None
    created: str
    last_modified: str


@dataclass
class DeploymentGroupInfo:
    """Information about a deployment group."""
    deployment_group_id: str
    deployment_group_name: str
    application_name: str
    deployment_config_name: str
    ec2_tag_set: Optional[Dict[str, Any]] = None
    on_premises_tag_set: Optional[Dict[str, Any]] = None
    service_role_arn: str
    auto_rollback_config: Optional[Dict[str, Any]] = None
    alarm_config: Optional[Dict[str, Any]] = None
    trigger_config: Optional[List[Dict[str, Any]]] = None
    load_balancer_info: Optional[Dict[str, Any]] = None
    blue_green_deployment_config: Optional[Dict[str, Any]] = None
    deployment_style: Optional[Dict[str, Any]] = None
    target_instances: Optional[Dict[str, Any]] = None
    instances_count: Optional[Dict[str, int]] = None
    created: str
    last_modified: str


@dataclass
class DeploymentInfo:
    """Information about a deployment."""
    deployment_id: str
    application_name: str
    deployment_group_name: str
    deployment_config_name: Optional[str] = None
    status: Optional[str] = None
    error_information: Optional[Dict[str, Any]] = None
    create_time: Optional[str] = None
    complete_time: Optional[str] = None
    deployment_exposure_time: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    ignore_application_stop_failures: bool = False
    auto_rollback_enabled: bool = False
    update_outdated_instances_only: bool = False
    rollback_info: Optional[Dict[str, Any]] = None
    deployment_style: Optional[Dict[str, Any]] = None
    target_instances: Optional[Dict[str, Any]] = None
    instance_wave: Optional[Dict[str, Any]] = None
    load_balancer_info: Optional[Dict[str, Any]] = None
    revision: Optional[Dict[str, Any]] = None


@dataclass
class InstanceInfo:
    """Information about a deployment instance."""
    instance_id: str
    deployment_id: str
    status: Optional[str] = None
    last_updated: Optional[str] = None
    lifecycle_events: Optional[List[Dict[str, Any]]] = None
    instance_type: Optional[str] = None


@dataclass
class LifecycleEventInfo:
    """Information about a lifecycle event."""
    lifecycle_event_name: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: Optional[str] = None
    diagnostics: Optional[Dict[str, Any]] = None


@dataclass
class AppSpecContent:
    """AppSpec content configuration."""
    content: Optional[str] = None
    content_sha: Optional[str] = None
    bucket_name: Optional[str] = None
    object_key: Optional[str] = None


@dataclass
class AppSpecHooks:
    """AppSpec hooks configuration for ECS deployment."""
    after_allow_traffic: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    before_allow_traffic: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    after_block_traffic: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    before_block_traffic: Optional[List[Dict[str, Any]]] = field(default_factory=list)


@dataclass
class AppSpecResources:
    """AppSpec resources configuration for Lambda/ECS."""
    target_versions: Optional[Dict[str, Any]] = field(default_factory=dict)
    task_definitions: Optional[List[Dict[str, Any]]] = field(default_factory=list)


@dataclass
class ECSContainerImage:
    """ECS container image details."""
    image: str
    container_name: str


@dataclass
class ECSLoadBalancerInfo:
    """ECS load balancer configuration."""
    prod_traffic_route: Optional[Dict[str, Any]] = None
    test_traffic_route: Optional[Dict[str, Any]] = None
    target_group_pair_info: Optional[Dict[str, Any]] = None


@dataclass
class LambdaFunction:
    """Lambda function configuration."""
    name: str
    alias: Optional[str] = None
    current_version: Optional[str] = None
    target_version: Optional[str] = None


@dataclass
class CloudWatchEventConfig:
    """CloudWatch event configuration for deployment."""
    event_type: str
    trigger_name: Optional[str] = None
    target_role_arn: Optional[str] = None
    target_arn: Optional[str] = None
    rule_name: Optional[str] = None


class AppSpecValidator:
    """Validates AppSpec file structure."""
    
    @staticmethod
    def validate_ec2_appspec(appspec: Dict[str, Any]) -> bool:
        """
        Validate EC2/on-premises AppSpec structure.
        
        Required fields:
        - version (0.0)
        - os
        - files
        """
        if "version" not in appspec:
            raise ValueError("AppSpec missing required 'version' field")
        if "os" not in appspec:
            raise ValueError("AppSpec missing required 'os' field")
        if "files" not in appspec:
            raise ValueError("AppSpec missing required 'files' field")
        return True
    
    @staticmethod
    def validate_ecs_appspec(appspec: Dict[str, Any]) -> bool:
        """
        Validate ECS AppSpec structure.
        
        Required fields:
        - version
        - resources
        - hooks
        """
        if "version" not in appspec:
            raise ValueError("AppSpec missing required 'version' field")
        if "resources" not in appspec:
            raise ValueError("AppSpec missing required 'resources' field")
        if "hooks" not in appspec:
            raise ValueError("AppSpec missing required 'hooks' field")
        return True
    
    @staticmethod
    def validate_lambda_appspec(appspec: Dict[str, Any]) -> bool:
        """
        Validate Lambda AppSpec structure.
        
        Required fields:
        - version
        - resources
        - hooks
        """
        if "version" not in appspec:
            raise ValueError("AppSpec missing required 'version' field")
        if "resources" not in appspec:
            raise ValueError("AppSpec missing required 'resources' field")
        if "hooks" not in appspec:
            raise ValueError("AppSpec missing required 'hooks' field")
        return True
    
    @staticmethod
    def validate_yaml(appspec_content: str) -> Dict[str, Any]:
        """Parse and validate YAML AppSpec content."""
        try:
            parsed = yaml.safe_load(appspec_content)
            if not isinstance(parsed, dict):
                raise ValueError("AppSpec content must be a YAML dictionary")
            return parsed
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in AppSpec: {e}")
    
    @staticmethod
    def validate_json(appspec_content: str) -> Dict[str, Any]:
        """Parse and validate JSON AppSpec content."""
        try:
            parsed = json.loads(appspec_content)
            if not isinstance(parsed, dict):
                raise ValueError("AppSpec content must be a JSON object")
            return parsed
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in AppSpec: {e}")


class CodeDeployIntegration:
    """
    AWS CodeDeploy integration class for deployment automation.
    
    Supports:
    - Application creation, update, delete, and management
    - Deployment group creation and management
    - Deployment execution, monitoring, and management
    - Deployment configuration management
    - Automatic rollback configuration
    - EC2 and on-premises deployment support
    - ECS deployment support
    - Lambda deployment support
    - AppSpec file management and validation
    - CloudWatch events integration
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        codedeploy_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None,
        events_client: Optional[Any] = None,
        s3_client: Optional[Any] = None,
        iam_client: Optional[Any] = None
    ):
        """
        Initialize CodeDeploy integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: CodeDeploy endpoint URL (for testing with LocalStack, etc.)
            codedeploy_client: Pre-configured CodeDeploy client (overrides boto3 creation)
            cloudwatch_client: Pre-configured CloudWatch client
            events_client: Pre-configured CloudWatch Events client
            s3_client: Pre-configured S3 client
            iam_client: Pre-configured IAM client
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        
        self._clients = {}
        self._codedeploy_client = codedeploy_client
        self._cloudwatch_client = cloudwatch_client
        self._events_client = events_client
        self._s3_client = s3_client
        self._iam_client = iam_client
        
        self._lock = threading.Lock()
        self._applications_cache: Dict[str, ApplicationInfo] = {}
        self._deployment_groups_cache: Dict[str, DeploymentGroupInfo] = {}
        self._deployments_cache: Dict[str, DeploymentInfo] = {}
        self._deployment_configs_cache: Dict[str, DeploymentConfigInfo] = {}
        
        if BOTO3_AVAILABLE and codedeploy_client is None:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize boto3 clients."""
        with self._lock:
            if not BOTO3_AVAILABLE:
                logger.warning("boto3 not available, AWS SDK features disabled")
                return
            
            session_kwargs = {
                'region_name': self.region_name
            }
            
            if self.aws_access_key_id and self.aws_access_key_key:
                session_kwargs['aws_access_key_id'] = self.aws_access_key_id
                session_kwargs['aws_secret_access_key'] = self.aws_secret_access_key
                if self.aws_session_token:
                    session_kwargs['aws_session_token'] = self.aws_session_token
            
            try:
                session = boto3.Session(**session_kwargs)
                
                if self._codedeploy_client is None:
                    self._clients['codedeploy'] = session.client(
                        'codedeploy',
                        endpoint_url=self.endpoint_url
                    )
                
                if self._cloudwatch_client is None:
                    self._clients['cloudwatch'] = session.client('cloudwatch')
                
                if self._events_client is None:
                    self._clients['events'] = session.client('events')
                
                if self._s3_client is None:
                    self._clients['s3'] = session.client('s3')
                
                if self._iam_client is None:
                    self._clients['iam'] = session.client('iam')
                    
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to initialize AWS clients: {e}")
                raise
    
    @property
    def codedeploy(self):
        """Get CodeDeploy client."""
        if self._codedeploy_client is None:
            if 'codedeploy' not in self._clients:
                self._initialize_clients()
            self._codedeploy_client = self._clients.get('codedeploy')
        return self._codedeploy_client
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        if self._cloudwatch_client is None:
            if 'cloudwatch' not in self._clients:
                self._initialize_clients()
            self._cloudwatch_client = self._clients.get('cloudwatch')
        return self._cloudwatch_client
    
    @property
    def events(self):
        """Get CloudWatch Events client."""
        if self._events_client is None:
            if 'events' not in self._clients:
                self._initialize_clients()
            self._events_client = self._clients.get('events')
        return self._events_client
    
    @property
    def s3(self):
        """Get S3 client."""
        if self._s3_client is None:
            if 's3' not in self._clients:
                self._initialize_clients()
            self._s3_client = self._clients.get('s3')
        return self._s3_client
    
    @property
    def iam(self):
        """Get IAM client."""
        if self._iam_client is None:
            if 'iam' not in self._clients:
                self._initialize_clients()
            self._iam_client = self._clients.get('iam')
        return self._iam_client

    # =========================================================================
    # Application Management
    # =========================================================================
    
    def create_application(
        self,
        application_name: str,
        compute_platform: Optional[Union[ComputePlatform, str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> ApplicationInfo:
        """
        Create a CodeDeploy application.
        
        Args:
            application_name: Name of the application
            compute_platform: Compute platform (EC2, Lambda, ECS)
            tags: Optional tags for the application
            
        Returns:
            ApplicationInfo object with application details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {
            'applicationName': application_name
        }
        
        if compute_platform:
            params['computePlatform'] = str(compute_platform.value) if isinstance(compute_platform, ComputePlatform) else compute_platform
        
        try:
            response = self.codedeploy.create_application(**params)
            
            app_info = ApplicationInfo(
                application_id=response.get('applicationId', ''),
                application_name=application_name,
                compute_platform=params.get('computePlatform'),
                linked_to_github=False,
                created=datetime.utcnow().isoformat(),
                last_modified=datetime.utcnow().isoformat()
            )
            
            if tags:
                self.add_tags_to_application(application_name, tags)
                app_info.created = datetime.utcnow().isoformat()
            
            with self._lock:
                self._applications_cache[application_name] = app_info
            
            logger.info(f"Created CodeDeploy application: {application_name}")
            return app_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create application {application_name}: {e}")
            raise
    
    def get_application(self, application_name: str) -> Optional[ApplicationInfo]:
        """
        Get information about a CodeDeploy application.
        
        Args:
            application_name: Name of the application
            
        Returns:
            ApplicationInfo object or None if not found
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        with self._lock:
            if application_name in self._applications_cache:
                return self._applications_cache[application_name]
        
        try:
            response = self.codedeploy.get_application(applicationName=application_name)
            app = response.get('application', {})
            
            app_info = ApplicationInfo(
                application_id=app.get('applicationId', ''),
                application_name=app.get('applicationName', ''),
                compute_platform=app.get('computePlatform'),
                linked_to_github=app.get('linkedToGitHub', False),
                github_account_name=app.get('gitHubAccountName'),
                created=app.get('created', ''),
                last_modified=app.get('lastModified', '')
            )
            
            with self._lock:
                self._applications_cache[application_name] = app_info
            
            return app_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get application {application_name}: {e}")
            return None
    
    def list_applications(
        self,
        max_items: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List all CodeDeploy applications.
        
        Args:
            max_items: Maximum number of applications to return
            
        Returns:
            List of application summaries
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            params = {}
            if max_items:
                params['maxItems'] = max_items
            
            response = self.codedeploy.list_applications(**params)
            return response.get('applications', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list applications: {e}")
            raise
    
    def update_application(
        self,
        application_name: str,
        new_application_name: Optional[str] = None,
        compute_platform: Optional[Union[ComputePlatform, str]] = None
    ) -> bool:
        """
        Update a CodeDeploy application.
        
        Args:
            application_name: Current name of the application
            new_application_name: New name for the application
            compute_platform: New compute platform
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'applicationName': application_name}
        
        if new_application_name:
            params['newApplicationName'] = new_application_name
        
        if compute_platform:
            params['computePlatform'] = str(compute_platform.value) if isinstance(compute_platform, ComputePlatform) else compute_platform
        
        try:
            self.codedeploy.update_application(**params)
            
            with self._lock:
                if application_name in self._applications_cache:
                    del self._applications_cache[application_name]
                if new_application_name:
                    self._applications_cache[new_application_name] = ApplicationInfo(
                        application_id='',
                        application_name=new_application_name,
                        compute_platform=params.get('computePlatform'),
                        linked_to_github=False,
                        created=datetime.utcnow().isoformat(),
                        last_modified=datetime.utcnow().isoformat()
                    )
            
            logger.info(f"Updated CodeDeploy application: {application_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update application {application_name}: {e}")
            raise
    
    def delete_application(self, application_name: str) -> bool:
        """
        Delete a CodeDeploy application.
        
        Args:
            application_name: Name of the application to delete
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.delete_application(applicationName=application_name)
            
            with self._lock:
                if application_name in self._applications_cache:
                    del self._applications_cache[application_name]
            
            logger.info(f"Deleted CodeDeploy application: {application_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete application {application_name}: {e}")
            raise
    
    def add_tags_to_application(
        self,
        application_name: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to a CodeDeploy application.
        
        Args:
            application_name: Name of the application
            tags: Dictionary of tag key-value pairs
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            tag_list = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self.codedeploy.tag_resources(
                resourceARNs=[f"arn:aws:codedeploy:{self.region_name}:*:application:{application_name}"],
                tags={k: v for k, v in tags.items()}
            )
            
            logger.info(f"Added tags to application {application_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add tags to application {application_name}: {e}")
            raise

    # =========================================================================
    # Deployment Group Management
    # =========================================================================
    
    def create_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        deployment_config_name: Optional[str] = None,
        deployment_style: Optional[DeploymentStyle] = None,
        ec2_tag_filters: Optional[List[EC2TagFilter]] = None,
        on_premises_instance_tag_filters: Optional[List[OnPremisesTagFilter]] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None,
        alarm_config: Optional[AlarmConfig] = None,
        blue_green_deployment_config: Optional[BlueGreenDeploymentConfig] = None,
        load_balancer_info: Optional[LoadBalancerInfo] = None,
        service_role_arn: Optional[str] = None,
        trigger_config: Optional[List[Dict[str, Any]]] = None,
        auto_scaling_groups: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> DeploymentGroupInfo:
        """
        Create a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            deployment_config_name: Name of the deployment configuration
            deployment_style: Deployment type (in-place or blue/green)
            ec2_tag_filters: Tag filters for EC2 instances
            on_premises_instance_tag_filters: Tag filters for on-premises instances
            auto_rollback_config: Automatic rollback configuration
            alarm_config: Alarm configuration
            blue_green_deployment_config: Blue/green deployment configuration
            load_balancer_info: Load balancer configuration
            service_role_arn: IAM service role ARN
            trigger_config: Notification trigger configuration
            auto_scaling_groups: Auto scaling group names
            tags: Tags for the deployment group
            
        Returns:
            DeploymentGroupInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {
            'applicationName': application_name,
            'deploymentGroupName': deployment_group_name
        }
        
        if deployment_config_name:
            params['deploymentConfigName'] = deployment_config_name
        
        if deployment_style:
            params['deploymentStyle'] = {
                'deploymentType': str(deployment_style.deployment_type.value) if isinstance(deployment_style.deployment_type, DeploymentType) else deployment_style.deployment_type,
                'deploymentOption': str(deployment_style.deployment_option.value) if isinstance(deployment_style.deployment_option, DeploymentOption) else deployment_style.deployment_option
            }
        
        if ec2_tag_filters:
            params['ec2TagSet'] = {
                'ec2TagSetList': [[{
                    'Key': f.key,
                    'Value': f.value,
                    'Type': f.type
                } for f in ec2_tag_filters]]
            }
        
        if on_premises_instance_tag_filters:
            params['onPremisesTagSet'] = {
                'onPremisesTagSetList': [[{
                    'Key': f.key,
                    'Value': f.value,
                    'Type': f.type
                } for f in on_premises_instance_tag_filters]]
            }
        
        if auto_rollback_config:
            params['autoRollbackConfiguration'] = {
                'enabled': auto_rollback_config.enabled,
                'events': auto_rollback_config.events
            }
        
        if alarm_config:
            params['alarmConfiguration'] = {
                'alarms': alarm_config.alarms,
                'enabled': alarm_config.enabled,
                'ignorePollAlarmFailure': alarm_config.ignore_poll_alarm_failure
            }
        
        if blue_green_deployment_config:
            bg_config = {}
            if blue_green_deployment_config.terminate_blue_instances_on_deployment_success:
                bg_config['terminateBlueInstancesOnDeploymentSuccess'] = \
                    blue_green_deployment_config.terminate_blue_instances_on_deployment_success
            if blue_green_deployment_config.deployment_ready_option:
                bg_config['deploymentReadyOption'] = {
                    'actionOnTimeout': str(blue_green_deployment_config.deployment_ready_option.value)
                }
            if blue_green_deployment_config.green_fleet_provisioning_option:
                bg_config['greenFleetProvisioningOption'] = \
                    blue_green_deployment_config.green_fleet_provisioning_option
            params['blueGreenDeploymentConfiguration'] = bg_config
        
        if load_balancer_info:
            lb_config = {}
            if load_balancer_info.elb_infos:
                lb_config['elbInfoList'] = load_balancer_info.elb_infos
            if load_balancer_info.target_group_infos:
                lb_config['targetGroupInfoList'] = load_balancer_info.target_group_infos
            if load_balancer_info.target_groups:
                lb_config['targetGroupPairInfoList'] = load_balancer_info.target_groups
            params['loadBalancerInfo'] = lb_config
        
        if service_role_arn:
            params['serviceRoleArn'] = service_role_arn
        
        if trigger_config:
            params['triggerConfiguration'] = trigger_config
        
        if auto_scaling_groups:
            params['autoScalingGroups'] = auto_scaling_groups
        
        try:
            response = self.codedeploy.create_deployment_group(**params)
            
            dg_info = DeploymentGroupInfo(
                deployment_group_id=response.get('deploymentGroupId', ''),
                deployment_group_name=deployment_group_name,
                application_name=application_name,
                deployment_config_name=deployment_config_name or 'CodeDeployDefault.OneAtATime',
                service_role_arn=service_role_arn or '',
                auto_rollback_config=params.get('autoRollbackConfiguration'),
                alarm_config=params.get('alarmConfiguration'),
                trigger_config=params.get('triggerConfiguration'),
                load_balancer_info=params.get('loadBalancerInfo'),
                blue_green_deployment_config=params.get('blueGreenDeploymentConfiguration'),
                deployment_style=params.get('deploymentStyle'),
                created=datetime.utcnow().isoformat(),
                last_modified=datetime.utcnow().isoformat()
            )
            
            if tags:
                self.add_tags_to_deployment_group(application_name, deployment_group_name, tags)
            
            with self._lock:
                self._deployment_groups_cache[f"{application_name}:{deployment_group_name}"] = dg_info
            
            logger.info(f"Created deployment group: {application_name}/{deployment_group_name}")
            return dg_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create deployment group {deployment_group_name}: {e}")
            raise
    
    def get_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str
    ) -> Optional[DeploymentGroupInfo]:
        """
        Get information about a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            
        Returns:
            DeploymentGroupInfo object or None if not found
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        cache_key = f"{application_name}:{deployment_group_name}"
        with self._lock:
            if cache_key in self._deployment_groups_cache:
                return self._deployment_groups_cache[cache_key]
        
        try:
            response = self.codedeploy.get_deployment_group(
                applicationName=application_name,
                deploymentGroupName=deployment_group_name
            )
            
            dg = response.get('deploymentGroup', {})
            
            dg_info = DeploymentGroupInfo(
                deployment_group_id=dg.get('deploymentGroupId', ''),
                deployment_group_name=dg.get('deploymentGroupName', ''),
                application_name=dg.get('applicationName', ''),
                deployment_config_name=dg.get('deploymentConfigName', ''),
                ec2_tag_set=dg.get('ec2TagSet'),
                on_premises_tag_set=dg.get('onPremisesTagSet'),
                service_role_arn=dg.get('serviceRoleArn', ''),
                auto_rollback_config=dg.get('autoRollbackConfiguration'),
                alarm_config=dg.get('alarmConfiguration'),
                trigger_config=dg.get('triggerConfiguration'),
                load_balancer_info=dg.get('loadBalancerInfo'),
                blue_green_deployment_config=dg.get('blueGreenDeploymentConfiguration'),
                deployment_style=dg.get('deploymentStyle'),
                target_instances=dg.get('targetInstances'),
                instances_count=dg.get('instancesCount'),
                created=dg.get('created', ''),
                last_modified=dg.get('lastModified', '')
            )
            
            with self._lock:
                self._deployment_groups_cache[cache_key] = dg_info
            
            return dg_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get deployment group {deployment_group_name}: {e}")
            return None
    
    def list_deployment_groups(
        self,
        application_name: str
    ) -> List[str]:
        """
        List all deployment groups for an application.
        
        Args:
            application_name: Name of the application
            
        Returns:
            List of deployment group names
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            response = self.codedeploy.list_deployment_groups(
                applicationName=application_name
            )
            return response.get('deploymentGroups', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list deployment groups for {application_name}: {e}")
            raise
    
    def update_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        new_deployment_group_name: Optional[str] = None,
        deployment_config_name: Optional[str] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None,
        alarm_config: Optional[AlarmConfig] = None,
        service_role_arn: Optional[str] = None,
        trigger_config: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """
        Update a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Current name of the deployment group
            new_deployment_group_name: New name for the deployment group
            deployment_config_name: New deployment configuration name
            auto_rollback_config: New automatic rollback configuration
            alarm_config: New alarm configuration
            service_role_arn: New service role ARN
            trigger_config: New notification trigger configuration
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {
            'applicationName': application_name,
            'currentDeploymentGroupName': deployment_group_name
        }
        
        if new_deployment_group_name:
            params['newDeploymentGroupName'] = new_deployment_group_name
        
        if deployment_config_name:
            params['deploymentConfigName'] = deployment_config_name
        
        if auto_rollback_config:
            params['autoRollbackConfiguration'] = {
                'enabled': auto_rollback_config.enabled,
                'events': auto_rollback_config.events
            }
        
        if alarm_config:
            params['alarmConfiguration'] = {
                'alarms': alarm_config.alarms,
                'enabled': alarm_config.enabled,
                'ignorePollAlarmFailure': alarm_config.ignore_poll_alarm_failure
            }
        
        if service_role_arn:
            params['serviceRoleArn'] = service_role_arn
        
        if trigger_config:
            params['triggerConfiguration'] = trigger_config
        
        try:
            self.codedeploy.update_deployment_group(**params)
            
            with self._lock:
                old_key = f"{application_name}:{deployment_group_name}"
                if old_key in self._deployment_groups_cache:
                    del self._deployment_groups_cache[old_key]
                new_key = f"{application_name}:{new_deployment_group_name or deployment_group_name}"
                self._deployment_groups_cache[new_key] = DeploymentGroupInfo(
                    deployment_group_id='',
                    deployment_group_name=new_deployment_group_name or deployment_group_name,
                    application_name=application_name,
                    deployment_config_name=deployment_config_name or 'CodeDeployDefault.OneAtATime',
                    service_role_arn=service_role_arn or '',
                    created=datetime.utcnow().isoformat(),
                    last_modified=datetime.utcnow().isoformat()
                )
            
            logger.info(f"Updated deployment group: {deployment_group_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update deployment group {deployment_group_name}: {e}")
            raise
    
    def delete_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str
    ) -> bool:
        """
        Delete a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group to delete
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.delete_deployment_group(
                applicationName=application_name,
                deploymentGroupName=deployment_group_name
            )
            
            with self._lock:
                cache_key = f"{application_name}:{deployment_group_name}"
                if cache_key in self._deployment_groups_cache:
                    del self._deployment_groups_cache[cache_key]
            
            logger.info(f"Deleted deployment group: {application_name}/{deployment_group_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete deployment group {deployment_group_name}: {e}")
            raise
    
    def add_tags_to_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            tags: Dictionary of tag key-value pairs
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.tag_resources(
                resourceARNs=[f"arn:aws:codedeploy:{self.region_name}:*:deploymentgroup:{application_name}/{deployment_group_name}"],
                tags={k: v for k, v in tags.items()}
            )
            
            logger.info(f"Added tags to deployment group {deployment_group_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add tags to deployment group {deployment_group_name}: {e}")
            raise

    # =========================================================================
    # Deployment Configuration Management
    # =========================================================================
    
    def create_deployment_config(
        self,
        deployment_config_name: str,
        minimum_healthy_hosts: Optional[Dict[str, Any]] = None,
        traffic_routing_config: Optional[Dict[str, Any]] = None
    ) -> DeploymentConfigInfo:
        """
        Create a deployment configuration.
        
        Args:
            deployment_config_name: Name of the deployment configuration
            minimum_healthy_hosts: Minimum healthy hosts configuration
            traffic_routing_config: Traffic routing configuration for time-based deployments
            
        Returns:
            DeploymentConfigInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'deploymentConfigName': deployment_config_name}
        
        if minimum_healthy_hosts:
            params['minimumHealthyHosts'] = minimum_healthy_hosts
        
        if traffic_routing_config:
            params['trafficRoutingConfig'] = traffic_routing_config
        
        try:
            response = self.codedeploy.create_deployment_config(**params)
            
            config_info = DeploymentConfigInfo(
                deployment_config_name=deployment_config_name,
                deployment_config_arn=response.get('deploymentConfigArn', ''),
                created=datetime.utcnow().isoformat()
            )
            
            with self._lock:
                self._deployment_configs_cache[deployment_config_name] = config_info
            
            logger.info(f"Created deployment configuration: {deployment_config_name}")
            return config_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create deployment configuration {deployment_config_name}: {e}")
            raise
    
    def get_deployment_config(
        self,
        deployment_config_name: str
    ) -> Optional[DeploymentConfigInfo]:
        """
        Get information about a deployment configuration.
        
        Args:
            deployment_config_name: Name of the deployment configuration
            
        Returns:
            DeploymentConfigInfo object or None if not found
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        with self._lock:
            if deployment_config_name in self._deployment_configs_cache:
                return self._deployment_configs_cache[deployment_config_name]
        
        try:
            response = self.codedeploy.get_deployment_config(
                deploymentConfigName=deployment_config_name
            )
            
            config = response.get('deploymentConfig', {})
            
            config_info = DeploymentConfigInfo(
                deployment_config_name=config.get('deploymentConfigName', ''),
                deployment_config_arn=config.get('deploymentConfigArn', ''),
                created=config.get('created', ''),
                minimum_hosts=config.get('minimumHealthyHosts', {}).get('value', 0),
                maximum_hosts=0
            )
            
            with self._lock:
                self._deployment_configs_cache[deployment_config_name] = config_info
            
            return config_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get deployment configuration {deployment_config_name}: {e}")
            return None
    
    def list_deployment_configs(self) -> List[str]:
        """
        List all deployment configurations.
        
        Returns:
            List of deployment configuration names
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            response = self.codedeploy.list_deployment_configs()
            return response.get('deploymentConfigsList', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list deployment configurations: {e}")
            raise
    
    def delete_deployment_config(self, deployment_config_name: str) -> bool:
        """
        Delete a deployment configuration.
        
        Args:
            deployment_config_name: Name of the deployment configuration to delete
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.delete_deployment_config(
                deploymentConfigName=deployment_config_name
            )
            
            with self._lock:
                if deployment_config_name in self._deployment_configs_cache:
                    del self._deployment_configs_cache[deployment_config_name]
            
            logger.info(f"Deleted deployment configuration: {deployment_config_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete deployment configuration {deployment_config_name}: {e}")
            raise

    # =========================================================================
    # Deployment Management
    # =========================================================================
    
    def create_deployment(
        self,
        application_name: str,
        deployment_group_name: str,
        revision: Optional[RevisionLocation] = None,
        deployment_config_name: Optional[str] = None,
        description: Optional[str] = None,
        ignore_application_stop_failures: bool = False,
        auto_rollback_enabled: bool = False,
        update_outdated_instances_only: bool = False,
        file_exists_behavior: Optional[str] = None
    ) -> DeploymentInfo:
        """
        Create a deployment.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            revision: Revision location (S3, GitHub, etc.)
            deployment_config_name: Name of the deployment configuration
            description: Description for the deployment
            ignore_application_stop_failures: Ignore application stop failures
            auto_rollback_enabled: Enable automatic rollback on failure
            update_outdated_instances_only: Update only outdated instances
            file_exists_behavior: File exists behavior (DISALLOW, OVERWRITE, RETAIN)
            
        Returns:
            DeploymentInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {
            'applicationName': application_name,
            'deploymentGroupName': deployment_group_name
        }
        
        if revision:
            if revision.revision_type == RevisionLocationType.S3 and revision.s3_location:
                params['revision'] = {
                    'revisionType': 'S3',
                    's3Location': {
                        'bucket': revision.s3_location.bucket,
                        'key': revision.s3_location.key,
                        'bundleType': revision.s3_location.bundle_type,
                        'version': revision.s3_location.version,
                        'eTag': revision.s3_location.e_tag
                    }
                }
            elif revision.revision_type == RevisionLocationType.GITHUB and revision.git_hub_location:
                params['revision'] = {
                    'revisionType': 'GitHub',
                    'gitHubLocation': {
                        'repository': revision.git_hub_location.repository,
                        'commitId': revision.git_hub_location.commit_id
                    }
                }
            elif revision.revision_type == RevisionLocationType.STRING and revision.app_spec_content:
                params['revision'] = {
                    'revisionType': 'String',
                    'appSpecContent': {
                        'content': revision.app_spec_content
                    }
                }
        
        if deployment_config_name:
            params['deploymentConfigName'] = deployment_config_name
        
        if description:
            params['description'] = description
        
        params['ignoreApplicationStopFailures'] = ignore_application_stop_failures
        params['autoRollbackEnabled'] = auto_rollback_enabled
        params['updateOutdatedInstancesOnly'] = update_outdated_instances_only
        
        if file_exists_behavior:
            params['fileExistsBehavior'] = file_exists_behavior
        
        try:
            response = self.codedeploy.create_deployment(**params)
            
            deployment_id = response.get('deploymentId', '')
            
            deploy_info = DeploymentInfo(
                deployment_id=deployment_id,
                application_name=application_name,
                deployment_group_name=deployment_group_name,
                deployment_config_name=deployment_config_name,
                status='Pending',
                create_time=datetime.utcnow().isoformat(),
                ignore_application_stop_failures=ignore_application_stop_failures,
                auto_rollback_enabled=auto_rollback_enabled,
                update_outdated_instances_only=update_outdated_instances_only,
                revision=params.get('revision')
            )
            
            with self._lock:
                self._deployments_cache[deployment_id] = deploy_info
            
            logger.info(f"Created deployment: {deployment_id}")
            return deploy_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create deployment: {e}")
            raise
    
    def get_deployment(self, deployment_id: str) -> Optional[DeploymentInfo]:
        """
        Get information about a deployment.
        
        Args:
            deployment_id: ID of the deployment
            
        Returns:
            DeploymentInfo object or None if not found
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        with self._lock:
            if deployment_id in self._deployments_cache:
                cached = self._deployments_cache[deployment_id]
                if cached.status in [DeploymentStatus.SUCCEEDED.value, DeploymentStatus.FAILED.value, DeploymentStatus.STOPPED.value]:
                    return cached
        
        try:
            response = self.codedeploy.get_deployment(deploymentId=deployment_id)
            
            d = response.get('deploymentInfo', {})
            
            deploy_info = DeploymentInfo(
                deployment_id=deployment_id,
                application_name=d.get('applicationName', ''),
                deployment_group_name=d.get('deploymentGroupName', ''),
                deployment_config_name=d.get('deploymentConfigName'),
                status=d.get('status'),
                error_information=d.get('errorInformation'),
                create_time=d.get('createTime'),
                complete_time=d.get('completeTime'),
                deployment_exposure_time=d.get('deploymentExposureTime'),
                start_time=d.get('startTime'),
                end_time=d.get('endTime'),
                ignore_application_stop_failures=d.get('ignoreApplicationStopFailures', False),
                auto_rollback_enabled=d.get('autoRollbackEnabled', False),
                update_outdated_instances_only=d.get('updateOutdatedInstancesOnly', False),
                rollback_info=d.get('rollbackInfo'),
                deployment_style=d.get('deploymentStyle'),
                target_instances=d.get('targetInstances'),
                instance_wave=d.get('instanceWave'),
                load_balancer_info=d.get('loadBalancerInfo'),
                revision=d.get('revision')
            )
            
            with self._lock:
                self._deployments_cache[deployment_id] = deploy_info
            
            return deploy_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get deployment {deployment_id}: {e}")
            return None
    
    def list_deployments(
        self,
        application_name: str,
        deployment_group_name: Optional[str] = None,
        include_target_instances: bool = False,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None
    ) -> List[str]:
        """
        List deployments for an application.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group (optional)
            include_target_instances: Include target instances
            created_after: Filter by creation time after this date
            created_before: Filter by creation time before this date
            
        Returns:
            List of deployment IDs
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'applicationName': application_name}
        
        if deployment_group_name:
            params['deploymentGroupName'] = deployment_group_name
        
        params['includeTargetInstances'] = include_target_instances
        
        if created_after:
            params['createdAfter'] = created_after.isoformat()
        
        if created_before:
            params['createdBefore'] = created_before.isoformat()
        
        try:
            response = self.codedeploy.list_deployments(**params)
            return response.get('deployments', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list deployments: {e}")
            raise
    
    def stop_deployment(
        self,
        deployment_id: str,
        auto_rollback_enabled: bool = True
    ) -> bool:
        """
        Stop a deployment.
        
        Args:
            deployment_id: ID of the deployment to stop
            auto_rollback_enabled: Enable automatic rollback when stopping
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.stop_deployment(
                deploymentId=deployment_id,
                autoRollbackEnabled=auto_rollback_enabled
            )
            
            with self._lock:
                if deployment_id in self._deployments_cache:
                    self._deployments_cache[deployment_id].status = DeploymentStatus.STOPPED.value
            
            logger.info(f"Stopped deployment: {deployment_id}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to stop deployment {deployment_id}: {e}")
            raise
    
    def redeploy_revision(
        self,
        application_name: str,
        deployment_group_name: str,
        revision: RevisionLocation,
        description: Optional[str] = None
    ) -> DeploymentInfo:
        """
        Redeploy a revision to an existing deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            revision: Revision to deploy
            description: Description for the deployment
            
        Returns:
            DeploymentInfo object
        """
        return self.create_deployment(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            revision=revision,
            description=description
        )
    
    def register_application_revision(
        self,
        application_name: str,
        revision: RevisionLocation,
        description: Optional[str] = None
    ) -> bool:
        """
        Register a revision with an application.
        
        Args:
            application_name: Name of the application
            revision: Revision to register
            description: Description for the revision
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'applicationName': application_name}
        
        if revision.revision_type == RevisionLocationType.S3 and revision.s3_location:
            params['revision'] = {
                'revisionType': 'S3',
                's3Location': {
                    'bucket': revision.s3_location.bucket,
                    'key': revision.s3_location.key,
                    'bundleType': revision.s3_location.bundle_type,
                    'version': revision.s3_location.version,
                    'eTag': revision.s3_location.e_tag
                }
            }
        elif revision.revision_type == RevisionLocationType.GITHUB and revision.git_hub_location:
            params['revision'] = {
                'revisionType': 'GitHub',
                'gitHubLocation': {
                    'repository': revision.git_hub_location.repository,
                    'commitId': revision.git_hub_location.commit_id
                }
            }
        
        if description:
            params['description'] = description
        
        try:
            self.codedeploy.register_application_revision(**params)
            logger.info(f"Registered application revision for {application_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to register application revision: {e}")
            raise
    
    def get_application_revision(
        self,
        application_name: str,
        revision: RevisionLocation
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about an application revision.
        
        Args:
            application_name: Name of the application
            revision: Revision to get
            
        Returns:
            Revision information or None
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'applicationName': application_name}
        
        if revision.revision_type == RevisionLocationType.S3 and revision.s3_location:
            params['revision'] = {
                'revisionType': 'S3',
                's3Location': {
                    'bucket': revision.s3_location.bucket,
                    'key': revision.s3_location.key,
                    'bundleType': revision.s3_location.bundle_type,
                    'version': revision.s3_location.version,
                    'eTag': revision.s3_location.e_tag
                }
            }
        elif revision.revision_type == RevisionLocationType.GITHUB and revision.git_hub_location:
            params['revision'] = {
                'revisionType': 'GitHub',
                'gitHubLocation': {
                    'repository': revision.git_hub_location.repository,
                    'commitId': revision.git_hub_location.commit_id
                }
            }
        
        try:
            response = self.codedeploy.get_application_revision(**params)
            return response.get('applicationName')
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get application revision: {e}")
            return None
    
    def list_application_revisions(
        self,
        application_name: str,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        deployed: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List application revisions.
        
        Args:
            application_name: Name of the application
            sort_by: Sort by (registerTime, firstUsedTime, lastUsedTime)
            sort_order: Sort order (ascending, descending)
            s3_bucket: S3 bucket to filter by
            deployed: Filter by deployment status (INCLUDE, EXCLUDE, IGNORE)
            
        Returns:
            List of revision information
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'applicationName': application_name}
        
        if sort_by:
            params['sortBy'] = sort_by
        if sort_order:
            params['sortOrder'] = sort_order
        if s3_bucket:
            params['s3Bucket'] = s3_bucket
        if deployed:
            params['deployed'] = deployed
        
        try:
            response = self.codedeploy.list_application_revisions(**params)
            return response.get('revisions', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list application revisions: {e}")
            raise

    # =========================================================================
    # Instance Management
    # =========================================================================
    
    def list_deployment_instances(
        self,
        deployment_id: str,
        instance_status_filter: Optional[Union[InstanceStatus, str]] = None,
        next_token: Optional[str] = None
    ) -> List[str]:
        """
        List instances for a deployment.
        
        Args:
            deployment_id: ID of the deployment
            instance_status_filter: Filter by instance status
            next_token: Pagination token
            
        Returns:
            List of instance IDs
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {'deploymentId': deployment_id}
        
        if instance_status_filter:
            params['instanceStatusFilter'] = str(instance_status_filter.value) if isinstance(instance_status_filter, InstanceStatus) else instance_status_filter
        
        if next_token:
            params['nextToken'] = next_token
        
        try:
            response = self.codedeploy.list_deployment_instances(**params)
            return response.get('instancesList', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list deployment instances: {e}")
            raise
    
    def get_instance_instance(
        self,
        deployment_id: str,
        instance_id: str
    ) -> Optional[InstanceInfo]:
        """
        Get information about a deployment instance.
        
        Args:
            deployment_id: ID of the deployment
            instance_id: ID of the instance
            
        Returns:
            InstanceInfo object or None if not found
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            response = self.codedeploy.get_deployment_instance(
                deploymentId=deployment_id,
                instanceId=instance_id
            )
            
            instance = response.get('instanceInfo', {})
            
            return InstanceInfo(
                instance_id=instance.get('instanceId', ''),
                deployment_id=deployment_id,
                status=instance.get('status'),
                last_updated=instance.get('lastUpdatedAt'),
                lifecycle_events=instance.get('lifecycleEvents'),
                instance_type=instance.get('instanceType')
            )
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get deployment instance {instance_id}: {e}")
            return None
    
    def list_on_premises_instances(
        self,
        tag_filters: Optional[List[Dict[str, Any]]] = None,
        registration_status: Optional[str] = None
    ) -> List[str]:
        """
        List on-premises instances.
        
        Args:
            tag_filters: Tag filters to apply
            registration_status: Filter by registration status (Registered, Deregistered)
            
        Returns:
            List of instance names
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        params = {}
        
        if tag_filters:
            params['tagFilters'] = tag_filters
        
        if registration_status:
            params['registrationStatus'] = registration_status
        
        try:
            response = self.codedeploy.list_on_premises_instances(**params)
            return response.get('instanceNames', [])
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list on-premises instances: {e}")
            raise

    # =========================================================================
    # EC2/On-Premises Deployment Support
    # =========================================================================
    
    def create_ec2_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        ec2_tag_filters: List[EC2TagFilter],
        deployment_config_name: Optional[str] = None,
        service_role_arn: Optional[str] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None,
        alarm_config: Optional[AlarmConfig] = None
    ) -> DeploymentGroupInfo:
        """
        Create an EC2 deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            ec2_tag_filters: Tag filters to identify EC2 instances
            deployment_config_name: Deployment configuration name
            service_role_arn: IAM service role ARN
            auto_rollback_config: Automatic rollback configuration
            alarm_config: Alarm configuration
            
        Returns:
            DeploymentGroupInfo object
        """
        return self.create_deployment_group(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            deployment_config_name=deployment_config_name,
            ec2_tag_filters=ec2_tag_filters,
            service_role_arn=service_role_arn,
            auto_rollback_config=auto_rollback_config,
            alarm_config=alarm_config
        )
    
    def create_on_premises_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        on_premises_tag_filters: List[OnPremisesTagFilter],
        deployment_config_name: Optional[str] = None,
        service_role_arn: Optional[str] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None
    ) -> DeploymentGroupInfo:
        """
        Create an on-premises deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            on_premises_tag_filters: Tag filters to identify on-premises instances
            deployment_config_name: Deployment configuration name
            service_role_arn: IAM service role ARN
            auto_rollback_config: Automatic rollback configuration
            
        Returns:
            DeploymentGroupInfo object
        """
        return self.create_deployment_group(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            deployment_config_name=deployment_config_name,
            on_premises_instance_tag_filters=on_premises_tag_filters,
            service_role_arn=service_role_arn,
            auto_rollback_config=auto_rollback_config
        )
    
    def register_on_premises_instance(
        self,
        instance_name: str
    ) -> str:
        """
        Register an on-premises instance.
        
        Args:
            instance_name: Name of the instance
            
        Returns:
            IAM user ARN to configure the instance
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            response = self.codedeploy.register_on_premises_instance(
                instanceName=instance_name
            )
            return response.get('IamUserArn', '')
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to register on-premises instance {instance_name}: {e}")
            raise
    
    def deregister_on_premises_instance(
        self,
        instance_name: str
    ) -> bool:
        """
        Deregister an on-premises instance.
        
        Args:
            instance_name: Name of the instance
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            self.codedeploy.deregister_on_premises_instance(
                instanceName=instance_name
            )
            logger.info(f"Deregistered on-premises instance: {instance_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to deregister on-premises instance {instance_name}: {e}")
            raise
    
    def add_tags_to_on_premises_instances(
        self,
        instance_names: List[str],
        tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to on-premises instances.
        
        Args:
            instance_names: Names of the instances
            tags: Dictionary of tag key-value pairs
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            iam_user_arns = [f"arn:aws:iam::{self.region_name}::instance/{name}" for name in instance_names]
            self.codedeploy.tag_resources(
                resourceARNs=instance_names,
                tags={k: v for k, v in tags.items()}
            )
            
            logger.info(f"Added tags to on-premises instances: {instance_names}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add tags to on-premises instances: {e}")
            raise

    # =========================================================================
    # ECS Deployment Support
    # =========================================================================
    
    def create_ecs_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        service_role_arn: str,
        ecs_cluster_name: str,
        ecs_service_name: str,
        load_balancer_info: ECSLoadBalancerInfo,
        deployment_config_name: Optional[str] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None,
        blue_green_deployment_config: Optional[BlueGreenDeploymentConfig] = None
    ) -> DeploymentGroupInfo:
        """
        Create an ECS deployment group.
        
        Args:
            application_name: Name of the CodeDeploy application
            deployment_group_name: Name of the deployment group
            service_role_arn: IAM service role ARN
            ecs_cluster_name: Name of the ECS cluster
            ecs_service_name: Name of the ECS service
            load_balancer_info: ECS load balancer configuration
            deployment_config_name: Deployment configuration name
            auto_rollback_config: Automatic rollback configuration
            blue_green_deployment_config: Blue/green deployment configuration
            
        Returns:
            DeploymentGroupInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        lb_config = {}
        if load_balancer_info.prod_traffic_route:
            lb_config['prodTrafficRoute'] = load_balancer_info.prod_traffic_route
        if load_balancer_info.test_traffic_route:
            lb_config['testTrafficRoute'] = load_balancer_info.test_traffic_route
        if load_balancer_info.target_group_pair_info:
            lb_config['targetGroupPairInfo'] = load_balancer_info.target_group_pair_info
        
        deployment_style = DeploymentStyle(
            deployment_type=DeploymentType.BLUE_GREEN,
            deployment_option=DeploymentOption.WITH_TRAFFIC
        )
        
        return self.create_deployment_group(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            deployment_config_name=deployment_config_name,
            deployment_style=deployment_style,
            service_role_arn=service_role_arn,
            auto_rollback_config=auto_rollback_config,
            blue_green_deployment_config=blue_green_deployment_config,
            load_balancer_info=LoadBalancerInfo(target_groups=[lb_config])
        )
    
    def create_ecs_revision(
        self,
        application_name: str,
        revision_content: str
    ) -> bool:
        """
        Create an ECS AppSpec revision.
        
        Args:
            application_name: Name of the CodeDeploy application
            revision_content: AppSpec content as YAML string
            
        Returns:
            True if successful
        """
        revision = RevisionLocation(
            revision_type=RevisionLocationType.STRING,
            app_spec_content=revision_content
        )
        
        return self.register_application_revision(
            application_name=application_name,
            revision=revision,
            description="ECS AppSpec revision"
        )
    
    def deploy_ecs_revision(
        self,
        application_name: str,
        deployment_group_name: str,
        revision_content: str,
        description: Optional[str] = None
    ) -> DeploymentInfo:
        """
        Deploy an ECS AppSpec revision.
        
        Args:
            application_name: Name of the CodeDeploy application
            deployment_group_name: Name of the deployment group
            revision_content: AppSpec content as YAML string
            description: Description for the deployment
            
        Returns:
            DeploymentInfo object
        """
        revision = RevisionLocation(
            revision_type=RevisionLocationType.STRING,
            app_spec_content=revision_content
        )
        
        return self.create_deployment(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            revision=revision,
            description=description
        )

    # =========================================================================
    # Lambda Deployment Support
    # =========================================================================
    
    def create_lambda_deployment_group(
        self,
        application_name: str,
        deployment_group_name: str,
        service_role_arn: str,
        lambda_functions: List[LambdaFunction],
        deployment_config_name: Optional[str] = None,
        auto_rollback_config: Optional[AutoRollbackConfig] = None,
        alarm_config: Optional[AlarmConfig] = None
    ) -> DeploymentGroupInfo:
        """
        Create a Lambda deployment group.
        
        Args:
            application_name: Name of the CodeDeploy application
            deployment_group_name: Name of the deployment group
            service_role_arn: IAM service role ARN
            lambda_functions: List of Lambda functions to deploy
            deployment_config_name: Deployment configuration name
            auto_rollback_config: Automatic rollback configuration
            alarm_config: Alarm configuration
            
        Returns:
            DeploymentGroupInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        deployment_style = DeploymentStyle(
            deployment_type=DeploymentType.BLUE_GREEN,
            deployment_option=DeploymentOption.WITH_TRAFFIC
        )
        
        target_info = {
            'targetResource': lambda_functions[0].name if lambda_functions else '',
            'lambdaFunctions': [{
                'functionName': f.name,
                'aliasName': f.alias,
                'currentVersion': f.current_version,
                'targetVersion': f.target_version
            } for f in lambda_functions]
        }
        
        return self.create_deployment_group(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            deployment_config_name=deployment_config_name,
            deployment_style=deployment_style,
            service_role_arn=service_role_arn,
            auto_rollback_config=auto_rollback_config,
            alarm_config=alarm_config
        )
    
    def deploy_lambda_revision(
        self,
        application_name: str,
        deployment_group_name: str,
        revision_content: str,
        description: Optional[str] = None
    ) -> DeploymentInfo:
        """
        Deploy a Lambda AppSpec revision.
        
        Args:
            application_name: Name of the CodeDeploy application
            deployment_group_name: Name of the deployment group
            revision_content: AppSpec content as YAML string
            description: Description for the deployment
            
        Returns:
            DeploymentInfo object
        """
        revision = RevisionLocation(
            revision_type=RevisionLocationType.STRING,
            app_spec_content=revision_content
        )
        
        return self.create_deployment(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            revision=revision,
            description=description
        )

    # =========================================================================
    # AppSpec File Management
    # =========================================================================
    
    def create_appspec(
        self,
        version: str = "0.0",
        os: str = "linux",
        files: Optional[List[Dict[str, Any]]] = None,
        hooks: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        resources: Optional[List[Dict[str, Any]]] = None,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create an AppSpec file structure.
        
        Args:
            version: AppSpec version (0.0)
            os: Operating system (linux, windows)
            files: List of files to copy
            hooks: Lifecycle hooks
            resources: ECS or Lambda resources
            permissions: File permissions
            
        Returns:
            AppSpec dictionary
        """
        appspec = {
            'version': version,
            'os': os
        }
        
        if files:
            appspec['files'] = files
        
        if hooks:
            appspec['hooks'] = hooks
        
        if resources:
            appspec['resources'] = resources
        
        if permissions:
            appspec['permissions'] = permissions
        
        return appspec
    
    def create_ec2_appspec(
        self,
        files: List[Dict[str, Any]],
        hooks: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        permissions: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create an EC2 AppSpec file structure.
        
        Args:
            files: List of files to copy
            hooks: Lifecycle hooks
            permissions: File permissions
            
        Returns:
            AppSpec dictionary
        """
        appspec = self.create_appspec(
            version="0.0",
            os="linux",
            files=files,
            hooks=hooks,
            permissions=permissions
        )
        
        AppSpecValidator.validate_ec2_appspec(appspec)
        return appspec
    
    def create_ecs_appspec(
        self,
        resources: List[Dict[str, Any]],
        hooks: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Create an ECS AppSpec file structure.
        
        Args:
            resources: ECS task definition resources
            hooks: Lifecycle hooks (required for ECS)
            
        Returns:
            AppSpec dictionary
        """
        appspec = self.create_appspec(
            version="0.0",
            os="linux",
            hooks=hooks,
            resources=resources
        )
        
        AppSpecValidator.validate_ecs_appspec(appspec)
        return appspec
    
    def create_lambda_appspec(
        self,
        resources: List[Dict[str, Any]],
        hooks: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> Dict[str, Any]:
        """
        Create a Lambda AppSpec file structure.
        
        Args:
            resources: Lambda function resources
            hooks: Lifecycle hooks
            
        Returns:
            AppSpec dictionary
        """
        appspec = self.create_appspec(
            version="0.0",
            hooks=hooks,
            resources=resources
        )
        
        AppSpecValidator.validate_lambda_appspec(appspec)
        return appspec
    
    def appspec_to_yaml(self, appspec: Dict[str, Any]) -> str:
        """
        Convert AppSpec dictionary to YAML string.
        
        Args:
            appspec: AppSpec dictionary
            
        Returns:
            YAML string
        """
        return yaml.dump(appspec, default_flow_style=False, sort_keys=False)
    
    def appspec_to_json(self, appspec: Dict[str, Any]) -> str:
        """
        Convert AppSpec dictionary to JSON string.
        
        Args:
            appspec: AppSpec dictionary
            
        Returns:
            JSON string
        """
        return json.dumps(appspec, indent=2)
    
    def validate_appspec_content(
        self,
        content: str,
        content_type: str = "yaml"
    ) -> Dict[str, Any]:
        """
        Validate AppSpec content.
        
        Args:
            content: AppSpec content string
            content_type: Content type (yaml or json)
            
        Returns:
            Parsed AppSpec dictionary
        """
        if content_type.lower() == "yaml":
            parsed = AppSpecValidator.validate_yaml(content)
        else:
            parsed = AppSpecValidator.validate_json(content)
        
        return parsed
    
    def create_file_hooks(
        self,
        before_block_traffic: Optional[List[Dict[str, Any]]] = None,
        after_block_traffic: Optional[List[Dict[str, Any]]] = None,
        before_allow_traffic: Optional[List[Dict[str, Any]]] = None,
        after_allow_traffic: Optional[List[Dict[str, Any]]] = None,
        application_stop: Optional[List[Dict[str, Any]]] = None,
        before_install: Optional[List[Dict[str, Any]]] = None,
        after_install: Optional[List[Dict[str, Any]]] = None,
        after_decrypt: Optional[List[Dict[str, Any]]] = None,
        validate_service: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create hooks configuration for EC2 deployment.
        
        Args:
            before_block_traffic: Hooks before blocking traffic
            after_block_traffic: Hooks after blocking traffic
            before_allow_traffic: Hooks before allowing traffic
            after_allow_traffic: Hooks after allowing traffic
            application_stop: Hooks for application stop
            before_install: Hooks before installation
            after_install: Hooks after installation
            after_decrypt: Hooks after decryption
            validate_service: Hooks for validation
            
        Returns:
            Hooks dictionary
        """
        hooks = {}
        
        if before_block_traffic:
            hooks['BeforeBlockTraffic'] = before_block_traffic
        if after_block_traffic:
            hooks['AfterBlockTraffic'] = after_block_traffic
        if before_allow_traffic:
            hooks['BeforeAllowTraffic'] = before_allow_traffic
        if after_allow_traffic:
            hooks['AfterAllowTraffic'] = after_allow_traffic
        if application_stop:
            hooks['ApplicationStop'] = application_stop
        if before_install:
            hooks['BeforeInstall'] = before_install
        if after_install:
            hooks['AfterInstall'] = after_install
        if after_decrypt:
            hooks['AfterDecrypt'] = after_decrypt
        if validate_service:
            hooks['ValidateService'] = validate_service
        
        return hooks
    
    def create_ecs_hooks(
        self,
        before_allow_traffic: Optional[List[Dict[str, Any]]] = None,
        after_allow_traffic: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create hooks configuration for ECS deployment.
        
        Args:
            before_allow_traffic: Hooks before allowing traffic
            after_allow_traffic: Hooks after allowing traffic
            
        Returns:
            Hooks dictionary
        """
        hooks = {}
        
        if before_allow_traffic:
            hooks['BeforeAllowTraffic'] = before_allow_traffic
        if after_allow_traffic:
            hooks['AfterAllowTraffic'] = after_allow_traffic
        
        return hooks
    
    def create_lambda_hooks(
        self,
        before_allow_traffic: Optional[List[Dict[str, Any]]] = None,
        after_allow_traffic: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create hooks configuration for Lambda deployment.
        
        Args:
            before_allow_traffic: Hooks before allowing traffic
            after_allow_traffic: Hooks after allowing traffic
            
        Returns:
            Hooks dictionary
        """
        return self.create_ecs_hooks(before_allow_traffic, after_allow_traffic)
    
    def upload_appspec_to_s3(
        self,
        appspec_content: str,
        bucket_name: str,
        key: str,
        content_type: str = "yaml"
    ) -> S3Location:
        """
        Upload AppSpec file to S3.
        
        Args:
            appspec_content: AppSpec content as string
            bucket_name: S3 bucket name
            key: S3 object key
            content_type: Content type (yaml or json)
            
        Returns:
            S3Location object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for S3 operations")
        
        try:
            content = appspec_content.encode('utf-8')
            bundle_type = "zip" if content_type == "yaml" else content_type
            
            self.s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=content,
                ContentType='text/plain'
            )
            
            return S3Location(
                bucket=bucket_name,
                key=key,
                bundle_type=bundle_type
            )
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to upload AppSpec to S3: {e}")
            raise

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def create_deployment_trigger(
        self,
        application_name: str,
        deployment_group_name: str,
        trigger_name: str,
        trigger_target_arn: Optional[str] = None,
        trigger_events: Optional[List[str]] = None
    ) -> bool:
        """
        Create a notification trigger for deployments.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            trigger_name: Name of the trigger
            trigger_target_arn: ARN to notify (SNS topic ARN)
            trigger_events: List of events to trigger on
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        if trigger_events is None:
            trigger_events = [
                "DeploymentStart",
                "DeploymentSuccess",
                "DeploymentFailure",
                "DeploymentStop",
                "DeploymentRollback",
                "DeploymentReady"
            ]
        
        trigger_config = {
            'triggerName': trigger_name,
            'triggerTargetArn': trigger_target_arn,
            'triggerEvents': trigger_events
        }
        
        try:
            self.codedeploy.update_deployment_group(
                applicationName=application_name,
                deploymentGroupName=deployment_group_name,
                triggerConfiguration=trigger_config
            )
            
            logger.info(f"Created deployment trigger: {trigger_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create deployment trigger: {e}")
            raise
    
    def create_cloudwatch_event_rule(
        self,
        name: str,
        description: Optional[str] = None,
        event_pattern: Optional[str] = None
    ) -> str:
        """
        Create a CloudWatch event rule for deployment events.
        
        Args:
            name: Name of the rule
            description: Description of the rule
            event_pattern: Event pattern to match
            
        Returns:
            Rule ARN
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CloudWatch Events operations")
        
        if event_pattern is None:
            event_pattern = json.dumps({
                "source": ["aws.codedeploy"],
                "detail-type": ["CodeDeploy Deployment State-change Notification"]
            })
        
        params = {
            'Name': name,
            'EventPattern': event_pattern
        }
        
        if description:
            params['Description'] = description
        
        try:
            response = self.events.put_rule(**params)
            rule_arn = response.get('RuleArn', '')
            
            logger.info(f"Created CloudWatch event rule: {name}")
            return rule_arn
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create CloudWatch event rule: {e}")
            raise
    
    def add_cloudwatch_event_target(
        self,
        rule_name: str,
        target_arn: str,
        target_id: Optional[str] = None,
        input_template: Optional[str] = None
    ) -> bool:
        """
        Add a target to a CloudWatch event rule.
        
        Args:
            rule_name: Name of the rule
            target_arn: ARN of the target
            target_id: Optional target ID
            input_template: Optional input template
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CloudWatch Events operations")
        
        target = {
            'Id': target_id or str(uuid.uuid4()),
            'Arn': target_arn
        }
        
        if input_template:
            target['InputTransformer'] = {
                'InputTemplate': input_template,
                'InputPathsMap': {}
            }
        
        try:
            self.events.put_targets(
                Rule=rule_name,
                Targets=[target]
            )
            
            logger.info(f"Added CloudWatch event target to rule: {rule_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add CloudWatch event target: {e}")
            raise
    
    def put_cloudwatch_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        statistic: str = "Average",
        period: int = 300,
        threshold: float = 1.0,
        comparison_operator: str = "LessThanThreshold",
        evaluation_periods: int = 1
    ) -> bool:
        """
        Create a CloudWatch alarm for deployment monitoring.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Name of the metric
            namespace: Namespace of the metric
            statistic: Statistic to use
            period: Period in seconds
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CloudWatch operations")
        
        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace=namespace,
                Statistic=statistic,
                Period=period,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods
            )
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            raise
    
    def get_deployment_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_name: str = "Deployments"
    ) -> Dict[str, Any]:
        """
        Get deployment metrics from CloudWatch.
        
        Args:
            start_time: Start time for metrics
            end_time: End time for metrics
            metric_name: Name of the metric
            
        Returns:
            Metrics data
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CloudWatch operations")
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/CodeDeploy',
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum', 'Average']
            )
            
            return {
                'label': response.get('Label', ''),
                'datapoints': response.get('Datapoints', [])
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get deployment metrics: {e}")
            raise

    # =========================================================================
    # Rollback Management
    # =========================================================================
    
    def configure_auto_rollback(
        self,
        application_name: str,
        deployment_group_name: str,
        enabled: bool = True,
        events: Optional[List[str]] = None
    ) -> bool:
        """
        Configure automatic rollback for a deployment group.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            enabled: Enable automatic rollback
            events: Events to trigger rollback (DEPLOYMENT_FAILURE, DEPLOYMENT_STOP_ON_REQUEST)
            
        Returns:
            True if successful
        """
        if events is None:
            events = ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_REQUEST"]
        
        auto_rollback_config = AutoRollbackConfig(
            enabled=enabled,
            events=events
        )
        
        return self.update_deployment_group(
            application_name=application_name,
            deployment_group_name=deployment_group_name,
            auto_rollback_config=auto_rollback_config
        )
    
    def rollback_deployment(
        self,
        application_name: str,
        deployment_group_name: str
    ) -> Optional[DeploymentInfo]:
        """
        Rollback a deployment to the last successful deployment.
        
        Args:
            application_name: Name of the application
            deployment_group_name: Name of the deployment group
            
        Returns:
            New rollback DeploymentInfo object or None
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeDeploy operations")
        
        try:
            response = self.codedeploy.stop_deployment(
                deploymentId='',
                autoRollbackEnabled=True
            )
            
            deployments = self.list_deployments(
                application_name=application_name,
                deployment_group_name=deployment_group_name
            )
            
            if not deployments:
                logger.warning(f"No deployments found for rollback: {application_name}/{deployment_group_name}")
                return None
            
            last_deployment = self.get_deployment(deployments[0])
            if last_deployment and last_deployment.rollback_info:
                rollback_deployment_id = last_deployment.rollback_info.get('rollbackDeploymentId')
                if rollback_deployment_id:
                    return self.get_deployment(rollback_deployment_id)
            
            logger.info(f"Initiated rollback for: {application_name}/{deployment_group_name}")
            return last_deployment
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to rollback deployment: {e}")
            raise
    
    def get_rollback_info(
        self,
        deployment_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get rollback information for a deployment.
        
        Args:
            deployment_id: ID of the deployment
            
        Returns:
            Rollback information dictionary or None
        """
        deployment = self.get_deployment(deployment_id)
        if deployment:
            return deployment.rollback_info
        return None

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def wait_for_deployment(
        self,
        deployment_id: str,
        timeout: int = 3600,
        poll_interval: int = 15
    ) -> Optional[DeploymentInfo]:
        """
        Wait for a deployment to complete.
        
        Args:
            deployment_id: ID of the deployment
            timeout: Maximum time to wait in seconds
            poll_interval: Time between polls in seconds
            
        Returns:
            Final DeploymentInfo object or None on timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            deployment = self.get_deployment(deployment_id)
            
            if deployment and deployment.status in [
                DeploymentStatus.SUCCEEDED.value,
                DeploymentStatus.FAILED.value,
                DeploymentStatus.STOPPED.value
            ]:
                return deployment
            
            time.sleep(poll_interval)
        
        logger.warning(f"Timeout waiting for deployment: {deployment_id}")
        return None
    
    def get_deployment_status(
        self,
        deployment_id: str
    ) -> Optional[str]:
        """
        Get the status of a deployment.
        
        Args:
            deployment_id: ID of the deployment
            
        Returns:
            Status string or None
        """
        deployment = self.get_deployment(deployment_id)
        if deployment:
            return deployment.status
        return None
    
    def get_deployment_target_groups(
        self,
        deployment_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get target group information for a deployment.
        
        Args:
            deployment_id: ID of the deployment
            
        Returns:
            Target group information or None
        """
        deployment = self.get_deployment(deployment_id)
        if deployment:
            return deployment.load_balancer_info
        return None
    
    def batch_get_deployments(
        self,
        deployment_ids: List[str]
    ) -> List[DeploymentInfo]:
        """
        Get information for multiple deployments.
        
        Args:
            deployment_ids: List of deployment IDs
            
        Returns:
            List of DeploymentInfo objects
        """
        deployments = []
        for dep_id in deployment_ids:
            dep = self.get_deployment(dep_id)
            if dep:
                deployments.append(dep)
        return deployments
    
    def cleanup_caches(self) -> bool:
        """
        Clear internal caches.
        
        Returns:
            True if successful
        """
        with self._lock:
            self._applications_cache.clear()
            self._deployment_groups_cache.clear()
            self._deployments_cache.clear()
            self._deployment_configs_cache.clear()
        
        logger.info("Cleaned up CodeDeploy caches")
        return True
