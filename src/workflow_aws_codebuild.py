"""
AWS CodeBuild Serverless Integration Module for Workflow System

Implements a CodeBuildIntegration class with:
1. Project management: Create/manage CodeBuild projects
2. Build management: Start/manage builds
3. Build specs: Manage build specifications
4. Source providers: GitHub, CodeCommit, S3 sources
5. Environment: Build environment configuration
6. Artifacts: Build artifact management
7. Logs: CloudWatch logs integration
8. Webhooks: GitHub webhooks
9. Caching: Build caching
10. Reports: Test report integration

Commit: 'feat(aws-codebuild): add AWS CodeBuild integration with project management, builds, build specs, sources, environment, artifacts, logs, webhooks, caching, reports'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
import os
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


class SourceType(Enum):
    """Source type for CodeBuild projects."""
    CODECOMMIT = "CODECOMMIT"
    CODEPIPELINE = "CODEPIPELINE"
    GITHUB = "GITHUB"
    GITHUB_ENTERPRISE = "GITHUB_ENTERPRISE"
    BITBUCKET = "BITBUCKET"
    S3 = "S3"
    NO_SOURCE = "NO_SOURCE"


class SourceAuthType(Enum):
    """Source authentication types."""
    OAUTH = "OAUTH"
    BASIC_AUTH = "BASIC_AUTH"
    PERSONAL_ACCESS_TOKEN = "PERSONAL_ACCESS_TOKEN"
    SECRETS_MANAGER = "SECRETS_MANAGER"


class EnvironmentType(Enum):
    """Environment type for build images."""
    LINUX_CONTAINER = "LINUX_CONTAINER"
    LINUX_GPU_CONTAINER = "LINUX_GPU_CONTAINER"
    ARM_CONTAINER = "ARM_CONTAINER"
    WINDOWS_CONTAINER = "WINDOWS_CONTAINER"
    WINDOWS_SERVER_2019_CONTAINER = "WINDOWS_SERVER_2019_CONTAINER"
    LINUX_EC2 = "LINUX_EC2"
    ARM_EC2 = "ARM_EC2"
    WINDOWS_EC2 = "WINDOWS_EC2"


class ComputeType(Enum):
    """Compute type for build environment."""
    BUILD_GENERAL_SMALL = "BUILD_GENERAL_SMALL"
    BUILD_GENERAL_MEDIUM = "BUILD_GENERAL_MEDIUM"
    BUILD_GENERAL_LARGE = "BUILD_GENERAL_LARGE"
    BUILD_GENERAL_XLARGE = "BUILD_GENERAL_XLARGE"
    BUILD_GENERAL_2XLARGE = "BUILD_GENERAL_2XLARGE"


class CacheType(Enum):
    """Cache type for build projects."""
    NO_CACHE = "NO_CACHE"
    S3 = "S3"
    LOCAL = "LOCAL"


class LocalCacheMode(Enum):
    """Local cache mode options."""
    LOCAL_SOURCE_MODE = "LOCAL_SOURCE_MODE"
    LOCAL_DOCKER_LAYER_MODE = "LOCAL_DOCKER_LAYER_MODE"
    LOCAL_CUSTOM_CACHE_MODE = "LOCAL_CUSTOM_CACHE_MODE"


class BuildStatus(Enum):
    """Build status values."""
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    FAULT = "FAULT"
    TIMED_OUT = "TIMED_OUT"
    IN_PROGRESS = "IN_PROGRESS"
    STOPPED = "STOPPED"


class ReportType(Enum):
    """Test report type."""
    TEST = "TEST"
    CODE_COVERAGE = "CODE_COVERAGE"


class ReportStatus(Enum):
    """Report status."""
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    FAILED = "FAILED"


class ArtifactType(Enum):
    """Artifact type."""
    CODEPIPELINE = "CODEPIPELINE"
    S3 = "S3"
    NO_ARTIFACTS = "NO_ARTIFACTS"


class EncryptionKeyType(Enum):
    """Encryption key type."""
    KMS = "KMS"
    S3 = "S3"


class WebhookFilterType(Enum):
    """Webhook filter types."""
    EVENT = "EVENT"
    BASE_REF = "BASE_REF"
    HEAD_REF = "HEAD_REF"
    ACTOR_ACCOUNT_ID = "ACTOR_ACCOUNT_ID"
    FILE_PATH = "FILE_PATH"
    COMMIT_MESSAGE = "COMMIT_MESSAGE"


class WebhookBuildType(Enum):
    """Webhook build type."""
    BUILD = "BUILD"
    BUILD_BATCH = "BUILD_BATCH"


@dataclass
class CodeBuildConfig:
    """Configuration for CodeBuild connection."""
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
class SourceConfig:
    """Configuration for build source."""
    source_type: Union[SourceType, str]
    location: Optional[str] = None
    git_clone_depth: Optional[int] = None
    git_submodules_config: Optional[Dict[str, Any]] = None
    build_status_config: Optional[Dict[str, Any]] = None
    auth: Optional[Dict[str, Any]] = None
    report_build_status: bool = False


@dataclass
class GitHubSourceConfig:
    """Configuration for GitHub source."""
    owner: str
    repo: str
    branch: str
    oauth_token: Optional[str] = None
    personal_access_token: Optional[str] = None
    webhook_secret: Optional[str] = None
    url: Optional[str] = None


@dataclass
class CodeCommitSourceConfig:
    """Configuration for CodeCommit source."""
    repository: str
    branch: str
    repository_owner: Optional[str] = None


@dataclass
class S3SourceConfig:
    """Configuration for S3 source."""
    bucket: str
    path: str
    key: Optional[str] = None
    version_id: Optional[str] = None


@dataclass
class EnvironmentConfig:
    """Configuration for build environment."""
    environment_type: Union[EnvironmentType, str] = EnvironmentType.LINUX_CONTAINER
    image: str = "aws/codebuild/standard:5.0"
    compute_type: Union[ComputeType, str] = ComputeType.BUILD_GENERAL_SMALL
    privileged_mode: bool = False
    environment_variables: Dict[str, str] = field(default_factory=dict)
    registry_credential: Optional[Dict[str, str]] = None
    image_pull_credentials_type: str = "CODEBUILD"


@dataclass
class ArtifactsConfig:
    """Configuration for build artifacts."""
    artifacts_type: Union[ArtifactType, str] = ArtifactType.NO_ARTIFACTS
    location: Optional[str] = None
    name: Optional[str] = None
    namespace_type: str = "BUILD_ID"
    packaging: str = "NONE"
    path: Optional[str] = None
    encryption_disabled: bool = False
    artifact_identifier: Optional[str] = None


@dataclass
class CacheConfig:
    """Configuration for build caching."""
    cache_type: Union[CacheType, str] = CacheType.NO_CACHE
    location: Optional[str] = None
    modes: List[Union[LocalCacheMode, str]] = field(default_factory=list)


@dataclass
class LogsConfig:
    """Configuration for CloudWatch logs."""
    cloud_watch_logs_enabled: bool = True
    cloud_watch_logs_group_name: Optional[str] = None
    cloud_watch_logs_status: str = "ENABLED"
    s3_logs_enabled: bool = False
    s3_logs_location: Optional[str] = None
    s3_logs_status: str = "DISABLED"
    encryption_disabled: bool = False


@dataclass
class WebhookConfig:
    """Configuration for GitHub webhooks."""
    filter_groups: List[List[Dict[str, Any]]] = field(default_factory=list)
    webhook_secret: Optional[str] = None
    retry_limit: int = 2


@dataclass
class ReportGroupConfig:
    """Configuration for test report groups."""
    name: str
    report_type: Union[ReportType, str] = ReportType.TEST
    export_config: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    delete_reports: bool = True


@dataclass
class ProjectConfig:
    """Configuration for creating a CodeBuild project."""
    name: str
    description: str = ""
    source: Optional[SourceConfig] = None
    environment: Optional[EnvironmentConfig] = None
    artifacts: Optional[ArtifactsConfig] = None
    cache: Optional[CacheConfig] = None
    logs: Optional[LogsConfig] = None
    vpc_config: Optional[Dict[str, Any]] = None
    timeout_in_minutes: int = 60
    queued_timeout_in_minutes: int = 480
    encryption_key: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    badge_enabled: bool = False
    concurrent_build_limit: int = 1
    project_visibility: str = "PRIVATE"
    resource_access_role: Optional[str] = None


@dataclass
class ProjectInfo:
    """Information about a CodeBuild project."""
    name: str
    arn: str
    description: str
    source: Dict[str, Any]
    environment: Dict[str, Any]
    artifacts: Dict[str, Any]
    cache: Dict[str, Any]
    logs_config: Dict[str, Any]
    vpc_config: Optional[Dict[str, Any]] = None
    timeout_in_minutes: int = 60
    queued_timeout_in_minutes: int = 480
    created: str
    last_modified: str
    tags: Dict[str, str] = field(default_factory=dict)
    project_visibility: Optional[str] = None
    badge: Optional[Dict[str, Any]] = None


@dataclass
class BuildInfo:
    """Information about a build."""
    id: str
    arn: str
    project_name: str
    build_number: int
    build_status: Optional[str] = None
    source_version: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    current_phase: Optional[str] = None
    source: Dict[str, Any] = field(default_factory=dict)
    environment: Dict[str, Any] = field(default_factory=dict)
    artifacts: Optional[Dict[str, Any]] = None
    logs: Optional[Dict[str, Any]] = None
    cache: Optional[Dict[str, Any]] = None
    phases: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReportGroupInfo:
    """Information about a report group."""
    arn: str
    name: str
    type: str
    export_config: Dict[str, Any]
    created: str
    last_modified: str
    tags: Dict[str, str] = field(default_factory=dict)


class CodeBuildIntegration:
    """
    AWS CodeBuild integration class for build automation.
    
    Supports:
    - Project creation, update, delete, and management
    - Build execution, monitoring, and management
    - Build specification management (buildspec.yml)
    - Source provider integration (GitHub, CodeCommit, S3)
    - Environment configuration (compute, images, variables)
    - Artifact management (S3, CodePipeline)
    - CloudWatch Logs integration
    - GitHub webhook configuration
    - Build caching (S3, local)
    - Test report generation and management
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        codebuild_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None,
        logs_client: Optional[Any] = None,
        s3_client: Optional[Any] = None,
        iam_client: Optional[Any] = None
    ):
        """
        Initialize CodeBuild integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: CodeBuild endpoint URL (for testing with LocalStack, etc.)
            codebuild_client: Pre-configured CodeBuild client (overrides boto3 creation)
            cloudwatch_client: Pre-configured CloudWatch client
            logs_client: Pre-configured CloudWatch Logs client
            s3_client: Pre-configured S3 client
            iam_client: Pre-configured IAM client
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        
        self._clients = {}
        self._codebuild_client = codebuild_client
        self._cloudwatch_client = cloudwatch_client
        self._logs_client = logs_client
        self._s3_client = s3_client
        self._iam_client = iam_client
        
        self._lock = threading.Lock()
        self._projects_cache: Dict[str, ProjectInfo] = {}
        self._builds_cache: Dict[str, List[BuildInfo]] = defaultdict(list)
        
        if BOTO3_AVAILABLE and codebuild_client is None:
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
            
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs['aws_access_key_id'] = self.aws_access_key_id
                session_kwargs['aws_secret_access_key'] = self.aws_secret_access_key
                if self.aws_session_token:
                    session_kwargs['aws_session_token'] = self.aws_session_token
            
            try:
                session = boto3.Session(**session_kwargs)
                
                if self._codebuild_client is None:
                    self._clients['codebuild'] = session.client(
                        'codebuild',
                        endpoint_url=self.endpoint_url
                    )
                
                if self._cloudwatch_client is None:
                    self._clients['cloudwatch'] = session.client('cloudwatch')
                
                if self._logs_client is None:
                    self._clients['logs'] = session.client('logs')
                
                if self._s3_client is None:
                    self._clients['s3'] = session.client('s3')
                
                if self._iam_client is None:
                    self._clients['iam'] = session.client('iam')
                    
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to initialize AWS clients: {e}")
                raise
    
    @property
    def codebuild(self):
        """Get CodeBuild client."""
        if self._codebuild_client is None:
            if 'codebuild' not in self._clients:
                self._initialize_clients()
            self._codebuild_client = self._clients.get('codebuild')
        return self._codebuild_client
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        if self._cloudwatch_client is None:
            if 'cloudwatch' not in self._clients:
                self._initialize_clients()
            self._cloudwatch_client = self._clients.get('cloudwatch')
        return self._cloudwatch_client
    
    @property
    def logs(self):
        """Get CloudWatch Logs client."""
        if self._logs_client is None:
            if 'logs' not in self._clients:
                self._initialize_clients()
            self._logs_client = self._clients.get('logs')
        return self._logs_client
    
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
    # Project Management
    # =========================================================================
    
    def create_project(self, config: ProjectConfig) -> ProjectInfo:
        """
        Create a new CodeBuild project.
        
        Args:
            config: Project configuration
            
        Returns:
            ProjectInfo object with created project details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        project_config = {
            'name': config.name,
            'description': config.description,
            'timeoutInMinutes': config.timeout_in_minutes,
            'queuedTimeoutInMinutes': config.queued_timeout_in_minutes,
            'encryptionKey': config.encryption_key,
            'tags': [{'key': k, 'value': v} for k, v in config.tags.items()],
            'badgeEnabled': config.badge_enabled,
            'concurrentBuildLimit': config.concurrent_build_limit,
        }
        
        if config.source:
            project_config['source'] = self._build_source_dict(config.source)
        
        if config.environment:
            project_config['environment'] = self._build_environment_dict(config.environment)
        
        if config.artifacts:
            project_config['artifacts'] = self._build_artifacts_dict(config.artifacts)
        
        if config.cache:
            project_config['cache'] = self._build_cache_dict(config.cache)
        
        if config.logs:
            project_config['logsConfig'] = self._build_logs_dict(config.logs)
        
        if config.vpc_config:
            project_config['vpcConfig'] = config.vpc_config
        
        try:
            response = self.codebuild.create_project(**project_config)
            project = response['project']
            return self._parse_project_info(project)
        except ClientError as e:
            logger.error(f"Failed to create project {config.name}: {e}")
            raise
    
    def update_project(self, name: str, updates: Dict[str, Any]) -> ProjectInfo:
        """
        Update an existing CodeBuild project.
        
        Args:
            name: Project name
            updates: Dictionary of fields to update
            
        Returns:
            ProjectInfo object with updated project details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        update_config = {'name': name}
        update_config.update(updates)
        
        try:
            response = self.codebuild.update_project(**update_config)
            project = response['project']
            return self._parse_project_info(project)
        except ClientError as e:
            logger.error(f"Failed to update project {name}: {e}")
            raise
    
    def delete_project(self, name: str) -> bool:
        """
        Delete a CodeBuild project.
        
        Args:
            name: Project name
            
        Returns:
            True if deletion was successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            self.codebuild.delete_project(name=name)
            with self._lock:
                self._projects_cache.pop(name, None)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete project {name}: {e}")
            raise
    
    def get_project(self, name: str) -> ProjectInfo:
        """
        Get information about a CodeBuild project.
        
        Args:
            name: Project name
            
        Returns:
            ProjectInfo object with project details
        """
        with self._lock:
            if name in self._projects_cache:
                return self._projects_cache[name]
        
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_projects(names=[name])
            if not response['projects']:
                raise ValueError(f"Project {name} not found")
            
            project = response['projects'][0]
            project_info = self._parse_project_info(project)
            
            with self._lock:
                self._projects_cache[name] = project_info
            
            return project_info
        except ClientError as e:
            logger.error(f"Failed to get project {name}: {e}")
            raise
    
    def list_projects(self, sort_by: str = "NAME", ascending: bool = True) -> List[str]:
        """
        List all CodeBuild projects.
        
        Args:
            sort_by: Field to sort by (NAME, CREATED_TIME, LAST_MODIFIED)
            ascending: Sort in ascending order
            
        Returns:
            List of project names
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.list_projects(
                sortBy=sort_by,
                sortOrder='ASCENDING' if ascending else 'DESCENDING'
            )
            return response['projects']
        except ClientError as e:
            logger.error(f"Failed to list projects: {e}")
            raise
    
    def batch_get_projects(self, names: List[str]) -> List[ProjectInfo]:
        """
        Get information about multiple projects.
        
        Args:
            names: List of project names
            
        Returns:
            List of ProjectInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_projects(names=names)
            return [self._parse_project_info(p) for p in response['projects']]
        except ClientError as e:
            logger.error(f"Failed to batch get projects: {e}")
            raise
    
    # =========================================================================
    # Build Management
    # =========================================================================
    
    def start_build(
        self,
        project_name: str,
        source_version: Optional[str] = None,
        artifacts_override: Optional[Dict[str, Any]] = None,
        environment_override: Optional[Dict[str, Any]] = None,
        source_override: Optional[Dict[str, Any]] = None,
        buildspec_override: Optional[str] = None,
        timeout_in_minutes_override: Optional[int] = None,
        queue_position_override: Optional[int] = None,
        comment: Optional[str] = None
    ) -> BuildInfo:
        """
        Start a new build.
        
        Args:
            project_name: Project name
            source_version: Source version (commit ID, branch, tag)
            artifacts_override: Override artifact settings
            environment_override: Override environment settings
            source_override: Override source settings
            buildspec_override: Override buildspec content
            timeout_in_minutes_override: Override timeout
            queue_position_override: Override queue position
            comment: Build comment
            
        Returns:
            BuildInfo object with build details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        build_params = {'projectName': project_name}
        
        if source_version:
            build_params['sourceVersion'] = source_version
        
        if artifacts_override:
            build_params['artifactsOverride'] = artifacts_override
        
        if environment_override:
            build_params['environmentOverride'] = environment_override
        
        if source_override:
            build_params['sourceOverride'] = source_override
        
        if buildspec_override:
            build_params['buildspecOverride'] = buildspec_override
        
        if timeout_in_minutes_override:
            build_params['timeoutInMinutesOverride'] = timeout_in_minutes_override
        
        if queue_position_override is not None:
            build_params['queuePositionOverride'] = queue_position_override
        
        if comment:
            build_params['comment'] = comment
        
        try:
            response = self.codebuild.start_build(**build_params)
            build = response['build']
            return self._parse_build_info(build)
        except ClientError as e:
            logger.error(f"Failed to start build for project {project_name}: {e}")
            raise
    
    def start_batch_build(
        self,
        project_name: str,
        batch_timeout_in_minutes: Optional[int] = None,
        compute_type_override: Optional[str] = None,
        environment_type_override: Optional[str] = None,
        source_version: Optional[str] = None,
        buildspec_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Start a batch build.
        
        Args:
            project_name: Project name
            batch_timeout_in_minutes: Batch timeout in minutes
            compute_type_override: Override compute type
            environment_type_override: Override environment type
            source_version: Source version
            buildspec_file: Path to buildspec file
            
        Returns:
            Batch build response
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        batch_params = {'projectName': project_name}
        
        if batch_timeout_in_minutes:
            batch_params['batchTimeoutInMinutes'] = batch_timeout_in_minutes
        
        if compute_type_override:
            batch_params['computeTypeOverride'] = compute_type_override
        
        if environment_type_override:
            batch_params['environmentTypeOverride'] = environment_type_override
        
        if source_version:
            batch_params['sourceVersion'] = source_version
        
        if buildspec_file:
            batch_params['buildspecOverride'] = buildspec_file
        
        try:
            response = self.codebuild.start_build_batch(**batch_params)
            return response['buildBatch']
        except ClientError as e:
            logger.error(f"Failed to start batch build for project {project_name}: {e}")
            raise
    
    def stop_build(self, id: str) -> BuildInfo:
        """
        Stop a running build.
        
        Args:
            id: Build ID
            
        Returns:
            BuildInfo object with final build details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.stop_build(id=id)
            return self._parse_build_info(response['build'])
        except ClientError as e:
            logger.error(f"Failed to stop build {id}: {e}")
            raise
    
    def stop_batch_build(self, id: str) -> Dict[str, Any]:
        """
        Stop a running batch build.
        
        Args:
            id: Build batch ID
            
        Returns:
            Build batch details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.stop_build_batch(id=id)
            return response['buildBatch']
        except ClientError as e:
            logger.error(f"Failed to stop batch build {id}: {e}")
            raise
    
    def get_build(self, id: str) -> BuildInfo:
        """
        Get information about a build.
        
        Args:
            id: Build ID
            
        Returns:
            BuildInfo object with build details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_builds(ids=[id])
            if not response['builds']:
                raise ValueError(f"Build {id} not found")
            return self._parse_build_info(response['builds'][0])
        except ClientError as e:
            logger.error(f"Failed to get build {id}: {e}")
            raise
    
    def list_builds(
        self,
        project_name: Optional[str] = None,
        sort_by: str = "BUILD_ID",
        ascending: bool = True
    ) -> List[str]:
        """
        List builds for a project or all projects.
        
        Args:
            project_name: Project name (optional for all projects)
            sort_by: Field to sort by
            ascending: Sort in ascending order
            
        Returns:
            List of build IDs
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            if project_name:
                response = self.codebuild.list_builds_for_project(
                    projectName=project_name,
                    sortBy=sort_by,
                    sortOrder='ASCENDING' if ascending else 'DESCENDING'
                )
            else:
                response = self.codebuild.list_builds(
                    sortBy=sort_by,
                    sortOrder='ASCENDING' if ascending else 'DESCENDING'
                )
            return response['ids']
        except ClientError as e:
            logger.error(f"Failed to list builds: {e}")
            raise
    
    def batch_get_builds(self, ids: List[str]) -> List[BuildInfo]:
        """
        Get information about multiple builds.
        
        Args:
            ids: List of build IDs
            
        Returns:
            List of BuildInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_builds(ids=ids)
            return [self._parse_build_info(b) for b in response['builds']]
        except ClientError as e:
            logger.error(f"Failed to batch get builds: {e}")
            raise
    
    def get_build_batch(self, id: str) -> Dict[str, Any]:
        """
        Get information about a batch build.
        
        Args:
            id: Build batch ID
            
        Returns:
            Build batch details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_builds(ids=[id])
            if not response['builds']:
                raise ValueError(f"Build batch {id} not found")
            return response['builds'][0]
        except ClientError as e:
            logger.error(f"Failed to get build batch {id}: {e}")
            raise
    
    def get_build_history(
        self,
        project_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        max_results: int = 100
    ) -> List[BuildInfo]:
        """
        Get build history for a project.
        
        Args:
            project_name: Project name
            start_time: Start time filter
            end_time: End time filter
            max_results: Maximum number of results
            
        Returns:
            List of BuildInfo objects
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            kwargs = {
                'projectName': project_name,
                'maxResults': min(max_results, 100)
            }
            
            if start_time:
                kwargs['startTime'] = start_time.isoformat()
            
            if end_time:
                kwargs['endTime'] = end_time.isoformat()
            
            builds = []
            paginator = self.codebuild.get_paginator('list_builds_for_project')
            
            for page in paginator.paginate(**kwargs):
                build_ids = page.get('ids', [])
                if build_ids:
                    batch = self.batch_get_builds(build_ids)
                    builds.extend(batch)
            
            return builds
        except ClientError as e:
            logger.error(f"Failed to get build history for {project_name}: {e}")
            raise
    
    # =========================================================================
    # Build Specs Management
    # =========================================================================
    
    def validate_buildspec(self, buildspec: Union[str, Dict[str, Any]]) -> bool:
        """
        Validate a buildspec.
        
        Args:
            buildspec: Buildspec content (YAML string or dict)
            
        Returns:
            True if valid
        """
        try:
            if isinstance(buildspec, str):
                import yaml
                parsed = yaml.safe_load(buildspec)
                buildspec = parsed
            
            required_keys = ['version', 'phases']
            if not all(key in buildspec for key in required_keys):
                return False
            
            if 'phases' in buildspec:
                valid_phases = ['install', 'pre_build', 'build', 'post_build']
                for phase_name in buildspec['phases']:
                    if phase_name not in valid_phases:
                        return False
            
            return True
        except Exception:
            return False
    
    def generate_buildspec(
        self,
        version: str = "0.2",
        install_commands: Optional[List[str]] = None,
        pre_build_commands: Optional[List[str]] = None,
        build_commands: Optional[List[str]] = None,
        post_build_commands: Optional[List[str]] = None,
        env_variables: Optional[Dict[str, str]] = None,
        phases: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate a buildspec from components.
        
        Args:
            version: Buildspec version
            install_commands: Commands to run during install phase
            pre_build_commands: Commands to run during pre_build phase
            build_commands: Commands to run during build phase
            post_build_commands: Commands to run during post_build phase
            env_variables: Environment variables
            phases: Custom phase definitions
            
        Returns:
            Buildspec dictionary
        """
        buildspec = {
            'version': version,
            'env': {}
        }
        
        if env_variables:
            buildspec['env'] = {
                'variables': env_variables
            }
        
        if phases:
            buildspec['phases'] = phases
        else:
            buildspec['phases'] = {}
            
            def add_phase(name: str, commands: Optional[List[str]]) -> Dict[str, Any]:
                if commands:
                    return {
                        name: {
                            'commands': commands
                        }
                    }
                return {}
            
            buildspec['phases'].update(add_phase('install', install_commands))
            buildspec['phases'].update(add_phase('pre_build', pre_build_commands))
            buildspec['phases'].update(add_phase('build', build_commands))
            buildspec['phases'].update(add_phase('post_build', post_build_commands))
        
        return buildspec
    
    def get_buildspec_from_s3(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retrieve buildspec from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            version_id: Object version ID
            
        Returns:
            Parsed buildspec dictionary
        """
        try:
            kwargs = {'Bucket': bucket, 'Key': key}
            if version_id:
                kwargs['VersionId'] = version_id
            
            response = self.s3.get_object(**kwargs)
            content = response['Body'].read().decode('utf-8')
            
            import yaml
            return yaml.safe_load(content)
        except ClientError as e:
            logger.error(f"Failed to get buildspec from S3: {e}")
            raise
    
    def put_buildspec_to_s3(
        self,
        buildspec: Dict[str, Any],
        bucket: str,
        key: str,
        format: str = "yaml"
    ) -> str:
        """
        Upload buildspec to S3.
        
        Args:
            buildspec: Buildspec dictionary
            bucket: S3 bucket name
            key: Object key
            format: Output format (yaml or json)
            
        Returns:
            S3 object version ID
        """
        try:
            if format == "yaml":
                import yaml
                content = yaml.dump(buildspec)
            else:
                content = json.dumps(buildspec)
            
            response = self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode('utf-8')
            )
            return response.get('VersionId', '')
        except ClientError as e:
            logger.error(f"Failed to put buildspec to S3: {e}")
            raise
    
    # =========================================================================
    # Source Provider Management
    # =========================================================================
    
    def configure_github_source(
        self,
        owner: str,
        repo: str,
        branch: str,
        oauth_token: Optional[str] = None,
        personal_access_token: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        report_build_status: bool = True
    ) -> SourceConfig:
        """
        Configure GitHub as a source provider.
        
        Args:
            owner: GitHub owner
            repo: GitHub repository
            branch: Branch to build
            oauth_token: GitHub OAuth token
            personal_access_token: GitHub personal access token
            webhook_secret: Webhook secret for triggering builds
            report_build_status: Report build status to GitHub
            
        Returns:
            SourceConfig object
        """
        auth = {}
        if oauth_token:
            auth = {
                'type': 'OAUTH',
                'resource': oauth_token
            }
        elif personal_access_token:
            auth = {
                'type': 'PERSONAL_ACCESS_TOKEN',
                'resource': personal_access_token
            }
        
        return SourceConfig(
            source_type=SourceType.GITHUB,
            location=f"https://github.com/{owner}/{repo}",
            auth=auth if auth else None,
            report_build_status=report_build_status,
            build_status_config={
                'context': 'continuous-build',
                'targetUrl': ''
            } if report_build_status else None
        )
    
    def configure_codecommit_source(
        self,
        repository: str,
        branch: str,
        repository_owner: Optional[str] = None,
        git_clone_depth: int = 1
    ) -> SourceConfig:
        """
        Configure CodeCommit as a source provider.
        
        Args:
            repository: CodeCommit repository name
            branch: Branch to build
            repository_owner: Repository owner AWS account ID
            git_clone_depth: Git clone depth
            
        Returns:
            SourceConfig object
        """
        return SourceConfig(
            source_type=SourceType.CODECOMMIT,
            location=repository,
            git_clone_depth=git_clone_depth,
            report_build_status=False
        )
    
    def configure_s3_source(
        self,
        bucket: str,
        path: str,
        key: Optional[str] = None,
        version_id: Optional[str] = None
    ) -> SourceConfig:
        """
        Configure S3 as a source provider.
        
        Args:
            bucket: S3 bucket name
            path: Path within the bucket
            key: Object key
            version_id: Object version ID
            
        Returns:
            SourceConfig object
        """
        location = f"{bucket}/{path}"
        if key:
            location = f"{bucket}/{key}"
        
        return SourceConfig(
            source_type=SourceType.S3,
            location=location,
            report_build_status=False
        )
    
    def configure_no_source(self) -> SourceConfig:
        """
        Configure no source (for builds without source).
        
        Returns:
            SourceConfig object
        """
        return SourceConfig(source_type=SourceType.NO_SOURCE)
    
    # =========================================================================
    # Environment Management
    # =========================================================================
    
    def create_environment_config(
        self,
        image: str = "aws/codebuild/standard:5.0",
        environment_type: Union[EnvironmentType, str] = EnvironmentType.LINUX_CONTAINER,
        compute_type: Union[ComputeType, str] = ComputeType.BUILD_GENERAL_SMALL,
        privileged_mode: bool = False,
        environment_variables: Optional[Dict[str, str]] = None,
        image_pull_credentials_type: str = "CODEBUILD"
    ) -> EnvironmentConfig:
        """
        Create an environment configuration.
        
        Args:
            image: Build image
            environment_type: Environment type
            compute_type: Compute type
            privileged_mode: Enable privileged mode
            environment_variables: Environment variables
            image_pull_credentials_type: Image pull credentials type
            
        Returns:
            EnvironmentConfig object
        """
        return EnvironmentConfig(
            environment_type=environment_type,
            image=image,
            compute_type=compute_type,
            privileged_mode=privileged_mode,
            environment_variables=environment_variables or {},
            image_pull_credentials_type=image_pull_credentials_type
        )
    
    def get_available_images(self) -> Dict[str, List[str]]:
        """
        Get available CodeBuild images.
        
        Returns:
            Dictionary of environment types to available images
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.list_curated_environment_images()
            images = {}
            
            for env_type in ['LINUX_CONTAINER', 'LINUX_GPU_CONTAINER', 'ARM_CONTAINER', 
                            'WINDOWS_CONTAINER', 'WINDOWS_SERVER_2019_CONTAINER']:
                env_images = response.get(env_type, [])
                images[env_type] = [img['name'] for img in env_images]
            
            return images
        except ClientError as e:
            logger.error(f"Failed to get available images: {e}")
            raise
    
    # =========================================================================
    # Artifacts Management
    # =========================================================================
    
    def create_artifacts_config(
        self,
        artifacts_type: Union[ArtifactType, str] = ArtifactType.NO_ARTIFACTS,
        bucket: Optional[str] = None,
        name: Optional[str] = None,
        namespace_type: str = "BUILD_ID",
        packaging: str = "NONE",
        path: Optional[str] = None,
        encryption_disabled: bool = False
    ) -> ArtifactsConfig:
        """
        Create an artifacts configuration.
        
        Args:
            artifacts_type: Type of artifacts
            bucket: S3 bucket for artifacts
            name: Artifact name template
            namespace_type: Namespace type (BUILD_ID or NONE)
            packaging: Packaging type (NONE or ZIP)
            path: Path within the bucket
            encryption_disabled: Disable encryption
            
        Returns:
            ArtifactsConfig object
        """
        location = f"{bucket}/{path}" if bucket and path else None
        
        return ArtifactsConfig(
            artifacts_type=artifacts_type,
            location=location,
            name=name,
            namespace_type=namespace_type,
            packaging=packaging,
            path=path,
            encryption_disabled=encryption_disabled
        )
    
    def get_artifact_urls(
        self,
        project_name: str,
        build_id: str
    ) -> List[Dict[str, str]]:
        """
        Get artifact URLs for a completed build.
        
        Args:
            project_name: Project name
            build_id: Build ID
            
        Returns:
            List of artifact information dictionaries
        """
        build_info = self.get_build(build_id)
        
        if not build_info.artifacts:
            return []
        
        artifact_urls = []
        location = build_info.artifacts.get('location', '')
        
        if location:
            if location.startswith('s3://'):
                parts = location[5:].split('/', 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ''
                
                try:
                    response = self.s3.list_objects_v2(
                        Bucket=bucket,
                        Prefix=prefix
                    )
                    
                    for obj in response.get('Contents', []):
                        artifact_urls.append({
                            'key': obj['Key'],
                            'url': f"s3://{bucket}/{obj['Key']}",
                            'size': str(obj['Size'])
                        })
                except ClientError:
                    pass
        
        return artifact_urls
    
    # =========================================================================
    # Caching Management
    # =========================================================================
    
    def create_cache_config(
        self,
        cache_type: Union[CacheType, str] = CacheType.NO_CACHE,
        bucket: Optional[str] = None,
        modes: Optional[List[Union[LocalCacheMode, str]]] = None
    ) -> CacheConfig:
        """
        Create a cache configuration.
        
        Args:
            cache_type: Type of cache
            bucket: S3 bucket for cache (required for S3 cache)
            modes: Local cache modes
            
        Returns:
            CacheConfig object
        """
        location = None
        if cache_type == CacheType.S3 and bucket:
            location = bucket
        
        return CacheConfig(
            cache_type=cache_type,
            location=location,
            modes=modes or []
        )
    
    def invalidate_cache(self, project_name: str) -> bool:
        """
        Invalidate the cache for a project.
        
        Args:
            project_name: Project name
            
        Returns:
            True if successful
        """
        try:
            self.codebuild.invalidate_project_cache(projectName=project_name)
            return True
        except ClientError as e:
            logger.error(f"Failed to invalidate cache for {project_name}: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Logs Integration
    # =========================================================================
    
    def configure_logs(
        self,
        cloud_watch_logs_enabled: bool = True,
        group_name: Optional[str] = None,
        stream_name: Optional[str] = None,
        s3_logs_enabled: bool = False,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None
    ) -> LogsConfig:
        """
        Configure CloudWatch logs.
        
        Args:
            cloud_watch_logs_enabled: Enable CloudWatch logs
            group_name: CloudWatch log group name
            stream_name: CloudWatch log stream name
            s3_logs_enabled: Enable S3 logs
            s3_bucket: S3 bucket for logs
            s3_prefix: S3 prefix for logs
            
        Returns:
            LogsConfig object
        """
        return LogsConfig(
            cloud_watch_logs_enabled=cloud_watch_logs_enabled,
            cloud_watch_logs_group_name=group_name,
            cloud_watch_logs_status="ENABLED" if cloud_watch_logs_enabled else "DISABLED",
            s3_logs_enabled=s3_logs_enabled,
            s3_logs_location=f"{s3_bucket}/{s3_prefix}" if s3_bucket else None,
            s3_logs_status="ENABLED" if s3_logs_enabled else "DISABLED"
        )
    
    def get_build_logs(
        self,
        project_name: str,
        build_id: str,
        start_time: Optional[int] = None,
        limit: int = 100
    ) -> List[str]:
        """
        Get build logs.
        
        Args:
            project_name: Project name
            build_id: Build ID
            start_time: Start time in milliseconds
            limit: Maximum number of log events
            
        Returns:
            List of log lines
        """
        try:
            kwargs = {
                'projectName': project_name,
                'id': build_id
            }
            
            if start_time:
                kwargs['startTime'] = start_time
            
            response = self.codebuild.get_build_logs(**kwargs)
            return response.get('logs', [])
        except ClientError as e:
            logger.error(f"Failed to get build logs: {e}")
            raise
    
    def get_cloudwatch_logs_link(
        self,
        project_name: str,
        build_id: str
    ) -> Optional[str]:
        """
        Get CloudWatch Logs link for a build.
        
        Args:
            project_name: Project name
            build_id: Build ID
            
        Returns:
            CloudWatch Logs URL or None
        """
        build_info = self.get_build(build_id)
        
        if not build_info.logs:
            return None
        
        group_name = build_info.logs.get('groupName')
        stream_name = build_info.logs.get('streamName')
        
        if not group_name or not stream_name:
            return None
        
        region = self.region_name
        return (
            f"https://console.aws.amazon.com/cloudwatch/home?"
            f"region={region}#logs:/log-groups/{group_name}/log-events/{stream_name}"
        )
    
    # =========================================================================
    # Webhook Management
    # =========================================================================
    
    def create_webhook(
        self,
        project_name: str,
        filter_groups: List[List[Dict[str, Any]]],
        webhook_secret: Optional[str] = None,
        retry_limit: int = 2
    ) -> Dict[str, Any]:
        """
        Create a webhook for a project.
        
        Args:
            project_name: Project name
            filter_groups: Filter groups for webhook triggers
            webhook_secret: Secret for webhook validation
            retry_limit: Retry limit for failed builds
            
        Returns:
            Webhook information
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        webhook_params = {
            'projectName': project_name,
            'filterGroups': filter_groups,
            'retryLimit': retry_limit
        }
        
        if webhook_secret:
            webhook_params['secretToken'] = webhook_secret
        
        try:
            response = self.codebuild.create_webhook(**webhook_params)
            return response['webhook']
        except ClientError as e:
            logger.error(f"Failed to create webhook for {project_name}: {e}")
            raise
    
    def delete_webhook(self, project_name: str) -> bool:
        """
        Delete webhook for a project.
        
        Args:
            project_name: Project name
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            self.codebuild.delete_webhook(projectName=project_name)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete webhook for {project_name}: {e}")
            raise
    
    def get_webhook(self, project_name: str) -> Optional[Dict[str, Any]]:
        """
        Get webhook information for a project.
        
        Args:
            project_name: Project name
            
        Returns:
            Webhook information or None
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.list_webhooks(projectName=project_name)
            webhooks = response.get('webhooks', [])
            return webhooks[0] if webhooks else None
        except ClientError as e:
            logger.error(f"Failed to get webhook for {project_name}: {e}")
            raise
    
    def update_webhook(
        self,
        project_name: str,
        filter_groups: Optional[List[List[Dict[str, Any]]]] = None,
        retry_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Update webhook for a project.
        
        Args:
            project_name: Project name
            filter_groups: New filter groups
            retry_limit: New retry limit
            
        Returns:
            Updated webhook information
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        webhook_params = {'projectName': project_name}
        
        if filter_groups is not None:
            webhook_params['filterGroups'] = filter_groups
        
        if retry_limit is not None:
            webhook_params['retryLimit'] = retry_limit
        
        try:
            response = self.codebuild.update_webhook(**webhook_params)
            return response['webhook']
        except ClientError as e:
            logger.error(f"Failed to update webhook for {project_name}: {e}")
            raise
    
    def create_pull_request_filter(
        self,
        project_name: str,
        filter_pattern: str = "PR_OPEN, PR_MERGED, PR_CLOSED"
    ) -> Dict[str, Any]:
        """
        Create a filter for pull request events.
        
        Args:
            project_name: Project name
            filter_pattern: Pull request event pattern
            
        Returns:
            Filter group configuration
        """
        filter_group = [
            [
                {
                    'type': 'EVENT',
                    'pattern': filter_pattern
                }
            ]
        ]
        
        return {
            'filterGroups': filter_group,
            'projectName': project_name
        }
    
    def create_push_filter(
        self,
        branch: str,
        file_paths: Optional[List[str]] = None
    ) -> List[List[Dict[str, Any]]]:
        """
        Create a filter for push events.
        
        Args:
            branch: Branch pattern to match
            file_paths: File paths that trigger builds
            
        Returns:
            Filter group configuration
        """
        filter_group = [
            [
                {
                    'type': 'HEAD_REF',
                    'pattern': f'refs/heads/{branch}'
                }
            ]
        ]
        
        if file_paths:
            for path in file_paths:
                filter_group[0].append({
                    'type': 'FILE_PATH',
                    'pattern': path
                })
        
        return filter_group
    
    # =========================================================================
    # Test Reports Management
    # =========================================================================
    
    def create_report_group(
        self,
        config: ReportGroupConfig
    ) -> ReportGroupInfo:
        """
        Create a test report group.
        
        Args:
            config: Report group configuration
            
        Returns:
            ReportGroupInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        report_config = {
            'name': config.name,
            'type': config.report_type.value if isinstance(config.report_type, ReportType) else config.report_type,
            'exportConfig': config.export_config,
            'tags': [{'key': k, 'value': v} for k, v in config.tags.items()],
            'deleteReports': config.delete_reports
        }
        
        try:
            response = self.codebuild.create_report_group(**report_config)
            report_group = response['reportGroup']
            
            return ReportGroupInfo(
                arn=report_group['arn'],
                name=report_group['name'],
                type=report_group['type'],
                export_config=report_group['exportConfig'],
                created=report_group['created'],
                last_modified=report_group['lastModified'],
                tags={t['key']: t['value'] for t in report_group.get('tags', [])}
            )
        except ClientError as e:
            logger.error(f"Failed to create report group {config.name}: {e}")
            raise
    
    def delete_report_group(self, arn: str) -> bool:
        """
        Delete a report group.
        
        Args:
            arn: Report group ARN
            
        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            self.codebuild.delete_report_group(arn=arn)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete report group {arn}: {e}")
            raise
    
    def list_report_groups(self) -> List[str]:
        """
        List all report groups.
        
        Returns:
            List of report group ARNs
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.list_report_groups()
            return response['reportGroups']
        except ClientError as e:
            logger.error(f"Failed to list report groups: {e}")
            raise
    
    def get_report_group(self, name: str) -> ReportGroupInfo:
        """
        Get report group information.
        
        Args:
            name: Report group name
            
        Returns:
            ReportGroupInfo object
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.list_report_groups(name=name)
            arns = response.get('reportGroups', [])
            
            if not arns:
                raise ValueError(f"Report group {name} not found")
            
            arn = arns[0]
            response = self.codebuild.batch_get_report_groups(arns=[arn])
            
            if not response['reportGroups']:
                raise ValueError(f"Report group {name} not found")
            
            rg = response['reportGroups'][0]
            return ReportGroupInfo(
                arn=rg['arn'],
                name=rg['name'],
                type=rg['type'],
                export_config=rg['exportConfig'],
                created=rg['created'],
                last_modified=rg['lastModified'],
                tags={t['key']: t['value'] for t in rg.get('tags', [])}
            )
        except ClientError as e:
            logger.error(f"Failed to get report group {name}: {e}")
            raise
    
    def get_reports(
        self,
        report_group_arn: str,
        filter_status: Optional[ReportStatus] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get reports from a report group.
        
        Args:
            report_group_arn: Report group ARN
            filter_status: Filter by status
            max_results: Maximum number of results
            
        Returns:
            List of report information
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            kwargs = {
                'reportGroupArn': report_group_arn,
                'maxResults': min(max_results, 100)
            }
            
            if filter_status:
                kwargs['filter'] = {
                    'status': filter_status.value
                }
            
            reports = []
            paginator = self.codebuild.get_paginator('list_reports_for_report_group')
            
            for page in paginator.paginate(**kwargs):
                report_arns = page.get('reports', [])
                if report_arns:
                    batch = self.codebuild.batch_get_reports(arns=report_arns)
                    reports.extend(batch.get('reports', []))
            
            return reports
        except ClientError as e:
            logger.error(f"Failed to get reports for {report_group_arn}: {e}")
            raise
    
    def get_report(self, arn: str) -> Dict[str, Any]:
        """
        Get report details.
        
        Args:
            arn: Report ARN
            
        Returns:
            Report information
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        try:
            response = self.codebuild.batch_get_reports(arns=[arn])
            if not response['reports']:
                raise ValueError(f"Report {arn} not found")
            return response['reports'][0]
        except ClientError as e:
            logger.error(f"Failed to get report {arn}: {e}")
            raise
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _build_source_dict(self, source: SourceConfig) -> Dict[str, Any]:
        """Build source dictionary from SourceConfig."""
        source_dict = {
            'type': source.source_type.value if isinstance(source.source_type, SourceType) else source.source_type
        }
        
        if source.location:
            source_dict['location'] = source.location
        
        if source.git_clone_depth:
            source_dict['gitCloneDepth'] = source.git_clone_depth
        
        if source.git_submodules_config:
            source_dict['gitSubmodulesConfig'] = source.git_submodules_config
        
        if source.build_status_config:
            source_dict['buildStatusConfig'] = source.build_status_config
        
        if source.auth:
            source_dict['auth'] = source.auth
        
        source_dict['reportBuildStatus'] = source.report_build_status
        
        return source_dict
    
    def _build_environment_dict(self, env: EnvironmentConfig) -> Dict[str, Any]:
        """Build environment dictionary from EnvironmentConfig."""
        return {
            'type': env.environment_type.value if isinstance(env.environment_type, EnvironmentType) else env.environment_type,
            'image': env.image,
            'computeType': env.compute_type.value if isinstance(env.compute_type, ComputeType) else env.compute_type,
            'privilegedMode': env.privileged_mode,
            'environmentVariables': [
                {'name': k, 'value': v} for k, v in env.environment_variables.items()
            ] if env.environment_variables else [],
            'registryCredential': env.registry_credential,
            'imagePullCredentialsType': env.image_pull_credentials_type
        }
    
    def _build_artifacts_dict(self, artifacts: ArtifactsConfig) -> Dict[str, Any]:
        """Build artifacts dictionary from ArtifactsConfig."""
        artifacts_dict = {
            'type': artifacts.artifacts_type.value if isinstance(artifacts.artifacts_type, ArtifactType) else artifacts.artifacts_type
        }
        
        if artifacts.location:
            artifacts_dict['location'] = artifacts.location
        
        if artifacts.name:
            artifacts_dict['name'] = artifacts.name
        
        artifacts_dict['namespaceType'] = artifacts.namespace_type
        artifacts_dict['packaging'] = artifacts.packaging
        
        if artifacts.path:
            artifacts_dict['path'] = artifacts.path
        
        artifacts_dict['encryptionDisabled'] = artifacts.encryption_disabled
        
        if artifacts.artifact_identifier:
            artifacts_dict['artifactIdentifier'] = artifacts.artifact_identifier
        
        return artifacts_dict
    
    def _build_cache_dict(self, cache: CacheConfig) -> Dict[str, Any]:
        """Build cache dictionary from CacheConfig."""
        cache_dict = {
            'type': cache.cache_type.value if isinstance(cache.cache_type, CacheType) else cache.cache_type
        }
        
        if cache.location:
            cache_dict['location'] = cache.location
        
        if cache.modes:
            cache_dict['modes'] = [
                m.value if isinstance(m, LocalCacheMode) else m
                for m in cache.modes
            ]
        
        return cache_dict
    
    def _build_logs_dict(self, logs: LogsConfig) -> Dict[str, Any]:
        """Build logs dictionary from LogsConfig."""
        logs_dict = {
            'cloudWatchLogsEnabled': logs.cloud_watch_logs_enabled,
            'cloudWatchLogsStatus': logs.cloud_watch_logs_status
        }
        
        if logs.cloud_watch_logs_group_name:
            logs_dict['cloudWatchLogsGroupName'] = logs.cloud_watch_logs_group_name
        
        logs_dict['s3LogsEnabled'] = logs.s3_logs_enabled
        logs_dict['s3LogsStatus'] = logs.s3_logs_status
        
        if logs.s3_logs_location:
            logs_dict['s3LogsLocation'] = logs.s3_logs_location
        
        logs_dict['encryptionDisabled'] = logs.encryption_disabled
        
        return logs_dict
    
    def _parse_project_info(self, project: Dict[str, Any]) -> ProjectInfo:
        """Parse project information from API response."""
        return ProjectInfo(
            name=project['name'],
            arn=project['arn'],
            description=project.get('description', ''),
            source=project.get('source', {}),
            environment=project.get('environment', {}),
            artifacts=project.get('artifacts', {}),
            cache=project.get('cache', {}),
            logs_config=project.get('logsConfig', {}),
            vpc_config=project.get('vpcConfig'),
            timeout_in_minutes=project.get('timeoutInMinutes', 60),
            queued_timeout_in_minutes=project.get('queuedTimeoutInMinutes', 480),
            created=project.get('created', ''),
            last_modified=project.get('lastModified', ''),
            tags={t['key']: t['value'] for t in project.get('tags', [])},
            project_visibility=project.get('projectVisibility'),
            badge=project.get('badge')
        )
    
    def _parse_build_info(self, build: Dict[str, Any]) -> BuildInfo:
        """Parse build information from API response."""
        return BuildInfo(
            id=build['id'],
            arn=build['arn'],
            project_name=build['projectName'],
            build_number=build.get('buildNumber', 0),
            build_status=build.get('buildStatus'),
            source_version=build.get('sourceVersion'),
            start_time=build.get('startTime'),
            end_time=build.get('endTime'),
            current_phase=build.get('currentPhase'),
            source=build.get('source', {}),
            environment=build.get('environment', {}),
            artifacts=build.get('artifacts'),
            logs=build.get('logs'),
            cache=build.get('cache'),
            phases=build.get('phases', [])
        )
    
    def create_service_role(self, project_name: str) -> str:
        """
        Create a service role for a CodeBuild project.
        
        Args:
            project_name: Project name
            
        Returns:
            Role ARN
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for CodeBuild operations")
        
        role_name = f"codebuild-{project_name}-role"
        policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'codebuild.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }
        
        try:
            response = self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(policy_document),
                Description=f'Service role for CodeBuild project {project_name}'
            )
            role_arn = response['Role']['Arn']
            
            policy_arn = f'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess' if ':' in self.region_name and not self.region_name.startswith('cn-') else f'arn:aws:iam::aws:policy/AmazonS3FullAccess'
            
            self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            
            return role_arn
        except ClientError as e:
            logger.error(f"Failed to create service role: {e}")
            raise
    
    def cleanup(self):
        """Clean up resources and clear caches."""
        with self._lock:
            self._projects_cache.clear()
            self._builds_cache.clear()
            self._clients.clear()
