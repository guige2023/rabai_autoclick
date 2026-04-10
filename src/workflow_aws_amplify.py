"""
AWS Amplify Serverless Integration Module for Workflow System

Implements an AmplifyIntegration class with:
1. App management: Create/manage Amplify apps
2. Branches: Manage branches
3. Deployments: Create/manage deployments
4. Domain association: Manage custom domains
5. Webhooks: Manage webhooks
6. Backend environments: Manage backend environments
7. Artifacts: Manage build artifacts
8. Deploy preview: Configure deploy previews
9. Artifacts: Build and deploy artifacts
10. CloudWatch integration: Build and deployment metrics

Commit: 'feat(aws-amplify): add AWS Amplify with app management, branches, deployments, domains, webhooks, backend environments, artifacts, deploy preview, CloudWatch'
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


class BranchFramework(Enum):
    """Web framework for branch."""
    WEB_DYNAMIC = "WEB_DYNAMIC"
    WEB_COMPUTE = "WEB_COMPUTE"
    NEXT_JS = "NEXT_JS"
    NEXT_JS_SSR = "NEXT_JS_SSR"
    Nuxt_JS = "Nuxt.js"
    Vue = "Vue"
    React = "React"
    Angular = "Angular"
    Ionic = "Ionic"
    DEVIC_FARM = "DEVICE_FARM"
    Web = "Web"
    Express = "Express"
    Flutter = "Flutter"


class BuildStatus(Enum):
    """Build status values."""
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    FAULT = "FAULT"
    TIMED_OUT = "TIMED_OUT"
    IN_PROGRESS = "IN_PROGRESS"
    STOPPED = "STOPPED"


class DeploymentStatus(Enum):
    """Deployment status."""
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"


class DomainStatus(Enum):
    """Domain association status."""
    PENDING_VERIFICATION = "PENDING_VERIFICATION"
    PENDING_DEPLOYMENT = "PENDING_DEPLOYMENT"
    IN_USE = "IN_USE"
    AVAILABLE = "AVAILABLE"
    DELETE_IN_PROGRESS = "DELETE_IN_PROGRESS"
    FAILED = "FAILED"


class WebhookStatus(Enum):
    """Webhook status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class BackendEnvironmentStatus(Enum):
    """Backend environment status."""
    AVAILABLE = "AVAILABLE"
    CREATING = "CREATING"
    DELETING = "DELETING"
    FAILED = "FAILED"


class DeployPreviewStatus(Enum):
    """Deploy preview status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PROVISIONING = "PROVISIONING"


@dataclass
class AmplifyConfig:
    """Configuration for Amplify connection."""
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
class AppConfig:
    """Configuration for Amplify app."""
    name: str
    description: Optional[str] = None
    repository: Optional[str] = None
    platform: str = "WEB"
    build_spec: Optional[str] = None
    environment_variables: Optional[Dict[str, str]] = None
    enable_branch_auto_build: bool = True
    enable_branch_auto_deletion: bool = False
    enable_basic_auth: bool = False
    basic_auth_credentials: Optional[str] = None
    custom_rule: Optional[List[Dict[str, str]]] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class BranchConfig:
    """Configuration for Amplify branch."""
    branch_name: str
    description: Optional[str] = None
    stage: Optional[str] = None
    framework: Optional[str] = None
    enable_notification: bool = False
    enable_auto_build: bool = True
    environment_variables: Optional[Dict[str, str]] = None
    basic_auth_credentials: Optional[str] = None
    enable_basic_auth: bool = False
    pull_request_environment_name: Optional[str] = None
    build_spec: Optional[str] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class DomainConfig:
    """Configuration for custom domain."""
    domain_name: str
    sub_domain: Optional[str] = None
    enable_auto_sub_domain: bool = True
    branch_name: Optional[str] = None


@dataclass
class WebhookConfig:
    """Configuration for webhook."""
    webhook_name: str
    branch_name: str
    description: Optional[str] = None


@dataclass
class BackendEnvironmentConfig:
    """Configuration for backend environment."""
    environment_name: str
    stack_name: Optional[str] = None
    deployment_mechanism: Optional[str] = None


@dataclass
class DeployPreviewConfig:
    """Configuration for deploy preview."""
    branch_name: str
    enable_pull_request_preview: bool = True
    pull_request_environment_name: Optional[str] = None


@dataclass
class ArtifactInfo:
    """Build artifact information."""
    artifact_id: str
    artifact_name: str
    artifact_arn: str
    artifact_type: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BuildInfo:
    """Build information."""
    build_id: str
    build_number: int
    build_status: BuildStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[int] = None
    log_url: Optional[str] = None
    artifacts: Optional[List[ArtifactInfo]] = None
    source_version: Optional[str] = None
    commit_id: Optional[str] = None
    commit_message: Optional[str] = None


@dataclass
class DeploymentInfo:
    """Deployment information."""
    deployment_id: str
    branch_name: str
    deployment_status: DeploymentStatus
    created_at: datetime
    updated_at: datetime
    target_url: Optional[str] = None
    build_artifacts: Optional[List[ArtifactInfo]] = None


@dataclass
class DomainInfo:
    """Domain information."""
    domain_arn: str
    domain_name: str
    domain_status: DomainStatus
    branch_name: Optional[str] = None
    certificate_verification_dns_record: Optional[str] = None
    enable_auto_sub_domain: bool = True


@dataclass
class WebhookInfo:
    """Webhook information."""
    webhook_id: str
    webhook_name: str
    webhook_url: str
    branch_name: str
    description: Optional[str] = None
    create_time: datetime
    update_time: datetime
    webhook_status: WebhookStatus = WebhookStatus.ACTIVE


@dataclass
class BackendEnvironmentInfo:
    """Backend environment information."""
    environment_id: str
    environment_name: str
    stack_name: Optional[str] = None
    deployment_mechanism: Optional[str] = None
    status: BackendEnvironmentStatus = BackendEnvironmentStatus.AVAILABLE
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class DeployPreviewInfo:
    """Deploy preview information."""
    preview_id: str
    branch_name: str
    preview_url: Optional[str] = None
    status: DeployPreviewStatus = DeployPreviewStatus.ACTIVE
    pull_request_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class MetricsData:
    """CloudWatch metrics data."""
    metric_name: str
    value: float
    unit: str
    timestamp: datetime
    dimensions: Optional[Dict[str, str]] = None


class AmplifyIntegration:
    """
    AWS Amplify Integration for workflow automation.
    
    Provides comprehensive management for:
    - App management: Create/manage Amplify apps
    - Branches: Manage branches
    - Deployments: Create/manage deployments
    - Domain association: Manage custom domains
    - Webhooks: Manage webhooks
    - Backend environments: Manage backend environments
    - Artifacts: Manage build artifacts
    - Deploy preview: Configure deploy previews
    - CloudWatch integration: Build and deployment metrics
    """
    
    def __init__(self, config: Optional[AmplifyConfig] = None):
        """Initialize Amplify integration.
        
        Args:
            config: Amplify configuration. If None, uses default config.
        """
        self.config = config or AmplifyConfig()
        self._client = None
        self._cloudwatch_client = None
        self._resourcegroups_client = None
        self._lock = threading.RLock()
        self._apps_cache: Dict[str, Dict[str, Any]] = {}
        self._branches_cache: Dict[str, Dict[str, Any]] = {}
        self._deployments_cache: Dict[str, Dict[str, Any]] = {}
        self._domains_cache: Dict[str, Dict[str, Any]] = {}
        self._webhooks_cache: Dict[str, Dict[str, Any]] = {}
        self._backend_envs_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.RLock()
        
    @property
    def client(self):
        """Get or create Amplify client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    if not BOTO3_AVAILABLE:
                        raise ImportError("boto3 is required for AmplifyIntegration")
                    
                    client_kwargs = {
                        'region_name': self.config.region_name,
                        'config': {
                            'connect_timeout': self.config.timeout,
                            'read_timeout': self.config.timeout,
                        }
                    }
                    
                    if self.config.endpoint_url:
                        client_kwargs['endpoint_url'] = self.config.endpoint_url
                    
                    if self.config.profile_name:
                        client_kwargs['profile_name'] = self.config.profile_name
                    else:
                        if self.config.aws_access_key_id:
                            client_kwargs['aws_access_key_id'] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            client_kwargs['aws_secret_access_key'] = self.config.aws_secret_access_key
                        if self.config.aws_session_token:
                            client_kwargs['aws_session_token'] = self.config.aws_session_token
                    
                    self._client = boto3.client('amplify', **client_kwargs)
        return self._client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            with self._lock:
                if self._cloudwatch_client is None:
                    if not BOTO3_AVAILABLE:
                        raise ImportError("boto3 is required for CloudWatch integration")
                    
                    client_kwargs = {
                        'region_name': self.config.region_name,
                    }
                    
                    if self.config.profile_name:
                        client_kwargs['profile_name'] = self.config.profile_name
                    else:
                        if self.config.aws_access_key_id:
                            client_kwargs['aws_access_key_id'] = self.config.aws_access_key_id
                        if self.config.aws_secret_access_key:
                            client_kwargs['aws_secret_access_key'] = self.config.aws_secret_access_key
                    
                    self._cloudwatch_client = boto3.client('cloudwatch', **client_kwargs)
        return self._cloudwatch_client
    
    # ==================== App Management ====================
    
    def create_app(self, app_config: AppConfig) -> Dict[str, Any]:
        """Create a new Amplify app.
        
        Args:
            app_config: App configuration.
            
        Returns:
            Created app information.
        """
        try:
            kwargs = {
                'name': app_config.name,
                'platform': app_config.platform,
            }
            
            if app_config.description:
                kwargs['description'] = app_config.description
            if app_config.repository:
                kwargs['repository'] = app_config.repository
            if app_config.build_spec:
                kwargs['buildSpec'] = app_config.build_spec
            if app_config.environment_variables:
                kwargs['environmentVariables'] = app_config.environment_variables
            if app_config.tags:
                kwargs['tags'] = app_config.tags
            
            kwargs['enableBranchAutoBuild'] = app_config.enable_branch_auto_build
            kwargs['enableBranchAutoDeletion'] = app_config.enable_branch_auto_deletion
            
            if app_config.enable_basic_auth:
                kwargs['enableBasicAuth'] = True
                if app_config.basic_auth_credentials:
                    kwargs['basicAuthCredentials'] = base64.b64encode(
                        app_config.basic_auth_credentials.encode()
                    ).decode()
            
            if app_config.custom_rule:
                kwargs['customRules'] = app_config.custom_rule
            
            response = self.client.create_app(**kwargs)
            app = response['app']
            
            with self._cache_lock:
                self._apps_cache[app['appId']] = app
            
            logger.info(f"Created Amplify app: {app['appId']} - {app['name']}")
            return app
            
        except ClientError as e:
            logger.error(f"Failed to create Amplify app: {e}")
            raise
    
    def get_app(self, app_id: str) -> Dict[str, Any]:
        """Get Amplify app by ID.
        
        Args:
            app_id: App ID.
            
        Returns:
            App information.
        """
        try:
            with self._cache_lock:
                if app_id in self._apps_cache:
                    return self._apps_cache[app_id]
            
            response = self.client.get_app(appId=app_id)
            app = response['app']
            
            with self._cache_lock:
                self._apps_cache[app_id] = app
            
            return app
            
        except ClientError as e:
            logger.error(f"Failed to get Amplify app {app_id}: {e}")
            raise
    
    def list_apps(self, max_results: int = 100) -> List[Dict[str, Any]]:
        """List all Amplify apps.
        
        Args:
            max_results: Maximum number of results.
            
        Returns:
            List of apps.
        """
        try:
            apps = []
            paginator = self.client.get_paginator('list_apps')
            
            for page in paginator.paginate(MaxResults=max_results):
                apps.extend(page['apps'])
            
            with self._cache_lock:
                for app in apps:
                    self._apps_cache[app['appId']] = app
            
            return apps
            
        except ClientError as e:
            logger.error(f"Failed to list Amplify apps: {e}")
            raise
    
    def update_app(self, app_id: str, app_config: AppConfig) -> Dict[str, Any]:
        """Update an Amplify app.
        
        Args:
            app_id: App ID.
            app_config: Updated app configuration.
            
        Returns:
            Updated app information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'name': app_config.name,
                'platform': app_config.platform,
            }
            
            if app_config.description:
                kwargs['description'] = app_config.description
            if app_config.build_spec:
                kwargs['buildSpec'] = app_config.build_spec
            if app_config.environment_variables:
                kwargs['environmentVariables'] = app_config.environment_variables
            if app_config.tags:
                kwargs['tags'] = app_config.tags
            
            kwargs['enableBranchAutoBuild'] = app_config.enable_branch_auto_build
            kwargs['enableBranchAutoDeletion'] = app_config.enable_branch_auto_deletion
            
            if app_config.enable_basic_auth:
                kwargs['enableBasicAuth'] = True
                if app_config.basic_auth_credentials:
                    kwargs['basicAuthCredentials'] = base64.b64encode(
                        app_config.basic_auth_credentials.encode()
                    ).decode()
            
            if app_config.custom_rule:
                kwargs['customRules'] = app_config.custom_rule
            
            response = self.client.update_app(**kwargs)
            app = response['app']
            
            with self._cache_lock:
                self._apps_cache[app_id] = app
            
            logger.info(f"Updated Amplify app: {app_id}")
            return app
            
        except ClientError as e:
            logger.error(f"Failed to update Amplify app {app_id}: {e}")
            raise
    
    def delete_app(self, app_id: str) -> bool:
        """Delete an Amplify app.
        
        Args:
            app_id: App ID.
            
        Returns:
            True if deleted successfully.
        """
        try:
            self.client.delete_app(appId=app_id)
            
            with self._cache_lock:
                if app_id in self._apps_cache:
                    del self._apps_cache[app_id]
            
            logger.info(f"Deleted Amplify app: {app_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete Amplify app {app_id}: {e}")
            raise
    
    # ==================== Branch Management ====================
    
    def create_branch(self, app_id: str, branch_config: BranchConfig) -> Dict[str, Any]:
        """Create a new branch.
        
        Args:
            app_id: App ID.
            branch_config: Branch configuration.
            
        Returns:
            Created branch information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'branchName': branch_config.branch_name,
            }
            
            if branch_config.description:
                kwargs['description'] = branch_config.description
            if branch_config.stage:
                kwargs['stage'] = branch_config.stage
            if branch_config.framework:
                kwargs['framework'] = branch_config.framework
            if branch_config.enable_notification:
                kwargs['enableNotification'] = True
            if branch_config.enable_auto_build:
                kwargs['enableAutoBuild'] = True
            else:
                kwargs['enableAutoBuild'] = False
            if branch_config.environment_variables:
                kwargs['environmentVariables'] = branch_config.environment_variables
            if branch_config.enable_basic_auth:
                kwargs['enableBasicAuth'] = True
                if branch_config.basic_auth_credentials:
                    kwargs['basicAuthCredentials'] = base64.b64encode(
                        branch_config.basic_auth_credentials.encode()
                    ).decode()
            if branch_config.pull_request_environment_name:
                kwargs['pullRequestEnvironmentName'] = branch_config.pull_request_environment_name
            if branch_config.build_spec:
                kwargs['buildSpec'] = branch_config.build_spec
            if branch_config.tags:
                kwargs['tags'] = branch_config.tags
            
            response = self.client.create_branch(**kwargs)
            branch = response['branch']
            
            cache_key = f"{app_id}:{branch['branchName']}"
            with self._cache_lock:
                self._branches_cache[cache_key] = branch
            
            logger.info(f"Created branch: {app_id}/{branch['branchName']}")
            return branch
            
        except ClientError as e:
            logger.error(f"Failed to create branch: {e}")
            raise
    
    def get_branch(self, app_id: str, branch_name: str) -> Dict[str, Any]:
        """Get branch information.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            Branch information.
        """
        try:
            cache_key = f"{app_id}:{branch_name}"
            with self._cache_lock:
                if cache_key in self._branches_cache:
                    return self._branches_cache[cache_key]
            
            response = self.client.get_branch(appId=app_id, branchName=branch_name)
            branch = response['branch']
            
            with self._cache_lock:
                self._branches_cache[cache_key] = branch
            
            return branch
            
        except ClientError as e:
            logger.error(f"Failed to get branch {branch_name}: {e}")
            raise
    
    def list_branches(self, app_id: str) -> List[Dict[str, Any]]:
        """List all branches for an app.
        
        Args:
            app_id: App ID.
            
        Returns:
            List of branches.
        """
        try:
            response = self.client.list_branches(appId=app_id)
            branches = response['branches']
            
            with self._cache_lock:
                for branch in branches:
                    cache_key = f"{app_id}:{branch['branchName']}"
                    self._branches_cache[cache_key] = branch
            
            return branches
            
        except ClientError as e:
            logger.error(f"Failed to list branches: {e}")
            raise
    
    def update_branch(self, app_id: str, branch_name: str, branch_config: BranchConfig) -> Dict[str, Any]:
        """Update a branch.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            branch_config: Updated branch configuration.
            
        Returns:
            Updated branch information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'branchName': branch_name,
            }
            
            if branch_config.description:
                kwargs['description'] = branch_config.description
            if branch_config.stage:
                kwargs['stage'] = branch_config.stage
            if branch_config.framework:
                kwargs['framework'] = branch_config.framework
            if branch_config.enable_notification:
                kwargs['enableNotification'] = True
            if branch_config.enable_auto_build is not None:
                kwargs['enableAutoBuild'] = branch_config.enable_auto_build
            if branch_config.environment_variables:
                kwargs['environmentVariables'] = branch_config.environment_variables
            if branch_config.enable_basic_auth:
                kwargs['enableBasicAuth'] = True
                if branch_config.basic_auth_credentials:
                    kwargs['basicAuthCredentials'] = base64.b64encode(
                        branch_config.basic_auth_credentials.encode()
                    ).decode()
            if branch_config.pull_request_environment_name:
                kwargs['pullRequestEnvironmentName'] = branch_config.pull_request_environment_name
            if branch_config.build_spec:
                kwargs['buildSpec'] = branch_config.build_spec
            
            response = self.client.update_branch(**kwargs)
            branch = response['branch']
            
            cache_key = f"{app_id}:{branch_name}"
            with self._cache_lock:
                self._branches_cache[cache_key] = branch
            
            logger.info(f"Updated branch: {app_id}/{branch_name}")
            return branch
            
        except ClientError as e:
            logger.error(f"Failed to update branch {branch_name}: {e}")
            raise
    
    def delete_branch(self, app_id: str, branch_name: str) -> bool:
        """Delete a branch.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            True if deleted successfully.
        """
        try:
            self.client.delete_branch(appId=app_id, branchName=branch_name)
            
            cache_key = f"{app_id}:{branch_name}"
            with self._cache_lock:
                if cache_key in self._branches_cache:
                    del self._branches_cache[cache_key]
            
            logger.info(f"Deleted branch: {app_id}/{branch_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete branch {branch_name}: {e}")
            raise
    
    # ==================== Deployment Management ====================
    
    def start_deployment(self, app_id: str, branch_name: str, 
                        source_version: Optional[str] = None) -> Dict[str, Any]:
        """Start a deployment.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            source_version: Source version (commit ID or artifact ID).
            
        Returns:
            Deployment information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'branchName': branch_name,
            }
            
            if source_version:
                kwargs['sourceVersion'] = source_version
            
            response = self.client.start_deployment(**kwargs)
            deployment = response['deployment']
            
            cache_key = f"{app_id}:{branch_name}:{deployment['deploymentId']}"
            with self._cache_lock:
                self._deployments_cache[cache_key] = deployment
            
            logger.info(f"Started deployment: {app_id}/{branch_name} - {deployment['deploymentId']}")
            return deployment
            
        except ClientError as e:
            logger.error(f"Failed to start deployment: {e}")
            raise
    
    def get_deployment(self, app_id: str, branch_name: str, 
                       deployment_id: str) -> Dict[str, Any]:
        """Get deployment information.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            deployment_id: Deployment ID.
            
        Returns:
            Deployment information.
        """
        try:
            cache_key = f"{app_id}:{branch_name}:{deployment_id}"
            with self._cache_lock:
                if cache_key in self._deployments_cache:
                    return self._deployments_cache[cache_key]
            
            response = self.client.get_deployment(
                appId=app_id,
                branchName=branch_name,
                deploymentId=deployment_id
            )
            deployment = response['deployment']
            
            with self._cache_lock:
                self._deployments_cache[cache_key] = deployment
            
            return deployment
            
        except ClientError as e:
            logger.error(f"Failed to get deployment {deployment_id}: {e}")
            raise
    
    def stop_deployment(self, app_id: str, branch_name: str, 
                         deployment_id: str) -> bool:
        """Stop a deployment.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            deployment_id: Deployment ID.
            
        Returns:
            True if stopped successfully.
        """
        try:
            self.client.stop_deployment(
                appId=app_id,
                branchName=branch_name,
                deploymentId=deployment_id
            )
            
            logger.info(f"Stopped deployment: {app_id}/{branch_name}/{deployment_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to stop deployment {deployment_id}: {e}")
            raise
    
    def list_deployments(self, app_id: str, branch_name: str) -> List[Dict[str, Any]]:
        """List deployments for a branch.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            List of deployments.
        """
        try:
            response = self.client.list_deployments(
                appId=app_id,
                branchName=branch_name
            )
            deployments = response['deployments']
            
            with self._cache_lock:
                for deployment in deployments:
                    cache_key = f"{app_id}:{branch_name}:{deployment['deploymentId']}"
                    self._deployments_cache[cache_key] = deployment
            
            return deployments
            
        except ClientError as e:
            logger.error(f"Failed to list deployments: {e}")
            raise
    
    # ==================== Domain Association ====================
    
    def create_domain_association(self, app_id: str, 
                                   domain_config: DomainConfig) -> Dict[str, Any]:
        """Create a domain association.
        
        Args:
            app_id: App ID.
            domain_config: Domain configuration.
            
        Returns:
            Domain association information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'domainName': domain_config.domain_name,
            }
            
            sub_domain_settings = []
            if domain_config.sub_domain:
                sub_domain_settings.append({
                    'prefix': domain_config.sub_domain,
                    'branchName': domain_config.branch_name or 'main'
                })
            else:
                sub_domain_settings.append({
                    'prefix': '',
                    'branchName': domain_config.branch_name or 'main'
                })
            
            kwargs['subDomainSettings'] = sub_domain_settings
            kwargs['enableAutoSubDomain'] = domain_config.enable_auto_sub_domain
            
            response = self.client.create_domain_association(**kwargs)
            domain = response['domainAssociation']
            
            with self._cache_lock:
                self._domains_cache[domain['domainName']] = domain
            
            logger.info(f"Created domain association: {domain_config.domain_name}")
            return domain
            
        except ClientError as e:
            logger.error(f"Failed to create domain association: {e}")
            raise
    
    def get_domain_association(self, app_id: str, 
                               domain_name: str) -> Dict[str, Any]:
        """Get domain association.
        
        Args:
            app_id: App ID.
            domain_name: Domain name.
            
        Returns:
            Domain association information.
        """
        try:
            with self._cache_lock:
                if domain_name in self._domains_cache:
                    return self._domains_cache[domain_name]
            
            response = self.client.get_domain_association(
                appId=app_id,
                domainName=domain_name
            )
            domain = response['domainAssociation']
            
            with self._cache_lock:
                self._domains_cache[domain_name] = domain
            
            return domain
            
        except ClientError as e:
            logger.error(f"Failed to get domain association {domain_name}: {e}")
            raise
    
    def list_domain_associations(self, app_id: str) -> List[Dict[str, Any]]:
        """List domain associations for an app.
        
        Args:
            app_id: App ID.
            
        Returns:
            List of domain associations.
        """
        try:
            response = self.client.list_domain_associations(appId=app_id)
            domains = response['domainAssociations']
            
            with self._cache_lock:
                for domain in domains:
                    self._domains_cache[domain['domainName']] = domain
            
            return domains
            
        except ClientError as e:
            logger.error(f"Failed to list domain associations: {e}")
            raise
    
    def update_domain_association(self, app_id: str, domain_name: str,
                                  domain_config: DomainConfig) -> Dict[str, Any]:
        """Update a domain association.
        
        Args:
            app_id: App ID.
            domain_name: Domain name.
            domain_config: Updated domain configuration.
            
        Returns:
            Updated domain association information.
        """
        try:
            sub_domain_settings = []
            if domain_config.sub_domain:
                sub_domain_settings.append({
                    'prefix': domain_config.sub_domain,
                    'branchName': domain_config.branch_name or 'main'
                })
            else:
                sub_domain_settings.append({
                    'prefix': '',
                    'branchName': domain_config.branch_name or 'main'
                })
            
            response = self.client.update_domain_association(
                appId=app_id,
                domainName=domain_name,
                subDomainSettings=sub_domain_settings,
                enableAutoSubDomain=domain_config.enable_auto_sub_domain
            )
            domain = response['domainAssociation']
            
            with self._cache_lock:
                self._domains_cache[domain_name] = domain
            
            logger.info(f"Updated domain association: {domain_name}")
            return domain
            
        except ClientError as e:
            logger.error(f"Failed to update domain association {domain_name}: {e}")
            raise
    
    def delete_domain_association(self, app_id: str, domain_name: str) -> bool:
        """Delete a domain association.
        
        Args:
            app_id: App ID.
            domain_name: Domain name.
            
        Returns:
            True if deleted successfully.
        """
        try:
            self.client.delete_domain_association(
                appId=app_id,
                domainName=domain_name
            )
            
            with self._cache_lock:
                if domain_name in self._domains_cache:
                    del self._domains_cache[domain_name]
            
            logger.info(f"Deleted domain association: {domain_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete domain association {domain_name}: {e}")
            raise
    
    # ==================== Webhook Management ====================
    
    def create_webhook(self, app_id: str, webhook_config: WebhookConfig) -> Dict[str, Any]:
        """Create a webhook.
        
        Args:
            app_id: App ID.
            webhook_config: Webhook configuration.
            
        Returns:
            Created webhook information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'branchName': webhook_config.branch_name,
                'webhookName': webhook_config.webhook_name,
            }
            
            if webhook_config.description:
                kwargs['description'] = webhook_config.description
            
            response = self.client.create_webhook(**kwargs)
            webhook = response['webhook']
            
            with self._cache_lock:
                self._webhooks_cache[webhook['webhookId']] = webhook
            
            logger.info(f"Created webhook: {webhook['webhookId']}")
            return webhook
            
        except ClientError as e:
            logger.error(f"Failed to create webhook: {e}")
            raise
    
    def get_webhook(self, app_id: str, webhook_id: str) -> Dict[str, Any]:
        """Get webhook information.
        
        Args:
            app_id: App ID.
            webhook_id: Webhook ID.
            
        Returns:
            Webhook information.
        """
        try:
            with self._cache_lock:
                if webhook_id in self._webhooks_cache:
                    return self._webhooks_cache[webhook_id]
            
            response = self.client.get_webhook(appId=app_id, webhookId=webhook_id)
            webhook = response['webhook']
            
            with self._cache_lock:
                self._webhooks_cache[webhook_id] = webhook
            
            return webhook
            
        except ClientError as e:
            logger.error(f"Failed to get webhook {webhook_id}: {e}")
            raise
    
    def list_webhooks(self, app_id: str) -> List[Dict[str, Any]]:
        """List webhooks for an app.
        
        Args:
            app_id: App ID.
            
        Returns:
            List of webhooks.
        """
        try:
            response = self.client.list_webhooks(appId=app_id)
            webhooks = response['webhooks']
            
            with self._cache_lock:
                for webhook in webhooks:
                    self._webhooks_cache[webhook['webhookId']] = webhook
            
            return webhooks
            
        except ClientError as e:
            logger.error(f"Failed to list webhooks: {e}")
            raise
    
    def update_webhook(self, app_id: str, webhook_id: str,
                       webhook_name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """Update a webhook.
        
        Args:
            app_id: App ID.
            webhook_id: Webhook ID.
            webhook_name: New webhook name.
            description: New description.
            
        Returns:
            Updated webhook information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'webhookId': webhook_id,
                'webhookName': webhook_name,
            }
            
            if description:
                kwargs['description'] = description
            
            response = self.client.update_webhook(**kwargs)
            webhook = response['webhook']
            
            with self._cache_lock:
                self._webhooks_cache[webhook_id] = webhook
            
            logger.info(f"Updated webhook: {webhook_id}")
            return webhook
            
        except ClientError as e:
            logger.error(f"Failed to update webhook {webhook_id}: {e}")
            raise
    
    def delete_webhook(self, app_id: str, webhook_id: str) -> bool:
        """Delete a webhook.
        
        Args:
            app_id: App ID.
            webhook_id: Webhook ID.
            
        Returns:
            True if deleted successfully.
        """
        try:
            self.client.delete_webhook(appId=app_id, webhookId=webhook_id)
            
            with self._cache_lock:
                if webhook_id in self._webhooks_cache:
                    del self._webhooks_cache[webhook_id]
            
            logger.info(f"Deleted webhook: {webhook_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete webhook {webhook_id}: {e}")
            raise
    
    # ==================== Backend Environments ====================
    
    def create_backend_environment(self, app_id: str,
                                   env_config: BackendEnvironmentConfig) -> Dict[str, Any]:
        """Create a backend environment.
        
        Args:
            app_id: App ID.
            env_config: Backend environment configuration.
            
        Returns:
            Created backend environment information.
        """
        try:
            kwargs = {
                'appId': app_id,
                'environmentName': env_config.environment_name,
            }
            
            if env_config.stack_name:
                kwargs['stackName'] = env_config.stack_name
            if env_config.deployment_mechanism:
                kwargs['deploymentMechanism'] = env_config.deployment_mechanism
            
            response = self.client.create_backend_environment(**kwargs)
            env = response['backendEnvironment']
            
            with self._cache_lock:
                self._backend_envs_cache[env['environmentId']] = env
            
            logger.info(f"Created backend environment: {env['environmentId']}")
            return env
            
        except ClientError as e:
            logger.error(f"Failed to create backend environment: {e}")
            raise
    
    def get_backend_environment(self, app_id: str,
                                 environment_id: str) -> Dict[str, Any]:
        """Get backend environment information.
        
        Args:
            app_id: App ID.
            environment_id: Environment ID.
            
        Returns:
            Backend environment information.
        """
        try:
            with self._cache_lock:
                if environment_id in self._backend_envs_cache:
                    return self._backend_envs_cache[environment_id]
            
            response = self.client.get_backend_environment(
                appId=app_id,
                environmentId=environment_id
            )
            env = response['backendEnvironment']
            
            with self._cache_lock:
                self._backend_envs_cache[environment_id] = env
            
            return env
            
        except ClientError as e:
            logger.error(f"Failed to get backend environment {environment_id}: {e}")
            raise
    
    def list_backend_environments(self, app_id: str) -> List[Dict[str, Any]]:
        """List backend environments for an app.
        
        Args:
            app_id: App ID.
            
        Returns:
            List of backend environments.
        """
        try:
            response = self.client.list_backend_environments(appId=app_id)
            envs = response['backendEnvironments']
            
            with self._cache_lock:
                for env in envs:
                    self._backend_envs_cache[env['environmentId']] = env
            
            return envs
            
        except ClientError as e:
            logger.error(f"Failed to list backend environments: {e}")
            raise
    
    def delete_backend_environment(self, app_id: str,
                                    environment_id: str) -> bool:
        """Delete a backend environment.
        
        Args:
            app_id: App ID.
            environment_id: Environment ID.
            
        Returns:
            True if deleted successfully.
        """
        try:
            self.client.delete_backend_environment(
                appId=app_id,
                environmentId=environment_id
            )
            
            with self._cache_lock:
                if environment_id in self._backend_envs_cache:
                    del self._backend_envs_cache[environment_id]
            
            logger.info(f"Deleted backend environment: {environment_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete backend environment {environment_id}: {e}")
            raise
    
    # ==================== Artifacts Management ====================
    
    def list_artifacts(self, app_id: str, branch_name: str,
                       deployment_id: str) -> List[Dict[str, Any]]:
        """List artifacts for a deployment.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            deployment_id: Deployment ID.
            
        Returns:
            List of artifacts.
        """
        try:
            response = self.client.list_artifacts(
                appId=app_id,
                branchName=branch_name,
                deploymentId=deployment_id
            )
            return response['artifacts']
            
        except ClientError as e:
            logger.error(f"Failed to list artifacts: {e}")
            raise
    
    def get_artifact_url(self, app_id: str, artifact_id: str) -> str:
        """Get artifact download URL.
        
        Args:
            app_id: App ID.
            artifact_id: Artifact ID.
            
        Returns:
            Artifact download URL.
        """
        try:
            response = self.client.get_artifact_url(
                appId=app_id,
                artifactId=artifact_id
            )
            return response['artifactUrl']
            
        except ClientError as e:
            logger.error(f"Failed to get artifact URL {artifact_id}: {e}")
            raise
    
    # ==================== Deploy Preview ====================
    
    def create_deploy_preview(self, app_id: str,
                               preview_config: DeployPreviewConfig) -> Dict[str, Any]:
        """Create a deploy preview configuration.
        
        Args:
            app_id: App ID.
            preview_config: Deploy preview configuration.
            
        Returns:
            Deploy preview configuration.
        """
        try:
            branch_config = BranchConfig(
                branch_name=preview_config.branch_name,
                enable_notification=True,
                pull_request_environment_name=preview_config.pull_request_environment_name
            )
            
            self.update_branch(app_id, preview_config.branch_name, branch_config)
            
            response = self.get_branch(app_id, preview_config.branch_name)
            
            logger.info(f"Created deploy preview: {app_id}/{preview_config.branch_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create deploy preview: {e}")
            raise
    
    def get_deploy_preview(self, app_id: str, branch_name: str) -> Dict[str, Any]:
        """Get deploy preview status.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            Deploy preview information.
        """
        try:
            branch = self.get_branch(app_id, branch_name)
            
            preview_info = {
                'branchName': branch_name,
                'previewEnabled': branch.get('enableAutoBuild', False),
                'pullRequestUrl': branch.get('pullRequestURL'),
                'stage': branch.get('stage'),
            }
            
            return preview_info
            
        except ClientError as e:
            logger.error(f"Failed to get deploy preview: {e}")
            raise
    
    def list_deploy_previews(self, app_id: str) -> List[Dict[str, Any]]:
        """List all deploy previews for an app.
        
        Args:
            app_id: App ID.
            
        Returns:
            List of deploy previews.
        """
        try:
            branches = self.list_branches(app_id)
            
            previews = []
            for branch in branches:
                if branch.get('pullRequestId'):
                    previews.append({
                        'branchName': branch['branchName'],
                        'previewEnabled': branch.get('enableAutoBuild', False),
                        'pullRequestId': branch.get('pullRequestId'),
                        'pullRequestUrl': branch.get('pullRequestURL'),
                        'stage': branch.get('stage'),
                    })
            
            return previews
            
        except ClientError as e:
            logger.error(f"Failed to list deploy previews: {e}")
            raise
    
    # ==================== Build Management ====================
    
    def start_build(self, app_id: str, branch_name: str) -> Dict[str, Any]:
        """Start a build.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            Build job information.
        """
        try:
            response = self.client.start_build(
                appId=app_id,
                branchName=branch_name
            )
            
            logger.info(f"Started build: {app_id}/{branch_name}")
            return response['buildJob']
            
        except ClientError as e:
            logger.error(f"Failed to start build: {e}")
            raise
    
    def get_build(self, app_id: str, branch_name: str,
                  build_id: str) -> Dict[str, Any]:
        """Get build information.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            build_id: Build ID.
            
        Returns:
            Build information.
        """
        try:
            response = self.client.get_build(
                appId=app_id,
                branchName=branch_name,
                buildId=build_id
            )
            return response['buildJob']
            
        except ClientError as e:
            logger.error(f"Failed to get build {build_id}: {e}")
            raise
    
    def list_builds(self, app_id: str, branch_name: str) -> List[Dict[str, Any]]:
        """List builds for a branch.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            List of builds.
        """
        try:
            response = self.client.list_builds(
                appId=app_id,
                branchName=branch_name
            )
            return response['builds']
            
        except ClientError as e:
            logger.error(f"Failed to list builds: {e}")
            raise
    
    def stop_build(self, app_id: str, branch_name: str,
                   build_id: str) -> bool:
        """Stop a build.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            build_id: Build ID.
            
        Returns:
            True if stopped successfully.
        """
        try:
            self.client.stop_build(
                appId=app_id,
                branchName=branch_name,
                buildId=build_id
            )
            
            logger.info(f"Stopped build: {app_id}/{branch_name}/{build_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to stop build {build_id}: {e}")
            raise
    
    # ==================== CloudWatch Metrics ====================
    
    def get_build_metrics(self, app_id: str, branch_name: str,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> List[MetricsData]:
        """Get build metrics from CloudWatch.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            start_time: Start time for metrics query.
            end_time: End time for metrics query.
            
        Returns:
            List of metrics data.
        """
        try:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=7)
            if end_time is None:
                end_time = datetime.now()
            
            namespace = 'AWS/Amplify'
            dimensions = [
                {'Name': 'App', 'Value': app_id},
                {'Name': 'Branch', 'Value': branch_name}
            ]
            
            metrics_to_get = [
                'Build Time',
                'Build Duration',
                'Number of Builds',
                'Number of Failed Builds',
                'Number of Successful Builds',
            ]
            
            all_metrics = []
            
            for metric_name in metrics_to_get:
                try:
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace=namespace,
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Average', 'Sum', 'Maximum', 'Minimum']
                    )
                    
                    for point in response['Datapoints']:
                        all_metrics.append(MetricsData(
                            metric_name=metric_name,
                            value=point['Average'],
                            unit=point.get('Unit', 'None'),
                            timestamp=point['Timestamp'],
                            dimensions={'App': app_id, 'Branch': branch_name}
                        ))
                        
                except Exception as e:
                    logger.warning(f"Failed to get metric {metric_name}: {e}")
            
            return all_metrics
            
        except ClientError as e:
            logger.error(f"Failed to get build metrics: {e}")
            raise
    
    def get_deployment_metrics(self, app_id: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> List[MetricsData]:
        """Get deployment metrics from CloudWatch.
        
        Args:
            app_id: App ID.
            start_time: Start time for metrics query.
            end_time: End time for metrics query.
            
        Returns:
            List of metrics data.
        """
        try:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=7)
            if end_time is None:
                end_time = datetime.now()
            
            namespace = 'AWS/Amplify'
            dimensions = [{'Name': 'App', 'Value': app_id}]
            
            metrics_to_get = [
                'Deployments',
                'Deployment Duration',
                'Number of Deployments',
                'Number of Failed Deployments',
                'Number of Successful Deployments',
            ]
            
            all_metrics = []
            
            for metric_name in metrics_to_get:
                try:
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace=namespace,
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Average', 'Sum', 'Maximum', 'Minimum']
                    )
                    
                    for point in response['Datapoints']:
                        all_metrics.append(MetricsData(
                            metric_name=metric_name,
                            value=point['Average'],
                            unit=point.get('Unit', 'None'),
                            timestamp=point['Timestamp'],
                            dimensions={'App': app_id}
                        ))
                        
                except Exception as e:
                    logger.warning(f"Failed to get metric {metric_name}: {e}")
            
            return all_metrics
            
        except ClientError as e:
            logger.error(f"Failed to get deployment metrics: {e}")
            raise
    
    def put_metric_data(self, metric_name: str, value: float,
                        unit: str = 'Count',
                        dimensions: Optional[Dict[str, str]] = None) -> bool:
        """Put custom metric data to CloudWatch.
        
        Args:
            metric_name: Metric name.
            value: Metric value.
            unit: Metric unit.
            dimensions: Metric dimensions.
            
        Returns:
            True if successful.
        """
        try:
            kwargs = {
                'Namespace': 'RabAI/Amplify',
                'MetricData': [
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': datetime.now(),
                    }
                ]
            }
            
            if dimensions:
                kwargs['MetricData'][0]['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            self.cloudwatch_client.put_metric_data(**kwargs)
            
            logger.info(f"Put metric: {metric_name} = {value}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def create_alarm(self, alarm_name: str, metric_name: str,
                     comparison_operator: str, threshold: float,
                     period: int = 60, evaluation_periods: int = 1,
                     dimensions: Optional[Dict[str, str]] = None) -> str:
        """Create a CloudWatch alarm for Amplify metrics.
        
        Args:
            alarm_name: Alarm name.
            metric_name: Metric name.
            comparison_operator: Comparison operator.
            threshold: Threshold value.
            period: Evaluation period in seconds.
            evaluation_periods: Number of evaluation periods.
            dimensions: Metric dimensions.
            
        Returns:
            Alarm ARN.
        """
        try:
            kwargs = {
                'AlarmName': alarm_name,
                'MetricName': metric_name,
                'Namespace': 'AWS/Amplify',
                'Statistic': 'Average',
                'Period': period,
                'EvaluationPeriods': evaluation_periods,
                'Threshold': threshold,
                'ComparisonOperator': comparison_operator,
            }
            
            if dimensions:
                kwargs['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            response = self.cloudwatch_client.put_alarm(**kwargs)
            
            logger.info(f"Created alarm: {alarm_name}")
            return alarm_name
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    # ==================== Utility Methods ====================
    
    def get_app_url(self, app_id: str) -> str:
        """Get Amplify app console URL.
        
        Args:
            app_id: App ID.
            
        Returns:
            Console URL.
        """
        return f"https://{self.config.region_name}.console.aws.amazon.com/amplify/home?region={self.config.region_name}#/{app_id}"
    
    def get_branch_url(self, app_id: str, branch_name: str) -> str:
        """Get branch URL.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            
        Returns:
            Branch URL.
        """
        return f"{self.get_app_url(app_id)}/{branch_name}"
    
    def wait_for_build(self, app_id: str, branch_name: str,
                       build_id: str, timeout: int = 3600,
                       poll_interval: int = 30) -> bool:
        """Wait for build to complete.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            build_id: Build ID.
            timeout: Maximum wait time in seconds.
            poll_interval: Poll interval in seconds.
            
        Returns:
            True if build succeeded.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            build = self.get_build(app_id, branch_name, build_id)
            status = build.get('buildStatus')
            
            if status in ['SUCCEEDED', 'FAILED', 'FAULT', 'STOPPED', 'TIMED_OUT']:
                return status == 'SUCCEEDED'
            
            time.sleep(poll_interval)
        
        return False
    
    def wait_for_deployment(self, app_id: str, branch_name: str,
                            deployment_id: str, timeout: int = 3600,
                            poll_interval: int = 30) -> bool:
        """Wait for deployment to complete.
        
        Args:
            app_id: App ID.
            branch_name: Branch name.
            deployment_id: Deployment ID.
            timeout: Maximum wait time in seconds.
            poll_interval: Poll interval in seconds.
            
        Returns:
            True if deployment succeeded.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            deployment = self.get_deployment(app_id, branch_name, deployment_id)
            status = deployment.get('status')
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status == 'SUCCEEDED'
            
            time.sleep(poll_interval)
        
        return False
    
    def clear_cache(self):
        """Clear all cached data."""
        with self._cache_lock:
            self._apps_cache.clear()
            self._branches_cache.clear()
            self._deployments_cache.clear()
            self._domains_cache.clear()
            self._webhooks_cache.clear()
            self._backend_envs_cache.clear()
    
    def close(self):
        """Close the integration and cleanup resources."""
        self._client = None
        self._cloudwatch_client = None
        self._resourcegroups_client = None
        self.clear_cache()


def create_amplify_integration(config: Optional[AmplifyConfig] = None) -> AmplifyIntegration:
    """Create an AmplifyIntegration instance.
    
    Args:
        config: Optional configuration.
        
    Returns:
        AmplifyIntegration instance.
    """
    return AmplifyIntegration(config)


# Convenience functions for common operations
def create_app(name: str, repository: Optional[str] = None,
               platform: str = "WEB", **kwargs) -> AmplifyIntegration:
    """Create a new Amplify app.
    
    Args:
        name: App name.
        repository: Repository URL.
        platform: Platform type.
        **kwargs: Additional app configuration.
        
    Returns:
        AmplifyIntegration instance.
    """
    config = AmplifyConfig(**kwargs) if kwargs else AmplifyConfig()
    integration = AmplifyIntegration(config)
    
    app_config = AppConfig(
        name=name,
        repository=repository,
        platform=platform
    )
    
    for key, value in kwargs.items():
        if hasattr(app_config, key):
            setattr(app_config, key, value)
    
    return integration.create_app(app_config)


def list_all_apps(**kwargs) -> List[Dict[str, Any]]:
    """List all Amplify apps.
    
    Args:
        **kwargs: Configuration parameters.
        
    Returns:
        List of apps.
    """
    config = AmplifyConfig(**kwargs) if kwargs else AmplifyConfig()
    integration = AmplifyIntegration(config)
    return integration.list_apps()
