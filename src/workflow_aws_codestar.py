"""
AWS CodeStar Integration Module for Workflow System

Implements a CodeStarIntegration class with:
1. Project management: Create/manage CodeStar projects
2. Team members: Manage project team
3. Resources: List project resources
4. Application lifecycle: Application lifecycle management
5. Notifications: CodeStar notifications
6. Connections: GitHub connections
7. Project templates: CodeStar project templates
8. Pipelines: CodeStar pipeline integration
9. Tools: CodeStar tools integration
10. CloudWatch integration: Project metrics and monitoring

Commit: 'feat(aws-codestar): add AWS CodeStar with project management, team members, resources, application lifecycle, notifications, GitHub connections, project templates, pipelines, CloudWatch'
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


class ProjectStatus(Enum):
    """CodeStar project status states."""
    CREATING = "Creating"
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    PENDING = "Pending"
    ERROR = "Error"
    DELETING = "Deleting"
    DELETED = "Deleted"


class ProjectTemplate(Enum):
    """CodeStar project templates."""
    WEB_APP_PYTHON = "python-web-app"
    WEB_APP_NODE = "nodejs-web-app"
    WEB_APP_JAVA = "java-web-app"
    WEB_APP_DOTNET = "dotnet-web-app"
    Lambda_PYTHON = "aws-lambda-python"
    Lambda_NODE = "aws-lambda-nodejs"
    Lambda_JAVA = "aws-lambda-java"
    EC2_PYTHON = "ec2-python"
    EC2_NODE = "ec2-nodejs"
    ECS_CANARY = "ecs-canary"
    CODEPIPELINE = "codepipeline"
    BEanstalk_PYTHON = "elastic-beanstalk-python"
    BEanstalk_NODE = "elastic-beanstalk-nodejs"
    BEanstalk_JAVA = "elastic-beanstalk-java"
    BEanstalk_DOTNET = "elastic-beanstalk-dotnet"


class TeamMemberRole(Enum):
    """CodeStar team member roles."""
    OWNER = "Owner"
    CONTRIBUTOR = "Contributor"
    VIEWER = "Viewer"


class Permission(Enum):
    """CodeStar permissions."""
    PULL_REQUESTS = "pullrequests"
    COMMITS = "commits"
    CODE = "code"
    DEPLOYMENTS = "deployments"
    BUILDS = "builds"
    PIPELINES = "pipelines"


class ResourceStatus(Enum):
    """Resource status states."""
    CREATING = "CREATING"
    CREATED = "CREATED"
    DELETING = "DELETING"
    DELETED = "DELETED"
    ERROR = "ERROR"


class ApplicationStatus(Enum):
    """Application lifecycle status."""
    PENDING = "PENDING"
    DEPLOYING = "DEPLOYING"
    DEPLOYED = "DEPLOYED"
    ERROR = "ERROR"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"


class NotificationEventType(Enum):
    """CodeStar notification event types."""
    PROJECT_CREATED = "codestar:project-created"
    PROJECT_UPDATED = "codestar:project-updated"
    PROJECT_DELETED = "codestar:project-deleted"
    PIPELINE_STARTED = "codestar:pipeline-started"
    PIPELINE_SUCCEEDED = "codestar:pipeline-succeeded"
    PIPELINE_FAILED = "codestar:pipeline-failed"
    PIPELINE_CANCELLED = "codestar:pipeline-cancelled"
    DEPLOYMENT_STARTED = "codestar:deployment-started"
    DEPLOYMENT_SUCCEEDED = "codestar:deployment-succeeded"
    DEPLOYMENT_FAILED = "codestar:deployment-failed"
    TEAM_MEMBER_ADDED = "codestar:team-member-added"
    TEAM_MEMBER_REMOVED = "codestar:team-member-removed"
    REPO_CREATED = "codestar:repo-created"
    REPO_PUSHED = "codestar:repo-pushed"


class ConnectionStatus(Enum):
    """GitHub connection status."""
    PENDING = "PENDING"
    AVAILABLE = "AVAILABLE"
    ERROR = "ERROR"
    DELETED = "DELETED"


@dataclass
class Project:
    """CodeStar project representation."""
    id: str
    name: str
    description: str
    region: str
    template_id: str
    status: ProjectStatus
    created_time: datetime
    updated_time: datetime
    stack_id: Optional[str] = None
    application_arn: Optional[str] = None
    artifact_store_bucket: Optional[str] = None
    repository_url: Optional[str] = None
    project_readme_url: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TeamMember:
    """CodeStar team member representation."""
    user_arn: str
    project_arn: str
    role: TeamMemberRole
    display_name: Optional[str] = None
    email_address: Optional[str] = None
    remote_access_allowed: bool = False


@dataclass
class ProjectResource:
    """CodeStar project resource representation."""
    id: str
    project_arn: str
    type: str
    name: str
    status: ResourceStatus
    created_time: datetime
    updated_time: datetime


@dataclass
class Application:
    """Application lifecycle representation."""
    arn: str
    project_id: str
    name: str
    status: ApplicationStatus
    created_time: datetime
    updated_time: datetime
    last_deployment_time: Optional[datetime] = None
    deployment_history: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class NotificationRule:
    """Notification rule representation."""
    id: str
    arn: str
    name: str
    project_arn: str
    event_types: List[str]
    target_type: str
    target_address: str
    enabled: bool = True
    created_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None


@dataclass
class Connection:
    """GitHub connection representation."""
    connection_arn: str
    connection_id: str
    name: str
    status: ConnectionStatus
    owner_account_id: str
    provider_type: str
    created_time: datetime


@dataclass
class ProjectTemplateInfo:
    """Project template information."""
    id: str
    name: str
    description: str
    category: str
    tags: List[str]
    is_available: bool = True


@dataclass
class Pipeline:
    """CodeStar pipeline representation."""
    arn: str
    project_id: str
    name: str
    status: str
    created_time: datetime
    updated_time: datetime
    stages: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Toolchain:
    """CodeStar toolchain representation."""
    arn: str
    project_id: str
    name: str
    status: str
    created_time: datetime
    updated_time: datetime
    template_url: Optional[str] = None
    service_role_arn: Optional[str] = None


@dataclass
class ProjectMetrics:
    """CloudWatch metrics for a project."""
    project_id: str
    pipeline_execution_count: int = 0
    successful_deployments: int = 0
    failed_deployments: int = 0
    average_deployment_duration: float = 0.0
    total_build_duration: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class CodeStarIntegration:
    """AWS CodeStar Integration for project management and CI/CD orchestration."""

    def __init__(self, region: str = "us-east-1", profile_name: Optional[str] = None):
        """Initialize CodeStar integration.
        
        Args:
            region: AWS region
            profile_name: Optional AWS profile name
        """
        self.region = region
        self.profile_name = profile_name
        self.boto3_available = BOTO3_AVAILABLE
        
        if BOTO3_AVAILABLE:
            session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
            self.codestar_client = session.client("codestar", region_name=region)
            self.codestar_notifications_client = session.client("codestar-notifications", region_name=region)
            self.codepipeline_client = session.client("codepipeline", region_name=region)
            self.codeconnections_client = session.client("codeconnections", region_name=region)
            self.cloudwatch_client = session.client("cloudwatch", region_name=region)
            self.cloudformation_client = session.client("cloudformation", region_name=region)
            self.iam_client = session.client("iam", region_name=region)
        
        self._projects: Dict[str, Project] = {}
        self._team_members: Dict[str, List[TeamMember]] = defaultdict(list)
        self._resources: Dict[str, List[ProjectResource]] = defaultdict(list)
        self._applications: Dict[str, Application] = {}
        self._notification_rules: Dict[str, NotificationRule] = {}
        self._connections: Dict[str, Connection] = {}
        self._templates: Dict[str, ProjectTemplateInfo] = {}
        self._pipelines: Dict[str, Pipeline] = {}
        self._toolchains: Dict[str, Toolchain] = {}
        self._metrics: Dict[str, ProjectMetrics] = {}
        self._callbacks: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()

    def _generate_id(self, prefix: str) -> str:
        """Generate a unique ID."""
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _to_datetime(self, dt: Any) -> datetime:
        """Convert various datetime formats."""
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, str):
            try:
                return datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except ValueError:
                return datetime.now()
        return datetime.now()

    def _invoke_callbacks(self, event_type: str, data: Any):
        """Invoke registered callbacks for an event."""
        with self._lock:
            callbacks = self._callbacks.get(event_type, []).copy()
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(data))
                else:
                    callback(data)
            except Exception as e:
                logger.error(f"Callback error for {event_type}: {e}")

    def on(self, event_type: str, callback: Callable):
        """Register a callback for an event type.
        
        Args:
            event_type: Event type to listen for
            callback: Callback function
        """
        with self._lock:
            self._callbacks[event_type].append(callback)

    # =========================================================================
    # 1. PROJECT MANAGEMENT
    # =========================================================================

    async def create_project(
        self,
        name: str,
        description: str,
        template_id: str,
        tags: Optional[Dict[str, str]] = None,
        stack_name: Optional[str] = None
    ) -> Project:
        """Create a new CodeStar project.
        
        Args:
            name: Project name
            description: Project description
            template_id: Project template ID
            tags: Optional project tags
            stack_name: Optional CloudFormation stack name
            
        Returns:
            Created Project object
        """
        project_id = name.lower().replace(" ", "-")[:20]
        project_id = f"{project_id}-{uuid.uuid4().hex[:8]}"
        
        if self.boto3_available:
            try:
                params = {
                    "name": name,
                    "id": project_id,
                    "description": description,
                    "templateCodeStarProjectTemplateId": template_id,
                }
                if tags:
                    params["tags"] = tags
                if stack_name:
                    params["stackName"] = stack_name
                
                response = self.codestar_client.create_project(**params)
                
                project = Project(
                    id=response.get("projectId", project_id),
                    name=name,
                    description=description,
                    region=self.region,
                    template_id=template_id,
                    status=ProjectStatus.CREATING,
                    created_time=datetime.now(),
                    updated_time=datetime.now(),
                    stack_id=response.get("stackId"),
                    application_arn=response.get("applicationArn"),
                    artifact_store_bucket=response.get("artifactStore", {}).get("bucket"),
                    repository_url=response.get("sourceCode", [{}])[0].get("repositoryUrl") if response.get("sourceCode") else None,
                    tags=tags or {}
                )
            except ClientError as e:
                logger.error(f"Failed to create project: {e}")
                raise
        else:
            project = Project(
                id=project_id,
                name=name,
                description=description,
                region=self.region,
                template_id=template_id,
                status=ProjectStatus.CREATING,
                created_time=datetime.now(),
                updated_time=datetime.now(),
                tags=tags or {}
            )
        
        with self._lock:
            self._projects[project.id] = project
        
        self._invoke_callbacks("project.created", project)
        logger.info(f"Created project: {project.id}")
        return project

    async def get_project(self, project_id: str) -> Optional[Project]:
        """Get a project by ID.
        
        Args:
            project_id: Project ID
            
        Returns:
            Project object or None
        """
        if project_id in self._projects:
            return self._projects[project_id]
        
        if self.boto3_available:
            try:
                response = self.codestar_client.describe_project(projectId=project_id)
                project = Project(
                    id=response.get("id", project_id),
                    name=response.get("name", ""),
                    description=response.get("description", ""),
                    region=self.region,
                    template_id=response.get("projectTemplateId", ""),
                    status=ProjectStatus(response.get("status", "CREATING")),
                    created_time=self._to_datetime(response.get("createdTime")),
                    updated_time=self._to_datetime(response.get("updatedTime")),
                    stack_id=response.get("stackId"),
                    application_arn=response.get("applicationArn"),
                    artifact_store_bucket=response.get("artifactStore", {}).get("bucket"),
                    repository_url=response.get("sourceCode", [{}])[0].get("repositoryUrl") if response.get("sourceCode") else None,
                    tags=response.get("tags", {})
                )
                with self._lock:
                    self._projects[project.id] = project
                return project
            except ClientError:
                return None
        return None

    async def list_projects(
        self,
        filter_by_tag: Optional[Dict[str, str]] = None,
        sort_by: Optional[str] = None
    ) -> List[Project]:
        """List CodeStar projects.
        
        Args:
            filter_by_tag: Optional tag filters
            sort_by: Optional sort key
            
        Returns:
            List of Project objects
        """
        projects = list(self._projects.values())
        
        if self.boto3_available:
            try:
                response = self.codestar_client.list_projects()
                for item in response.get("projects", []):
                    project_id = item.get("projectId")
                    if project_id and project_id not in self._projects:
                        project = await self.get_project(project_id)
                        if project:
                            projects.append(project)
            except ClientError as e:
                logger.error(f"Failed to list projects: {e}")
        
        if filter_by_tag:
            projects = [
                p for p in projects
                if all(p.tags.get(k) == v for k, v in filter_by_tag.items())
            ]
        
        return projects

    async def update_project(
        self,
        project_id: str,
        description: Optional[str] = None,
        name: Optional[str] = None
    ) -> Project:
        """Update a project.
        
        Args:
            project_id: Project ID
            description: New description
            name: New name
            
        Returns:
            Updated Project object
        """
        project = await self.get_project(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")
        
        if self.boto3_available:
            try:
                params = {"projectId": project_id}
                if description:
                    params["description"] = description
                if name:
                    params["name"] = name
                
                self.codestar_client.update_project(**params)
            except ClientError as e:
                logger.error(f"Failed to update project: {e}")
                raise
        
        if description:
            project.description = description
        if name:
            project.name = name
        project.updated_time = datetime.now()
        
        self._invoke_callbacks("project.updated", project)
        logger.info(f"Updated project: {project_id}")
        return project

    async def delete_project(self, project_id: str) -> bool:
        """Delete a project.
        
        Args:
            project_id: Project ID
            
        Returns:
            True if successful
        """
        project = await self.get_project(project_id)
        if not project:
            return False
        
        if self.boto3_available:
            try:
                self.codestar_client.delete_project(projectId=project_id)
            except ClientError as e:
                logger.error(f"Failed to delete project: {e}")
                raise
        
        with self._lock:
            if project_id in self._projects:
                del self._projects[project_id]
        
        self._invoke_callbacks("project.deleted", project_id)
        logger.info(f"Deleted project: {project_id}")
        return True

    # =========================================================================
    # 2. TEAM MEMBERS
    # =========================================================================

    async def add_team_member(
        self,
        project_id: str,
        user_arn: str,
        role: TeamMemberRole,
        display_name: Optional[str] = None,
        email_address: Optional[str] = None,
        remote_access_allowed: bool = False
    ) -> TeamMember:
        """Add a team member to a project.
        
        Args:
            project_id: Project ID
            user_arn: User ARN
            role: Team member role
            display_name: Optional display name
            email_address: Optional email address
            remote_access_allowed: Whether remote access is allowed
            
        Returns:
            TeamMember object
        """
        project = await self.get_project(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")
        
        project_arn = f"arn:aws:codestar:{self.region}:{self._get_account_id()}:project/{project_id}"
        
        if self.boto3_available:
            try:
                self.codestar_client.associate_team_member(
                    projectId=project_id,
                    userArn=user_arn,
                    projectRole=role.value,
                    remoteAccessAllowed=remote_access_allowed
                )
            except ClientError as e:
                logger.error(f"Failed to add team member: {e}")
                raise
        
        member = TeamMember(
            user_arn=user_arn,
            project_arn=project_arn,
            role=role,
            display_name=display_name,
            email_address=email_address,
            remote_access_allowed=remote_access_allowed
        )
        
        with self._lock:
            self._team_members[project_id].append(member)
        
        self._invoke_callbacks("team.member.added", member)
        logger.info(f"Added team member {user_arn} to project {project_id}")
        return member

    async def remove_team_member(self, project_id: str, user_arn: str) -> bool:
        """Remove a team member from a project.
        
        Args:
            project_id: Project ID
            user_arn: User ARN
            
        Returns:
            True if successful
        """
        if self.boto3_available:
            try:
                self.codestar_client.disassociate_team_member(
                    projectId=project_id,
                    userArn=user_arn
                )
            except ClientError as e:
                logger.error(f"Failed to remove team member: {e}")
                raise
        
        with self._lock:
            members = self._team_members.get(project_id, [])
            self._team_members[project_id] = [m for m in members if m.user_arn != user_arn]
        
        self._invoke_callbacks("team.member.removed", user_arn)
        logger.info(f"Removed team member {user_arn} from project {project_id}")
        return True

    async def list_team_members(self, project_id: str) -> List[TeamMember]:
        """List team members of a project.
        
        Args:
            project_id: Project ID
            
        Returns:
            List of TeamMember objects
        """
        if self._team_members.get(project_id):
            return self._team_members[project_id].copy()
        
        if self.boto3_available:
            try:
                response = self.codestar_client.list_team_members(projectId=project_id)
                members = []
                for item in response.get("teamMembers", []):
                    member = TeamMember(
                        user_arn=item.get("userArn", ""),
                        project_arn=item.get("projectArn", ""),
                        role=TeamMemberRole(item.get("projectRole", "VIEWER")),
                        display_name=item.get("displayName"),
                        email_address=item.get("emailAddress"),
                        remote_access_allowed=item.get("remoteAccessAllowed", False)
                    )
                    members.append(member)
                with self._lock:
                    self._team_members[project_id] = members
                return members
            except ClientError as e:
                logger.error(f"Failed to list team members: {e}")
        
        return []

    async def update_team_member_role(
        self,
        project_id: str,
        user_arn: str,
        role: TeamMemberRole
    ) -> TeamMember:
        """Update a team member's role.
        
        Args:
            project_id: Project ID
            user_arn: User ARN
            role: New role
            
        Returns:
            Updated TeamMember object
        """
        if self.boto3_available:
            try:
                self.codestar_client.update_team_member(
                    projectId=project_id,
                    userArn=user_arn,
                    projectRole=role.value
                )
            except ClientError as e:
                logger.error(f"Failed to update team member role: {e}")
                raise
        
        with self._lock:
            members = self._team_members.get(project_id, [])
            for member in members:
                if member.user_arn == user_arn:
                    member.role = role
                    self._invoke_callbacks("team.member.updated", member)
                    return member
        
        raise ValueError(f"Team member not found: {user_arn}")

    # =========================================================================
    # 3. PROJECT RESOURCES
    # =========================================================================

    async def list_project_resources(self, project_id: str) -> List[ProjectResource]:
        """List all resources associated with a project.
        
        Args:
            project_id: Project ID
            
        Returns:
            List of ProjectResource objects
        """
        if self._resources.get(project_id):
            return self._resources[project_id].copy()
        
        if self.boto3_available:
            try:
                response = self.codestar_client.list_project_resources(projectId=project_id)
                resources = []
                for item in response.get("resources", []):
                    resource = ProjectResource(
                        id=item.get("id", ""),
                        project_arn=item.get("projectArn", ""),
                        type=item.get("type", ""),
                        name=item.get("name", ""),
                        status=ResourceStatus(item.get("status", "CREATED")),
                        created_time=self._to_datetime(item.get("createdTime")),
                        updated_time=self._to_datetime(item.get("updatedTime"))
                    )
                    resources.append(resource)
                with self._lock:
                    self._resources[project_id] = resources
                return resources
            except ClientError as e:
                logger.error(f"Failed to list project resources: {e}")
        
        return []

    async def get_resource(self, project_id: str, resource_id: str) -> Optional[ProjectResource]:
        """Get a specific project resource.
        
        Args:
            project_id: Project ID
            resource_id: Resource ID
            
        Returns:
            ProjectResource object or None
        """
        resources = await self.list_project_resources(project_id)
        for resource in resources:
            if resource.id == resource_id:
                return resource
        return None

    # =========================================================================
    # 4. APPLICATION LIFECYCLE
    # =========================================================================

    async def create_application(
        self,
        project_id: str,
        name: str
    ) -> Application:
        """Create an application lifecycle entry.
        
        Args:
            project_id: Project ID
            name: Application name
            
        Returns:
            Application object
        """
        app = Application(
            arn=f"arn:aws:codestar:{self.region}:{self._get_account_id()}:application/{project_id}/{name}",
            project_id=project_id,
            name=name,
            status=ApplicationStatus.PENDING,
            created_time=datetime.now(),
            updated_time=datetime.now()
        )
        
        with self._lock:
            self._applications[f"{project_id}/{name}"] = app
        
        self._invoke_callbacks("application.created", app)
        logger.info(f"Created application: {app.name} for project {project_id}")
        return app

    async def get_application(self, project_id: str, name: str) -> Optional[Application]:
        """Get an application.
        
        Args:
            project_id: Project ID
            name: Application name
            
        Returns:
            Application object or None
        """
        return self._applications.get(f"{project_id}/{name}")

    async def update_application_status(
        self,
        project_id: str,
        name: str,
        status: ApplicationStatus
    ) -> Application:
        """Update application status.
        
        Args:
            project_id: Project ID
            name: Application name
            status: New status
            
        Returns:
            Updated Application object
        """
        app = await self.get_application(project_id, name)
        if not app:
            raise ValueError(f"Application not found: {project_id}/{name}")
        
        old_status = app.status
        app.status = status
        app.updated_time = datetime.now()
        
        if status == ApplicationStatus.DEPLOYED:
            app.last_deployment_time = datetime.now()
            app.deployment_history.append({
                "status": status.value,
                "timestamp": datetime.now().isoformat()
            })
        
        self._invoke_callbacks("application.status_changed", {
            "application": app,
            "old_status": old_status,
            "new_status": status
        })
        logger.info(f"Updated application {name} status to {status.value}")
        return app

    async def deploy_application(
        self,
        project_id: str,
        name: str,
        deployment_properties: Optional[Dict[str, str]] = None
    ) -> Application:
        """Deploy an application.
        
        Args:
            project_id: Project ID
            name: Application name
            deployment_properties: Optional deployment properties
            
        Returns:
            Updated Application object
        """
        app = await self.get_application(project_id, name)
        if not app:
            app = await self.create_application(project_id, name)
        
        await self.update_application_status(project_id, name, ApplicationStatus.DEPLOYING)
        
        deployment_info = {
            "status": "DEPLOYING",
            "timestamp": datetime.now().isoformat(),
            "properties": deployment_properties or {}
        }
        app.deployment_history.append(deployment_info)
        
        try:
            await self.update_application_status(project_id, name, ApplicationStatus.DEPLOYED)
        except Exception as e:
            await self.update_application_status(project_id, name, ApplicationStatus.ERROR)
            raise
        
        self._invoke_callbacks("application.deployed", app)
        logger.info(f"Deployed application: {name}")
        return app

    async def rollback_application(self, project_id: str, name: str) -> Application:
        """Rollback an application to previous version.
        
        Args:
            project_id: Project ID
            name: Application name
            
        Returns:
            Updated Application object
        """
        app = await self.get_application(project_id, name)
        if not app:
            raise ValueError(f"Application not found: {project_id}/{name}")
        
        await self.update_application_status(project_id, name, ApplicationStatus.ROLLING_BACK)
        
        try:
            await self.update_application_status(project_id, name, ApplicationStatus.ROLLED_BACK)
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            raise
        
        self._invoke_callbacks("application.rolled_back", app)
        logger.info(f"Rolled back application: {name}")
        return app

    # =========================================================================
    # 5. NOTIFICATIONS
    # =========================================================================

    async def create_notification_rule(
        self,
        name: str,
        project_arn: str,
        event_types: List[NotificationEventType],
        target_type: str,
        target_address: str,
        enabled: bool = True
    ) -> NotificationRule:
        """Create a notification rule.
        
        Args:
            name: Rule name
            project_arn: Project ARN
            event_types: List of event types
            target_type: Target type (SNS, Chatbot, etc.)
            target_address: Target address
            enabled: Whether rule is enabled
            
        Returns:
            NotificationRule object
        """
        rule_id = self._generate_id("rule")
        
        if self.boto3_available:
            try:
                response = self.codestar_notifications_client.create_notification_rule(
                    name=name,
                    resource=project_arn,
                    eventTypeIds=[e.value for e in event_types],
                    target=target_type,
                    targetAddress=target_address,
                    enabled=enabled
                )
                rule_id = response.get("arn", rule_id)
            except ClientError as e:
                logger.error(f"Failed to create notification rule: {e}")
                raise
        
        rule = NotificationRule(
            id=rule_id,
            arn=rule_id,
            name=name,
            project_arn=project_arn,
            event_types=[e.value for e in event_types],
            target_type=target_type,
            target_address=target_address,
            enabled=enabled,
            created_time=datetime.now(),
            updated_time=datetime.now()
        )
        
        with self._lock:
            self._notification_rules[rule.id] = rule
        
        self._invoke_callbacks("notification.created", rule)
        logger.info(f"Created notification rule: {name}")
        return rule

    async def get_notification_rule(self, rule_id: str) -> Optional[NotificationRule]:
        """Get a notification rule.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            NotificationRule object or None
        """
        return self._notification_rules.get(rule_id)

    async def list_notification_rules(
        self,
        project_arn: Optional[str] = None
    ) -> List[NotificationRule]:
        """List notification rules.
        
        Args:
            project_arn: Optional project ARN filter
            
        Returns:
            List of NotificationRule objects
        """
        rules = list(self._notification_rules.values())
        
        if project_arn:
            rules = [r for r in rules if r.project_arn == project_arn]
        
        return rules

    async def update_notification_rule(
        self,
        rule_id: str,
        enabled: Optional[bool] = None,
        event_types: Optional[List[NotificationEventType]] = None
    ) -> NotificationRule:
        """Update a notification rule.
        
        Args:
            rule_id: Rule ID
            enabled: Optional enabled status
            event_types: Optional event types
            
        Returns:
            Updated NotificationRule object
        """
        rule = self._notification_rules.get(rule_id)
        if not rule:
            raise ValueError(f"Notification rule not found: {rule_id}")
        
        if enabled is not None:
            rule.enabled = enabled
        
        if event_types:
            rule.event_types = [e.value for e in event_types]
        
        rule.updated_time = datetime.now()
        
        if self.boto3_available:
            try:
                params = {"arn": rule.arn}
                if enabled is not None:
                    params["enabled"] = enabled
                if event_types:
                    params["eventTypeIds"] = rule.event_types
                self.codestar_notifications_client.update_notification_rule(**params)
            except ClientError as e:
                logger.error(f"Failed to update notification rule: {e}")
        
        self._invoke_callbacks("notification.updated", rule)
        logger.info(f"Updated notification rule: {rule_id}")
        return rule

    async def delete_notification_rule(self, rule_id: str) -> bool:
        """Delete a notification rule.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            True if successful
        """
        rule = self._notification_rules.get(rule_id)
        if not rule:
            return False
        
        if self.boto3_available:
            try:
                self.codestar_notifications_client.delete_notification_rule(arn=rule.arn)
            except ClientError as e:
                logger.error(f"Failed to delete notification rule: {e}")
                raise
        
        with self._lock:
            del self._notification_rules[rule_id]
        
        self._invoke_callbacks("notification.deleted", rule_id)
        logger.info(f"Deleted notification rule: {rule_id}")
        return True

    async def describe_notification_events(
        self,
        project_arn: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Describe notification events for a project.
        
        Args:
            project_arn: Project ARN
            start_time: Optional start time
            end_time: Optional end time
            
        Returns:
            List of notification events
        """
        events = []
        
        if self.boto3_available:
            try:
                params = {"resource": project_arn}
                if start_time:
                    params["startTime"] = start_time.isoformat()
                if end_time:
                    params["endTime"] = end_time.isoformat()
                
                response = self.codestar_notifications_client.describe_notification_events(**params)
                events = response.get("events", [])
            except ClientError as e:
                logger.error(f"Failed to describe notification events: {e}")
        
        return events

    # =========================================================================
    # 6. GITHUB CONNECTIONS
    # =========================================================================

    async def create_connection(
        self,
        name: str,
        provider_type: str = "GitHub"
    ) -> Connection:
        """Create a GitHub connection.
        
        Args:
            name: Connection name
            provider_type: Provider type (GitHub, Bitbucket)
            
        Returns:
            Connection object
        """
        connection_id = self._generate_id("conn")
        
        if self.boto3_available:
            try:
                response = self.codeconnections_client.create_connection(
                    ProviderType=provider_type,
                    ConnectionName=name
                )
                connection_id = response.get("ConnectionArn", connection_id)
                status = ConnectionStatus(response.get("ConnectionStatus", "PENDING"))
            except ClientError as e:
                logger.error(f"Failed to create connection: {e}")
                raise
        
        connection = Connection(
            connection_arn=connection_id,
            connection_id=connection_id.split("/")[-1],
            name=name,
            status=ConnectionStatus.AVAILABLE,
            owner_account_id=self._get_account_id(),
            provider_type=provider_type,
            created_time=datetime.now()
        )
        
        with self._lock:
            self._connections[connection.connection_id] = connection
        
        self._invoke_callbacks("connection.created", connection)
        logger.info(f"Created connection: {name}")
        return connection

    async def get_connection(self, connection_id: str) -> Optional[Connection]:
        """Get a connection.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Connection object or None
        """
        if connection_id in self._connections:
            return self._connections[connection_id]
        
        if self.boto3_available:
            try:
                response = self.codeconnections_client.get_connection(ConnectionArn=connection_id)
                conn = response.get("Connection", {})
                connection = Connection(
                    connection_arn=conn.get("ConnectionArn", ""),
                    connection_id=conn.get("ConnectionId", ""),
                    name=conn.get("ConnectionName", ""),
                    status=ConnectionStatus(conn.get("ConnectionStatus", "PENDING")),
                    owner_account_id=conn.get("OwnerAccountId", ""),
                    provider_type=conn.get("ProviderType", ""),
                    created_time=self._to_datetime(conn.get("CreatedAt"))
                )
                with self._lock:
                    self._connections[connection.connection_id] = connection
                return connection
            except ClientError:
                return None
        
        return None

    async def list_connections(
        self,
        provider_type_filter: Optional[str] = None
    ) -> List[Connection]:
        """List connections.
        
        Args:
            provider_type_filter: Optional provider type filter
            
        Returns:
            List of Connection objects
        """
        connections = list(self._connections.values())
        
        if provider_type_filter:
            connections = [
                c for c in connections
                if c.provider_type == provider_type_filter
            ]
        
        if self.boto3_available:
            try:
                params = {}
                if provider_type_filter:
                    params["ProviderTypeFilter"] = provider_type_filter
                
                response = self.codeconnections_client.list_connections(**params)
                for item in response.get("Connections", []):
                    conn_id = item.get("ConnectionId")
                    if conn_id and conn_id not in self._connections:
                        conn = await self.get_connection(conn_id)
                        if conn:
                            connections.append(conn)
            except ClientError as e:
                logger.error(f"Failed to list connections: {e}")
        
        return connections

    async def delete_connection(self, connection_id: str) -> bool:
        """Delete a connection.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            True if successful
        """
        connection = self._connections.get(connection_id)
        
        if self.boto3_available:
            try:
                self.codeconnections_client.delete_connection(ConnectionArn=connection.connection_arn)
            except ClientError as e:
                logger.error(f"Failed to delete connection: {e}")
                raise
        
        with self._lock:
            if connection_id in self._connections:
                del self._connections[connection_id]
        
        self._invoke_callbacks("connection.deleted", connection_id)
        logger.info(f"Deleted connection: {connection_id}")
        return True

    async def update_connection(
        self,
        connection_id: str,
        name: Optional[str] = None
    ) -> Connection:
        """Update a connection.
        
        Args:
            connection_id: Connection ID
            name: New name
            
        Returns:
            Updated Connection object
        """
        connection = await self.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")
        
        if name:
            connection.name = name
        
        if self.boto3_available:
            try:
                self.codeconnections_client.update_connection(
                    ConnectionArn=connection.connection_arn,
                    ConnectionName=name
                )
            except ClientError as e:
                logger.error(f"Failed to update connection: {e}")
                raise
        
        self._invoke_callbacks("connection.updated", connection)
        return connection

    # =========================================================================
    # 7. PROJECT TEMPLATES
    # =========================================================================

    async def list_project_templates(
        self,
        category_filter: Optional[str] = None
    ) -> List[ProjectTemplateInfo]:
        """List CodeStar project templates.
        
        Args:
            category_filter: Optional category filter
            
        Returns:
            List of ProjectTemplateInfo objects
        """
        templates = list(self._templates.values())
        
        if not templates and self.boto3_available:
            try:
                response = self.codestar_client.list_project_templates()
                for item in response.get("projectTemplates", []):
                    template = ProjectTemplateInfo(
                        id=item.get("id", ""),
                        name=item.get("name", ""),
                        description=item.get("description", ""),
                        category=item.get("category", ""),
                        tags=item.get("tags", []),
                        is_available=item.get("available", True)
                    )
                    templates.append(template)
                    self._templates[template.id] = template
            except ClientError as e:
                logger.error(f"Failed to list project templates: {e}")
        
        if category_filter:
            templates = [t for t in templates if t.category == category_filter]
        
        return templates

    async def get_project_template(self, template_id: str) -> Optional[ProjectTemplateInfo]:
        """Get a project template.
        
        Args:
            template_id: Template ID
            
        Returns:
            ProjectTemplateInfo object or None
        """
        if template_id in self._templates:
            return self._templates[template_id]
        
        if self.boto3_available:
            try:
                response = self.codestar_client.get_project_template(id=template_id)
                item = response.get("projectTemplate", {})
                template = ProjectTemplateInfo(
                    id=item.get("id", ""),
                    name=item.get("name", ""),
                    description=item.get("description", ""),
                    category=item.get("category", ""),
                    tags=item.get("tags", []),
                    is_available=item.get("available", True)
                )
                self._templates[template_id] = template
                return template
            except ClientError:
                return None
        
        return None

    # =========================================================================
    # 8. PIPELINES
    # =========================================================================

    async def create_pipeline(
        self,
        project_id: str,
        name: str,
        stages: Optional[List[Dict[str, Any]]] = None
    ) -> Pipeline:
        """Create a pipeline for a project.
        
        Args:
            project_id: Project ID
            name: Pipeline name
            stages: Optional list of stages
            
        Returns:
            Pipeline object
        """
        project = await self.get_project(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")
        
        pipeline_arn = f"arn:aws:codepipeline:{self.region}:{self._get_account_id()}:{project_id}/{name}"
        
        if self.boto3_available:
            try:
                response = self.codepipeline_client.create_pipeline(
                    pipeline={
                        "name": f"{project_id}-{name}",
                        "roleArn": self._get_pipeline_role_arn(project_id),
                        "artifactStore": {
                            "type": "S3",
                            "location": project.artifact_store_bucket or f"{project_id}-artifacts"
                        },
                        "stages": stages or []
                    }
                )
                pipeline_arn = response.get("pipeline", {}).get("arn", pipeline_arn)
            except ClientError as e:
                logger.error(f"Failed to create pipeline: {e}")
                raise
        
        pipeline = Pipeline(
            arn=pipeline_arn,
            project_id=project_id,
            name=name,
            status="ACTIVE",
            created_time=datetime.now(),
            updated_time=datetime.now(),
            stages=stages or []
        )
        
        with self._lock:
            self._pipelines[f"{project_id}/{name}"] = pipeline
        
        self._invoke_callbacks("pipeline.created", pipeline)
        logger.info(f"Created pipeline: {name} for project {project_id}")
        return pipeline

    async def get_pipeline(self, project_id: str, name: str) -> Optional[Pipeline]:
        """Get a pipeline.
        
        Args:
            project_id: Project ID
            name: Pipeline name
            
        Returns:
            Pipeline object or None
        """
        key = f"{project_id}/{name}"
        if key in self._pipelines:
            return self._pipelines[key]
        
        if self.boto3_available:
            try:
                response = self.codepipeline_client.get_pipeline(name=f"{project_id}-{name}")
                p = response.get("pipeline", {})
                pipeline = Pipeline(
                    arn=p.get("arn", ""),
                    project_id=project_id,
                    name=name,
                    status=p.get("status", "ACTIVE"),
                    created_time=self._to_datetime(p.get("created")),
                    updated_time=self._to_datetime(p.get("updated")),
                    stages=p.get("stages", [])
                )
                with self._lock:
                    self._pipelines[key] = pipeline
                return pipeline
            except ClientError:
                return None
        
        return None

    async def list_pipelines(self, project_id: str) -> List[Pipeline]:
        """List pipelines for a project.
        
        Args:
            project_id: Project ID
            
        Returns:
            List of Pipeline objects
        """
        pipelines = [p for k, p in self._pipelines.items() if k.startswith(f"{project_id}/")]
        
        if self.boto3_available:
            try:
                response = self.codepipeline_client.list_pipelines()
                for item in response.get("pipelines", []):
                    name = item.get("name", "")
                    if name.startswith(f"{project_id}-"):
                        pipeline_name = name[len(f"{project_id}-"):]
                        pipeline = await self.get_pipeline(project_id, pipeline_name)
                        if pipeline and pipeline not in pipelines:
                            pipelines.append(pipeline)
            except ClientError as e:
                logger.error(f"Failed to list pipelines: {e}")
        
        return pipelines

    async def start_pipeline_execution(
        self,
        project_id: str,
        name: str,
        wait_for_completion: bool = False
    ) -> Dict[str, Any]:
        """Start a pipeline execution.
        
        Args:
            project_id: Project ID
            name: Pipeline name
            wait_for_completion: Whether to wait for completion
            
        Returns:
            Execution information
        """
        pipeline = await self.get_pipeline(project_id, name)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {project_id}/{name}")
        
        execution_id = self._generate_id("exec")
        
        if self.boto3_available:
            try:
                response = self.codepipeline_client.start_pipeline_execution(
                    name=f"{project_id}-{name}"
                )
                execution_id = response.get("pipelineExecutionId", execution_id)
            except ClientError as e:
                logger.error(f"Failed to start pipeline execution: {e}")
                raise
        
        self._invoke_callbacks("pipeline.started", {
            "project_id": project_id,
            "pipeline": name,
            "execution_id": execution_id
        })
        
        logger.info(f"Started pipeline execution: {execution_id}")
        return {
            "execution_id": execution_id,
            "pipeline": name,
            "project_id": project_id,
            "status": "InProgress"
        }

    async def get_pipeline_execution(
        self,
        project_id: str,
        name: str,
        execution_id: str
    ) -> Dict[str, Any]:
        """Get pipeline execution details.
        
        Args:
            project_id: Project ID
            name: Pipeline name
            execution_id: Execution ID
            
        Returns:
            Execution details
        """
        if self.boto3_available:
            try:
                response = self.codepipeline_client.get_pipeline_execution(
                    pipelineName=f"{project_id}-{name}",
                    executionId=execution_id
                )
                return response.get("pipelineExecution", {})
            except ClientError as e:
                logger.error(f"Failed to get pipeline execution: {e}")
        
        return {
            "pipelineExecutionId": execution_id,
            "status": "Unknown"
        }

    async def stop_pipeline_execution(
        self,
        project_id: str,
        name: str,
        execution_id: str,
        abandon: bool = False
    ) -> bool:
        """Stop a pipeline execution.
        
        Args:
            project_id: Project ID
            name: Pipeline name
            execution_id: Execution ID
            abandon: Whether to abandon instead of stopping
            
        Returns:
            True if successful
        """
        if self.boto3_available:
            try:
                self.codepipeline_client.stop_pipeline_execution(
                    pipelineName=f"{project_id}-{name}",
                    pipelineExecutionId=execution_id,
                    abandon=abandon
                )
            except ClientError as e:
                logger.error(f"Failed to stop pipeline execution: {e}")
                raise
        
        self._invoke_callbacks("pipeline.stopped", {
            "project_id": project_id,
            "pipeline": name,
            "execution_id": execution_id
        })
        logger.info(f"Stopped pipeline execution: {execution_id}")
        return True

    # =========================================================================
    # 9. TOOLS
    # =========================================================================

    async def create_toolchain(
        self,
        project_id: str,
        name: str,
        template_url: str,
        service_role_arn: Optional[str] = None
    ) -> Toolchain:
        """Create a toolchain for a project.
        
        Args:
            project_id: Project ID
            name: Toolchain name
            template_url: CloudFormation template URL
            service_role_arn: Optional service role ARN
            
        Returns:
            Toolchain object
        """
        toolchain_arn = f"arn:aws:codestar:{self.region}:{self._get_account_id()}:toolchain/{project_id}/{name}"
        
        if self.boto3_available:
            try:
                if not service_role_arn:
                    service_role_arn = self._create_toolchain_role(project_id)
                
                response = self.cloudformation_client.create_stack(
                    StackName=f"{project_id}-toolchain-{name}",
                    TemplateURL=template_url,
                    RoleARN=service_role_arn,
                    Capabilities=["CAPABILITY_IAM"]
                )
                toolchain_arn = response.get("StackId", toolchain_arn)
            except ClientError as e:
                logger.error(f"Failed to create toolchain: {e}")
                raise
        
        toolchain = Toolchain(
            arn=toolchain_arn,
            project_id=project_id,
            name=name,
            status="CREATE_IN_PROGRESS",
            created_time=datetime.now(),
            updated_time=datetime.now(),
            template_url=template_url,
            service_role_arn=service_role_arn
        )
        
        with self._lock:
            self._toolchains[f"{project_id}/{name}"] = toolchain
        
        self._invoke_callbacks("toolchain.created", toolchain)
        logger.info(f"Created toolchain: {name} for project {project_id}")
        return toolchain

    async def get_toolchain(self, project_id: str, name: str) -> Optional[Toolchain]:
        """Get a toolchain.
        
        Args:
            project_id: Project ID
            name: Toolchain name
            
        Returns:
            Toolchain object or None
        """
        key = f"{project_id}/{name}"
        return self._toolchains.get(key)

    async def list_toolchains(self, project_id: str) -> List[Toolchain]:
        """List toolchains for a project.
        
        Args:
            project_id: Project ID
            
        Returns:
            List of Toolchain objects
        """
        return [
            t for k, t in self._toolchains.items()
            if k.startswith(f"{project_id}/")
        ]

    async def update_toolchain(
        self,
        project_id: str,
        name: str,
        template_url: Optional[str] = None
    ) -> Toolchain:
        """Update a toolchain.
        
        Args:
            project_id: Project ID
            name: Toolchain name
            template_url: Optional new template URL
            
        Returns:
            Updated Toolchain object
        """
        toolchain = await self.get_toolchain(project_id, name)
        if not toolchain:
            raise ValueError(f"Toolchain not found: {project_id}/{name}")
        
        if template_url:
            toolchain.template_url = template_url
        
        toolchain.updated_time = datetime.now()
        
        self._invoke_callbacks("toolchain.updated", toolchain)
        return toolchain

    async def delete_toolchain(self, project_id: str, name: str) -> bool:
        """Delete a toolchain.
        
        Args:
            project_id: Project ID
            name: Toolchain name
            
        Returns:
            True if successful
        """
        toolchain = await self.get_toolchain(project_id, name)
        if not toolchain:
            return False
        
        if self.boto3_available:
            try:
                self.cloudformation_client.delete_stack(StackName=f"{project_id}-toolchain-{name}")
            except ClientError as e:
                logger.error(f"Failed to delete toolchain: {e}")
                raise
        
        key = f"{project_id}/{name}"
        with self._lock:
            del self._toolchains[key]
        
        self._invoke_callbacks("toolchain.deleted", key)
        logger.info(f"Deleted toolchain: {project_id}/{name}")
        return True

    # =========================================================================
    # 10. CLOUDWATCH INTEGRATION
    # =========================================================================

    async def get_project_metrics(
        self,
        project_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> ProjectMetrics:
        """Get CloudWatch metrics for a project.
        
        Args:
            project_id: Project ID
            start_time: Optional start time
            end_time: Optional end time
            
        Returns:
            ProjectMetrics object
        """
        if project_id in self._metrics:
            return self._metrics[project_id]
        
        metrics = ProjectMetrics(project_id=project_id)
        
        if not start_time:
            start_time = datetime.now() - timedelta(days=7)
        if not end_time:
            end_time = datetime.now()
        
        if self.boto3_available:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/CodeStar",
                    MetricName="PipelineExecutionCount",
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=["Sum"]
                )
                for datapoint in response.get("Datapoints", []):
                    metrics.pipeline_execution_count += int(datapoint.get("Sum", 0))
            except ClientError as e:
                logger.error(f"Failed to get pipeline metrics: {e}")
            
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/CodeStar",
                    MetricName="DeploymentSuccess",
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=["Sum"]
                )
                for datapoint in response.get("Datapoints", []):
                    metrics.successful_deployments += int(datapoint.get("Sum", 0))
            except ClientError:
                pass
            
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/CodeStar",
                    MetricName="DeploymentFailure",
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=["Sum"]
                )
                for datapoint in response.get("Datapoints", []):
                    metrics.failed_deployments += int(datapoint.get("Sum", 0))
            except ClientError:
                pass
        
        with self._lock:
            self._metrics[project_id] = metrics
        
        return metrics

    async def put_metric_data(
        self,
        project_id: str,
        metric_name: str,
        value: float,
        unit: str = "Count"
    ) -> bool:
        """Put custom metric data to CloudWatch.
        
        Args:
            project_id: Project ID
            metric_name: Metric name
            value: Metric value
            unit: Metric unit
            
        Returns:
            True if successful
        """
        if not self.boto3_available:
            return False
        
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="AWS/CodeStar",
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Dimensions": [
                            {
                                "Name": "ProjectId",
                                "Value": project_id
                            }
                        ],
                        "Value": value,
                        "Unit": unit
                    }
                ]
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            return False

    async def create_dashboard(
        self,
        project_id: str,
        name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a CloudWatch dashboard for a project.
        
        Args:
            project_id: Project ID
            name: Optional dashboard name
            
        Returns:
            Dashboard information
        """
        dashboard_name = name or f"{project_id}-dashboard"
        dashboard_body = json.dumps({
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/CodeStar", "PipelineExecutionCount", {"id": "m1"}],
                            [".", "DeploymentSuccess", {"id": "m2"}],
                            [".", "DeploymentFailure", {"id": "m3"}]
                        ],
                        "period": 3600,
                        "stat": "Sum",
                        "region": self.region,
                        "title": f"CodeStar Project {project_id}"
                    }
                },
                {
                    "type": "log",
                    "properties": {
                        "query": f"fields @timestamp, @message | filter projectId = '{project_id}' | sort @timestamp desc | limit 20",
                        "region": self.region,
                        "title": f"Logs for {project_id}"
                    }
                }
            ]
        })
        
        if self.boto3_available:
            try:
                self.cloudwatch_client.put_dashboard(
                    DashboardName=dashboard_name,
                    DashboardBody=dashboard_body
                )
            except ClientError as e:
                logger.error(f"Failed to create dashboard: {e}")
                raise
        
        self._invoke_callbacks("dashboard.created", dashboard_name)
        logger.info(f"Created dashboard: {dashboard_name}")
        return {
            "name": dashboard_name,
            "project_id": project_id
        }

    async def create_alarm(
        self,
        project_id: str,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 2
    ) -> Dict[str, Any]:
        """Create a CloudWatch alarm for a project metric.
        
        Args:
            project_id: Project ID
            alarm_name: Alarm name
            metric_name: Metric name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods
            
        Returns:
            Alarm information
        """
        alarm_arn = f"arn:aws:cloudwatch:{self.region}:{self._get_account_id()}:alarm:{alarm_name}"
        
        if self.boto3_available:
            try:
                self.cloudwatch_client.put_alarm(
                    AlarmName=alarm_name,
                    AlarmDescription=f"CodeStar alarm for project {project_id}",
                    Namespace="AWS/CodeStar",
                    MetricName=metric_name,
                    Threshold=threshold,
                    ComparisonOperator=comparison_operator,
                    Period=period,
                    EvaluationPeriods=evaluation_periods,
                    Statistic="Sum",
                    Dimensions=[
                        {
                            "Name": "ProjectId",
                            "Value": project_id
                        }
                    ]
                )
            except ClientError as e:
                logger.error(f"Failed to create alarm: {e}")
                raise
        
        self._invoke_callbacks("alarm.created", {
            "alarm_name": alarm_name,
            "project_id": project_id
        })
        logger.info(f"Created alarm: {alarm_name}")
        return {
            "alarm_name": alarm_name,
            "alarm_arn": alarm_arn,
            "project_id": project_id
        }

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        if self.boto3_available:
            try:
                return boto3.client("sts").get_caller_identity()["Account"]
            except ClientError:
                return "000000000000"
        return "000000000000"

    def _get_pipeline_role_arn(self, project_id: str) -> str:
        """Get or create pipeline role ARN."""
        role_name = f"CodeStar-{project_id}-Pipeline"
        if self.boto3_available:
            try:
                return self.iam_client.get_role(RoleName=role_name)["Role"]["Arn"]
            except ClientError:
                pass
        return f"arn:aws:iam::{self._get_account_id()}:role/{role_name}"

    def _create_toolchain_role(self, project_id: str) -> str:
        """Create toolchain service role."""
        role_name = f"CodeStar-{project_id}-Toolchain"
        
        if not self.boto3_available:
            return f"arn:aws:iam::{self._get_account_id()}:role/{role_name}"
        
        try:
            self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {"Service": "cloudformation.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }]
                })
            )
        except ClientError:
            pass
        
        return f"arn:aws:iam::{self._get_account_id()}:role/{role_name}"

    async def export_project_config(self, project_id: str) -> Dict[str, Any]:
        """Export project configuration.
        
        Args:
            project_id: Project ID
            
        Returns:
            Project configuration dictionary
        """
        project = await self.get_project(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")
        
        team_members = await self.list_team_members(project_id)
        resources = await self.list_project_resources(project_id)
        pipelines = await self.list_pipelines(project_id)
        toolchains = await self.list_toolchains(project_id)
        
        return {
            "project": {
                "id": project.id,
                "name": project.name,
                "description": project.description,
                "region": project.region,
                "template_id": project.template_id,
                "tags": project.tags
            },
            "team_members": [
                {
                    "user_arn": m.user_arn,
                    "role": m.role.value,
                    "display_name": m.display_name
                }
                for m in team_members
            ],
            "resources": [
                {"id": r.id, "type": r.type, "name": r.name}
                for r in resources
            ],
            "pipelines": [
                {"name": p.name, "arn": p.arn}
                for p in pipelines
            ],
            "toolchains": [
                {"name": t.name, "arn": t.arn}
                for t in toolchains
            ]
        }

    async def import_project_config(self, config: Dict[str, Any]) -> Project:
        """Import project configuration.
        
        Args:
            config: Project configuration dictionary
            
        Returns:
            Imported Project object
        """
        project_config = config.get("project", {})
        
        project = await self.create_project(
            name=project_config.get("name", ""),
            description=project_config.get("description", ""),
            template_id=project_config.get("template_id", ""),
            tags=project_config.get("tags", {})
        )
        
        for member_config in config.get("team_members", []):
            await self.add_team_member(
                project_id=project.id,
                user_arn=member_config["user_arn"],
                role=TeamMemberRole(member_config["role"]),
                display_name=member_config.get("display_name")
            )
        
        self._invoke_callbacks("project.config_imported", project)
        logger.info(f"Imported project configuration for: {project.id}")
        return project


# Add asyncio import for callback handling
import asyncio
