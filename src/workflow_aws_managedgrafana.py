"""
Amazon Managed Grafana Integration Module for Workflow System

Implements a ManagedGrafanaIntegration class with:
1. Workspace management: Create/manage Grafana workspaces
2. Users: Manage workspace users and permissions
3. Teams: Manage Grafana teams
4. Data sources: Configure data sources
5. Dashboards: Provision dashboards
6. Alerts: Manage alerting settings
7. API keys: Manage Grafana API keys
8. SSO: Single Sign-On configuration
9. Notifications: Notification channels
10. CloudWatch integration: Workspace and user metrics

Commit: 'feat(aws-managedgrafana): add Amazon Managed Grafana with workspace management, users, teams, data sources, dashboards, alerts, API keys, SSO, notifications, CloudWatch'
"""

import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import threading
import uuid

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


class WorkspaceStatus(Enum):
    """Managed Grafana workspace status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    CREATION_FAILED = "CREATION_FAILED"


class PermissionRole(Enum):
    """Grafana workspace permission roles."""
    ADMIN = "ADMIN"
    EDITOR = "EDITOR"
    VIEWER = "VIEWER"


class DataSourceType(Enum):
    """Data source types for Grafana."""
    PROMETHEUS = "prometheus"
    ELASTICSEARCH = "elasticsearch"
    INFLUXDB = "influxdb"
    GRAPHITE = "graphite"
    DATADOG = "datadog"
    CLOUDWATCH = "cloudwatch"
    INFLUXDB_V2 = "influxdb_v2"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    LOKI = "loki"
    TEMPO = "tempo"
    JAEGER = "jaeger"
    ZIPKIN = "zipkin"
    OPENTSDB = "opentsdb"
    PARCA = "parca"


class AlertState(Enum):
    """Alert states."""
    OK = "ok"
    ALERTING = "alerting"
    NO_DATA = "no_data"
    PENDING = "pending"
    PAUSED = "paused"


class NotificationChannelType(Enum):
    """Notification channel types."""
    EMAIL = "email"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    OPSGENIE = "opsgenie"
    DISCORD = "discord"
    WEBHOOK = "webhook"
    TEAMS = "teams"
    Telegram = "telegram"
    SNS = "sns"


class SSOProvider(Enum):
    """SSO provider types."""
    SAML = "SAML"
    OAUTH = "OAUTH"
    AWS_SSO = "AWS SSO"
    OKTA = "OKTA"
    AZURE_AD = "AZURE_AD"
    GOOGLE = "GOOGLE"


class UserPermissionType(Enum):
    """User permission types in workspace."""
    ADMIN = "ADMIN"
    EDITOR = "EDITOR"
    VIEWER = "VIEWER"


@dataclass
class WorkspaceConfig:
    """Managed Grafana workspace configuration."""
    name: str = ""
    description: str = ""
    account_access_type: str = "CURRENT_ACCOUNT"
    authentication_providers: List[str] = field(default_factory=list)
    kms_key_arn: str = ""
    notification_destinations: List[str] = field(default_factory=list)
    vpc_configuration: Dict[str, Any] = field(default_factory=dict)
    iam_role_arn: str = ""
    permission_type: str = "EDITOR"
    tag_map: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkspaceInfo:
    """Managed Grafana workspace information."""
    workspace_id: str
    arn: str
    status: WorkspaceStatus = WorkspaceStatus.ACTIVE
    name: str = ""
    description: str = ""
    endpoint: str = ""
    created_at: str = ""
    updated_at: str = ""
    authentication_providers: List[str] = field(default_factory=list)
    notification_destinations: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class UserInfo:
    """Workspace user information."""
    user_id: str
    email: str
    role: UserPermissionType = UserPermissionType.VIEWER
    status: str = "ACTIVE"
    groups: List[str] = field(default_factory=list)
    last_seen: str = ""


@dataclass
class TeamInfo:
    """Grafana team information."""
    team_id: int
    name: str
    email: str = ""
    members_count: int = 0
    permission: str = "Member"
    created: str = ""
    updated: str = ""


@dataclass
class DataSource:
    """Data source configuration."""
    name: str
    type: DataSourceType
    url: str = ""
    access: str = "proxy"
    database: str = ""
    user: str = ""
    password: str = ""
    secure_json_data: Dict[str, str] = field(default_factory=dict)
    json_data: Dict[str, Any] = field(default_factory=dict)
    is_default: bool = False
    uid: str = ""


@dataclass
class Dashboard:
    """Dashboard configuration."""
    title: str
    uid: str = ""
    folder_id: int = 0
    folder_title: str = "General"
    overwrite: bool = True
    message: str = ""
    dashboard_json: Dict[str, Any] = field(default_factory=dict)
    panels: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AlertRule:
    """Alert rule configuration."""
    name: str
    condition: str = "A"
    data: List[Dict[str, Any]] = field(default_factory=list)
    folder_title: str = "Default"
    interval: str = "1m"
    no_data_state: AlertState = AlertState.NO_DATA
    exec_err_state: AlertState = AlertState.ALERTING
    tags: Dict[str, str] = field(default_factory=dict)
    rule_group: str = "default"


@dataclass
class APIKey:
    """Grafana API key."""
    name: str
    role: str = "Viewer"
    expires_in: int = 0
    key_id: int = 0
    key: str = ""
    created_at: str = ""


@dataclass
class NotificationChannel:
    """Notification channel configuration."""
    name: str
    type: NotificationChannelType
    email_settings: Dict[str, Any] = field(default_factory=dict)
    slack_settings: Dict[str, Any] = field(default_factory=dict)
    webhook_settings: Dict[str, Any] = field(default_factory=dict)
    pagerduty_settings: Dict[str, Any] = field(default_factory=dict)
    opsgenie_settings: Dict[str, Any] = field(default_factory=dict)
    discord_settings: Dict[str, Any] = field(default_factory=dict)
    teams_settings: Dict[str, Any] = field(default_factory=dict)
    sns_settings: Dict[str, Any] = field(default_factory=dict)
    is_default: bool = False
    uid: str = ""
    secure_fields: List[str] = field(default_factory=list)


@dataclass
class SSOConfig:
    """SSO configuration."""
    provider: SSOProvider
    enabled: bool = True
    allowed_groups: List[str] = field(default_factory=list)
    allowed_organizations: List[str] = field(default_factory=list)
    oidc_settings: Dict[str, Any] = field(default_factory=dict)
    saml_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CloudWatchMetrics:
    """CloudWatch metrics for Managed Grafana."""
    workspace_id: str
    metric_name: str
    timestamp: str = ""
    value: float = 0.0
    unit: str = "Count"
    dimensions: Dict[str, str] = field(default_factory=dict)


class ManagedGrafanaIntegration:
    """
    Amazon Managed Grafana integration for workflow automation.
    
    Provides comprehensive management of Grafana workspaces including:
    - Workspace lifecycle management
    - User and team administration
    - Data source configuration
    - Dashboard provisioning
    - Alert management
    - API key management
    - SSO configuration
    - Notification channels
    - CloudWatch metrics integration
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        workspace_id: Optional[str] = None,
        boto_client: Optional[Any] = None,
        grafana_api_key: Optional[str] = None,
        grafana_endpoint: Optional[str] = None
    ):
        """
        Initialize Managed Grafana integration.
        
        Args:
            region: AWS region for the service
            workspace_id: Specific workspace ID to manage
            boto_client: Pre-configured boto3 client
            grafana_api_key: Grafana API key for direct API access
            grafana_endpoint: Grafana API endpoint URL
        """
        self.region = region
        self.workspace_id = workspace_id
        self.grafana_api_key = grafana_api_key
        self.grafana_endpoint = grafana_endpoint
        self._boto_client = boto_client
        self._lock = threading.Lock()
        self._workspace_cache: Dict[str, WorkspaceInfo] = {}
        self._user_cache: Dict[str, UserInfo] = {}
        self._team_cache: Dict[int, TeamInfo] = {}
        
    @property
    def grafana_client(self):
        """Get or create boto3 Grafana client."""
        if self._boto_client is None:
            if not BOTO3_AVAILABLE:
                raise ImportError("boto3 is required for AWS integration")
            self._boto_client = boto3.client("grafana", region_name=self.region)
        return self._boto_client
    
    def _generate_workspace_id(self) -> str:
        """Generate a unique workspace identifier."""
        return f"wg-{uuid.uuid4().hex[:8]}"
    
    def _make_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make HTTP request to Grafana API."""
        import requests
        
        default_headers = {"Content-Type": "application/json"}
        if self.grafana_api_key:
            default_headers["Authorization"] = f"Bearer {self.grafana_api_key}"
        
        if headers:
            default_headers.update(headers)
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=default_headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Grafana API request failed: {e}")
            raise
    
    # =========================================================================
    # WORKSPACE MANAGEMENT
    # =========================================================================
    
    def create_workspace(
        self,
        workspace_name: str,
        workspace_type: str = "WORKSPACE_STANDARD",
        account_access_type: str = "CURRENT_ACCOUNT",
        authentication_providers: Optional[List[str]] = None,
        permission_type: str = "EDITOR",
        kms_key_arn: str = "",
        notification_destinations: Optional[List[str]] = None,
        vpc_configuration: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> WorkspaceInfo:
        """
        Create a new Managed Grafana workspace.
        
        Args:
            workspace_name: Name for the workspace
            workspace_type: Type of workspace (WORKSPACE_STANDARD, WORKSPACE_AUTHORIZATION)
            account_access_type: Account access type
            authentication_providers: List of auth providers (e.g., ['AWS_SSO', 'SAML'])
            permission_type: Default permission type
            kms_key_arn: KMS key ARN for encryption
            notification_destinations: Notification destination types
            vpc_configuration: VPC configuration for private workspaces
            tags: Resource tags
            
        Returns:
            WorkspaceInfo object with workspace details
        """
        with self._lock:
            try:
                params = {
                    "name": workspace_name,
                    "accountAccessType": account_access_type,
                    "permissionType": permission_type,
                    "workspaceType": workspace_type,
                }
                
                if authentication_providers:
                    params["authentication_providers"] = authentication_providers
                if kms_key_arn:
                    params["kmsKeyArn"] = kms_key_arn
                if notification_destinations:
                    params["notificationDestinations"] = notification_destinations
                if vpc_configuration:
                    params["vpcConfiguration"] = vpc_configuration
                if tags:
                    params["tags"] = tags
                
                response = self.grafana_client.create_workspace(**params)
                
                workspace_id = response.get("workspace", {}).get("id", "")
                workspace_info = self._parse_workspace_response(response.get("workspace", {}))
                
                if workspace_id:
                    self.workspace_id = workspace_id
                    self._workspace_cache[workspace_id] = workspace_info
                
                logger.info(f"Created Managed Grafana workspace: {workspace_id}")
                return workspace_info
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create workspace: {e}")
                raise
    
    def get_workspace(self, workspace_id: Optional[str] = None) -> WorkspaceInfo:
        """
        Get workspace information.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            WorkspaceInfo object
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        if workspace_id in self._workspace_cache:
            return self._workspace_cache[workspace_id]
        
        try:
            response = self.grafana_client.describe_workspace(workspaceId=workspace_id)
            workspace_info = self._parse_workspace_response(response.get("workspace", {}))
            self._workspace_cache[workspace_id] = workspace_info
            return workspace_info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get workspace: {e}")
            raise
    
    def list_workspaces(
        self,
        max_results: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List all Managed Grafana workspaces.
        
        Args:
            max_results: Maximum number of results
            next_token: Pagination token
            
        Returns:
            Dict with workspaces and pagination info
        """
        try:
            params = {"maxResults": max_results}
            if next_token:
                params["nextToken"] = next_token
            
            response = self.grafana_client.list_workspaces(**params)
            
            workspaces = [
                self._parse_workspace_response(ws) 
                for ws in response.get("workspaces", [])
            ]
            
            return {
                "workspaces": workspaces,
                "next_token": response.get("nextToken"),
                "total": len(workspaces)
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list workspaces: {e}")
            raise
    
    def update_workspace(
        self,
        workspace_id: Optional[str] = None,
        description: str = "",
        notification_destinations: Optional[List[str]] = None,
        authentication_providers: Optional[List[str]] = None,
        permission_type: str = ""
    ) -> WorkspaceInfo:
        """
        Update workspace configuration.
        
        Args:
            workspace_id: Workspace ID
            description: New description
            notification_destinations: Updated notification destinations
            authentication_providers: Updated auth providers
            permission_type: Updated permission type
            
        Returns:
            Updated WorkspaceInfo
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        with self._lock:
            try:
                params = {"workspaceId": workspace_id}
                
                if description:
                    params["description"] = description
                if notification_destinations:
                    params["notificationDestinations"] = notification_destinations
                if authentication_providers:
                    params["authentication_providers"] = authentication_providers
                if permission_type:
                    params["permissionType"] = permission_type
                
                response = self.grafana_client.update_workspace(**params)
                workspace_info = self._parse_workspace_response(response.get("workspace", {}))
                
                if workspace_id in self._workspace_cache:
                    self._workspace_cache[workspace_id] = workspace_info
                
                return workspace_info
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to update workspace: {e}")
                raise
    
    def delete_workspace(self, workspace_id: Optional[str] = None) -> bool:
        """
        Delete a Managed Grafana workspace.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        with self._lock:
            try:
                self.grafana_client.delete_workspace(workspaceId=workspace_id)
                
                if workspace_id in self._workspace_cache:
                    del self._workspace_cache[workspace_id]
                
                if self.workspace_id == workspace_id:
                    self.workspace_id = None
                
                logger.info(f"Deleted workspace: {workspace_id}")
                return True
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to delete workspace: {e}")
                raise
    
    def update_workspace_permissions(
        self,
        workspace_id: Optional[str] = None,
        permission_entries: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """
        Update workspace permissions.
        
        Args:
            workspace_id: Workspace ID
            permission_entries: List of permission entries with 'user' and 'permission'
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            params = {
                "workspaceId": workspace_id,
                "permissionEntries": permission_entries or []
            }
            
            self.grafana_client.update_workspace_permissions(**params)
            logger.info(f"Updated permissions for workspace: {workspace_id}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update permissions: {e}")
            raise
    
    def _parse_workspace_response(self, workspace: Dict[str, Any]) -> WorkspaceInfo:
        """Parse workspace API response into WorkspaceInfo."""
        return WorkspaceInfo(
            workspace_id=workspace.get("id", ""),
            arn=workspace.get("arn", ""),
            status=WorkspaceStatus(workspace.get("status", "ACTIVE")),
            name=workspace.get("name", ""),
            description=workspace.get("description", ""),
            endpoint=workspace.get("endpoint", ""),
            created_at=workspace.get("createdAt", ""),
            updated_at=workspace.get("updatedAt", ""),
            authentication_providers=workspace.get("authentication_providers", []),
            notification_destinations=workspace.get("notificationDestinations", []),
            tags=workspace.get("tags", {})
        )
    
    # =========================================================================
    # USER MANAGEMENT
    # =========================================================================
    
    def list_users(
        self,
        workspace_id: Optional[str] = None,
        max_results: int = 100
    ) -> List[UserInfo]:
        """
        List users in the workspace.
        
        Args:
            workspace_id: Workspace ID
            max_results: Maximum number of results
            
        Returns:
            List of UserInfo objects
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            response = self.grafana_client.list_workspaces(
                workspaceIds=[workspace_id]
            )
            
            users = []
            for ws in response.get("workspaces", []):
                for user in ws.get("users", []):
                    user_info = UserInfo(
                        user_id=user.get("id", ""),
                        email=user.get("email", ""),
                        role=UserPermissionType(user.get("role", "VIEWER")),
                        status=user.get("status", "ACTIVE"),
                        groups=user.get("groups", [])
                    )
                    users.append(user_info)
                    self._user_cache[user_info.user_id] = user_info
            
            return users
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list users: {e}")
            raise
    
    def invite_user(
        self,
        email: str,
        role: UserPermissionType = UserPermissionType.VIEWER,
        workspace_id: Optional[str] = None
    ) -> UserInfo:
        """
        Invite a user to the workspace.
        
        Args:
            email: User email address
            role: User permission role
            workspace_id: Workspace ID
            
        Returns:
            UserInfo for the invited user
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            response = self.grafana_client.create_workspace_api_key(
                workspaceId=workspace_id,
                keyName=f"user-invite-{email.split('@')[0]}",
                keyRole="WORKSPACE_ADMIN",
                secondsToLive=3600
            )
            
            user_info = UserInfo(
                user_id=str(uuid.uuid4()),
                email=email,
                role=role,
                status="PENDING"
            )
            
            self._user_cache[user_info.user_id] = user_info
            logger.info(f"Invited user {email} to workspace {workspace_id}")
            return user_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to invite user: {e}")
            raise
    
    def update_user_role(
        self,
        user_id: str,
        role: UserPermissionType,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Update user permission role.
        
        Args:
            user_id: User ID
            role: New role
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            self.grafana_client.update_workspace_permissions(
                workspaceId=workspace_id,
                permissionEntries=[{
                    "userId": user_id,
                    "permission": role.value
                }]
            )
            
            if user_id in self._user_cache:
                self._user_cache[user_id].role = role
            
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update user role: {e}")
            raise
    
    def remove_user(
        self,
        user_id: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Remove a user from the workspace.
        
        Args:
            user_id: User ID
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            self.grafana_client.update_workspace_permissions(
                workspaceId=workspace_id,
                permissionEntries=[{
                    "userId": user_id,
                    "permission": "NONE"
                }]
            )
            
            if user_id in self._user_cache:
                del self._user_cache[user_id]
            
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to remove user: {e}")
            raise
    
    # =========================================================================
    # TEAM MANAGEMENT
    # =========================================================================
    
    def list_teams(
        self,
        workspace_id: Optional[str] = None,
        max_results: int = 100
    ) -> List[TeamInfo]:
        """
        List teams in the workspace.
        
        Args:
            workspace_id: Workspace ID
            max_results: Maximum number of results
            
        Returns:
            List of TeamInfo objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required for team operations")
        
        try:
            url = f"{self.grafana_endpoint}/api/teams"
            response = self._make_request("GET", url)
            
            teams = []
            for team in response:
                team_info = TeamInfo(
                    team_id=team.get("id", 0),
                    name=team.get("name", ""),
                    email=team.get("email", ""),
                    members_count=team.get("membersCount", 0),
                    permission=team.get("permission", "Member"),
                    created=team.get("created", ""),
                    updated=team.get("updated", "")
                )
                teams.append(team_info)
                self._team_cache[team_info.team_id] = team_info
            
            return teams
        except Exception as e:
            logger.error(f"Failed to list teams: {e}")
            raise
    
    def create_team(
        self,
        name: str,
        email: str = "",
        workspace_id: Optional[str] = None
    ) -> TeamInfo:
        """
        Create a new team.
        
        Args:
            name: Team name
            email: Team email
            workspace_id: Workspace ID
            
        Returns:
            TeamInfo for the created team
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required for team operations")
        
        try:
            url = f"{self.grafana_endpoint}/api/teams"
            data = {"name": name, "email": email}
            
            response = self._make_request("POST", url, data=data)
            
            team_info = TeamInfo(
                team_id=response.get("teamId", 0),
                name=name,
                email=email
            )
            
            self._team_cache[team_info.team_id] = team_info
            logger.info(f"Created team: {name}")
            return team_info
        except Exception as e:
            logger.error(f"Failed to create team: {e}")
            raise
    
    def add_team_member(
        self,
        team_id: int,
        user_id: int,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Add a member to a team.
        
        Args:
            team_id: Team ID
            user_id: User ID
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/teams/{team_id}/members"
            data = {"userId": user_id}
            
            self._make_request("POST", url, data=data)
            logger.info(f"Added user {user_id} to team {team_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add team member: {e}")
            raise
    
    def remove_team_member(
        self,
        team_id: int,
        user_id: int
    ) -> bool:
        """
        Remove a member from a team.
        
        Args:
            team_id: Team ID
            user_id: User ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/teams/{team_id}/members/{user_id}"
            self._make_request("DELETE", url)
            logger.info(f"Removed user {user_id} from team {team_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to remove team member: {e}")
            raise
    
    def delete_team(self, team_id: int) -> bool:
        """
        Delete a team.
        
        Args:
            team_id: Team ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/teams/{team_id}"
            self._make_request("DELETE", url)
            
            if team_id in self._team_cache:
                del self._team_cache[team_id]
            
            return True
        except Exception as e:
            logger.error(f"Failed to delete team: {e}")
            raise
    
    # =========================================================================
    # DATA SOURCES
    # =========================================================================
    
    def list_data_sources(
        self,
        workspace_id: Optional[str] = None
    ) -> List[DataSource]:
        """
        List configured data sources.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            List of DataSource objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required for data source operations")
        
        try:
            url = f"{self.grafana_endpoint}/api/datasources"
            response = self._make_request("GET", url)
            
            data_sources = []
            for ds in response:
                ds_type = ds.get("type", "")
                try:
                    ds_type_enum = DataSourceType(ds_type)
                except ValueError:
                    ds_type_enum = DataSourceType.PROMETHEUS
                
                data_source = DataSource(
                    name=ds.get("name", ""),
                    type=ds_type_enum,
                    url=ds.get("url", ""),
                    access=ds.get("access", "proxy"),
                    database=ds.get("database", ""),
                    user=ds.get("user", ""),
                    is_default=ds.get("isDefault", False),
                    uid=ds.get("uid", "")
                )
                data_sources.append(data_source)
            
            return data_sources
        except Exception as e:
            logger.error(f"Failed to list data sources: {e}")
            raise
    
    def create_data_source(
        self,
        data_source: DataSource,
        workspace_id: Optional[str] = None
    ) -> DataSource:
        """
        Create a new data source.
        
        Args:
            data_source: DataSource configuration
            workspace_id: Workspace ID
            
        Returns:
            Created DataSource
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/datasources"
            
            payload = {
                "name": data_source.name,
                "type": data_source.type.value,
                "url": data_source.url,
                "access": data_source.access,
                "database": data_source.database,
                "user": data_source.user,
                "secureJsonData": data_source.secure_json_data,
                "jsonData": data_source.json_data,
                "isDefault": data_source.is_default
            }
            
            if data_source.uid:
                payload["uid"] = data_source.uid
            
            response = self._make_request("POST", url, data=payload)
            
            data_source.uid = response.get("uid", data_source.uid)
            logger.info(f"Created data source: {data_source.name}")
            return data_source
        except Exception as e:
            logger.error(f"Failed to create data source: {e}")
            raise
    
    def update_data_source(
        self,
        data_source_id: int,
        data_source: DataSource
    ) -> DataSource:
        """
        Update an existing data source.
        
        Args:
            data_source_id: Data source ID
            data_source: Updated DataSource configuration
            
        Returns:
            Updated DataSource
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/datasources/{data_source_id}"
            
            payload = {
                "name": data_source.name,
                "type": data_source.type.value,
                "url": data_source.url,
                "access": data_source.access,
                "database": data_source.database,
                "user": data_source.user,
                "secureJsonData": data_source.secure_json_data,
                "jsonData": data_source.json_data,
                "isDefault": data_source.is_default
            }
            
            self._make_request("PUT", url, data=payload)
            logger.info(f"Updated data source: {data_source.name}")
            return data_source
        except Exception as e:
            logger.error(f"Failed to update data source: {e}")
            raise
    
    def delete_data_source(self, data_source_id: int) -> bool:
        """
        Delete a data source.
        
        Args:
            data_source_id: Data source ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/datasources/{data_source_id}"
            self._make_request("DELETE", url)
            logger.info(f"Deleted data source: {data_source_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete data source: {e}")
            raise
    
    def test_data_source(self, data_source_id: int) -> Dict[str, Any]:
        """
        Test a data source connection.
        
        Args:
            data_source_id: Data source ID
            
        Returns:
            Test result with status
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/datasources/{data_source_id}/health"
            response = self._make_request("GET", url)
            return response
        except Exception as e:
            logger.error(f"Failed to test data source: {e}")
            raise
    
    # =========================================================================
    # DASHBOARDS
    # =========================================================================
    
    def list_dashboards(
        self,
        folder_id: Optional[int] = None,
        workspace_id: Optional[str] = None
    ) -> List[Dashboard]:
        """
        List dashboards.
        
        Args:
            folder_id: Filter by folder ID
            workspace_id: Workspace ID
            
        Returns:
            List of Dashboard objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/search"
            params = ""
            if folder_id:
                params = f"folderId={folder_id}"
            
            url = f"{url}?{params}" if params else url
            response = self._make_request("GET", url)
            
            dashboards = []
            for ds in response:
                dashboard = Dashboard(
                    title=ds.get("title", ""),
                    uid=ds.get("uid", ""),
                    folder_id=ds.get("folderId", 0),
                    folder_title=ds.get("folderTitle", "General")
                )
                dashboards.append(dashboard)
            
            return dashboards
        except Exception as e:
            logger.error(f"Failed to list dashboards: {e}")
            raise
    
    def create_dashboard(
        self,
        dashboard: Dashboard,
        workspace_id: Optional[str] = None
    ) -> Dashboard:
        """
        Create or update a dashboard.
        
        Args:
            dashboard: Dashboard configuration
            workspace_id: Workspace ID
            
        Returns:
            Created Dashboard with UID
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/dashboards/db"
            
            payload = {
                "dashboard": dashboard.dashboard_json or {
                    "title": dashboard.title,
                    "panels": dashboard.panels,
                    "uid": dashboard.uid
                },
                "overwrite": dashboard.overwrite,
                "message": dashboard.message,
                "folderId": dashboard.folder_id,
                "folderTitle": dashboard.folder_title
            }
            
            response = self._make_request("POST", url, data=payload)
            
            dashboard.uid = response.get("uid", dashboard.uid)
            logger.info(f"Created/updated dashboard: {dashboard.title}")
            return dashboard
        except Exception as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def get_dashboard(
        self,
        dashboard_uid: str,
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get dashboard by UID.
        
        Args:
            dashboard_uid: Dashboard UID
            workspace_id: Workspace ID
            
        Returns:
            Dashboard data
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/dashboards/uid/{dashboard_uid}"
            return self._make_request("GET", url)
        except Exception as e:
            logger.error(f"Failed to get dashboard: {e}")
            raise
    
    def delete_dashboard(
        self,
        dashboard_uid: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete a dashboard.
        
        Args:
            dashboard_uid: Dashboard UID
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/dashboards/uid/{dashboard_uid}"
            self._make_request("DELETE", url)
            logger.info(f"Deleted dashboard: {dashboard_uid}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete dashboard: {e}")
            raise
    
    def provision_dashboard(
        self,
        dashboard_json: Dict[str, Any],
        folder_title: str = "Default",
        overwrite: bool = True,
        workspace_id: Optional[str] = None
    ) -> Dashboard:
        """
        Provision a dashboard from JSON.
        
        Args:
            dashboard_json: Dashboard JSON definition
            folder_title: Target folder title
            overwrite: Whether to overwrite existing
            workspace_id: Workspace ID
            
        Returns:
            Provisioned Dashboard
        """
        dashboard = Dashboard(
            title=dashboard_json.get("title", "Untitled"),
            uid=dashboard_json.get("uid", ""),
            folder_title=folder_title,
            overwrite=overwrite,
            dashboard_json=dashboard_json,
            panels=dashboard_json.get("panels", [])
        )
        return self.create_dashboard(dashboard, workspace_id)
    
    # =========================================================================
    # ALERTS
    # =========================================================================
    
    def list_alert_rules(
        self,
        folder_title: Optional[str] = None,
        rule_group: Optional[str] = None
    ) -> List[AlertRule]:
        """
        List alert rules.
        
        Args:
            folder_title: Filter by folder title
            rule_group: Filter by rule group
            
        Returns:
            List of AlertRule objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/ruler/grafana/api/v1/rules"
            response = self._make_request("GET", url)
            
            rules = []
            for group in response.get("groups", []):
                for rule in group.get("rules", []):
                    alert_rule = AlertRule(
                        name=rule.get("name", ""),
                        condition=rule.get("condition", "A"),
                        data=rule.get("data", []),
                        folder_title=rule.get("folderTitle", "Default"),
                        interval=rule.get("interval", "1m"),
                        no_data_state=AlertState(rule.get("noDataState", "NoData")),
                        exec_err_state=AlertState(rule.get("execErrState", "Alerting")),
                        tags=rule.get("labels", {}),
                        rule_group=group.get("name", "default")
                    )
                    rules.append(alert_rule)
            
            if folder_title:
                rules = [r for r in rules if r.folder_title == folder_title]
            if rule_group:
                rules = [r for r in rules if r.rule_group == rule_group]
            
            return rules
        except Exception as e:
            logger.error(f"Failed to list alert rules: {e}")
            raise
    
    def create_alert_rule(
        self,
        alert_rule: AlertRule,
        workspace_id: Optional[str] = None
    ) -> AlertRule:
        """
        Create an alert rule.
        
        Args:
            alert_rule: AlertRule configuration
            workspace_id: Workspace ID
            
        Returns:
            Created AlertRule
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/ruler/grafana/api/v1/rules/{alert_rule.folder_title}"
            
            payload = {
                "name": alert_rule.name,
                "condition": alert_rule.condition,
                "data": alert_rule.data,
                "interval": alert_rule.interval,
                "noDataState": alert_rule.no_data_state.value,
                "execErrState": alert_rule.exec_err_state.value,
                "labels": alert_rule.tags,
                "folderTitle": alert_rule.folder_title
            }
            
            rule_group_payload = {
                alert_rule.rule_group: [payload]
            }
            
            self._make_request("POST", url, data=rule_group_payload)
            logger.info(f"Created alert rule: {alert_rule.name}")
            return alert_rule
        except Exception as e:
            logger.error(f"Failed to create alert rule: {e}")
            raise
    
    def update_alert_rule(
        self,
        alert_rule: AlertRule,
        workspace_id: Optional[str] = None
    ) -> AlertRule:
        """
        Update an alert rule.
        
        Args:
            alert_rule: Updated AlertRule configuration
            workspace_id: Workspace ID
            
        Returns:
            Updated AlertRule
        """
        return self.create_alert_rule(alert_rule, workspace_id)
    
    def delete_alert_rule(
        self,
        rule_name: str,
        folder_title: str = "Default",
        rule_group: str = "default"
    ) -> bool:
        """
        Delete an alert rule.
        
        Args:
            rule_name: Rule name
            folder_title: Folder title
            rule_group: Rule group
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/ruler/grafana/api/v1/rules/{folder_title}/groups/{rule_group}/rules/{rule_name}"
            self._make_request("DELETE", url)
            logger.info(f"Deleted alert rule: {rule_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete alert rule: {e}")
            raise
    
    def get_alert_state(
        self,
        alert_rule_uid: str
    ) -> Dict[str, Any]:
        """
        Get current alert state.
        
        Args:
            alert_rule_uid: Alert rule UID
            
        Returns:
            Alert state information
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/alertmanager/grafana/api/v2/alerts/{alert_rule_uid}"
            return self._make_request("GET", url)
        except Exception as e:
            logger.error(f"Failed to get alert state: {e}")
            raise
    
    # =========================================================================
    # API KEYS
    # =========================================================================
    
    def list_api_keys(self) -> List[APIKey]:
        """
        List Grafana API keys.
        
        Returns:
            List of APIKey objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/auth/keys"
            response = self._make_request("GET", url)
            
            keys = []
            for key in response:
                api_key = APIKey(
                    name=key.get("name", ""),
                    role=key.get("role", "Viewer"),
                    key_id=key.get("id", 0),
                    created_at=key.get("createdAt", "")
                )
                keys.append(api_key)
            
            return keys
        except Exception as e:
            logger.error(f"Failed to list API keys: {e}")
            raise
    
    def create_api_key(
        self,
        key_name: str,
        role: str = "Viewer",
        seconds_to_live: int = 0
    ) -> APIKey:
        """
        Create a new API key.
        
        Args:
            key_name: Name for the key
            role: Key role (Viewer, Editor, Admin)
            seconds_to_live: TTL in seconds (0 for no expiry)
            
        Returns:
            APIKey with the generated key
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/auth/keys"
            
            payload = {
                "name": key_name,
                "role": role
            }
            if seconds_to_live > 0:
                payload["secondsToLive"] = seconds_to_live
            
            response = self._make_request("POST", url, data=payload)
            
            api_key = APIKey(
                name=key_name,
                role=role,
                key_id=response.get("id", 0),
                key=response.get("key", ""),
                expires_in=seconds_to_live,
                created_at=datetime.utcnow().isoformat()
            )
            
            logger.info(f"Created API key: {key_name}")
            return api_key
        except Exception as e:
            logger.error(f"Failed to create API key: {e}")
            raise
    
    def delete_api_key(self, key_id: int) -> bool:
        """
        Delete an API key.
        
        Args:
            key_id: Key ID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/api/auth/keys/{key_id}"
            self._make_request("DELETE", url)
            logger.info(f"Deleted API key: {key_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete API key: {e}")
            raise
    
    # =========================================================================
    # SSO CONFIGURATION
    # =========================================================================
    
    def get_sso_config(
        self,
        workspace_id: Optional[str] = None
    ) -> SSOConfig:
        """
        Get SSO configuration.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            SSOConfig object
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            response = self.grafana_client.get_sso_config(workspaceId=workspace_id)
            sso_config = response.get("ssoConfig", {})
            
            return SSOConfig(
                provider=SSOProvider(sso_config.get("provider", "SAML")),
                enabled=sso_config.get("enabled", True),
                allowed_groups=sso_config.get("allowedGroups", []),
                allowed_organizations=sso_config.get("allowedOrganizations", []),
                saml_settings=sso_config.get("samlSettings", {}),
                oidc_settings=sso_config.get("oidcSettings", {})
            )
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get SSO config: {e}")
            raise
    
    def update_sso_config(
        self,
        sso_config: SSOConfig,
        workspace_id: Optional[str] = None
    ) -> SSOConfig:
        """
        Update SSO configuration.
        
        Args:
            sso_config: SSOConfig with updated settings
            workspace_id: Workspace ID
            
        Returns:
            Updated SSOConfig
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            params = {
                "workspaceId": workspace_id,
                "ssoConfig": {
                    "provider": sso_config.provider.value,
                    "enabled": sso_config.enabled,
                    "allowedGroups": sso_config.allowed_groups,
                    "allowedOrganizations": sso_config.allowed_organizations
                }
            }
            
            if sso_config.saml_settings:
                params["ssoConfig"]["samlSettings"] = sso_config.saml_settings
            if sso_config.oidc_settings:
                params["ssoConfig"]["oidcSettings"] = sso_config.oidc_settings
            
            self.grafana_client.update_sso_config(**params)
            logger.info(f"Updated SSO config for workspace: {workspace_id}")
            return sso_config
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update SSO config: {e}")
            raise
    
    def configure_saml_sso(
        self,
        idp_metadata_url: str = "",
        assertion_consumer_service_url: str = "",
        allowed_groups: Optional[List[str]] = None,
        allowed_organizations: Optional[List[str]] = None,
        workspace_id: Optional[str] = None
    ) -> SSOConfig:
        """
        Configure SAML SSO.
        
        Args:
            idp_metadata_url: Identity provider metadata URL
            assertion_consumer_service_url: ACS URL
            allowed_groups: Groups allowed to access
            allowed_organizations: Organizations allowed
            workspace_id: Workspace ID
            
        Returns:
            SSOConfig for SAML
        """
        saml_settings = {
            "idpMetadataUrl": idp_metadata_url,
            "assertionConsumerServiceUrl": assertion_consumer_service_url
        }
        
        sso_config = SSOConfig(
            provider=SSOProvider.SAML,
            enabled=True,
            allowed_groups=allowed_groups or [],
            allowed_organizations=allowed_organizations or [],
            saml_settings=saml_settings
        )
        
        return self.update_sso_config(sso_config, workspace_id)
    
    def configure_oidc_sso(
        self,
        client_id: str,
        client_secret: str,
        scopes: Optional[List[str]] = None,
        allowed_groups: Optional[List[str]] = None,
        workspace_id: Optional[str] = None
    ) -> SSOConfig:
        """
        Configure OIDC SSO.
        
        Args:
            client_id: OIDC client ID
            client_secret: OIDC client secret
            scopes: OAuth scopes
            allowed_groups: Groups allowed to access
            workspace_id: Workspace ID
            
        Returns:
            SSOConfig for OIDC
        """
        oidc_settings = {
            "clientId": client_id,
            "clientSecret": client_secret,
            "scopes": scopes or ["openid", "profile", "email"]
        }
        
        sso_config = SSOConfig(
            provider=SSOProvider.OAUTH,
            enabled=True,
            allowed_groups=allowed_groups or [],
            oidc_settings=oidc_settings
        )
        
        return self.update_sso_config(sso_config, workspace_id)
    
    # =========================================================================
    # NOTIFICATION CHANNELS
    # =========================================================================
    
    def list_notification_channels(self) -> List[NotificationChannel]:
        """
        List notification channels.
        
        Returns:
            List of NotificationChannel objects
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/apialert-notifications/channels"
            response = self._make_request("GET", url)
            
            channels = []
            for ch in response:
                channel_type = ch.get("type", "email")
                try:
                    channel_type_enum = NotificationChannelType(channel_type)
                except ValueError:
                    channel_type_enum = NotificationChannelType.EMAIL
                
                channel = NotificationChannel(
                    name=ch.get("name", ""),
                    type=channel_type_enum,
                    is_default=ch.get("isDefault", False),
                    uid=ch.get("uid", "")
                )
                channels.append(channel)
            
            return channels
        except Exception as e:
            logger.error(f"Failed to list notification channels: {e}")
            raise
    
    def create_notification_channel(
        self,
        channel: NotificationChannel
    ) -> NotificationChannel:
        """
        Create a notification channel.
        
        Args:
            channel: NotificationChannel configuration
            
        Returns:
            Created NotificationChannel
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/apialert-notifications/channels"
            
            payload = {
                "name": channel.name,
                "type": channel.type.value,
                "isDefault": channel.is_default,
                "settings": {}
            }
            
            if channel.email_settings:
                payload["settings"].update(channel.email_settings)
            if channel.slack_settings:
                payload["settings"].update(channel.slack_settings)
            if channel.webhook_settings:
                payload["settings"].update(channel.webhook_settings)
            if channel.pagerduty_settings:
                payload["settings"].update(channel.pagerduty_settings)
            if channel.opsgenie_settings:
                payload["settings"].update(channel.opsgenie_settings)
            if channel.discord_settings:
                payload["settings"].update(channel.discord_settings)
            if channel.teams_settings:
                payload["settings"].update(channel.teams_settings)
            if channel.sns_settings:
                payload["settings"].update(channel.sns_settings)
            
            if channel.secure_fields:
                payload["secureFields"] = {f: True for f in channel.secure_fields}
            
            response = self._make_request("POST", url, data=payload)
            
            channel.uid = response.get("uid", channel.uid)
            logger.info(f"Created notification channel: {channel.name}")
            return channel
        except Exception as e:
            logger.error(f"Failed to create notification channel: {e}")
            raise
    
    def update_notification_channel(
        self,
        channel_uid: str,
        channel: NotificationChannel
    ) -> NotificationChannel:
        """
        Update a notification channel.
        
        Args:
            channel_uid: Channel UID
            channel: Updated channel configuration
            
        Returns:
            Updated NotificationChannel
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/apialert-notifications/channels/{channel_uid}"
            
            payload = {
                "name": channel.name,
                "type": channel.type.value,
                "isDefault": channel.is_default,
                "settings": {}
            }
            
            if channel.email_settings:
                payload["settings"].update(channel.email_settings)
            if channel.slack_settings:
                payload["settings"].update(channel.slack_settings)
            
            self._make_request("PUT", url, data=payload)
            logger.info(f"Updated notification channel: {channel.name}")
            return channel
        except Exception as e:
            logger.error(f"Failed to update notification channel: {e}")
            raise
    
    def delete_notification_channel(self, channel_uid: str) -> bool:
        """
        Delete a notification channel.
        
        Args:
            channel_uid: Channel UID
            
        Returns:
            True if successful
        """
        if not self.grafana_endpoint:
            raise ValueError("Grafana endpoint is required")
        
        try:
            url = f"{self.grafana_endpoint}/apialert-notifications/channels/{channel_uid}"
            self._make_request("DELETE", url)
            logger.info(f"Deleted notification channel: {channel_uid}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete notification channel: {e}")
            raise
    
    def create_email_channel(
        self,
        name: str,
        email_addresses: List[str],
        single_email: bool = False,
        is_default: bool = False
    ) -> NotificationChannel:
        """
        Create an email notification channel.
        
        Args:
            name: Channel name
            email_addresses: List of email addresses
            single_email: Send single email to all recipients
            is_default: Set as default channel
            
        Returns:
            NotificationChannel
        """
        channel = NotificationChannel(
            name=name,
            type=NotificationChannelType.EMAIL,
            email_settings={
                "addresses": ",".join(email_addresses),
                "singleEmail": single_email
            },
            is_default=is_default
        )
        return self.create_notification_channel(channel)
    
    def create_slack_channel(
        self,
        name: str,
        slack_url: str,
        recipient: str = "",
        mention_channel: str = "",
        is_default: bool = False
    ) -> NotificationChannel:
        """
        Create a Slack notification channel.
        
        Args:
            name: Channel name
            slack_url: Slack webhook URL
            recipient: Slack channel or user
            mention_channel: Channel to mention
            is_default: Set as default channel
            
        Returns:
            NotificationChannel
        """
        channel = NotificationChannel(
            name=name,
            type=NotificationChannelType.SLACK,
            slack_settings={
                "url": slack_url,
                "recipient": recipient,
                "mentionChannel": mention_channel
            },
            is_default=is_default
        )
        return self.create_notification_channel(channel)
    
    def create_webhook_channel(
        self,
        name: str,
        webhook_url: str,
        http_method: str = "POST",
        is_default: bool = False
    ) -> NotificationChannel:
        """
        Create a webhook notification channel.
        
        Args:
            name: Channel name
            webhook_url: Webhook URL
            http_method: HTTP method
            is_default: Set as default channel
            
        Returns:
            NotificationChannel
        """
        channel = NotificationChannel(
            name=name,
            type=NotificationChannelType.WEBHOOK,
            webhook_settings={
                "url": webhook_url,
                "httpMethod": http_method
            },
            is_default=is_default
        )
        return self.create_notification_channel(channel)
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def get_workspace_metrics(
        self,
        workspace_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300
    ) -> List[CloudWatchMetrics]:
        """
        Get CloudWatch metrics for the workspace.
        
        Args:
            workspace_id: Workspace ID
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            
        Returns:
            List of CloudWatchMetrics
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch metrics")
        
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=self.region)
            
            end_time = end_time or datetime.utcnow()
            start_time = start_time or (end_time - timedelta(hours=1))
            
            metric_names = [
                "WorkspaceConnections",
                "WorkspaceActiveUsers",
                "WorkspaceCpuUtilization",
                "WorkspaceMemoryUtilization",
                "WorkspaceDiskUtilization"
            ]
            
            metrics = []
            for metric_name in metric_names:
                response = cloudwatch.get_metric_statistics(
                    Namespace="AWS/Grafana",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Sum"]
                )
                
                for dp in response.get("Datapoints", []):
                    cw_metric = CloudWatchMetrics(
                        workspace_id=workspace_id,
                        metric_name=metric_name,
                        timestamp=dp.get("Timestamp", "").isoformat() if hasattr(dp.get("Timestamp", ""), "isoformat") else str(dp.get("Timestamp", "")),
                        value=dp.get("Average", 0.0),
                        unit=dp.get("Unit", "Count"),
                        dimensions={
                            "WorkspaceId": workspace_id
                        }
                    )
                    metrics.append(cw_metric)
            
            return metrics
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get workspace metrics: {e}")
            raise
    
    def get_user_activity_metrics(
        self,
        workspace_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 3600
    ) -> Dict[str, Any]:
        """
        Get user activity metrics.
        
        Args:
            workspace_id: Workspace ID
            start_time: Start time
            end_time: End time
            period: Metric period
            
        Returns:
            User activity metrics
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch metrics")
        
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=self.region)
            
            end_time = end_time or datetime.utcnow()
            start_time = start_time or (end_time - timedelta(days=7))
            
            response = cloudwatch.get_metric_statistics(
                Namespace="AWS/Grafana",
                MetricName="UserActivity",
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Maximum", "Sum"],
                Dimensions=[{"Name": "WorkspaceId", "Value": workspace_id}]
            )
            
            return {
                "workspace_id": workspace_id,
                "datapoints": response.get("Datapoints", []),
                "period": period
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get user activity metrics: {e}")
            raise
    
    def enable_workspace_metrics(
        self,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Enable CloudWatch metrics for the workspace.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            self.grafana_client.update_workspace_configuration(
                workspaceId=workspace_id,
                loggingConfiguration={
                    "metrics": {
                        "enabled": True
                    }
                }
            )
            logger.info(f"Enabled CloudWatch metrics for workspace: {workspace_id}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to enable workspace metrics: {e}")
            raise
    
    def put_metric_data(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            metric_name: Metric name
            value: Metric value
            unit: Unit type
            dimensions: Metric dimensions
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch metrics")
        
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=self.region)
            
            metric_data = {
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Dimensions": [
                    {"Name": k, "Value": v} 
                    for k, v in (dimensions or {}).items()
                ]
            }
            
            if workspace_id:
                metric_data["Dimensions"].append(
                    {"Name": "WorkspaceId", "Value": workspace_id}
                )
            
            cloudwatch.put_metric_data(
                Namespace="AWS/Grafana",
                MetricData=[metric_data]
            )
            
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def create_metric_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 2,
        period: int = 300,
        workspace_id: Optional[str] = None
    ) -> str:
        """
        Create a CloudWatch metric alarm.
        
        Args:
            alarm_name: Alarm name
            metric_name: Metric to monitor
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            workspace_id: Workspace ID
            
        Returns:
            Alarm ARN
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch alarms")
        
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=self.region)
            
            alarm_config = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": "AWS/Grafana",
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": "Average",
                "Dimensions": [{"Name": "WorkspaceId", "Value": workspace_id}]
            }
            
            response = cloudwatch.put_metric_alarm(**alarm_config)
            
            alarm_arn = cloudwatch.describe_alarms(
                AlarmNames=[alarm_name]
            )["MetricAlarms"][0]["AlarmArn"]
            
            logger.info(f"Created metric alarm: {alarm_name}")
            return alarm_arn
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create metric alarm: {e}")
            raise
    
    def get_workspace_tags(
        self,
        workspace_id: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Get tags for a workspace.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            Dict of tag key-value pairs
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            response = self.grafana_client.list_tags_for_resource(
                resourceArn=f"arn:aws:grafana:{self.region}:{self._get_account_id()}:workspace/{workspace_id}"
            )
            return response.get("tags", {})
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get workspace tags: {e}")
            raise
    
    def tag_workspace(
        self,
        tags: Dict[str, str],
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Tag a workspace.
        
        Args:
            tags: Dict of tag key-value pairs
            workspace_id: Workspace ID
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("Workspace ID is required")
        
        try:
            self.grafana_client.tag_resource(
                resourceArn=f"arn:aws:grafana:{self.region}:{self._get_account_id()}:workspace/{workspace_id}",
                tags=tags
            )
            logger.info(f"Tagged workspace: {workspace_id}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to tag workspace: {e}")
            raise
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        if not BOTO3_AVAILABLE:
            return "123456789012"
        return boto3.client("sts").get_caller_identity()["Account"]
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_workspace_endpoint(
        self,
        workspace_id: Optional[str] = None
    ) -> str:
        """
        Get the Grafana workspace endpoint URL.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            Workspace endpoint URL
        """
        workspace_info = self.get_workspace(workspace_id)
        return workspace_info.endpoint
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the Managed Grafana integration.
        
        Returns:
            Health status dict
        """
        result = {
            "status": "healthy",
            "boto3_available": BOTO3_AVAILABLE,
            "workspace_id": self.workspace_id,
            "region": self.region,
            "checks": {}
        }
        
        try:
            if self.workspace_id:
                workspace = self.get_workspace()
                result["checks"]["workspace"] = {
                    "status": "ok",
                    "workspace_status": workspace.status.value
                }
            else:
                result["checks"]["workspace"] = {
                    "status": "not_configured"
                }
        except Exception as e:
            result["checks"]["workspace"] = {
                "status": "error",
                "error": str(e)
            }
            result["status"] = "degraded"
        
        if self.grafana_endpoint and self.grafana_api_key:
            try:
                url = f"{self.grafana_endpoint}/api/health"
                health = self._make_request("GET", url)
                result["checks"]["grafana_api"] = {
                    "status": "ok",
                    "version": health.get("version", "unknown")
                }
            except Exception as e:
                result["checks"]["grafana_api"] = {
                    "status": "error",
                    "error": str(e)
                }
                result["status"] = "degraded"
        else:
            result["checks"]["grafana_api"] = {
                "status": "not_configured"
            }
        
        return result
