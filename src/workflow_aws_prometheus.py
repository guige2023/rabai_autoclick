"""
Amazon Prometheus Service (AMP) Integration Module for Workflow System

Implements a PrometheusIntegration class with:
1. Workspace management: Create/manage Prometheus workspaces
2. Rule groups: Manage recording and alerting rules
3. Alert managers: Configure AlertManager endpoints
4. Targets: Monitor scrape targets
5. Labels: Manage metric labels
6. Service discovery: Managed service discovery
7. Remote write: Configure remote write endpoints
8. Metrics: Query metrics via AMP API
9. Grafana integration: AMP Grafana integration
10. CloudWatch integration: Workspace metrics and monitoring

Commit: 'feat(aws-prometheus): add Amazon Prometheus with workspace management, rule groups, AlertManager, targets, labels, service discovery, remote write, AMP API metrics, Grafana, CloudWatch'
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
    """AMP workspace status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"


class RuleType(Enum):
    """Rule type for recording/alerting rules."""
    RECORDING = "RECORDING"
    ALERTING = "ALERTING"


class AlertManagerStatus(Enum):
    """AlertManager configuration status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"


@dataclass
class WorkspaceConfig:
    """AMP workspace configuration."""
    alias: str = ""
    kms_key_arn: str = ""
    tag_map: Dict[str, str] = field(default_factory=dict)
    logging_configuration: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkspaceInfo:
    """AMP workspace information."""
    workspace_id: str
    arn: str
    status: WorkspaceStatus = WorkspaceStatus.ACTIVE
    alias: str = ""
    kms_key_arn: str = ""
    created_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)
    endpoints: Dict[str, str] = field(default_factory=dict)


@dataclass
class RuleGroup:
    """Prometheus rule group definition."""
    name: str
    interval: str = "60s"
    rules: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RecordingRule:
    """Prometheus recording rule."""
    name: str
    expr: str
    labels: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class AlertingRule:
    """Prometheus alerting rule."""
    name: str
    expr: str
    duration: str = "5m"
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    severity: str = "warning"


@dataclass
class AlertManagerConfig:
    """AlertManager endpoint configuration."""
    name: str
    endpoint: str
    secret_arn: str = ""
    status: AlertManagerStatus = AlertManagerStatus.ACTIVE
    notification_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScrapeTarget:
    """Scrape target configuration."""
    job_name: str
    targets: List[str]
    port: int = 9090
    metrics_path: str = "/metrics"
    interval: str = "60s"
    labels: Dict[str, str] = field(default_factory=dict)
    scrape_timeout: str = "30s"


@dataclass
class MetricLabel:
    """Metric label definition."""
    name: str
    value: str
    metric_name: str = ""


@dataclass
class ServiceDiscoveryTarget:
    """Service discovery target."""
    target_group_arn: str
    target_type: str = "eks"
    port: int = 9090
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class RemoteWriteConfig:
    """Remote write endpoint configuration."""
    name: str
    endpoint: str
    secret_arn: str = ""
    enabled: bool = True
    queue_config: Dict[str, Any] = field(default_factory=dict)
    metadata_config: Dict[str, Any] = field(default_factory=dict)
    write_timeout: int = 30


@dataclass
class QueryResult:
    """AMP query result."""
    metric: Dict[str, str]
    value: Optional[List[Any]] = None
    values: Optional[List[List[Any]]] = None
    status: str = "success"


class PrometheusIntegration:
    """
    Amazon Prometheus Service (AMP) integration class.
    
    Provides comprehensive management of Amazon Prometheus workspaces
    and their associated resources including rule groups, AlertManager
    configurations, scrape targets, and metric queries.
    
    Features:
    - Workspace management: Create/manage Prometheus workspaces
    - Rule groups: Manage recording and alerting rules
    - Alert managers: Configure AlertManager endpoints
    - Targets: Monitor scrape targets
    - Labels: Manage metric labels
    - Service discovery: Managed service discovery
    - Remote write: Configure remote write endpoints
    - Metrics: Query metrics via AMP API
    - Grafana integration: AMP Grafana integration
    - CloudWatch integration: Workspace metrics and monitoring
    """
    
    # AMP API endpoints
    AMP_API_VERSION = "2010-08-01"
    
    def __init__(
        self,
        region: str = "us-west-2",
        profile: Optional[str] = None,
        workspace_id: Optional[str] = None
    ):
        """
        Initialize AMP integration.
        
        Args:
            region: AWS region for AMP
            profile: AWS profile name (optional)
            workspace_id: Default workspace ID to use
        """
        self.region = region
        self.profile = profile
        self.workspace_id = workspace_id
        
        # Thread safety
        self._lock = threading.RLock()
        
        # AWS clients
        self._amp_client = None
        self._cloudwatch_client = None
        self._sts_client = None
        self._kms_client = None
        
        # Workspace cache
        self._workspaces: Dict[str, WorkspaceInfo] = {}
        
        # Configuration cache
        self._rule_groups: Dict[str, List[RuleGroup]] = {}
        self._alert_managers: Dict[str, AlertManagerConfig] = {}
        self._scrape_targets: Dict[str, List[ScrapeTarget]] = {}
        self._labels: Dict[str, List[MetricLabel]] = {}
        self._service_discovery: Dict[str, List[ServiceDiscoveryTarget]] = {}
        self._remote_write_configs: Dict[str, List[RemoteWriteConfig]] = {}
        
        if BOTO3_AVAILABLE:
            self._init_clients()
    
    def _init_clients(self) -> None:
        """Initialize AWS clients."""
        try:
            session_kwargs = {"region_name": self.region}
            if self.profile:
                session_kwargs["profile_name"] = self.profile
            
            session = boto3.Session(**session_kwargs)
            
            self._amp_client = session.client(
                "amp",
                region_name=self.region
            )
            self._cloudwatch_client = session.client(
                "cloudwatch",
                region_name=self.region
            )
            self._sts_client = session.client(
                "sts",
                region_name=self.region
            )
            self._kms_client = session.client(
                "kms",
                region_name=self.region
            )
            logger.info(f"Initialized AMP clients in region {self.region}")
        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
    
    @property
    def account_id(self) -> str:
        """Get AWS account ID."""
        if not self._sts_client:
            return ""
        try:
            return self._sts_client.get_caller_identity()["Account"]
        except Exception:
            return ""
    
    # =========================================================================
    # Workspace Management
    # =========================================================================
    
    def create_workspace(
        self,
        alias: Optional[str] = None,
        kms_key_arn: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> WorkspaceInfo:
        """
        Create a new AMP workspace.
        
        Args:
            alias: Optional workspace alias
            kms_key_arn: Optional KMS key ARN for encryption
            tags: Optional tags for the workspace
            
        Returns:
            WorkspaceInfo object with workspace details
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = str(uuid.uuid4())[:8]
        alias = alias or f"rabai-workspace-{workspace_id}"
        
        create_params = {
            "alias": alias,
            "tags": tags or {}
        }
        
        if kms_key_arn:
            create_params["kmsKeyArn"] = kms_key_arn
        
        try:
            response = self._amp_client.create_workspace(**create_params)
            workspace_id = response["workspace"]["workspaceId"]
            
            # Wait for workspace to become active
            workspace = self._wait_for_workspace_active(workspace_id)
            
            with self._lock:
                self._workspaces[workspace_id] = workspace
            
            logger.info(f"Created AMP workspace: {workspace_id}")
            return workspace
            
        except ClientError as e:
            logger.error(f"Failed to create workspace: {e}")
            raise
    
    def _wait_for_workspace_active(
        self,
        workspace_id: str,
        timeout: int = 300,
        check_interval: int = 10
    ) -> WorkspaceInfo:
        """Wait for workspace to become active."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            workspace = self.describe_workspace(workspace_id)
            if workspace.status == WorkspaceStatus.ACTIVE:
                return workspace
            
            logger.debug(f"Workspace {workspace_id} status: {workspace.status}")
            time.sleep(check_interval)
        
        raise TimeoutError(f"Workspace {workspace_id} did not become active within {timeout}s")
    
    def describe_workspace(self, workspace_id: Optional[str] = None) -> WorkspaceInfo:
        """
        Describe an AMP workspace.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            WorkspaceInfo object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.describe_workspace(workspaceId=workspace_id)
            ws = response["workspace"]
            
            return WorkspaceInfo(
                workspace_id=ws["workspaceId"],
                arn=ws["arn"],
                status=WorkspaceStatus(ws.get("status", "ACTIVE")),
                alias=ws.get("alias", ""),
                kms_key_arn=ws.get("kmsKeyArn", ""),
                created_at=ws.get("createdAt"),
                tags=ws.get("tags", {}),
                endpoints=ws.get("endpoints", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to describe workspace: {e}")
            raise
    
    def list_workspaces(
        self,
        max_results: int = 100
    ) -> List[WorkspaceInfo]:
        """
        List all AMP workspaces.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of WorkspaceInfo objects
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        try:
            response = self._amp_client.list_workspaces(maxResults=max_results)
            workspaces = []
            
            for ws in response.get("workspaces", []):
                workspaces.append(WorkspaceInfo(
                    workspace_id=ws["workspaceId"],
                    arn=ws["arn"],
                    status=WorkspaceStatus(ws.get("status", "ACTIVE")),
                    alias=ws.get("alias", ""),
                    created_at=ws.get("createdAt"),
                    tags=ws.get("tags", {})
                ))
            
            return workspaces
            
        except ClientError as e:
            logger.error(f"Failed to list workspaces: {e}")
            raise
    
    def delete_workspace(self, workspace_id: Optional[str] = None) -> bool:
        """
        Delete an AMP workspace.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if deletion was successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.delete_workspace(workspaceId=workspace_id)
            
            with self._lock:
                if workspace_id in self._workspaces:
                    del self._workspaces[workspace_id]
            
            logger.info(f"Deleted AMP workspace: {workspace_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete workspace: {e}")
            raise
    
    def update_workspace_alias(
        self,
        alias: str,
        workspace_id: Optional[str] = None
    ) -> WorkspaceInfo:
        """
        Update workspace alias.
        
        Args:
            alias: New alias for the workspace
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Updated WorkspaceInfo object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.update_workspace_alias(
                workspaceId=workspace_id,
                alias=alias
            )
            
            return self.describe_workspace(workspace_id)
            
        except ClientError as e:
            logger.error(f"Failed to update workspace alias: {e}")
            raise
    
    def get_workspace_endpoints(
        self,
        workspace_id: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Get workspace endpoints.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Dictionary of endpoint types and URLs
        """
        workspace = self.describe_workspace(workspace_id)
        return workspace.endpoints
    
    def get_amp_endpoint(
        self,
        workspace_id: Optional[str] = None
    ) -> str:
        """
        Get the AMP API endpoint for querying metrics.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            AMP API endpoint URL
        """
        endpoints = self.get_workspace_endpoints(workspace_id)
        return endpoints.get("default", "")
    
    # =========================================================================
    # Rule Groups Management
    # =========================================================================
    
    def put_rule_group(
        self,
        rule_group: RuleGroup,
        rule_type: RuleType = RuleType.RECORDING,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Create or update a rule group.
        
        Args:
            rule_group: RuleGroup object with rules
            rule_type: Type of rules (RECORDING or ALERTING)
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        # Convert rule group to Prometheus rule group format
        rules_content = {
            "name": rule_group.name,
            "interval": rule_group.interval,
            "rules": rule_group.rules
        }
        
        try:
            self._amp_client.put_rule_group(
                workspaceId=workspace_id,
                name=rule_group.name,
                ruleGroup=json.dumps(rules_content)
            )
            
            with self._lock:
                if workspace_id not in self._rule_groups:
                    self._rule_groups[workspace_id] = []
                self._rule_groups[workspace_id].append(rule_group)
            
            logger.info(f"Created/updated rule group: {rule_group.name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create rule group: {e}")
            raise
    
    def create_recording_rules(
        self,
        rules: List[RecordingRule],
        rule_group_name: str = "rabai_recording_rules",
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Create recording rules.
        
        Args:
            rules: List of RecordingRule objects
            rule_group_name: Name for the rule group
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        rule_group = RuleGroup(
            name=rule_group_name,
            interval="60s",
            rules=[
                {
                    "record": rule.name,
                    "expr": rule.expr,
                    "labels": rule.labels
                }
                for rule in rules
            ]
        )
        
        return self.put_rule_group(rule_group, RuleType.RECORDING, workspace_id)
    
    def create_alerting_rules(
        self,
        rules: List[AlertingRule],
        rule_group_name: str = "rabai_alerting_rules",
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Create alerting rules.
        
        Args:
            rules: List of AlertingRule objects
            rule_group_name: Name for the rule group
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        rule_group = RuleGroup(
            name=rule_group_name,
            interval="60s",
            rules=[
                {
                    "alert": rule.name,
                    "expr": rule.expr,
                    "for": rule.duration,
                    "labels": rule.labels,
                    "annotations": rule.annotations
                }
                for rule in rules
            ]
        )
        
        return self.put_rule_group(rule_group, RuleType.ALERTING, workspace_id)
    
    def list_rule_groups(
        self,
        workspace_id: Optional[str] = None
    ) -> List[str]:
        """
        List rule group names.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of rule group names
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.list_ruleGroups(workspaceId=workspace_id)
            return [rg["name"] for rg in response.get("ruleGroups", [])]
            
        except ClientError as e:
            logger.error(f"Failed to list rule groups: {e}")
            raise
    
    def describe_rule_group(
        self,
        rule_group_name: str,
        workspace_id: Optional[str] = None
    ) -> RuleGroup:
        """
        Describe a rule group.
        
        Args:
            rule_group_name: Name of the rule group
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            RuleGroup object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.describeRuleGroup(
                workspaceId=workspace_id,
                name=rule_group_name
            )
            
            rule_group_data = json.loads(response["ruleGroup"]["content"])
            
            return RuleGroup(
                name=rule_group_data["name"],
                interval=rule_group_data.get("interval", "60s"),
                rules=rule_group_data.get("rules", [])
            )
            
        except ClientError as e:
            logger.error(f"Failed to describe rule group: {e}")
            raise
    
    def delete_rule_group(
        self,
        rule_group_name: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete a rule group.
        
        Args:
            rule_group_name: Name of the rule group
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.deleteRuleGroup(
                workspaceId=workspace_id,
                name=rule_group_name
            )
            
            with self._lock:
                if workspace_id in self._rule_groups:
                    self._rule_groups[workspace_id] = [
                        rg for rg in self._rule_groups[workspace_id]
                        if rg.name != rule_group_name
                    ]
            
            logger.info(f"Deleted rule group: {rule_group_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete rule group: {e}")
            raise
    
    # =========================================================================
    # AlertManager Configuration
    # =========================================================================
    
    def create_alert_manager(
        self,
        config: AlertManagerConfig,
        workspace_id: Optional[str] = None
    ) -> AlertManagerConfig:
        """
        Create an AlertManager configuration.
        
        Args:
            config: AlertManagerConfig object
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Created AlertManagerConfig object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.createAlertManager(
                workspaceId=workspace_id,
                name=config.name,
                endpoint=config.endpoint,
                secretArn=config.secret_arn
            )
            
            with self._lock:
                self._alert_managers[config.name] = config
            
            logger.info(f"Created AlertManager configuration: {config.name}")
            return config
            
        except ClientError as e:
            logger.error(f"Failed to create AlertManager: {e}")
            raise
    
    def get_alert_manager(
        self,
        name: str,
        workspace_id: Optional[str] = None
    ) -> AlertManagerConfig:
        """
        Get AlertManager configuration.
        
        Args:
            name: AlertManager configuration name
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            AlertManagerConfig object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.getAlertManager(
                workspaceId=workspace_id,
                name=name
            )
            
            am = response["alertManager"]
            return AlertManagerConfig(
                name=am["name"],
                endpoint=am["endpoint"],
                secret_arn=am.get("secretArn", ""),
                status=AlertManagerStatus(am.get("status", "ACTIVE")),
                notification_config=am.get("notificationConfiguration", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get AlertManager: {e}")
            raise
    
    def update_alert_manager(
        self,
        name: str,
        endpoint: Optional[str] = None,
        secret_arn: Optional[str] = None,
        workspace_id: Optional[str] = None
    ) -> AlertManagerConfig:
        """
        Update AlertManager configuration.
        
        Args:
            name: AlertManager configuration name
            endpoint: New endpoint URL
            secret_arn: New secret ARN
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Updated AlertManagerConfig object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        update_params = {"workspaceId": workspace_id, "name": name}
        if endpoint:
            update_params["endpoint"] = endpoint
        if secret_arn:
            update_params["secretArn"] = secret_arn
        
        try:
            self._amp_client.updateAlertManager(**update_params)
            
            return self.get_alert_manager(name, workspace_id)
            
        except ClientError as e:
            logger.error(f"Failed to update AlertManager: {e}")
            raise
    
    def delete_alert_manager(
        self,
        name: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete AlertManager configuration.
        
        Args:
            name: AlertManager configuration name
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.deleteAlertManager(
                workspaceId=workspace_id,
                name=name
            )
            
            with self._lock:
                if name in self._alert_managers:
                    del self._alert_managers[name]
            
            logger.info(f"Deleted AlertManager: {name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete AlertManager: {e}")
            raise
    
    # =========================================================================
    # Scrape Targets Management
    # =========================================================================
    
    def create_scrape_targets(
        self,
        targets: List[ScrapeTarget],
        workspace_id: Optional[str] = None
    ) -> List[ScrapeTarget]:
        """
        Create scrape target configurations.
        
        Args:
            targets: List of ScrapeTarget objects
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of created ScrapeTarget objects
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        created_targets = []
        
        for target in targets:
            try:
                self._amp_client.createTarget(
                    workspaceId=workspace_id,
                    name=target.job_name,
                    targetTargets=target.targets,
                    port=target.port,
                    metricsPath=target.metrics_path,
                    interval=target.interval,
                    scrapeTimeout=target.scrape_timeout,
                    labels=target.labels
                )
                created_targets.append(target)
                
            except ClientError as e:
                logger.error(f"Failed to create scrape target {target.job_name}: {e}")
                raise
        
        with self._lock:
            if workspace_id not in self._scrape_targets:
                self._scrape_targets[workspace_id] = []
            self._scrape_targets[workspace_id].extend(created_targets)
        
        logger.info(f"Created {len(created_targets)} scrape targets")
        return created_targets
    
    def list_scrape_targets(
        self,
        workspace_id: Optional[str] = None
    ) -> List[ScrapeTarget]:
        """
        List scrape targets.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of ScrapeTarget objects
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.listTargets(
                workspaceId=workspace_id
            )
            
            targets = []
            for t in response.get("targets", []):
                targets.append(ScrapeTarget(
                    job_name=t["name"],
                    targets=t["targetTargets"],
                    port=t.get("port", 9090),
                    metrics_path=t.get("metricsPath", "/metrics"),
                    interval=t.get("interval", "60s"),
                    labels=t.get("labels", {})
                ))
            
            return targets
            
        except ClientError as e:
            logger.error(f"Failed to list scrape targets: {e}")
            raise
    
    def delete_scrape_target(
        self,
        job_name: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete a scrape target.
        
        Args:
            job_name: Job name of the target
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.deleteTarget(
                workspaceId=workspace_id,
                name=job_name
            )
            
            with self._lock:
                if workspace_id in self._scrape_targets:
                    self._scrape_targets[workspace_id] = [
                        t for t in self._scrape_targets[workspace_id]
                        if t.job_name != job_name
                    ]
            
            logger.info(f"Deleted scrape target: {job_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete scrape target: {e}")
            raise
    
    # =========================================================================
    # Labels Management
    # =========================================================================
    
    def create_label(
        self,
        metric_name: str,
        name: str,
        value: str,
        workspace_id: Optional[str] = None
    ) -> MetricLabel:
        """
        Create a metric label.
        
        Args:
            metric_name: Name of the metric
            name: Label name
            value: Label value
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Created MetricLabel object
        """
        label = MetricLabel(
            name=name,
            value=value,
            metric_name=metric_name
        )
        
        with self._lock:
            if workspace_id not in self._labels:
                self._labels[workspace_id] = []
            self._labels[workspace_id].append(label)
        
        logger.info(f"Created label {name}={value} for metric {metric_name}")
        return label
    
    def list_labels(
        self,
        metric_name: Optional[str] = None,
        workspace_id: Optional[str] = None
    ) -> List[MetricLabel]:
        """
        List metric labels.
        
        Args:
            metric_name: Optional filter by metric name
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of MetricLabel objects
        """
        workspace_id = workspace_id or self.workspace_id
        
        with self._lock:
            labels = self._labels.get(workspace_id, [])
            
            if metric_name:
                labels = [l for l in labels if l.metric_name == metric_name]
            
            return labels
    
    def update_label(
        self,
        metric_name: str,
        name: str,
        value: str,
        workspace_id: Optional[str] = None
    ) -> MetricLabel:
        """
        Update a metric label.
        
        Args:
            metric_name: Name of the metric
            name: Label name
            value: New label value
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Updated MetricLabel object
        """
        workspace_id = workspace_id or self.workspace_id
        
        with self._lock:
            labels = self._labels.get(workspace_id, [])
            for label in labels:
                if label.metric_name == metric_name and label.name == name:
                    label.value = value
                    return label
        
        return self.create_label(metric_name, name, value, workspace_id)
    
    def delete_label(
        self,
        metric_name: str,
        name: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete a metric label.
        
        Args:
            metric_name: Name of the metric
            name: Label name
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        
        with self._lock:
            labels = self._labels.get(workspace_id, [])
            self._labels[workspace_id] = [
                l for l in labels
                if not (l.metric_name == metric_name and l.name == name)
            ]
        
        logger.info(f"Deleted label {name} from metric {metric_name}")
        return True
    
    # =========================================================================
    # Service Discovery
    # =========================================================================
    
    def register_service_discovery_target(
        self,
        target: ServiceDiscoveryTarget,
        workspace_id: Optional[str] = None
    ) -> ServiceDiscoveryTarget:
        """
        Register a service discovery target.
        
        Args:
            target: ServiceDiscoveryTarget object
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Created ServiceDiscoveryTarget object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.createTarget(
                workspaceId=workspace_id,
                name=f"sd-{target.target_type}-{target.target_group_arn}",
                targetTargets=[target.target_group_arn],
                port=target.port,
                labels=target.labels
            )
            
            with self._lock:
                if workspace_id not in self._service_discovery:
                    self._service_discovery[workspace_id] = []
                self._service_discovery[workspace_id].append(target)
            
            logger.info(f"Registered service discovery target: {target.target_group_arn}")
            return target
            
        except ClientError as e:
            logger.error(f"Failed to register service discovery target: {e}")
            raise
    
    def list_service_discovery_targets(
        self,
        workspace_id: Optional[str] = None
    ) -> List[ServiceDiscoveryTarget]:
        """
        List service discovery targets.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of ServiceDiscoveryTarget objects
        """
        workspace_id = workspace_id or self.workspace_id
        
        with self._lock:
            return self._service_discovery.get(workspace_id, [])
    
    def deregister_service_discovery_target(
        self,
        target_group_arn: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Deregister a service discovery target.
        
        Args:
            target_group_arn: Target group ARN
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        workspace_id = workspace_id or self.workspace_id
        
        with self._lock:
            if workspace_id in self._service_discovery:
                self._service_discovery[workspace_id] = [
                    t for t in self._service_discovery[workspace_id]
                    if t.target_group_arn != target_group_arn
                ]
        
        logger.info(f"Deregistered service discovery target: {target_group_arn}")
        return True
    
    # =========================================================================
    # Remote Write Configuration
    # =========================================================================
    
    def create_remote_write_config(
        self,
        config: RemoteWriteConfig,
        workspace_id: Optional[str] = None
    ) -> RemoteWriteConfig:
        """
        Create a remote write configuration.
        
        Args:
            config: RemoteWriteConfig object
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Created RemoteWriteConfig object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        remote_write_params = {
            "name": config.name,
            "endpoint": config.endpoint,
            "secretArn": config.secret_arn,
            "enabled": config.enabled,
            "queueConfig": config.queue_config,
            "metadataConfig": config.metadata_config,
            "writeTimeout": config.write_timeout
        }
        
        try:
            self._amp_client.createRemoteWrite(
                workspaceId=workspace_id,
                **remote_write_params
            )
            
            with self._lock:
                if workspace_id not in self._remote_write_configs:
                    self._remote_write_configs[workspace_id] = []
                self._remote_write_configs[workspace_id].append(config)
            
            logger.info(f"Created remote write config: {config.name}")
            return config
            
        except ClientError as e:
            logger.error(f"Failed to create remote write config: {e}")
            raise
    
    def list_remote_write_configs(
        self,
        workspace_id: Optional[str] = None
    ) -> List[RemoteWriteConfig]:
        """
        List remote write configurations.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of RemoteWriteConfig objects
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.listRemoteWrites(
                workspaceId=workspace_id
            )
            
            configs = []
            for rw in response.get("remoteWrites", []):
                configs.append(RemoteWriteConfig(
                    name=rw["name"],
                    endpoint=rw["endpoint"],
                    secret_arn=rw.get("secretArn", ""),
                    enabled=rw.get("enabled", True),
                    queue_config=rw.get("queueConfig", {}),
                    metadata_config=rw.get("metadataConfig", {}),
                    write_timeout=rw.get("writeTimeout", 30)
                ))
            
            return configs
            
        except ClientError as e:
            logger.error(f"Failed to list remote write configs: {e}")
            raise
    
    def update_remote_write_config(
        self,
        name: str,
        endpoint: Optional[str] = None,
        secret_arn: Optional[str] = None,
        enabled: Optional[bool] = None,
        workspace_id: Optional[str] = None
    ) -> RemoteWriteConfig:
        """
        Update a remote write configuration.
        
        Args:
            name: Configuration name
            endpoint: New endpoint URL
            secret_arn: New secret ARN
            enabled: New enabled status
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Updated RemoteWriteConfig object
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        update_params = {"workspaceId": workspace_id, "name": name}
        if endpoint:
            update_params["endpoint"] = endpoint
        if secret_arn:
            update_params["secretArn"] = secret_arn
        if enabled is not None:
            update_params["enabled"] = enabled
        
        try:
            self._amp_client.updateRemoteWrite(**update_params)
            
            # Return updated config
            configs = self.list_remote_write_configs(workspace_id)
            for config in configs:
                if config.name == name:
                    return config
            
            raise ValueError(f"Remote write config {name} not found after update")
            
        except ClientError as e:
            logger.error(f"Failed to update remote write config: {e}")
            raise
    
    def delete_remote_write_config(
        self,
        name: str,
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Delete a remote write configuration.
        
        Args:
            name: Configuration name
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            self._amp_client.deleteRemoteWrite(
                workspaceId=workspace_id,
                name=name
            )
            
            with self._lock:
                if workspace_id in self._remote_write_configs:
                    self._remote_write_configs[workspace_id] = [
                        c for c in self._remote_write_configs[workspace_id]
                        if c.name != name
                    ]
            
            logger.info(f"Deleted remote write config: {name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete remote write config: {e}")
            raise
    
    # =========================================================================
    # Metrics Query (AMP API)
    # =========================================================================
    
    def query_metrics(
        self,
        query: str,
        time: Optional[str] = None,
        timeout: Optional[str] = None,
        workspace_id: Optional[str] = None
    ) -> List[QueryResult]:
        """
        Query metrics using PromQL.
        
        Args:
            query: PromQL query string
            time: Evaluation time (RFC3339 or Unix timestamp)
            timeout: Query timeout
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            List of QueryResult objects
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        query_params = {"workspaceId": workspace_id, "query": query}
        if time:
            query_params["time"] = time
        if timeout:
            query_params["timeout"] = timeout
        
        try:
            response = self._amp_client.queryMetrics(**query_params)
            
            results = []
            for result in response.get("results", []):
                if "values" in result:
                    results.append(QueryResult(
                        metric=result.get("metric", {}),
                        values=result["values"],
                        status=result.get("status", "success")
                    ))
                elif "value" in result:
                    results.append(QueryResult(
                        metric=result.get("metric", {}),
                        value=result["value"],
                        status=result.get("status", "success")
                    ))
            
            return results
            
        except ClientError as e:
            logger.error(f"Failed to query metrics: {e}")
            raise
    
    def query_range(
        self,
        query: str,
        start: str,
        end: str,
        step: str,
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query metrics over a time range.
        
        Args:
            query: PromQL query string
            start: Start time (RFC3339 or Unix timestamp)
            end: End time (RFC3339 or Unix timestamp)
            step: Query resolution step
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Query range results
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.queryRange(
                workspaceId=workspace_id,
                query=query,
                start=start,
                end=end,
                step=step
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to query metrics range: {e}")
            raise
    
    def get_metric_data(
        self,
        metric_data_queries: List[Dict[str, Any]],
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get metric data using MetricDataQueries.
        
        Args:
            metric_data_queries: List of metric data query specifications
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Metric data results
        """
        if not self._amp_client:
            raise RuntimeError("AMP client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._amp_client.getMetricData(
                workspaceId=workspace_id,
                metricDataQueries=metric_data_queries
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get metric data: {e}")
            raise
    
    # =========================================================================
    # Grafana Integration
    # =========================================================================
    
    def get_grafana_workspace_config(
        self,
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get Grafana workspace configuration for AMP.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Grafana workspace configuration
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        endpoints = self.get_workspace_endpoints(workspace_id)
        amp_endpoint = endpoints.get("default", "")
        
        grafana_config = {
            "amp_endpoint": amp_endpoint,
            "workspace_id": workspace_id,
            "prometheus_datasource_type": "amp",
            "query_config": {
                "httpMethod": "POST",
                "queryTimeout": "60s"
            }
        }
        
        return grafana_config
    
    def create_grafana_dashboard_json(
        self,
        dashboard_title: str = "AMP Metrics Dashboard",
        panels: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a Grafana dashboard JSON for AMP metrics.
        
        Args:
            dashboard_title: Title of the dashboard
            panels: Optional list of panel configurations
            
        Returns:
            Grafana dashboard JSON
        """
        default_panels = panels or [
            {
                "title": "Container CPU Usage",
                "targets": [
                    {
                        "expr": "container_cpu_usage_seconds_total",
                        "legendFormat": "{{container}}"
                    }
                ],
                "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8}
            },
            {
                "title": "Container Memory Usage",
                "targets": [
                    {
                        "expr": "container_memory_usage_bytes",
                        "legendFormat": "{{container}}"
                    }
                ],
                "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8}
            },
            {
                "title": "Request Rate",
                "targets": [
                    {
                        "expr": "rate(http_requests_total[5m])",
                        "legendFormat": "{{method}} {{path}}"
                    }
                ],
                "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8}
            },
            {
                "title": "Error Rate",
                "targets": [
                    {
                        "expr": "rate(http_requests_total{status=~\"5..\"}[5m])",
                        "legendFormat": "{{method}} {{path}}"
                    }
                ],
                "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8}
            }
        ]
        
        dashboard = {
            "title": dashboard_title,
            "uid": f"amp-{hashlib.md5(dashboard_title.encode()).hexdigest()[:8]}",
            "version": 1,
            "panels": default_panels,
            "time": {"from": "now-1h", "to": "now"},
            "refresh": "30s",
            "datasource": "AMP"
        }
        
        return dashboard
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def get_workspace_metrics(
        self,
        namespace: str = "AWS/Prometheus",
        workspace_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 60,
        statistics: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for AMP workspace.
        
        Args:
            namespace: CloudWatch namespace
            workspace_id: Workspace ID (uses default if not provided)
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            statistics: Statistics to retrieve
            
        Returns:
            List of CloudWatch metrics
        """
        if not self._cloudwatch_client:
            raise RuntimeError("CloudWatch client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        statistics = statistics or ["Sum", "Average", "Maximum"]
        
        # Default metrics for AMP workspace
        metric_names = [
            "ActiveAlerts",
            "ErrorFailedRequests",
            "IngestionRate",
            "IngestionRateBytes",
            "NumberOfActiveTargets",
            "PendingChunks",
            "QueryLatency",
            "ActiveRecordingRules"
        ]
        
        end_time = end_time or datetime.now()
        start_time = start_time or end_time - timedelta(hours=1)
        
        try:
            metrics = []
            
            for metric_name in metric_names:
                response = self._cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "WorkspaceId", "Value": workspace_id}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=statistics
                )
                
                metrics.append({
                    "metric_name": metric_name,
                    "datapoints": response.get("Datapoints", [])
                })
            
            return metrics
            
        except ClientError as e:
            logger.error(f"Failed to get workspace metrics: {e}")
            raise
    
    def put_workspace_metric_data(
        self,
        metric_data: List[Dict[str, Any]],
        namespace: str = "AWS/Prometheus",
        workspace_id: Optional[str] = None
    ) -> bool:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            metric_data: List of metric data
            namespace: CloudWatch namespace
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            True if successful
        """
        if not self._cloudwatch_client:
            raise RuntimeError("CloudWatch client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        
        metric_data_params = []
        for md in metric_data:
            metric_data_params.append({
                "MetricName": md["metric_name"],
                "Dimensions": [
                    {"Name": "WorkspaceId", "Value": workspace_id or "unknown"}
                ],
                "Timestamp": md.get("timestamp", datetime.now()),
                "Value": md["value"],
                "Unit": md.get("unit", "None")
            })
        
        try:
            self._cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data_params
            )
            
            logger.info(f"Put {len(metric_data)} metric data points to CloudWatch")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def describe_workspace_alarms(
        self,
        workspace_id: Optional[str] = None,
        alarm_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe CloudWatch alarms for AMP workspace.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            alarm_names: Optional list of alarm names
            
        Returns:
            List of alarm descriptions
        """
        if not self._cloudwatch_client:
            raise RuntimeError("CloudWatch client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        
        try:
            if alarm_names:
                response = self._cloudwatch_client.describe_alarms(
                    AlarmNames=alarm_names
                )
            else:
                response = self._cloudwatch_client.describe_alarms(
                    Filters=[
                        {"Namespace": "AWS/Prometheus"}
                    ]
                )
            
            alarms = []
            for alarm in response.get("MetricAlarms", []):
                dims = alarm.get("Dimensions", [])
                if any(d.get("Value") == workspace_id for d in dims):
                    alarms.append(alarm)
            
            return alarms
            
        except ClientError as e:
            logger.error(f"Failed to describe alarms: {e}")
            raise
    
    def create_workspace_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 60,
        statistic: str = "Average",
        workspace_id: Optional[str] = None,
        alarm_description: str = ""
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for AMP workspace metrics.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Metric name to alarm on
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic to use
            workspace_id: Workspace ID (uses default if not provided)
            alarm_description: Alarm description
            
        Returns:
            Created alarm details
        """
        if not self._cloudwatch_client:
            raise RuntimeError("CloudWatch client not initialized")
        
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            response = self._cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=alarm_description,
                Namespace="AWS/Prometheus",
                MetricName=metric_name,
                Dimensions=[
                    {"Name": "WorkspaceId", "Value": workspace_id}
                ],
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=period,
                Statistic=statistic
            )
            
            logger.info(f"Created alarm: {alarm_name}")
            return {"alarm_name": alarm_name, "status": "created"}
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_amp_health(self, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get AMP workspace health status.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Health status information
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            workspace = self.describe_workspace(workspace_id)
            
            # Get basic health info
            health = {
                "workspace_id": workspace_id,
                "status": workspace.status.value,
                "alias": workspace.alias,
                "endpoints": workspace.endpoints,
                "rule_groups_count": len(self.list_rule_groups(workspace_id)),
                "targets_count": len(self.list_scrape_targets(workspace_id)),
                "remote_writes_count": len(self.list_remote_write_configs(workspace_id))
            }
            
            return health
            
        except Exception as e:
            logger.error(f"Failed to get AMP health: {e}")
            return {
                "workspace_id": workspace_id,
                "status": "error",
                "error": str(e)
            }
    
    def generate_prometheus_config(
        self,
        workspace_id: Optional[str] = None,
        include_remote_write: bool = True
    ) -> Dict[str, Any]:
        """
        Generate Prometheus configuration for AMP.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            include_remote_write: Include remote write configuration
            
        Returns:
            Prometheus configuration dictionary
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        endpoints = self.get_workspace_endpoints(workspace_id)
        remote_write_endpoint = endpoints.get("default", "")
        
        config = {
            "global": {
                "scrape_interval": "60s",
                "evaluation_interval": "60s"
            },
            "scrape_configs": [
                {
                    "job_name": "rabai-autoclick",
                    "static_configs": [
                        {
                            "targets": ["localhost:9090"]
                        }
                    ]
                }
            ]
        }
        
        if include_remote_write and remote_write_endpoint:
            config["remote_write"] = [
                {
                    "name": "amp",
                    "url": remote_write_endpoint + "/api/v1/remote_write",
                    "sigv4": {
                        "region": self.region
                    }
                }
            ]
        
        return config
    
    def export_rules_yaml(
        self,
        recording_rules: Optional[List[RecordingRule]] = None,
        alerting_rules: Optional[List[AlertingRule]] = None
    ) -> str:
        """
        Export rules in Prometheus YAML format.
        
        Args:
            recording_rules: List of recording rules
            alerting_rules: List of alerting rules
            
        Returns:
            YAML formatted rules
        """
        groups = []
        
        if recording_rules:
            groups.append({
                "name": "recording_rules",
                "interval": "60s",
                "rules": [
                    {
                        "record": rule.name,
                        "expr": rule.expr,
                        "labels": rule.labels
                    }
                    for rule in recording_rules
                ]
            })
        
        if alerting_rules:
            groups.append({
                "name": "alerting_rules",
                "interval": "60s",
                "rules": [
                    {
                        "alert": rule.name,
                        "expr": rule.expr,
                        "for": rule.duration,
                        "labels": rule.labels,
                        "annotations": rule.annotations
                    }
                    for rule in alerting_rules
                ]
            })
        
        return yaml.dump({"groups": groups}, default_flow_style=False)
    
    def get_metrics_summary(
        self,
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary of all metrics in the workspace.
        
        Args:
            workspace_id: Workspace ID (uses default if not provided)
            
        Returns:
            Summary of available metrics
        """
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            raise ValueError("No workspace_id provided or configured")
        
        try:
            # Get active metrics
            results = self.query_metrics(
                query="count({__name__=~\".+\"}) by (__name__)",
                workspace_id=workspace_id
            )
            
            metrics_count = 0
            if results:
                for result in results:
                    if result.values:
                        metrics_count = sum(v[1] for v in result.values if len(v) > 1)
            
            # Get active series count
            series_count = self.query_metrics(
                query="count({__name__=~\".+\"})",
                workspace_id=workspace_id
            )
            
            total_series = 0
            if series_count:
                for result in series_count:
                    if result.value and len(result.value) > 1:
                        total_series = float(result.value[1])
            
            return {
                "workspace_id": workspace_id,
                "metrics_count": int(metrics_count),
                "total_series": int(total_series),
                "rule_groups": len(self.list_rule_groups(workspace_id)),
                "scrape_targets": len(self.list_scrape_targets(workspace_id)),
                "alert_managers": len(self._alert_managers),
                "remote_writes": len(self.list_remote_write_configs(workspace_id))
            }
            
        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            raise
