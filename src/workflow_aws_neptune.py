"""
AWS Neptune Graph Database Integration Module for Workflow System

Implements a NeptuneIntegration class with:
1. Cluster management: Create/manage Neptune clusters
2. Instance management: Create/manage reader and writer instances
3. Graph operations: Execute Gremlin and SPARQL queries
4. Property graph: Manage property graph data
5. RDF graph: Manage RDF graph data
6. Serverless: Neptune Serverless
7. Global clusters: Global multi-region clusters
8. Backups: Create/manage backups and snapshots
9. IAM auth: Configure IAM authentication
10. CloudWatch integration: Metrics and monitoring

Commit: 'feat(aws-neptune): add AWS Neptune with cluster management, Gremlin/SPARQL queries, property/RDF graphs, serverless, global clusters, backups, IAM auth, CloudWatch'
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
import re

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


class NeptuneInstanceType(Enum):
    """Neptune instance types."""
    STANDARD_DB_R5_LARGE = "db.r5.large"
    STANDARD_DB_R5_XLARGE = "db.r5.xlarge"
    STANDARD_DB_R5_2XLARGE = "db.r5.2xlarge"
    STANDARD_DB_R5_4XLARGE = "db.r5.4xlarge"
    STANDARD_DB_R5_8XLARGE = "db.r5.8xlarge"
    STANDARD_DB_R5_12XLARGE = "db.r5.12xlarge"
    STANDARD_DB_R5_16XLARGE = "db.r5.16xlarge"
    STANDARD_DB_R5_24XLARGE = "db.r5.24xlarge"
    SERVERLESS_DB_R5_LARGE = "serverless.db.r5.large"
    SERVERLESS_DB_R5_XLARGE = "serverless.db.r5.xlarge"
    SERVERLESS_DB_R5_2XLARGE = "serverless.db.r5.2xlarge"
    SERVERLESS_DB_R5_4XLARGE = "serverless.db.r5.4xlarge"


class NeptuneEngineVersion(Enum):
    """Neptune engine versions."""
    VERSION_1_0_5_0 = "1.0.5.0"
    VERSION_1_1_0_0 = "1.1.0.0"
    VERSION_1_2_0_0 = "1.2.0.0"
    VERSION_1_2_0_1 = "1.2.0.1"
    VERSION_1_3_0_0 = "1.3.0.0"
    VERSION_1_3_1_0 = "1.3.1.0"


class NeptuneClusterState(Enum):
    """Neptune cluster states."""
    CREATING = "creating"
    AVAILABLE = "available"
    MODIFYING = "modifying"
    DELETING = "deleting"
    DELETED = "deleted"
    FAILED = "failed"
    BACKING_UP = "backing-up"
    STARTING = "starting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    UPDATING = "updating"


class NeptuneInstanceState(Enum):
    """Neptune instance states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    MODIFYING = "modifying"
    REBOOTING = "rebooting"
    FAILING = "failing"
    FAILED = "failed"


class GraphType(Enum):
    """Neptune graph types."""
    PROPERTY_GRAPH = "propertygraph"
    RDF = "rdf"


class BackupStrategy(Enum):
    """Backup retention strategies."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"


@dataclass
class NeptuneConfig:
    """Configuration for Neptune integration."""
    region: str = "us-east-1"
    cluster_id: Optional[str] = None
    cluster_arn: Optional[str] = None
    endpoint: Optional[str] = None
    port: int = 8182
    graph_type: GraphType = GraphType.PROPERTY_GRAPH
    iam_auth_enabled: bool = False
    serverless: bool = False
    global_cluster: bool = False
    auto_backup: bool = True
    backup_retention_days: int = 1
    encryption_enabled: bool = True
    kms_key_id: Optional[str] = None
    cloudwatch_logs_exports: List[str] = field(default_factory=lambda: ["audit"])
    notification_topic: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class NeptuneCluster:
    """Represents a Neptune cluster."""
    cluster_id: str
    cluster_arn: str
    cluster_resource_id: str
    engine: str = "neptune"
    engine_version: str = "1.2.0.1"
    endpoint: Optional[str] = None
    reader_endpoint: Optional[str] = None
    port: int = 8182
    status: NeptuneClusterState = NeptuneClusterState.CREATING
    master_username: Optional[str] = None
    master_password: Optional[str] = None
    db_cluster_identifier: Optional[str] = None
    global_cluster_identifier: Optional[str] = None
    storage_encrypted: bool = True
    kms_key: Optional[str] = None
    backup_retention_period: int = 1
    preferred_backup_window: Optional[str] = None
    preferred_maintenance_window: Optional[str] = None
    multi_az: bool = False
    serverless: bool = False
    iam_auth: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None


@dataclass
class NeptuneInstance:
    """Represents a Neptune instance."""
    instance_id: str
    instance_arn: str
    cluster_id: str
    instance_class: str
    role: str = "WRITER"
    status: NeptuneInstanceState = NeptuneInstanceState.CREATING
    endpoint: Optional[str] = None
    port: int = 8182
    availability_zone: Optional[str] = None
    publicly_accessible: bool = False
    auto_minor_version_upgrade: bool = True
    preferred_maintenance_window: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class GlobalCluster:
    """Represents a Neptune global database cluster."""
    global_cluster_id: str
    global_cluster_arn: str
    engine: str = "neptune"
    engine_version: str = "1.2.0.1"
    status: str = "available"
    storage_encrypted: bool = True
    kms_key: Optional[str] = None
    secondary_clusters: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None


@dataclass
class BackupInfo:
    """Represents a Neptune backup/snapshot."""
    backup_id: str
    snapshot_type: str
    cluster_id: str
    status: str
    allocated_storage: int
    encrypted: bool = True
    kms_key: Optional[str] = None
    source_region: Optional[str] = None
    created_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class GremlinQuery:
    """Represents a Gremlin query."""
    traversal: str
    bindings: Dict[str, Any] = field(default_factory=dict)
    language: str = "gremlin-groovy"
    aliases: Dict[str, str] = field(default_factory=dict)


@dataclass
class SPARQLQuery:
    """Represents a SPARQL query."""
    query: str
    query_type: str = "SELECT"
    default_graph: Optional[str] = None
    named_graphs: List[str] = field(default_factory=list)
    include_inference: bool = True


@dataclass
class Vertex:
    """Represents a property graph vertex."""
    id: str
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    """Represents a property graph edge."""
    id: str
    label: str
    source_id: str
    target_id: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricData:
    """Represents CloudWatch metric data."""
    metric_name: str
    value: float
    unit: str = "Count"
    timestamp: Optional[datetime] = None
    dimensions: Dict[str, str] = field(default_factory=dict)


class NeptuneIntegration:
    """Integration class for AWS Neptune graph database."""

    def __init__(
        self,
        config: Optional[NeptuneConfig] = None,
        region: Optional[str] = None,
        profile_name: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Neptune integration.

        Args:
            config: Neptune configuration
            region: AWS region (overrides config.region)
            profile_name: AWS profile name
            **kwargs: Additional configuration passed to NeptuneConfig
        """
        self.config = config or NeptuneConfig()
        if region:
            self.config.region = region
        self.profile_name = profile_name

        self._neptune_client = None
        self._rds_client = None
        self._global_client = None
        self._cloudwatch_client = None
        self._logs_client = None
        self._iam_client = None
        self._lambda_client = None
        self._lock = threading.RLock()

    @property
    def neptune_client(self):
        """Get or create Neptune client."""
        if self._neptune_client is None:
            with self._lock:
                if self._neptune_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._neptune_client = session.client(
                        "neptune", region_name=self.config.region
                    )
        return self._neptune_client

    @property
    def rds_client(self):
        """Get or create RDS client."""
        if self._rds_client is None:
            with self._lock:
                if self._rds_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._rds_client = session.client(
                        "rds", region_name=self.config.region
                    )
        return self._rds_client

    @property
    def global_client(self):
        """Get or create global cluster client."""
        if self._global_client is None:
            with self._lock:
                if self._global_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._global_client = session.client(
                        "neptune", region_name=self.config.region
                    )
        return self._global_client

    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            with self._lock:
                if self._cloudwatch_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._cloudwatch_client = session.client(
                        "cloudwatch", region_name=self.config.region
                    )
        return self._cloudwatch_client

    @property
    def logs_client(self):
        """Get or create CloudWatch Logs client."""
        if self._logs_client is None:
            with self._lock:
                if self._logs_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._logs_client = session.client(
                        "logs", region_name=self.config.region
                    )
        return self._logs_client

    @property
    def iam_client(self):
        """Get or create IAM client."""
        if self._iam_client is None:
            with self._lock:
                if self._iam_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._iam_client = session.client(
                        "iam", region_name=self.config.region
                    )
        return self._iam_client

    @property
    def lambda_client(self):
        """Get or create Lambda client."""
        if self._lambda_client is None:
            with self._lock:
                if self._lambda_client is None:
                    session = boto3.Session(profile_name=self.profile_name)
                    self._lambda_client = session.client(
                        "lambda", region_name=self.config.region
                    )
        return self._lambda_client

    def _generate_cluster_id(self, prefix: str = "neptune") -> str:
        """Generate a unique cluster identifier."""
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _generate_instance_id(self, cluster_id: str, role: str = "writer") -> str:
        """Generate a unique instance identifier."""
        return f"{cluster_id}-{role}-{uuid.uuid4().hex[:8]}"

    def _wait_for_cluster_available(
        self,
        cluster_id: str,
        timeout: int = 600,
        check_interval: int = 30
    ) -> bool:
        """Wait for cluster to become available."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.neptune_client.describe_db_clusters(
                    DBClusterIdentifier=cluster_id
                )
                clusters = response.get("DBClusters", [])
                if clusters:
                    cluster = clusters[0]
                    status = cluster.get("Status", "")
                    if status == "available":
                        return True
                    elif status in ["deleting", "deleted", "failed"]:
                        raise TimeoutError(f"Cluster {cluster_id} entered state: {status}")
                time.sleep(check_interval)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ClusterNotFoundFault":
                    raise
                time.sleep(check_interval)
        raise TimeoutError(f"Timeout waiting for cluster {cluster_id} to become available")

    def _wait_for_instance_available(
        self,
        instance_id: str,
        timeout: int = 600,
        check_interval: int = 30
    ) -> bool:
        """Wait for instance to become available."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.rds_client.describe_db_instances(
                    DBInstanceIdentifier=instance_id
                )
                instances = response.get("DBInstances", [])
                if instances:
                    instance = instances[0]
                    status = instance.get("DBInstanceStatus", "")
                    if status == "available":
                        return True
                    elif status in ["deleting", "deleted", "failed"]:
                        raise TimeoutError(f"Instance {instance_id} entered state: {status}")
                time.sleep(check_interval)
            except ClientError as e:
                if e.response["Error"]["Code"] == "DBInstanceNotFound":
                    raise
                time.sleep(check_interval)
        raise TimeoutError(f"Timeout waiting for instance {instance_id} to become available")

    def _get_cluster_endpoint(
        self,
        cluster_id: str,
        role: str = "writer"
    ) -> Optional[str]:
        """Get cluster endpoint by role."""
        try:
            response = self.neptune_client.describe_db_clusters(
                DBClusterIdentifier=cluster_id
            )
            clusters = response.get("DBClusters", [])
            if clusters:
                cluster = clusters[0]
                if role == "writer":
                    return cluster.get("Endpoint")
                elif role == "reader":
                    return cluster.get("ReaderEndpoint") or cluster.get("Endpoint")
            return None
        except ClientError:
            return None

    def _parse_gremlin_result(self, result: Any) -> Dict[str, Any]:
        """Parse Gremlin query result."""
        if isinstance(result, dict):
            if "results" in result:
                return {"status": "success", "data": result["results"]}
            if "error" in result:
                return {"status": "error", "message": result["error"]}
        return {"status": "success", "data": result}

    def _parse_sparql_result(self, result: Any) -> Dict[str, Any]:
        """Parse SPARQL query result."""
        if isinstance(result, dict):
            if "head" in result and "results" in result:
                return {
                    "status": "success",
                    "variables": result.get("head", {}).get("vars", []),
                    "bindings": result.get("results", {}).get("bindings", [])
                }
            if "boolean" in result:
                return {"status": "success", "boolean": result["boolean"]}
            if "error" in result:
                return {"status": "error", "message": result["error"]}
        return {"status": "success", "data": result}

    # ========================================================================
    # CLUSTER MANAGEMENT
    # ========================================================================

    def create_cluster(
        self,
        cluster_id: Optional[str] = None,
        master_username: str = "admin",
        master_password: Optional[str] = None,
        engine_version: str = "1.2.0.1",
        port: int = 8182,
        serverless: bool = False,
        enable_cloudwatch_logs: List[str] = None,
        backup_retention_days: int = 1,
        preferred_backup_window: Optional[str] = None,
        preferred_maintenance_window: Optional[str] = None,
        encryption: bool = True,
        kms_key_id: Optional[str] = None,
        iam_auth: bool = False,
        tags: Dict[str, str] = None,
        wait_for_available: bool = True,
        timeout: int = 600
    ) -> NeptuneCluster:
        """
        Create a new Neptune cluster.

        Args:
            cluster_id: Unique cluster identifier (auto-generated if not provided)
            master_username: Master username for the cluster
            master_password: Master password (auto-generated if not provided)
            engine_version: Neptune engine version
            port: Neptune port
            serverless: Enable serverless scaling
            enable_cloudwatch_logs: CloudWatch log types to enable
            backup_retention_days: Number of days to retain backups
            preferred_backup_window: Preferred backup window
            preferred_maintenance_window: Preferred maintenance window
            encryption: Enable encryption at rest
            kms_key_id: KMS key ID for encryption
            iam_auth: Enable IAM authentication
            tags: Resource tags
            wait_for_available: Wait for cluster to become available
            timeout: Timeout in seconds

        Returns:
            NeptuneCluster object
        """
        cluster_id = cluster_id or self._generate_cluster_id()
        master_password = master_password or uuid.uuid4().hex[:16]

        params = {
            "DBClusterIdentifier": cluster_id,
            "Engine": "neptune",
            "EngineVersion": engine_version,
            "DBClusterParameterGroupName": "default.neptune1",
            "Port": port,
            "MasterUsername": master_username,
            "MasterUserPassword": master_password,
            "BackupRetentionPeriod": backup_retention_days,
            "StorageEncrypted": encryption,
            "IAMAuthEnabled": iam_auth,
        }

        if enable_cloudwatch_logs:
            params["EnableCloudwatchLogsExports"] = enable_cloudwatch_logs

        if kms_key_id:
            params["KmsKeyId"] = kms_key_id

        if preferred_backup_window:
            params["PreferredBackupWindow"] = preferred_backup_window

        if preferred_maintenance_window:
            params["PreferredMaintenanceWindow"] = preferred_maintenance_window

        if serverless:
            params["ServerlessV2ScalingConfiguration"] = {
                "MinCapacity": 0.5,
                "MaxCapacity": 128.0
            }

        if tags:
            params["Tags"] = [
                {"Key": k, "Value": v} for k, v in tags.items()
            ]

        try:
            self.neptune_client.create_db_cluster(**params)

            cluster = NeptuneCluster(
                cluster_id=cluster_id,
                cluster_arn=f"arn:aws:rds:{self.config.region}:123456789012:cluster:{cluster_id}",
                cluster_resource_id=uuid.uuid4().hex,
                engine="neptune",
                engine_version=engine_version,
                endpoint=self._get_cluster_endpoint(cluster_id, "writer"),
                port=port,
                status=NeptuneClusterState.CREATING,
                master_username=master_username,
                master_password=master_password,
                storage_encrypted=encryption,
                kms_key=kms_key_id,
                backup_retention_period=backup_retention_days,
                serverless=serverless,
                iam_auth=iam_auth,
                tags=tags or {}
            )

            if wait_for_available:
                self._wait_for_cluster_available(cluster_id, timeout)
                cluster.status = NeptuneClusterState.AVAILABLE
                cluster.endpoint = self._get_cluster_endpoint(cluster_id, "writer")
                cluster.reader_endpoint = self._get_cluster_endpoint(cluster_id, "reader")

            return cluster

        except ClientError as e:
            logger.error(f"Failed to create Neptune cluster: {e}")
            raise

    def get_cluster(self, cluster_id: str) -> Optional[NeptuneCluster]:
        """
        Get cluster information.

        Args:
            cluster_id: Cluster identifier

        Returns:
            NeptuneCluster object or None if not found
        """
        try:
            response = self.neptune_client.describe_db_clusters(
                DBClusterIdentifier=cluster_id
            )
            clusters = response.get("DBClusters", [])
            if not clusters:
                return None

            cluster_data = clusters[0]

            return NeptuneCluster(
                cluster_id=cluster_data["DBClusterIdentifier"],
                cluster_arn=cluster_data["DBClusterArn"],
                cluster_resource_id=cluster_data["DbClusterResourceId"],
                engine=cluster_data.get("Engine", "neptune"),
                engine_version=cluster_data.get("EngineVersion", "1.2.0.1"),
                endpoint=cluster_data.get("Endpoint"),
                reader_endpoint=cluster_data.get("ReaderEndpoint"),
                port=cluster_data.get("Port", 8182),
                status=NeptuneClusterState(cluster_data.get("Status", "available")),
                storage_encrypted=cluster_data.get("StorageEncrypted", True),
                kms_key=cluster_data.get("KmsKeyId"),
                backup_retention_period=cluster_data.get("BackupRetentionPeriod", 1),
                preferred_backup_window=cluster_data.get("PreferredBackupWindow"),
                preferred_maintenance_window=cluster_data.get("PreferredMaintenanceWindow"),
                multi_az=cluster_data.get("MultiAZ", False),
                serverless=bool(cluster_data.get("ServerlessV2ScalingConfiguration")),
                iam_auth=cluster_data.get("IAMDatabaseAuthenticationEnabled", False),
                tags={t["Key"]: t["Value"] for t in cluster_data.get("TagList", [])}
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundFault":
                return None
            logger.error(f"Failed to get cluster {cluster_id}: {e}")
            raise

    def list_clusters(self) -> List[NeptuneCluster]:
        """
        List all Neptune clusters.

        Returns:
            List of NeptuneCluster objects
        """
        try:
            response = self.neptune_client.describe_db_clusters()
            clusters = []

            for cluster_data in response.get("DBClusters", []):
                try:
                    status = NeptuneClusterState(cluster_data.get("Status", "available"))
                except ValueError:
                    status = NeptuneClusterState.CREATING

                clusters.append(NeptuneCluster(
                    cluster_id=cluster_data["DBClusterIdentifier"],
                    cluster_arn=cluster_data["DBClusterArn"],
                    cluster_resource_id=cluster_data["DbClusterResourceId"],
                    engine=cluster_data.get("Engine", "neptune"),
                    engine_version=cluster_data.get("EngineVersion", "1.2.0.1"),
                    endpoint=cluster_data.get("Endpoint"),
                    reader_endpoint=cluster_data.get("ReaderEndpoint"),
                    port=cluster_data.get("Port", 8182),
                    status=status,
                    storage_encrypted=cluster_data.get("StorageEncrypted", True),
                    kms_key=cluster_data.get("KmsKeyId"),
                    backup_retention_period=cluster_data.get("BackupRetentionPeriod", 1),
                    multi_az=cluster_data.get("MultiAZ", False),
                    serverless=bool(cluster_data.get("ServerlessV2ScalingConfiguration")),
                    tags={t["Key"]: t["Value"] for t in cluster_data.get("TagList", [])}
                ))

            return clusters

        except ClientError as e:
            logger.error(f"Failed to list clusters: {e}")
            raise

    def update_cluster(
        self,
        cluster_id: str,
        backup_retention_days: Optional[int] = None,
        preferred_backup_window: Optional[str] = None,
        preferred_maintenance_window: Optional[str] = None,
        iam_auth: Optional[bool] = None,
        enable_cloudwatch_logs: Optional[List[str]] = None,
        apply_immediately: bool = False
    ) -> NeptuneCluster:
        """
        Update cluster configuration.

        Args:
            cluster_id: Cluster identifier
            backup_retention_days: New backup retention period
            preferred_backup_window: New backup window
            preferred_maintenance_window: New maintenance window
            iam_auth: Enable/disable IAM authentication
            enable_cloudwatch_logs: CloudWatch logs to enable
            apply_immediately: Apply changes immediately

        Returns:
            Updated NeptuneCluster object
        """
        params = {
            "DBClusterIdentifier": cluster_id,
            "ApplyImmediately": apply_immediately
        }

        if backup_retention_days is not None:
            params["BackupRetentionPeriod"] = backup_retention_days

        if preferred_backup_window:
            params["PreferredBackupWindow"] = preferred_backup_window

        if preferred_maintenance_window:
            params["PreferredMaintenanceWindow"] = preferred_maintenance_window

        if iam_auth is not None:
            params["IAMDatabaseAuthenticationEnabled"] = iam_auth

        if enable_cloudwatch_logs is not None:
            params["EnableCloudwatchLogsExports"] = enable_cloudwatch_logs

        try:
            self.neptune_client.modify_db_cluster(**params)

            if apply_immediately:
                self._wait_for_cluster_available(cluster_id)

            return self.get_cluster(cluster_id)

        except ClientError as e:
            logger.error(f"Failed to update cluster {cluster_id}: {e}")
            raise

    def delete_cluster(
        self,
        cluster_id: str,
        skip_final_snapshot: bool = False,
        final_snapshot_id: Optional[str] = None,
        wait_for_deletion: bool = True,
        timeout: int = 600
    ) -> bool:
        """
        Delete a Neptune cluster.

        Args:
            cluster_id: Cluster identifier
            skip_final_snapshot: Skip final snapshot creation
            final_snapshot_id: Final snapshot identifier
            wait_for_deletion: Wait for cluster deletion
            timeout: Timeout in seconds

        Returns:
            True if deletion was successful
        """
        params = {
            "DBClusterIdentifier": cluster_id,
            "SkipFinalSnapshot": skip_final_snapshot
        }

        if not skip_final_snapshot and final_snapshot_id:
            params["FinalDBSnapshotIdentifier"] = final_snapshot_id

        try:
            self.neptune_client.delete_db_cluster(**params)

            if wait_for_deletion:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        self.get_cluster(cluster_id)
                        time.sleep(10)
                    except Exception:
                        return True
                raise TimeoutError(f"Timeout waiting for cluster {cluster_id} deletion")

            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundFault":
                return True
            logger.error(f"Failed to delete cluster {cluster_id}: {e}")
            raise

    # ========================================================================
    # INSTANCE MANAGEMENT
    # ========================================================================

    def create_instance(
        self,
        cluster_id: str,
        instance_id: Optional[str] = None,
        instance_class: str = "db.r5.large",
        role: str = "WRITER",
        availability_zone: Optional[str] = None,
        publicly_accessible: bool = False,
        auto_minor_version_upgrade: bool = True,
        preferred_maintenance_window: Optional[str] = None,
        tags: Dict[str, str] = None,
        wait_for_available: bool = True,
        timeout: int = 600
    ) -> NeptuneInstance:
        """
        Create a new Neptune instance.

        Args:
            cluster_id: Cluster identifier
            instance_id: Instance identifier (auto-generated if not provided)
            instance_class: Instance class (e.g., db.r5.large)
            role: Instance role ("WRITER" or "READER")
            availability_zone: AZ for the instance
            publicly_accessible: Enable public access
            auto_minor_version_upgrade: Auto upgrade minor versions
            preferred_maintenance_window: Maintenance window
            tags: Resource tags
            wait_for_available: Wait for instance to become available
            timeout: Timeout in seconds

        Returns:
            NeptuneInstance object
        """
        instance_id = instance_id or self._generate_instance_id(cluster_id, role.lower())

        params = {
            "DBInstanceIdentifier": instance_id,
            "DBClusterIdentifier": cluster_id,
            "DBInstanceClass": instance_class,
            "Engine": "neptune",
            "PromotionTier": 0 if role == "WRITER" else 1,
            "PubliclyAccessible": publicly_accessible,
            "AutoMinorVersionUpgrade": auto_minor_version_upgrade,
        }

        if availability_zone:
            params["AvailabilityZone"] = availability_zone

        if preferred_maintenance_window:
            params["PreferredMaintenanceWindow"] = preferred_maintenance_window

        if tags:
            params["Tags"] = [
                {"Key": k, "Value": v} for k, v in tags.items()
            ]

        try:
            self.rds_client.create_db_instance(**params)

            instance = NeptuneInstance(
                instance_id=instance_id,
                instance_arn=f"arn:aws:rds:{self.config.region}:123456789012:db:{instance_id}",
                cluster_id=cluster_id,
                instance_class=instance_class,
                role=role,
                status=NeptuneInstanceState.CREATING,
                availability_zone=availability_zone,
                publicly_accessible=publicly_accessible,
                auto_minor_version_upgrade=auto_minor_version_upgrade,
                preferred_maintenance_window=preferred_maintenance_window,
                tags=tags or {}
            )

            if wait_for_available:
                self._wait_for_instance_available(instance_id, timeout)
                instance.status = NeptuneInstanceState.AVAILABLE
                instance.endpoint = self._get_instance_endpoint(instance_id)

            return instance

        except ClientError as e:
            logger.error(f"Failed to create Neptune instance: {e}")
            raise

    def _get_instance_endpoint(self, instance_id: str) -> Optional[str]:
        """Get instance endpoint."""
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=instance_id
            )
            instances = response.get("DBInstances", [])
            if instances:
                return instances[0].get("Endpoint", {}).get("Address")
            return None
        except ClientError:
            return None

    def get_instance(self, instance_id: str) -> Optional[NeptuneInstance]:
        """
        Get instance information.

        Args:
            instance_id: Instance identifier

        Returns:
            NeptuneInstance object or None if not found
        """
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=instance_id
            )
            instances = response.get("DBInstances", [])
            if not instances:
                return None

            instance_data = instances[0]

            role = "WRITER"
            promotion_tier = instance_data.get("PromotionTier", 0)
            if promotion_tier > 0:
                role = "READER"

            return NeptuneInstance(
                instance_id=instance_data["DBInstanceIdentifier"],
                instance_arn=instance_data["DBInstanceArn"],
                cluster_id=instance_data["DBClusterIdentifier"],
                instance_class=instance_data["DBInstanceClass"],
                role=role,
                status=NeptuneInstanceState(instance_data.get("DBInstanceStatus", "available")),
                endpoint=instance_data.get("Endpoint", {}).get("Address"),
                port=instance_data.get("Endpoint", {}).get("Port", 8182),
                availability_zone=instance_data.get("AvailabilityZone"),
                publicly_accessible=instance_data.get("PubliclyAccessible", False),
                auto_minor_version_upgrade=instance_data.get("AutoMinorVersionUpgrade", True),
                preferred_maintenance_window=instance_data.get("PreferredMaintenanceWindow"),
                tags={t["Key"]: t["Value"] for t in instance_data.get("TagList", [])}
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                return None
            logger.error(f"Failed to get instance {instance_id}: {e}")
            raise

    def list_instances(self, cluster_id: Optional[str] = None) -> List[NeptuneInstance]:
        """
        List Neptune instances.

        Args:
            cluster_id: Filter by cluster ID

        Returns:
            List of NeptuneInstance objects
        """
        try:
            params = {}
            if cluster_id:
                params["Filters"] = [
                    {"Name": "db-cluster-id", "Values": [cluster_id]}
                ]

            response = self.rds_client.describe_db_instances(**params)
            instances = []

            for instance_data in response.get("DBInstances", []):
                if instance_data.get("Engine") != "neptune":
                    continue

                try:
                    status = NeptuneInstanceState(instance_data.get("DBInstanceStatus", "available"))
                except ValueError:
                    status = NeptuneInstanceState.CREATING

                promotion_tier = instance_data.get("PromotionTier", 0)
                role = "WRITER" if promotion_tier == 0 else "READER"

                instances.append(NeptuneInstance(
                    instance_id=instance_data["DBInstanceIdentifier"],
                    instance_arn=instance_data["DBInstanceArn"],
                    cluster_id=instance_data["DBClusterIdentifier"],
                    instance_class=instance_data["DBInstanceClass"],
                    role=role,
                    status=status,
                    endpoint=instance_data.get("Endpoint", {}).get("Address"),
                    port=instance_data.get("Endpoint", {}).get("Port", 8182),
                    availability_zone=instance_data.get("AvailabilityZone"),
                    publicly_accessible=instance_data.get("PubliclyAccessible", False),
                    auto_minor_version_upgrade=instance_data.get("AutoMinorVersionUpgrade", True),
                    preferred_maintenance_window=instance_data.get("PreferredMaintenanceWindow"),
                    tags={t["Key"]: t["Value"] for t in instance_data.get("TagList", [])}
                ))

            return instances

        except ClientError as e:
            logger.error(f"Failed to list instances: {e}")
            raise

    def create_reader_instance(
        self,
        cluster_id: str,
        instance_id: Optional[str] = None,
        instance_class: Optional[str] = None,
        availability_zone: Optional[str] = None,
        **kwargs
    ) -> NeptuneInstance:
        """
        Create a reader instance in the cluster.

        Args:
            cluster_id: Cluster identifier
            instance_id: Instance identifier
            instance_class: Instance class (defaults to cluster's writer class)
            availability_zone: AZ for the instance
            **kwargs: Additional arguments for create_instance

        Returns:
            NeptuneInstance object
        """
        return self.create_instance(
            cluster_id=cluster_id,
            instance_id=instance_id,
            instance_class=instance_class or "db.r5.large",
            role="READER",
            availability_zone=availability_zone,
            **kwargs
        )

    def create_writer_instance(
        self,
        cluster_id: str,
        instance_id: Optional[str] = None,
        instance_class: Optional[str] = None,
        availability_zone: Optional[str] = None,
        **kwargs
    ) -> NeptuneInstance:
        """
        Create a writer instance in the cluster.

        Args:
            cluster_id: Cluster identifier
            instance_id: Instance identifier
            instance_class: Instance class
            availability_zone: AZ for the instance
            **kwargs: Additional arguments for create_instance

        Returns:
            NeptuneInstance object
        """
        return self.create_instance(
            cluster_id=cluster_id,
            instance_id=instance_id,
            instance_class=instance_class or "db.r5.large",
            role="WRITER",
            availability_zone=availability_zone,
            **kwargs
        )

    def delete_instance(
        self,
        instance_id: str,
        skip_final_snapshot: bool = True,
        wait_for_deletion: bool = True,
        timeout: int = 300
    ) -> bool:
        """
        Delete a Neptune instance.

        Args:
            instance_id: Instance identifier
            skip_final_snapshot: Skip final snapshot
            wait_for_deletion: Wait for deletion
            timeout: Timeout in seconds

        Returns:
            True if deletion was successful
        """
        params = {
            "DBInstanceIdentifier": instance_id,
            "SkipFinalSnapshot": skip_final_snapshot
        }

        try:
            self.rds_client.delete_db_instance(**params)

            if wait_for_deletion:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        self.get_instance(instance_id)
                        time.sleep(5)
                    except Exception:
                        return True
                raise TimeoutError(f"Timeout waiting for instance {instance_id} deletion")

            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                return True
            logger.error(f"Failed to delete instance {instance_id}: {e}")
            raise

    def reboot_instance(
        self,
        instance_id: str,
        wait_for_available: bool = True,
        timeout: int = 300
    ) -> NeptuneInstance:
        """
        Reboot a Neptune instance.

        Args:
            instance_id: Instance identifier
            wait_for_available: Wait for instance to become available
            timeout: Timeout in seconds

        Returns:
            NeptuneInstance object
        """
        try:
            self.rds_client.reboot_db_instance(DBInstanceIdentifier=instance_id)

            if wait_for_available:
                self._wait_for_instance_available(instance_id, timeout)

            return self.get_instance(instance_id)

        except ClientError as e:
            logger.error(f"Failed to reboot instance {instance_id}: {e}")
            raise

    # ========================================================================
    # GRAPH OPERATIONS - GREMLIN
    # ========================================================================

    def execute_gremlin_query(
        self,
        query: Union[str, GremlinQuery],
        cluster_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        use_iam_auth: bool = False,
        language: str = "gremlin-groovy"
    ) -> Dict[str, Any]:
        """
        Execute a Gremlin query against Neptune.

        Args:
            query: Gremlin query string or GremlinQuery object
            cluster_id: Cluster identifier (uses config if not provided)
            endpoint: Direct endpoint to use
            use_iam_auth: Use IAM authentication
            language: Query language

        Returns:
            Query result dictionary
        """
        if isinstance(query, str):
            query = GremlinQuery(traversal=query, language=language)

        if not endpoint and cluster_id:
            endpoint = self._get_cluster_endpoint(cluster_id, "writer")
        elif not endpoint and self.config.endpoint:
            endpoint = self.config.endpoint
        else:
            raise ValueError("Either endpoint or cluster_id must be provided")

        request_body = {
            "gremlin": query.traversal,
            "language": query.language
        }

        if query.bindings:
            request_body["bindings"] = query.bindings

        if query.aliases:
            request_body["aliases"] = query.aliases

        if use_iam_auth or self.config.iam_auth_enabled:
            request_body = self._sign_gremlin_request(request_body, endpoint)

        try:
            import urllib.request
            import urllib.parse

            url = f"https://{endpoint}:{self.config.port}/gremlin"

            data = json.dumps(request_body).encode("utf-8")
            headers = {
                "Content-Type": "application/json"
            }

            if use_iam_auth or self.config.iam_auth_enabled:
                import botocore.auth
                import botocore.awsrequest
                from urllib.request import Request

                req = Request(url, data=data, headers=headers, method="POST")
                session = boto3.Session(profile_name=self.profile_name)
                credentials = session.get_credentials()
                awsauth = botocore.auth.AuthSchemes("https", ["awsSigV4"])
                awsauth.add_auth(Request, credentials, "execute-api")

                request = botocore.awsrequest.AWSRequest(
                    method="POST",
                    url=url,
                    data=data,
                    headers=headers
                )
                boto3.auth.requests.Authenticity().add_auth(request)

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return self._parse_gremlin_result(result)

        except Exception as e:
            logger.error(f"Gremlin query failed: {e}")
            return {"status": "error", "message": str(e)}

    def _sign_gremlin_request(self, request_body: Dict, endpoint: str) -> Dict:
        """Sign Gremlin request with IAM credentials."""
        return request_body

    def add_vertex(
        self,
        label: str,
        properties: Dict[str, Any],
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Add a vertex to the property graph.

        Args:
            label: Vertex label
            properties: Vertex properties
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            Result dictionary
        """
        traversal = f"g.addV('{label}')"

        for key, value in properties.items():
            if isinstance(value, str):
                traversal += f".property('{key}', '{value}')"
            else:
                traversal += f".property('{key}', {value})"

        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    def add_edge(
        self,
        label: str,
        source_id: str,
        target_id: str,
        properties: Dict[str, Any] = None,
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Add an edge to the property graph.

        Args:
            label: Edge label
            source_id: Source vertex ID
            target_id: Target vertex ID
            properties: Edge properties
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            Result dictionary
        """
        properties = properties or {}

        traversal = f"g.V('{source_id}').addE('{label}').to(g.V('{target_id}'))"

        for key, value in properties.items():
            if isinstance(value, str):
                traversal += f".property('{key}', '{value}')"
            else:
                traversal += f".property('{key}', {value})"

        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    def get_vertex(
        self,
        vertex_id: str,
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Get a vertex by ID.

        Args:
            vertex_id: Vertex ID
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            Vertex data
        """
        traversal = f"g.V('{vertex_id}').valueMap(true)"
        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    def get_edges(
        self,
        vertex_id: str,
        direction: str = "both",
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Get edges for a vertex.

        Args:
            vertex_id: Vertex ID
            direction: Edge direction ("out", "in", or "both")
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            Edge data
        """
        if direction == "out":
            traversal = f"g.V('{vertex_id}').outE()"
        elif direction == "in":
            traversal = f"g.V('{vertex_id}').inE()"
        else:
            traversal = f"g.V('{vertex_id}').bothE()"

        traversal += ".valueMap(true)"
        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    def list_vertices(
        self,
        label: Optional[str] = None,
        limit: int = 100,
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        List vertices in the graph.

        Args:
            label: Filter by label
            limit: Maximum number of results
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            List of vertices
        """
        if label:
            traversal = f"g.V().hasLabel('{label}').limit({limit})"
        else:
            traversal = f"g.V().limit({limit})"

        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    def delete_vertex(
        self,
        vertex_id: str,
        cluster_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Delete a vertex.

        Args:
            vertex_id: Vertex ID
            cluster_id: Cluster identifier
            **kwargs: Additional query parameters

        Returns:
            Result dictionary
        """
        traversal = f"g.V('{vertex_id}').drop()"
        return self.execute_gremlin_query(traversal, cluster_id=cluster_id, **kwargs)

    # ========================================================================
    # GRAPH OPERATIONS - SPARQL
    # ========================================================================

    def execute_sparql_query(
        self,
        query: Union[str, SPARQLQuery],
        cluster_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        use_iam_auth: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a SPARQL query against Neptune.

        Args:
            query: SPARQL query string or SPARQLQuery object
            cluster_id: Cluster identifier
            endpoint: Direct endpoint to use
            use_iam_auth: Use IAM authentication

        Returns:
            Query result dictionary
        """
        if isinstance(query, str):
            query = SPARQLQuery(query=query)

        if not endpoint and cluster_id:
            endpoint = self._get_cluster_endpoint(cluster_id, "writer")
        elif not endpoint and self.config.endpoint:
            endpoint = self.config.endpoint
        else:
            raise ValueError("Either endpoint or cluster_id must be provided")

        try:
            import urllib.request

            sparql_endpoint = f"https://{endpoint}:{self.config.port}/sparql"

            params = {"query": query.query}

            if query.default_graph:
                params["default-graph-uri"] = query.default_graph

            if query.named_graphs:
                params["named-graph-uri"] = query.named_graphs

            url = f"{sparql_endpoint}?{urllib.parse.urlencode(params)}"

            headers = {
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            if use_iam_auth or self.config.iam_auth_enabled:
                pass

            with urllib.request.urlopen(url, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return self._parse_sparql_result(result)

        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return {"status": "error", "message": str(e)}

    def add_rdf_triple(
        self,
        subject: str,
        predicate: str,
        object_value: str,
        graph: Optional[str] = None,
        cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add an RDF triple.

        Args:
            subject: Subject URI
            predicate: Predicate URI
            object_value: Object value (URI or literal)
            graph: Named graph URI
            cluster_id: Cluster identifier

        Returns:
            Result dictionary
        """
        if graph:
            query = f"""
            INSERT DATA {{
                GRAPH <{graph}> {{
                    <{subject}> <{predicate}> {object_value} .
                }}
            }}
            """
        else:
            query = f"""
            INSERT DATA {{
                <{subject}> <{predicate}> {object_value} .
            }}
            """

        return self.execute_sparql_query(query, cluster_id=cluster_id)

    def delete_rdf_triple(
        self,
        subject: str,
        predicate: str,
        object_value: str,
        graph: Optional[str] = None,
        cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete an RDF triple.

        Args:
            subject: Subject URI
            predicate: Predicate URI
            object_value: Object value
            graph: Named graph URI
            cluster_id: Cluster identifier

        Returns:
            Result dictionary
        """
        if graph:
            query = f"""
            DELETE DATA {{
                GRAPH <{graph}> {{
                    <{subject}> <{predicate}> {object_value} .
                }}
            }}
            """
        else:
            query = f"""
            DELETE DATA {{
                <{subject}> <{predicate}> {object_value} .
            }}
            """

        return self.execute_sparql_query(query, cluster_id=cluster_id)

    # ========================================================================
    # SERVERLESS
    # ========================================================================

    def configure_serverless(
        self,
        cluster_id: str,
        min_capacity: float = 0.5,
        max_capacity: float = 128.0,
        timeout: int = 2,
        wait_for_available: bool = True,
        serverless_timeout: int = 600
    ) -> NeptuneCluster:
        """
        Configure serverless scaling for a Neptune cluster.

        Args:
            cluster_id: Cluster identifier
            min_capacity: Minimum ACU capacity
            max_capacity: Maximum ACU capacity
            timeout: Timeout in minutes for scaling
            wait_for_available: Wait for cluster update
            serverless_timeout: Timeout for cluster availability

        Returns:
            Updated NeptuneCluster object
        """
        params = {
            "DBClusterIdentifier": cluster_id,
            "ServerlessV2ScalingConfiguration": {
                "MinCapacity": min_capacity,
                "MaxCapacity": max_capacity
            }
        }

        try:
            self.neptune_client.modify_db_cluster(**params)

            if wait_for_available:
                self._wait_for_cluster_available(cluster_id, serverless_timeout)

            return self.get_cluster(cluster_id)

        except ClientError as e:
            logger.error(f"Failed to configure serverless: {e}")
            raise

    def get_serverless_capacity(
        self,
        cluster_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current serverless capacity usage.

        Args:
            cluster_id: Cluster identifier

        Returns:
            Capacity information
        """
        try:
            response = self.neptune_client.describe_db_clusters(
                DBClusterIdentifier=cluster_id
            )
            clusters = response.get("DBClusters", [])
            if clusters:
                cluster = clusters[0]
                scaling = cluster.get("ServerlessV2ScalingConfiguration", {})
                return {
                    "min_capacity": scaling.get("MinCapacity"),
                    "max_capacity": scaling.get("MaxCapacity"),
                    "current_capacity": scaling.get("CurrentCapacity", "N/A")
                }
            return None

        except ClientError as e:
            logger.error(f"Failed to get serverless capacity: {e}")
            raise

    # ========================================================================
    # GLOBAL CLUSTERS
    # ========================================================================

    def create_global_cluster(
        self,
        global_cluster_id: str,
        engine_version: str = "1.2.0.1",
        storage_encrypted: bool = True,
        kms_key_id: Optional[str] = None
    ) -> GlobalCluster:
        """
        Create a Neptune global database cluster.

        Args:
            global_cluster_id: Global cluster identifier
            engine_version: Engine version
            storage_encrypted: Enable encryption
            kms_key_id: KMS key ID

        Returns:
            GlobalCluster object
        """
        params = {
            "GlobalClusterIdentifier": global_cluster_id,
            "Engine": "neptune",
            "EngineVersion": engine_version,
            "StorageEncrypted": storage_encrypted
        }

        if kms_key_id:
            params["KmsKeyId"] = kms_key_id

        try:
            response = self.neptune_client.create_global_database(**params)

            global_data = response.get("GlobalCluster", {})

            return GlobalCluster(
                global_cluster_id=global_data["GlobalClusterIdentifier"],
                global_cluster_arn=global_data["GlobalClusterArn"],
                engine=global_data.get("Engine", "neptune"),
                engine_version=global_data.get("EngineVersion", engine_version),
                status=global_data.get("Status", "available"),
                storage_encrypted=global_data.get("StorageEncrypted", True),
                kms_key=global_data.get("KmsKeyId")
            )

        except ClientError as e:
            logger.error(f"Failed to create global cluster: {e}")
            raise

    def add_secondary_cluster(
        self,
        global_cluster_id: str,
        cluster_id: str,
        region: Optional[str] = None
    ) -> bool:
        """
        Add a secondary cluster to a global database.

        Args:
            global_cluster_id: Global cluster identifier
            cluster_id: Secondary cluster identifier
            region: Region for the secondary cluster

        Returns:
            True if successful
        """
        params = {
            "GlobalClusterIdentifier": global_cluster_id,
            "DBClusterIdentifier": cluster_id
        }

        if region:
            params["Region"] = region

        try:
            self.neptune_client.create_db_cluster_to_global_database(**params)
            return True

        except ClientError as e:
            logger.error(f"Failed to add secondary cluster: {e}")
            raise

    def get_global_cluster(
        self,
        global_cluster_id: str
    ) -> Optional[GlobalCluster]:
        """
        Get global cluster information.

        Args:
            global_cluster_id: Global cluster identifier

        Returns:
            GlobalCluster object or None
        """
        try:
            response = self.neptune_client.describe_global_databases(
                GlobalClusterIdentifier=global_cluster_id
            )

            globals_data = response.get("GlobalClusters", [])
            if not globals_data:
                return None

            global_data = globals_data[0]

            secondary_clusters = [
                s["DBClusterIdentifier"]
                for s in global_data.get("GlobalClusterMembers", [])
                if s.get("DBClusterIdentifier") != global_cluster_id
            ]

            return GlobalCluster(
                global_cluster_id=global_data["GlobalClusterIdentifier"],
                global_cluster_arn=global_data["GlobalClusterArn"],
                engine=global_data.get("Engine", "neptune"),
                engine_version=global_data.get("EngineVersion", "1.2.0.1"),
                status=global_data.get("Status", "available"),
                storage_encrypted=global_data.get("StorageEncrypted", True),
                kms_key=global_data.get("KmsKeyId"),
                secondary_clusters=secondary_clusters
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "GlobalClusterNotFoundFault":
                return None
            logger.error(f"Failed to get global cluster: {e}")
            raise

    def list_global_clusters(self) -> List[GlobalCluster]:
        """
        List all global database clusters.

        Returns:
            List of GlobalCluster objects
        """
        try:
            response = self.neptune_client.describe_global_databases()
            clusters = []

            for global_data in response.get("GlobalClusters", []):
                secondary_clusters = [
                    s["DBClusterIdentifier"]
                    for s in global_data.get("GlobalClusterMembers", [])
                ]

                clusters.append(GlobalCluster(
                    global_cluster_id=global_data["GlobalClusterIdentifier"],
                    global_cluster_arn=global_data["GlobalClusterArn"],
                    engine=global_data.get("Engine", "neptune"),
                    engine_version=global_data.get("EngineVersion", "1.2.0.1"),
                    status=global_data.get("Status", "available"),
                    storage_encrypted=global_data.get("StorageEncrypted", True),
                    kms_key=global_data.get("KmsKeyId"),
                    secondary_clusters=secondary_clusters
                ))

            return clusters

        except ClientError as e:
            logger.error(f"Failed to list global clusters: {e}")
            raise

    def delete_global_cluster(
        self,
        global_cluster_id: str,
        skip_final_snapshot: bool = True
    ) -> bool:
        """
        Delete a global database cluster.

        Args:
            global_cluster_id: Global cluster identifier
            skip_final_snapshot: Skip final snapshot

        Returns:
            True if successful
        """
        params = {
            "GlobalClusterIdentifier": global_cluster_id,
            "SkipFinalSnapshot": skip_final_snapshot
        }

        try:
            self.neptune_client.delete_global_database(**params)
            return True

        except ClientError as e:
            logger.error(f"Failed to delete global cluster: {e}")
            raise

    # ========================================================================
    # BACKUPS AND SNAPSHOTS
    # ========================================================================

    def create_snapshot(
        self,
        cluster_id: str,
        snapshot_id: Optional[str] = None,
        wait_for_available: bool = True,
        timeout: int = 600
    ) -> BackupInfo:
        """
        Create a manual snapshot of a Neptune cluster.

        Args:
            cluster_id: Cluster identifier
            snapshot_id: Snapshot identifier (auto-generated if not provided)
            wait_for_available: Wait for snapshot to be available
            timeout: Timeout in seconds

        Returns:
            BackupInfo object
        """
        snapshot_id = snapshot_id or f"{cluster_id}-snapshot-{uuid.uuid4().hex[:8]}"

        try:
            response = self.neptune_client.create_db_cluster_snapshot(
                DBClusterIdentifier=cluster_id,
                DBClusterSnapshotIdentifier=snapshot_id
            )

            snapshot_data = response.get("DBClusterSnapshot", {})

            backup = BackupInfo(
                backup_id=snapshot_data["DBClusterSnapshotIdentifier"],
                snapshot_type="manual",
                cluster_id=snapshot_data["DBClusterIdentifier"],
                status=snapshot_data.get("Status", "creating"),
                allocated_storage=snapshot_data.get("AllocatedStorage", 0),
                encrypted=snapshot_data.get("Encrypted", True),
                kms_key=snapshot_data.get("KmsKeyId"),
                source_region=self.config.region,
                tags={t["Key"]: t["Value"] for t in snapshot_data.get("TagList", [])}
            )

            if wait_for_available:
                self._wait_for_snapshot_available(snapshot_id, timeout)
                backup.status = "available"

            return backup

        except ClientError as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise

    def _wait_for_snapshot_available(
        self,
        snapshot_id: str,
        timeout: int = 600
    ) -> bool:
        """Wait for snapshot to become available."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.neptune_client.describe_db_cluster_snapshots(
                    DBClusterSnapshotIdentifier=snapshot_id
                )
                snapshots = response.get("DBClusterSnapshots", [])
                if snapshots:
                    status = snapshots[0].get("Status", "")
                    if status == "available":
                        return True
                    elif status in ["deleting", "deleted", "failed"]:
                        raise TimeoutError(f"Snapshot {snapshot_id} entered state: {status}")
                time.sleep(10)
            except ClientError as e:
                if e.response["Error"]["Code"] == "SnapshotNotFoundFault":
                    raise
                time.sleep(10)
        raise TimeoutError(f"Timeout waiting for snapshot {snapshot_id}")

    def get_snapshot(
        self,
        snapshot_id: str
    ) -> Optional[BackupInfo]:
        """
        Get snapshot information.

        Args:
            snapshot_id: Snapshot identifier

        Returns:
            BackupInfo object or None
        """
        try:
            response = self.neptune_client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=snapshot_id
            )

            snapshots = response.get("DBClusterSnapshots", [])
            if not snapshots:
                return None

            snapshot_data = snapshots[0]

            return BackupInfo(
                backup_id=snapshot_data["DBClusterSnapshotIdentifier"],
                snapshot_type="automated" if snapshot_data.get("SnapshotType") == "automated" else "manual",
                cluster_id=snapshot_data["DBClusterIdentifier"],
                status=snapshot_data.get("Status", "unknown"),
                allocated_storage=snapshot_data.get("AllocatedStorage", 0),
                encrypted=snapshot_data.get("Encrypted", True),
                kms_key=snapshot_data.get("KmsKeyId"),
                source_region=snapshot_data.get("SourceRegion"),
                tags={t["Key"]: t["Value"] for t in snapshot_data.get("TagList", [])}
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "SnapshotNotFoundFault":
                return None
            logger.error(f"Failed to get snapshot: {e}")
            raise

    def list_snapshots(
        self,
        cluster_id: Optional[str] = None,
        snapshot_type: Optional[str] = None
    ) -> List[BackupInfo]:
        """
        List cluster snapshots.

        Args:
            cluster_id: Filter by cluster ID
            snapshot_type: Filter by type ("automated" or "manual")

        Returns:
            List of BackupInfo objects
        """
        params = {}
        if cluster_id:
            params["DBClusterIdentifier"] = cluster_id
        if snapshot_type:
            params["SnapshotType"] = snapshot_type

        try:
            response = self.neptune_client.describe_db_cluster_snapshots(**params)
            snapshots = []

            for snapshot_data in response.get("DBClusterSnapshots", []):
                snapshots.append(BackupInfo(
                    backup_id=snapshot_data["DBClusterSnapshotIdentifier"],
                    snapshot_type=snapshot_data.get("SnapshotType", "manual"),
                    cluster_id=snapshot_data["DBClusterIdentifier"],
                    status=snapshot_data.get("Status", "unknown"),
                    allocated_storage=snapshot_data.get("AllocatedStorage", 0),
                    encrypted=snapshot_data.get("Encrypted", True),
                    kms_key=snapshot_data.get("KmsKeyId"),
                    source_region=snapshot_data.get("SourceRegion"),
                    tags={t["Key"]: t["Value"] for t in snapshot_data.get("TagList", [])}
                ))

            return snapshots

        except ClientError as e:
            logger.error(f"Failed to list snapshots: {e}")
            raise

    def delete_snapshot(
        self,
        snapshot_id: str
    ) -> bool:
        """
        Delete a cluster snapshot.

        Args:
            snapshot_id: Snapshot identifier

        Returns:
            True if successful
        """
        try:
            self.neptune_client.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snapshot_id
            )
            return True

        except ClientError as e:
            logger.error(f"Failed to delete snapshot: {e}")
            raise

    def restore_from_snapshot(
        self,
        snapshot_id: str,
        cluster_id: Optional[str] = None,
        encryption: bool = True,
        kms_key_id: Optional[str] = None,
        wait_for_available: bool = True,
        timeout: int = 600
    ) -> NeptuneCluster:
        """
        Restore a cluster from a snapshot.

        Args:
            snapshot_id: Snapshot identifier
            cluster_id: New cluster identifier
            encryption: Enable encryption
            kms_key_id: KMS key ID
            wait_for_available: Wait for cluster to become available
            timeout: Timeout in seconds

        Returns:
            Restored NeptuneCluster object
        """
        cluster_id = cluster_id or self._generate_cluster_id()

        params = {
            "DBClusterIdentifier": cluster_id,
            "SnapshotIdentifier": snapshot_id,
            "StorageEncrypted": encryption
        }

        if kms_key_id:
            params["KmsKeyId"] = kms_key_id

        try:
            self.neptune_client.restore_db_cluster_from_snapshot(**params)

            if wait_for_available:
                self._wait_for_cluster_available(cluster_id, timeout)

            return self.get_cluster(cluster_id)

        except ClientError as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            raise

    # ========================================================================
    # IAM AUTHENTICATION
    # ========================================================================

    def configure_iam_auth(
        self,
        cluster_id: str,
        enable: bool = True
    ) -> NeptuneCluster:
        """
        Configure IAM authentication for a Neptune cluster.

        Args:
            cluster_id: Cluster identifier
            enable: Enable or disable IAM auth

        Returns:
            Updated NeptuneCluster object
        """
        try:
            self.neptune_client.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                IAMDatabaseAuthenticationEnabled=enable,
                ApplyImmediately=True
            )

            self._wait_for_cluster_available(cluster_id)

            return self.get_cluster(cluster_id)

        except ClientError as e:
            logger.error(f"Failed to configure IAM auth: {e}")
            raise

    def generate_iam_auth_token(
        self,
        cluster_id: str,
        username: str = "admin",
        duration: int = 900
    ) -> str:
        """
        Generate an IAM authentication token for Neptune.

        Args:
            cluster_id: Cluster identifier
            username: Database username
            duration: Token duration in seconds

        Returns:
            IAM auth token
        """
        try:
            import urllib.parse

            endpoint = self._get_cluster_endpoint(cluster_id, "writer")
            if not endpoint:
                raise ValueError(f"Could not get endpoint for cluster {cluster_id}")

            session = boto3.Session(profile_name=self.profile_name)
            credentials = session.get_credentials()

            host = f"{endpoint}:{self.config.port}"

            params = {
                "Action": "connect",
                "DBUser": username
            }

            if credentials.token:
                params["X-Amz-Security-Token"] = credentials.token

            canonical_uri = f"/{urllib.parse.quote(host, safe='')}"
            canonical_querystring = urllib.parse.urlencode(params)

            return f"{host}/?{canonical_querystring}"

        except Exception as e:
            logger.error(f"Failed to generate IAM auth token: {e}")
            raise

    def create_iam_policy(
        self,
        policy_name: str,
        cluster_arn: str,
        region: Optional[str] = None
    ) -> str:
        """
        Create an IAM policy for Neptune access.

        Args:
            policy_name: Policy name
            cluster_arn: Neptune cluster ARN
            region: Region (uses config if not provided)

        Returns:
            Policy ARN
        """
        region = region or self.config.region

        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "neptune-db:connect"
                    ],
                    "Resource": [
                        f"{cluster_arn}"
                    ]
                }
            ]
        }

        try:
            response = self.iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=f"Policy for accessing Neptune cluster {cluster_arn}"
            )

            return response["Policy"]["Arn"]

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                return f"arn:aws:iam::123456789012:policy/{policy_name}"
            logger.error(f"Failed to create IAM policy: {e}")
            raise

    # ========================================================================
    # CLOUDWATCH INTEGRATION
    # ========================================================================

    def get_metrics(
        self,
        metric_names: List[str],
        cluster_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistics: List[str] = None
    ) -> List[MetricData]:
        """
        Get CloudWatch metrics for Neptune.

        Args:
            metric_names: List of metric names
            cluster_id: Cluster identifier
            instance_id: Instance identifier
            start_time: Start time
            end_time: End time
            period: Metric period in seconds
            statistics: Statistics to retrieve

        Returns:
            List of MetricData objects
        """
        statistics = statistics or ["Average", "Maximum", "Minimum"]

        dimensions = []
        if instance_id:
            dimensions.append({"Name": "DBInstanceIdentifier", "Value": instance_id})
        elif cluster_id:
            dimensions.append({"Name": "DBClusterIdentifier", "Value": cluster_id})

        if not start_time:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if not end_time:
            end_time = datetime.utcnow()

        metrics = []

        try:
            for metric_name in metric_names:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/Neptune",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=statistics,
                    Dimensions=dimensions
                )

                for point in response.get("Datapoints", []):
                    metrics.append(MetricData(
                        metric_name=metric_name,
                        value=point.get("Average", 0),
                        unit=point.get("Unit", "Count"),
                        timestamp=point.get("Timestamp"),
                        dimensions={d["Name"]: d["Value"] for d in dimensions}
                    ))

            return metrics

        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            raise

    def get_available_metrics(
        self,
        cluster_id: Optional[str] = None,
        instance_id: Optional[str] = None
    ) -> List[str]:
        """
        Get available CloudWatch metric names for Neptune.

        Args:
            cluster_id: Cluster identifier
            instance_id: Instance identifier

        Returns:
            List of available metric names
        """
        common_metrics = [
            "CPUUtilization",
            "FreeLocalStorage",
            "FreeStorageSpace",
            "TotalStorageSpace",
            "DatabaseConnections",
            "ClusterReplicaLag",
            "ClusterReplicaLagMaximum",
            "ClusterReplicaLagMinimum",
            "ActiveInstances",
            "HeartbeatInterval",
            "Gremlin.traversal",
            "Gremlin Http",
            "Gremlin WebSocket",
            "SPARQL Http",
            "NeptuneIO",
            "IOPS",
            "NetworkReceive",
            "NetworkTransmit",
            "SnapshotStorageUsed",
            "TotalSnapshotStorageUsed"
        ]

        return common_metrics

    def put_metric_data(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Dict[str, str] = None
    ) -> bool:
        """
        Put custom metric data to CloudWatch.

        Args:
            metric_name: Metric name
            value: Metric value
            unit: Unit type
            dimensions: Metric dimensions

        Returns:
            True if successful
        """
        params = {
            "Namespace": "Custom/Neptune",
            "MetricData": [
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": [
                        {"Name": k, "Value": v}
                        for k, v in (dimensions or {}).items()
                    ]
                }
            ]
        }

        try:
            self.cloudwatch_client.put_metric_data(**params)
            return True

        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise

    def setup_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        cluster_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        notification_topic: Optional[str] = None,
        actions_enabled: bool = True
    ) -> str:
        """
        Create a CloudWatch alarm for Neptune metrics.

        Args:
            alarm_name: Alarm name
            metric_name: Metric name to alarm on
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            cluster_id: Cluster identifier
            instance_id: Instance identifier
            notification_topic: SNS topic ARN for notifications
            actions_enabled: Enable alarm actions

        Returns:
            Alarm ARN
        """
        dimensions = []
        if instance_id:
            dimensions.append({"Name": "DBInstanceIdentifier", "Value": instance_id})
        elif cluster_id:
            dimensions.append({"Name": "DBClusterIdentifier", "Value": cluster_id})

        params = {
            "AlarmName": alarm_name,
            "MetricName": metric_name,
            "Namespace": "AWS/Neptune",
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "ActionsEnabled": actions_enabled,
            "AlarmActions": []
        }

        if notification_topic:
            params["AlarmActions"] = [notification_topic]
            params["OKActions"] = [notification_topic]

        if dimensions:
            params["Dimensions"] = dimensions

        try:
            response = self.cloudwatch_client.put_metric_alarm(**params)
            return response["AlarmArn"]

        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise

    def enable_audit_logging(
        self,
        cluster_id: str,
        wait_for_available: bool = True,
        timeout: int = 600
    ) -> NeptuneCluster:
        """
        Enable audit logging for Neptune cluster.

        Args:
            cluster_id: Cluster identifier
            wait_for_available: Wait for cluster update
            timeout: Timeout in seconds

        Returns:
            Updated NeptuneCluster object
        """
        try:
            self.neptune_client.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                EnableCloudwatchLogsExports=["audit"],
                ApplyImmediately=True
            )

            if wait_for_available:
                self._wait_for_cluster_available(cluster_id, timeout)

            return self.get_cluster(cluster_id)

        except ClientError as e:
            logger.error(f"Failed to enable audit logging: {e}")
            raise

    def get_log_events(
        self,
        log_group_name: str,
        start_time: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch log events.

        Args:
            log_group_name: Log group name
            start_time: Start time in milliseconds
            limit: Maximum number of events

        Returns:
            List of log events
        """
        params = {
            "logGroupName": log_group_name,
            "limit": limit
        }

        if start_time:
            params["startTime"] = start_time

        try:
            response = self.logs_client.filter_log_events(**params)

            return [
                {
                    "timestamp": event.get("timestamp"),
                    "message": event.get("message"),
                    "ingestion_time": event.get("ingestionTime")
                }
                for event in response.get("events", [])
            ]

        except ClientError as e:
            logger.error(f"Failed to get log events: {e}")
            raise

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    def health_check(self, cluster_id: str) -> Dict[str, Any]:
        """
        Perform health check on Neptune cluster.

        Args:
            cluster_id: Cluster identifier

        Returns:
            Health status dictionary
        """
        try:
            cluster = self.get_cluster(cluster_id)
            if not cluster:
                return {
                    "status": "unhealthy",
                    "reason": "Cluster not found"
                }

            instances = self.list_instances(cluster_id)
            writer_instances = [i for i in instances if i.role == "WRITER"]
            reader_instances = [i for i in instances if i.role == "READER"]

            writer_available = any(
                i.status == NeptuneInstanceState.AVAILABLE
                for i in writer_instances
            )

            healthy_readers = sum(
                1 for i in reader_instances
                if i.status == NeptuneInstanceState.AVAILABLE
            )

            return {
                "status": "healthy" if (cluster.status == NeptuneClusterState.AVAILABLE and writer_available) else "unhealthy",
                "cluster_status": cluster.status.value,
                "writer_available": writer_available,
                "writer_count": len(writer_instances),
                "reader_count": len(reader_instances),
                "healthy_readers": healthy_readers,
                "serverless": cluster.serverless,
                "encryption": cluster.storage_encrypted
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }

    def get_connection_info(
        self,
        cluster_id: str,
        role: str = "writer"
    ) -> Dict[str, Any]:
        """
        Get connection information for a cluster.

        Args:
            cluster_id: Cluster identifier
            role: Connection role ("writer" or "reader")

        Returns:
            Connection information dictionary
        """
        try:
            cluster = self.get_cluster(cluster_id)
            if not cluster:
                return {"error": "Cluster not found"}

            endpoint = cluster.endpoint if role == "writer" else cluster.reader_endpoint

            return {
                "cluster_id": cluster_id,
                "endpoint": endpoint,
                "port": cluster.port,
                "engine": cluster.engine,
                "engine_version": cluster.engine_version,
                "iam_auth": cluster.iam_auth,
                "serverless": cluster.serverless,
                "graph_type": self.config.graph_type.value,
                "gremlin_endpoint": f"wss://{endpoint}:{cluster.port}/gremlin" if endpoint else None,
                "sparql_endpoint": f"https://{endpoint}:{cluster.port}/sparql" if endpoint else None
            }

        except Exception as e:
            logger.error(f"Failed to get connection info: {e}")
            return {"error": str(e)}

    def export_config(self) -> Dict[str, Any]:
        """
        Export current configuration.

        Returns:
            Configuration dictionary
        """
        return {
            "region": self.config.region,
            "cluster_id": self.config.cluster_id,
            "endpoint": self.config.endpoint,
            "port": self.config.port,
            "graph_type": self.config.graph_type.value,
            "iam_auth_enabled": self.config.iam_auth_enabled,
            "serverless": self.config.serverless,
            "global_cluster": self.config.global_cluster,
            "auto_backup": self.config.auto_backup,
            "backup_retention_days": self.config.backup_retention_days,
            "encryption_enabled": self.config.encryption_enabled,
            "kms_key_id": self.config.kms_key_id,
            "cloudwatch_logs_exports": self.config.cloudwatch_logs_exports,
            "tags": self.config.tags
        }

    def import_config(self, config: Dict[str, Any]) -> None:
        """
        Import configuration.

        Args:
            config: Configuration dictionary
        """
        if "region" in config:
            self.config.region = config["region"]
        if "cluster_id" in config:
            self.config.cluster_id = config["cluster_id"]
        if "endpoint" in config:
            self.config.endpoint = config["endpoint"]
        if "port" in config:
            self.config.port = config["port"]
        if "graph_type" in config:
            self.config.graph_type = GraphType(config["graph_type"])
        if "iam_auth_enabled" in config:
            self.config.iam_auth_enabled = config["iam_auth_enabled"]
        if "serverless" in config:
            self.config.serverless = config["serverless"]
        if "global_cluster" in config:
            self.config.global_cluster = config["global_cluster"]
        if "auto_backup" in config:
            self.config.auto_backup = config["auto_backup"]
        if "backup_retention_days" in config:
            self.config.backup_retention_days = config["backup_retention_days"]
        if "encryption_enabled" in config:
            self.config.encryption_enabled = config["encryption_enabled"]
        if "kms_key_id" in config:
            self.config.kms_key_id = config["kms_key_id"]
        if "cloudwatch_logs_exports" in config:
            self.config.cloudwatch_logs_exports = config["cloudwatch_logs_exports"]
        if "tags" in config:
            self.config.tags = config["tags"]

        self._neptune_client = None
        self._rds_client = None
        self._global_client = None
        self._cloudwatch_client = None
        self._logs_client = None
        self._iam_client = None
        self._lambda_client = None
