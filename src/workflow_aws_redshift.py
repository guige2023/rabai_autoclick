"""
AWS Redshift Data Warehouse Integration Module for Workflow System

Implements a RedshiftIntegration class with:
1. Cluster management: Create/manage Redshift clusters
2. Serverless: Redshift Serverless
3. Node management: Manage node types and scaling
4. Database operations: Create/manage databases
5. User management: Create/manage users
6. Snapshot management: Create/manage snapshots
7. Data sharing: Redshift data sharing
8. Query: Execute queries via Data API
9. IAM auth: Configure IAM auth
10. CloudWatch integration: Performance metrics and monitoring

Commit: 'feat(aws-redshift): add AWS Redshift with cluster management, serverless, node management, databases, users, snapshots, data sharing, queries, IAM auth, CloudWatch'
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


class ClusterState(Enum):
    """Redshift cluster states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    MODIFYING = "modifying"
    REBOOTING = "rebooting"
    RESIZING = "resizing"
    FAILING = "failing"
    FAILED = "failed"
    Hibernating = "hibernating"
    Hibernated = "hibernated"


class NodeType(Enum):
    """Redshift node types."""
    DC1_LARGE = "dc1.large"
    DC1_XLARGE = "dc1.xlarge"
    DC2_LARGE = "dc2.large"
    DC2_XLARGE = "dc2.xlarge"
    RA3_XLPLUS = "ra3.xlplus"
    RA3_4XLARGE = "ra3.4xlarge"
    RA3_16XLARGE = "ra3.16xlarge"


class SnapshotType(Enum):
    """Snapshot types."""
    AUTOMATED = "automated"
    MANUAL = "manual"


class DataSharingStatus(Enum):
    """Data sharing status."""
    ACTIVE = "ACTIVE"
    PENDING = "PENDING"
    REVOKED = "REVOKED"


@dataclass
class RedshiftConfig:
    """Configuration for Redshift connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class ClusterConfig:
    """Configuration for creating a Redshift cluster."""
    cluster_identifier: str
    node_type: NodeType = NodeType.RA3_XLPLUS
    number_of_nodes: int = 1
    master_username: str = "admin"
    master_password: str = ""
    cluster_type: str = "single-node"
    db_name: str = "dev"
    port: int = 5439
    cluster_version: str = "1.0"
    cluster_parameter_group_name: Optional[str] = None
    cluster_subnet_group_name: Optional[str] = None
    vpc_security_group_ids: List[str] = field(default_factory=list)
    availability_zone: Optional[str] = None
    preferred_maintenance_window: Optional[str] = None
    automated_snapshot_retention_period: int = 1
    manual_snapshot_retention_period: int = -1
    port: int = 5439
    cluster_version: str = "1.0"
    allow_version_update: bool = True
    number_of_nodes: int = 1
    publicly_accessible: bool = False
    encrypted: bool = True
    kms_key_id: Optional[str] = None
    enhanced_vpc_routing: bool = False
    additional_info: Optional[str] = None
    iam_roles: List[str] = field(default_factory=list)
    maintenance_track_name: Optional[str] = None
    aqua_configuration_status: Optional[str] = None
    default_radio_dsh_kms_key_id: Optional[str] = None
    elastic_ip: Optional[str] = None
    aws_secret_kms_key_id: Optional[str] = None
   复原: bool = False
    snapshot_identifier: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ServerlessConfig:
    """Configuration for Redshift Serverless."""
    namespace_name: str
    iam_roles: List[str] = field(default_factory=list)
    admin_username: Optional[str] = None
    admin_password: Optional[str] = None
    db_name: Optional[str] = None
    kms_key_id: Optional[str] = None
    log_export: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None


@dataclass
class NamespaceConfig:
    """Configuration for Redshift Serverless namespace."""
    namespace_name: str
    admin_username: Optional[str] = None
    admin_password: Optional[str] = None
    db_name: Optional[str] = None
    iam_role_arn: Optional[str] = None
    kms_key_id: Optional[str] = None
    log_export: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkgroupConfig:
    """Configuration for Redshift Serverless workgroup."""
    workgroup_name: str
    namespace_name: str
    base_capacity: int = 32
    enhanced_vpc_routing: bool = False
    port: Optional[int] = 5439
    publicly_accessible: bool = True
    security_group_ids: List[str] = field(default_factory=list)
    subnet_ids: List[str] = field(default_factory=list)
    vpc_endpoint: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DatabaseConfig:
    """Configuration for creating a database."""
    database_name: str
    owner_username: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class UserConfig:
    """Configuration for creating a user."""
    username: str
    password: Optional[str] = None
    super_user: bool = False
    create_database_user: bool = True


@dataclass
class SnapshotConfig:
    """Configuration for creating a snapshot."""
    snapshot_identifier: str
    cluster_identifier: Optional[str] = None
    manual_snapshot_retention_period: int = -1


@dataclass
class DataShareConfig:
    """Configuration for data sharing."""
    data_share_name: str
    producer_arn: Optional[str] = None
    allow_publicly_accessible_consumer: bool = True
    consumer_identifier: Optional[str] = None
    consumer_region: Optional[str] = None


@dataclass
class QueryResult:
    """Query execution result from Data API."""
    query_id: str
    query: str
    status: str
    result: Optional[List[List[Any]]] = None
    column_metadata: Optional[List[Dict[str, Any]]] = None
    number_of_rows_inserted: Optional[int] = None
    error_message: Optional[str] = None
    duration: int = 0
    started_at: Optional[datetime] = None


class RedshiftIntegration:
    """
    AWS Redshift Data Warehouse Integration.
    
    Provides comprehensive management capabilities for:
    - Cluster management (provisioned clusters)
    - Redshift Serverless
    - Node types and scaling
    - Database operations
    - User management
    - Snapshot management
    - Data sharing
    - Query execution via Data API
    - IAM authentication
    - CloudWatch monitoring
    """
    
    def __init__(
        self,
        config: Optional[RedshiftConfig] = None,
        cache_ttl: int = 300
    ):
        """
        Initialize Redshift integration.
        
        Args:
            config: Redshift configuration
            cache_ttl: Cache time-to-live in seconds
        """
        self.config = config or RedshiftConfig()
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, tuple[Any, float]] = {}
        self._cache_lock = threading.RLock()
        
        self._redshift_client = None
        self._serverless_client = None
        self._dataapi_client = None
        self._cloudwatch_client = None
        self._iam_client = None
        self._sts_client = None
        self._region = self.config.region_name
        self._endpoint_cache: Dict[str, str] = {}
        
    @property
    def redshift_client(self):
        """Get or create Redshift client."""
        if self._redshift_client is None:
            self._init_clients()
        return self._redshift_client
    
    @property
    def serverless_client(self):
        """Get or create Redshift Serverless client."""
        if self._serverless_client is None:
            self._init_clients()
        return self._serverless_client
    
    @property
    def dataapi_client(self):
        """Get or create Redshift Data API client."""
        if self._dataapi_client is None:
            self._init_clients()
        return self._dataapi_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            self._init_clients()
        return self._cloudwatch_client
    
    @property
    def iam_client(self):
        """Get or create IAM client."""
        if self._iam_client is None:
            self._init_clients()
        return self._iam_client
    
    def _init_clients(self):
        """Initialize all AWS clients."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for Redshift integration")
        
        client_kwargs = {"region_name": self.config.region_name}
        
        if self.config.profile_name:
            session = boto3.Session(profile_name=self.config.profile_name)
        else:
            session_kwargs = {"region_name": self.config.region_name}
            if self.config.aws_access_key_id:
                session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            if self.config.aws_secret_access_key:
                session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
            if self.config.aws_session_token:
                session_kwargs["aws_session_token"] = self.config.aws_session_token
            session = boto3.Session(**session_kwargs)
        
        self._redshift_client = session.client("redshift")
        self._serverless_client = session.client("redshift-serverless")
        self._dataapi_client = session.client("redshift-data")
        self._cloudwatch_client = session.client("cloudwatch")
        self._iam_client = session.client("iam")
        self._sts_client = session.client("sts")
    
    def _get_cache(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        with self._cache_lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    return value
                del self._cache[key]
        return None
    
    def _set_cache(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache with TTL."""
        with self._cache_lock:
            expiry = time.time() + (ttl or self.cache_ttl)
            self._cache[key] = (value, expiry)
    
    def _invalidate_cache(self, key: str):
        """Invalidate cache entry."""
        with self._cache_lock:
            if key in self._cache:
                del self._cache[key]
    
    def _invalidate_pattern(self, pattern: str):
        """Invalidate all cache entries matching pattern."""
        with self._cache_lock:
            keys_to_delete = [k for k in self._cache.keys() if k.startswith(pattern)]
            for key in keys_to_delete:
                del self._cache[key]
    
    def _parse_cluster_state(self, state: str) -> ClusterState:
        """Parse cluster state string to enum."""
        try:
            return ClusterState(state.lower())
        except ValueError:
            return ClusterState.AVAILABLE
    
    def _wait_for_cluster_state(
        self,
        cluster_identifier: str,
        target_states: Set[ClusterState],
        timeout: int = 1800,
        check_interval: int = 30
    ) -> Dict[str, Any]:
        """Wait for cluster to reach target state."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            cluster = self.get_cluster(cluster_identifier)
            if cluster:
                current_state = self._parse_cluster_state(cluster.get("ClusterStatus", ""))
                if current_state in target_states:
                    return cluster
                if current_state == ClusterState.FAILED:
                    raise RuntimeError(f"Cluster {cluster_identifier} failed")
            time.sleep(check_interval)
        raise TimeoutError(f"Cluster {cluster_identifier} did not reach target state within {timeout}s")
    
    # ========================================================================
    # Cluster Management
    # ========================================================================
    
    def create_cluster(self, config: ClusterConfig) -> Dict[str, Any]:
        """
        Create a Redshift cluster.
        
        Args:
            config: Cluster configuration
            
        Returns:
            Created cluster information
        """
        try:
            params = {
                "ClusterIdentifier": config.cluster_identifier,
                "NodeType": config.node_type.value if isinstance(config.node_type, NodeType) else config.node_type,
                "MasterUsername": config.master_username,
                "MasterUserPassword": config.master_password,
                "ClusterType": config.cluster_type,
                "DBName": config.db_name,
                "Port": config.port,
                "ClusterVersion": config.cluster_version,
                "AllowVersionUpgrade": config.allow_version_update,
                "NumberOfNodes": config.number_of_nodes,
                "PubliclyAccessible": config.publicly_accessible,
                "Encrypted": config.encrypted,
                "EnhancedVpcRouting": config.enhanced_vpc_routing,
                "AutomatedSnapshotRetentionPeriod": config.automated_snapshot_retention_period,
                "ManualSnapshotRetentionPeriod": config.manual_snapshot_retention_period,
            }
            
            if config.cluster_parameter_group_name:
                params["ClusterParameterGroupName"] = config.cluster_parameter_group_name
            if config.cluster_subnet_group_name:
                params["ClusterSubnetGroupName"] = config.cluster_subnet_group_name
            if config.vpc_security_group_ids:
                params["VpcSecurityGroupIds"] = config.vpc_security_group_ids
            if config.availability_zone:
                params["AvailabilityZone"] = config.availability_zone
            if config.preferred_maintenance_window:
                params["PreferredMaintenanceWindow"] = config.preferred_maintenance_window
            if config.kms_key_id:
                params["KmsKeyId"] = config.kms_key_id
            if config.additional_info:
                params["AdditionalInfo"] = config.additional_info
            if config.iam_roles:
                params["IamRoles"] = config.iam_roles
            if config.maintenance_track_name:
                params["MaintenanceTrackName"] = config.maintenance_track_name
            if config.aqua_configuration_status:
                params["AquaConfigurationStatus"] = config.aqua_configuration_status
            if config.elastic_ip:
                params["ElasticIp"] = config.elastic_ip
            if config复原 and config.snapshot_identifier:
                params["Restore"] = True
                params["SnapshotIdentifier"] = config.snapshot_identifier
            if config.tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.redshift_client.create_cluster(**params)
            cluster = response.get("Cluster", {})
            
            self._invalidate_pattern("cluster:")
            self._invalidate_pattern("clusters")
            
            logger.info(f"Created Redshift cluster: {config.cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to create cluster: {e}")
            raise
    
    def get_cluster(self, cluster_identifier: str) -> Optional[Dict[str, Any]]:
        """
        Get Redshift cluster information.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Cluster information or None if not found
        """
        cache_key = f"cluster:{cluster_identifier}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            response = self.redshift_client.describe_clusters(
                ClusterIdentifier=cluster_identifier
            )
            cluster = response["Clusters"][0] if response["Clusters"] else None
            
            if cluster:
                self._set_cache(cache_key, cluster)
            
            return cluster
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFound":
                return None
            logger.error(f"Failed to get cluster: {e}")
            raise
    
    def list_clusters(
        self,
        tag_keys: Optional[List[str]] = None,
        tag_values: Optional[List[str]] = None,
        max_records: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List Redshift clusters with optional tag filtering.
        
        Args:
            tag_keys: Filter by tag keys
            tag_values: Filter by tag values
            max_records: Maximum number of records
            
        Returns:
            List of cluster information
        """
        try:
            params: Dict[str, Any] = {"MaxRecords": max_records}
            
            if tag_keys or tag_values:
                filters = []
                if tag_keys:
                    filters.append({"Key": "tag:Key", "Values": tag_keys})
                if tag_values:
                    filters.append({"Key": "tag:Value", "Values": tag_values})
                params["TagKeys"] = tag_keys or []
                params["TagValues"] = tag_values or []
            
            clusters = []
            paginator = self.redshift_client.get_paginator("describe_clusters")
            
            for page in paginator.paginate(**params):
                clusters.extend(page.get("Clusters", []))
            
            return clusters
            
        except ClientError as e:
            logger.error(f"Failed to list clusters: {e}")
            raise
    
    def modify_cluster(
        self,
        cluster_identifier: str,
        modifications: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Modify a Redshift cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            modifications: Dict of modifications to apply
            
        Returns:
            Modified cluster information
        """
        try:
            params = {"ClusterIdentifier": cluster_identifier}
            params.update(modifications)
            
            response = self.redshift_client.modify_cluster(**params)
            cluster = response["Cluster"]
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            self._invalidate_pattern("clusters")
            
            logger.info(f"Modified Redshift cluster: {cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to modify cluster: {e}")
            raise
    
    def resize_cluster(
        self,
        cluster_identifier: str,
        number_of_nodes: int,
        cluster_type: Optional[str] = None,
        wait_for_completion: bool = True,
        timeout: int = 1800
    ) -> Dict[str, Any]:
        """
        Resize a Redshift cluster (change number of nodes or node type).
        
        Args:
            cluster_identifier: The cluster identifier
            number_of_nodes: New number of nodes
            cluster_type: Optional new cluster type (single-node or multi-node)
            wait_for_completion: Whether to wait for resize to complete
            timeout: Maximum time to wait in seconds
            
        Returns:
            Modified cluster information
        """
        params = {
            "ClusterIdentifier": cluster_identifier,
            "NumberOfNodes": number_of_nodes
        }
        
        if cluster_type:
            params["ClusterType"] = cluster_type
        
        try:
            response = self.redshift_client.modify_cluster(**params)
            cluster = response["Cluster"]
            
            if wait_for_completion:
                cluster = self._wait_for_cluster_state(
                    cluster_identifier,
                    {ClusterState.AVAILABLE, ClusterState.RESIZING},
                    timeout=timeout
                )
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            
            logger.info(f"Resized Redshift cluster: {cluster_identifier} to {number_of_nodes} nodes")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to resize cluster: {e}")
            raise
    
    def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_snapshot: bool = False,
        final_cluster_snapshot_identifier: Optional[str] = None,
        final_cluster_snapshot_retention_period: int = -1
    ) -> Dict[str, Any]:
        """
        Delete a Redshift cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            skip_final_snapshot: Whether to skip final snapshot
            final_cluster_snapshot_identifier: Identifier for final snapshot
            final_cluster_snapshot_retention_period: Retention period for snapshot
            
        Returns:
            Deleted cluster information
        """
        try:
            params: Dict[str, Any] = {
                "ClusterIdentifier": cluster_identifier,
                "SkipFinalClusterSnapshot": skip_final_snapshot,
                "FinalClusterSnapshotRetentionPeriod": final_cluster_snapshot_retention_period
            }
            
            if not skip_final_snapshot and final_cluster_snapshot_identifier:
                params["FinalClusterSnapshotIdentifier"] = final_cluster_snapshot_identifier
            
            response = self.redshift_client.delete_cluster(**params)
            cluster = response.get("Cluster", {})
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            self._invalidate_pattern("clusters")
            
            logger.info(f"Deleted Redshift cluster: {cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to delete cluster: {e}")
            raise
    
    def reboot_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Reboot a Redshift cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Rebooted cluster information
        """
        try:
            response = self.redshift_client.reboot_cluster(
                ClusterIdentifier=cluster_identifier
            )
            cluster = response["Cluster"]
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            
            logger.info(f"Rebooted Redshift cluster: {cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to reboot cluster: {e}")
            raise
    
    def pause_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Pause a Redshift cluster (hibernation).
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Cluster information
        """
        try:
            response = self.redshift_client.pause_cluster(
                ClusterIdentifier=cluster_identifier
            )
            cluster = response["Cluster"]
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            
            logger.info(f"Paused Redshift cluster: {cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to pause cluster: {e}")
            raise
    
    def resume_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Resume a paused Redshift cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Cluster information
        """
        try:
            response = self.redshift_client.resume_cluster(
                ClusterIdentifier=cluster_identifier
            )
            cluster = response["Cluster"]
            
            self._invalidate_cache(f"cluster:{cluster_identifier}")
            
            logger.info(f"Resumed Redshift cluster: {cluster_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to resume cluster: {e}")
            raise
    
    # ========================================================================
    # Node Management
    # ========================================================================
    
    def get_node_types(self) -> List[str]:
        """
        Get available Redshift node types.
        
        Returns:
            List of available node types
        """
        try:
            response = self.redshift_client.describe_node_types()
            return [nt["NodeType"] for nt in response.get("NodeTypes", [])]
        except ClientError as e:
            logger.error(f"Failed to get node types: {e}")
            raise
    
    def describe_orderable_node_options(
        self,
        node_type: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get orderable node options.
        
        Args:
            node_type: Filter by node type
            filters: Additional filters
            
        Returns:
            List of orderable options
        """
        try:
            params: Dict[str, Any] = {}
            if node_type:
                params["NodeType"] = node_type
            if filters:
                params["Filters"] = filters
            
            response = self.redshift_client.describe_orderable_node_options(**params)
            return response.get("OrderableNodeOptions", [])
            
        except ClientError as e:
            logger.error(f"Failed to get orderable node options: {e}")
            raise
    
    def get_cluster_version(self, cluster_identifier: str) -> Optional[Dict[str, Any]]:
        """
        Get cluster version information.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Cluster version info
        """
        cluster = self.get_cluster(cluster_identifier)
        if cluster:
            return {
                "cluster_version": cluster.get("ClusterVersion"),
                "cluster_revision_number": cluster.get("ClusterRevisionNumber"),
                "allow_version_upgrade": cluster.get("AllowVersionUpgrade")
            }
        return None
    
    # ========================================================================
    # Serverless Management
    # ========================================================================
    
    def create_serverless_namespace(self, config: NamespaceConfig) -> Dict[str, Any]:
        """
        Create a Redshift Serverless namespace.
        
        Args:
            config: Namespace configuration
            
        Returns:
            Created namespace information
        """
        try:
            params: Dict[str, Any] = {
                "namespaceName": config.namespace_name
            }
            
            if config.admin_username:
                params["adminUsername"] = config.admin_username
            if config.admin_password:
                params["adminPassword"] = config.admin_password
            if config.db_name:
                params["dbName"] = config.db_name
            if config.iam_role_arn:
                params["iamRoles"] = [config.iam_role_arn]
            if config.kms_key_id:
                params["kmsKeyId"] = config.kms_key_id
            if config.log_export:
                params["logExports"] = [config.log_export]
            if config.tags:
                params["tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.serverless_client.create_namespace(**params)
            namespace = response.get("namespace", {})
            
            self._invalidate_pattern("serverless:namespace")
            
            logger.info(f"Created Redshift Serverless namespace: {config.namespace_name}")
            return namespace
            
        except ClientError as e:
            logger.error(f"Failed to create serverless namespace: {e}")
            raise
    
    def get_serverless_namespace(self, namespace_name: str) -> Optional[Dict[str, Any]]:
        """
        Get Redshift Serverless namespace information.
        
        Args:
            namespace_name: The namespace name
            
        Returns:
            Namespace information or None
        """
        cache_key = f"serverless:namespace:{namespace_name}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            response = self.serverless_client.get_namespace(
                namespaceName=namespace_name
            )
            namespace = response.get("namespace", {})
            
            if namespace:
                self._set_cache(cache_key, namespace)
            
            return namespace
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            logger.error(f"Failed to get serverless namespace: {e}")
            raise
    
    def list_serverless_namespaces(self) -> List[Dict[str, Any]]:
        """
        List all Redshift Serverless namespaces.
        
        Returns:
            List of namespaces
        """
        try:
            namespaces = []
            paginator = self.serverless_client.get_paginator("list_namespaces")
            
            for page in paginator.paginate():
                namespaces.extend(page.get("namespaces", []))
            
            return namespaces
            
        except ClientError as e:
            logger.error(f"Failed to list serverless namespaces: {e}")
            raise
    
    def update_serverless_namespace(
        self,
        namespace_name: str,
        modifications: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a Redshift Serverless namespace.
        
        Args:
            namespace_name: The namespace name
            modifications: Dict of modifications
            
        Returns:
            Updated namespace information
        """
        try:
            params = {"namespaceName": namespace_name}
            params.update(modifications)
            
            response = self.serverless_client.update_namespace(**params)
            namespace = response.get("namespace", {})
            
            self._invalidate_cache(f"serverless:namespace:{namespace_name}")
            
            logger.info(f"Updated Redshift Serverless namespace: {namespace_name}")
            return namespace
            
        except ClientError as e:
            logger.error(f"Failed to update serverless namespace: {e}")
            raise
    
    def delete_serverless_namespace(self, namespace_name: str) -> Dict[str, Any]:
        """
        Delete a Redshift Serverless namespace.
        
        Args:
            namespace_name: The namespace name
            
        Returns:
            Deleted namespace information
        """
        try:
            response = self.serverless_client.delete_namespace(
                namespaceName=namespace_name
            )
            namespace = response.get("namespace", {})
            
            self._invalidate_cache(f"serverless:namespace:{namespace_name}")
            
            logger.info(f"Deleted Redshift Serverless namespace: {namespace_name}")
            return namespace
            
        except ClientError as e:
            logger.error(f"Failed to delete serverless namespace: {e}")
            raise
    
    def create_serverless_workgroup(self, config: WorkgroupConfig) -> Dict[str, Any]:
        """
        Create a Redshift Serverless workgroup.
        
        Args:
            config: Workgroup configuration
            
        Returns:
            Created workgroup information
        """
        try:
            params: Dict[str, Any] = {
                "workgroupName": config.workgroup_name,
                "namespaceName": config.namespace_name,
                "baseCapacity": config.base_capacity,
                "enhancedVpcRouting": config.enhanced_vpc_routing,
                "port": config.port,
                "publiclyAccessible": config.publicly_accessible,
            }
            
            if config.security_group_ids:
                params["securityGroupIds"] = config.security_group_ids
            if config.subnet_ids:
                params["subnetIds"] = config.subnet_ids
            if config.tags:
                params["tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.serverless_client.create_workgroup(**params)
            workgroup = response.get("workgroup", {})
            
            self._invalidate_pattern("serverless:workgroup")
            
            logger.info(f"Created Redshift Serverless workgroup: {config.workgroup_name}")
            return workgroup
            
        except ClientError as e:
            logger.error(f"Failed to create serverless workgroup: {e}")
            raise
    
    def get_serverless_workgroup(self, workgroup_name: str) -> Optional[Dict[str, Any]]:
        """
        Get Redshift Serverless workgroup information.
        
        Args:
            workgroup_name: The workgroup name
            
        Returns:
            Workgroup information or None
        """
        cache_key = f"serverless:workgroup:{workgroup_name}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            response = self.serverless_client.get_workgroup(
                workgroupName=workgroup_name
            )
            workgroup = response.get("workgroup", {})
            
            if workgroup:
                self._set_cache(cache_key, workgroup)
            
            return workgroup
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            logger.error(f"Failed to get serverless workgroup: {e}")
            raise
    
    def list_serverless_workgroups(self) -> List[Dict[str, Any]]:
        """
        List all Redshift Serverless workgroups.
        
        Returns:
            List of workgroups
        """
        try:
            workgroups = []
            paginator = self.serverless_client.get_paginator("list_workgroups")
            
            for page in paginator.paginate():
                workgroups.extend(page.get("workgroups", []))
            
            return workgroups
            
        except ClientError as e:
            logger.error(f"Failed to list serverless workgroups: {e}")
            raise
    
    def update_serverless_workgroup(
        self,
        workgroup_name: str,
        modifications: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a Redshift Serverless workgroup.
        
        Args:
            workgroup_name: The workgroup name
            modifications: Dict of modifications
            
        Returns:
            Updated workgroup information
        """
        try:
            params = {"workgroupName": workgroup_name}
            params.update(modifications)
            
            response = self.serverless_client.update_workgroup(**params)
            workgroup = response.get("workgroup", {})
            
            self._invalidate_cache(f"serverless:workgroup:{workgroup_name}")
            
            logger.info(f"Updated Redshift Serverless workgroup: {workgroup_name}")
            return workgroup
            
        except ClientError as e:
            logger.error(f"Failed to update serverless workgroup: {e}")
            raise
    
    def delete_serverless_workgroup(self, workgroup_name: str) -> Dict[str, Any]:
        """
        Delete a Redshift Serverless workgroup.
        
        Args:
            workgroup_name: The workgroup name
            
        Returns:
            Deleted workgroup information
        """
        try:
            response = self.serverless_client.delete_workgroup(
                workgroupName=workgroup_name
            )
            workgroup = response.get("workgroup", {})
            
            self._invalidate_cache(f"serverless:workgroup:{workgroup_name}")
            
            logger.info(f"Deleted Redshift Serverless workgroup: {workgroup_name}")
            return workgroup
            
        except ClientError as e:
            logger.error(f"Failed to delete serverless workgroup: {e}")
            raise
    
    def get_serverless_endpoint(self, workgroup_name: str) -> Optional[Dict[str, Any]]:
        """
        Get VPC endpoint information for serverless workgroup.
        
        Args:
            workgroup_name: The workgroup name
            
        Returns:
            Endpoint information
        """
        workgroup = self.get_serverless_workgroup(workgroup_name)
        if workgroup:
            return {
                "address": workgroup.get("address"),
                "port": workgroup.get("port"),
                "vpc_endpoint": workgroup.get("vpcEndpoint")
            }
        return None
    
    # ========================================================================
    # Database Operations
    # ========================================================================
    
    def execute_query(
        self,
        sql: str,
        database: Optional[str] = None,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: Optional[str] = None,
        auto_commit: bool = False,
        wait_for_completion: bool = True,
        timeout: int = 300
    ) -> QueryResult:
        """
        Execute a SQL query using the Redshift Data API.
        
        Args:
            sql: SQL query to execute
            database: Target database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless workgroups
            db_user: Database user
            auto_commit: Auto-commit the transaction
            wait_for_completion: Wait for query to complete
            timeout: Maximum wait time in seconds
            
        Returns:
            QueryResult with query execution details
        """
        try:
            params: Dict[str, Any] = {
                "Sql": sql,
                "WithEvent": True
            }
            
            if database:
                params["Database"] = database
            if cluster_identifier:
                params["ClusterIdentifier"] = cluster_identifier
            if serverless_workgroup:
                params["WorkgroupName"] = serverless_workgroup
            if db_user:
                params["DbUser"] = db_user
            if auto_commit:
                params["AutoCommit"] = True
            
            response = self.dataapi_client.execute_statement(**params)
            
            query_id = response.get("Id", "")
            result = QueryResult(
                query_id=query_id,
                query=sql,
                status="STARTED",
                started_at=datetime.now()
            )
            
            if wait_for_completion:
                result = self._wait_for_query_completion(query_id, timeout)
            
            return result
            
        except ClientError as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def _wait_for_query_completion(
        self,
        query_id: str,
        timeout: int = 300
    ) -> QueryResult:
        """Wait for query to complete and get results."""
        start_time = time.time()
        check_interval = 1
        
        while time.time() - start_time < timeout:
            response = self.dataapi_client.describe_statement(Id=query_id)
            status = response.get("Status", "")
            
            if status == "FINISHED":
                result_rows = None
                if response.get("HasResultSet"):
                    result_response = self.dataapi_client.get_statement_result(Id=query_id)
                    result_rows = result_response.get("Records", [])
                
                return QueryResult(
                    query_id=query_id,
                    query="",
                    status=status,
                    result=result_rows,
                    column_metadata=response.get("ColumnMetadata"),
                    number_of_rows_inserted=response.get("NumberOfRowsInserted"),
                    duration=response.get("Duration", 0),
                    started_at=response.get("StartedAt"),
                    completed_at=response.get("CompletedAt")
                )
            
            elif status in ["FAILED", "ABORTED"]:
                return QueryResult(
                    query_id=query_id,
                    query="",
                    status=status,
                    error_message=response.get("Error"),
                    duration=response.get("Duration", 0)
                )
            
            time.sleep(check_interval)
            check_interval = min(check_interval * 1.5, 10)
        
        return QueryResult(
            query_id=query_id,
            query="",
            status="TIMED_OUT",
            error_message=f"Query did not complete within {timeout}s"
        )
    
    def list_databases(
        self,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        database: str = "dev",
        db_user: str = "admin"
    ) -> List[Dict[str, Any]]:
        """
        List databases in a cluster or serverless namespace.
        
        Args:
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            database: Database to query
            db_user: Database user
            
        Returns:
            List of database information
        """
        query = """
            SELECT datname, datdba, datistemplate, datallowconn, datconnlimit, datlastsysoid
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
        """
        
        result = self.execute_query(
            sql=query,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            wait_for_completion=True
        )
        
        databases = []
        if result.result:
            for row in result.result:
                databases.append({
                    "name": row[0].get("stringValue", ""),
                    "owner": row[1].get("stringValue", ""),
                    "is_template": row[2].get("boolValue", False),
                    "allow_connections": row[3].get("boolValue", False),
                    "connection_limit": row[4].get("longValue", -1),
                    "last_system_oid": row[5].get("longValue", 0)
                })
        
        return databases
    
    def create_database(
        self,
        database_name: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        database: str = "dev",
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Create a database.
        
        Args:
            database_name: Name of the database to create
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            database: Database to execute against
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"CREATE DATABASE {database_name}"
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def drop_database(
        self,
        database_name: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        database: str = "dev",
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Drop a database.
        
        Args:
            database_name: Name of the database to drop
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            database: Database to execute against
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"DROP DATABASE {database_name}"
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def list_schemas(
        self,
        database: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> List[Dict[str, Any]]:
        """
        List schemas in a database.
        
        Args:
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            List of schema information
        """
        query = """
            SELECT schema_name, schema_owner, schema_acl
            FROM information_schema.schemata
            ORDER BY schema_name
        """
        
        result = self.execute_query(
            sql=query,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user
        )
        
        schemas = []
        if result.result:
            for row in result.result:
                schemas.append({
                    "name": row[0].get("stringValue", ""),
                    "owner": row[1].get("stringValue", ""),
                    "acl": row[2].get("stringValue", "")
                })
        
        return schemas
    
    def create_schema(
        self,
        schema_name: str,
        database: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin",
        owner: Optional[str] = None
    ) -> QueryResult:
        """
        Create a schema.
        
        Args:
            schema_name: Name of the schema
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            owner: Schema owner
            
        Returns:
            QueryResult
        """
        sql = f"CREATE SCHEMA"
        if owner:
            sql += f" {schema_name} AUTHORIZATION {owner}"
        else:
            sql += f" {schema_name}"
        
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def list_tables(
        self,
        schema: str,
        database: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> List[Dict[str, Any]]:
        """
        List tables in a schema.
        
        Args:
            schema: Schema name
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            List of table information
        """
        query = f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """
        
        result = self.execute_query(
            sql=query,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user
        )
        
        tables = []
        if result.result:
            for row in result.result:
                tables.append({
                    "name": row[0].get("stringValue", ""),
                    "type": row[1].get("stringValue", "")
                })
        
        return tables
    
    # ========================================================================
    # User Management
    # ========================================================================
    
    def list_users(
        self,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> List[Dict[str, Any]]:
        """
        List users in the cluster.
        
        Args:
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            List of user information
        """
        query = """
            SELECT usename, usesuper, usecreatedb, valuntil
            FROM pg_user
            ORDER BY usename
        """
        
        result = self.execute_query(
            sql=query,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user
        )
        
        users = []
        if result.result:
            for row in result.result:
                users.append({
                    "name": row[0].get("stringValue", ""),
                    "super": row[1].get("boolValue", False),
                    "can_create_db": row[2].get("boolValue", False),
                    "valid_until": row[3].get("stringValue", None)
                })
        
        return users
    
    def create_user(
        self,
        username: str,
        password: Optional[str] = None,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin",
        create_db_user: bool = True
    ) -> QueryResult:
        """
        Create a user.
        
        Args:
            username: Username
            password: User password
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            create_db_user: Whether to create the DB user (vs group)
            
        Returns:
            QueryResult
        """
        if password:
            sql = f"CREATE USER {username} WITH PASSWORD '{password}'"
        else:
            sql = f"CREATE USER {username}"
        
        if create_db_user:
            sql += " CREATEDB"
        
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def alter_user(
        self,
        username: str,
        password: Optional[str] = None,
        valid_until: Optional[str] = None,
        resume: bool = False,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Alter a user.
        
        Args:
            username: Username
            password: New password
            valid_until: Password expiration
            resume: Resume a suspended user
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"ALTER USER {username}"
        
        modifications = []
        if password:
            modifications.append(f"WITH PASSWORD '{password}'")
        if valid_until:
            modifications.append(f"VALID UNTIL '{valid_until}'")
        if resume:
            modifications.append("RESUME")
        
        if modifications:
            sql += " ".join(modifications)
        
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def drop_user(
        self,
        username: str,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Drop a user.
        
        Args:
            username: Username to drop
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"DROP USER {username}"
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def grant_privileges(
        self,
        privileges: str,
        on_object: str,
        object_type: str,
        to_user: str,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Grant privileges to a user.
        
        Args:
            privileges: Privileges to grant (e.g., SELECT, INSERT)
            on_object: Object name
            object_type: Object type (TABLE, SCHEMA, DATABASE, etc.)
            to_user: Target user
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"GRANT {privileges} ON {object_type} {on_object} TO {to_user}"
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    def revoke_privileges(
        self,
        privileges: str,
        on_object: str,
        object_type: str,
        from_user: str,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin"
    ) -> QueryResult:
        """
        Revoke privileges from a user.
        
        Args:
            privileges: Privileges to revoke
            on_object: Object name
            object_type: Object type
            from_user: Target user
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            
        Returns:
            QueryResult
        """
        sql = f"REVOKE {privileges} ON {object_type} {on_object} FROM {from_user}"
        return self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            serverless_workgroup=serverless_workgroup,
            db_user=db_user,
            auto_commit=True
        )
    
    # ========================================================================
    # Snapshot Management
    # ========================================================================
    
    def create_snapshot(
        self,
        snapshot_identifier: str,
        cluster_identifier: Optional[str] = None,
        manual_snapshot_retention_period: int = -1
    ) -> Dict[str, Any]:
        """
        Create a cluster snapshot.
        
        Args:
            snapshot_identifier: Identifier for the snapshot
            cluster_identifier: Cluster to snapshot (optional for serverless)
            manual_snapshot_retention_period: Retention period in days
            
        Returns:
            Snapshot information
        """
        try:
            params: Dict[str, Any] = {
                "SnapshotIdentifier": snapshot_identifier,
                "ManualSnapshotRetentionPeriod": manual_snapshot_retention_period
            }
            
            if cluster_identifier:
                params["ClusterIdentifier"] = cluster_identifier
            
            response = self.redshift_client.create_cluster_snapshot(**params)
            snapshot = response.get("Snapshot", {})
            
            self._invalidate_pattern("snapshot:")
            
            logger.info(f"Created snapshot: {snapshot_identifier}")
            return snapshot
            
        except ClientError as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise
    
    def get_snapshot(self, snapshot_identifier: str) -> Optional[Dict[str, Any]]:
        """
        Get snapshot information.
        
        Args:
            snapshot_identifier: The snapshot identifier
            
        Returns:
            Snapshot information or None
        """
        cache_key = f"snapshot:{snapshot_identifier}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            response = self.redshift_client.describe_cluster_snapshots(
                SnapshotIdentifier=snapshot_identifier
            )
            snapshot = response["Snapshots"][0] if response["Snapshots"] else None
            
            if snapshot:
                self._set_cache(cache_key, snapshot)
            
            return snapshot
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "SnapshotNotFound":
                return None
            logger.error(f"Failed to get snapshot: {e}")
            raise
    
    def list_snapshots(
        self,
        cluster_identifier: Optional[str] = None,
        snapshot_type: Optional[str] = None,
        max_records: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List snapshots.
        
        Args:
            cluster_identifier: Filter by cluster
            snapshot_type: Filter by type (automated or manual)
            max_records: Maximum records
            
        Returns:
            List of snapshots
        """
        try:
            params: Dict[str, Any] = {"MaxRecords": max_records}
            
            if cluster_identifier:
                params["ClusterIdentifier"] = cluster_identifier
            if snapshot_type:
                params["SnapshotType"] = snapshot_type
            
            snapshots = []
            paginator = self.redshift_client.get_paginator("describe_cluster_snapshots")
            
            for page in paginator.paginate(**params):
                snapshots.extend(page.get("Snapshots", []))
            
            return snapshots
            
        except ClientError as e:
            logger.error(f"Failed to list snapshots: {e}")
            raise
    
    def delete_snapshot(self, snapshot_identifier: str) -> Dict[str, Any]:
        """
        Delete a snapshot.
        
        Args:
            snapshot_identifier: The snapshot identifier
            
        Returns:
            Deleted snapshot information
        """
        try:
            response = self.redshift_client.delete_cluster_snapshot(
                SnapshotIdentifier=snapshot_identifier
            )
            snapshot = response.get("Snapshot", {})
            
            self._invalidate_cache(f"snapshot:{snapshot_identifier}")
            
            logger.info(f"Deleted snapshot: {snapshot_identifier}")
            return snapshot
            
        except ClientError as e:
            logger.error(f"Failed to delete snapshot: {e}")
            raise
    
    def restore_from_snapshot(
        self,
        snapshot_identifier: str,
        cluster_identifier: str,
        port: Optional[int] = None,
        availability_zone: Optional[str] = None,
        allow_version_upgrade: bool = True,
        publicly_accessible: bool = False,
        enhanced_vpc_routing: bool = False,
        wait_for_completion: bool = True,
        timeout: int = 1800
    ) -> Dict[str, Any]:
        """
        Restore a cluster from a snapshot.
        
        Args:
            snapshot_identifier: Source snapshot
            cluster_identifier: New cluster identifier
            port: Cluster port
            availability_zone: AZ for new cluster
            allow_version_upgrade: Allow version upgrades
            publicly_accessible: Public accessibility
            enhanced_vpc_routing: Enhanced VPC routing
            wait_for_completion: Wait for restore to complete
            timeout: Maximum wait time
            
        Returns:
            Restored cluster information
        """
        try:
            params: Dict[str, Any] = {
                "SnapshotIdentifier": snapshot_identifier,
                "ClusterIdentifier": cluster_identifier,
                "AllowVersionUpgrade": allow_version_upgrade,
                "PubliclyAccessible": publicly_accessible,
                "EnhancedVpcRouting": enhanced_vpc_routing
            }
            
            if port:
                params["Port"] = port
            if availability_zone:
                params["AvailabilityZone"] = availability_zone
            
            response = self.redshift_client.restore_from_cluster_snapshot(**params)
            cluster = response.get("Cluster", {})
            
            if wait_for_completion:
                cluster = self._wait_for_cluster_state(
                    cluster_identifier,
                    {ClusterState.AVAILABLE},
                    timeout=timeout
                )
            
            self._invalidate_pattern("cluster:")
            
            logger.info(f"Restored cluster {cluster_identifier} from snapshot {snapshot_identifier}")
            return cluster
            
        except ClientError as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            raise
    
    # ========================================================================
    # Data Sharing
    # ========================================================================
    
    def create_datashare(
        self,
        datashare_name: str,
        producer_namespace: Optional[str] = None,
        allow_publicly_accessible_consumer: bool = True,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a datashare.
        
        Args:
            datashare_name: Name of the datashare
            producer_namespace: Producer namespace ARN
            allow_publicly_accessible_consumer: Allow public consumers
            comment: Datashare comment
            
        Returns:
            Datashare information
        """
        try:
            params: Dict[str, Any] = {
                "DataShareName": datashare_name,
                "AllowPubliclyAccessibleConsumer": allow_publicly_accessible_consumer
            }
            
            if producer_namespace:
                params["ProducerNamespace"] = producer_namespace
            if comment:
                params["Comment"] = comment
            
            response = self.redshift_client.create_datashare(**params)
            datashare = response.get("DataShare", {})
            
            self._invalidate_pattern("datashare:")
            
            logger.info(f"Created datashare: {datashare_name}")
            return datashare
            
        except ClientError as e:
            logger.error(f"Failed to create datashare: {e}")
            raise
    
    def get_datashare(self, datashare_arn: str) -> Optional[Dict[str, Any]]:
        """
        Get datashare information.
        
        Args:
            datashare_arn: Datashare ARN
            
        Returns:
            Datashare information or None
        """
        cache_key = f"datashare:{datashare_arn}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        try:
            response = self.redshift_client.describe_datashare(datashare_arn=datashare_arn)
            datashare = response.get("DataShare", {})
            
            if datashare:
                self._set_cache(cache_key, datashare)
            
            return datashare
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            logger.error(f"Failed to get datashare: {e}")
            raise
    
    def list_datashares(
        self,
        producer_namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List datashares.
        
        Args:
            producer_namespace: Filter by producer namespace
            
        Returns:
            List of datashares
        """
        try:
            params: Dict[str, Any] = {}
            
            if producer_namespace:
                params["ProducerNamespace"] = producer_namespace
            
            response = self.redshift_client.list_datashares(**params)
            return response.get("DataShares", [])
            
        except ClientError as e:
            logger.error(f"Failed to list datashares: {e}")
            raise
    
    def alter_datashare(
        self,
        datashare_arn: str,
        add_schema_names: Optional[List[str]] = None,
        remove_schema_names: Optional[List[str]] = None,
        allow_publicly_accessible_consumer: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Alter a datashare (add/remove schemas).
        
        Args:
            datashare_arn: Datashare ARN
            add_schema_names: Schemas to add
            remove_schema_names: Schemas to remove
            allow_publicly_accessible_consumer: Change public access
            
        Returns:
            Updated datashare information
        """
        try:
            params: Dict[str, Any] = {"DataShareArn": datashare_arn}
            
            if add_schema_names:
                params["AddSchemaNames"] = add_schema_names
            if remove_schema_names:
                params["RemoveSchemaNames"] = remove_schema_names
            if allow_publicly_accessible_consumer is not None:
                params["AllowPubliclyAccessibleConsumer"] = allow_publicly_accessible_consumer
            
            response = self.redshift_client.alter_datashare(**params)
            datashare = response.get("DataShare", {})
            
            self._invalidate_cache(f"datashare:{datashare_arn}")
            
            logger.info(f"Altered datashare: {datashare_arn}")
            return datashare
            
        except ClientError as e:
            logger.error(f"Failed to alter datashare: {e}")
            raise
    
    def delete_datashare(self, datashare_arn: str) -> Dict[str, Any]:
        """
        Delete a datashare.
        
        Args:
            datashare_arn: Datashare ARN
            
        Returns:
            Deleted datashare information
        """
        try:
            response = self.redshift_client.delete_datashare(
                DataShareArn=datashare_arn
            )
            datashare = response.get("DataShare", {})
            
            self._invalidate_cache(f"datashare:{datashare_arn}")
            
            logger.info(f"Deleted datashare: {datashare_arn}")
            return datashare
            
        except ClientError as e:
            logger.error(f"Failed to delete datashare: {e}")
            raise
    
    def authorize_datashare(
        self,
        datashare_arn: str,
        consumer_identifier: Optional[str] = None,
        consumer_region: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Authorize a datashare for a consumer.
        
        Args:
            datashare_arn: Datashare ARN
            consumer_identifier: Consumer account or namespace
            consumer_region: Consumer region for cross-region sharing
            
        Returns:
            Datashare information
        """
        try:
            params: Dict[str, Any] = {"DataShareArn": datashare_arn}
            
            if consumer_identifier:
                params["ConsumerIdentifier"] = consumer_identifier
            if consumer_region:
                params["ConsumerRegion"] = consumer_region
            
            response = self.redshift_client.authorize_datashare(**params)
            datashare = response.get("DataShare", {})
            
            self._invalidate_cache(f"datashare:{datashare_arn}")
            
            logger.info(f"Authorized datashare: {datashare_arn}")
            return datashare
            
        except ClientError as e:
            logger.error(f"Failed to authorize datashare: {e}")
            raise
    
    def revoke_datashare(
        self,
        datashare_arn: str,
        consumer_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Revoke datashare authorization from a consumer.
        
        Args:
            datashare_arn: Datashare ARN
            consumer_identifier: Consumer to revoke
            
        Returns:
            Datashare information
        """
        try:
            params: Dict[str, Any] = {"DataShareArn": datashare_arn}
            
            if consumer_identifier:
                params["ConsumerIdentifier"] = consumer_identifier
            
            response = self.redshift_client.revoke_datashare(**params)
            datashare = response.get("DataShare", {})
            
            self._invalidate_cache(f"datashare:{datashare_arn}")
            
            logger.info(f"Revoked datashare: {datashare_arn}")
            return datashare
            
        except ClientError as e:
            logger.error(f"Failed to revoke datashare: {e}")
            raise
    
    def get_datashare_privileges(
        self,
        datashare_arn: str
    ) -> Dict[str, Any]:
        """
        Get datashare privileges.
        
        Args:
            datashare_arn: Datashare ARN
            
        Returns:
            Datashare privileges information
        """
        try:
            response = self.redshift_client.describe_datashare_privileges(
                datashare_arn=datashare_arn
            )
            return {
                "producer_privileges": response.get("ProducerNameSpace", {}),
                "consumer_privileges": response.get("ConsumerARN", [])
            }
        except ClientError as e:
            logger.error(f"Failed to get datashare privileges: {e}")
            raise
    
    # ========================================================================
    # IAM Authentication
    # ========================================================================
    
    def get_iam_policy(self, cluster_identifier: str) -> str:
        """
        Get IAM policy for cluster access.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            IAM policy document
        """
        cluster = self.get_cluster(cluster_identifier)
        if not cluster:
            raise ValueError(f"Cluster {cluster_identifier} not found")
        
        region = self._region
        account_id = self._get_account_id()
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "redshift:GetClusterCredentials",
                        "redshift:CreateClusterCredentials",
                        "redshift:DescribeClusters",
                        "redshift-data:ExecuteStatement",
                        "redshift-data:DescribeStatement",
                        "redshift-data:GetStatementResult"
                    ],
                    "Resource": [
                        f"arn:aws:redshift:{region}:{account_id}:cluster:{cluster_identifier}",
                        f"arn:aws:redshift:{region}:{account_id}:dbname:{cluster_identifier}/*",
                        f"arn:aws:redshift:{region}:{account_id}:dbuser:{cluster_identifier}/*"
                    ]
                }
            ]
        }
        
        return json.dumps(policy)
    
    def create_iam_role_for_cluster(
        self,
        role_name: str,
        cluster_identifier: str
    ) -> Dict[str, Any]:
        """
        Create IAM role for cluster access.
        
        Args:
            role_name: Name of the IAM role
            cluster_identifier: Cluster to grant access to
            
        Returns:
            IAM role information
        """
        try:
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": ["redshift.amazonaws.com"]},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            role = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                Description=f"Role for Redshift cluster {cluster_identifier}"
            )
            
            role_arn = role["Role"]["Arn"]
            
            policy = self.get_iam_policy(cluster_identifier)
            policy_name = f"redshift-{cluster_identifier}-access"
            
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=policy
            )
            
            self.redshift_client.modify_cluster(
                ClusterIdentifier=cluster_identifier,
                IamRoles=[role_arn]
            )
            
            logger.info(f"Created IAM role {role_name} for cluster {cluster_identifier}")
            return role["Role"]
            
        except ClientError as e:
            logger.error(f"Failed to create IAM role: {e}")
            raise
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        try:
            return self._sts_client.get_caller_identity()["Account"]
        except ClientError:
            return "123456789012"
    
    def generate_db_user_token(
        self,
        cluster_identifier: str,
        db_user: str,
        duration_seconds: int = 3600
    ) -> str:
        """
        Generate temporary database user token using IAM auth.
        
        Args:
            cluster_identifier: The cluster identifier
            db_user: Database username
            duration_seconds: Token duration
            
        Returns:
            Access token
        """
        try:
            credentials = self.redshift_client.get_cluster_credentials(
                ClusterIdentifier=cluster_identifier,
                DbUser=db_user,
                AutoCreate=False,
                DurationSeconds=duration_seconds
            )
            
            return credentials.get("DbPassword", "")
            
        except ClientError as e:
            logger.error(f"Failed to generate DB user token: {e}")
            raise
    
    # ========================================================================
    # CloudWatch Integration
    # ========================================================================
    
    def get_metrics(
        self,
        metric_names: List[str],
        start_time: datetime,
        end_time: datetime,
        period: int = 60,
        cluster_identifier: Optional[str] = None,
        namespace: str = "AWS/Redshift"
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for Redshift.
        
        Args:
            metric_names: List of metric names to retrieve
            start_time: Start of time range
            end_time: End of time range
            period: Metric period in seconds
            cluster_identifier: Filter by cluster
            namespace: CloudWatch namespace
            
        Returns:
            List of metric data points
        """
        try:
            params: Dict[str, Any] = {
                "Namespace": namespace,
                "MetricNames": metric_names,
                "StartTime": start_time,
                "EndTime": end_time,
                "Period": period
            }
            
            if cluster_identifier:
                params["Dimensions"] = [
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier}
                ]
            
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": f"m{i}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": namespace,
                                "MetricName": metric_name,
                                "Dimensions": params.get("Dimensions", [])
                            },
                            "Period": period,
                            "Stat": "Average"
                        }
                    }
                    for i, metric_name in enumerate(metric_names)
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            return response.get("MetricDataResults", [])
            
        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            raise
    
    def list_metrics(
        self,
        namespace: str = "AWS/Redshift",
        metric_name: Optional[str] = None,
        cluster_identifier: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List available CloudWatch metrics.
        
        Args:
            namespace: Metric namespace
            metric_name: Filter by metric name
            cluster_identifier: Filter by cluster
            
        Returns:
            List of metrics
        """
        try:
            params: Dict[str, Any] = {"Namespace": namespace}
            
            if metric_name:
                params["MetricName"] = metric_name
            
            if cluster_identifier:
                params["Dimensions"] = [
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier}
                ]
            
            metrics = []
            paginator = self.cloudwatch_client.get_paginator("list_metrics")
            
            for page in paginator.paginate(**params):
                metrics.extend(page.get("Metrics", []))
            
            return metrics
            
        except ClientError as e:
            logger.error(f"Failed to list CloudWatch metrics: {e}")
            raise
    
    def get_cluster_performance_metrics(
        self,
        cluster_identifier: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 60
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get cluster performance metrics.
        
        Args:
            cluster_identifier: The cluster identifier
            start_time: Start time
            end_time: End time
            period: Metric period
            
        Returns:
            Dictionary of metrics by name
        """
        metric_names = [
            "CPUUtilization",
            "DatabaseConnections",
            "QueriesCompletedPerSecond",
            "QueryRuntime",
            "StorageUtilization",
            "NetworkReceiveThroughput",
            "NetworkTransmitThroughput",
            "ReadIOPS",
            "WriteIOPS"
        ]
        
        metrics_data = self.get_metrics(
            metric_names=metric_names,
            start_time=start_time,
            end_time=end_time,
            period=period,
            cluster_identifier=cluster_identifier
        )
        
        result = {}
        for i, metric_name in enumerate(metric_names):
            if i < len(metrics_data):
                result[metric_name] = metrics_data[i].get("Values", [])
        
        return result
    
    def put_metric_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        cluster_identifier: Optional[str] = None,
        namespace: str = "AWS/Redshift",
        statistic: str = "Average"
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch metric alarm.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Metric to monitor
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            cluster_identifier: Cluster to monitor
            namespace: CloudWatch namespace
            statistic: Statistic type
            
        Returns:
            Alarm information
        """
        try:
            dimensions = []
            if cluster_identifier:
                dimensions.append(
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier}
                )
            
            params: Dict[str, Any] = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": namespace,
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": statistic
            }
            
            if dimensions:
                params["Dimensions"] = dimensions
            
            response = self.cloudwatch_client.put_metric_alarm(**params)
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create metric alarm: {e}")
            raise
    
    def list_alarms(
        self,
        alarm_prefix: Optional[str] = None,
        alarm_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms.
        
        Args:
            alarm_prefix: Filter by alarm name prefix
            alarm_names: Specific alarm names
            
        Returns:
            List of alarms
        """
        try:
            params: Dict[str, Any] = {}
            
            if alarm_prefix:
                params["AlarmPrefix"] = alarm_prefix
            if alarm_names:
                params["AlarmNames"] = alarm_names
            
            response = self.cloudwatch_client.describe_alarms(**params)
            return response.get("MetricAlarms", [])
            
        except ClientError as e:
            logger.error(f"Failed to list alarms: {e}")
            raise
    
    def delete_alarm(self, alarm_name: str) -> Dict[str, Any]:
        """
        Delete a CloudWatch alarm.
        
        Args:
            alarm_name: Alarm to delete
            
        Returns:
            Response information
        """
        try:
            response = self.cloudwatch_client.delete_alarms(
                AlarmNames=[alarm_name]
            )
            
            logger.info(f"Deleted CloudWatch alarm: {alarm_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete alarm: {e}")
            raise
    
    def get_query_execution_metrics(
        self,
        cluster_identifier: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Get query execution metrics for a cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            start_time: Start time
            end_time: End time
            
        Returns:
            Query execution metrics
        """
        metrics = self.get_cluster_performance_metrics(
            cluster_identifier=cluster_identifier,
            start_time=start_time,
            end_time=end_time,
            period=300
        )
        
        return {
            "queries_completed_per_second": metrics.get("QueriesCompletedPerSecond", []),
            "query_runtime": metrics.get("QueryRuntime", []),
            "cpu_utilization": metrics.get("CPUUtilization", []),
            "database_connections": metrics.get("DatabaseConnections", [])
        }
    
    def enable_performance_insights(
        self,
        cluster_identifier: str,
        retention_period: int = 7,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Enable Performance Insights on a cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            retention_period: Retention period in days
            kms_key_id: KMS key for encryption
            
        Returns:
            Modified cluster information
        """
        modifications = {
            "PerformanceInsightsEnabled": True,
            "PerformanceInsightsRetentionPeriod": retention_period
        }
        
        if kms_key_id:
            modifications["PerformanceInsightsKMSKeyId"] = kms_key_id
        
        return self.modify_cluster(cluster_identifier, modifications)
    
    def disable_performance_insights(
        self,
        cluster_identifier: str
    ) -> Dict[str, Any]:
        """
        Disable Performance Insights on a cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Modified cluster information
        """
        return self.modify_cluster(cluster_identifier, {
            "PerformanceInsightsEnabled": False
        })
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def get_cluster_endpoints(
        self,
        cluster_identifier: str
    ) -> Dict[str, Any]:
        """
        Get cluster connection endpoints.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Endpoint information
        """
        cluster = self.get_cluster(cluster_identifier)
        if not cluster:
            raise ValueError(f"Cluster {cluster_identifier} not found")
        
        endpoint = cluster.get("Endpoint", {})
        return {
            "address": endpoint.get("Address"),
            "port": endpoint.get("Port"),
            "vpc_endpoints": cluster.get("VpcEndpoints", [])
        }
    
    def get_cluster_credentials(
        self,
        cluster_identifier: str,
        db_name: str,
        auto_create: bool = False,
        duration_seconds: int = 900
    ) -> Dict[str, Any]:
        """
        Get cluster credentials.
        
        Args:
            cluster_identifier: The cluster identifier
            db_name: Database name
            auto_create: Auto-create user if not exists
            duration_seconds: Credential duration
            
        Returns:
            Credentials information
        """
        try:
            response = self.redshift_client.get_cluster_credentials(
                ClusterIdentifier=cluster_identifier,
                DbName=db_name,
                AutoCreate=auto_create,
                DurationSeconds=duration_seconds
            )
            
            return {
                "db_user": response.get("DbUser"),
                "db_password": response.get("DbPassword"),
                "expiration": response.get("Expiration")
            }
            
        except ClientError as e:
            logger.error(f"Failed to get cluster credentials: {e}")
            raise
    
    def get_service_account(self, cluster_identifier: str) -> str:
        """
        Get the default service account for the cluster.
        
        Args:
            cluster_identifier: The cluster identifier
            
        Returns:
            Service account name
        """
        return f"rsServiceAccount/{cluster_identifier}"
    
    def create_cluster_subnet_group(
        self,
        group_name: str,
        description: str,
        subnet_ids: List[str],
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a cluster subnet group.
        
        Args:
            group_name: Subnet group name
            description: Group description
            subnet_ids: List of subnet IDs
            tags: Optional tags
            
        Returns:
            Created subnet group
        """
        try:
            params: Dict[str, Any] = {
                "ClusterSubnetGroupName": group_name,
                "Description": description,
                "SubnetIds": subnet_ids
            }
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.redshift_client.create_cluster_subnet_group(**params)
            group = response.get("ClusterSubnetGroup", {})
            
            logger.info(f"Created cluster subnet group: {group_name}")
            return group
            
        except ClientError as e:
            logger.error(f"Failed to create subnet group: {e}")
            raise
    
    def list_cluster_subnet_groups(self) -> List[Dict[str, Any]]:
        """
        List cluster subnet groups.
        
        Returns:
            List of subnet groups
        """
        try:
            response = self.redshift_client.describe_cluster_subnet_groups()
            return response.get("ClusterSubnetGroups", [])
        except ClientError as e:
            logger.error(f"Failed to list subnet groups: {e}")
            raise
    
    def get_parameter_groups(self) -> List[Dict[str, Any]]:
        """
        Get available cluster parameter groups.
        
        Returns:
            List of parameter groups
        """
        try:
            response = self.redshift_client.describe_cluster_parameter_groups()
            return response.get("ParameterGroups", [])
        except ClientError as e:
            logger.error(f"Failed to get parameter groups: {e}")
            raise
    
    def get_parameters_in_group(
        self,
        parameter_group_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get parameters in a parameter group.
        
        Args:
            parameter_group_name: Parameter group name
            
        Returns:
            List of parameters
        """
        try:
            response = self.redshift_client.describe_cluster_parameters(
                ParameterGroupName=parameter_group_name
            )
            return response.get("Parameters", [])
        except ClientError as e:
            logger.error(f"Failed to get parameters: {e}")
            raise
    
    # ========================================================================
    # Batch Operations
    # ========================================================================
    
    def batch_execute_query(
        self,
        queries: List[Dict[str, Any]],
        database: str,
        cluster_identifier: Optional[str] = None,
        serverless_workgroup: Optional[str] = None,
        db_user: str = "admin",
        wait_for_completion: bool = True,
        timeout: int = 300
    ) -> List[QueryResult]:
        """
        Execute multiple queries in batch.
        
        Args:
            queries: List of query configs with 'sql' key
            database: Database name
            cluster_identifier: For provisioned clusters
            serverless_workgroup: For serverless
            db_user: Database user
            wait_for_completion: Wait for each query
            timeout: Timeout per query
            
        Returns:
            List of QueryResults
        """
        results = []
        
        for query_config in queries:
            sql = query_config.get("sql", "")
            result = self.execute_query(
                sql=sql,
                database=database,
                cluster_identifier=cluster_identifier,
                serverless_workgroup=serverless_workgroup,
                db_user=db_user,
                auto_commit=query_config.get("auto_commit", False),
                wait_for_completion=wait_for_completion,
                timeout=timeout
            )
            results.append(result)
            
            if result.status == "FAILED":
                logger.warning(f"Query failed: {sql}")
                if query_config.get("fail_on_error", False):
                    break
        
        return results
    
    def create_cluster_with_config(
        self,
        config: ClusterConfig,
        wait_for_completion: bool = True,
        timeout: int = 1800
    ) -> Dict[str, Any]:
        """
        Create cluster with full configuration including networking.
        
        Args:
            config: Full cluster configuration
            wait_for_completion: Wait for cluster creation
            timeout: Maximum wait time
            
        Returns:
            Created cluster information
        """
        cluster = self.create_cluster(config)
        
        if wait_for_completion:
            cluster = self._wait_for_cluster_state(
                config.cluster_identifier,
                {ClusterState.AVAILABLE},
                timeout=timeout
            )
        
        return cluster
    
    def scale_cluster(
        self,
        cluster_identifier: str,
        number_of_nodes: int,
        wait_for_completion: bool = True,
        timeout: int = 1800
    ) -> Dict[str, Any]:
        """
        Scale cluster to specified number of nodes.
        
        Args:
            cluster_identifier: The cluster identifier
            number_of_nodes: Target number of nodes
            wait_for_completion: Wait for resize
            timeout: Maximum wait time
            
        Returns:
            Modified cluster information
        """
        cluster = self.get_cluster(cluster_identifier)
        if not cluster:
            raise ValueError(f"Cluster {cluster_identifier} not found")
        
        current_nodes = cluster.get("NumberOfNodes", 1)
        cluster_type = "single-node" if number_of_nodes == 1 else "multi-node"
        
        return self.resize_cluster(
            cluster_identifier=cluster_identifier,
            number_of_nodes=number_of_nodes,
            cluster_type=cluster_type,
            wait_for_completion=wait_for_completion,
            timeout=timeout
        )
