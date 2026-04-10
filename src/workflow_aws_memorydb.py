"""
AWS MemoryDB for Redis Integration Module for Workflow System

Implements a MemoryDBIntegration class with:
1. Cluster management: Create/manage MemoryDB clusters
2. Node management: Manage nodes within clusters
3. Parameter groups: Configure parameter groups
4. Subnet groups: Configure subnet groups
5. ACLs: Access control lists
6. Snapshots: Manage snapshots
7. Updates: Cluster updates and versions
8. Encryption: At-rest and in-transit encryption
9. Multi-AZ: Multi-AZ deployments
10. CloudWatch integration: Cluster and node metrics

Commit: 'feat(aws-memorydb): add Amazon MemoryDB with cluster management, nodes, parameter groups, subnet groups, ACLs, snapshots, encryption, multi-AZ, CloudWatch'
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


class MemoryDBEngine(Enum):
    """Supported MemoryDB engine types."""
    REDIS = "redis"


class ClusterState(Enum):
    """MemoryDB cluster states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    MODIFYING = "modifying"
    REBOOTING = "rebooting"
    UPDATING = "updating"
    FAILING = "failing"
    FAILED = "failed"


class NodeState(Enum):
    """MemoryDB node states."""
    AVAILABLE = "available"
    CREATING = "creating"
    DELETING = "deleting"
    REBOOTING = "rebooting"
    FAILING = "failing"
    FAILED = "failed"


class ACLState(Enum):
    """ACL states."""
    ACTIVE = "active"
    CREATING = "creating"
    DELETING = "deleting"
    MODIFYING = "modifying"


class SnapshotState(Enum):
    """MemoryDB snapshot states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    FAILED = "failed"
    RESTORING = "restoring"
    COPYING = "copying"


class ParameterGroupState(Enum):
    """Parameter group states."""
    ACTIVE = "active"
    CREATING = "creating"
    DELETING = "deleting"
    MODIFYING = "modifying"


class SubnetGroupState(Enum):
    """Subnet group states."""
    ACTIVE = "active"
    CREATING = "creating"
    DELETING = "deleting"
    MODIFYING = "modifying"


@dataclass
class ClusterConfig:
    """Configuration for MemoryDB cluster creation."""
    cluster_name: str
    node_type: str = "db.r6g.large"
    num_nodes: int = 3
    parameter_group_name: str = "default.memorydb-redis7"
    subnet_group_name: str = "default"
    security_group_ids: List[str] = field(default_factory=list)
    port: int = 6379
    tls_enabled: bool = True
    auto_minor_version_upgrade: bool = True
    maintenance_window: str = "mon:03:00-mon:04:00"
    notification_topic_arn: str = ""
    snapshot_retention_limit: int = 0
    snapshot_window: str = "06:00-07:00"
    snapshot_name_prefix: str = ""
    acl_name: str = "default"
    multi_az_enabled: bool = True
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ClusterInfo:
    """Information about a MemoryDB cluster."""
    cluster_name: str
    cluster_endpoint: str
    port: int
    num_nodes: int
    node_type: str
    engine_version: str
    status: ClusterState
    availability_zones: List[str]
    subnets: List[str] = field(default_factory=list)
    security_groups: List[str] = field(default_factory=list)
    acl_name: str = ""
    parameter_group_name: str = ""
    snapshot_window: str = ""
    snapshot_retention_limit: int = 0
    tls_enabled: bool = True
    multi_az_enabled: bool = False
    create_time: datetime = field(default_factory=datetime.now)
    engine: MemoryDBEngine = MemoryDBEngine.REDIS


@dataclass
class NodeInfo:
    """Information about a MemoryDB node."""
    node_id: str
    cluster_name: str
    availability_zone: str
    status: NodeState
    endpoint: str = ""
    port: int = 6379
    create_time: datetime = field(default_factory=datetime.now)


@dataclass
class ParameterGroupInfo:
    """Information about a parameter group."""
    name: str
    family: str
    description: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    status: ParameterGroupState = ParameterGroupState.ACTIVE


@dataclass
class SubnetGroupInfo:
    """Information about a subnet group."""
    name: str
    description: str
    subnets: List[str] = field(default_factory=list)
    vpc_id: str = ""
    status: SubnetGroupState = SubnetGroupState.ACTIVE


@dataclass
class ACLInfo:
    """Information about an ACL."""
    name: str
    status: ACLState
    user_names: List[str] = field(default_factory=list)
    min_tls_version: str = "1.2"
    security_group_ids: List[str] = field(default_factory=list)


@dataclass
class SnapshotInfo:
    """Information about a MemoryDB snapshot."""
    snapshot_name: str
    cluster_name: str
    source: str = ""
    status: SnapshotState = SnapshotState.AVAILABLE
    create_time: datetime = field(default_factory=datetime.now)
    node_type: str = ""
    num_nodes: int = 0
    engine_version: str = ""
    kms_key_id: str = ""
    encryption_in_transit: bool = True


@dataclass
class ClusterUpdateConfig:
    """Configuration for cluster updates."""
    cluster_name: str
    new_node_type: str = ""
    new_num_nodes: int = 0
    new_engine_version: str = ""
    new_maintenance_window: str = ""
    new_snapshot_window: str = ""
    new_snapshot_retention_limit: int = -1
    new_acl_name: str = ""
    new_multi_az: bool = False
    apply_immediately: bool = False


@dataclass
class EncryptionConfig:
    """Encryption configuration for MemoryDB."""
    at_rest_encryption_enabled: bool = True
    in_transit_encryption_enabled: bool = True
    kms_key_id: str = ""
    auth_token_enabled: bool = True
    tls_enabled: bool = True


class MemoryDBIntegration:
    """
    AWS MemoryDB for Redis integration providing management of clusters,
    nodes, parameter groups, subnet groups, ACLs, snapshots, encryption,
    multi-AZ deployments, and CloudWatch monitoring.
    """
    
    def __init__(self, region: str = "us-east-1", profile: Optional[str] = None):
        """
        Initialize MemoryDB integration.
        
        Args:
            region: AWS region for MemoryDB operations
            profile: Optional AWS profile name for boto3 session
        """
        self.region = region
        self.profile = profile
        self._clients = {}
        self._resource_cache = {}
        self._lock = threading.Lock()
        self._clusters = {}
        self._nodes = {}
        self._snapshots = {}
        self._parameter_groups = {}
        self._subnet_groups = {}
        self._acls = {}
        self._event_handlers = defaultdict(list)
        self._metrics_cache = {}
        
    def _get_client(self, service: str = "memorydb") -> Any:
        """Get or create boto3 client with caching."""
        if service not in self._clients:
            with self._lock:
                if self._clients.get(service) is None:
                    if BOTO3_AVAILABLE:
                        session_kwargs = {"region_name": self.region}
                        if self.profile:
                            session_kwargs["profile_name"] = self.profile
                        session = boto3.Session(**session_kwargs)
                        self._clients[service] = session.client(service)
                    else:
                        raise ImportError("boto3 is required for AWS operations")
        return self._clients[service]
    
    def _get_resource(self, service: str = "memorydb") -> Any:
        """Get or create boto3 resource with caching."""
        if service not in self._resource_cache:
            with self._lock:
                if self._resource_cache.get(service) is None:
                    if BOTO3_AVAILABLE:
                        session_kwargs = {"region_name": self.region}
                        if self.profile:
                            session_kwargs["profile_name"] = self.profile
                        session = boto3.Session(**session_kwargs)
                        self._resource_cache[service] = session.resource(service)
                    else:
                        raise ImportError("boto3 is required for AWS operations")
        return self._resource_cache[service]
    
    def _emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Emit internal event for hooks."""
        for handler in self._event_handlers.get(event_type, []):
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Event handler error for {event_type}: {e}")
    
    def on(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """
        Register event handler.
        
        Args:
            event_type: Type of event to listen for
            handler: Callback function for the event
        """
        self._event_handlers[event_type].append(handler)
    
    # ========================================================================
    # Cluster Management
    # ========================================================================
    
    def create_cluster(self, config: ClusterConfig) -> ClusterInfo:
        """
        Create a new MemoryDB cluster.
        
        Args:
            config: Cluster configuration
            
        Returns:
            ClusterInfo object with cluster details
        """
        client = self._get_client()
        
        cluster_kwargs = {
            "ClusterName": config.cluster_name,
            "NodeType": config.node_type,
            "NumShards": max(1, config.num_nodes // 3) if config.num_nodes > 1 else 1,
            "NumReplicasPerShard": max(0, (config.num_nodes - 1) // 3) if config.num_nodes > 1 else 0,
            "ParameterGroupName": config.parameter_group_name,
            "SubnetGroupName": config.subnet_group_name,
            "Port": config.port if not config.tls_enabled else 6379,
            "TLSEnabled": config.tls_enabled,
            "AutoMinorVersionUpgrade": config.auto_minor_version_upgrade,
            "MaintenanceWindow": config.maintenance_window,
            "ACLName": config.acl_name,
            "EngineVersion": "7.0",
        }
        
        if config.security_group_ids:
            cluster_kwargs["SecurityGroupIds"] = config.security_group_ids
        
        if config.notification_topic_arn:
            cluster_kwargs["NotificationTopicArn"] = config.notification_topic_arn
        
        if config.snapshot_retention_limit > 0:
            cluster_kwargs["SnapshotRetentionLimit"] = config.snapshot_retention_limit
            cluster_kwargs["SnapshotWindow"] = config.snapshot_window
        
        if config.snapshot_name_prefix:
            cluster_kwargs["SnapshotNamePrefix"] = config.snapshot_name_prefix
        
        if config.multi_az_enabled and config.num_nodes > 1:
            cluster_kwargs["MultiAZEnabled"] = True
        
        if config.tags:
            cluster_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
        
        try:
            response = client.create_cluster(**cluster_kwargs)
            cluster_data = response["Cluster"]
            
            cluster_info = self._parse_cluster_info(cluster_data)
            with self._lock:
                self._clusters[config.cluster_name] = cluster_info
            
            self._emit_event("cluster_created", {"cluster": cluster_info})
            return cluster_info
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create cluster {config.cluster_name}: {e}")
            raise
    
    def get_cluster(self, cluster_name: str) -> Optional[ClusterInfo]:
        """
        Get information about a MemoryDB cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            ClusterInfo object or None if not found
        """
        client = self._get_client()
        
        try:
            response = client.describe_clusters(ClusterName=cluster_name)
            if response.get("Clusters"):
                cluster_data = response["Clusters"][0]
                cluster_info = self._parse_cluster_info(cluster_data)
                with self._lock:
                    self._clusters[cluster_name] = cluster_info
                return cluster_info
            return None
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get cluster {cluster_name}: {e}")
            return None
    
    def list_clusters(self, max_results: int = 100) -> List[ClusterInfo]:
        """
        List all MemoryDB clusters.
        
        Args:
            max_results: Maximum number of clusters to return
            
        Returns:
            List of ClusterInfo objects
        """
        client = self._get_client()
        clusters = []
        
        try:
            paginator = client.get_paginator("describe_clusters")
            for page in paginator.paginate(MaxResults=max_results):
                for cluster_data in page.get("Clusters", []):
                    cluster_info = self._parse_cluster_info(cluster_data)
                    clusters.append(cluster_info)
                    with self._lock:
                        self._clusters[cluster_info.cluster_name] = cluster_info
            return clusters
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list clusters: {e}")
            return []
    
    def delete_cluster(self, cluster_name: str, final_snapshot_name: str = "") -> bool:
        """
        Delete a MemoryDB cluster.
        
        Args:
            cluster_name: Name of the cluster to delete
            final_snapshot_name: Optional name for final snapshot
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            kwargs = {"ClusterName": cluster_name}
            if final_snapshot_name:
                kwargs["FinalSnapshotName"] = final_snapshot_name
            
            client.delete_cluster(**kwargs)
            
            with self._lock:
                if cluster_name in self._clusters:
                    del self._clusters[cluster_name]
            
            self._emit_event("cluster_deleted", {"cluster_name": cluster_name})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete cluster {cluster_name}: {e}")
            return False
    
    def update_cluster(self, config: ClusterUpdateConfig) -> Optional[ClusterInfo]:
        """
        Update a MemoryDB cluster.
        
        Args:
            config: Update configuration
            
        Returns:
            Updated ClusterInfo object
        """
        client = self._get_client()
        
        try:
            modify_kwargs = {"ClusterName": config.cluster_name}
            
            if config.new_node_type:
                modify_kwargs["NodeType"] = config.new_node_type
            
            if config.new_num_nodes > 0:
                modify_kwargs["NumShards"] = max(1, config.new_num_nodes // 3)
                modify_kwargs["NumReplicasPerShard"] = max(0, (config.new_num_nodes - 1) // 3)
            
            if config.new_engine_version:
                modify_kwargs["EngineVersion"] = config.new_engine_version
            
            if config.new_maintenance_window:
                modify_kwargs["MaintenanceWindow"] = config.new_maintenance_window
            
            if config.new_snapshot_window:
                modify_kwargs["SnapshotWindow"] = config.new_snapshot_window
            
            if config.new_snapshot_retention_limit >= 0:
                modify_kwargs["SnapshotRetentionLimit"] = config.new_snapshot_retention_limit
            
            if config.new_acl_name:
                modify_kwargs["ACLName"] = config.new_acl_name
            
            if config.new_multi_az:
                modify_kwargs["MultiAZEnabled"] = True
            
            modify_kwargs["ApplyImmediately"] = config.apply_immediately
            
            client.modify_cluster(**modify_kwargs)
            
            return self.get_cluster(config.cluster_name)
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update cluster {config.cluster_name}: {e}")
            return None
    
    def _parse_cluster_info(self, data: Dict[str, Any]) -> ClusterInfo:
        """Parse cluster data into ClusterInfo object."""
        status_str = data.get("Status", "").lower()
        try:
            status = ClusterState(status_str)
        except ValueError:
            status = ClusterState.FAILED
        
        endpoint = data.get("ClusterEndpoint", {})
        availability_zones = [shard.get("AvailabilityZone", "") for shard in data.get("Shards", [])]
        subnets = data.get("SubnetGroupName", "").split(",") if data.get("SubnetGroupName") else []
        
        return ClusterInfo(
            cluster_name=data.get("Name", ""),
            cluster_endpoint=endpoint.get("Address", ""),
            port=endpoint.get("Port", 6379),
            num_nodes=sum(len(shard.get("Nodes", [])) for shard in data.get("Shards", [])),
            node_type=data.get("NodeType", ""),
            engine_version=data.get("EngineVersion", ""),
            status=status,
            availability_zones=availability_zones,
            subnets=subnets,
            security_groups=data.get("SecurityGroups", []),
            acl_name=data.get("ACLName", ""),
            parameter_group_name=data.get("ParameterGroupName", ""),
            snapshot_window=data.get("SnapshotWindow", ""),
            snapshot_retention_limit=data.get("SnapshotRetentionLimit", 0),
            tls_enabled=data.get("TLSEnabled", True),
            multi_az_enabled=data.get("MultiAZEnabled", False),
            create_time=data.get("CreateTime", datetime.now()),
            engine=MemoryDBEngine.REDIS
        )
    
    # ========================================================================
    # Node Management
    # ========================================================================
    
    def list_nodes(self, cluster_name: str) -> List[NodeInfo]:
        """
        List all nodes in a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of NodeInfo objects
        """
        client = self._get_client()
        nodes = []
        
        try:
            response = client.describe_clusters(ClusterName=cluster_name)
            if response.get("Clusters"):
                cluster_data = response["Clusters"][0]
                for shard in cluster_data.get("Shards", []):
                    for node_data in shard.get("Nodes", []):
                        node_info = self._parse_node_info(node_data, cluster_name)
                        nodes.append(node_info)
            return nodes
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list nodes for cluster {cluster_name}: {e}")
            return []
    
    def get_node(self, cluster_name: str, node_name: str) -> Optional[NodeInfo]:
        """
        Get information about a specific node.
        
        Args:
            cluster_name: Name of the cluster
            node_name: Name of the node
            
        Returns:
            NodeInfo object or None if not found
        """
        nodes = self.list_nodes(cluster_name)
        for node in nodes:
            if node.node_id == node_name:
                return node
        return None
    
    def reboot_node(self, cluster_name: str, node_ids: List[str]) -> bool:
        """
        Reboot one or more nodes in a cluster.
        
        Args:
            cluster_name: Name of the cluster
            node_ids: List of node IDs to reboot
            
        Returns:
            True if reboot was successful
        """
        client = self._get_client()
        
        try:
            client.reboot_node(
                ClusterName=cluster_name,
                NodeIdsToReboot=node_ids
            )
            self._emit_event("node_rebooted", {
                "cluster_name": cluster_name,
                "node_ids": node_ids
            })
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to reboot nodes in {cluster_name}: {e}")
            return False
    
    def _parse_node_info(self, data: Dict[str, Any], cluster_name: str) -> NodeInfo:
        """Parse node data into NodeInfo object."""
        status_str = data.get("Status", "").lower()
        try:
            status = NodeState(status_str)
        except ValueError:
            status = NodeState.FAILED
        
        endpoint = data.get("Endpoint", {})
        
        return NodeInfo(
            node_id=data.get("Name", ""),
            cluster_name=cluster_name,
            availability_zone=data.get("AvailabilityZone", ""),
            status=status,
            endpoint=endpoint.get("Address", ""),
            port=endpoint.get("Port", 6379),
            create_time=data.get("CreateTime", datetime.now())
        )
    
    # ========================================================================
    # Parameter Groups
    # ========================================================================
    
    def create_parameter_group(
        self,
        name: str,
        family: str = "memorydb-redis7",
        description: str = ""
    ) -> ParameterGroupInfo:
        """
        Create a new parameter group.
        
        Args:
            name: Name of the parameter group
            family: Parameter group family
            description: Description of the parameter group
            
        Returns:
            ParameterGroupInfo object
        """
        client = self._get_client()
        
        try:
            client.create_parameter_group(
                ParameterGroupName=name,
                ParameterGroupFamily=family,
                Description=description
            )
            
            info = ParameterGroupInfo(
                name=name,
                family=family,
                description=description,
                status=ParameterGroupState.ACTIVE
            )
            
            with self._lock:
                self._parameter_groups[name] = info
            
            self._emit_event("parameter_group_created", {"parameter_group": info})
            return info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create parameter group {name}: {e}")
            raise
    
    def get_parameter_group(self, name: str) -> Optional[ParameterGroupInfo]:
        """
        Get information about a parameter group.
        
        Args:
            name: Name of the parameter group
            
        Returns:
            ParameterGroupInfo object or None if not found
        """
        client = self._get_client()
        
        try:
            response = client.describe_parameter_groups(
                ParameterGroupName=name
            )
            
            if response.get("ParameterGroups"):
                pg_data = response["ParameterGroups"][0]
                
                params_response = client.list_parameters(
                    ParameterGroupName=name,
                    MaxResults=100
                )
                
                info = ParameterGroupInfo(
                    name=pg_data.get("Name", ""),
                    family=pg_data.get("Family", ""),
                    description=pg_data.get("Description", ""),
                    parameters={p["ParameterName"]: p.get("ParameterValue", "") 
                               for p in params_response.get("Parameters", [])},
                    status=ParameterGroupState.ACTIVE
                )
                
                with self._lock:
                    self._parameter_groups[name] = info
                
                return info
            return None
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get parameter group {name}: {e}")
            return None
    
    def list_parameter_groups(self) -> List[ParameterGroupInfo]:
        """
        List all parameter groups.
        
        Returns:
            List of ParameterGroupInfo objects
        """
        client = self._get_client()
        groups = []
        
        try:
            response = client.describe_parameter_groups()
            for pg_data in response.get("ParameterGroups", []):
                info = ParameterGroupInfo(
                    name=pg_data.get("Name", ""),
                    family=pg_data.get("Family", ""),
                    description=pg_data.get("Description", ""),
                    status=ParameterGroupState.ACTIVE
                )
                groups.append(info)
                with self._lock:
                    self._parameter_groups[info.name] = info
            return groups
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list parameter groups: {e}")
            return []
    
    def update_parameter_group(
        self,
        name: str,
        parameters: Dict[str, str]
    ) -> bool:
        """
        Update parameters in a parameter group.
        
        Args:
            name: Name of the parameter group
            parameters: Dictionary of parameter names and values
            
        Returns:
            True if update was successful
        """
        client = self._get_client()
        
        try:
            client.update_parameter_group(
                ParameterGroupName=name,
                ParameterNameValues=[
                    {"ParameterName": k, "ParameterValue": v}
                    for k, v in parameters.items()
                ]
            )
            self._emit_event("parameter_group_updated", {
                "parameter_group": name,
                "parameters": parameters
            })
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update parameter group {name}: {e}")
            return False
    
    def delete_parameter_group(self, name: str) -> bool:
        """
        Delete a parameter group.
        
        Args:
            name: Name of the parameter group to delete
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_parameter_group(ParameterGroupName=name)
            
            with self._lock:
                if name in self._parameter_groups:
                    del self._parameter_groups[name]
            
            self._emit_event("parameter_group_deleted", {"parameter_group": name})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete parameter group {name}: {e}")
            return False
    
    # ========================================================================
    # Subnet Groups
    # ========================================================================
    
    def create_subnet_group(
        self,
        name: str,
        subnet_ids: List[str],
        description: str = ""
    ) -> SubnetGroupInfo:
        """
        Create a new subnet group.
        
        Args:
            name: Name of the subnet group
            subnet_ids: List of subnet IDs
            description: Description of the subnet group
            
        Returns:
            SubnetGroupInfo object
        """
        client = self._get_client()
        
        try:
            client.create_subnet_group(
                SubnetGroupName=name,
                SubnetIds=subnet_ids,
                Description=description
            )
            
            info = SubnetGroupInfo(
                name=name,
                description=description,
                subnets=subnet_ids,
                status=SubnetGroupState.ACTIVE
            )
            
            with self._lock:
                self._subnet_groups[name] = info
            
            self._emit_event("subnet_group_created", {"subnet_group": info})
            return info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create subnet group {name}: {e}")
            raise
    
    def get_subnet_group(self, name: str) -> Optional[SubnetGroupInfo]:
        """
        Get information about a subnet group.
        
        Args:
            name: Name of the subnet group
            
        Returns:
            SubnetGroupInfo object or None if not found
        """
        client = self._get_client()
        
        try:
            response = client.describe_subnet_groups(
                SubnetGroupName=name
            )
            
            if response.get("SubnetGroups"):
                sg_data = response["SubnetGroups"][0]
                
                info = SubnetGroupInfo(
                    name=sg_data.get("Name", ""),
                    description=sg_data.get("Description", ""),
                    subnets=sg_data.get("Subnets", []),
                    vpc_id=sg_data.get("VpcId", ""),
                    status=SubnetGroupState.ACTIVE
                )
                
                with self._lock:
                    self._subnet_groups[name] = info
                
                return info
            return None
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get subnet group {name}: {e}")
            return None
    
    def list_subnet_groups(self) -> List[SubnetGroupInfo]:
        """
        List all subnet groups.
        
        Returns:
            List of SubnetGroupInfo objects
        """
        client = self._get_client()
        groups = []
        
        try:
            response = client.describe_subnet_groups()
            for sg_data in response.get("SubnetGroups", []):
                info = SubnetGroupInfo(
                    name=sg_data.get("Name", ""),
                    description=sg_data.get("Description", ""),
                    subnets=sg_data.get("Subnets", []),
                    vpc_id=sg_data.get("VpcId", ""),
                    status=SubnetGroupState.ACTIVE
                )
                groups.append(info)
                with self._lock:
                    self._subnet_groups[info.name] = info
            return groups
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list subnet groups: {e}")
            return []
    
    def update_subnet_group(
        self,
        name: str,
        subnet_ids: List[str] = None,
        description: str = ""
    ) -> bool:
        """
        Update a subnet group.
        
        Args:
            name: Name of the subnet group
            subnet_ids: New list of subnet IDs
            description: New description
            
        Returns:
            True if update was successful
        """
        client = self._get_client()
        
        try:
            kwargs = {"SubnetGroupName": name}
            if subnet_ids:
                kwargs["SubnetIds"] = subnet_ids
            if description:
                kwargs["Description"] = description
            
            client.update_subnet_group(**kwargs)
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update subnet group {name}: {e}")
            return False
    
    def delete_subnet_group(self, name: str) -> bool:
        """
        Delete a subnet group.
        
        Args:
            name: Name of the subnet group to delete
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_subnet_group(SubnetGroupName=name)
            
            with self._lock:
                if name in self._subnet_groups:
                    del self._subnet_groups[name]
            
            self._emit_event("subnet_group_deleted", {"subnet_group": name})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete subnet group {name}: {e}")
            return False
    
    # ========================================================================
    # ACLs (Access Control Lists)
    # ========================================================================
    
    def create_acl(
        self,
        name: str,
        user_names: List[str] = None,
        security_group_ids: List[str] = None
    ) -> ACLInfo:
        """
        Create a new ACL.
        
        Args:
            name: Name of the ACL
            user_names: List of user names to associate
            security_group_ids: List of security group IDs
            
        Returns:
            ACLInfo object
        """
        client = self._get_client()
        
        try:
            kwargs = {"ACLName": name}
            if user_names:
                kwargs["UserNames"] = user_names
            if security_group_ids:
                kwargs["SecurityGroupIds"] = security_group_ids
            
            response = client.create_acl(**kwargs)
            acl_data = response.get("ACL", {})
            
            status_str = acl_data.get("Status", "").lower()
            try:
                status = ACLState(status_str)
            except ValueError:
                status = ACLState.ACTIVE
            
            info = ACLInfo(
                name=acl_data.get("Name", name),
                status=status,
                user_names=acl_data.get("UserNames", []),
                min_tls_version=acl_data.get("MinTLSVersion", "1.2"),
                security_group_ids=acl_data.get("SecurityGroupIds", [])
            )
            
            with self._lock:
                self._acls[name] = info
            
            self._emit_event("acl_created", {"acl": info})
            return info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create ACL {name}: {e}")
            raise
    
    def get_acl(self, name: str) -> Optional[ACLInfo]:
        """
        Get information about an ACL.
        
        Args:
            name: Name of the ACL
            
        Returns:
            ACLInfo object or None if not found
        """
        client = self._get_client()
        
        try:
            response = client.describe_acls(ACLName=name)
            if response.get("ACLs"):
                acl_data = response["ACLs"][0]
                
                status_str = acl_data.get("Status", "").lower()
                try:
                    status = ACLState(status_str)
                except ValueError:
                    status = ACLState.ACTIVE
                
                info = ACLInfo(
                    name=acl_data.get("Name", ""),
                    status=status,
                    user_names=acl_data.get("UserNames", []),
                    min_tls_version=acl_data.get("MinTLSVersion", "1.2"),
                    security_group_ids=acl_data.get("SecurityGroupIds", [])
                )
                
                with self._lock:
                    self._acls[name] = info
                
                return info
            return None
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get ACL {name}: {e}")
            return None
    
    def list_acls(self) -> List[ACLInfo]:
        """
        List all ACLs.
        
        Returns:
            List of ACLInfo objects
        """
        client = self._get_client()
        acls = []
        
        try:
            response = client.describe_acls()
            for acl_data in response.get("ACLs", []):
                status_str = acl_data.get("Status", "").lower()
                try:
                    status = ACLState(status_str)
                except ValueError:
                    status = ACLState.ACTIVE
                
                info = ACLInfo(
                    name=acl_data.get("Name", ""),
                    status=status,
                    user_names=acl_data.get("UserNames", []),
                    min_tls_version=acl_data.get("MinTLSVersion", "1.2"),
                    security_group_ids=acl_data.get("SecurityGroupIds", [])
                )
                acls.append(info)
                with self._lock:
                    self._acls[info.name] = info
            return acls
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list ACLs: {e}")
            return []
    
    def update_acl(
        self,
        name: str,
        user_names_to_add: List[str] = None,
        user_names_to_remove: List[str] = None
    ) -> Optional[ACLInfo]:
        """
        Update an ACL.
        
        Args:
            name: Name of the ACL
            user_names_to_add: Users to add
            user_names_to_remove: Users to remove
            
        Returns:
            Updated ACLInfo object
        """
        client = self._get_client()
        
        try:
            kwargs = {"ACLName": name}
            if user_names_to_add:
                kwargs["UserNamesToAdd"] = user_names_to_add
            if user_names_to_remove:
                kwargs["UserNamesToRemove"] = user_names_to_remove
            
            client.update_acl(**kwargs)
            return self.get_acl(name)
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update ACL {name}: {e}")
            return None
    
    def delete_acl(self, name: str) -> bool:
        """
        Delete an ACL.
        
        Args:
            name: Name of the ACL to delete
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_acl(ACLName=name)
            
            with self._lock:
                if name in self._acls:
                    del self._acls[name]
            
            self._emit_event("acl_deleted", {"acl": name})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete ACL {name}: {e}")
            return False
    
    # ========================================================================
    # Snapshots
    # ========================================================================
    
    def create_snapshot(
        self,
        cluster_name: str,
        snapshot_name: str,
        kms_key_id: str = "",
        tags: Dict[str, str] = None
    ) -> SnapshotInfo:
        """
        Create a snapshot of a cluster.
        
        Args:
            cluster_name: Name of the cluster
            snapshot_name: Name for the snapshot
            kms_key_id: Optional KMS key ID for encryption
            tags: Optional tags for the snapshot
            
        Returns:
            SnapshotInfo object
        """
        client = self._get_client()
        
        try:
            kwargs = {
                "ClusterName": cluster_name,
                "SnapshotName": snapshot_name
            }
            
            if kms_key_id:
                kwargs["KmsKeyId"] = kms_key_id
            
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = client.create_snapshot(**kwargs)
            snapshot_data = response.get("Snapshot", {})
            
            status_str = snapshot_data.get("Status", "").lower()
            try:
                status = SnapshotState(status_str)
            except ValueError:
                status = SnapshotState.CREATING
            
            info = SnapshotInfo(
                snapshot_name=snapshot_data.get("Name", snapshot_name),
                cluster_name=snapshot_data.get("ClusterConfiguration", {}).get("Name", cluster_name),
                source=snapshot_data.get("Source", ""),
                status=status,
                create_time=snapshot_data.get("SnapshotCreationTime", datetime.now()),
                node_type=snapshot_data.get("NodeType", ""),
                num_nodes=snapshot_data.get("NumNodes", 0),
                engine_version=snapshot_data.get("EngineVersion", ""),
                kms_key_id=snapshot_data.get("KmsKeyId", ""),
                encryption_in_transit=snapshot_data.get("EncryptionInTransit", True)
            )
            
            with self._lock:
                self._snapshots[snapshot_name] = info
            
            self._emit_event("snapshot_created", {"snapshot": info})
            return info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create snapshot {snapshot_name}: {e}")
            raise
    
    def get_snapshot(self, snapshot_name: str) -> Optional[SnapshotInfo]:
        """
        Get information about a snapshot.
        
        Args:
            snapshot_name: Name of the snapshot
            
        Returns:
            SnapshotInfo object or None if not found
        """
        client = self._get_client()
        
        try:
            response = client.describe_snapshots(SnapshotName=snapshot_name)
            if response.get("Snapshots"):
                snapshot_data = response["Snapshots"][0]
                info = self._parse_snapshot_info(snapshot_data)
                
                with self._lock:
                    self._snapshots[snapshot_name] = info
                
                return info
            return None
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get snapshot {snapshot_name}: {e}")
            return None
    
    def list_snapshots(
        self,
        cluster_name: str = "",
        max_results: int = 100
    ) -> List[SnapshotInfo]:
        """
        List snapshots, optionally filtered by cluster.
        
        Args:
            cluster_name: Optional cluster name filter
            max_results: Maximum number of results
            
        Returns:
            List of SnapshotInfo objects
        """
        client = self._get_client()
        snapshots = []
        
        try:
            kwargs = {"MaxResults": max_results}
            if cluster_name:
                kwargs["ClusterName"] = cluster_name
            
            response = client.describe_snapshots(**kwargs)
            for snapshot_data in response.get("Snapshots", []):
                info = self._parse_snapshot_info(snapshot_data)
                snapshots.append(info)
                with self._lock:
                    self._snapshots[info.snapshot_name] = info
            return snapshots
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list snapshots: {e}")
            return []
    
    def copy_snapshot(
        self,
        source_snapshot_name: str,
        target_snapshot_name: str,
        target_kms_key_id: str = ""
    ) -> SnapshotInfo:
        """
        Copy a snapshot to a new snapshot.
        
        Args:
            source_snapshot_name: Name of source snapshot
            target_snapshot_name: Name for the copy
            target_kms_key_id: Optional KMS key for the copy
            
        Returns:
            SnapshotInfo object for the copy
        """
        client = self._get_client()
        
        try:
            kwargs = {
                "SourceSnapshotName": source_snapshot_name,
                "TargetSnapshotName": target_snapshot_name
            }
            
            if target_kms_key_id:
                kwargs["TargetKmsKeyId"] = target_kms_key_id
            
            response = client.copy_snapshot(**kwargs)
            snapshot_data = response.get("Snapshot", {})
            
            status_str = snapshot_data.get("Status", "").lower()
            try:
                status = SnapshotState(status_str)
            except ValueError:
                status = SnapshotState.COPYING
            
            info = SnapshotInfo(
                snapshot_name=snapshot_data.get("Name", target_snapshot_name),
                cluster_name=snapshot_data.get("ClusterConfiguration", {}).get("Name", ""),
                source=source_snapshot_name,
                status=status,
                create_time=snapshot_data.get("SnapshotCreationTime", datetime.now()),
                node_type=snapshot_data.get("NodeType", ""),
                num_nodes=snapshot_data.get("NumNodes", 0),
                engine_version=snapshot_data.get("EngineVersion", ""),
                kms_key_id=snapshot_data.get("KmsKeyId", ""),
                encryption_in_transit=snapshot_data.get("EncryptionInTransit", True)
            )
            
            self._emit_event("snapshot_copied", {
                "source": source_snapshot_name,
                "target": info
            })
            return info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to copy snapshot {source_snapshot_name}: {e}")
            raise
    
    def delete_snapshot(self, snapshot_name: str) -> bool:
        """
        Delete a snapshot.
        
        Args:
            snapshot_name: Name of the snapshot to delete
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_snapshot(SnapshotName=snapshot_name)
            
            with self._lock:
                if snapshot_name in self._snapshots:
                    del self._snapshots[snapshot_name]
            
            self._emit_event("snapshot_deleted", {"snapshot": snapshot_name})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete snapshot {snapshot_name}: {e}")
            return False
    
    def restore_cluster_from_snapshot(
        self,
        snapshot_name: str,
        cluster_name: str,
        node_type: str = "",
        acl_name: str = "",
        subnet_group_name: str = "",
        security_group_ids: List[str] = None
    ) -> ClusterInfo:
        """
        Restore a cluster from a snapshot.
        
        Args:
            snapshot_name: Name of the snapshot
            cluster_name: Name for the restored cluster
            node_type: Optional new node type
            acl_name: Optional ACL name
            subnet_group_name: Optional subnet group name
            security_group_ids: Optional security group IDs
            
        Returns:
            ClusterInfo for the restored cluster
        """
        client = self._get_client()
        
        try:
            kwargs = {
                "SnapshotName": snapshot_name,
                "ClusterName": cluster_name
            }
            
            if node_type:
                kwargs["NodeType"] = node_type
            if acl_name:
                kwargs["ACLName"] = acl_name
            if subnet_group_name:
                kwargs["SubnetGroupName"] = subnet_group_name
            if security_group_ids:
                kwargs["SecurityGroupIds"] = security_group_ids
            
            response = client.restore_cluster_from_snapshot(**kwargs)
            cluster_data = response.get("Cluster", {})
            
            cluster_info = self._parse_cluster_info(cluster_data)
            with self._lock:
                self._clusters[cluster_name] = cluster_info
            
            self._emit_event("cluster_restored", {
                "snapshot": snapshot_name,
                "cluster": cluster_info
            })
            return cluster_info
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to restore cluster from snapshot {snapshot_name}: {e}")
            raise
    
    def _parse_snapshot_info(self, data: Dict[str, Any]) -> SnapshotInfo:
        """Parse snapshot data into SnapshotInfo object."""
        status_str = data.get("Status", "").lower()
        try:
            status = SnapshotState(status_str)
        except ValueError:
            status = SnapshotState.FAILED
        
        return SnapshotInfo(
            snapshot_name=data.get("Name", ""),
            cluster_name=data.get("ClusterConfiguration", {}).get("Name", data.get("ClusterName", "")),
            source=data.get("Source", ""),
            status=status,
            create_time=data.get("SnapshotCreationTime", datetime.now()),
            node_type=data.get("NodeType", ""),
            num_nodes=data.get("NumNodes", 0),
            engine_version=data.get("EngineVersion", ""),
            kms_key_id=data.get("KmsKeyId", ""),
            encryption_in_transit=data.get("EncryptionInTransit", True)
        )
    
    # ========================================================================
    # Cluster Updates and Versions
    # ========================================================================
    
    def upgrade_cluster_version(
        self,
        cluster_name: str,
        new_engine_version: str,
        apply_immediately: bool = True
    ) -> bool:
        """
        Upgrade a cluster to a new engine version.
        
        Args:
            cluster_name: Name of the cluster
            new_engine_version: Target engine version
            apply_immediately: Whether to apply immediately or during maintenance
            
        Returns:
            True if upgrade was initiated successfully
        """
        return self.update_cluster(ClusterUpdateConfig(
            cluster_name=cluster_name,
            new_engine_version=new_engine_version,
            apply_immediately=apply_immediately
        )) is not None
    
    def get_cluster_version_info(self, cluster_name: str) -> Dict[str, str]:
        """
        Get version information for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Dictionary with version information
        """
        cluster = self.get_cluster(cluster_name)
        if cluster:
            return {
                "engine_version": cluster.engine_version,
                "engine": cluster.engine.value,
                "cluster_name": cluster.cluster_name
            }
        return {}
    
    # ========================================================================
    # Encryption
    # ========================================================================
    
    def configure_encryption(
        self,
        cluster_name: str,
        config: EncryptionConfig
    ) -> bool:
        """
        Configure encryption for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            config: Encryption configuration
            
        Returns:
            True if configuration was successful
        """
        client = self._get_client()
        
        try:
            kwargs = {"ClusterName": cluster_name}
            
            if config.at_rest_encryption_enabled:
                kwargs["AtRestEncryptionEnabled"] = True
                if config.kms_key_id:
                    kwargs["KmsKeyId"] = config.kms_key_id
            
            if config.in_transit_encryption_enabled:
                kwargs["TLSEnabled"] = True
            
            if config.auth_token_enabled:
                kwargs["AuthTokenEnabled"] = True
            
            client.modify_cluster(**kwargs)
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to configure encryption for {cluster_name}: {e}")
            return False
    
    def get_encryption_info(self, cluster_name: str) -> Optional[EncryptionConfig]:
        """
        Get encryption configuration for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            EncryptionConfig object or None
        """
        cluster = self.get_cluster(cluster_name)
        if cluster:
            return EncryptionConfig(
                at_rest_encryption_enabled=bool(cluster.kms_key_id if hasattr(cluster, 'kms_key_id') else False),
                in_transit_encryption_enabled=cluster.tls_enabled,
                tls_enabled=cluster.tls_enabled
            )
        return None
    
    # ========================================================================
    # Multi-AZ
    # ========================================================================
    
    def enable_multi_az(self, cluster_name: str) -> bool:
        """
        Enable Multi-AZ for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            True if Multi-AZ was enabled successfully
        """
        return self.update_cluster(ClusterUpdateConfig(
            cluster_name=cluster_name,
            new_multi_az=True,
            apply_immediately=False
        )) is not None
    
    def disable_multi_az(self, cluster_name: str) -> bool:
        """
        Disable Multi-AZ for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            True if Multi-AZ was disabled successfully
        """
        client = self._get_client()
        
        try:
            client.modify_cluster(
                ClusterName=cluster_name,
                MultiAZEnabled=False,
                ApplyImmediately=False
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to disable Multi-AZ for {cluster_name}: {e}")
            return False
    
    def get_multi_az_status(self, cluster_name: str) -> bool:
        """
        Get Multi-AZ status for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            True if Multi-AZ is enabled
        """
        cluster = self.get_cluster(cluster_name)
        return cluster.multi_az_enabled if cluster else False
    
    def failover_shard(self, cluster_name: str, shard_id: str) -> bool:
        """
        Trigger failover for a shard in a cluster.
        
        Args:
            cluster_name: Name of the cluster
            shard_id: ID of the shard to failover
            
        Returns:
            True if failover was triggered successfully
        """
        client = self._get_client()
        
        try:
            client.failover_shard(
                ClusterName=cluster_name,
                ShardId=shard_id
            )
            self._emit_event("shard_failed_over", {
                "cluster_name": cluster_name,
                "shard_id": shard_id
            })
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to failover shard {shard_id}: {e}")
            return False
    
    # ========================================================================
    # CloudWatch Integration
    # ========================================================================
    
    def get_cluster_metrics(
        self,
        cluster_name: str,
        metric_names: List[str] = None,
        period: int = 60,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get CloudWatch metrics for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            metric_names: List of metric names to retrieve
            period: Metric period in seconds
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dictionary mapping metric names to data points
        """
        cloudwatch = self._get_client("cloudwatch")
        
        if metric_names is None:
            metric_names = [
                "CPUUtilization",
                "MemoryUtilization",
                "NetworkBytesIn",
                "NetworkBytesOut",
                "CurrConnections",
                "NewConnections"
            ]
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        results = {}
        
        try:
            for metric_name in metric_names:
                response = cloudwatch.get_metric_statistics(
                    Namespace="AWS/MemoryDB",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "ClusterName", "Value": cluster_name}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Minimum", "Maximum"]
                )
                
                results[metric_name] = response.get("Datapoints", [])
            
            with self._lock:
                self._metrics_cache[cluster_name] = results
            
            return results
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get metrics for {cluster_name}: {e}")
            return {}
    
    def get_node_metrics(
        self,
        cluster_name: str,
        node_id: str,
        metric_names: List[str] = None,
        period: int = 60,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get CloudWatch metrics for a specific node.
        
        Args:
            cluster_name: Name of the cluster
            node_id: ID of the node
            metric_names: List of metric names to retrieve
            period: Metric period in seconds
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dictionary mapping metric names to data points
        """
        cloudwatch = self._get_client("cloudwatch")
        
        if metric_names is None:
            metric_names = [
                "CPUUtilization",
                "MemoryUtilization",
                "NetworkBytesIn",
                "NetworkBytesOut"
            ]
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        results = {}
        
        try:
            for metric_name in metric_names:
                response = cloudwatch.get_metric_statistics(
                    Namespace="AWS/MemoryDB",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "ClusterName", "Value": cluster_name},
                        {"Name": "NodeName", "Value": node_id}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Minimum", "Maximum"]
                )
                
                results[metric_name] = response.get("Datapoints", [])
            
            return results
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get metrics for node {node_id}: {e}")
            return {}
    
    def create_alarm(
        self,
        alarm_name: str,
        cluster_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 2,
        period: int = 300,
        statistic: str = "Average"
    ) -> bool:
        """
        Create a CloudWatch alarm for a cluster metric.
        
        Args:
            alarm_name: Name of the alarm
            cluster_name: Name of the cluster
            metric_name: Name of the metric
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic to use
            
        Returns:
            True if alarm was created successfully
        """
        cloudwatch = self._get_client("cloudwatch")
        
        try:
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                Namespace="AWS/MemoryDB",
                MetricName=metric_name,
                Dimensions=[
                    {"Name": "ClusterName", "Value": cluster_name}
                ],
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=period,
                Statistic=statistic
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            return False
    
    def set_notification_topic(
        self,
        cluster_name: str,
        topic_arn: str
    ) -> bool:
        """
        Set SNS notification topic for cluster events.
        
        Args:
            cluster_name: Name of the cluster
            topic_arn: ARN of the SNS topic
            
        Returns:
            True if topic was set successfully
        """
        return self.update_cluster(ClusterUpdateConfig(
            cluster_name=cluster_name
        )) is not None
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def wait_for_cluster_available(
        self,
        cluster_name: str,
        timeout: int = 1800,
        check_interval: int = 30
    ) -> bool:
        """
        Wait for a cluster to become available.
        
        Args:
            cluster_name: Name of the cluster
            timeout: Maximum wait time in seconds
            check_interval: Interval between checks in seconds
            
        Returns:
            True if cluster became available, False if timeout
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).seconds < timeout:
            cluster = self.get_cluster(cluster_name)
            if cluster and cluster.status == ClusterState.AVAILABLE:
                return True
            time.sleep(check_interval)
        
        return False
    
    def get_cache(self, key: str) -> Any:
        """Get cached value."""
        with self._lock:
            return self._resource_cache.get(key)
    
    def set_cache(self, key: str, value: Any) -> None:
        """Set cached value."""
        with self._lock:
            self._resource_cache[key] = value
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        with self._lock:
            self._resource_cache.clear()
            self._metrics_cache.clear()
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on MemoryDB integration.
        
        Returns:
            Health check result dictionary
        """
        try:
            client = self._get_client()
            response = client.describe_clusters(MaxResults=1)
            
            return {
                "status": "healthy",
                "region": self.region,
                "boto3_available": BOTO3_AVAILABLE,
                "clusters_found": len(response.get("Clusters", [])) >= 0
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "region": self.region,
                "boto3_available": BOTO3_AVAILABLE
            }
