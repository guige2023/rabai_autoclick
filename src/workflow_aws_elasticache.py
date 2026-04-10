"""
AWS ElastiCache Integration Module for Workflow System

Implements an ElastiCacheIntegration class with:
1. Redis clusters: Create/manage Redis clusters
2. Memcached clusters: Create/manage Memcached clusters
3. Parameter groups: Manage parameter groups
4. Subnet groups: Manage subnet groups
5. Security groups: Manage security groups
6. Snapshots: Create/manage snapshots
7. Global replication: Global datastores
8. Serverless: ElastiCache Serverless
9. Valkey clusters: Create/manage Valkey clusters
10. CloudWatch integration: Metrics and monitoring

Commit: 'feat(aws-elasticache): add AWS ElastiCache with Redis, Memcached, Valkey clusters, parameter groups, subnet groups, security groups, snapshots, global replication, serverless, CloudWatch'
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


class CacheEngine(Enum):
    """Supported ElastiCache engine types."""
    REDIS = "redis"
    MEMCACHED = "memcached"
    VALKEY = "valkey"


class CacheInstanceState(Enum):
    """ElastiCache cluster/node states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    MODIFYING = "modifying"
    REBOOTING = "rebooting"
    FAILING = "failing"
    FAILED = "failed"


class SnapshotState(Enum):
    """ElastiCache snapshot states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    FAILED = "failed"
    RESTORING = "restoring"


class ReplicationState(Enum):
    """Global replication state."""
    CREATING = "creating"
    ACTIVE = "active"
    DELETING = "deleting"
    MODIFYING = "modifying"


class ServerlessCacheState(Enum):
    """Serverless cache states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    FAILED = "failed"


@dataclass
class CacheClusterConfig:
    """Configuration for cache cluster creation."""
    cluster_id: str
    engine: CacheEngine
    node_type: str = "cache.t3.medium"
    num_nodes: int = 1
    parameter_group_name: str = "default"
    subnet_group_name: str = "default"
    security_group_ids: List[str] = field(default_factory=list)
    port: int = 6379
    maintenance_window: str = "mon:03:00-mon:04:00"
    notification_topic_arn: str = ""
    snapshot_retention_limit: int = 0
    snapshot_window: str = "06:00-07:00"
    auto_minor_version_upgrade: bool = True
    at_rest_encryption_enabled: bool = False
    transit_encryption_enabled: bool = False
    auth_token_enabled: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    

@dataclass
class CacheNodeInfo:
    """Information about a cache node."""
    node_id: str
    node_type: str
    status: str
    port: int
    availability_zone: str
    create_time: datetime
    endpoint: str = ""
    

@dataclass
class SnapshotInfo:
    """Information about a cache snapshot."""
    snapshot_name: str
    cache_cluster_id: str
    engine: CacheEngine
    snapshot_status: SnapshotState
    create_time: datetime
    node_type: str
    num_nodes: int
    engine_version: str
    source: str = ""
    

@dataclass
class GlobalReplicationInfo:
    """Information about global replication datastore."""
    global_replication_group_id: str
    global_replication_group_description: str
    status: ReplicationState
    primary_replication_group_id: str
    secondary_replication_group_ids: List[str] = field(default_factory=list)
    

@dataclass
class ServerlessCacheInfo:
    """Information about serverless cache."""
    cache_name: str
    status: ServerlessCacheState
    create_time: datetime
    endpoint: str = ""
    port: int = 6379
    engine: CacheEngine = CacheEngine.REDIS


class ElastiCacheIntegration:
    """
    AWS ElastiCache integration providing management of Redis, Memcached, 
    and Valkey clusters, parameter groups, subnet groups, security groups,
    snapshots, global replication, serverless caches, and CloudWatch monitoring.
    """
    
    def __init__(self, region: str = "us-east-1", profile: Optional[str] = None):
        """
        Initialize ElastiCache integration.
        
        Args:
            region: AWS region for ElastiCache operations
            profile: Optional AWS profile name for boto3 session
        """
        self.region = region
        self.profile = profile
        self._clients = {}
        self._resource_cache = {}
        self._lock = threading.Lock()
        self._clusters = {}
        self._snapshots = {}
        self._parameter_groups = {}
        self._subnet_groups = {}
        self._global_replications = {}
        self._serverless_caches = {}
        
    def _get_client(self, service: str = "elasticache") -> Any:
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
    
    def _get_resource(self, service: str = "elasticache") -> Any:
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
    
    # =========================================================================
    # REDIS CLUSTERS
    # =========================================================================
    
    def create_redis_cluster(self, config: CacheClusterConfig) -> Dict[str, Any]:
        """
        Create a Redis cluster.
        
        Args:
            config: CacheClusterConfig with cluster settings
            
        Returns:
            Dict with cluster creation response
        """
        client = self._get_client()
        
        kwargs = {
            "CacheClusterId": config.cluster_id,
            "Engine": "redis",
            "EngineVersion": "7.0",
            "CacheNodeType": config.node_type,
            "NumCacheNodes": config.num_nodes,
            "CacheParameterGroupName": config.parameter_group_name,
            "CacheSubnetGroupName": config.subnet_group_name,
            "SecurityGroupIds": config.security_group_ids,
            "Port": config.port,
            "PreferredMaintenanceWindow": config.maintenance_window,
            "SnapshotRetentionLimit": config.snapshot_retention_limit,
            "SnapshotWindow": config.snapshot_window,
            "AutoMinorVersionUpgrade": config.auto_minor_version_upgrade,
            "Tags": [{"Key": k, "Value": v} for k, v in config.tags.items()],
        }
        
        if config.notification_topic_arn:
            kwargs["NotificationTopicArn"] = config.notification_topic_arn
        
        if config.at_rest_encryption_enabled:
            kwargs["AtRestEncryptionEnabled"] = True
            
        if config.transit_encryption_enabled:
            kwargs["TransitEncryptionEnabled"] = True
            
        if config.auth_token_enabled:
            kwargs["AuthToken"] = str(uuid.uuid4())
            kwargs["TransitEncryptionMode"] = "required"
        
        try:
            response = client.create_cache_cluster(**kwargs)
            cluster = response.get("CacheCluster", {})
            self._clusters[config.cluster_id] = cluster
            return {
                "cluster": cluster,
                "status": "creating",
                "cluster_id": config.cluster_id
            }
        except ClientError as e:
            logger.error(f"Failed to create Redis cluster: {e}")
            raise
    
    def get_redis_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get information about a Redis cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with cluster information
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_clusters(
                CacheClusterId=cluster_id,
                ShowCacheNodeInfo=True
            )
            clusters = response.get("CacheClusters", [])
            if clusters:
                return clusters[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get Redis cluster: {e}")
            raise
    
    def list_redis_clusters(self) -> List[Dict[str, Any]]:
        """
        List all Redis clusters in the region.
        
        Returns:
            List of Redis cluster information
        """
        client = self._get_client()
        clusters = []
        
        try:
            paginator = client.get_paginator("describe_cache_clusters")
            for page in paginator.paginate(Engine="redis"):
                clusters.extend(page.get("CacheClusters", []))
            return clusters
        except ClientError as e:
            logger.error(f"Failed to list Redis clusters: {e}")
            raise
    
    def delete_redis_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Delete a Redis cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with deletion response
        """
        client = self._get_client()
        
        try:
            response = client.delete_cache_cluster(CacheClusterId=cluster_id)
            if cluster_id in self._clusters:
                del self._clusters[cluster_id]
            return response.get("CacheCluster", {})
        except ClientError as e:
            logger.error(f"Failed to delete Redis cluster: {e}")
            raise
    
    def modify_redis_cluster(self, cluster_id: str, 
                           modifications: Dict[str, Any]) -> Dict[str, Any]:
        """
        Modify a Redis cluster's attributes.
        
        Args:
            cluster_id: The cluster identifier
            modifications: Dict of modifications to apply
            
        Returns:
            Dict with modified cluster information
        """
        client = self._get_client()
        
        kwargs = {"CacheClusterId": cluster_id}
        
        if "node_type" in modifications:
            kwargs["CacheNodeType"] = modifications["node_type"]
        if "num_nodes" in modifications:
            kwargs["NumCacheNodes"] = modifications["num_nodes"]
        if "parameter_group" in modifications:
            kwargs["CacheParameterGroupName"] = modifications["parameter_group"]
        if "security_groups" in modifications:
            kwargs["SecurityGroupIds"] = modifications["security_groups"]
        if "notification_topic" in modifications:
            kwargs["NotificationTopicArn"] = modifications["notification_topic"]
        if "auto_minor_version_upgrade" in modifications:
            kwargs["AutoMinorVersionUpgrade"] = modifications["auto_minor_version_upgrade"]
        if "maintenance_window" in modifications:
            kwargs["PreferredMaintenanceWindow"] = modifications["maintenance_window"]
        if "snapshot_retention" in modifications:
            kwargs["SnapshotRetentionLimit"] = modifications["snapshot_retention"]
        if "snapshot_window" in modifications:
            kwargs["SnapshotWindow"] = modifications["snapshot_window"]
        
        try:
            response = client.modify_cache_cluster(**kwargs)
            return response.get("CacheCluster", {})
        except ClientError as e:
            logger.error(f"Failed to modify Redis cluster: {e}")
            raise
    
    def reboot_redis_cluster(self, cluster_id: str, 
                            node_ids: Optional[List[str]] = None) -> bool:
        """
        Reboot cache nodes in a Redis cluster.
        
        Args:
            cluster_id: The cluster identifier
            node_ids: Optional list of specific node IDs to reboot
            
        Returns:
            True if reboot initiated successfully
        """
        client = self._get_client()
        
        kwargs = {"CacheClusterId": cluster_id}
        if node_ids:
            kwargs["CacheNodeIdsToReboot"] = node_ids
        
        try:
            client.reboot_cache_cluster(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to reboot Redis cluster: {e}")
            raise
    
    # =========================================================================
    # MEMCACHED CLUSTERS
    # =========================================================================
    
    def create_memcached_cluster(self, config: CacheClusterConfig) -> Dict[str, Any]:
        """
        Create a Memcached cluster.
        
        Args:
            config: CacheClusterConfig with cluster settings
            
        Returns:
            Dict with cluster creation response
        """
        client = self._get_client()
        
        kwargs = {
            "CacheClusterId": config.cluster_id,
            "Engine": "memcached",
            "EngineVersion": "1.6.12",
            "CacheNodeType": config.node_type,
            "NumCacheNodes": config.num_nodes,
            "CacheParameterGroupName": config.parameter_group_name,
            "CacheSubnetGroupName": config.subnet_group_name,
            "SecurityGroupIds": config.security_group_ids,
            "Port": config.port,
            "PreferredMaintenanceWindow": config.maintenance_window,
            "AutoMinorVersionUpgrade": config.auto_minor_version_upgrade,
            "Tags": [{"Key": k, "Value": v} for k, v in config.tags.items()],
        }
        
        if config.notification_topic_arn:
            kwargs["NotificationTopicArn"] = config.notification_topic_arn
        
        try:
            response = client.create_cache_cluster(**kwargs)
            cluster = response.get("CacheCluster", {})
            self._clusters[config.cluster_id] = cluster
            return {
                "cluster": cluster,
                "status": "creating",
                "cluster_id": config.cluster_id
            }
        except ClientError as e:
            logger.error(f"Failed to create Memcached cluster: {e}")
            raise
    
    def get_memcached_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get information about a Memcached cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with cluster information
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_clusters(
                CacheClusterId=cluster_id,
                ShowCacheNodeInfo=True
            )
            clusters = response.get("CacheClusters", [])
            if clusters:
                return clusters[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get Memcached cluster: {e}")
            raise
    
    def list_memcached_clusters(self) -> List[Dict[str, Any]]:
        """
        List all Memcached clusters in the region.
        
        Returns:
            List of Memcached cluster information
        """
        client = self._get_client()
        clusters = []
        
        try:
            paginator = client.get_paginator("describe_cache_clusters")
            for page in paginator.paginate(Engine="memcached"):
                clusters.extend(page.get("CacheClusters", []))
            return clusters
        except ClientError as e:
            logger.error(f"Failed to list Memcached clusters: {e}")
            raise
    
    def delete_memcached_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Delete a Memcached cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with deletion response
        """
        client = self._get_client()
        
        try:
            response = client.delete_cache_cluster(CacheClusterId=cluster_id)
            if cluster_id in self._clusters:
                del self._clusters[cluster_id]
            return response.get("CacheCluster", {})
        except ClientError as e:
            logger.error(f"Failed to delete Memcached cluster: {e}")
            raise
    
    # =========================================================================
    # VALKEY CLUSTERS
    # =========================================================================
    
    def create_valkey_cluster(self, config: CacheClusterConfig) -> Dict[str, Any]:
        """
        Create a Valkey cluster (successor to Redis with open source backing).
        
        Args:
            config: CacheClusterConfig with cluster settings
            
        Returns:
            Dict with cluster creation response
        """
        client = self._get_client()
        
        kwargs = {
            "CacheClusterId": config.cluster_id,
            "Engine": "valkey",
            "EngineVersion": "7.0",
            "CacheNodeType": config.node_type,
            "NumCacheNodes": config.num_nodes,
            "CacheParameterGroupName": config.parameter_group_name,
            "CacheSubnetGroupName": config.subnet_group_name,
            "SecurityGroupIds": config.security_group_ids,
            "Port": config.port,
            "PreferredMaintenanceWindow": config.maintenance_window,
            "SnapshotRetentionLimit": config.snapshot_retention_limit,
            "SnapshotWindow": config.snapshot_window,
            "AutoMinorVersionUpgrade": config.auto_minor_version_upgrade,
            "AtRestEncryptionEnabled": config.at_rest_encryption_enabled,
            "TransitEncryptionEnabled": config.transit_encryption_enabled,
            "Tags": [{"Key": k, "Value": v} for k, v in config.tags.items()],
        }
        
        if config.notification_topic_arn:
            kwargs["NotificationTopicArn"] = config.notification_topic_arn
            
        if config.auth_token_enabled:
            kwargs["AuthToken"] = str(uuid.uuid4())
        
        try:
            response = client.create_cache_cluster(**kwargs)
            cluster = response.get("CacheCluster", {})
            self._clusters[config.cluster_id] = cluster
            return {
                "cluster": cluster,
                "status": "creating",
                "cluster_id": config.cluster_id
            }
        except ClientError as e:
            logger.error(f"Failed to create Valkey cluster: {e}")
            raise
    
    def get_valkey_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get information about a Valkey cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with cluster information
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_clusters(
                CacheClusterId=cluster_id,
                ShowCacheNodeInfo=True
            )
            clusters = response.get("CacheClusters", [])
            if clusters:
                return clusters[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get Valkey cluster: {e}")
            raise
    
    def list_valkey_clusters(self) -> List[Dict[str, Any]]:
        """
        List all Valkey clusters in the region.
        
        Returns:
            List of Valkey cluster information
        """
        client = self._get_client()
        clusters = []
        
        try:
            paginator = client.get_paginator("describe_cache_clusters")
            for page in paginator.paginate(Engine="valkey"):
                clusters.extend(page.get("CacheClusters", []))
            return clusters
        except ClientError as e:
            logger.error(f"Failed to list Valkey clusters: {e}")
            raise
    
    # =========================================================================
    # PARAMETER GROUPS
    # =========================================================================
    
    def create_parameter_group(self, group_name: str, 
                               engine: CacheEngine,
                               description: str = "") -> Dict[str, Any]:
        """
        Create a cache parameter group.
        
        Args:
            group_name: Name for the parameter group
            engine: The engine type (redis, memcached, valkey)
            description: Optional description
            
        Returns:
            Dict with parameter group creation response
        """
        client = self._get_client()
        
        kwargs = {
            "CacheParameterGroupName": group_name,
            "CacheParameterGroupFamily": f"{engine.value}7" if engine != CacheEngine.MEMCACHED else "memcached1.6",
            "Description": description or f"{engine.value.title()} parameter group"
        }
        
        try:
            response = client.create_cache_parameter_group(**kwargs)
            pg = response.get("CacheParameterGroup", {})
            self._parameter_groups[group_name] = pg
            return pg
        except ClientError as e:
            logger.error(f"Failed to create parameter group: {e}")
            raise
    
    def get_parameter_group(self, group_name: str) -> Dict[str, Any]:
        """
        Get information about a cache parameter group.
        
        Args:
            group_name: The parameter group name
            
        Returns:
            Dict with parameter group information
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_parameter_groups(
                CacheParameterGroupName=group_name
            )
            groups = response.get("CacheParameterGroups", [])
            if groups:
                return groups[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get parameter group: {e}")
            raise
    
    def list_parameter_groups(self, engine: Optional[CacheEngine] = None) -> List[Dict[str, Any]]:
        """
        List cache parameter groups.
        
        Args:
            engine: Optional filter by engine type
            
        Returns:
            List of parameter groups
        """
        client = self._get_client()
        groups = []
        
        try:
            if engine:
                family = f"{engine.value}7" if engine != CacheEngine.MEMCACHED else "memcached1.6"
                response = client.describe_cache_parameter_groups(
                    CacheParameterGroupFamily=family
                )
                groups = response.get("CacheParameterGroups", [])
            else:
                paginator = client.get_paginator("describe_cache_parameter_groups")
                for page in paginator.paginate():
                    groups.extend(page.get("CacheParameterGroups", []))
            return groups
        except ClientError as e:
            logger.error(f"Failed to list parameter groups: {e}")
            raise
    
    def modify_parameter_group(self, group_name: str,
                               parameters: Dict[str, Any]) -> bool:
        """
        Modify parameters in a cache parameter group.
        
        Args:
            group_name: The parameter group name
            parameters: Dict of parameter name to value mappings
            
        Returns:
            True if modification was successful
        """
        client = self._get_client()
        
        param_list = [{"ParameterName": k, "ParameterValue": str(v)} 
                      for k, v in parameters.items()]
        
        try:
            client.modify_cache_parameter_group(
                CacheParameterGroupName=group_name,
                ParameterNameValues=param_list
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to modify parameter group: {e}")
            raise
    
    def delete_parameter_group(self, group_name: str) -> bool:
        """
        Delete a cache parameter group.
        
        Args:
            group_name: The parameter group name
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_cache_parameter_group(CacheParameterGroupName=group_name)
            if group_name in self._parameter_groups:
                del self._parameter_groups[group_name]
            return True
        except ClientError as e:
            logger.error(f"Failed to delete parameter group: {e}")
            raise
    
    def reset_parameter_group(self, group_name: str,
                             reset_all: bool = False) -> bool:
        """
        Reset parameters in a cache parameter group to defaults.
        
        Args:
            group_name: The parameter group name
            reset_all: If True, reset all parameters
            
        Returns:
            True if reset was successful
        """
        client = self._get_client()
        
        kwargs = {"CacheParameterGroupName": group_name}
        
        if reset_all:
            kwargs["ParameterNameValues"] = {"ParameterNames": ["*"]}
        
        try:
            client.reset_cache_parameter_group(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to reset parameter group: {e}")
            raise
    
    # =========================================================================
    # SUBNET GROUPS
    # =========================================================================
    
    def create_subnet_group(self, group_name: str,
                            subnet_ids: List[str],
                            description: str = "") -> Dict[str, Any]:
        """
        Create a cache subnet group.
        
        Args:
            group_name: Name for the subnet group
            subnet_ids: List of subnet IDs to include
            description: Optional description
            
        Returns:
            Dict with subnet group creation response
        """
        client = self._get_client()
        
        try:
            response = client.create_cache_subnet_group(
                CacheSubnetGroupName=group_name,
                CacheSubnetGroupDescription=description or f"Subnet group {group_name}",
                SubnetIds=subnet_ids
            )
            sg = response.get("CacheSubnetGroup", {})
            self._subnet_groups[group_name] = sg
            return sg
        except ClientError as e:
            logger.error(f"Failed to create subnet group: {e}")
            raise
    
    def get_subnet_group(self, group_name: str) -> Dict[str, Any]:
        """
        Get information about a cache subnet group.
        
        Args:
            group_name: The subnet group name
            
        Returns:
            Dict with subnet group information
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_subnet_groups(
                CacheSubnetGroupName=group_name
            )
            groups = response.get("CacheSubnetGroups", [])
            if groups:
                return groups[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get subnet group: {e}")
            raise
    
    def list_subnet_groups(self) -> List[Dict[str, Any]]:
        """
        List all cache subnet groups.
        
        Returns:
            List of subnet groups
        """
        client = self._get_client()
        groups = []
        
        try:
            paginator = client.get_paginator("describe_cache_subnet_groups")
            for page in paginator.paginate():
                groups.extend(page.get("CacheSubnetGroups", []))
            return groups
        except ClientError as e:
            logger.error(f"Failed to list subnet groups: {e}")
            raise
    
    def modify_subnet_group(self, group_name: str,
                             subnet_ids: Optional[List[str]] = None,
                             description: Optional[str] = None) -> Dict[str, Any]:
        """
        Modify a cache subnet group.
        
        Args:
            group_name: The subnet group name
            subnet_ids: New list of subnet IDs
            description: New description
            
        Returns:
            Dict with modified subnet group
        """
        client = self._get_client()
        
        kwargs = {"CacheSubnetGroupName": group_name}
        if subnet_ids:
            kwargs["SubnetIds"] = subnet_ids
        if description is not None:
            kwargs["CacheSubnetGroupDescription"] = description
        
        try:
            response = client.modify_cache_subnet_group(**kwargs)
            return response.get("CacheSubnetGroup", {})
        except ClientError as e:
            logger.error(f"Failed to modify subnet group: {e}")
            raise
    
    def delete_subnet_group(self, group_name: str) -> bool:
        """
        Delete a cache subnet group.
        
        Args:
            group_name: The subnet group name
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_cache_subnet_group(CacheSubnetGroupName=group_name)
            if group_name in self._subnet_groups:
                del self._subnet_groups[group_name]
            return True
        except ClientError as e:
            logger.error(f"Failed to delete subnet group: {e}")
            raise
    
    # =========================================================================
    # SECURITY GROUPS
    # =========================================================================
    
    def create_security_group(self, name: str,
                              description: str = "",
                              vpc_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a cache security group.
        
        Args:
            name: Name for the security group
            description: Optional description
            vpc_id: VPC ID to create security group in
            
        Returns:
            Dict with security group creation response
        """
        ec2_client = self._get_client("ec2")
        
        kwargs = {
            "GroupName": name,
            "Description": description or f"Security group {name}",
        }
        
        if vpc_id:
            kwargs["VpcId"] = vpc_id
        
        try:
            response = ec2_client.create_security_group(**kwargs)
            return {
                "GroupId": response.get("GroupId"),
                "GroupName": name,
                "VpcId": vpc_id or ""
            }
        except ClientError as e:
            logger.error(f"Failed to create security group: {e}")
            raise
    
    def authorize_security_group(self, group_id: str,
                                 protocol: str = "tcp",
                                 port: int = 6379,
                                 cidr_ip: str = "0.0.0.0/0") -> bool:
        """
        Authorize ingress on a security group.
        
        Args:
            group_id: The security group ID
            protocol: Protocol (tcp, udp, icmp)
            port: Port or port range
            cidr_ip: CIDR IP range
            
        Returns:
            True if authorization was successful
        """
        ec2_client = self._get_client("ec2")
        
        try:
            ec2_client.authorize_security_group_ingress(
                GroupId=group_id,
                IpPermissions=[{
                    "IpProtocol": protocol,
                    "FromPort": port,
                    "ToPort": port,
                    "IpRanges": [{"CidrIp": cidr_ip}]
                }]
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to authorize security group: {e}")
            raise
    
    def revoke_security_group(self, group_id: str,
                              protocol: str = "tcp",
                              port: int = 6379,
                              cidr_ip: str = "0.0.0.0/0") -> bool:
        """
        Revoke ingress on a security group.
        
        Args:
            group_id: The security group ID
            protocol: Protocol (tcp, udp, icmp)
            port: Port or port range
            cidr_ip: CIDR IP range
            
        Returns:
            True if revocation was successful
        """
        ec2_client = self._get_client("ec2")
        
        try:
            ec2_client.revoke_security_group_ingress(
                GroupId=group_id,
                IpPermissions=[{
                    "IpProtocol": protocol,
                    "FromPort": port,
                    "ToPort": port,
                    "IpRanges": [{"CidrIp": cidr_ip}]
                }]
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to revoke security group: {e}")
            raise
    
    def list_security_groups(self, vpc_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List cache security groups.
        
        Args:
            vpc_id: Optional VPC ID to filter by
            
        Returns:
            List of security groups
        """
        ec2_client = self._get_client("ec2")
        
        kwargs = {"GroupNames": [f"default"]} if not vpc_id else {"Filters": [{"Name": "vpc-id", "Values": [vpc_id]}]}
        
        try:
            response = ec2_client.describe_security_groups(**kwargs)
            return response.get("SecurityGroups", [])
        except ClientError as e:
            logger.error(f"Failed to list security groups: {e}")
            raise
    
    def delete_security_group(self, group_id: str) -> bool:
        """
        Delete a security group.
        
        Args:
            group_id: The security group ID
            
        Returns:
            True if deletion was successful
        """
        ec2_client = self._get_client("ec2")
        
        try:
            ec2_client.delete_security_group(GroupId=group_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete security group: {e}")
            raise
    
    # =========================================================================
    # SNAPSHOTS
    # =========================================================================
    
    def create_snapshot(self, snapshot_name: str,
                        cluster_id: str,
                        replicate: bool = False) -> Dict[str, Any]:
        """
        Create a snapshot of a cache cluster.
        
        Args:
            snapshot_name: Name for the snapshot
            cluster_id: Source cluster ID
            replicate: Whether to replicate across regions
            
        Returns:
            Dict with snapshot creation response
        """
        client = self._get_client()
        
        kwargs = {
            "SnapshotName": snapshot_name,
            "CacheClusterId": cluster_id
        }
        
        try:
            response = client.create_snapshot(**kwargs)
            snapshot = response.get("Snapshot", {})
            self._snapshots[snapshot_name] = snapshot
            return snapshot
        except ClientError as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise
    
    def create_manual_snapshot(self, snapshot_name: str,
                               cluster_id: str) -> Dict[str, Any]:
        """
        Create a manual snapshot (via backup API).
        
        Args:
            snapshot_name: Name for the snapshot
            cluster_id: Source cluster ID
            
        Returns:
            Dict with snapshot information
        """
        return self.create_snapshot(snapshot_name, cluster_id)
    
    def get_snapshot(self, snapshot_name: str) -> Dict[str, Any]:
        """
        Get information about a snapshot.
        
        Args:
            snapshot_name: The snapshot name
            
        Returns:
            Dict with snapshot information
        """
        client = self._get_client()
        
        try:
            response = client.describe_snapshots(
                SnapshotName=snapshot_name
            )
            snapshots = response.get("Snapshots", [])
            if snapshots:
                return snapshots[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get snapshot: {e}")
            raise
    
    def list_snapshots(self, 
                       cluster_id: Optional[str] = None,
                       engine: Optional[CacheEngine] = None) -> List[Dict[str, Any]]:
        """
        List cache snapshots.
        
        Args:
            cluster_id: Optional cluster ID to filter by
            engine: Optional engine type to filter by
            
        Returns:
            List of snapshots
        """
        client = self._get_client()
        snapshots = []
        
        try:
            kwargs = {}
            if cluster_id:
                kwargs["CacheClusterId"] = cluster_id
            
            paginator = client.get_paginator("describe_snapshots")
            for page in paginator.paginate(**kwargs):
                for snap in page.get("Snapshots", []):
                    if engine is None or snap.get("Engine") == engine.value:
                        snapshots.append(snap)
            return snapshots
        except ClientError as e:
            logger.error(f"Failed to list snapshots: {e}")
            raise
    
    def copy_snapshot(self, source_snapshot: str,
                      target_snapshot: str,
                      target_region: Optional[str] = None) -> Dict[str, Any]:
        """
        Copy a snapshot.
        
        Args:
            source_snapshot: Source snapshot name
            target_snapshot: Target snapshot name
            target_region: Optional target region for cross-region copy
            
        Returns:
            Dict with copy response
        """
        client = self._get_client()
        
        kwargs = {
            "SourceSnapshotName": source_snapshot,
            "TargetSnapshotName": target_snapshot
        }
        
        if target_region:
            kwargs["TargetBucket"] = target_region
        
        try:
            response = client.copy_snapshot(**kwargs)
            return response.get("Snapshot", {})
        except ClientError as e:
            logger.error(f"Failed to copy snapshot: {e}")
            raise
    
    def delete_snapshot(self, snapshot_name: str) -> bool:
        """
        Delete a snapshot.
        
        Args:
            snapshot_name: The snapshot name
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_snapshot(SnapshotName=snapshot_name)
            if snapshot_name in self._snapshots:
                del self._snapshots[snapshot_name]
            return True
        except ClientError as e:
            logger.error(f"Failed to delete snapshot: {e}")
            raise
    
    def restore_snapshot(self, snapshot_name: str,
                         cluster_id: str) -> Dict[str, Any]:
        """
        Restore a snapshot to a new cluster.
        
        Args:
            snapshot_name: The snapshot name
            cluster_id: New cluster ID to restore to
            
        Returns:
            Dict with restore response
        """
        client = self._get_client()
        
        try:
            response = client.restore_from_snapshot(
                CacheClusterId=cluster_id,
                SnapshotName=snapshot_name
            )
            return response.get("CacheCluster", {})
        except ClientError as e:
            logger.error(f"Failed to restore snapshot: {e}")
            raise
    
    # =========================================================================
    # GLOBAL REPLICATION (Global Datastores)
    # =========================================================================
    
    def create_global_replication(self,
                                  primary_cluster_id: str,
                                  description: str = "") -> Dict[str, Any]:
        """
        Create a global replication datastore.
        
        Args:
            primary_cluster_id: Primary cluster ID
            description: Optional description
            
        Returns:
            Dict with global replication info
        """
        client = self._get_client()
        
        kwargs = {
            "PrimaryReplicationGroupId": primary_cluster_id,
            "GlobalReplicationGroupDescription": description or f"Global replication for {primary_cluster_id}"
        }
        
        try:
            response = client.create_global_replication_group(**kwargs)
            grg = response.get("GlobalReplicationGroup", {})
            grg_id = grg.get("GlobalReplicationGroupId", "")
            self._global_replications[grg_id] = grg
            return grg
        except ClientError as e:
            logger.error(f"Failed to create global replication: {e}")
            raise
    
    def get_global_replication(self, 
                               global_replication_group_id: str) -> Dict[str, Any]:
        """
        Get information about a global replication group.
        
        Args:
            global_replication_group_id: The global replication group ID
            
        Returns:
            Dict with global replication info
        """
        client = self._get_client()
        
        try:
            response = client.describe_global_replication_groups(
                GlobalReplicationGroupId=global_replication_group_id
            )
            groups = response.get("GlobalReplicationGroups", [])
            if groups:
                return groups[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get global replication: {e}")
            raise
    
    def list_global_replications(self) -> List[Dict[str, Any]]:
        """
        List all global replication groups.
        
        Returns:
            List of global replication groups
        """
        client = self._get_client()
        groups = []
        
        try:
            paginator = client.get_paginator("describe_global_replication_groups")
            for page in paginator.paginate():
                groups.extend(page.get("GlobalReplicationGroups", []))
            return groups
        except ClientError as e:
            logger.error(f"Failed to list global replications: {e}")
            raise
    
    def add_replication_member(self,
                              global_replication_group_id: str,
                              replication_group_id: str,
                              region: str) -> Dict[str, Any]:
        """
        Add a secondary replication group to global replication.
        
        Args:
            global_replication_group_id: The global replication group ID
            replication_group_id: The replication group to add
            region: AWS region of the secondary
            
        Returns:
            Dict with updated global replication info
        """
        client = self._get_client()
        
        try:
            response = client.describe_replication_groups(
                ReplicationGroupId=replication_group_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to add replication member: {e}")
            raise
    
    def remove_replication_member(self,
                                  global_replication_group_id: str,
                                  replication_group_id: str) -> bool:
        """
        Remove a secondary replication group from global replication.
        
        Args:
            global_replication_group_id: The global replication group ID
            replication_group_id: The replication group to remove
            
        Returns:
            True if removal was successful
        """
        client = self._get_client()
        
        try:
            client.remove_member_from_global_replication_group(
                GlobalReplicationGroupId=global_replication_group_id,
                ReplicationGroupId=replication_group_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to remove replication member: {e}")
            raise
    
    def delete_global_replication(self, 
                                  global_replication_group_id: str) -> bool:
        """
        Delete a global replication group.
        
        Args:
            global_replication_group_id: The global replication group ID
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_global_replication_group(
                GlobalReplicationGroupId=global_replication_group_id
            )
            if global_replication_group_id in self._global_replications:
                del self._global_replications[global_replication_group_id]
            return True
        except ClientError as e:
            logger.error(f"Failed to delete global replication: {e}")
            raise
    
    # =========================================================================
    # SERVERLESS CACHES
    # =========================================================================
    
    def create_serverless_cache(self,
                                cache_name: str,
                                engine: CacheEngine = CacheEngine.REDIS,
                                description: str = "") -> Dict[str, Any]:
        """
        Create an ElastiCache Serverless cache.
        
        Args:
            cache_name: Name for the serverless cache
            engine: Cache engine type
            description: Optional description
            
        Returns:
            Dict with serverless cache info
        """
        client = self._get_client()
        
        kwargs = {
            "ServerlessCacheName": cache_name,
            "Engine": engine.value,
            "Description": description or f"Serverless {engine.value} cache"
        }
        
        try:
            response = client.create_serverless_cache(**kwargs)
            cache = response.get("ServerlessCache", {})
            self._serverless_caches[cache_name] = cache
            return cache
        except ClientError as e:
            logger.error(f"Failed to create serverless cache: {e}")
            raise
    
    def get_serverless_cache(self, cache_name: str) -> Dict[str, Any]:
        """
        Get information about a serverless cache.
        
        Args:
            cache_name: The serverless cache name
            
        Returns:
            Dict with serverless cache info
        """
        client = self._get_client()
        
        try:
            response = client.describe_serverless_caches(
                ServerlessCacheName=cache_name
            )
            caches = response.get("ServerlessCaches", [])
            if caches:
                return caches[0]
            return {}
        except ClientError as e:
            logger.error(f"Failed to get serverless cache: {e}")
            raise
    
    def list_serverless_caches(self,
                               engine: Optional[CacheEngine] = None) -> List[Dict[str, Any]]:
        """
        List all serverless caches.
        
        Args:
            engine: Optional engine filter
            
        Returns:
            List of serverless caches
        """
        client = self._get_client()
        caches = []
        
        try:
            paginator = client.get_paginator("describe_serverless_caches")
            for page in paginator.paginate():
                for cache in page.get("ServerlessCaches", []):
                    if engine is None or cache.get("Engine") == engine.value:
                        caches.append(cache)
            return caches
        except ClientError as e:
            logger.error(f"Failed to list serverless caches: {e}")
            raise
    
    def delete_serverless_cache(self, cache_name: str) -> bool:
        """
        Delete a serverless cache.
        
        Args:
            cache_name: The serverless cache name
            
        Returns:
            True if deletion was successful
        """
        client = self._get_client()
        
        try:
            client.delete_serverless_cache(ServerlessCacheName=cache_name)
            if cache_name in self._serverless_caches:
                del self._serverless_caches[cache_name]
            return True
        except ClientError as e:
            logger.error(f"Failed to delete serverless cache: {e}")
            raise
    
    def modify_serverless_cache(self, cache_name: str,
                                cache_usage_limit: Optional[Dict[str, Any]] = None,
                                description: Optional[str] = None) -> Dict[str, Any]:
        """
        Modify a serverless cache.
        
        Args:
            cache_name: The serverless cache name
            cache_usage_limit: Optional usage limit configuration
            description: Optional new description
            
        Returns:
            Dict with modified serverless cache info
        """
        client = self._get_client()
        
        kwargs = {"ServerlessCacheName": cache_name}
        if description is not None:
            kwargs["Description"] = description
        if cache_usage_limit:
            kwargs["CacheUsageLimits"] = cache_usage_limit
        
        try:
            response = client.modify_serverless_cache(**kwargs)
            return response.get("ServerlessCache", {})
        except ClientError as e:
            logger.error(f"Failed to modify serverless cache: {e}")
            raise
    
    # =========================================================================
    # CLOUDWATCH METRICS AND MONITORING
    # =========================================================================
    
    def get_metrics(self, cluster_id: str,
                    metric_names: List[str],
                    period: int = 60,
                    start_time: Optional[datetime] = None,
                    end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a cache cluster.
        
        Args:
            cluster_id: The cluster identifier
            metric_names: List of metric names to retrieve
            period: Metric period in seconds
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dict with metric data
        """
        cloudwatch = self._get_client("cloudwatch")
        
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        namespace = "AWS/ElastiCache"
        
        metrics_data = {}
        for metric in metric_names:
            try:
                response = cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric,
                    Dimensions=[{"Name": "CacheClusterId", "Value": cluster_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum"]
                )
                metrics_data[metric] = response.get("Datapoints", [])
            except ClientError as e:
                logger.error(f"Failed to get metric {metric}: {e}")
                metrics_data[metric] = []
        
        return metrics_data
    
    def get_recommended_alarms(self) -> List[Dict[str, str]]:
        """
        Get recommended CloudWatch alarms for ElastiCache.
        
        Returns:
            List of recommended alarm configurations
        """
        return [
            {
                "AlarmName": "CPUUtilization-High",
                "MetricName": "CPUUtilization",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 80,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 60,
                "EvaluationPeriods": 5,
                "Statistic": "Average"
            },
            {
                "AlarmName": "MemoryUtilization-High",
                "MetricName": "DatabaseMemoryUsagePercentage",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 80,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 60,
                "EvaluationPeriods": 5,
                "Statistic": "Average"
            },
            {
                "AlarmName": "EngineCPUUtilization-High",
                "MetricName": "EngineCPUUtilization",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 75,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 60,
                "EvaluationPeriods": 5,
                "Statistic": "Average"
            },
            {
                "AlarmName": "SwapUsage-High",
                "MetricName": "SwapUsage",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 52428800,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 60,
                "EvaluationPeriods": 5,
                "Statistic": "Average"
            },
            {
                "AlarmName": "Evictions-High",
                "MetricName": "Evictions",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 1000,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 300,
                "EvaluationPeriods": 3,
                "Statistic": "Sum"
            },
            {
                "AlarmName": "ReplicationLag-High",
                "MetricName": "ReplicationLag",
                "Namespace": "AWS/ElastiCache",
                "Threshold": 30,
                "ComparisonOperator": "GreaterThanThreshold",
                "Period": 60,
                "EvaluationPeriods": 5,
                "Statistic": "Maximum"
            }
        ]
    
    def create_alarm(self, alarm_name: str,
                     metric_name: str,
                     threshold: float,
                     comparison: str = "GreaterThanThreshold",
                     period: int = 60,
                     evaluation_periods: int = 5,
                     statistic: str = "Average",
                     cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for ElastiCache metrics.
        
        Args:
            alarm_name: Name for the alarm
            metric_name: Metric name to monitor
            threshold: Threshold value
            comparison: Comparison operator
            period: Evaluation period in seconds
            evaluation_periods: Number of periods to evaluate
            statistic: Statistic type
            cluster_id: Optional cluster ID for dimensions
            
        Returns:
            Dict with alarm creation response
        """
        cloudwatch = self._get_client("cloudwatch")
        
        dimensions = [{"Name": "CacheClusterId", "Value": cluster_id}] if cluster_id else []
        
        kwargs = {
            "AlarmName": alarm_name,
            "MetricName": metric_name,
            "Namespace": "AWS/ElastiCache",
            "Threshold": threshold,
            "ComparisonOperator": comparison,
            "Period": period,
            "EvaluationPeriods": evaluation_periods,
            "Statistic": statistic,
            "Dimensions": dimensions
        }
        
        try:
            response = cloudwatch.put_alarm(**kwargs)
            return {"alarm_name": alarm_name, "status": "created"}
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def list_alarms(self, cluster_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms for ElastiCache.
        
        Args:
            cluster_id: Optional cluster ID to filter by
            
        Returns:
            List of alarms
        """
        cloudwatch = self._get_client("cloudwatch")
        
        filters = [{"Name": "Namespace", "Value": "AWS/ElastiCache"}]
        if cluster_id:
            filters.append({"Name": "Dimensions.CacheClusterId.Value", "Value": cluster_id})
        
        try:
            response = cloudwatch.describe_alarms(
                AlarmTypes=["MetricAlarm"],
                Filters=filters
            )
            return response.get("MetricAlarms", [])
        except ClientError as e:
            logger.error(f"Failed to list alarms: {e}")
            raise
    
    def get_dashboard(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get CloudWatch dashboard data for a cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            Dict with dashboard metrics
        """
        metrics = [
            "CPUUtilization",
            "DatabaseMemoryUsagePercentage",
            "EngineCPUUtilization",
            "CurrConnections",
            "NewConnections",
            "Evictions",
            "Reclaimed",
            "ReplicationLag",
            "GetTypeCmds",
            "SetTypeCmds",
            "BytesUsedForCache",
            "CasHits",
            "CasMisses",
            "HyperLogLogCmds",
            "PubSubCmds"
        ]
        
        return self.get_metrics(cluster_id, metrics)
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def wait_for_cluster(self, cluster_id: str,
                         target_state: str = "available",
                         timeout: int = 1800,
                         poll_interval: int = 30) -> bool:
        """
        Wait for a cluster to reach a target state.
        
        Args:
            cluster_id: The cluster identifier
            target_state: Target state to wait for
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            True if target state reached, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            cluster = self.get_redis_cluster(cluster_id) if cluster_id in str(self.list_redis_clusters()) else self.get_memcached_cluster(cluster_id)
            if cluster.get("CacheClusterStatus") == target_state:
                return True
            time.sleep(poll_interval)
        
        return False
    
    def get_cluster_endpoints(self, cluster_id: str) -> List[str]:
        """
        Get all endpoints for a cluster.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            List of endpoint addresses
        """
        cluster = self.get_redis_cluster(cluster_id)
        if not cluster:
            cluster = self.get_memcached_cluster(cluster_id)
        
        endpoints = []
        for node in cluster.get("CacheNodes", []):
            if node.get("ConfigurationEndpoint"):
                endpoints.append(f"{node['ConfigurationEndpoint']['Address']}:{node['ConfigurationEndpoint']['Port']}")
            else:
                endpoint = node.get("Endpoint", {})
                if endpoint:
                    endpoints.append(f"{endpoint.get('Address', '')}:{endpoint.get('Port', 6379)}")
        
        return endpoints
    
    def get_cache_node_info(self, cluster_id: str) -> List[CacheNodeInfo]:
        """
        Get detailed information about cache nodes.
        
        Args:
            cluster_id: The cluster identifier
            
        Returns:
            List of CacheNodeInfo objects
        """
        cluster = self.get_redis_cluster(cluster_id)
        if not cluster:
            cluster = self.get_memcached_cluster(cluster_id)
        
        nodes = []
        for node in cluster.get("CacheNodes", []):
            endpoint = node.get("Endpoint", {})
            nodes.append(CacheNodeInfo(
                node_id=node.get("CacheNodeId", ""),
                node_type=cluster.get("CacheNodeType", ""),
                status=node.get("CacheNodeStatus", ""),
                port=endpoint.get("Port", 6379),
                availability_zone=node.get("AvailabilityZone", ""),
                create_time=node.get("CacheNodeCreateTime", datetime.min),
                endpoint=endpoint.get("Address", "")
            ))
        
        return nodes
    
    def list_tags_for_resource(self, resource_arn: str) -> Dict[str, str]:
        """
        List tags for an ElastiCache resource.
        
        Args:
            resource_arn: ARN of the resource
            
        Returns:
            Dict of tag key to value
        """
        client = self._get_client()
        
        try:
            response = client.list_tags_for_resource(ResourceName=resource_arn)
            tags = {}
            for tag in response.get("TagList", []):
                tags[tag["Key"]] = tag["Value"]
            return tags
        except ClientError as e:
            logger.error(f"Failed to list tags: {e}")
            raise
    
    def add_tags_to_resource(self, resource_arn: str,
                              tags: Dict[str, str]) -> bool:
        """
        Add tags to an ElastiCache resource.
        
        Args:
            resource_arn: ARN of the resource
            tags: Dict of tag key to value
            
        Returns:
            True if tags added successfully
        """
        client = self._get_client()
        
        tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        try:
            client.add_tags_to_resource(ResourceName=resource_arn, Tags=tag_list)
            return True
        except ClientError as e:
            logger.error(f"Failed to add tags: {e}")
            raise
    
    def remove_tags_from_resource(self, resource_arn: str,
                                  tag_keys: List[str]) -> bool:
        """
        Remove tags from an ElastiCache resource.
        
        Args:
            resource_arn: ARN of the resource
            tag_keys: List of tag keys to remove
            
        Returns:
            True if tags removed successfully
        """
        client = self._get_client()
        
        try:
            client.remove_tags_from_resource(
                ResourceName=resource_arn,
                TagKeys=tag_keys
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to remove tags: {e}")
            raise
    
    def get_available_node_types(self) -> List[str]:
        """
        Get list of available cache node types.
        
        Returns:
            List of node type strings
        """
        client = self._get_client()
        
        try:
            response = client.describe_cache_node_types()
            return [nt.get("CacheNodeType") for nt in response.get("CacheNodeTypes", [])]
        except ClientError as e:
            logger.error(f"Failed to get node types: {e}")
            raise
    
    def get_engine_versions(self, engine: CacheEngine) -> List[str]:
        """
        Get available engine versions.
        
        Args:
            engine: The cache engine type
            
        Returns:
            List of version strings
        """
        client = self._get_client()
        
        try:
            response = client.describe_engine_default_parameters(
                Engine=engine.value,
                CacheParameterGroupFamily=f"{engine.value}7" if engine != CacheEngine.MEMCACHED else "memcached1.6"
            )
            return response.get("EngineDefaults", {}).get("EngineVersion", [])
        except ClientError as e:
            logger.error(f"Failed to get engine versions: {e}")
            raise
    
    def get_events(self, duration_minutes: int = 60) -> List[Dict[str, Any]]:
        """
        Get ElastiCache events.
        
        Args:
            duration_minutes: Duration to look back in minutes
            
        Returns:
            List of events
        """
        client = self._get_client()
        
        try:
            response = client.describe_events(
                SourceType="cache-cluster",
                Duration=duration_minutes
            )
            return response.get("Events", [])
        except ClientError as e:
            logger.error(f"Failed to get events: {e}")
            raise
