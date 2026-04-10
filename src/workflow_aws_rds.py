"""
AWS RDS Database Management Integration Module for Workflow System

Implements an RDSIntegration class with:
1. Instance management: Create/manage RDS instances
2. DB engines: Support MySQL, PostgreSQL, MariaDB, Oracle, SQL Server, Aurora
3. Read replicas: Manage read replicas
4. Multi-AZ: Configure Multi-AZ deployments
5. Backup management: Automated backups and manual snapshots
6. Point-in-time recovery: Restore to point in time
7. Encryption: Enable encryption at rest
8. Performance insights: Enable Performance Insights
9. CloudWatch integration: Monitoring and metrics
10. Parameter groups: Manage DB parameter groups

Commit: 'feat(aws-rds): add AWS RDS integration with instance management, multiple engines, read replicas, Multi-AZ, backups, snapshots, encryption, Performance Insights, CloudWatch, parameter groups'
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


class DBEngine(Enum):
    """Supported RDS database engines."""
    MYSQL = "mysql"
    POSTGRESQL = "postgres"
    MARIADB = "mariadb"
    ORACLE = "oracle-ee"
    ORACLE_SE = "oracle-se"
    ORACLE_SE1 = "oracle-se1"
    ORACLE_SE2 = "oracle-se2"
    SQLSERVER_EE = "sqlserver-ee"
    SQLSERVER_EX = "sqlserver-ex"
    SQLSERVER_SE = "sqlserver-se"
    SQLSERVER_WEB = "sqlserver-web"
    AURORA_MYSQL = "aurora-mysql"
    AURORA_POSTGRESQL = "aurora-postgresql"
    AURORA = "aurora"


class DBInstanceState(Enum):
    """RDS instance states."""
    CREATING = "creating"
    AVAILABLE = "available"
    DELETING = "deleting"
    DELETED = "deleted"
    MODIFYING = "modifying"
    REBOOTING = "rebooting"
    FAILING = "failing"
    FAILED = "failed"
    BACKING_UP = "backing-up"
    STARTING = "starting"
    STOPPING = "stopping"
    STOPPED = "stopped"


class BackupStrategy(Enum):
    """Backup retention strategies."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"


class StorageType(Enum):
    """RDS storage types."""
    STANDARD = "standard"
    PIOPS = "piops"
    GP2 = "gp2"
    GP3 = "gp3"
    IO1 = "io1"
    IO2 = "io2"


@dataclass
class RDSConfig:
    """Configuration for RDS connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class DBInstanceConfig:
    """Configuration for creating an RDS instance."""
    db_instance_identifier: str
    db_instance_class: str = "db.t3.micro"
    engine: DBEngine = DBEngine.POSTGRESQL
    engine_version: Optional[str] = None
    master_username: str = "admin"
    master_password: str = ""
    allocated_storage: int = 20
    max_allocated_storage: Optional[int] = None
    storage_type: StorageType = StorageType.GP2
    iops: Optional[int] = None
    multi_az: bool = False
    availability_zone: Optional[str] = None
    preferred_availability_zone: Optional[str] = None
    preferred_maintenance_window: Optional[str] = None
    preferred_backup_window: Optional[str] = None
    backup_retention_period: int = 7
    port: int = 5432
    db_name: Optional[str] = None
    db_subnet_group_name: Optional[str] = None
    db_security_groups: List[str] = field(default_factory=list)
    db_parameter_group_name: Optional[str] = None
    enable_encryption: bool = False
    kms_key_id: Optional[str] = None
    enable_performance_insights: bool = False
    performance_insights_retention_period: int = 7
    performance_insights_kms_key_id: Optional[str] = None
    enable_cloudwatch_logs_exports: List[str] = field(default_factory=list)
    enable_auto_minor_version_upgrade: bool = True
    license_model: Optional[str] = None
    publicly_accessible: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    storage_encrypted: bool = False


@dataclass
class ReadReplicaConfig:
    """Configuration for creating a read replica."""
    db_instance_identifier: str
    source_db_instance_identifier: str
    db_instance_class: str = "db.t3.micro"
    engine: Optional[DBEngine] = None
    availability_zone: Optional[str] = None
    port: Optional[int] = None
    db_subnet_group_name: Optional[str] = None
    db_security_groups: List[str] = field(default_factory=list)
    db_parameter_group_name: Optional[str] = None
    enable_encryption: bool = False
    kms_key_id: Optional[str] = None
    enable_performance_insights: bool = False
    source_region: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SnapshotConfig:
    """Configuration for creating a snapshot."""
    db_snapshot_identifier: str
    db_instance_identifier: str
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ParameterGroupConfig:
    """Configuration for a DB parameter group."""
    db_parameter_group_name: str
    db_parameter_group_family: str
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


class RDSIntegration:
    """
    AWS RDS Database Management Integration.
    
    Provides comprehensive RDS instance management including:
    - Instance lifecycle management (create, modify, delete, start, stop)
    - Multiple database engine support
    - Read replica management
    - Multi-AZ deployment configuration
    - Automated and manual backup management
    - Point-in-time recovery
    - Encryption at rest
    - Performance Insights
    - CloudWatch monitoring integration
    - DB parameter group management
    """
    
    def __init__(self, config: Optional[RDSConfig] = None):
        """
        Initialize the RDS integration.
        
        Args:
            config: RDS configuration. If None, uses default config with
                   credentials from environment or IAM role.
        """
        self.config = config or RDSConfig()
        self._rds_client = None
        self._rds_resource = None
        self._cw_client = None
        self._lock = threading.RLock()
        self._cache = {}
        self._cache_ttl = 60
    
    @property
    def rds_client(self):
        """Get or create RDS client with lazy initialization."""
        if self._rds_client is None:
            with self._lock:
                if self._rds_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    
                    if BOTO3_AVAILABLE:
                        self._rds_client = boto3.client("rds", **kwargs)
                    else:
                        raise RuntimeError("boto3 is not available. Install with: pip install boto3")
        return self._rds_client
    
    @property
    def cw_client(self):
        """Get or create CloudWatch client for monitoring."""
        if self._cw_client is None:
            with self._lock:
                if self._cw_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    
                    if BOTO3_AVAILABLE:
                        self._cw_client = boto3.client("cloudwatch", **kwargs)
                    else:
                        raise RuntimeError("boto3 is not available")
        return self._cw_client
    
    def _get_cache_key(self, operation: str, identifier: str) -> str:
        """Generate a cache key for an operation."""
        return f"{operation}:{identifier}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self._cache:
            return False
        entry = self._cache[cache_key]
        return (datetime.now() - entry["timestamp"]).total_seconds() < self._cache_ttl
    
    def _set_cache(self, cache_key: str, value: Any) -> None:
        """Set a cache entry with current timestamp."""
        self._cache[cache_key] = {
            "value": value,
            "timestamp": datetime.now()
        }
    
    def _invalidate_cache(self, pattern: Optional[str] = None) -> None:
        """Invalidate cache entries matching pattern."""
        if pattern is None:
            self._cache.clear()
        else:
            keys_to_remove = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_remove:
                del self._cache[key]
    
    # ========================================================================
    # Instance Management
    # ========================================================================
    
    def create_instance(self, config: DBInstanceConfig) -> Dict[str, Any]:
        """
        Create a new RDS instance.
        
        Args:
            config: DB instance configuration
            
        Returns:
            Dict containing created instance information
        """
        try:
            params = {
                "DBInstanceIdentifier": config.db_instance_identifier,
                "DBInstanceClass": config.db_instance_class,
                "Engine": config.engine.value,
                "MasterUsername": config.master_username,
                "MasterUserPassword": config.master_password,
                "AllocatedStorage": config.allocated_storage,
                "BackupRetentionPeriod": config.backup_retention_period,
                "PreferredBackupWindow": config.preferred_backup_window or "03:00-04:00",
                "PreferredMaintenanceWindow": config.preferred_maintenance_window or "mon:04:00-mon:05:00",
                "Port": config.port,
                "MultiAZ": config.multi_az,
                "AutoMinorVersionUpgrade": config.enable_auto_minor_version_upgrade,
                "PubliclyAccessible": config.publicly_accessible,
                "Tags": [
                    {"Key": k, "Value": v} for k, v in config.tags.items()
                ] if config.tags else [],
            }
            
            if config.engine_version:
                params["EngineVersion"] = config.engine_version
            
            if config.max_allocated_storage:
                params["MaxAllocatedStorage"] = config.max_allocated_storage
            
            if config.storage_type != StorageType.STANDARD:
                params["StorageType"] = config.storage_type.value
            
            if config.iops:
                params["Iops"] = config.iops
            
            if config.availability_zone:
                params["AvailabilityZone"] = config.availability_zone
            
            if config.preferred_availability_zone:
                params["PreferredAvailabilityZone"] = config.preferred_availability_zone
            
            if config.db_name:
                params["DBName"] = config.db_name
            
            if config.db_subnet_group_name:
                params["DBSubnetGroupName"] = config.db_subnet_group_name
            
            if config.db_security_groups:
                params["DBSecurityGroups"] = config.db_security_groups
            
            if config.db_parameter_group_name:
                params["DBParameterGroupName"] = config.db_parameter_group_name
            
            if config.enable_encryption or config.storage_encrypted:
                params["StorageEncrypted"] = True
                if config.kms_key_id:
                    params["KmsKeyId"] = config.kms_key_id
            
            if config.enable_performance_insights:
                params["EnablePerformanceInsights"] = True
                if config.performance_insights_retention_period:
                    params["PerformanceInsightsRetentionPeriod"] = config.performance_insights_retention_period
                if config.performance_insights_kms_key_id:
                    params["PerformanceInsightsKMSKeyId"] = config.performance_insights_kms_key_id
            
            if config.enable_cloudwatch_logs_exports:
                params["EnableCloudwatchLogsExports"] = config.enable_cloudwatch_logs_exports
            
            if config.license_model:
                params["LicenseModel"] = config.license_model
            
            response = self.rds_client.create_db_instance(**params)
            instance = response["DBInstance"]
            
            self._invalidate_cache("instances")
            
            logger.info(f"Created RDS instance: {config.db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to create RDS instance: {e}")
            raise
    
    def get_instance(self, db_instance_identifier: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get information about an RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            use_cache: Whether to use cached data if available
            
        Returns:
            DB instance information dict or None if not found
        """
        cache_key = self._get_cache_key("instance", db_instance_identifier)
        
        if use_cache and self._is_cache_valid(cache_key):
            return self._cache[cache_key]["value"]
        
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance = response["DBInstances"][0] if response["DBInstances"] else None
            
            if instance:
                self._set_cache(cache_key, instance)
            
            return instance
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                return None
            logger.error(f"Failed to get RDS instance: {e}")
            raise
    
    def list_instances(
        self,
        filters: Optional[List[Dict[str, Any]]] = None,
        max_records: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List RDS instances with optional filtering.
        
        Args:
            filters: Optional filters for listing instances
            max_records: Maximum number of records to return
            
        Returns:
            List of DB instance information dicts
        """
        try:
            params = {"MaxRecords": max_records}
            
            if filters:
                params["Filters"] = filters
            
            instances = []
            paginator = self.rds_client.get_paginator("describe_db_instances")
            
            for page in paginator.paginate(**params):
                instances.extend(page["DBInstances"])
            
            return instances
            
        except ClientError as e:
            logger.error(f"Failed to list RDS instances: {e}")
            raise
    
    def modify_instance(
        self,
        db_instance_identifier: str,
        modifications: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Modify an RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            modifications: Dict of modifications to apply
            
        Returns:
            Modified DB instance information
        """
        try:
            params = {"DBInstanceIdentifier": db_instance_identifier}
            params.update(modifications)
            
            response = self.rds_client.modify_db_instance(**params)
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            self._invalidate_cache("instances")
            
            logger.info(f"Modified RDS instance: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to modify RDS instance: {e}")
            raise
    
    def delete_instance(
        self,
        db_instance_identifier: str,
        skip_final_snapshot: bool = False,
        final_db_snapshot_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete an RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            skip_final_snapshot: Whether to skip final snapshot creation
            final_db_snapshot_identifier: Identifier for final snapshot if not skipping
            
        Returns:
            Deleted DB instance information
        """
        try:
            params = {
                "DBInstanceIdentifier": db_instance_identifier,
                "SkipFinalSnapshot": skip_final_snapshot,
            }
            
            if not skip_final_snapshot and final_db_snapshot_identifier:
                params["FinalDBSnapshotIdentifier"] = final_db_snapshot_identifier
            
            response = self.rds_client.delete_db_instance(**params)
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            self._invalidate_cache("instances")
            
            logger.info(f"Deleted RDS instance: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to delete RDS instance: {e}")
            raise
    
    def start_instance(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Start a stopped RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            DB instance information
        """
        try:
            response = self.rds_client.start_db_instance(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            
            logger.info(f"Started RDS instance: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to start RDS instance: {e}")
            raise
    
    def stop_instance(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Stop a running RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            DB instance information
        """
        try:
            response = self.rds_client.stop_db_instance(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            
            logger.info(f"Stopped RDS instance: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to stop RDS instance: {e}")
            raise
    
    def reboot_instance(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Reboot an RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            DB instance information
        """
        try:
            response = self.rds_client.reboot_db_instance(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            
            logger.info(f"Rebooted RDS instance: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to reboot RDS instance: {e}")
            raise
    
    # ========================================================================
    # Read Replica Management
    # ========================================================================
    
    def create_read_replica(self, config: ReadReplicaConfig) -> Dict[str, Any]:
        """
        Create a read replica.
        
        Args:
            config: Read replica configuration
            
        Returns:
            Created read replica information
        """
        try:
            params = {
                "DBInstanceIdentifier": config.db_instance_identifier,
                "SourceDBInstanceIdentifier": config.source_db_instance_identifier,
                "DBInstanceClass": config.db_instance_class,
            }
            
            if config.engine:
                params["Engine"] = config.engine.value
            
            if config.availability_zone:
                params["AvailabilityZone"] = config.availability_zone
            
            if config.port:
                params["Port"] = config.port
            
            if config.db_subnet_group_name:
                params["DBSubnetGroupName"] = config.db_subnet_group_name
            
            if config.db_security_groups:
                params["DBSecurityGroups"] = config.db_security_groups
            
            if config.db_parameter_group_name:
                params["DBParameterGroupName"] = config.db_parameter_group_name
            
            if config.enable_encryption:
                params["StorageEncrypted"] = True
                if config.kms_key_id:
                    params["KmsKeyId"] = config.kms_key_id
            
            if config.enable_performance_insights:
                params["EnablePerformanceInsights"] = True
            
            if config.source_region:
                params["SourceRegion"] = config.source_region
            
            if config.tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
            
            response = self.rds_client.create_db_instance_read_replica(**params)
            replica = response["DBInstance"]
            
            self._invalidate_cache("instances")
            
            logger.info(f"Created read replica: {config.db_instance_identifier}")
            return replica
            
        except ClientError as e:
            logger.error(f"Failed to create read replica: {e}")
            raise
    
    def list_read_replicas(self, source_db_instance_identifier: str) -> List[Dict[str, Any]]:
        """
        List read replicas for a source instance.
        
        Args:
            source_db_instance_identifier: Source DB instance identifier
            
        Returns:
            List of read replica information
        """
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=source_db_instance_identifier
            )
            
            if not response["DBInstances"]:
                return []
            
            source = response["DBInstances"][0]
            replica_identifiers = source.get("ReadReplicaDBInstanceIdentifiers", [])
            
            replicas = []
            for replica_id in replica_identifiers:
                try:
                    replica_response = self.rds_client.describe_db_instances(
                        DBInstanceIdentifier=replica_id
                    )
                    if replica_response["DBInstances"]:
                        replicas.append(replica_response["DBInstances"][0])
                except ClientError:
                    continue
            
            return replicas
            
        except ClientError as e:
            logger.error(f"Failed to list read replicas: {e}")
            raise
    
    def promote_read_replica(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Promote a read replica to a standalone instance.
        
        Args:
            db_instance_identifier: Read replica identifier
            
        Returns:
            Promoted instance information
        """
        try:
            response = self.rds_client.promote_read_replica(
                DBInstanceIdentifier=db_instance_identifier
            )
            instance = response["DBInstance"]
            
            self._invalidate_cache(f"instance:{db_instance_identifier}")
            self._invalidate_cache("instances")
            
            logger.info(f"Promoted read replica: {db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to promote read replica: {e}")
            raise
    
    # ========================================================================
    # Multi-AZ Management
    # ========================================================================
    
    def enable_multi_az(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Enable Multi-AZ for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Modified instance information
        """
        return self.modify_instance(
            db_instance_identifier,
            {"MultiAZ": True}
        )
    
    def disable_multi_az(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Disable Multi-AZ for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Modified instance information
        """
        return self.modify_instance(
            db_instance_identifier,
            {"MultiAZ": False}
        )
    
    def get_multi_az_status(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Get Multi-AZ status for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Dict with Multi-AZ status information
        """
        instance = self.get_instance(db_instance_identifier)
        if not instance:
            raise ValueError(f"Instance not found: {db_instance_identifier}")
        
        return {
            "multi_az": instance.get("MultiAZ", False),
            "secondary_availability_zone": instance.get("SecondaryAvailabilityZone"),
            "status": instance.get("StatusInfos", [])
        }
    
    # ========================================================================
    # Backup and Snapshot Management
    # ========================================================================
    
    def create_snapshot(
        self,
        db_instance_identifier: str,
        snapshot_identifier: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a manual DB snapshot.
        
        Args:
            db_instance_identifier: The DB instance identifier
            snapshot_identifier: Optional snapshot identifier (auto-generated if not provided)
            tags: Optional tags for the snapshot
            
        Returns:
            Created snapshot information
        """
        try:
            if not snapshot_identifier:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                snapshot_identifier = f"{db_instance_identifier}-{timestamp}"
            
            params = {
                "DBSnapshotIdentifier": snapshot_identifier,
                "DBInstanceIdentifier": db_instance_identifier,
            }
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.rds_client.create_db_snapshot(**params)
            snapshot = response["DBSnapshot"]
            
            logger.info(f"Created snapshot: {snapshot_identifier}")
            return snapshot
            
        except ClientError as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise
    
    def list_snapshots(
        self,
        db_instance_identifier: Optional[str] = None,
        snapshot_type: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List DB snapshots.
        
        Args:
            db_instance_identifier: Optional filter by DB instance
            snapshot_type: Optional filter by snapshot type (automated, manual)
            filters: Optional additional filters
            
        Returns:
            List of snapshot information
        """
        try:
            params = {}
            
            if db_instance_identifier:
                params["DBInstanceIdentifier"] = db_instance_identifier
            
            if filters:
                params["Filters"] = filters
            
            if snapshot_type:
                if "Filters" not in params:
                    params["Filters"] = []
                params["Filters"].append({
                    "Name": "snapshot-type",
                    "Values": [snapshot_type]
                })
            
            snapshots = []
            paginator = self.rds_client.get_paginator("describe_db_snapshots")
            
            for page in paginator.paginate(**params):
                snapshots.extend(page["DBSnapshots"])
            
            return snapshots
            
        except ClientError as e:
            logger.error(f"Failed to list snapshots: {e}")
            raise
    
    def delete_snapshot(self, snapshot_identifier: str) -> None:
        """
        Delete a DB snapshot.
        
        Args:
            snapshot_identifier: The snapshot identifier
        """
        try:
            self.rds_client.delete_db_snapshot(
                DBSnapshotIdentifier=snapshot_identifier
            )
            logger.info(f"Deleted snapshot: {snapshot_identifier}")
            
        except ClientError as e:
            logger.error(f"Failed to delete snapshot: {e}")
            raise
    
    def copy_snapshot(
        self,
        source_snapshot_identifier: str,
        target_snapshot_identifier: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Copy a DB snapshot.
        
        Args:
            source_snapshot_identifier: Source snapshot identifier
            target_snapshot_identifier: Target snapshot identifier
            tags: Optional tags for the copied snapshot
            
        Returns:
            Copied snapshot information
        """
        try:
            params = {
                "SourceDBSnapshotIdentifier": source_snapshot_identifier,
                "TargetDBSnapshotIdentifier": target_snapshot_identifier,
            }
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.rds_client.copy_db_snapshot(**params)
            snapshot = response["DBSnapshot"]
            
            logger.info(f"Copied snapshot: {source_snapshot_identifier} -> {target_snapshot_identifier}")
            return snapshot
            
        except ClientError as e:
            logger.error(f"Failed to copy snapshot: {e}")
            raise
    
    def get_backup_info(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Get backup information for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Dict with backup information
        """
        instance = self.get_instance(db_instance_identifier)
        if not instance:
            raise ValueError(f"Instance not found: {db_instance_identifier}")
        
        return {
            "backup_retention_period": instance.get("BackupRetentionPeriod", 0),
            "preferred_backup_window": instance.get("PreferredBackupWindow"),
            "latest_restorable_time": instance.get("LatestRestorableDateTime"),
            "encrypted": instance.get("StorageEncrypted", False),
        }
    
    def modify_backup_retention(
        self,
        db_instance_identifier: str,
        retention_period: int
    ) -> Dict[str, Any]:
        """
        Modify backup retention period for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            retention_period: New retention period in days (0 to disable)
            
        Returns:
            Modified instance information
        """
        return self.modify_instance(
            db_instance_identifier,
            {"BackupRetentionPeriod": retention_period}
        )
    
    # ========================================================================
    # Point-in-Time Recovery
    # ========================================================================
    
    def restore_to_point_in_time(
        self,
        source_db_instance_identifier: str,
        target_db_instance_identifier: str,
        restore_time: Optional[datetime] = None,
        use_latest_restorable_time: bool = False,
        db_instance_class: Optional[str] = None,
        port: Optional[int] = None,
        availability_zone: Optional[str] = None,
        db_subnet_group_name: Optional[str] = None,
        multi_az: Optional[bool] = None,
        publicly_accessible: Optional[bool] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Restore a DB instance to a point in time.
        
        Args:
            source_db_instance_identifier: Source instance identifier
            target_db_instance_identifier: Target restored instance identifier
            restore_time: Specific time to restore to
            use_latest_restorable_time: Whether to restore to latest restorable time
            db_instance_class: Optional instance class for restored instance
            port: Optional port for restored instance
            availability_zone: Optional AZ for restored instance
            db_subnet_group_name: Optional subnet group for restored instance
            multi_az: Optional Multi-AZ setting for restored instance
            publicly_accessible: Optional public access setting
            tags: Optional tags for restored instance
            
        Returns:
            Restored instance information
        """
        try:
            params = {
                "SourceDBInstanceIdentifier": source_db_instance_identifier,
                "TargetDBInstanceIdentifier": target_db_instance_identifier,
            }
            
            if use_latest_restorable_time:
                params["UseLatestRestorableTime"] = True
            elif restore_time:
                params["RestoreTime"] = restore_time.isoformat()
            
            if db_instance_class:
                params["DBInstanceClass"] = db_instance_class
            
            if port:
                params["Port"] = port
            
            if availability_zone:
                params["AvailabilityZone"] = availability_zone
            
            if db_subnet_group_name:
                params["DBSubnetGroupName"] = db_subnet_group_name
            
            if multi_az is not None:
                params["MultiAZ"] = multi_az
            
            if publicly_accessible is not None:
                params["PubliclyAccessible"] = publicly_accessible
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.rds_client.restore_db_instance_to_point_in_time(**params)
            instance = response["DBInstance"]
            
            self._invalidate_cache("instances")
            
            logger.info(f"Restored instance to point in time: {target_db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to restore to point in time: {e}")
            raise
    
    def restore_from_snapshot(
        self,
        snapshot_identifier: str,
        target_db_instance_identifier: str,
        db_instance_class: Optional[str] = None,
        port: Optional[int] = None,
        availability_zone: Optional[str] = None,
        db_subnet_group_name: Optional[str] = None,
        multi_az: Optional[bool] = None,
        publicly_accessible: Optional[bool] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Restore a DB instance from a snapshot.
        
        Args:
            snapshot_identifier: Snapshot identifier
            target_db_instance_identifier: Target restored instance identifier
            db_instance_class: Optional instance class for restored instance
            port: Optional port for restored instance
            availability_zone: Optional AZ for restored instance
            db_subnet_group_name: Optional subnet group for restored instance
            multi_az: Optional Multi-AZ setting for restored instance
            publicly_accessible: Optional public access setting
            tags: Optional tags for restored instance
            
        Returns:
            Restored instance information
        """
        try:
            params = {
                "DBSnapshotIdentifier": snapshot_identifier,
                "DBInstanceIdentifier": target_db_instance_identifier,
            }
            
            if db_instance_class:
                params["DBInstanceClass"] = db_instance_class
            
            if port:
                params["Port"] = port
            
            if availability_zone:
                params["AvailabilityZone"] = availability_zone
            
            if db_subnet_group_name:
                params["DBSubnetGroupName"] = db_subnet_group_name
            
            if multi_az is not None:
                params["MultiAZ"] = multi_az
            
            if publicly_accessible is not None:
                params["PubliclyAccessible"] = publicly_accessible
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.rds_client.restore_db_instance_from_db_snapshot(**params)
            instance = response["DBInstance"]
            
            self._invalidate_cache("instances")
            
            logger.info(f"Restored instance from snapshot: {target_db_instance_identifier}")
            return instance
            
        except ClientError as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            raise
    
    # ========================================================================
    # Encryption Management
    # ========================================================================
    
    def enable_encryption(
        self,
        db_instance_identifier: str,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Enable encryption at rest for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            kms_key_id: Optional KMS key ID (uses AWS managed key if not provided)
            
        Returns:
            Modified instance information
        """
        modifications = {
            "StorageEncrypted": True
        }
        
        if kms_key_id:
            modifications["KmsKeyId"] = kms_key_id
        
        return self.modify_instance(db_instance_identifier, modifications)
    
    def get_encryption_status(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Get encryption status for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Dict with encryption status information
        """
        instance = self.get_instance(db_instance_identifier)
        if not instance:
            raise ValueError(f"Instance not found: {db_instance_identifier}")
        
        return {
            "encrypted": instance.get("StorageEncrypted", False),
            "kms_key_id": instance.get("KmsKeyId"),
        }
    
    # ========================================================================
    # Performance Insights
    # ========================================================================
    
    def enable_performance_insights(
        self,
        db_instance_identifier: str,
        retention_period: int = 7,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Enable Performance Insights for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            retention_period: Retention period in days
            kms_key_id: Optional KMS key ID for encryption
            
        Returns:
            Modified instance information
        """
        modifications = {
            "EnablePerformanceInsights": True,
            "PerformanceInsightsRetentionPeriod": retention_period
        }
        
        if kms_key_id:
            modifications["PerformanceInsightsKMSKeyId"] = kms_key_id
        
        return self.modify_instance(db_instance_identifier, modifications)
    
    def disable_performance_insights(self, db_instance_identifier: str) -> Dict[str, Any]:
        """
        Disable Performance Insights for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Modified instance information
        """
        return self.modify_instance(
            db_instance_identifier,
            {"EnablePerformanceInsights": False}
        )
    
    def get_performance_insights_status(
        self,
        db_instance_identifier: str
    ) -> Dict[str, Any]:
        """
        Get Performance Insights status for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            
        Returns:
            Dict with Performance Insights status
        """
        instance = self.get_instance(db_instance_identifier)
        if not instance:
            raise ValueError(f"Instance not found: {db_instance_identifier}")
        
        return {
            "performance_insights_enabled": instance.get("PerformanceInsightsEnabled", False),
            "performance_insights_kms_key_id": instance.get("PerformanceInsightsKMSKeyId"),
            "performance_insights_retention_period": instance.get("PerformanceInsightsRetentionPeriod"),
        }
    
    # ========================================================================
    # CloudWatch Integration
    # ========================================================================
    
    def get_instance_metrics(
        self,
        db_instance_identifier: str,
        metric_names: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 60
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get CloudWatch metrics for an instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            metric_names: List of metric names to retrieve
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Period in seconds
            
        Returns:
            Dict mapping metric names to their data points
        """
        try:
            if not start_time:
                start_time = datetime.now() - timedelta(hours=1)
            if not end_time:
                end_time = datetime.now()
            
            namespace = "AWS/RDS"
            dimensions = [{"Name": "DBInstanceIdentifier", "Value": db_instance_identifier}]
            
            metrics_data = {}
            
            for metric_name in metric_names:
                response = self.cw_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=dimensions,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum"]
                )
                
                metrics_data[metric_name] = response.get("Datapoints", [])
            
            return metrics_data
            
        except ClientError as e:
            logger.error(f"Failed to get instance metrics: {e}")
            raise
    
    def get_standard_metrics(
        self,
        db_instance_identifier: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get standard RDS metrics.
        
        Args:
            db_instance_identifier: The DB instance identifier
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dict with standard RDS metrics
        """
        standard_metrics = [
            "CPUUtilization",
            "DatabaseConnections",
            "FreeStorageSpace",
            "FreeableMemory",
            "WriteIOPS",
            "ReadIOPS",
            "WriteThroughput",
            "ReadThroughput",
            "NetworkReceiveThroughput",
            "NetworkTransmitThroughput",
            "DiskQueueDepth",
            "SwapUsage",
        ]
        
        return self.get_instance_metrics(
            db_instance_identifier,
            standard_metrics,
            start_time,
            end_time
        )
    
    def set_alarm(
        self,
        db_instance_identifier: str,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 60,
        statistic: str = "Average",
        alarm_actions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for an RDS instance.
        
        Args:
            db_instance_identifier: The DB instance identifier
            alarm_name: Name of the alarm
            metric_name: Metric name to alarm on
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic to use
            alarm_actions: Optional list of ARNs for alarm actions
            
        Returns:
            Created alarm configuration
        """
        try:
            params = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": "AWS/RDS",
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": statistic,
                "Dimensions": [
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier}
                ],
            }
            
            if alarm_actions:
                params["AlarmActions"] = alarm_actions
            
            response = self.cw_client.put_metric_alarm(**params)
            
            logger.info(f"Created alarm: {alarm_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def get_alarms(self, db_instance_identifier: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get CloudWatch alarms for RDS.
        
        Args:
            db_instance_identifier: Optional specific instance identifier
            
        Returns:
            List of alarm configurations
        """
        try:
            if db_instance_identifier:
                response = self.cw_client.describe_alarms(
                    AlarmTypes=["MetricAlarm"],
                    Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_identifier}]
                )
            else:
                response = self.cw_client.describe_alarms(
                    AlarmTypes=["MetricAlarm"]
                )
            
            return response.get("MetricAlarms", [])
            
        except ClientError as e:
            logger.error(f"Failed to get alarms: {e}")
            raise
    
    # ========================================================================
    # Parameter Group Management
    # ========================================================================
    
    def create_parameter_group(
        self,
        parameter_group_name: str,
        family: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a DB parameter group.
        
        Args:
            parameter_group_name: Name for the parameter group
            family: Parameter group family (e.g., postgres13, mysql8.0)
            description: Description of the parameter group
            tags: Optional tags
            
        Returns:
            Created parameter group information
        """
        try:
            params = {
                "DBParameterGroupName": parameter_group_name,
                "ParameterGroupFamily": family,
                "Description": description or f"Custom parameter group: {parameter_group_name}",
            }
            
            if tags:
                params["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.rds_client.create_db_parameter_group(**params)
            group = response["DBParameterGroup"]
            
            logger.info(f"Created parameter group: {parameter_group_name}")
            return group
            
        except ClientError as e:
            logger.error(f"Failed to create parameter group: {e}")
            raise
    
    def get_parameter_group(self, parameter_group_name: str) -> Dict[str, Any]:
        """
        Get information about a parameter group.
        
        Args:
            parameter_group_name: Name of the parameter group
            
        Returns:
            Parameter group information
        """
        try:
            response = self.rds_client.describe_db_parameter_groups(
                DBParameterGroupName=parameter_group_name
            )
            groups = response.get("DBParameterGroups", [])
            return groups[0] if groups else None
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBParameterGroupNotFound":
                return None
            logger.error(f"Failed to get parameter group: {e}")
            raise
    
    def list_parameter_groups(
        self,
        family: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List DB parameter groups.
        
        Args:
            family: Optional filter by parameter group family
            filters: Optional additional filters
            
        Returns:
            List of parameter group information
        """
        try:
            params = {}
            
            if family:
                params["ParameterGroupFamily"] = family
            
            if filters:
                params["Filters"] = filters
            
            response = self.rds_client.describe_db_parameter_groups(**params)
            return response.get("DBParameterGroups", [])
            
        except ClientError as e:
            logger.error(f"Failed to list parameter groups: {e}")
            raise
    
    def delete_parameter_group(self, parameter_group_name: str) -> None:
        """
        Delete a DB parameter group.
        
        Args:
            parameter_group_name: Name of the parameter group
        """
        try:
            self.rds_client.delete_db_parameter_group(
                DBParameterGroupName=parameter_group_name
            )
            logger.info(f"Deleted parameter group: {parameter_group_name}")
            
        except ClientError as e:
            logger.error(f"Failed to delete parameter group: {e}")
            raise
    
    def get_parameters(
        self,
        parameter_group_name: str,
        source: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get parameters in a parameter group.
        
        Args:
            parameter_group_name: Name of the parameter group
            source: Optional filter by source (system, engine-default, customer)
            
        Returns:
            List of parameter information
        """
        try:
            params = {"DBParameterGroupName": parameter_group_name}
            
            if source:
                params["Source"] = source
            
            parameters = []
            paginator = self.rds_client.get_paginator("describe_db_parameters")
            
            for page in paginator.paginate(**params):
                parameters.extend(page.get("Parameters", []))
            
            return parameters
            
        except ClientError as e:
            logger.error(f"Failed to get parameters: {e}")
            raise
    
    def modify_parameters(
        self,
        parameter_group_name: str,
        parameters: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Modify parameters in a parameter group.
        
        Args:
            parameter_group_name: Name of the parameter group
            parameters: List of parameter modifications
            
        Returns:
            Modified parameter group information
        """
        try:
            formatted_params = []
            for param in parameters:
                formatted_params.append({
                    "ParameterName": param["name"],
                    "ParameterValue": str(param["value"]),
                    "ApplyMethod": param.get("method", "immediate")
                })
            
            response = self.rds_client.modify_db_parameter_group(
                DBParameterGroupName=parameter_group_name,
                Parameters=formatted_params
            )
            
            logger.info(f"Modified parameters in group: {parameter_group_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to modify parameters: {e}")
            raise
    
    def reset_parameters(
        self,
        parameter_group_name: str,
        parameter_names: List[str]
    ) -> Dict[str, Any]:
        """
        Reset parameters to default values.
        
        Args:
            parameter_group_name: Name of the parameter group
            parameter_names: List of parameter names to reset
            
        Returns:
            Modified parameter group information
        """
        try:
            formatted_params = []
            for name in parameter_names:
                formatted_params.append({
                    "ParameterName": name,
                    "ApplyMethod": "immediate"
                })
            
            response = self.rds_client.reset_db_parameter_group(
                DBParameterGroupName=parameter_group_name,
                Parameters=formatted_params
            )
            
            logger.info(f"Reset parameters in group: {parameter_group_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to reset parameters: {e}")
            raise
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def get_available_engine_versions(
        self,
        engine: Optional[DBEngine] = None,
        engine_version: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get available engine versions.
        
        Args:
            engine: Optional engine to filter by
            engine_version: Optional engine version to filter by
            
        Returns:
            List of available engine versions
        """
        try:
            params = {}
            
            if engine:
                params["Engine"] = engine.value
            
            if engine_version:
                params["EngineVersion"] = engine_version
            
            versions = []
            paginator = self.rds_client.get_paginator("describe_db_engine_versions")
            
            for page in paginator.paginate(**params):
                versions.extend(page.get("DBEngineVersions", []))
            
            return versions
            
        except ClientError as e:
            logger.error(f"Failed to get engine versions: {e}")
            raise
    
    def get_instance_classes(
        self,
        engine: Optional[DBEngine] = None
    ) -> List[Dict[str, Any]]:
        """
        Get available DB instance classes.
        
        Args:
            engine: Optional engine to filter by
            
        Returns:
            List of available instance classes
        """
        try:
            response = self.rds_client.describe_orderable_db_instance_options(
                Engine=engine.value if engine else None
            )
            
            return response.get("OrderableDBInstanceOptions", [])
            
        except ClientError as e:
            logger.error(f"Failed to get instance classes: {e}")
            raise
    
    def get_reserved_instance_offerings(self) -> List[Dict[str, Any]]:
        """
        Get available reserved RDS instance offerings.
        
        Returns:
            List of reserved instance offerings
        """
        try:
            response = self.rds_client.describe_reserved_db_instances_offerings()
            return response.get("ReservedDBInstancesOfferings", [])
            
        except ClientError as e:
            logger.error(f"Failed to get reserved instance offerings: {e}")
            raise
    
    def purchase_reserved_instance(
        self,
        reserved_db_instance_offering_id: str,
        db_instance_count: int = 1,
        db_instance_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Purchase a reserved RDS instance.
        
        Args:
            reserved_db_instance_offering_id: Reserved offering ID
            db_instance_count: Number of instances to reserve
            db_instance_identifier: Optional identifier for the reservation
            
        Returns:
            Reserved instance information
        """
        try:
            params = {
                "ReservedDBInstanceOfferingId": reserved_db_instance_offering_id,
                "DBInstanceCount": db_instance_count,
            }
            
            if db_instance_identifier:
                params["ReservedDBInstanceIdentifier"] = db_instance_identifier
            
            response = self.rds_client.purchase_reserved_db_instances_offering(**params)
            return response.get("ReservedDBInstance", {})
            
        except ClientError as e:
            logger.error(f"Failed to purchase reserved instance: {e}")
            raise
    
    def add_tags(
        self,
        resource_arn: str,
        tags: Dict[str, str]
    ) -> None:
        """
        Add tags to an RDS resource.
        
        Args:
            resource_arn: ARN of the resource
            tags: Dict of tags to add
        """
        try:
            self.rds_client.add_tags_to_resource(
                ResourceName=resource_arn,
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )
            
        except ClientError as e:
            logger.error(f"Failed to add tags: {e}")
            raise
    
    def remove_tags(self, resource_arn: str, tag_keys: List[str]) -> None:
        """
        Remove tags from an RDS resource.
        
        Args:
            resource_arn: ARN of the resource
            tag_keys: List of tag keys to remove
        """
        try:
            self.rds_client.remove_tags_from_resource(
                ResourceName=resource_arn,
                TagKeys=tag_keys
            )
            
        except ClientError as e:
            logger.error(f"Failed to remove tags: {e}")
            raise
    
    def wait_for_instance_available(
        self,
        db_instance_identifier: str,
        timeout: int = 600,
        check_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for an instance to become available.
        
        Args:
            db_instance_identifier: The DB instance identifier
            timeout: Maximum wait time in seconds
            check_interval: Interval between checks in seconds
            
        Returns:
            Final instance state
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            instance = self.get_instance(db_instance_identifier, use_cache=False)
            
            if not instance:
                raise TimeoutError(f"Instance {db_instance_identifier} not found")
            
            status = instance.get("DBInstanceStatus", "")
            
            if status == "available":
                return instance
            
            if status in ["failed", "deleting", "deleted"]:
                raise TimeoutError(f"Instance {db_instance_identifier} entered state: {status}")
            
            time.sleep(check_interval)
        
        raise TimeoutError(f"Timeout waiting for instance {db_instance_identifier}")
    
    def wait_for_instance_stopped(
        self,
        db_instance_identifier: str,
        timeout: int = 600,
        check_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for an instance to stop.
        
        Args:
            db_instance_identifier: The DB instance identifier
            timeout: Maximum wait time in seconds
            check_interval: Interval between checks in seconds
            
        Returns:
            Final instance state
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            instance = self.get_instance(db_instance_identifier, use_cache=False)
            
            if not instance:
                raise TimeoutError(f"Instance {db_instance_identifier} not found")
            
            status = instance.get("DBInstanceStatus", "")
            
            if status == "stopped":
                return instance
            
            if status in ["failed", "deleting", "deleted"]:
                raise TimeoutError(f"Instance {db_instance_identifier} entered state: {status}")
            
            time.sleep(check_interval)
        
        raise TimeoutError(f"Timeout waiting for instance {db_instance_identifier}")
