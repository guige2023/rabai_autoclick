"""
AWS Keyspaces Integration Module for Workflow System

Implements a KeyspacesIntegration class with:
1. Keyspace management: Create/manage keyspaces
2. Table management: Create/manage tables
3. Point-in-time recovery: Enable/disable PITR
4. Data import: Import data from S3
5. Point-in-time restore: Restore tables from PITR
6. Encryption: Server-side encryption
7. TTL: Time-to-live management
8. Multi-Region replication: Multi-region keyspaces
9. Spark connector: Apache Spark integration
10. CloudWatch integration: Metrics and monitoring

Commit: 'feat(aws-keyspaces): add Amazon Keyspaces with keyspace/table management, PITR, data import, restore, encryption, TTL, multi-region, Spark connector, CloudWatch'
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


class KeyspaceState(Enum):
    """Keyspace states."""
    CREATING = "creating"
    ACTIVE = "active"
    DELETING = "deleting"
    DELETED = "deleted"


class TableState(Enum):
    """Table states."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    DELETED = "DELETED"


class ReplicationStrategy(Enum):
    """Keyspaces replication strategies."""
    SINGLE_REGION = "SINGLE_REGION"
    MULTI_REGION = "MULTI_REGION"


class CapacityMode(Enum):
    """Capacity modes for Keyspaces."""
    ON_DEMAND = "ON_DEMAND"
    PROVISIONED = "PROVISIONED"


class ImportTaskStatus(Enum):
    """Import task statuses."""
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RestoreTaskStatus(Enum):
    """Restore task statuses."""
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SparkConnectorStatus(Enum):
    """Spark connector status."""
    NOT_CONFIGURED = "NOT_CONFIGURED"
    CONFIGURED = "CONFIGURED"
    CONNECTED = "CONNECTED"
    ERROR = "ERROR"


@dataclass
class KeyspacesConfig:
    """Configuration for Keyspaces connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class KeyspaceConfig:
    """Configuration for creating a keyspace."""
    keyspace_name: str
    replication_settings: Dict[str, Any] = field(default_factory=dict)
    point_in_time_recovery: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    capacity_mode: str = "PROVISIONED"
    throughput: int = 1000
    max_throughput: Optional[int] = None


@dataclass
class TableConfig:
    """Configuration for creating a table."""
    keyspace_name: str
    table_name: str
    schema_definition: Dict[str, Any] = field(default_factory=dict)
    partition_key_columns: List[str] = field(default_factory=list)
    clustering_key_columns: List[str] = field(default_factory=list)
    static_columns: List[str] = field(default_factory=list)
    default_time_to_live: int = 0
    point_in_time_recovery: bool = False
    encryption_settings: Dict[str, Any] = field(default_factory=dict)
    capacity_mode: str = "PROVISIONED"
    throughput: int = 1000
    max_throughput: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None


@dataclass
class ColumnDefinition:
    """Column definition for table schema."""
    name: str
    column_type: str  # ascii, bigint, blob, boolean, decimal, double, float, frozen, inet, int, list, map, set, smallint, text, timestamp, uuid, varchar, varint


@dataclass
class ImportConfig:
    """Configuration for data import from S3."""
    keyspace_name: str
    table_name: str
    s3_bucket: str
    s3_prefix: str
    iam_role_arn: str
    compression_type: str = "NONE"  # NONE, GZIP, ZSTD
    secret_id: Optional[str] = None


@dataclass
class RestoreConfig:
    """Configuration for point-in-time restore."""
    source_keyspace_name: str
    source_table_name: str
    target_keyspace_name: str
    target_table_name: str
    restore_timestamp: str
    encryption_settings: Optional[Dict[str, Any]] = None


@dataclass
class SparkConnectorConfig:
    """Configuration for Spark connector."""
    spark_master_url: str = "local[*]"
    app_name: str = "keyspaces-spark-connector"
    keyspaces_connection_timeout_ms: int = 10000
    keyspaces_request_timeout_ms: int = 30000
    spark_conf: Dict[str, str] = field(default_factory=dict)


@dataclass
class CloudWatchConfig:
    """Configuration for CloudWatch monitoring."""
    metrics_enabled: bool = True
    log_level: str = "INFO"
    metrics_namespace: str = "AWS/Keyspaces"
    custom_metrics: List[str] = field(default_factory=list)


class KeyspacesIntegration:
    """
    AWS Keyspaces Integration.
    
    Provides comprehensive Amazon Keyspaces (Cassandra-compatible) management including:
    - Keyspace lifecycle management (create, modify, delete)
    - Table management with schema definitions
    - Point-in-time recovery (PITR) enable/disable
    - Data import from S3
    - Point-in-time restore of tables
    - Server-side encryption configuration
    - TTL (Time-to-Live) management
    - Multi-region replication
    - Apache Spark connector integration
    - CloudWatch metrics and monitoring
    """
    
    def __init__(self, config: Optional[KeyspacesConfig] = None):
        """
        Initialize Keyspaces integration.
        
        Args:
            config: Keyspaces configuration options
        """
        self.config = config or KeyspacesConfig()
        self._client = None
        self._resource = None
        self._keyspaces_lock = threading.RLock()
        self._tables_lock = threading.RLock()
        self._imports_lock = threading.RLock()
        self._restores_lock = threading.RLock()
        self._spark_lock = threading.RLock()
        self._keyspaces: Dict[str, Dict[str, Any]] = {}
        self._tables: Dict[str, Dict[str, Any]] = {}
        self._import_tasks: Dict[str, Dict[str, Any]] = {}
        self._restore_tasks: Dict[str, Dict[str, Any]] = {}
        self._spark_connectors: Dict[str, Dict[str, Any]] = {}
        self._cloudwatch_metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._monitoring_callbacks: List[Callable] = []
        self._encryption_defaults = {
            "encryption_type": "AWS_OWNED_KMS_KEY",
            "kms_key_identifier": None
        }
        
    @property
    def client(self):
        """Get or create Keyspaces client."""
        if self._client is None:
            kwargs = {"region_name": self.config.region_name}
            if self.config.aws_access_key_id:
                kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            if self.config.aws_secret_access_key:
                kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
            if self.config.aws_session_token:
                kwargs["aws_session_token"] = self.config.aws_session_token
            if self.config.profile_name:
                kwargs["profile_name"] = self.config.profile_name
            self._client = boto3.client("cassandra", **kwargs)
        return self._client

    # =========================================================================
    # Keyspace Management
    # =========================================================================
    
    def create_keyspace(
        self,
        config: KeyspaceConfig,
        wait_for_completion: bool = True,
        timeout: int = 120
    ) -> Dict[str, Any]:
        """
        Create a keyspace.
        
        Args:
            config: Keyspace configuration
            wait_for_completion: Wait for keyspace to be active
            timeout: Maximum time to wait in seconds
            
        Returns:
            Keyspace information dict
        """
        with self._keyspaces_lock:
            logger.info(f"Creating keyspace: {config.keyspace_name}")
            
            if not BOTO3_AVAILABLE:
                keyspace = self._create_mock_keyspace(config)
                self._keyspaces[config.keyspace_name] = keyspace
                return keyspace
            
            params = {
                "keyspaceName": config.keyspace_name,
            }
            
            if config.replication_settings:
                params["replicationSpecification"] = config.replication_settings
            if config.tags:
                params["tags"] = [{"key": k, "value": v} for k, v in config.tags.items()]
                
            try:
                response = self.client.create_keyspace(**params)
                keyspace = response.get("keyspace", {})
                
                result = {
                    "keyspace_name": keyspace.get("keyspaceName", config.keyspace_name),
                    "arn": keyspace.get("keyspaceArn"),
                    "replication_settings": keyspace.get("replicationSpecification", config.replication_settings),
                    "point_in_time_recovery": config.point_in_time_recovery,
                    "created_at": datetime.utcnow().isoformat(),
                }
                
                self._keyspaces[config.keyspace_name] = result
                
                if config.point_in_time_recovery:
                    self.enable_keyspace_pitr(config.keyspace_name)
                
                return result
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create keyspace: {e}")
                raise
    
    def _create_mock_keyspace(self, config: KeyspaceConfig) -> Dict[str, Any]:
        """Create a mock keyspace for testing without boto3."""
        return {
            "keyspace_name": config.keyspace_name,
            "arn": f"arn:aws:cassandra:{self.config.region_name}:123456789012:/keyspace/{config.keyspace_name}",
            "replication_settings": config.replication_settings or {
                "regionList": [self.config.region_name],
                "replicationStrategy": "SINGLE_REGION"
            },
            "point_in_time_recovery": config.point_in_time_recovery,
            "created_at": datetime.utcnow().isoformat(),
        }
    
    def describe_keyspace(self, keyspace_name: str) -> Dict[str, Any]:
        """
        Describe a keyspace.
        
        Args:
            keyspace_name: Keyspace name
            
        Returns:
            Keyspace information dict
        """
        with self._keyspaces_lock:
            if keyspace_name in self._keyspaces:
                return self._keyspaces[keyspace_name]
            
            if not BOTO3_AVAILABLE:
                return {}
            
            try:
                response = self.client.get_keyspace(keyspaceName=keyspace_name)
                keyspace = response.get("keyspace", {})
                
                result = {
                    "keyspace_name": keyspace.get("keyspaceName"),
                    "arn": keyspace.get("keyspaceArn"),
                    "replication_settings": keyspace.get("replicationSpecification"),
                    "point_in_time_recovery": keyspace.get("pointInTimeRecovery", {}).get("status") == "ENABLED",
                }
                
                self._keyspaces[keyspace_name] = result
                return result
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return {}
                raise
    
    def list_keyspaces(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List keyspaces.
        
        Args:
            filters: Optional filters
            
        Returns:
            List of keyspace information dicts
        """
        with self._keyspaces_lock:
            if not BOTO3_AVAILABLE:
                return list(self._keyspaces.values())
            
            try:
                response = self.client.list_keyspaces()
                keyspaces = []
                
                for keyspace in response.get("keyspaces", []):
                    result = {
                        "keyspace_name": keyspace.get("keyspaceName"),
                        "arn": keyspace.get("keyspaceArn"),
                    }
                    keyspaces.append(result)
                    
                return keyspaces
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list keyspaces: {e}")
                raise
    
    def delete_keyspace(self, keyspace_name: str) -> Dict[str, Any]:
        """
        Delete a keyspace.
        
        Args:
            keyspace_name: Keyspace name
            
        Returns:
            Deletion result
        """
        with self._keyspaces_lock:
            logger.info(f"Deleting keyspace: {keyspace_name}")
            
            if not BOTO3_AVAILABLE:
                if keyspace_name in self._keyspaces:
                    del self._keyspaces[keyspace_name]
                return {"keyspace_name": keyspace_name, "status": "deleted"}
            
            try:
                self.client.delete_keyspace(keyspaceName=keyspace_name)
                
                if keyspace_name in self._keyspaces:
                    del self._keyspaces[keyspace_name]
                
                return {"keyspace_name": keyspace_name, "status": "deleted"}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to delete keyspace: {e}")
                raise
    
    def enable_keyspace_pitr(self, keyspace_name: str) -> Dict[str, Any]:
        """
        Enable point-in-time recovery for a keyspace.
        
        Args:
            keyspace_name: Keyspace name
            
        Returns:
            PITR status
        """
        with self._keyspaces_lock:
            logger.info(f"Enabling PITR for keyspace: {keyspace_name}")
            
            if not BOTO3_AVAILABLE:
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["point_in_time_recovery"] = True
                return {"keyspace_name": keyspace_name, "pitr_enabled": True}
            
            try:
                self.client.update_keyspace(
                    keyspaceName=keyspace_name,
                    pointInTimeRecovery={
                        "status": "ENABLED"
                    }
                )
                
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["point_in_time_recovery"] = True
                
                return {"keyspace_name": keyspace_name, "pitr_enabled": True}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to enable PITR: {e}")
                raise
    
    def disable_keyspace_pitr(self, keyspace_name: str) -> Dict[str, Any]:
        """
        Disable point-in-time recovery for a keyspace.
        
        Args:
            keyspace_name: Keyspace name
            
        Returns:
            PITR status
        """
        with self._keyspaces_lock:
            logger.info(f"Disabling PITR for keyspace: {keyspace_name}")
            
            if not BOTO3_AVAILABLE:
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["point_in_time_recovery"] = False
                return {"keyspace_name": keyspace_name, "pitr_enabled": False}
            
            try:
                self.client.update_keyspace(
                    keyspaceName=keyspace_name,
                    pointInTimeRecovery={
                        "status": "DISABLED"
                    }
                )
                
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["point_in_time_recovery"] = False
                
                return {"keyspace_name": keyspace_name, "pitr_enabled": False}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to disable PITR: {e}")
                raise

    # =========================================================================
    # Table Management
    # =========================================================================
    
    def create_table(
        self,
        config: TableConfig,
        wait_for_completion: bool = True,
        timeout: int = 180
    ) -> Dict[str, Any]:
        """
        Create a table.
        
        Args:
            config: Table configuration
            wait_for_completion: Wait for table to be active
            timeout: Maximum time to wait in seconds
            
        Returns:
            Table information dict
        """
        table_id = f"{config.keyspace_name}.{config.table_name}"
        with self._tables_lock:
            logger.info(f"Creating table: {table_id}")
            
            if not BOTO3_AVAILABLE:
                table = self._create_mock_table(config)
                self._tables[table_id] = table
                return table
            
            schema = self._build_schema_definition(config)
            
            params = {
                "keyspaceName": config.keyspace_name,
                "tableName": config.table_name,
                "schemaDefinition": schema,
            }
            
            if config.comment:
                params["comment"] = config.comment
            if config.default_time_to_live > 0:
                params["defaultTimeToLive"] = config.default_time_to_live
            if config.point_in_time_recovery:
                params["pointInTimeRecovery"] = {"status": "ENABLED"}
            if config.encryption_settings:
                params["encryptionSpecification"] = config.encryption_settings
            if config.capacity_mode:
                params["capacitySpecification"] = {
                    "throughputMode": config.capacity_mode,
                    "provisionedThroughput": {
                        "readCapacityUnits": config.throughput,
                        "writeCapacityUnits": config.throughput
                    } if config.capacity_mode == "PROVISIONED" else None
                }
            if config.max_throughput and config.capacity_mode == "PROVISIONED":
                params["capacitySpecification"]["provisionedThroughput"]["maxCapacityUnits"] = config.max_throughput
            if config.tags:
                params["tags"] = [{"key": k, "value": v} for k, v in config.tags.items()]
                
            try:
                response = self.client.create_table(**params)
                table = response.get("table", {})
                
                result = self._parse_table_response(table, config)
                self._tables[table_id] = result
                
                return result
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create table: {e}")
                raise
    
    def _build_schema_definition(self, config: TableConfig) -> Dict[str, Any]:
        """Build schema definition for table creation."""
        schema = {
            "partitionKeys": [{"name": col, "types": "UTF8"} for col in config.partition_key_columns],
        }
        
        if config.clustering_key_columns:
            schema["clusteringKeys"] = [{"name": col, "types": "UTF8", "orderBy": "ASC"} for col in config.clustering_key_columns]
        
        if config.static_columns:
            schema["staticKeys"] = [{"name": col, "types": "UTF8"} for col in config.static_columns]
        
        return schema
    
    def _create_mock_table(self, config: TableConfig) -> Dict[str, Any]:
        """Create a mock table for testing without boto3."""
        table_id = f"{config.keyspace_name}.{config.table_name}"
        return {
            "keyspace_name": config.keyspace_name,
            "table_name": config.table_name,
            "arn": f"arn:aws:cassandra:{self.config.region_name}:123456789012:/keyspace/{config.keyspace_name}/table/{config.table_name}",
            "partition_key_columns": config.partition_key_columns,
            "clustering_key_columns": config.clustering_key_columns,
            "static_columns": config.static_columns,
            "default_time_to_live": config.default_time_to_live,
            "point_in_time_recovery": config.point_in_time_recovery,
            "encryption_settings": config.encryption_settings or self._encryption_defaults,
            "capacity_mode": config.capacity_mode,
            "throughput": config.throughput,
            "status": "ACTIVE",
            "created_at": datetime.utcnow().isoformat(),
        }
    
    def _parse_table_response(self, table: Dict[str, Any], config: TableConfig) -> Dict[str, Any]:
        """Parse table response into standardized format."""
        schema = table.get("schemaDefinition", {})
        partition_keys = schema.get("partitionKeys", [])
        clustering_keys = schema.get("clusteringKeys", [])
        static_keys = schema.get("staticKeys", [])
        
        return {
            "keyspace_name": table.get("keyspaceName", config.keyspace_name),
            "table_name": table.get("tableName", config.table_name),
            "arn": table.get("tableArn"),
            "partition_key_columns": [k["name"] for k in partition_keys],
            "clustering_key_columns": [k["name"] for k in clustering_keys],
            "static_columns": [k["name"] for k in static_keys],
            "default_time_to_live": table.get("defaultTimeToLive", 0),
            "point_in_time_recovery": table.get("pointInTimeRecovery", {}).get("status") == "ENABLED",
            "encryption_settings": table.get("encryptionSpecification", self._encryption_defaults),
            "capacity_mode": table.get("capacitySpecification", {}).get("throughputMode", "PROVISIONED"),
            "throughput": table.get("capacitySpecification", {}).get("provisionedThroughput", {}).get("readCapacityUnits", 0),
            "status": table.get("status", "UNKNOWN"),
            "created_at": datetime.utcnow().isoformat(),
        }
    
    def describe_table(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Describe a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            Table information dict
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            if table_id in self._tables:
                return self._tables[table_id]
            
            if not BOTO3_AVAILABLE:
                return {}
            
            try:
                response = self.client.get_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name
                )
                table = response.get("table", {})
                
                result = {
                    "keyspace_name": table.get("keyspaceName"),
                    "table_name": table.get("tableName"),
                    "arn": table.get("tableArn"),
                    "status": table.get("status"),
                    "default_time_to_live": table.get("defaultTimeToLive", 0),
                    "point_in_time_recovery": table.get("pointInTimeRecovery", {}).get("status") == "ENABLED",
                }
                
                self._tables[table_id] = result
                return result
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return {}
                raise
    
    def list_tables(self, keyspace_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List tables in a keyspace.
        
        Args:
            keyspace_name: Keyspace name
            filters: Optional filters
            
        Returns:
            List of table information dicts
        """
        with self._tables_lock:
            if not BOTO3_AVAILABLE:
                return [t for t in self._tables.values() if t.get("keyspace_name") == keyspace_name]
            
            try:
                response = self.client.list_tables(keyspaceName=keyspace_name)
                tables = []
                
                for table in response.get("tables", []):
                    result = {
                        "keyspace_name": table.get("keyspaceName"),
                        "table_name": table.get("tableName"),
                        "arn": table.get("tableArn"),
                    }
                    tables.append(result)
                    
                return tables
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list tables: {e}")
                raise
    
    def delete_table(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Delete a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            Deletion result
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            logger.info(f"Deleting table: {table_id}")
            
            if not BOTO3_AVAILABLE:
                if table_id in self._tables:
                    del self._tables[table_id]
                return {"keyspace_name": keyspace_name, "table_name": table_name, "status": "deleted"}
            
            try:
                self.client.delete_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name
                )
                
                if table_id in self._tables:
                    del self._tables[table_id]
                
                return {"keyspace_name": keyspace_name, "table_name": table_name, "status": "deleted"}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to delete table: {e}")
                raise
    
    def update_table_ttl(self, keyspace_name: str, table_name: str, default_time_to_live: int) -> Dict[str, Any]:
        """
        Update table TTL (Time-to-Live).
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            default_time_to_live: New TTL in seconds (0 to disable)
            
        Returns:
            Update result
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            logger.info(f"Updating TTL for table: {table_id} to {default_time_to_live} seconds")
            
            if not BOTO3_AVAILABLE:
                if table_id in self._tables:
                    self._tables[table_id]["default_time_to_live"] = default_time_to_live
                return {"keyspace_name": keyspace_name, "table_name": table_name, "default_time_to_live": default_time_to_live}
            
            try:
                self.client.update_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name,
                    defaultTimeToLive=default_time_to_live
                )
                
                if table_id in self._tables:
                    self._tables[table_id]["default_time_to_live"] = default_time_to_live
                
                return {"keyspace_name": keyspace_name, "table_name": table_name, "default_time_to_live": default_time_to_live}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to update table TTL: {e}")
                raise

    # =========================================================================
    # Point-in-Time Recovery
    # =========================================================================
    
    def enable_table_pitr(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Enable point-in-time recovery for a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            PITR status
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            logger.info(f"Enabling PITR for table: {table_id}")
            
            if not BOTO3_AVAILABLE:
                if table_id in self._tables:
                    self._tables[table_id]["point_in_time_recovery"] = True
                return {"keyspace_name": keyspace_name, "table_name": table_name, "pitr_enabled": True}
            
            try:
                self.client.update_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name,
                    pointInTimeRecovery={"status": "ENABLED"}
                )
                
                if table_id in self._tables:
                    self._tables[table_id]["point_in_time_recovery"] = True
                
                return {"keyspace_name": keyspace_name, "table_name": table_name, "pitr_enabled": True}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to enable table PITR: {e}")
                raise
    
    def disable_table_pitr(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Disable point-in-time recovery for a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            PITR status
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            logger.info(f"Disabling PITR for table: {table_id}")
            
            if not BOTO3_AVAILABLE:
                if table_id in self._tables:
                    self._tables[table_id]["point_in_time_recovery"] = False
                return {"keyspace_name": keyspace_name, "table_name": table_name, "pitr_enabled": False}
            
            try:
                self.client.update_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name,
                    pointInTimeRecovery={"status": "DISABLED"}
                )
                
                if table_id in self._tables:
                    self._tables[table_id]["point_in_time_recovery"] = False
                
                return {"keyspace_name": keyspace_name, "table_name": table_name, "pitr_enabled": False}
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to disable table PITR: {e}")
                raise
    
    def get_table_pitr_status(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get point-in-time recovery status for a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            PITR status information
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            table = self.describe_table(keyspace_name, table_name)
            
            return {
                "keyspace_name": keyspace_name,
                "table_name": table_name,
                "pitr_enabled": table.get("point_in_time_recovery", False),
                "earliest_restorable_timestamp": table.get("earliest_restorable_timestamp"),
            }

    # =========================================================================
    # Data Import from S3
    # =========================================================================
    
    def create_import_task(
        self,
        config: ImportConfig,
        wait_for_completion: bool = True,
        timeout: int = 3600
    ) -> Dict[str, Any]:
        """
        Create and execute a data import task from S3.
        
        Args:
            config: Import configuration
            wait_for_completion: Wait for import to complete
            timeout: Maximum time to wait in seconds
            
        Returns:
            Import task information
        """
        import_id = str(uuid.uuid4())
        with self._imports_lock:
            logger.info(f"Creating import task: {import_id}")
            
            if not BOTO3_AVAILABLE:
                task = self._create_mock_import_task(config, import_id)
                self._import_tasks[import_id] = task
                return task
            
            params = {
                "keyspaceName": config.keyspace_name,
                "tableName": config.table_name,
                "s3Bucket": config.s3_bucket,
                "s3Prefix": config.s3_prefix,
                "iamRoleArn": config.iam_role_arn,
                "compressionType": config.compression_type,
            }
            
            if config.secret_id:
                params["secretId"] = config.secret_id
                
            try:
                response = self.client.create_import_task(**params)
                task = response.get("importTask", {})
                
                result = {
                    "import_id": task.get("importTaskId", import_id),
                    "keyspace_name": task.get("keyspaceName", config.keyspace_name),
                    "table_name": task.get("tableName", config.table_name),
                    "s3_bucket": task.get("s3Bucket", config.s3_bucket),
                    "s3_prefix": task.get("s3Prefix", config.s3_prefix),
                    "status": task.get("status", "IN_PROGRESS"),
                    "created_at": datetime.utcnow().isoformat(),
                }
                
                self._import_tasks[import_id] = result
                
                if wait_for_completion:
                    self._wait_for_import_complete(import_id, timeout)
                    result["status"] = "COMPLETED"
                
                return result
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create import task: {e}")
                raise
    
    def _create_mock_import_task(self, config: ImportConfig, import_id: str) -> Dict[str, Any]:
        """Create a mock import task for testing."""
        return {
            "import_id": import_id,
            "keyspace_name": config.keyspace_name,
            "table_name": config.table_name,
            "s3_bucket": config.s3_bucket,
            "s3_prefix": config.s3_prefix,
            "status": "IN_PROGRESS",
            "created_at": datetime.utcnow().isoformat(),
        }
    
    def _wait_for_import_complete(self, import_id: str, timeout: int = 3600):
        """Wait for import task to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            task = self.get_import_task(import_id)
            status = task.get("status", "")
            if status == "COMPLETED":
                return
            elif status == "FAILED":
                raise TimeoutError(f"Import task {import_id} failed")
            time.sleep(30)
        raise TimeoutError(f"Timeout waiting for import task {import_id}")
    
    def get_import_task(self, import_id: str) -> Dict[str, Any]:
        """
        Get import task status.
        
        Args:
            import_id: Import task ID
            
        Returns:
            Import task information
        """
        with self._imports_lock:
            if import_id in self._import_tasks:
                return self._import_tasks[import_id]
            
            if not BOTO3_AVAILABLE:
                return {}
            
            try:
                response = self.client.get_import_task(importTaskId=import_id)
                task = response.get("importTask", {})
                
                return {
                    "import_id": task.get("importTaskId"),
                    "keyspace_name": task.get("keyspaceName"),
                    "table_name": task.get("tableName"),
                    "status": task.get("status"),
                    "imported_bytes": task.get("importedBytes", 0),
                    "imported_records": task.get("importedRecords", 0),
                    "failed_records": task.get("failedRecords", 0),
                    "error_count": task.get("errorCount", 0),
                    "created_at": task.get("startTime"),
                    "completed_at": task.get("endTime"),
                }
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return {}
                raise
    
    def list_import_tasks(
        self,
        keyspace_name: Optional[str] = None,
        table_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List import tasks.
        
        Args:
            keyspace_name: Optional keyspace filter
            table_name: Optional table filter
            
        Returns:
            List of import tasks
        """
        with self._imports_lock:
            if not BOTO3_AVAILABLE:
                tasks = list(self._import_tasks.values())
                if keyspace_name:
                    tasks = [t for t in tasks if t.get("keyspace_name") == keyspace_name]
                if table_name:
                    tasks = [t for t in tasks if t.get("table_name") == table_name]
                return tasks
            
            try:
                params = {}
                if keyspace_name:
                    params["keyspaceName"] = keyspace_name
                if table_name:
                    params["tableName"] = table_name
                    
                response = self.client.list_import_tasks(**params)
                tasks = []
                
                for task in response.get("importTasks", []):
                    result = {
                        "import_id": task.get("importTaskId"),
                        "keyspace_name": task.get("keyspaceName"),
                        "table_name": task.get("tableName"),
                        "status": task.get("status"),
                    }
                    tasks.append(result)
                    
                return tasks
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list import tasks: {e}")
                raise

    # =========================================================================
    # Point-in-Time Restore
    # =========================================================================
    
    def restore_table(
        self,
        config: RestoreConfig,
        wait_for_completion: bool = True,
        timeout: int = 1800
    ) -> Dict[str, Any]:
        """
        Restore a table from point-in-time.
        
        Args:
            config: Restore configuration
            wait_for_completion: Wait for restore to complete
            timeout: Maximum time to wait in seconds
            
        Returns:
            Restore task information
        """
        restore_id = str(uuid.uuid4())
        with self._restores_lock:
            logger.info(f"Restoring table: {config.source_keyspace_name}.{config.source_table_name}")
            
            if not BOTO3_AVAILABLE:
                task = self._create_mock_restore_task(config, restore_id)
                self._restore_tasks[restore_id] = task
                return task
            
            params = {
                "sourceKeyspaceName": config.source_keyspace_name,
                "sourceTableName": config.source_table_name,
                "targetKeyspaceName": config.target_keyspace_name,
                "targetTableName": config.target_table_name,
                "restoreTimestamp": config.restore_timestamp,
            }
            
            if config.encryption_settings:
                params["encryptionSpecification"] = config.encryption_settings
                
            try:
                response = self.client.restore_table(**params)
                task = response.get("restoreTask", {})
                
                result = {
                    "restore_id": task.get("restoreTaskId", restore_id),
                    "source_keyspace_name": task.get("sourceKeyspaceName", config.source_keyspace_name),
                    "source_table_name": task.get("sourceTableName", config.source_table_name),
                    "target_keyspace_name": task.get("targetKeyspaceName", config.target_keyspace_name),
                    "target_table_name": task.get("targetTableName", config.target_table_name),
                    "restore_timestamp": task.get("restoreTimestamp", config.restore_timestamp),
                    "status": task.get("status", "IN_PROGRESS"),
                    "created_at": datetime.utcnow().isoformat(),
                }
                
                self._restore_tasks[restore_id] = result
                
                if wait_for_completion:
                    self._wait_for_restore_complete(restore_id, timeout)
                    result["status"] = "COMPLETED"
                
                return result
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to restore table: {e}")
                raise
    
    def _create_mock_restore_task(self, config: RestoreConfig, restore_id: str) -> Dict[str, Any]:
        """Create a mock restore task for testing."""
        return {
            "restore_id": restore_id,
            "source_keyspace_name": config.source_keyspace_name,
            "source_table_name": config.source_table_name,
            "target_keyspace_name": config.target_keyspace_name,
            "target_table_name": config.target_table_name,
            "restore_timestamp": config.restore_timestamp,
            "status": "IN_PROGRESS",
            "created_at": datetime.utcnow().isoformat(),
        }
    
    def _wait_for_restore_complete(self, restore_id: str, timeout: int = 1800):
        """Wait for restore task to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            task = self.get_restore_task(restore_id)
            status = task.get("status", "")
            if status == "COMPLETED":
                return
            elif status == "FAILED":
                raise TimeoutError(f"Restore task {restore_id} failed")
            time.sleep(30)
        raise TimeoutError(f"Timeout waiting for restore task {restore_id}")
    
    def get_restore_task(self, restore_id: str) -> Dict[str, Any]:
        """
        Get restore task status.
        
        Args:
            restore_id: Restore task ID
            
        Returns:
            Restore task information
        """
        with self._restores_lock:
            if restore_id in self._restore_tasks:
                return self._restore_tasks[restore_id]
            
            if not BOTO3_AVAILABLE:
                return {}
            
            try:
                response = self.client.get_restore_task(restoreTaskId=restore_id)
                task = response.get("restoreTask", {})
                
                return {
                    "restore_id": task.get("restoreTaskId"),
                    "source_keyspace_name": task.get("sourceKeyspaceName"),
                    "source_table_name": task.get("sourceTableName"),
                    "target_keyspace_name": task.get("targetKeyspaceName"),
                    "target_table_name": task.get("targetTableName"),
                    "restore_timestamp": task.get("restoreTimestamp"),
                    "status": task.get("status"),
                    "created_at": task.get("startTime"),
                    "completed_at": task.get("endTime"),
                }
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return {}
                raise
    
    def list_restore_tasks(
        self,
        keyspace_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List restore tasks.
        
        Args:
            keyspace_name: Optional keyspace filter
            
        Returns:
            List of restore tasks
        """
        with self._restores_lock:
            if not BOTO3_AVAILABLE:
                tasks = list(self._restore_tasks.values())
                if keyspace_name:
                    tasks = [t for t in tasks if t.get("target_keyspace_name") == keyspace_name]
                return tasks
            
            try:
                params = {}
                if keyspace_name:
                    params["keyspaceName"] = keyspace_name
                    
                response = self.client.list_restore_tasks(**params)
                tasks = []
                
                for task in response.get("restoreTasks", []):
                    result = {
                        "restore_id": task.get("restoreTaskId"),
                        "source_keyspace_name": task.get("sourceKeyspaceName"),
                        "source_table_name": task.get("sourceTableName"),
                        "target_keyspace_name": task.get("targetKeyspaceName"),
                        "target_table_name": task.get("targetTableName"),
                        "status": task.get("status"),
                    }
                    tasks.append(result)
                    
                return tasks
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list restore tasks: {e}")
                raise

    # =========================================================================
    # Encryption
    # =========================================================================
    
    def get_encryption_settings(self, keyspace_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get encryption settings for a table.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            
        Returns:
            Encryption settings
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            table = self.describe_table(keyspace_name, table_name)
            encryption = table.get("encryption_settings", self._encryption_defaults)
            
            return {
                "keyspace_name": keyspace_name,
                "table_name": table_name,
                "encryption_type": encryption.get("encryptionType", "AWS_OWNED_KMS_KEY"),
                "kms_key_identifier": encryption.get("kmsKeyIdentifier"),
            }
    
    def update_table_encryption(
        self,
        keyspace_name: str,
        table_name: str,
        encryption_type: str,
        kms_key_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update table encryption settings.
        
        Args:
            keyspace_name: Keyspace name
            table_name: Table name
            encryption_type: Encryption type (AWS_OWNED_KMS_KEY, CUSTOMER_MANAGED_KMS_KEY)
            kms_key_identifier: KMS key ID (required for CUSTOMER_MANAGED_KMS_KEY)
            
        Returns:
            Update result
        """
        table_id = f"{keyspace_name}.{table_name}"
        with self._tables_lock:
            logger.info(f"Updating encryption for table: {table_id}")
            
            encryption_settings = {
                "encryptionType": encryption_type,
            }
            if kms_key_identifier:
                encryption_settings["kmsKeyIdentifier"] = kms_key_identifier
            
            if not BOTO3_AVAILABLE:
                if table_id in self._tables:
                    self._tables[table_id]["encryption_settings"] = encryption_settings
                return {
                    "keyspace_name": keyspace_name,
                    "table_name": table_name,
                    "encryption_settings": encryption_settings,
                }
            
            try:
                self.client.update_table(
                    keyspaceName=keyspace_name,
                    tableName=table_name,
                    encryptionSpecification=encryption_settings
                )
                
                if table_id in self._tables:
                    self._tables[table_id]["encryption_settings"] = encryption_settings
                
                return {
                    "keyspace_name": keyspace_name,
                    "table_name": table_name,
                    "encryption_settings": encryption_settings,
                }
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to update encryption: {e}")
                raise

    # =========================================================================
    # Multi-Region Replication
    # =========================================================================
    
    def create_multi_region_keyspace(
        self,
        keyspace_name: str,
        region_list: List[str],
        point_in_time_recovery: bool = False,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a multi-region keyspace.
        
        Args:
            keyspace_name: Keyspace name
            region_list: List of AWS regions for replication
            point_in_time_recovery: Enable PITR
            tags: Optional tags
            
        Returns:
            Keyspace information
        """
        with self._keyspaces_lock:
            logger.info(f"Creating multi-region keyspace: {keyspace_name} in regions: {region_list}")
            
            replication_settings = {
                "replicationStrategy": "MULTI_REGION",
                "regionList": region_list,
            }
            
            if not BOTO3_AVAILABLE:
                config = KeyspaceConfig(
                    keyspace_name=keyspace_name,
                    replication_settings=replication_settings,
                    point_in_time_recovery=point_in_time_recovery,
                    tags=tags or {}
                )
                keyspace = self._create_mock_keyspace(config)
                self._keyspaces[keyspace_name] = keyspace
                return keyspace
            
            try:
                response = self.client.create_keyspace(
                    keyspaceName=keyspace_name,
                    replicationSpecification=replication_settings,
                    tags=[{"key": k, "value": v} for k, v in (tags or {}).items()] if tags else []
                )
                
                keyspace = response.get("keyspace", {})
                result = {
                    "keyspace_name": keyspace.get("keyspaceName", keyspace_name),
                    "arn": keyspace.get("keyspaceArn"),
                    "replication_settings": replication_settings,
                    "point_in_time_recovery": point_in_time_recovery,
                    "created_at": datetime.utcnow().isoformat(),
                }
                
                self._keyspaces[keyspace_name] = result
                
                if point_in_time_recovery:
                    self.enable_keyspace_pitr(keyspace_name)
                
                return result
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create multi-region keyspace: {e}")
                raise
    
    def add_region_to_keyspace(
        self,
        keyspace_name: str,
        region_name: str
    ) -> Dict[str, Any]:
        """
        Add a region to an existing multi-region keyspace.
        
        Args:
            keyspace_name: Keyspace name
            region_name: Region to add
            
        Returns:
            Update result
        """
        with self._keyspaces_lock:
            logger.info(f"Adding region {region_name} to keyspace: {keyspace_name}")
            
            keyspace = self.describe_keyspace(keyspace_name)
            current_regions = keyspace.get("replication_settings", {}).get("regionList", [])
            
            if region_name in current_regions:
                return {
                    "keyspace_name": keyspace_name,
                    "region_added": region_name,
                    "status": "already_exists",
                }
            
            new_regions = current_regions + [region_name]
            
            if not BOTO3_AVAILABLE:
                keyspace["replication_settings"]["regionList"] = new_regions
                self._keyspaces[keyspace_name] = keyspace
                return {
                    "keyspace_name": keyspace_name,
                    "region_added": region_name,
                    "all_regions": new_regions,
                }
            
            try:
                self.client.update_keyspace(
                    keyspaceName=keyspace_name,
                    replicationSpecification={
                        "regionList": new_regions,
                        "replicationStrategy": "MULTI_REGION"
                    }
                )
                
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["replication_settings"]["regionList"] = new_regions
                
                return {
                    "keyspace_name": keyspace_name,
                    "region_added": region_name,
                    "all_regions": new_regions,
                }
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to add region to keyspace: {e}")
                raise
    
    def remove_region_from_keyspace(
        self,
        keyspace_name: str,
        region_name: str
    ) -> Dict[str, Any]:
        """
        Remove a region from an existing multi-region keyspace.
        
        Args:
            keyspace_name: Keyspace name
            region_name: Region to remove
            
        Returns:
            Update result
        """
        with self._keyspaces_lock:
            logger.info(f"Removing region {region_name} from keyspace: {keyspace_name}")
            
            keyspace = self.describe_keyspace(keyspace_name)
            current_regions = keyspace.get("replication_settings", {}).get("regionList", [])
            
            if region_name not in current_regions:
                return {
                    "keyspace_name": keyspace_name,
                    "region_removed": region_name,
                    "status": "not_found",
                }
            
            if len(current_regions) <= 1:
                raise ValueError("Cannot remove the last region from a keyspace")
            
            new_regions = [r for r in current_regions if r != region_name]
            
            if not BOTO3_AVAILABLE:
                keyspace["replication_settings"]["regionList"] = new_regions
                self._keyspaces[keyspace_name] = keyspace
                return {
                    "keyspace_name": keyspace_name,
                    "region_removed": region_name,
                    "all_regions": new_regions,
                }
            
            try:
                self.client.update_keyspace(
                    keyspaceName=keyspace_name,
                    replicationSpecification={
                        "regionList": new_regions,
                        "replicationStrategy": "MULTI_REGION"
                    }
                )
                
                if keyspace_name in self._keyspaces:
                    self._keyspaces[keyspace_name]["replication_settings"]["regionList"] = new_regions
                
                return {
                    "keyspace_name": keyspace_name,
                    "region_removed": region_name,
                    "all_regions": new_regions,
                }
                
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to remove region from keyspace: {e}")
                raise

    # =========================================================================
    # Apache Spark Connector
    # =========================================================================
    
    def configure_spark_connector(
        self,
        config: SparkConnectorConfig
    ) -> Dict[str, Any]:
        """
        Configure Apache Spark connector for Keyspaces.
        
        Args:
            config: Spark connector configuration
            
        Returns:
            Connector configuration status
        """
        connector_id = str(uuid.uuid4())
        with self._spark_lock:
            logger.info(f"Configuring Spark connector: {connector_id}")
            
            spark_conf = {
                "spark.master": config.spark_master_url,
                "spark.app.name": config.app_name,
                "spark.cassandra.connection.timeout_ms": str(config.keyspaces_connection_timeout_ms),
                "spark.cassandra.read.timeout_ms": str(config.keyspaces_request_timeout_ms),
            }
            spark_conf.update(config.spark_conf)
            
            connector = {
                "connector_id": connector_id,
                "spark_master_url": config.spark_master_url,
                "app_name": config.app_name,
                "connection_timeout_ms": config.keyspaces_connection_timeout_ms,
                "request_timeout_ms": config.keyspaces_request_timeout_ms,
                "spark_conf": spark_conf,
                "status": "CONFIGURED",
                "configured_at": datetime.utcnow().isoformat(),
            }
            
            self._spark_connectors[connector_id] = connector
            
            return {
                "connector_id": connector_id,
                "status": "CONFIGURED",
                "spark_conf": spark_conf,
            }
    
    def get_spark_connector_config(self, connector_id: str) -> Dict[str, Any]:
        """
        Get Spark connector configuration.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            Connector configuration
        """
        with self._spark_lock:
            return self._spark_connectors.get(connector_id, {})
    
    def get_spark_dataframe(
        self,
        connector_id: str,
        keyspace_name: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a Spark DataFrame for reading from Keyspaces.
        
        Args:
            connector_id: Spark connector ID
            keyspace_name: Keyspace name
            table_name: Table name
            columns: Optional list of columns to read
            where_clause: Optional WHERE clause filter
            
        Returns:
            DataFrame configuration
        """
        with self._spark_lock:
            connector = self._spark_connectors.get(connector_id, {})
            
            if not connector:
                raise ValueError(f"Spark connector {connector_id} not found")
            
            df_config = {
                "connector_id": connector_id,
                "keyspace_name": keyspace_name,
                "table_name": table_name,
                "columns": columns,
                "where_clause": where_clause,
                "spark_conf": connector.get("spark_conf", {}),
            }
            
            return {
                "dataframe_config": df_config,
                "read_options": {
                    "spark.cassandra.input.keyspace": keyspace_name,
                    "spark.cassandra.input.table": table_name,
                    "spark.cassandra.input.columns": ",".join(columns) if columns else "*",
                },
            }
    
    def write_spark_dataframe(
        self,
        connector_id: str,
        keyspace_name: str,
        table_name: str,
        write_mode: str = "append"  # append, overwrite, ignore, errorifexists
    ) -> Dict[str, Any]:
        """
        Get write configuration for Spark DataFrame to Keyspaces.
        
        Args:
            connector_id: Spark connector ID
            keyspace_name: Keyspace name
            table_name: Table name
            write_mode: Write mode
            
        Returns:
            Write configuration
        """
        with self._spark_lock:
            connector = self._spark_connectors.get(connector_id, {})
            
            if not connector:
                raise ValueError(f"Spark connector {connector_id} not found")
            
            return {
                "connector_id": connector_id,
                "keyspace_name": keyspace_name,
                "table_name": table_name,
                "write_mode": write_mode,
                "write_options": {
                    "spark.cassandra.output.keyspace": keyspace_name,
                    "spark.cassandra.output.table": table_name,
                    "spark.cassandra.output.consistency.level": "LOCAL_QUORUM",
                },
            }
    
    def list_spark_connectors(self) -> List[Dict[str, Any]]:
        """
        List configured Spark connectors.
        
        Returns:
            List of Spark connector configurations
        """
        with self._spark_lock:
            return list(self._spark_connectors.values())

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def enable_monitoring(
        self,
        config: Optional[CloudWatchConfig] = None
    ) -> Dict[str, Any]:
        """
        Enable CloudWatch monitoring for Keyspaces.
        
        Args:
            config: CloudWatch configuration
            
        Returns:
            Monitoring status
        """
        cloudwatch_config = config or CloudWatchConfig()
        
        if not BOTO3_AVAILABLE:
            return {
                "monitoring_enabled": cloudwatch_config.metrics_enabled,
                "metrics_namespace": cloudwatch_config.metrics_namespace,
                "log_level": cloudwatch_config.log_level,
            }
        
        try:
            cloudwatch_client = boto3.client("cloudwatch", region_name=self.config.region_name)
            
            dashboards = cloudwatch_client.list_dashboards()
            existing_dashboards = dashboards.get("DashboardEntries", [])
            
            dashboard_name = f"Keyspaces-Monitoring-{self.config.region_name}"
            dashboard_exists = any(d.get("DashboardName") == dashboard_name for d in existing_dashboards)
            
            if not dashboard_exists:
                dashboard_body = json.dumps({
                    "widgets": [
                        {
                            "type": "metric",
                            "properties": {
                                "metrics": [
                                    ["AWS/Keyspaces", "SuccessfulRequestCount", {"stat": "Sum"}],
                                    [".", "FailedRequestCount", {"stat": "Sum"}],
                                    [".", "Latency", {"stat": "Average"}],
                                ],
                                "period": 300,
                                "stat": "Sum",
                                "region": self.config.region_name,
                                "title": "Keyspaces Metrics"
                            }
                        }
                    ]
                })
                
                cloudwatch_client.put_dashboard(
                    DashboardName=dashboard_name,
                    DashboardBody=dashboard_body
                )
            
            return {
                "monitoring_enabled": cloudwatch_config.metrics_enabled,
                "metrics_namespace": cloudwatch_config.metrics_namespace,
                "dashboard_name": dashboard_name,
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to enable monitoring: {e}")
            raise
    
    def get_metrics(
        self,
        metric_names: List[str],
        keyspace_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistics: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for Keyspaces.
        
        Args:
            metric_names: List of metric names to retrieve
            keyspace_name: Optional keyspace dimension filter
            table_name: Optional table dimension filter
            start_time: Start time for metrics query
            end_time: End time for metrics query
            period: Period in seconds
            statistics: List of statistics (Sum, Average, Maximum, Minimum)
            
        Returns:
            List of metric data points
        """
        if not BOTO3_AVAILABLE:
            return []
        
        if not start_time:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if not end_time:
            end_time = datetime.utcnow()
        if not statistics:
            statistics = ["Sum", "Average"]
        
        try:
            cloudwatch_client = boto3.client("cloudwatch", region_name=self.config.region_name)
            
            dimensions = []
            if keyspace_name:
                dimensions.append({"Name": "KeyspaceName", "Value": keyspace_name})
            if table_name:
                dimensions.append({"Name": "TableName", "Value": table_name})
            
            metrics = []
            for metric_name in metric_names:
                params = {
                    "Namespace": "AWS/Keyspaces",
                    "MetricName": metric_name,
                    "StartTime": start_time,
                    "EndTime": end_time,
                    "Period": period,
                    "Statistics": statistics,
                }
                if dimensions:
                    params["Dimensions"] = dimensions
                
                response = cloudwatch_client.get_metric_statistics(**params)
                
                for datapoint in response.get("Datapoints", []):
                    metrics.append({
                        "metric_name": metric_name,
                        "timestamp": datapoint.get("Timestamp").isoformat() if datapoint.get("Timestamp") else None,
                        "value": datapoint.get("Sum", datapoint.get("Average", 0)),
                        "unit": datapoint.get("Unit"),
                        "dimensions": dimensions,
                    })
            
            self._cloudwatch_metrics["latest"] = metrics
            return metrics
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get metrics: {e}")
            raise
    
    def create_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Sum",
        keyspace_name: Optional[str] = None,
        table_name: Optional[str] = None,
        sns_topic_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for Keyspaces metrics.
        
        Args:
            alarm_name: Alarm name
            metric_name: Metric name to monitor
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic type
            keyspace_name: Optional keyspace dimension
            table_name: Optional table dimension
            sns_topic_arn: Optional SNS topic ARN for notifications
            
        Returns:
            Alarm information
        """
        if not BOTO3_AVAILABLE:
            return {
                "alarm_name": alarm_name,
                "metric_name": metric_name,
                "threshold": threshold,
                "status": "created",
            }
        
        try:
            cloudwatch_client = boto3.client("cloudwatch", region_name=self.config.region_name)
            
            dimensions = []
            if keyspace_name:
                dimensions.append({"Name": "KeyspaceName", "Value": keyspace_name})
            if table_name:
                dimensions.append({"Name": "TableName", "Value": table_name})
            
            params = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": "AWS/Keyspaces",
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": statistic,
            }
            
            if dimensions:
                params["Dimensions"] = dimensions
            
            if sns_topic_arn:
                params["AlarmActions"] = [sns_topic_arn]
            
            cloudwatch_client.put_metric_alarm(**params)
            
            return {
                "alarm_name": alarm_name,
                "metric_name": metric_name,
                "threshold": threshold,
                "status": "created",
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def list_alarms(
        self,
        keyspace_name: Optional[str] = None,
        state_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms for Keyspaces.
        
        Args:
            keyspace_name: Optional keyspace filter
            state_filter: Optional state filter (OK, ALARM, INSUFFICIENT_DATA)
            
        Returns:
            List of alarms
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            cloudwatch_client = boto3.client("cloudwatch", region_name=self.config.region_name)
            
            params = {"Namespace": "AWS/Keyspaces"}
            if state_filter:
                params["StateValue"] = state_filter
            
            response = cloudwatch_client.describe_alarms_for_metric(**params)
            alarms = []
            
            for alarm in response.get("MetricAlarms", []):
                alarm_info = {
                    "alarm_name": alarm.get("AlarmName"),
                    "metric_name": alarm.get("MetricName"),
                    "state": alarm.get("StateValue"),
                    "threshold": alarm.get("Threshold"),
                    "dimensions": alarm.get("Dimensions", []),
                }
                
                if keyspace_name:
                    dimensions = {d["Name"]: d["Value"] for d in alarm.get("Dimensions", [])}
                    if dimensions.get("KeyspaceName") != keyspace_name:
                        continue
                
                alarms.append(alarm_info)
            
            return alarms
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list alarms: {e}")
            raise
    
    def register_monitoring_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for monitoring events.
        
        Args:
            callback: Callback function to receive monitoring events
        """
        self._monitoring_callbacks.append(callback)
    
    def trigger_monitoring_callback(self, event: Dict[str, Any]):
        """Trigger all registered monitoring callbacks."""
        for callback in self._monitoring_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Monitoring callback error: {e}")
    
    def record_metric(self, metric_name: str, value: float, dimensions: Optional[Dict[str, str]] = None):
        """
        Record a custom metric.
        
        Args:
            metric_name: Metric name
            value: Metric value
            dimensions: Optional dimensions
        """
        metric_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "value": value,
            "dimensions": dimensions or {},
        }
        
        if metric_name not in self._cloudwatch_metrics:
            self._cloudwatch_metrics[metric_name] = []
        
        self._cloudwatch_metrics[metric_name].append(metric_entry)
        
        self.trigger_monitoring_callback({
            "type": "metric",
            "name": metric_name,
            "value": value,
            "dimensions": dimensions,
        })
    
    def get_recorded_metrics(self, metric_name: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get recorded metrics.
        
        Args:
            metric_name: Optional specific metric name
            
        Returns:
            Dictionary of metrics
        """
        if metric_name:
            return {metric_name: self._cloudwatch_metrics.get(metric_name, [])}
        return dict(self._cloudwatch_metrics)

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on Keyspaces integration.
        
        Returns:
            Health check result
        """
        try:
            if BOTO3_AVAILABLE:
                self.client.list_keyspaces()
            
            return {
                "status": "healthy",
                "region": self.config.region_name,
                "boto3_available": BOTO3_AVAILABLE,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "region": self.config.region_name,
            }
    
    def get_resource_summary(self) -> Dict[str, Any]:
        """
        Get summary of all Keyspaces resources.
        
        Returns:
            Resource summary
        """
        return {
            "keyspaces": {
                "count": len(self._keyspaces),
                "keyspaces": list(self._keyspaces.keys()),
            },
            "tables": {
                "count": len(self._tables),
                "tables": [f"{t.get('keyspace_name')}.{t.get('table_name')}" for t in self._tables.values()],
            },
            "import_tasks": {
                "count": len(self._import_tasks),
            },
            "restore_tasks": {
                "count": len(self._restore_tasks),
            },
            "spark_connectors": {
                "count": len(self._spark_connectors),
            },
            "metrics_recorded": {
                "count": len(self._cloudwatch_metrics),
            },
        }
