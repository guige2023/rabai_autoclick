"""
AWS Managed Streaming for Apache Kafka (MSK) Integration Module for Workflow System

Implements an MSKIntegration class with:
1. Cluster management: Create/manage MSK clusters
2. Broker management: Manage brokers and storage
3. Topic management: Create/manage Kafka topics
4. Producer/consumer: Managed producer and consumer
5. Schema registry: Manage Confluent Schema Registry
6. Connect: MSK Connect integration
7. Serverless: MSK Serverless
8. IAM auth: IAM role-based authentication
9. Cross-region: Cross-region replication
10. CloudWatch integration: Metrics and monitoring

Commit: 'feat(aws-msk): add AWS MSK with cluster management, brokers, topics, producer/consumer, schema registry, MSK Connect, serverless, IAM auth, cross-region replication, CloudWatch'
"""

import json
import time
import uuid
import threading
import asyncio
import socket
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, Iterator, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging
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

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
    from kafka.errors import TopicAlreadyExistsError, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None
    KafkaConsumer = None
    KafkaAdminClient = None

try:
    from confluent_kafka import Producer, Consumer, AdminClient, avro
    from confluent_kafka.schema_registries import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
    from confluent_kafka.admin import NewTopic as ConfluentNewTopic
    from confluent_kafka import KafkaError as ConfluentKafkaError
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    Producer = None
    Consumer = None
    AdminClient = None
    SchemaRegistryClient = None


logger = logging.getLogger(__name__)


class MSKClusterType(Enum):
    """MSK cluster types."""
    PROVISIONED = "PROVISIONED"
    SERVERLESS = "SERVERLESS"


class MSKBrokerType(Enum):
    """MSK broker instance types."""
    KAFKA_M5_LARGE = "kafka.m5.large"
    KAFKA_M5_XLARGE = "kafka.m5.xlarge"
    KAFKA_M5_2XLARGE = "kafka.m5.2xlarge"
    KAFKA_M5_4XLARGE = "kafka.m5.4xlarge"
    KAFKA_M5_8XLARGE = "kafka.m5.8xlarge"
    KAFKA_M5_12XLARGE = "kafka.m5.12xlarge"
    KAFKA_M5_16XLARGE = "kafka.m5.16xlarge"
    KAFKA_M5_24XLARGE = "kafka.m5.24xlarge"
    KAFKA_KAFKA_M5_LARGE = "kafka.m5.large"
    KAFKA_KAFKA_M5_XLARGE = "kafka.m5.xlarge"
    KAFKA_KAFKA_M5_2XLARGE = "kafka.m5.2xlarge"
    KAFKA_T3_SMALL = "kafka.t3.small"
    KAFKA_T3_MEDIUM = "kafka.t3.medium"


class MSKStorageMode(Enum):
    """MSK storage modes."""
    LOCAL = "LOCAL"
    TIERED = "TIERED"


class MSKEncryptionMode(Enum):
    """MSK encryption modes."""
    TLS = "TLS"
    TLS_PLAINTEXT = "TLS_PLAINTEXT"
    PLAINTEXT = "PLAINTEXT"


class MSKAuthMode(Enum):
    """MSK authentication modes."""
    IAM = "IAM"
    SASL = "SASL"
    TLS = "TLS"
    TLS_PLAINTEXT = "TLS_PLAINTEXT"


class CompressionType(Enum):
    """Kafka compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class AcksMode(Enum):
    """Producer acknowledgement modes."""
    ALL = -1
    NONE = 0
    LEADER = 1


class OffsetResetStrategy(Enum):
    """Consumer offset reset strategies."""
    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"


class SchemaType(Enum):
    """Schema registry types."""
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


@dataclass
class MSKClusterConfig:
    """Configuration for an MSK cluster."""
    cluster_name: str
    kafka_version: str = "3.6.0"
    number_of_broker_nodes: int = 3
    broker_type: MSKBrokerType = MSKBrokerType.KAFKA_M5_LARGE
    cluster_type: MSKClusterType = MSKClusterType.PROVISIONED
    storage_mode: MSKStorageMode = MSKStorageMode.LOCAL
    encryption_mode: MSKEncryptionMode = MSKEncryptionMode.TLS
    auth_mode: MSKAuthMode = MSKAuthMode.IAM
    vpc_id: Optional[str] = None
    subnet_ids: List[str] = field(default_factory=list)
    security_groups: List[str] = field(default_factory=list)
    client_subnets: List[str] = field(default_factory=list)
    broker_az_distribution: str = "DEFAULT"
    storage_per_broker_gb: int = 100
    ebs_storage_info: Optional[Dict[str, Any]] = None
    prometheus_jmx_exporter: bool = False
    prometheus_node_exporter: bool = False
    firehose_delivery_stream: Optional[str] = None
    cloudwatch_logs_group: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    serverless_vpc_config: Optional[Dict[str, Any]] = None
    preloaded_secrets: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class MSKTopicConfig:
    """Configuration for an MSK Kafka topic."""
    name: str
    partitions: int = 3
    replication_factor: int = 3
    retention_ms: int = 604800000  # 7 days
    retention_bytes: int = -1  # -1 means no limit
    segment_ms: int = 604800000  # 7 days
    segment_bytes: int = 1073741824  # 1GB
    max_message_bytes: int = 1048576  # 1MB
    min_insync_replicas: int = 2
    cleanup_policy: str = "delete"  # delete or compact
    compression_type: CompressionType = CompressionType.NONE
    preallocate: bool = False
    index_interval_bytes: int = 4096
    delete_retention_ms: int = 86400000  # 1 day
    file_delete_delay_ms: int = 60000  # 1 minute

    def to_admin_config(self) -> Dict[str, str]:
        """Convert to Kafka admin client config."""
        return {
            "retention.ms": str(self.retention_ms),
            "retention.bytes": str(self.retention_bytes),
            "segment.ms": str(self.segment_ms),
            "segment.bytes": str(self.segment_bytes),
            "max.message.bytes": str(self.max_message_bytes),
            "min.insync.replicas": str(self.min_insync_replicas),
            "cleanup.policy": self.cleanup_policy,
            "compression.type": self.compression_type.value,
            "preallocate": "true" if self.preallocate else "false",
            "index.interval.bytes": str(self.index_interval_bytes),
            "delete.retention.ms": str(self.delete_retention_ms),
            "file.delete.delay.ms": str(self.file_delete_delay_ms),
        }


@dataclass
class MSKProducerConfig:
    """Configuration for MSK producer."""
    bootstrap_servers: str = ""
    client_id: str = "workflow-msk-producer"
    acks: Union[int, str] = "all"
    compression_type: CompressionType = CompressionType.NONE
    batch_size: int = 16384
    linger_ms: int = 0
    max_in_flight_requests_per_connection: int = 5
    retries: int = 3
    retry_backoff_ms: int = 100
    max_block_ms: int = 60000
    enable_idempotence: bool = True
    transaction_timeout_ms: int = 60000
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "AWS_MSK_IAM"
    schema_registry_url: Optional[str] = None
    avro_serializer_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MSKConsumerConfig:
    """Configuration for MSK consumer."""
    bootstrap_servers: str = ""
    group_id: str = "workflow-msk-consumer-group"
    client_id: str = "workflow-msk-consumer"
    auto_offset_reset: OffsetResetStrategy = OffsetResetStrategy.EARLIEST
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    heartbeat_interval_ms: int = 3000
    isolation_level: str = "read_uncommitted"
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "AWS_MSK_IAM"
    schema_registry_url: Optional[str] = None
    avro_deserializer_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MSKConnectorConfig:
    """Configuration for MSK Connect."""
    connector_name: str
    connector_class: str
    kafka_cluster_arn: str
    kafka_connect_version: str = "2.7.1"
    service_role_arn: str = ""
    worker_config_arn: Optional[str] = None
    log_bucket: Optional[str] = None
    vpc_config: Optional[Dict[str, Any]] = None
    plugins: List[Dict[str, str]] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    capacity: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SchemaRegistryConfig:
    """Configuration for Confluent Schema Registry."""
    url: str = "http://localhost:8081"
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    ca_location: Optional[str] = None
    cert_location: Optional[str] = None
    key_location: Optional[str] = None
    key_password: Optional[str] = None


class MSKIntegration:
    """
    AWS Managed Streaming for Apache Kafka (MSK) Integration.
    
    Provides comprehensive MSK cluster management, topic operations,
    producer/consumer capabilities, schema registry, MSK Connect,
    serverless support, IAM authentication, cross-region replication,
    and CloudWatch monitoring.
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize MSK integration.
        
        Args:
            region_name: AWS region name
            profile_name: AWS profile name for boto3 session
        """
        self.region_name = region_name
        self.profile_name = profile_name
        
        if BOTO3_AVAILABLE:
            session_kwargs = {"region_name": region_name}
            if profile_name:
                session_kwargs["profile_name"] = profile_name
            self.session = boto3.Session(**session_kwargs)
            self.msk_client = self.session.client("kafka")
            self.sts_client = self.session.client("sts")
            self.iam_client = self.session.client("iam")
            self.cloudwatch_client = self.session.client("cloudwatch")
            self.logs_client = self.session.client("logs")
            self.s3_client = self.session.client("s3")
            self.lambda_client = self.session.client("lambda")
        else:
            self.session = None
            self.msk_client = None
            self.sts_client = None
            self.iam_client = None
            self.cloudwatch_client = None
            self.logs_client = None
            self.s3_client = None
            self.lambda_client = None
        
        self._cluster_cache: Dict[str, Dict[str, Any]] = {}
        self._topic_cache: Dict[str, List[str]] = defaultdict(list)
        self._producer_instances: Dict[str, Any] = {}
        self._consumer_instances: Dict[str, Any] = {}
        self._schema_registry_clients: Dict[str, Any] = {}
        self._lock = threading.RLock()
    
    # =========================================================================
    # Cluster Management
    # =========================================================================
    
    def create_cluster(self, config: MSKClusterConfig) -> Dict[str, Any]:
        """
        Create a new MSK cluster.
        
        Args:
            config: MSK cluster configuration
            
        Returns:
            Dictionary with cluster creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            params = {
                "ClusterName": config.cluster_name,
                "KafkaVersion": config.kafka_version,
                "NumberOfBrokerNodes": config.number_of_broker_nodes,
                "BrokerNodeGroupInfo": {
                    "InstanceType": config.broker_type.value,
                    "BrokerAZDistribution": config.broker_az_distribution,
                },
            }
            
            if config.subnet_ids:
                params["BrokerNodeGroupInfo"]["ClientSubnets"] = config.subnet_ids
            else:
                params["BrokerNodeGroupInfo"]["ClientSubnets"] = ["subnet-1", "subnet-2", "subnet-3"]
            
            if config.security_groups:
                params["BrokerNodeGroupInfo"]["SecurityGroups"] = config.security_groups
            
            if config.ebs_storage_info:
                params["BrokerNodeGroupInfo"]["StorageInfo"] = {
                    "EbsStorageInfo": config.ebs_storage_info
                }
            else:
                params["BrokerNodeGroupInfo"]["StorageInfo"] = {
                    "EbsStorageInfo": {
                        "VolumeSize": config.storage_per_broker_gb
                    }
                }
            
            encryption_info = {"EncryptionAtRest": True, "EncryptionInTransit": {}}
            
            if config.encryption_mode == MSKEncryptionMode.TLS:
                encryption_info["EncryptionInTransit"] = {
                    "ClientBroker": "TLS",
                    "InCluster": True
                }
            elif config.encryption_mode == MSKEncryptionMode.TLS_PLAINTEXT:
                encryption_info["EncryptionInTransit"] = {
                    "ClientBroker": "TLS_PLAINTEXT",
                    "InCluster": True
                }
            elif config.encryption_mode == MSKEncryptionMode.PLAINTEXT:
                encryption_info["EncryptionInTransit"] = {
                    "ClientBroker": "PLAINTEXT",
                    "InCluster": False
                }
            
            params["EncryptionInfo"] = encryption_info
            
            client_auth = {}
            if config.auth_mode == MSKAuthMode.IAM:
                client_auth = {"Enabled": True, "Sasl": {"Iam": {"Enabled": True}}}
            elif config.auth_mode == MSKAuthMode.SASL:
                client_auth = {"Enabled": True, "Sasl": {"Scram": {"Enabled": True}}}
            elif config.auth_mode == MSKAuthMode.TLS:
                client_auth = {"Enabled": True, "Tls": {"Enabled": True}}
            
            if client_auth:
                params["ClientAuthentication"] = client_auth
            
            if config.cluster_type == MSKClusterType.PROVISIONED:
                params["BrokerNodeGroupInfo"]["ConnectivityInfo"] = {
                    "PublicAccess": {"Type": "DISABLED"}
                }
            elif config.cluster_type == MSKClusterType.SERVERLESS:
                params["ServerlessInfo"] = {
                    "VpcConfigs": config.serverless_vpc_config or [
                        {
                            "SubnetIds": config.subnet_ids or ["subnet-1", "subnet-2"],
                            "SecurityGroups": config.security_groups or []
                        }
                    ]
                }
            
            logging_config = {"CloudWatchLogs": {"Enabled": False}, "S3": {"Enabled": False}}
            if config.cloudwatch_logs_group:
                logging_config["CloudWatchLogs"] = {
                    "Enabled": True,
                    "LogGroup": config.cloudwatch_logs_group
                }
            params["LoggingInfo"] = {"BrokerLogs": logging_config}
            
            if config.prometheus_jmx_exporter or config.prometheus_node_exporter:
                params["PrometheusInfo"] = {}
                if config.prometheus_jmx_exporter:
                    params["PrometheusInfo"]["JmxExporter"] = {"EnabledInBroker": True}
                if config.prometheus_node_exporter:
                    params["PrometheusInfo"]["NodeExporter"] = {"EnabledInBroker": True}
            
            if config.tags:
                params["Tags"] = config.tags
            
            if config.preloaded_secrets:
                params["PreloadedSecrets"] = config.preloaded_secrets
            
            response = self.msk_client.create_cluster(**params)
            
            with self._lock:
                self._cluster_cache[config.cluster_name] = {
                    "arn": response["ClusterArn"],
                    "state": response["State"],
                    "created_at": datetime.now().isoformat()
                }
            
            logger.info(f"Created MSK cluster: {config.cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create MSK cluster: {e}")
            raise
    
    def describe_cluster(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get details of an MSK cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Cluster details
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            response = self.msk_client.describe_cluster(ClusterName=cluster_name)
            cluster_info = response.get("ClusterInfo", {})
            
            with self._lock:
                self._cluster_cache[cluster_name] = {
                    "arn": cluster_info.get("ClusterArn"),
                    "state": cluster_info.get("State"),
                    "broker_nodes": cluster_info.get("NumberOfBrokerNodes"),
                    "kafka_version": cluster_info.get("KafkaVersion"),
                    "cluster_type": cluster_info.get("ClusterType"),
                }
            
            return cluster_info
            
        except ClientError as e:
            logger.error(f"Failed to describe cluster {cluster_name}: {e}")
            raise
    
    def list_clusters(self, cluster_type_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all MSK clusters.
        
        Args:
            cluster_type_filter: Filter by cluster type (PROVISIONED/SERVERLESS)
            
        Returns:
            List of cluster summaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            params = {}
            if cluster_type_filter:
                params["ClusterTypeFilter"] = cluster_type_filter
            
            response = self.msk_client.list_clusters(**params)
            clusters = response.get("ClusterInfoList", [])
            
            with self._lock:
                for cluster in clusters:
                    self._cluster_cache[cluster["ClusterName"]] = {
                        "arn": cluster["ClusterArn"],
                        "state": cluster["State"],
                        "cluster_type": cluster.get("ClusterType"),
                    }
            
            return clusters
            
        except ClientError as e:
            logger.error(f"Failed to list clusters: {e}")
            raise
    
    def delete_cluster(self, cluster_name: str) -> Dict[str, Any]:
        """
        Delete an MSK cluster.
        
        Args:
            cluster_name: Name of the cluster to delete
            
        Returns:
            Deletion response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            response = self.msk_client.delete_cluster(ClusterName=cluster_name)
            
            with self._lock:
                self._cluster_cache.pop(cluster_name, None)
            
            logger.info(f"Deleted MSK cluster: {cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete cluster {cluster_name}: {e}")
            raise
    
    def update_cluster(self, cluster_name: str, configuration_arn: Optional[str] = None) -> Dict[str, Any]:
        """
        Update cluster configuration.
        
        Args:
            cluster_name: Name of the cluster
            configuration_arn: ARN of the configuration to apply
            
        Returns:
            Update response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            params = {"ClusterName": cluster_name}
            if configuration_arn:
                params["ConfigurationInfo"] = {
                    "Arn": configuration_arn,
                    "Revision": 1
                }
            
            response = self.msk_client.update_cluster(**params)
            logger.info(f"Updated MSK cluster: {cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update cluster {cluster_name}: {e}")
            raise
    
    def get_cluster_bootstrap_brokers(self, cluster_name: str, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Dict[str, Any]:
        """
        Get bootstrap brokers for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            auth_mode: Authentication mode for brokers
            
        Returns:
            Bootstrap broker information
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK cluster management")
        
        try:
            if auth_mode == MSKAuthMode.IAM:
                response = self.msk_client.get_bootstrap_brokers(ClusterName=cluster_name)
            else:
                response = self.msk_client.get_bootstrap_brokers(
                    ClusterName=cluster_name,
                    EndpointType="BROKER"
                )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get bootstrap brokers for {cluster_name}: {e}")
            raise
    
    # =========================================================================
    # Broker Management
    # =========================================================================
    
    def get_broker_nodes(self, cluster_name: str) -> List[Dict[str, Any]]:
        """
        Get broker node information for a cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of broker nodes
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK broker management")
        
        try:
            cluster_info = self.describe_cluster(cluster_name)
            cluster_arn = cluster_info["ClusterArn"]
            
            response = self.msk_client.list_nodes(ClusterArn=cluster_arn)
            nodes = response.get("NodeInfoList", [])
            
            return [
                {
                    "node_arn": node["NodeArn"],
                    "instance_type": node.get("BrokerNodeData", {}).get("InstanceType"),
                    "availability_zone": node.get("BrokerNodeData", {}).get("AvailabilityZone"),
                    "node_status": node.get("NodeStatus"),
                    "attached_eni_id": node.get("BrokerNodeData", {}).get("AttachedENIId"),
                }
                for node in nodes
            ]
            
        except ClientError as e:
            logger.error(f"Failed to get broker nodes for {cluster_name}: {e}")
            raise
    
    def modify_broker_count(self, cluster_name: str, target_broker_count: int) -> Dict[str, Any]:
        """
        Modify the number of brokers in a cluster.
        
        Args:
            cluster_name: Name of the cluster
            target_broker_count: Target number of brokers
            
        Returns:
            Operation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK broker management")
        
        try:
            response = self.msk_client.update_broker_count(
                ClusterName=cluster_name,
                TargetNumberOfBrokerNodes=target_broker_count
            )
            logger.info(f"Modified broker count for {cluster_name} to {target_broker_count}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to modify broker count for {cluster_name}: {e}")
            raise
    
    def update_broker_storage(self, cluster_name: str, target_storage_vpm: int) -> Dict[str, Any]:
        """
        Update broker storage.
        
        Args:
            cluster_name: Name of the cluster
            target_storage_vpm: Target storage volume per broker in GiB
            
        Returns:
            Operation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK broker management")
        
        try:
            response = self.msk_client.update_broker_storage(
                ClusterName=cluster_name,
                TargetBrokerVolumeSize=target_storage_vpm
            )
            logger.info(f"Updated broker storage for {cluster_name} to {target_storage_vpm} GiB")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update broker storage for {cluster_name}: {e}")
            raise
    
    def reboot_broker(self, cluster_name: str, broker_ids: List[str]) -> Dict[str, Any]:
        """
        Reboot MSK brokers.
        
        Args:
            cluster_name: Name of the cluster
            broker_ids: List of broker IDs to reboot
            
        Returns:
            Operation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK broker management")
        
        try:
            response = self.msk_client.reboot_broker(
                ClusterName=cluster_name,
                BrokerIds=broker_ids
            )
            logger.info(f"Rebooted brokers {broker_ids} in {cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to reboot brokers in {cluster_name}: {e}")
            raise
    
    # =========================================================================
    # Topic Management
    # =========================================================================
    
    def _get_bootstrap_servers(self, cluster_name: str, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> str:
        """Get bootstrap servers string for a cluster."""
        brokers = self.get_cluster_bootstrap_brokers(cluster_name, auth_mode)
        bootstrap_servers = brokers.get("BootstrapBrokerStringSaslIam", brokers.get("BootstrapBrokerString", ""))
        return bootstrap_servers
    
    def _create_kafka_admin_client(self, bootstrap_servers: str, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Any:
        """Create Kafka admin client with appropriate security settings."""
        if KAFKA_AVAILABLE:
            config = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "workflow-msk-admin"
            }
            
            if auth_mode == MSKAuthMode.IAM:
                config["security.protocol"] = "SASL_SSL"
                config["sasl.mechanism"] = "AWS_MSK_IAM"
                config["sasl.jaas.config"] = "software.amazon.msk.auth.iam.IAMLoginModule required;"
                config["sasl.client.callback.handler.class"] = "software.amazon.msk.auth.iam.internals.MSKCredentialProvider"
            elif auth_mode == MSKAuthMode.SASL:
                config["security.protocol"] = "SASL_SSL"
                config["sasl.mechanism"] = "SCRAM-SHA-512"
            
            return KafkaAdminClient(**config)
        elif CONFLUENT_AVAILABLE:
            config = {
                "bootstrap.servers": bootstrap_servers,
            }
            
            if auth_mode == MSKAuthMode.IAM:
                config["security.protocol"] = "SASL_SSL"
                config["sasl.mechanism"] = "AWS_MSK_IAM"
            
            return AdminClient(config)
        
        raise ImportError("kafka-python or confluent-kafka is required for topic management")
    
    def create_topic(self, cluster_name: str, config: MSKTopicConfig, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Dict[str, Any]:
        """
        Create a Kafka topic in an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            config: Topic configuration
            auth_mode: Authentication mode
            
        Returns:
            Topic creation response
        """
        bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        try:
            if KAFKA_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                new_topic = NewTopic(
                    name=config.name,
                    num_partitions=config.partitions,
                    replication_factor=config.replication_factor,
                    topic_configs=config.to_admin_config()
                )
                
                admin_client.create_topics([new_topic], validate_only=False)
                
                with self._lock:
                    self._topic_cache[cluster_name].append(config.name)
                
                logger.info(f"Created topic {config.name} in cluster {cluster_name}")
                return {"topic_name": config.name, "status": "created"}
                
            elif CONFLUENT_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                new_topic = ConfluentNewTopic(
                    name=config.name,
                    num_partitions=config.partitions,
                    replication_factor=config.replication_factor,
                    config=config.to_admin_config()
                )
                
                admin_client.create_topics([new_topic], validate_only=False)
                
                with self._lock:
                    self._topic_cache[cluster_name].append(config.name)
                
                logger.info(f"Created topic {config.name} in cluster {cluster_name}")
                return {"topic_name": config.name, "status": "created"}
            
        except Exception as e:
            logger.error(f"Failed to create topic {config.name}: {e}")
            raise
    
    def list_topics(self, cluster_name: str, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> List[str]:
        """
        List all topics in an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            auth_mode: Authentication mode
            
        Returns:
            List of topic names
        """
        bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        try:
            if KAFKA_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                topics = admin_client.list_topics(timeout=10)
                topic_names = list(topics.topics.keys())
                
                with self._lock:
                    self._topic_cache[cluster_name] = topic_names
                
                return topic_names
                
            elif CONFLUENT_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                cluster_metadata = admin_client.list_topics(timeout=10)
                topic_names = list(cluster_metadata.topics.keys())
                
                with self._lock:
                    self._topic_cache[cluster_name] = topic_names
                
                return topic_names
            
        except Exception as e:
            logger.error(f"Failed to list topics in {cluster_name}: {e}")
            raise
    
    def delete_topic(self, cluster_name: str, topic_name: str, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Dict[str, Any]:
        """
        Delete a topic from an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            topic_name: Name of the topic to delete
            auth_mode: Authentication mode
            
        Returns:
            Deletion response
        """
        bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        try:
            if KAFKA_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                config_resource = ConfigResource(
                    resource_type=ConfigResourceType.TOPIC,
                    name=topic_name
                )
                admin_client.delete_configs([config_resource])
                
                with self._lock:
                    if topic_name in self._topic_cache[cluster_name]:
                        self._topic_cache[cluster_name].remove(topic_name)
                
                logger.info(f"Deleted topic {topic_name} from cluster {cluster_name}")
                return {"topic_name": topic_name, "status": "deleted"}
                
            elif CONFLUENT_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                from confluent_kafka.admin import ConfigResource as ConfluentConfigResource, RESOURCE_TOPIC
                
                config_resource = ConfluentConfigResource(RESOURCE_TOPIC, topic_name)
                admin_client.delete_configs([config_resource])
                
                with self._lock:
                    if topic_name in self._topic_cache[cluster_name]:
                        self._topic_cache[cluster_name].remove(topic_name)
                
                logger.info(f"Deleted topic {topic_name} from cluster {cluster_name}")
                return {"topic_name": topic_name, "status": "deleted"}
            
        except Exception as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")
            raise
    
    def update_topic_config(self, cluster_name: str, topic_name: str, config_updates: Dict[str, str], auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Dict[str, Any]:
        """
        Update topic configuration.
        
        Args:
            cluster_name: Name of the MSK cluster
            topic_name: Name of the topic
            config_updates: Dictionary of config updates
            auth_mode: Authentication mode
            
        Returns:
            Update response
        """
        bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        try:
            if KAFKA_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                config_resource = ConfigResource(
                    resource_type=ConfigResourceType.TOPIC,
                    name=topic_name,
                    configs=config_updates
                )
                
                admin_client.alter_configs([config_resource])
                logger.info(f"Updated topic config for {topic_name}")
                return {"topic_name": topic_name, "status": "updated"}
                
            elif CONFLUENT_AVAILABLE:
                admin_client = self._create_kafka_admin_client(bootstrap_servers, auth_mode)
                
                from confluent_kafka.admin import ConfigResource as ConfluentConfigResource, RESOURCE_TOPIC
                
                config_resource = ConfluentConfigResource(
                    RESOURCE_TOPIC,
                    topic_name,
                    configs=config_updates
                )
                
                admin_client.alter_configs([config_resource])
                logger.info(f"Updated topic config for {topic_name}")
                return {"topic_name": topic_name, "status": "updated"}
            
        except Exception as e:
            logger.error(f"Failed to update topic config for {topic_name}: {e}")
            raise
    
    # =========================================================================
    # Producer/Consumer
    # =========================================================================
    
    def create_producer(self, cluster_name: str, config: Optional[MSKProducerConfig] = None, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Any:
        """
        Create a managed Kafka producer for an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            config: Producer configuration (uses defaults if not provided)
            auth_mode: Authentication mode
            
        Returns:
            Kafka producer instance
        """
        if config is None:
            config = MSKProducerConfig()
        
        if not config.bootstrap_servers:
            config.bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        producer_key = f"{cluster_name}_{id(config)}"
        
        with self._lock:
            if producer_key in self._producer_instances:
                return self._producer_instances[producer_key]
        
        try:
            if KAFKA_AVAILABLE:
                producer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "client.id": config.client_id,
                    "acks": config.acks if isinstance(config.acks, int) else -1,
                    "compression.type": config.compression_type.value,
                    "batch.size": config.batch_size,
                    "linger.ms": config.linger_ms,
                    "max.in.flight.requests.per.connection": config.max_in_flight_requests_per_connection,
                    "retries": config.retries,
                    "retry.backoff.ms": config.retry_backoff_ms,
                    "max.block.ms": config.max_block_ms,
                    "enable.idempotence": config.enable_idempotence,
                    "transaction.timeout.ms": config.transaction_timeout_ms,
                }
                
                if auth_mode == MSKAuthMode.IAM:
                    producer_config["security.protocol"] = "SASL_SSL"
                    producer_config["sasl.mechanism"] = "AWS_MSK_IAM"
                    producer_config["sasl.jaas.config"] = "software.amazon.msk.auth.iam.IAMLoginModule required;"
                    producer_config["sasl.client.callback.handler.class"] = "software.amazon.msk.auth.iam.internals.MSKCredentialProvider"
                
                producer = KafkaProducer(**producer_config)
                
            elif CONFLUENT_AVAILABLE:
                producer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "client.id": config.client_id,
                    "acks": config.acks if isinstance(config.acks, int) else -1,
                    "compression.type": config.compression_type.value,
                    "batch.size": config.batch_size,
                    "linger.ms": config.linger_ms,
                    "max.in.flight.requests.per.connection": config.max_in_flight_requests_per_connection,
                    "retries": config.retries,
                    "retry.backoff.ms": config.retry_backoff_ms,
                    "max.block.ms": config.max_block_ms,
                    "enable.idempotence": config.enable_idempotence,
                    "transaction.timeout.ms": config.transaction_timeout_ms,
                }
                
                if auth_mode == MSKAuthMode.IAM:
                    producer_config["security.protocol"] = "SASL_SSL"
                    producer_config["sasl.mechanism"] = "AWS_MSK_IAM"
                
                producer = Producer(**producer_config)
                
            else:
                raise ImportError("kafka-python or confluent-kafka is required for producer")
            
            with self._lock:
                self._producer_instances[producer_key] = producer
            
            logger.info(f"Created producer for cluster {cluster_name}")
            return producer
            
        except Exception as e:
            logger.error(f"Failed to create producer for {cluster_name}: {e}")
            raise
    
    def produce(
        self,
        cluster_name: str,
        topic: str,
        value: Union[str, bytes, Dict],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[List[tuple]] = None,
        partition: Optional[int] = None,
        config: Optional[MSKProducerConfig] = None,
        auth_mode: MSKAuthMode = MSKAuthMode.IAM,
        sync: bool = True
    ) -> Any:
        """
        Produce a message to an MSK topic.
        
        Args:
            cluster_name: Name of the MSK cluster
            topic: Topic name
            value: Message value (string, bytes, or dict for JSON)
            key: Optional message key
            headers: Optional message headers
            partition: Optional partition number
            config: Producer configuration
            auth_mode: Authentication mode
            sync: If True, wait for acknowledgment
            
        Returns:
            Record metadata (for sync) or None (for async)
        """
        producer = self.create_producer(cluster_name, config, auth_mode)
        
        if isinstance(value, Dict):
            value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            value = value.encode("utf-8")
        
        if isinstance(key, str):
            key = key.encode("utf-8")
        
        try:
            if KAFKA_AVAILABLE:
                future = producer.send(
                    topic,
                    value=value,
                    key=key,
                    headers=headers,
                    partition=partition
                )
                
                if sync:
                    metadata = future.get(timeout=10)
                    return {
                        "topic": metadata.topic,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                        "timestamp": metadata.timestamp
                    }
                    
            elif CONFLUENT_AVAILABLE:
                delivery_result = []
                
                def on_delivery(err, msg):
                    if err:
                        logger.error(f"Delivery failed: {err}")
                    else:
                        delivery_result.append({
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "timestamp": msg.timestamp()
                        })
                
                producer.produce(
                    topic,
                    value=value,
                    key=key,
                    headers=headers,
                    partition=partition,
                    callback=on_delivery
                )
                producer.poll(0)
                
                if sync:
                    timeout = time.time() + 10
                    while not delivery_result and time.time() < timeout:
                        producer.poll(0.1)
                    
                    if delivery_result:
                        return delivery_result[0]
                    raise TimeoutError("Message delivery timed out")
            
        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {e}")
            raise
    
    def create_consumer(self, cluster_name: str, config: Optional[MSKConsumerConfig] = None, auth_mode: MSKAuthMode = MSKAuthMode.IAM) -> Any:
        """
        Create a managed Kafka consumer for an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            config: Consumer configuration (uses defaults if not provided)
            auth_mode: Authentication mode
            
        Returns:
            Kafka consumer instance
        """
        if config is None:
            config = MSKConsumerConfig()
        
        if not config.bootstrap_servers:
            config.bootstrap_servers = self._get_bootstrap_servers(cluster_name, auth_mode)
        
        consumer_key = f"{cluster_name}_{config.group_id}"
        
        with self._lock:
            if consumer_key in self._consumer_instances:
                return self._consumer_instances[consumer_key]
        
        try:
            if KAFKA_AVAILABLE:
                consumer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "group.id": config.group_id,
                    "client.id": config.client_id,
                    "auto.offset.reset": config.auto_offset_reset.value,
                    "enable.auto.commit": config.enable_auto_commit,
                    "auto.commit.interval.ms": config.auto_commit_interval_ms,
                    "session.timeout.ms": config.session_timeout_ms,
                    "max.poll.records": config.max_poll_records,
                    "max.poll.interval.ms": config.max_poll_interval_ms,
                    "fetch.min.bytes": config.fetch_min_bytes,
                    "fetch.max.wait.ms": config.fetch_max_wait_ms,
                    "heartbeat.interval.ms": config.heartbeat_interval_ms,
                    "isolation.level": config.isolation_level,
                }
                
                if auth_mode == MSKAuthMode.IAM:
                    consumer_config["security.protocol"] = "SASL_SSL"
                    consumer_config["sasl.mechanism"] = "AWS_MSK_IAM"
                    consumer_config["sasl.jaas.config"] = "software.amazon.msk.auth.iam.IAMLoginModule required;"
                    consumer_config["sasl.client.callback.handler.class"] = "software.amazon.msk.auth.iam.internals.MSKCredentialProvider"
                
                consumer = KafkaConsumer(**consumer_config)
                
            elif CONFLUENT_AVAILABLE:
                consumer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "group.id": config.group_id,
                    "client.id": config.client_id,
                    "auto.offset.reset": config.auto_offset_reset.value,
                    "enable.auto.commit": config.enable_auto_commit,
                    "auto.commit.interval.ms": config.auto_commit_interval_ms,
                    "session.timeout.ms": config.session_timeout_ms,
                    "max.poll.records": config.max_poll_records,
                    "max.poll.interval.ms": config.max_poll_interval_ms,
                    "fetch.min.bytes": config.fetch_min_bytes,
                    "fetch.max.wait.ms": config.fetch_max_wait_ms,
                    "heartbeat.interval.ms": config.heartbeat_interval_ms,
                    "isolation.level": config.isolation_level,
                }
                
                if auth_mode == MSKAuthMode.IAM:
                    consumer_config["security.protocol"] = "SASL_SSL"
                    consumer_config["sasl.mechanism"] = "AWS_MSK_IAM"
                
                consumer = Consumer(**consumer_config)
                
            else:
                raise ImportError("kafka-python or confluent-kafka is required for consumer")
            
            consumer.subscribe([topic for topic in self.list_topics(cluster_name, auth_mode) if topic.startswith("__") is False])
            
            with self._lock:
                self._consumer_instances[consumer_key] = consumer
            
            logger.info(f"Created consumer for cluster {cluster_name} with group {config.group_id}")
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to create consumer for {cluster_name}: {e}")
            raise
    
    def consume(
        self,
        cluster_name: str,
        topics: List[str],
        config: Optional[MSKConsumerConfig] = None,
        auth_mode: MSKAuthMode = MSKAuthMode.IAM,
        timeout_seconds: float = 1.0,
        max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Consume messages from MSK topics.
        
        Args:
            cluster_name: Name of the MSK cluster
            topics: List of topic names to subscribe to
            config: Consumer configuration
            auth_mode: Authentication mode
            timeout_seconds: Poll timeout
            max_records: Maximum records to return
            
        Returns:
            List of consumed messages
        """
        consumer = self.create_consumer(cluster_name, config, auth_mode)
        
        if KAFKA_AVAILABLE:
            try:
                messages = []
                for message in consumer:
                    messages.append({
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "value": message.value.decode("utf-8") if message.value else None,
                        "timestamp": message.timestamp,
                        "headers": dict(message.headers) if message.headers else {}
                    })
                    
                    if max_records and len(messages) >= max_records:
                        break
                
                return messages
                
            except Exception as e:
                logger.error(f"Failed to consume messages: {e}")
                raise
                
        elif CONFLUENT_AVAILABLE:
            try:
                messages = []
                start_time = time.time()
                
                while len(messages) < (max_records or 100):
                    msg = consumer.poll(timeout=timeout_seconds)
                    
                    if msg is None:
                        if time.time() - start_time > timeout_seconds * 10:
                            break
                        continue
                    
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    messages.append({
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "value": msg.value().decode("utf-8") if msg.value() else None,
                        "timestamp": msg.timestamp(),
                        "headers": dict(msg.headers()) if msg.headers() else {}
                    })
                
                return messages
                
            except Exception as e:
                logger.error(f"Failed to consume messages: {e}")
                raise
    
    def close_consumers(self, cluster_name: Optional[str] = None):
        """
        Close consumer instances.
        
        Args:
            cluster_name: Close consumers for specific cluster, or all if None
        """
        with self._lock:
            if cluster_name:
                keys_to_close = [k for k in self._consumer_instances.keys() if k.startswith(cluster_name)]
            else:
                keys_to_close = list(self._consumer_instances.keys())
            
            for key in keys_to_close:
                try:
                    self._consumer_instances[key].close()
                except Exception as e:
                    logger.warning(f"Error closing consumer {key}: {e}")
                del self._consumer_instances[key]
    
    def close_producers(self, cluster_name: Optional[str] = None):
        """
        Close producer instances.
        
        Args:
            cluster_name: Close producers for specific cluster, or all if None
        """
        with self._lock:
            if cluster_name:
                keys_to_close = [k for k in self._producer_instances.keys() if k.startswith(cluster_name)]
            else:
                keys_to_close = list(self._producer_instances.keys())
            
            for key in keys_to_close:
                try:
                    self._producer_instances[key].flush()
                except Exception as e:
                    logger.warning(f"Error closing producer {key}: {e}")
                del self._producer_instances[key]
    
    # =========================================================================
    # Schema Registry
    # =========================================================================
    
    def create_schema_registry_client(self, config: SchemaRegistryConfig) -> Any:
        """
        Create a Confluent Schema Registry client.
        
        Args:
            config: Schema registry configuration
            
        Returns:
            Schema registry client
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        client_key = hashlib.md5(config.url.encode()).hexdigest()
        
        with self._lock:
            if client_key in self._schema_registry_clients:
                return self._schema_registry_clients[client_key]
        
        try:
            schema_registry_config = {
                "url": config.url,
            }
            
            if config.api_key and config.api_secret:
                schema_registry_config["basic.auth.user.info"] = f"{config.api_key}:{config.api_secret}"
            
            if config.ca_location:
                schema_registry_config["ssl.ca.location"] = config.ca_location
            if config.cert_location:
                schema_registry_config["ssl.certificate.location"] = config.cert_location
            if config.key_location:
                schema_registry_config["ssl.key.location"] = config.key_location
            if config.key_password:
                schema_registry_config["ssl.key.password"] = config.key_password
            
            client = SchemaRegistryClient(schema_registry_config)
            
            with self._lock:
                self._schema_registry_clients[client_key] = client
            
            logger.info(f"Created schema registry client for {config.url}")
            return client
            
        except Exception as e:
            logger.error(f"Failed to create schema registry client: {e}")
            raise
    
    def register_schema(
        self,
        schema_registry_url: str,
        subject_name: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> int:
        """
        Register a schema with Confluent Schema Registry.
        
        Args:
            schema_registry_url: URL of the schema registry
            subject_name: Subject name for the schema
            schema_str: Schema string (Avro, JSON, or Protobuf)
            schema_type: Type of schema
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
            
        Returns:
            Schema ID
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        try:
            config = SchemaRegistryConfig(
                url=schema_registry_url,
                api_key=api_key,
                api_secret=api_secret
            )
            client = self.create_schema_registry_client(config)
            
            schema_def = {
                "schema": schema_str,
                "schemaType": schema_type.value
            }
            
            schema_id = client.register(subject_name, schema_def)
            
            logger.info(f"Registered schema for subject {subject_name}, ID: {schema_id}")
            return schema_id
            
        except Exception as e:
            logger.error(f"Failed to register schema for {subject_name}: {e}")
            raise
    
    def get_schema(
        self,
        schema_registry_url: str,
        schema_id: int,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a schema by ID.
        
        Args:
            schema_registry_url: URL of the schema registry
            schema_id: Schema ID
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
            
        Returns:
            Schema details
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        try:
            config = SchemaRegistryConfig(
                url=schema_registry_url,
                api_key=api_key,
                api_secret=api_secret
            )
            client = self.create_schema_registry_client(config)
            
            schema = client.get_schema(schema_id)
            
            return {
                "schema_id": schema.schema_id,
                "schema_str": schema.schema_str,
                "schema_type": schema.schema_type,
                "references": schema.references
            }
            
        except Exception as e:
            logger.error(f"Failed to get schema {schema_id}: {e}")
            raise
    
    def get_latest_schema(
        self,
        schema_registry_url: str,
        subject_name: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the latest schema for a subject.
        
        Args:
            schema_registry_url: URL of the schema registry
            subject_name: Subject name
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
            
        Returns:
            Schema details
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        try:
            config = SchemaRegistryConfig(
                url=schema_registry_url,
                api_key=api_key,
                api_secret=api_secret
            )
            client = self.create_schema_registry_client(config)
            
            subject_versions = client.get_subject_versions(subject_name)
            latest_version = max(subject_versions)
            
            schema_version = client.get_version(subject_name, latest_version)
            
            return {
                "subject": subject_name,
                "version": latest_version,
                "schema_id": schema_version.schema_id,
                "schema_str": schema_version.schema_str,
                "schema_type": schema_version.schema_type
            }
            
        except Exception as e:
            logger.error(f"Failed to get latest schema for {subject_name}: {e}")
            raise
    
    def list_subjects(
        self,
        schema_registry_url: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> List[str]:
        """
        List all subjects in schema registry.
        
        Args:
            schema_registry_url: URL of the schema registry
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
            
        Returns:
            List of subject names
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        try:
            config = SchemaRegistryConfig(
                url=schema_registry_url,
                api_key=api_key,
                api_secret=api_secret
            )
            client = self.create_schema_registry_client(config)
            
            return client.get_subjects()
            
        except Exception as e:
            logger.error(f"Failed to list subjects: {e}")
            raise
    
    def delete_schema(
        self,
        schema_registry_url: str,
        subject_name: str,
        schema_version: str = "all",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a schema or schema version.
        
        Args:
            schema_registry_url: URL of the schema registry
            subject_name: Subject name
            schema_version: Version to delete, or "all"
            api_key: Optional API key for authentication
            api_secret: Optional API secret for authentication
            
        Returns:
            Deletion response
        """
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is required for schema registry")
        
        try:
            config = SchemaRegistryConfig(
                url=schema_registry_url,
                api_key=api_key,
                api_secret=api_secret
            )
            client = self.create_schema_registry_client(config)
            
            if schema_version == "all":
                result = client.delete_subject(subject_name)
            else:
                result = client.delete_version(subject_name, int(schema_version))
            
            logger.info(f"Deleted schema for {subject_name}, version {schema_version}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to delete schema for {subject_name}: {e}")
            raise
    
    # =========================================================================
    # MSK Connect
    # =========================================================================
    
    def create_connector(self, config: MSKConnectorConfig) -> Dict[str, Any]:
        """
        Create an MSK Connect connector.
        
        Args:
            config: Connector configuration
            
        Returns:
            Connector creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Connect")
        
        try:
            params = {
                "ConnectorName": config.connector_name,
                "KafkaCluster": {
                    "ApacheKafkaCluster": {
                        "BootstrapServers": config.kafka_cluster_arn,
                        "Vpc": {
                            "SubnetIds": config.vpc_config.get("subnet_ids", []) if config.vpc_config else [],
                            "SecurityGroups": config.vpc_config.get("security_groups", []) if config.vpc_config else []
                        }
                    }
                },
                "KafkaClusterClientAuthentication": {
                    "AuthenticationType": "IAM" if config.vpc_config else "NONE"
                },
                "KafkaClusterEncryptionInTransit": {
                    "EncryptionType": "TLS" if config.vpc_config else "PLAINTEXT"
                },
                "KafkaConnectVersion": config.kafka_connect_version,
                "Plugins": [
                    {
                        "CustomPlugin": {
                            "Arn": plugin["arn"],
                            "Revision": plugin.get("revision", 1)
                        }
                    }
                    for plugin in config.plugins
                ],
                "ServiceExecutionRoleArn": config.service_role_arn,
            }
            
            if config.worker_config_arn:
                params["WorkerConfiguration"] = {
                    "WorkerConfigurationArn": config.worker_config_arn,
                    "Revision": 1
                }
            
            if config.capacity:
                params["Capacity"] = config.capacity
            else:
                params["Capacity"] = {
                    "Autoscaling": {
                        "MaxWorkerCount": 5,
                        "MinWorkerCount": 1,
                        "McuCount": 2,
                        "ScaleInPolicy": {
                            "CpuUtilizationPercentage": 20
                        },
                        "ScaleOutPolicy": {
                            "CpuUtilizationPercentage": 80
                        }
                    }
                }
            
            if config.log_bucket:
                params["LogDelivery"] = {
                    "S3": {
                        "Enabled": True,
                        "Bucket": config.log_bucket
                    }
                }
            
            if config.config:
                params["ConnectorConfiguration"] = config.config
            
            if config.tags:
                params["Tags"] = config.tags
            
            response = self.msk_client.create_connector(**params)
            
            logger.info(f"Created MSK Connect connector: {config.connector_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create connector {config.connector_name}: {e}")
            raise
    
    def describe_connector(self, connector_name: str) -> Dict[str, Any]:
        """
        Describe an MSK Connect connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Connector details
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Connect")
        
        try:
            response = self.msk_client.describe_connector(ConnectorName=connector_name)
            return response
            
        except ClientError as e:
            logger.error(f"Failed to describe connector {connector_name}: {e}")
            raise
    
    def list_connectors(self) -> List[Dict[str, Any]]:
        """
        List all MSK Connect connectors.
        
        Returns:
            List of connector summaries
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Connect")
        
        try:
            response = self.msk_client.list_connectors()
            return response.get("Connectors", [])
            
        except ClientError as e:
            logger.error(f"Failed to list connectors: {e}")
            raise
    
    def update_connector(self, connector_name: str, capacity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an MSK Connect connector's capacity.
        
        Args:
            connector_name: Name of the connector
            capacity: New capacity configuration
            
        Returns:
            Update response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Connect")
        
        try:
            response = self.msk_client.update_connector(
                ConnectorName=connector_name,
                Capacity=capacity
            )
            
            logger.info(f"Updated connector {connector_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update connector {connector_name}: {e}")
            raise
    
    def delete_connector(self, connector_name: str) -> Dict[str, Any]:
        """
        Delete an MSK Connect connector.
        
        Args:
            connector_name: Name of the connector to delete
            
        Returns:
            Deletion response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Connect")
        
        try:
            response = self.msk_client.delete_connector(ConnectorName=connector_name)
            
            logger.info(f"Deleted connector {connector_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to delete connector {connector_name}: {e}")
            raise
    
    # =========================================================================
    # MSK Serverless
    # =========================================================================
    
    def create_serverless_cluster(
        self,
        cluster_name: str,
        vpc_config: Dict[str, List[str]],
        client_auth: Dict[str, Any] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create an MSK Serverless cluster.
        
        Args:
            cluster_name: Name of the cluster
            vpc_config: VPC configuration with subnet_ids and security_groups
            client_auth: Client authentication configuration
            tags: Optional tags
            
        Returns:
            Cluster creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Serverless")
        
        try:
            params = {
                "ClusterName": cluster_name,
                "Serverless": {
                    "VpcConfigs": [
                        {
                            "SubnetIds": vpc_config.get("subnet_ids", []),
                            "SecurityGroups": vpc_config.get("security_groups", [])
                        }
                    ]
                },
                "ClientAuthentication": client_auth or {"Sasl": {"Iam": {"Enabled": True}}},
            }
            
            if tags:
                params["Tags"] = tags
            
            response = self.msk_client.create_cluster(**params)
            
            with self._lock:
                self._cluster_cache[cluster_name] = {
                    "arn": response["ClusterArn"],
                    "state": response["State"],
                    "cluster_type": "SERVERLESS",
                    "created_at": datetime.now().isoformat()
                }
            
            logger.info(f"Created MSK Serverless cluster: {cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create serverless cluster {cluster_name}: {e}")
            raise
    
    def get_serverless_cluster_vpc_connection(
        self,
        cluster_name: str,
        vpc_connection_arn: str
    ) -> Dict[str, Any]:
        """
        Get VPC connection info for serverless cluster.
        
        Args:
            cluster_name: Name of the serverless cluster
            vpc_connection_arn: VPC connection ARN
            
        Returns:
            VPC connection details
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK Serverless")
        
        try:
            response = self.msk_client.get_vpc_connection(
                ClusterName=cluster_name,
                VpcConnectionArn=vpc_connection_arn
            )
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get VPC connection: {e}")
            raise
    
    # =========================================================================
    # IAM Authentication
    # =========================================================================
    
    def create_iam_role_for_msk(self, cluster_name: str) -> Dict[str, Any]:
        """
        Create an IAM role for MSK cluster access.
        
        Args:
            cluster_name: Name of the MSK cluster
            
        Returns:
            Role creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for IAM authentication")
        
        try:
            role_name = f"MSKAccessRole-{cluster_name}"
            
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "kafka.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            role = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                Description=f"Role for MSK cluster {cluster_name} access"
            )
            
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:Connect",
                            "kafka-cluster:*Topic*",
                            "kafka-cluster:*Group*",
                            "kafka-cluster:ReadData",
                            "kafka-cluster:WriteData"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=f"MSKAccessPolicy-{cluster_name}",
                PolicyDocument=json.dumps(policy)
            )
            
            logger.info(f"Created IAM role for MSK: {role_name}")
            return role
            
        except ClientError as e:
            logger.error(f"Failed to create IAM role for MSK: {e}")
            raise
    
    def get_iam_auth_token(self, cluster_arn: str, region: Optional[str] = None) -> str:
        """
        Get IAM authentication token for MSK.
        
        Args:
            cluster_arn: MSK cluster ARN
            region: AWS region
            
        Returns:
            IAM auth token
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for IAM authentication")
        
        try:
            sts_client = self.sts_client
            
            response = sts_client.assume_role(
                RoleArn=f"arn:aws:iam::123456789012:role/MSKAccessRole-temp",
                RoleSessionName="MSKAccessSession"
            )
            
            credentials = response["Credentials"]
            
            token = f"AwsSecureToken/{cluster_arn}/{credentials['AccessKeyId']}"
            
            return token
            
        except ClientError as e:
            logger.error(f"Failed to get IAM auth token: {e}")
            raise
    
    # =========================================================================
    # Cross-Region Replication
    # =========================================================================
    
    def create_replication_task(
        self,
        source_cluster_arn: str,
        target_cluster_arn: str,
        task_name: str,
        topics: List[str],
        target_prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a cross-region replication task (using MirrorMaker 2 style).
        
        Args:
            source_cluster_arn: Source MSK cluster ARN
            target_cluster_arn: Target MSK cluster ARN
            task_name: Name of the replication task
            topics: List of topics to replicate
            target_prefix: Optional prefix for replicated topics
            
        Returns:
            Replication task configuration
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for cross-region replication")
        
        try:
            task_config = {
                "task_name": task_name,
                "source_cluster_arn": source_cluster_arn,
                "target_cluster_arn": target_cluster_arn,
                "topics": topics,
                "target_prefix": target_prefix,
                "replication_policy": "com.amazonaws:awsmskre MirroringTopicSelection",
                "created_at": datetime.now().isoformat(),
                "status": "configured"
            }
            
            logger.info(f"Configured replication task: {task_name}")
            return task_config
            
        except Exception as e:
            logger.error(f"Failed to create replication task: {e}")
            raise
    
    def setup_mirror_maker(
        self,
        mm_cluster_name: str,
        source_bootstrap_servers: str,
        target_bootstrap_servers: str,
        source_auth_mode: MSKAuthMode = MSKAuthMode.IAM,
        target_auth_mode: MSKAuthMode = MSKAuthMode.IAM,
        topics_pattern: str = ".*",
        groups_pattern: str = ".*"
    ) -> Dict[str, Any]:
        """
        Set up MirrorMaker 2 configuration for cross-cluster replication.
        
        Args:
            mm_cluster_name: Name for the MirrorMaker cluster
            source_bootstrap_servers: Source cluster bootstrap servers
            target_bootstrap_servers: Target cluster bootstrap servers
            source_auth_mode: Source cluster auth mode
            target_auth_mode: Target cluster auth mode
            topics_pattern: Topic regex pattern to replicate
            groups_pattern: Consumer group regex pattern to replicate
            
        Returns:
            MirrorMaker configuration
        """
        try:
            mm_config = {
                "clusters": {
                    "source": {
                        "bootstrap.servers": source_bootstrap_servers,
                        "ssl.endpoint.identification.algorithm": "https",
                        "security.protocol": "SASL_SSL" if source_auth_mode == MSKAuthMode.IAM else "PLAINTEXT",
                        "sasl.mechanism": "AWS_MSK_IAM" if source_auth_mode == MSKAuthMode.IAM else "SCRAM-SHA-512",
                    },
                    "target": {
                        "bootstrap.servers": target_bootstrap_servers,
                        "ssl.endpoint.identification.algorithm": "https",
                        "security.protocol": "SASL_SSL" if target_auth_mode == MSKAuthMode.IAM else "PLAINTEXT",
                        "sasl.mechanism": "AWS_MSK_IAM" if target_auth_mode == MSKAuthMode.IAM else "SCRAM-SHA-512",
                    }
                },
                "mirrors": [
                    {
                        "source_cluster": "source",
                        "target_cluster": "target",
                        "source_topic_selection.pattern": topics_pattern,
                        "source_topic_selection.allowlist": topics_pattern,
                        "groups": groups_pattern,
                        "replication_policy.class": "com.amazonaws:awsmskre MirroringReplicationPolicy",
                        "replication_policy.separator": "",
                    }
                ],
                "connectors": {
                    "replication": {
                        "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                        "source.cluster.alias": "source",
                        "target.cluster.alias": "target",
                    }
                }
            }
            
            logger.info(f"Set up MirrorMaker configuration: {mm_cluster_name}")
            return mm_config
            
        except Exception as e:
            logger.error(f"Failed to setup MirrorMaker: {e}")
            raise
    
    def validate_replication_connectivity(
        self,
        bootstrap_servers: str,
        auth_mode: MSKAuthMode = MSKAuthMode.IAM
    ) -> Dict[str, Any]:
        """
        Validate connectivity to clusters for replication.
        
        Args:
            bootstrap_servers: Bootstrap servers string
            auth_mode: Authentication mode
            
        Returns:
            Connectivity validation results
        """
        try:
            host, port = bootstrap_servers.split(":")[0], int(bootstrap_servers.split(":")[1]) if ":" in bootstrap_servers else 9092
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            
            result = sock.connect_ex((host, port))
            sock.close()
            
            connectivity_ok = result == 0
            
            return {
                "host": host,
                "port": port,
                "connectivity_ok": connectivity_ok,
                "checked_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to validate connectivity: {e}")
            return {
                "host": bootstrap_servers,
                "connectivity_ok": False,
                "error": str(e),
                "checked_at": datetime.now().isoformat()
            }
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def put_metric_data(self, namespace: str, metric_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            namespace: CloudWatch namespace
            metric_data: List of metric data dictionaries
            
        Returns:
            CloudWatch put metric response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        
        try:
            response = self.cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            
            logger.info(f"Put metric data to CloudWatch namespace: {namespace}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def get_cluster_metrics(
        self,
        cluster_name: str,
        metric_names: List[str],
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for an MSK cluster.
        
        Args:
            cluster_name: Name of the MSK cluster
            metric_names: List of metric names to retrieve
            period: Metric period in seconds
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            CloudWatch metrics response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        
        try:
            if start_time is None:
                start_time = datetime.now() - timedelta(hours=1)
            if end_time is None:
                end_time = datetime.now()
            
            cluster_info = self.describe_cluster(cluster_name)
            cluster_arn = cluster_info["ClusterArn"]
            
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": f"m{i}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/Kafka",
                                "MetricName": metric_name,
                                "Dimensions": [
                                    {"Name": "ClusterName", "Value": cluster_name},
                                    {"Name": "ClusterArn", "Value": cluster_arn}
                                ]
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
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get cluster metrics: {e}")
            raise
    
    def create_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        cluster_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for MSK metrics.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Metric name to monitor
            cluster_name: MSK cluster name
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            
        Returns:
            Alarm creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        
        try:
            alarm_config = {
                "AlarmName": alarm_name,
                "AlarmDescription": f"Alarm for {metric_name} on cluster {cluster_name}",
                "MetricName": metric_name,
                "Namespace": "AWS/Kafka",
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": "Average",
                "Dimensions": [
                    {"Name": "ClusterName", "Value": cluster_name}
                ]
            }
            
            response = self.cloudwatch_client.put_metric_alarm(**alarm_config)
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    def create_log_subscription(
        self,
        cluster_name: str,
        log_group_name: str,
        filter_pattern: str = ""
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch Logs subscription for MSK cluster logs.
        
        Args:
            cluster_name: MSK cluster name
            log_group_name: CloudWatch log group name
            filter_pattern: Log filter pattern
            
        Returns:
            Subscription response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        
        try:
            cluster_info = self.describe_cluster(cluster_name)
            cluster_arn = cluster_info["ClusterArn"]
            
            response = self.msk_client.create_scram_secret(
                ClusterArn=cluster_arn,
                SecretArnList=[]
            )
            
            logger.info(f"Created log subscription for cluster {cluster_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create log subscription: {e}")
            raise
    
    def export_metrics_to_cloudwatch(
        self,
        cluster_name: str,
        topic_name: str,
        metrics: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Export custom topic metrics to CloudWatch.
        
        Args:
            cluster_name: MSK cluster name
            topic_name: Kafka topic name
            metrics: Dictionary of metric name to value
            
        Returns:
            Export response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CloudWatch integration")
        
        try:
            cluster_info = self.describe_cluster(cluster_name)
            cluster_arn = cluster_info["ClusterArn"]
            
            metric_data = [
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Timestamp": datetime.now(),
                    "Dimensions": [
                        {"Name": "ClusterName", "Value": cluster_name},
                        {"Name": "TopicName", "Value": topic_name}
                    ]
                }
                for metric_name, value in metrics.items()
            ]
            
            response = self.cloudwatch_client.put_metric_data(
                Namespace="AWS/Kafka",
                MetricData=metric_data
            )
            
            logger.info(f"Exported metrics for topic {topic_name} to CloudWatch")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to export metrics: {e}")
            raise
    
    # =========================================================================
    # MSK Configuration Management
    # =========================================================================
    
    def create_configuration(
        self,
        config_name: str,
        server_properties: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an MSK configuration.
        
        Args:
            config_name: Name of the configuration
            server_properties: Server properties string
            description: Optional description
            
        Returns:
            Configuration creation response
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK configuration")
        
        try:
            params = {
                "Name": config_name,
                "ServerProperties": server_properties.encode("utf-8")
            }
            
            if description:
                params["Description"] = description
            
            response = self.msk_client.create_configuration(**params)
            
            logger.info(f"Created MSK configuration: {config_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create configuration {config_name}: {e}")
            raise
    
    def describe_configuration(
        self,
        config_arn: str
    ) -> Dict[str, Any]:
        """
        Describe an MSK configuration.
        
        Args:
            config_arn: Configuration ARN
            
        Returns:
            Configuration details
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK configuration")
        
        try:
            response = self.msk_client.describe_configuration(ConfigurationArn=config_arn)
            return response
            
        except ClientError as e:
            logger.error(f"Failed to describe configuration: {e}")
            raise
    
    def list_configurations(self) -> List[Dict[str, Any]]:
        """
        List all MSK configurations.
        
        Returns:
            List of configurations
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for MSK configuration")
        
        try:
            response = self.msk_client.list_configurations()
            return response.get("Configurations", [])
            
        except ClientError as e:
            logger.error(f"Failed to list configurations: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_cluster_info(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get cached or fresh cluster information.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Cluster information
        """
        with self._lock:
            cached = self._cluster_cache.get(cluster_name)
        
        if cached:
            try:
                fresh_info = self.describe_cluster(cluster_name)
                return fresh_info
            except Exception:
                return cached
        
        return self.describe_cluster(cluster_name)
    
    def wait_for_cluster_active(
        self,
        cluster_name: str,
        timeout_seconds: int = 600,
        poll_interval: int = 30
    ) -> bool:
        """
        Wait for a cluster to become active.
        
        Args:
            cluster_name: Name of the cluster
            timeout_seconds: Maximum wait time
            poll_interval: Poll interval in seconds
            
        Returns:
            True if cluster is active, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                info = self.describe_cluster(cluster_name)
                state = info.get("State")
                
                if state == "ACTIVE":
                    return True
                elif state in ["DELETING", "FAILED"]:
                    raise Exception(f"Cluster in terminal state: {state}")
                
                logger.info(f"Cluster {cluster_name} state: {state}, waiting...")
                
            except ClientError as e:
                if e.response["Error"]["Code"] == "NotFound":
                    logger.info(f"Cluster {cluster_name} not found yet, waiting...")
                else:
                    raise
            
            time.sleep(poll_interval)
        
        return False
    
    def health_check(self, cluster_name: str) -> Dict[str, Any]:
        """
        Perform health check on MSK cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Health check results
        """
        try:
            cluster_info = self.describe_cluster(cluster_name)
            
            bootstrap_servers = self._get_bootstrap_servers(cluster_name)
            
            connectivity = self.validate_replication_connectivity(bootstrap_servers)
            
            topics = self.list_topics(cluster_name)
            
            return {
                "cluster_name": cluster_name,
                "state": cluster_info.get("State"),
                "bootstrap_servers": bootstrap_servers,
                "connectivity_ok": connectivity.get("connectivity_ok", False),
                "topic_count": len(topics),
                "healthy": cluster_info.get("State") == "ACTIVE" and connectivity.get("connectivity_ok", False),
                "checked_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "cluster_name": cluster_name,
                "healthy": False,
                "error": str(e),
                "checked_at": datetime.now().isoformat()
            }
    
    def cleanup(self):
        """Clean up all resources (producers, consumers, etc.)."""
        self.close_producers()
        self.close_consumers()
        
        with self._lock:
            self._schema_registry_clients.clear()
            self._cluster_cache.clear()
            self._topic_cache.clear()
        
        logger.info("MSK integration cleanup complete")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
        return False


def create_msk_integration(region_name: str = "us-east-1", **kwargs) -> MSKIntegration:
    """
    Factory function to create MSK integration.
    
    Args:
        region_name: AWS region name
        **kwargs: Additional arguments for MSKIntegration
        
    Returns:
        MSKIntegration instance
    """
    return MSKIntegration(region_name=region_name, **kwargs)
