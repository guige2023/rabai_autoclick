"""
Tests for workflow_aws_msk module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_msk
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Mock kafka module (not available)
mock_kafka = types.ModuleType('kafka')
mock_kafka.KafkaProducer = MagicMock()
mock_kafka.KafkaConsumer = MagicMock()
mock_kafka.KafkaAdminClient = MagicMock()
sys.modules['kafka'] = mock_kafka
sys.modules['kafka.admin'] = MagicMock()

# Mock confluent_kafka module (not available)
mock_confluent_kafka = types.ModuleType('confluent_kafka')
mock_confluent_kafka.Producer = MagicMock()
mock_confluent_kafka.Consumer = MagicMock()
mock_confluent_kafka.AdminClient = MagicMock()
mock_confluent_kafka.avro = MagicMock()
mock_confluent_kafka.KafkaError = Exception
sys.modules['confluent_kafka'] = mock_confluent_kafka
sys.modules['confluent_kafka.schema_registries'] = MagicMock()
sys.modules['confluent_kafka.schema_registry'] = MagicMock()

import src.workflow_aws_msk as _msk_module

if _msk_module is not None:
    MSKIntegration = _msk_module.MSKIntegration
    MSKClusterConfig = _msk_module.MSKClusterConfig
    MSKTopicConfig = _msk_module.MSKTopicConfig
    MSKProducerConfig = _msk_module.MSKProducerConfig
    MSKConsumerConfig = _msk_module.MSKConsumerConfig
    MSKConnectorConfig = _msk_module.MSKConnectorConfig
    SchemaRegistryConfig = _msk_module.SchemaRegistryConfig
    MSKClusterType = _msk_module.MSKClusterType
    MSKBrokerType = _msk_module.MSKBrokerType
    MSKStorageMode = _msk_module.MSKStorageMode
    MSKEncryptionMode = _msk_module.MSKEncryptionMode
    MSKAuthMode = _msk_module.MSKAuthMode
    CompressionType = _msk_module.CompressionType
    AcksMode = _msk_module.AcksMode
    OffsetResetStrategy = _msk_module.OffsetResetStrategy
    SchemaType = _msk_module.SchemaType


class TestMSKClusterConfig(unittest.TestCase):
    """Test MSKClusterConfig dataclass"""

    def test_config_defaults(self):
        """Test default configuration"""
        config = MSKClusterConfig(cluster_name="my-cluster")
        self.assertEqual(config.cluster_name, "my-cluster")
        self.assertEqual(config.kafka_version, "3.6.0")
        self.assertEqual(config.number_of_broker_nodes, 3)
        self.assertEqual(config.broker_type, MSKBrokerType.KAFKA_M5_LARGE)
        self.assertEqual(config.cluster_type, MSKClusterType.PROVISIONED)
        self.assertEqual(config.storage_mode, MSKStorageMode.LOCAL)
        self.assertEqual(config.encryption_mode, MSKEncryptionMode.TLS)
        self.assertEqual(config.auth_mode, MSKAuthMode.IAM)

    def test_config_custom(self):
        """Test custom configuration"""
        config = MSKClusterConfig(
            cluster_name="my-cluster",
            kafka_version="3.7.0",
            number_of_broker_nodes=5,
            broker_type=MSKBrokerType.KAFKA_M5_4XLARGE,
            cluster_type=MSKClusterType.SERVERLESS,
            storage_mode=MSKStorageMode.TIERED,
            encryption_mode=MSKEncryptionMode.TLS_PLAINTEXT,
            auth_mode=MSKAuthMode.SASL,
            tags={"env": "prod"}
        )
        self.assertEqual(config.kafka_version, "3.7.0")
        self.assertEqual(config.number_of_broker_nodes, 5)
        self.assertEqual(config.broker_type, MSKBrokerType.KAFKA_M5_4XLARGE)
        self.assertEqual(config.cluster_type, MSKClusterType.SERVERLESS)
        self.assertEqual(config.encryption_mode, MSKEncryptionMode.TLS_PLAINTEXT)
        self.assertEqual(config.auth_mode, MSKAuthMode.SASL)
        self.assertEqual(config.tags, {"env": "prod"})


class TestMSKTopicConfig(unittest.TestCase):
    """Test MSKTopicConfig dataclass"""

    def test_config_defaults(self):
        """Test default configuration"""
        config = MSKTopicConfig(name="my-topic")
        self.assertEqual(config.name, "my-topic")
        self.assertEqual(config.partitions, 3)
        self.assertEqual(config.replication_factor, 3)
        self.assertEqual(config.retention_ms, 604800000)  # 7 days
        self.assertEqual(config.max_message_bytes, 1048576)  # 1MB
        self.assertEqual(config.min_insync_replicas, 2)
        self.assertEqual(config.cleanup_policy, "delete")
        self.assertEqual(config.compression_type, CompressionType.NONE)

    def test_config_custom(self):
        """Test custom configuration"""
        config = MSKTopicConfig(
            name="my-topic",
            partitions=10,
            replication_factor=5,
            retention_ms=864000000,  # 10 days
            cleanup_policy="compact",
            compression_type=CompressionType.SNAPPY
        )
        self.assertEqual(config.partitions, 10)
        self.assertEqual(config.replication_factor, 5)
        self.assertEqual(config.cleanup_policy, "compact")
        self.assertEqual(config.compression_type, CompressionType.SNAPPY)

    def test_to_admin_config(self):
        """Test converting to admin config"""
        config = MSKTopicConfig(
            name="my-topic",
            retention_ms=604800000,
            max_message_bytes=1048576,
            min_insync_replicas=2,
            cleanup_policy="delete",
            compression_type=CompressionType.GZIP,
            preallocate=True
        )
        admin_config = config.to_admin_config()
        self.assertEqual(admin_config["retention.ms"], "604800000")
        self.assertEqual(admin_config["max.message.bytes"], "1048576")
        self.assertEqual(admin_config["min.insync.replicas"], "2")
        self.assertEqual(admin_config["cleanup.policy"], "delete")
        self.assertEqual(admin_config["compression.type"], "gzip")


class TestMSKProducerConfig(unittest.TestCase):
    """Test MSKProducerConfig dataclass"""

    def test_config_defaults(self):
        """Test default configuration"""
        config = MSKProducerConfig()
        self.assertEqual(config.bootstrap_servers, "")
        self.assertEqual(config.client_id, "workflow-msk-producer")
        self.assertEqual(config.acks, "all")
        self.assertEqual(config.compression_type, CompressionType.NONE)
        self.assertEqual(config.batch_size, 16384)
        self.assertEqual(config.retries, 3)
        self.assertTrue(config.enable_idempotence)

    def test_config_custom(self):
        """Test custom configuration"""
        config = MSKProducerConfig(
            bootstrap_servers="broker1:9092,broker2:9092",
            client_id="my-producer",
            acks=1,
            compression_type=CompressionType.LZ4,
            retries=5,
            enable_idempotence=False
        )
        self.assertEqual(config.bootstrap_servers, "broker1:9092,broker2:9092")
        self.assertEqual(config.client_id, "my-producer")
        self.assertEqual(config.acks, 1)
        self.assertEqual(config.compression_type, CompressionType.LZ4)
        self.assertEqual(config.retries, 5)
        self.assertFalse(config.enable_idempotence)


class TestMSKConsumerConfig(unittest.TestCase):
    """Test MSKConsumerConfig dataclass"""

    def test_config_defaults(self):
        """Test default configuration"""
        config = MSKConsumerConfig()
        self.assertEqual(config.bootstrap_servers, "")
        self.assertEqual(config.group_id, "workflow-msk-consumer-group")
        self.assertEqual(config.client_id, "workflow-msk-consumer")
        self.assertEqual(config.auto_offset_reset, OffsetResetStrategy.EARLIEST)
        self.assertTrue(config.enable_auto_commit)
        self.assertEqual(config.session_timeout_ms, 30000)

    def test_config_custom(self):
        """Test custom configuration"""
        config = MSKConsumerConfig(
            bootstrap_servers="broker1:9092,broker2:9092",
            group_id="my-group",
            client_id="my-consumer",
            auto_offset_reset=OffsetResetStrategy.LATEST,
            enable_auto_commit=False,
            max_poll_records=1000
        )
        self.assertEqual(config.bootstrap_servers, "broker1:9092,broker2:9092")
        self.assertEqual(config.group_id, "my-group")
        self.assertEqual(config.auto_offset_reset, OffsetResetStrategy.LATEST)
        self.assertFalse(config.enable_auto_commit)
        self.assertEqual(config.max_poll_records, 1000)


class TestMSKConnectorConfig(unittest.TestCase):
    """Test MSKConnectorConfig dataclass"""

    def test_config_required(self):
        """Test required fields"""
        config = MSKConnectorConfig(
            connector_name="my-connector",
            connector_class="com.mycompany.MyConnector",
            kafka_cluster_arn="arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/xyz"
        )
        self.assertEqual(config.connector_name, "my-connector")
        self.assertEqual(config.connector_class, "com.mycompany.MyConnector")
        self.assertEqual(config.kafka_cluster_arn, "arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/xyz")
        self.assertEqual(config.kafka_connect_version, "2.7.1")

    def test_config_with_vpc(self):
        """Test config with VPC configuration"""
        config = MSKConnectorConfig(
            connector_name="my-connector",
            connector_class="com.mycompany.MyConnector",
            kafka_cluster_arn="arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/xyz",
            vpc_config={"subnet_ids": ["subnet-1", "subnet-2"], "security_groups": ["sg-1"]}
        )
        self.assertEqual(config.vpc_config["subnet_ids"], ["subnet-1", "subnet-2"])


class TestSchemaRegistryConfig(unittest.TestCase):
    """Test SchemaRegistryConfig dataclass"""

    def test_config_defaults(self):
        """Test default configuration"""
        config = SchemaRegistryConfig()
        self.assertEqual(config.url, "http://localhost:8081")
        self.assertIsNone(config.api_key)
        self.assertIsNone(config.api_secret)

    def test_config_custom(self):
        """Test custom configuration"""
        config = SchemaRegistryConfig(
            url="https://schema-registry.mycompany.com",
            api_key="my-api-key",
            api_secret="my-secret",
            ca_location="/path/to/ca.pem",
            cert_location="/path/to/cert.pem",
            key_location="/path/to/key.pem"
        )
        self.assertEqual(config.url, "https://schema-registry.mycompany.com")
        self.assertEqual(config.api_key, "my-api-key")
        self.assertEqual(config.ca_location, "/path/to/ca.pem")


class TestMSKClusterType(unittest.TestCase):
    """Test MSKClusterType enum"""

    def test_cluster_type_values(self):
        """Test cluster type values"""
        self.assertEqual(MSKClusterType.PROVISIONED.value, "PROVISIONED")
        self.assertEqual(MSKClusterType.SERVERLESS.value, "SERVERLESS")


class TestMSKBrokerType(unittest.TestCase):
    """Test MSKBrokerType enum"""

    def test_broker_type_values(self):
        """Test broker type values"""
        self.assertEqual(MSKBrokerType.KAFKA_M5_LARGE.value, "kafka.m5.large")
        self.assertEqual(MSKBrokerType.KAFKA_M5_XLARGE.value, "kafka.m5.xlarge")
        self.assertEqual(MSKBrokerType.KAFKA_M5_2XLARGE.value, "kafka.m5.2xlarge")
        self.assertEqual(MSKBrokerType.KAFKA_M5_4XLARGE.value, "kafka.m5.4xlarge")
        self.assertEqual(MSKBrokerType.KAFKA_T3_SMALL.value, "kafka.t3.small")


class TestMSKStorageMode(unittest.TestCase):
    """Test MSKStorageMode enum"""

    def test_storage_mode_values(self):
        """Test storage mode values"""
        self.assertEqual(MSKStorageMode.LOCAL.value, "LOCAL")
        self.assertEqual(MSKStorageMode.TIERED.value, "TIERED")


class TestMSKEncryptionMode(unittest.TestCase):
    """Test MSKEncryptionMode enum"""

    def test_encryption_mode_values(self):
        """Test encryption mode values"""
        self.assertEqual(MSKEncryptionMode.TLS.value, "TLS")
        self.assertEqual(MSKEncryptionMode.TLS_PLAINTEXT.value, "TLS_PLAINTEXT")
        self.assertEqual(MSKEncryptionMode.PLAINTEXT.value, "PLAINTEXT")


class TestMSKAuthMode(unittest.TestCase):
    """Test MSKAuthMode enum"""

    def test_auth_mode_values(self):
        """Test auth mode values"""
        self.assertEqual(MSKAuthMode.IAM.value, "IAM")
        self.assertEqual(MSKAuthMode.SASL.value, "SASL")
        self.assertEqual(MSKAuthMode.TLS.value, "TLS")
        self.assertEqual(MSKAuthMode.TLS_PLAINTEXT.value, "TLS_PLAINTEXT")


class TestCompressionType(unittest.TestCase):
    """Test CompressionType enum"""

    def test_compression_type_values(self):
        """Test compression type values"""
        self.assertEqual(CompressionType.NONE.value, "none")
        self.assertEqual(CompressionType.GZIP.value, "gzip")
        self.assertEqual(CompressionType.SNAPPY.value, "snappy")
        self.assertEqual(CompressionType.LZ4.value, "lz4")
        self.assertEqual(CompressionType.ZSTD.value, "zstd")


class TestAcksMode(unittest.TestCase):
    """Test AcksMode enum"""

    def test_acks_mode_values(self):
        """Test acks mode values"""
        self.assertEqual(AcksMode.ALL.value, -1)
        self.assertEqual(AcksMode.NONE.value, 0)
        self.assertEqual(AcksMode.LEADER.value, 1)


class TestOffsetResetStrategy(unittest.TestCase):
    """Test OffsetResetStrategy enum"""

    def test_offset_reset_strategy_values(self):
        """Test offset reset strategy values"""
        self.assertEqual(OffsetResetStrategy.EARLIEST.value, "earliest")
        self.assertEqual(OffsetResetStrategy.LATEST.value, "latest")
        self.assertEqual(OffsetResetStrategy.NONE.value, "none")


class TestSchemaType(unittest.TestCase):
    """Test SchemaType enum"""

    def test_schema_type_values(self):
        """Test schema type values"""
        self.assertEqual(SchemaType.AVRO.value, "AVRO")
        self.assertEqual(SchemaType.JSON.value, "JSON")
        self.assertEqual(SchemaType.PROTOBUF.value, "PROTOBUF")


class TestMSKIntegration(unittest.TestCase):
    """Test MSKIntegration class"""

    def test_init_defaults(self):
        """Test initialization with defaults"""
        integration = MSKIntegration()
        self.assertEqual(integration.region_name, "us-east-1")
        self.assertIsNone(integration.profile_name)

    def test_init_custom(self):
        """Test initialization with custom values"""
        integration = MSKIntegration(region_name="us-west-2", profile_name="myprofile")
        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.profile_name, "myprofile")

    def test_cluster_cache_initialized(self):
        """Test cluster cache is initialized"""
        integration = MSKIntegration()
        self.assertIsInstance(integration._cluster_cache, dict)
        self.assertEqual(len(integration._cluster_cache), 0)

    def test_topic_cache_initialized(self):
        """Test topic cache is initialized"""
        integration = MSKIntegration()
        self.assertIsInstance(integration._topic_cache, dict)

    def test_producer_instances_initialized(self):
        """Test producer instances is initialized"""
        integration = MSKIntegration()
        self.assertIsInstance(integration._producer_instances, dict)

    def test_consumer_instances_initialized(self):
        """Test consumer instances is initialized"""
        integration = MSKIntegration()
        self.assertIsInstance(integration._consumer_instances, dict)

    def test_lock_initialized(self):
        """Test lock is initialized"""
        integration = MSKIntegration()
        self.assertIsNotNone(integration._lock)


class TestMSKIntegrationClusterOperations(unittest.TestCase):
    """Test MSKIntegration cluster operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_cluster_without_boto3_raises(self):
        """Test cluster creation without boto3 raises ImportError"""
        config = MSKClusterConfig(cluster_name="my-cluster")
        with self.assertRaises(ImportError):
            self.integration.create_cluster(config)

    def test_describe_cluster_without_boto3_raises(self):
        """Test describing cluster without boto3 raises ImportError"""
        with self.assertRaises(ImportError):
            self.integration.describe_cluster("my-cluster")

    def test_list_clusters_without_boto3_raises(self):
        """Test listing clusters without boto3 raises ImportError"""
        with self.assertRaises(ImportError):
            self.integration.list_clusters()


class TestMSKIntegrationTopicOperations(unittest.TestCase):
    """Test MSKIntegration topic operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_topic_returns_topic_info(self):
        """Test topic creation returns topic info"""
        config = MSKTopicConfig(name="my-topic")
        result = self.integration.create_topic(config)
        self.assertIn("name", result)

    def test_list_topics_returns_list(self):
        """Test listing topics returns list"""
        result = self.integration.list_topics()
        self.assertIsInstance(result, list)

    def test_delete_topic_returns_result(self):
        """Test deleting topic returns result"""
        result = self.integration.delete_topic("my-topic")
        self.assertIn("deleted", result)


class TestMSKIntegrationProducerOperations(unittest.TestCase):
    """Test MSKIntegration producer operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_producer_returns_producer(self):
        """Test producer creation returns producer"""
        config = MSKProducerConfig(bootstrap_servers="broker1:9092")
        result = self.integration.create_producer(config)
        self.assertIsNotNone(result)

    def test_send_message_returns_result(self):
        """Test sending message returns result"""
        result = self.integration.send_message("my-topic", {"key": "value"})
        self.assertIn("status", result)


class TestMSKIntegrationConsumerOperations(unittest.TestCase):
    """Test MSKIntegration consumer operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_consumer_returns_consumer(self):
        """Test consumer creation returns consumer"""
        config = MSKConsumerConfig(bootstrap_servers="broker1:9092")
        result = self.integration.create_consumer(config)
        self.assertIsNotNone(result)

    def test_consume_messages_returns_list(self):
        """Test consuming messages returns list"""
        result = self.integration.consume_messages("my-topic", max_records=10)
        self.assertIsInstance(result, list)


class TestMSKIntegrationSchemaRegistryOperations(unittest.TestCase):
    """Test MSKIntegration schema registry operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_register_schema(self):
        """Test schema registration"""
        result = self.integration.register_schema(
            schema_name="my-schema",
            schema_type=SchemaType.AVRO,
            schema_string='{"type": "record", "name": "Test", "fields": []}'
        )
        self.assertIn("schema_id", result)

    def test_get_schema(self):
        """Test getting schema"""
        result = self.integration.get_schema("my-schema")
        self.assertIn("schema", result)

    def test_list_schemas(self):
        """Test listing schemas"""
        result = self.integration.list_schemas()
        self.assertIsInstance(result, list)


class TestMSKIntegrationConnectorOperations(unittest.TestCase):
    """Test MSKIntegration connector operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_connector(self):
        """Test connector creation"""
        config = MSKConnectorConfig(
            connector_name="my-connector",
            connector_class="com.mycompany.MyConnector",
            kafka_cluster_arn="arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/xyz"
        )
        result = self.integration.create_connector(config)
        self.assertIn("connector_name", result)

    def test_list_connectors(self):
        """Test listing connectors"""
        result = self.integration.list_connectors()
        self.assertIsInstance(result, list)


class TestMSKIntegrationMonitoringOperations(unittest.TestCase):
    """Test MSKIntegration monitoring operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_get_cluster_metrics(self):
        """Test getting cluster metrics"""
        result = self.integration.get_cluster_metrics("my-cluster")
        self.assertIsInstance(result, dict)

    def test_get_topic_metrics(self):
        """Test getting topic metrics"""
        result = self.integration.get_topic_metrics("my-cluster", "my-topic")
        self.assertIsInstance(result, dict)

    def test_enable_monitoring(self):
        """Test enabling monitoring"""
        result = self.integration.enable_monitoring("my-cluster")
        self.assertIn("monitoring_enabled", result)


class TestMSKIntegrationSecurityOperations(unittest.TestCase):
    """Test MSKIntegration security operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_iam_role(self):
        """Test IAM role creation"""
        result = self.integration.create_iam_role("my-cluster")
        self.assertIn("role_arn", result)

    def test_configure_sasl_scram(self):
        """Test SASL/SCRAM configuration"""
        result = self.integration.configure_sasl_scram("my-cluster")
        self.assertIn("sasl_scram_enabled", result)


class TestMSKIntegrationServerlessOperations(unittest.TestCase):
    """Test MSKIntegration serverless operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.integration = MSKIntegration()

    def test_create_serverless_cluster(self):
        """Test serverless cluster creation"""
        config = MSKClusterConfig(
            cluster_name="my-serverless-cluster",
            cluster_type=MSKClusterType.SERVERLESS
        )
        result = self.integration.create_cluster(config)
        self.assertIn("cluster_name", result)


if __name__ == '__main__':
    unittest.main()
