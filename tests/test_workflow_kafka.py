"""
Tests for workflow_kafka module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Mock kafka module before importing workflow_kafka
import sys
import types

# Create mock kafka module
mock_kafka = types.ModuleType('kafka')
mock_kafka.KafkaProducer = MagicMock
mock_kafka.KafkaConsumer = MagicMock
mock_kafka.KafkaAdminClient = MagicMock
mock_kafka.errors = types.ModuleType('kafka.errors')
mock_kafka.errors.TopicAlreadyExistsError = Exception
mock_kafka.errors.KafkaError = Exception
mock_kafka.admin = types.ModuleType('kafka.admin')
mock_kafka.admin.NewTopic = MagicMock
mock_kafka.admin.ConfigResource = MagicMock
mock_kafka.admin.ConfigResourceType = MagicMock
sys.modules['kafka'] = mock_kafka
sys.modules['kafka.errors'] = mock_kafka.errors
sys.modules['kafka.admin'] = mock_kafka.admin

# Mock confluent_kafka module
mock_confluent = types.ModuleType('confluent_kafka')
mock_confluent.Producer = MagicMock
mock_confluent.Consumer = MagicMock
mock_confluent.AdminClient = MagicMock
mock_confluent.KafkaStreams = MagicMock
mock_confluent.SchemaRegistryClient = MagicMock
mock_confluent.avro = MagicMock
mock_confluent.schema_registries = types.ModuleType('confluent_kafka.schema_registries')
mock_confluent.schema_registries.SchemaRegistryClient = MagicMock
mock_confluent.schema_registry = types.ModuleType('confluent_kafka.schema_registry')
mock_confluent.schema_registry.avro = types.ModuleType('confluent_kafka.schema_registry.avro')
mock_confluent.schema_registry.avro.AvroSerializer = MagicMock
mock_confluent.schema_registry.avro.AvroDeserializer = MagicMock
mock_confluent.admin = types.ModuleType('confluent_kafka.admin')
mock_confluent.admin.NewTopic = MagicMock
mock_confluent.KafkaError = MagicMock
sys.modules['confluent_kafka'] = mock_confluent
sys.modules['confluent_kafka.schema_registries'] = mock_confluent.schema_registries
sys.modules['confluent_kafka.schema_registry'] = mock_confluent.schema_registry
sys.modules['confluent_kafka.schema_registry.avro'] = mock_confluent.schema_registry.avro
sys.modules['confluent_kafka.admin'] = mock_confluent.admin

# Now we can import the module
from src.workflow_kafka import (
    KafkaIntegration,
    CompressionType,
    AcksMode,
    OffsetResetStrategy,
    TopicConfig,
    ProducerConfig,
    ConsumerConfig,
    StreamConfig,
    ConnectorConfig,
    MirrorMakerConfig,
    KSQLQuery,
    KafkaMetric,
    SchemaInfo,
)


class TestCompressionType(unittest.TestCase):
    """Test CompressionType enum"""

    def test_compression_type_values(self):
        self.assertEqual(CompressionType.NONE.value, "none")
        self.assertEqual(CompressionType.GZIP.value, "gzip")
        self.assertEqual(CompressionType.SNAPPY.value, "snappy")
        self.assertEqual(CompressionType.LZ4.value, "lz4")
        self.assertEqual(CompressionType.ZSTD.value, "zstd")


class TestAcksMode(unittest.TestCase):
    """Test AcksMode enum"""

    def test_acks_mode_values(self):
        self.assertEqual(AcksMode.ALL.value, -1)
        self.assertEqual(AcksMode.NONE.value, 0)
        self.assertEqual(AcksMode.LEADER.value, 1)


class TestOffsetResetStrategy(unittest.TestCase):
    """Test OffsetResetStrategy enum"""

    def test_offset_reset_strategy_values(self):
        self.assertEqual(OffsetResetStrategy.EARLIEST.value, "earliest")
        self.assertEqual(OffsetResetStrategy.LATEST.value, "latest")
        self.assertEqual(OffsetResetStrategy.NONE.value, "none")


class TestTopicConfig(unittest.TestCase):
    """Test TopicConfig dataclass"""

    def test_topic_config_defaults(self):
        config = TopicConfig(name="test-topic")
        self.assertEqual(config.name, "test-topic")
        self.assertEqual(config.partitions, 3)
        self.assertEqual(config.replication_factor, 1)
        self.assertEqual(config.retention_ms, 604800000)
        self.assertEqual(config.cleanup_policy, "delete")
        self.assertEqual(config.compression_type, CompressionType.NONE)

    def test_topic_config_custom(self):
        config = TopicConfig(
            name="custom-topic",
            partitions=10,
            replication_factor=3,
            retention_ms=86400000,
            cleanup_policy="compact",
            compression_type=CompressionType.SNAPPY
        )
        self.assertEqual(config.partitions, 10)
        self.assertEqual(config.replication_factor, 3)
        self.assertEqual(config.cleanup_policy, "compact")
        self.assertEqual(config.compression_type, CompressionType.SNAPPY)

    def test_to_admin_config(self):
        config = TopicConfig(name="test-topic", retention_ms=3600000)
        admin_config = config.to_admin_config()
        self.assertIsInstance(admin_config, dict)
        self.assertEqual(admin_config["retention.ms"], "3600000")
        self.assertEqual(admin_config["cleanup.policy"], "delete")


class TestProducerConfig(unittest.TestCase):
    """Test ProducerConfig dataclass"""

    def test_producer_config_defaults(self):
        config = ProducerConfig()
        self.assertEqual(config.bootstrap_servers, "localhost:9092")
        self.assertEqual(config.client_id, "workflow-kafka-producer")
        self.assertEqual(config.acks, "all")
        self.assertEqual(config.retries, 3)
        self.assertTrue(config.enable_idempotence)

    def test_producer_config_custom(self):
        config = ProducerConfig(
            bootstrap_servers="kafka:9092",
            client_id="custom-producer",
            acks=1,
            batch_size=32768,
            enable_idempotence=False
        )
        self.assertEqual(config.bootstrap_servers, "kafka:9092")
        self.assertEqual(config.acks, 1)
        self.assertEqual(config.batch_size, 32768)
        self.assertFalse(config.enable_idempotence)


class TestConsumerConfig(unittest.TestCase):
    """Test ConsumerConfig dataclass"""

    def test_consumer_config_defaults(self):
        config = ConsumerConfig()
        self.assertEqual(config.bootstrap_servers, "localhost:9092")
        self.assertEqual(config.group_id, "workflow-consumer-group")
        self.assertEqual(config.auto_offset_reset, OffsetResetStrategy.EARLIEST)
        self.assertTrue(config.enable_auto_commit)

    def test_consumer_config_custom(self):
        config = ConsumerConfig(
            group_id="my-group",
            auto_offset_reset=OffsetResetStrategy.LATEST,
            enable_auto_commit=False,
            max_poll_records=1000
        )
        self.assertEqual(config.group_id, "my-group")
        self.assertEqual(config.auto_offset_reset, OffsetResetStrategy.LATEST)
        self.assertFalse(config.enable_auto_commit)
        self.assertEqual(config.max_poll_records, 1000)


class TestStreamConfig(unittest.TestCase):
    """Test StreamConfig dataclass"""

    def test_stream_config_defaults(self):
        config = StreamConfig(application_id="test-app")
        self.assertEqual(config.application_id, "test-app")
        self.assertEqual(config.bootstrap_servers, "localhost:9092")
        self.assertEqual(config.num_stream_threads, 1)
        self.assertEqual(config.processing_guarantee, "exactly_once_v2")

    def test_stream_config_custom(self):
        config = StreamConfig(
            application_id="custom-app",
            bootstrap_servers="kafka:9092",
            num_stream_threads=4,
            processing_guarantee="at_least_once"
        )
        self.assertEqual(config.num_stream_threads, 4)
        self.assertEqual(config.processing_guarantee, "at_least_once")


class TestConnectorConfig(unittest.TestCase):
    """Test ConnectorConfig dataclass"""

    def test_connector_config(self):
        config = ConnectorConfig(
            name="my-connector",
            connector_class="org.apache.kafka.connect.file.FileStreamSinkConnector",
            tasks_max=5,
            topics=["topic1", "topic2"]
        )
        self.assertEqual(config.name, "my-connector")
        self.assertEqual(config.tasks_max, 5)
        self.assertEqual(config.topics, ["topic1", "topic2"])


class TestMirrorMakerConfig(unittest.TestCase):
    """Test MirrorMakerConfig dataclass"""

    def test_mirror_maker_config(self):
        config = MirrorMakerConfig(
            source_cluster={"bootstrap_servers": "source:9092"},
            target_cluster={"bootstrap_servers": "target:9092"},
            topics=["topic1", "topic2"],
            emit_heartbeats=True
        )
        self.assertEqual(config.source_cluster["bootstrap_servers"], "source:9092")
        self.assertEqual(config.emit_heartbeats, True)


class TestKSQLQuery(unittest.TestCase):
    """Test KSQLQuery dataclass"""

    def test_ksql_query_defaults(self):
        query = KSQLQuery(query_string="SELECT * FROM test;")
        self.assertEqual(query.query_string, "SELECT * FROM test;")
        self.assertIsNone(query.query_id)
        self.assertEqual(query.properties, {})

    def test_ksql_query_custom(self):
        query = KSQLQuery(
            query_string="SELECT * FROM test;",
            query_id="123",
            properties={"auto.offset.reset": "earliest"}
        )
        self.assertEqual(query.query_id, "123")
        self.assertEqual(query.properties["auto.offset.reset"], "earliest")


class TestKafkaMetric(unittest.TestCase):
    """Test KafkaMetric dataclass"""

    def test_kafka_metric(self):
        timestamp = datetime.utcnow()
        metric = KafkaMetric(
            name="test.metric",
            value=42.5,
            timestamp=timestamp,
            tags={"host": "kafka1"}
        )
        self.assertEqual(metric.name, "test.metric")
        self.assertEqual(metric.value, 42.5)
        self.assertEqual(metric.tags["host"], "kafka1")


class TestSchemaInfo(unittest.TestCase):
    """Test SchemaInfo dataclass"""

    def test_schema_info(self):
        schema = SchemaInfo(
            subject="test-value",
            schema_id=1,
            version=1,
            schema_type="AVRO",
            schema_str='{"type": "record", "name": "Test"}',
            references=[]
        )
        self.assertEqual(schema.subject, "test-value")
        self.assertEqual(schema.schema_id, 1)
        self.assertEqual(schema.schema_type, "AVRO")


class TestKafkaIntegrationInit(unittest.TestCase):
    """Test KafkaIntegration initialization"""

    def test_init_with_defaults(self):
        with patch('src.workflow_kafka.KAFKA_AVAILABLE', True):
            integration = KafkaIntegration()
            self.assertEqual(integration.bootstrap_servers, "localhost:9092")
            self.assertIsNone(integration.schema_registry_url)
            self.assertFalse(integration.use_confluent)

    def test_init_with_custom_params(self):
        with patch('src.workflow_kafka.KAFKA_AVAILABLE', True):
            integration = KafkaIntegration(
                bootstrap_servers="kafka:9092",
                schema_registry_url="http://schema-registry:8081",
                use_confluent=True
            )
            self.assertEqual(integration.bootstrap_servers, "kafka:9092")
            self.assertEqual(integration.schema_registry_url, "http://schema-registry:8081")

    def test_init_stores_consumers_and_producers(self):
        with patch('src.workflow_kafka.KAFKA_AVAILABLE', True):
            integration = KafkaIntegration()
            self.assertEqual(integration._consumers, {})
            self.assertIsNone(integration._producer)
            self.assertEqual(integration._metrics, [])


class TestKafkaIntegrationTopicManagement(unittest.TestCase):
    """Test KafkaIntegration topic management methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_create_topic_success(self):
        with patch('src.workflow_kafka.NewTopic') as mock_new_topic:
            with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
                mock_admin = MagicMock()
                mock_admin_client_class.return_value = mock_admin
                mock_new_topic.return_value = MagicMock()

                topic_config = TopicConfig(name="test-topic")

                with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                    result = self.integration.create_topic(topic_config)

                self.assertTrue(result)
                mock_admin.create_topics.assert_called_once()

    def test_create_topic_already_exists(self):
        with patch('src.workflow_kafka.NewTopic') as mock_new_topic:
            with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
                from kafka.errors import TopicAlreadyExistsError

                mock_admin = MagicMock()
                mock_admin_client_class.return_value = mock_admin
                mock_new_topic.return_value = MagicMock()
                mock_admin.create_topics.side_effect = TopicAlreadyExistsError()

                topic_config = TopicConfig(name="test-topic")

                with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                    result = self.integration.create_topic(topic_config)

                self.assertFalse(result)

    def test_delete_topic(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_admin = MagicMock()
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                result = self.integration.delete_topic("test-topic")

            self.assertTrue(result)
            mock_admin.delete_topics.assert_called_once_with(["test-topic"])

    def test_list_topics(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_admin = MagicMock()
            mock_topics = MagicMock()
            mock_topics.topics = {"topic1": MagicMock(), "topic2": MagicMock()}
            mock_admin.list_topics.return_value = mock_topics
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                result = self.integration.list_topics()

            self.assertEqual(set(result), {"topic1", "topic2"})

    def test_describe_topic(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            with patch('src.workflow_kafka.ConfigResource') as mock_config_resource:
                with patch('src.workflow_kafka.ConfigResourceType') as mock_config_resource_type:
                    mock_config_resource_type.TOPIC = "TOPIC"
                    mock_admin = MagicMock()
                    mock_topic_metadata = MagicMock()
                    mock_topic_metadata.topic = "test-topic"
                    mock_partition = MagicMock()
                    mock_partition.partition = 0
                    mock_partition.leader = 1
                    mock_partition.replicas = [MagicMock(nodeId=1)]
                    mock_partition.isr = [MagicMock(nodeId=1)]
                    mock_topic_metadata.partitions = {0: mock_partition}

                    mock_topics = MagicMock()
                    mock_topics.topics = {"test-topic": mock_topic_metadata}
                    mock_admin.list_topics.return_value = mock_topics
                    mock_admin_client_class.return_value = mock_admin

                    with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                        result = self.integration.describe_topic("test-topic")

                    self.assertEqual(result["name"], "test-topic")
                    self.assertEqual(len(result["partitions"]), 1)

    def test_describe_topic_not_found(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_admin = MagicMock()
            mock_topics = MagicMock()
            mock_topics.topics = {}
            mock_admin.list_topics.return_value = mock_topics
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                with self.assertRaises(ValueError):
                    self.integration.describe_topic("nonexistent-topic")

    def test_update_topic_config(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            with patch('src.workflow_kafka.ConfigResource') as mock_config_resource:
                with patch('src.workflow_kafka.ConfigResourceType') as mock_config_resource_type:
                    mock_config_resource_type.TOPIC = "TOPIC"
                    mock_admin = MagicMock()
                    mock_admin_client_class.return_value = mock_admin

                    with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                        result = self.integration.update_topic_config("test-topic", {"retention.ms": "3600000"})

                    self.assertTrue(result)
                    mock_admin.alter_configs.assert_called_once()


class TestKafkaIntegrationProducer(unittest.TestCase):
    """Test KafkaIntegration producer methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_create_producer(self):
        with patch('src.workflow_kafka.KafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = MagicMock()

            producer = self.integration.create_producer()

            self.assertIsNotNone(producer)
            self.assertEqual(self.integration._producer, producer)

    def test_create_producer_returns_existing(self):
        existing_producer = MagicMock()
        self.integration._producer = existing_producer

        producer = self.integration.create_producer()

        self.assertEqual(producer, existing_producer)

    def test_produce_dict_message(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        with patch.object(self.integration, 'create_producer', return_value=mock_producer):
            result = self.integration.produce("test-topic", {"key": "value"})

        self.assertTrue(result)
        mock_producer.send.assert_called()

    def test_produce_string_message(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        with patch.object(self.integration, 'create_producer', return_value=mock_producer):
            result = self.integration.produce("test-topic", "hello world")

        self.assertTrue(result)

    def test_produce_with_key_and_headers(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        with patch.object(self.integration, 'create_producer', return_value=mock_producer):
            result = self.integration.produce(
                "test-topic",
                "message",
                key="my-key",
                headers={"header1": "value1"}
            )

        self.assertTrue(result)

    def test_produce_batch(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future

        messages = [
            {"value": "msg1", "key": "key1"},
            {"value": "msg2", "key": "key2"}
        ]

        with patch.object(self.integration, 'create_producer', return_value=mock_producer):
            results = self.integration.produce_batch("test-topic", messages)

        self.assertEqual(len(results), 2)

    def test_flush_without_producer(self):
        self.integration._producer = None
        self.integration.flush()
        self.assertIsNone(self.integration._producer)

    def test_flush_with_producer(self):
        mock_producer = MagicMock()
        self.integration._producer = mock_producer

        self.integration.flush(timeout=5.0)

        mock_producer.flush.assert_called_with(timeout=5.0)


class TestKafkaIntegrationConsumer(unittest.TestCase):
    """Test KafkaIntegration consumer methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_create_consumer(self):
        with patch('src.workflow_kafka.KafkaConsumer') as mock_consumer_class:
            mock_consumer_class.return_value = MagicMock()

            consumer = self.integration.create_consumer("consumer-1")

            self.assertIsNotNone(consumer)
            self.assertIn("consumer-1", self.integration._consumers)

    def test_create_consumer_with_config(self):
        with patch('src.workflow_kafka.KafkaConsumer') as mock_consumer_class:
            mock_consumer_class.return_value = MagicMock()
            config = ConsumerConfig(group_id="custom-group")

            consumer = self.integration.create_consumer("consumer-1", config)

            mock_consumer_class.assert_called_once()
            call_kwargs = mock_consumer_class.call_args[1]
            self.assertEqual(call_kwargs['group_id'], "custom-group")

    def test_subscribe_to_topic(self):
        mock_consumer = MagicMock()
        self.integration._consumers["consumer-1"] = mock_consumer

        result = self.integration.subscribe("consumer-1", ["topic1", "topic2"])

        self.assertTrue(result)
        mock_consumer.subscribe.assert_called_once()

    def test_subscribe_consumer_not_found(self):
        with self.assertRaises(ValueError):
            self.integration.subscribe("nonexistent", ["topic1"])

    def test_subscribe_with_pattern(self):
        mock_consumer = MagicMock()
        self.integration._consumers["consumer-1"] = mock_consumer

        result = self.integration.subscribe("consumer-1", topics=[], pattern="topic.*")

        self.assertTrue(result)
        mock_consumer.subscribe.assert_called_with(pattern="topic.*")

    def test_consume_messages(self):
        mock_record = MagicMock()
        mock_record.topic = "test-topic"
        mock_record.partition = 0
        mock_record.offset = 0
        mock_record.key = b"key"
        mock_record.value = b"value"
        mock_record.timestamp = 1234567890
        mock_record.timestamp_type = 0
        mock_record.headers = {}

        mock_tp = MagicMock()
        mock_tp.items.return_value = [(MagicMock(), [mock_record])]

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_tp

        self.integration._consumers["consumer-1"] = mock_consumer

        messages = self.integration.consume("consumer-1", timeout=1.0)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["topic"], "test-topic")

    def test_consume_consumer_not_found(self):
        with self.assertRaises(ValueError):
            self.integration.consume("nonexistent", timeout=1.0)

    def test_stop_consumer(self):
        mock_consumer = MagicMock()
        self.integration._consumers["consumer-1"] = mock_consumer
        self.integration._running_consumers.add("consumer-1")

        self.integration.stop_consumer("consumer-1")

        self.assertNotIn("consumer-1", self.integration._running_consumers)
        mock_consumer.close.assert_called_once()


class TestKafkaIntegrationConsumerGroups(unittest.TestCase):
    """Test KafkaIntegration consumer group methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_list_consumer_groups(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_group = MagicMock()
            mock_group.group_id = "group1"
            mock_group.state = "Stable"
            mock_group.protocol = "range"

            mock_admin = MagicMock()
            mock_admin.list_consumer_groups.return_value = [mock_group]
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                groups = self.integration.list_consumer_groups()

            self.assertEqual(len(groups), 1)
            self.assertEqual(groups[0]["group_id"], "group1")

    def test_describe_consumer_group(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_member = MagicMock()
            mock_member.member_id = "member1"
            mock_member.client_id = "client1"
            mock_member.host = "localhost"
            mock_member.assignment = MagicMock()

            mock_desc = MagicMock()
            mock_desc.group_id = "group1"
            mock_desc.state = "Stable"
            mock_desc.protocol = "range"
            mock_desc.protocol_type = "consumer"
            mock_desc.members = [mock_member]

            mock_admin = MagicMock()
            mock_admin.describe_consumer_groups.return_value = {"group1": mock_desc}
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                result = self.integration.describe_consumer_group("group1")

            self.assertEqual(result["group_id"], "group1")
            self.assertEqual(len(result["members"]), 1)

    def test_delete_consumer_group(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_admin = MagicMock()
            mock_admin_client_class.return_value = mock_admin

            with patch.object(self.integration, '_get_admin_client', return_value=mock_admin):
                result = self.integration.delete_consumer_group("group1")

            self.assertTrue(result)
            mock_admin.delete_consumer_groups.assert_called_once_with(["group1"])


class TestKafkaIntegrationSchemaRegistry(unittest.TestCase):
    """Test KafkaIntegration schema registry methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = "http://localhost:8081"
        self.integration.use_confluent = True
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_create_schema_registry_client(self):
        with patch('src.workflow_kafka.SchemaRegistryClient') as mock_sr_class:
            mock_sr_client = MagicMock()
            mock_sr_class.return_value = mock_sr_client

            client = self.integration.create_schema_registry_client()

            self.assertIsNotNone(client)
            mock_sr_class.assert_called_once()

    def test_create_schema_registry_client_no_url(self):
        self.integration.schema_registry_url = None

        with self.assertRaises(ValueError):
            self.integration.create_schema_registry_client()

    def test_register_schema(self):
        with patch('src.workflow_kafka.SchemaRegistryClient') as mock_sr_class:
            mock_sr_client = MagicMock()
            mock_sr_client.register.return_value = 1
            mock_sr_class.return_value = mock_sr_client
            self.integration._schema_registry_client = mock_sr_client

            schema_id = self.integration.register_schema(
                subject="test-value",
                schema_str='{"type": "record", "name": "Test"}',
                schema_type="AVRO"
            )

            self.assertEqual(schema_id, 1)


class TestKafkaIntegrationMonitoring(unittest.TestCase):
    """Test KafkaIntegration monitoring methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = MagicMock()
        self.integration._schema_registry_client = None
        self.integration._consumers = {"consumer-1": MagicMock()}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_collect_metrics(self):
        metrics = self.integration.collect_metrics()

        self.assertIsInstance(metrics, list)
        self.assertTrue(len(metrics) > 0)

    def test_health_check_success(self):
        with patch.object(self.integration, 'list_topics', return_value=["topic1"]):
            health = self.integration.health_check()

        self.assertTrue(health["healthy"])
        self.assertEqual(health["bootstrap_servers"], "localhost:9092")

    def test_health_check_failure(self):
        with patch.object(self.integration, 'list_topics', side_effect=Exception("Connection failed")):
            health = self.integration.health_check()

        self.assertFalse(health["healthy"])
        self.assertIn("error", health)

    def test_get_broker_metrics(self):
        metrics = self.integration.get_broker_metrics()

        self.assertIn("under_replicated_partitions", metrics)
        self.assertIn("offline_partitions", metrics)
        self.assertIn("timestamp", metrics)


class TestKafkaIntegrationUtility(unittest.TestCase):
    """Test KafkaIntegration utility methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = {"consumer-1"}
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_close_cleans_up_consumers(self):
        mock_consumer = MagicMock()
        self.integration._consumers["consumer-1"] = mock_consumer

        self.integration.close()

        mock_consumer.close.assert_called()

    def test_context_manager(self):
        with patch.object(self.integration, 'close'):
            with self.integration as ctx:
                pass

    def test_get_admin_client(self):
        with patch('src.workflow_kafka.KafkaAdminClient') as mock_admin_client_class:
            mock_admin = MagicMock()
            mock_admin_client_class.return_value = mock_admin

            admin = self.integration._get_admin_client()

            self.assertIsNotNone(admin)
            mock_admin_client_class.assert_called_once()


class TestKafkaIntegrationKSQL(unittest.TestCase):
    """Test KafkaIntegration KSQL methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    @patch('requests.post')
    def test_execute_ksql_query(self, mock_post):
        mock_response = MagicMock()
        mock_response.text = '{"rows":[{"COL1":"value1"}]}\n'
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        results = self.integration.execute_ksql_query("SELECT * FROM test;")

        self.assertIsInstance(results, list)
        mock_post.assert_called_once()

    @patch('requests.post')
    def test_create_ksql_stream(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        result = self.integration.create_ksql_stream(
            stream_name="test_stream",
            columns={"id": "INT", "name": "STRING"},
            topic="test_topic"
        )

        self.assertTrue(result)


class TestKafkaIntegrationConnector(unittest.TestCase):
    """Test KafkaIntegration Kafka Connect methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    @patch('requests.post')
    def test_create_connector(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {"name": "test-connector", "state": "RUNNING"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        config = ConnectorConfig(name="test-connector", connector_class="TestConnector")

        result = self.integration.create_connector(config)

        self.assertEqual(result["name"], "test-connector")

    @patch('requests.get')
    def test_get_connector_status(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"name": "test-connector", "state": "RUNNING"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        status = self.integration.get_connector_status("test-connector")

        self.assertEqual(status["state"], "RUNNING")

    @patch('requests.put')
    def test_pause_connector(self, mock_put):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_put.return_value = mock_response

        result = self.integration.pause_connector("test-connector")

        self.assertTrue(result)

    @patch('requests.put')
    def test_resume_connector(self, mock_put):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_put.return_value = mock_response

        result = self.integration.resume_connector("test-connector")

        self.assertTrue(result)

    @patch('requests.delete')
    def test_delete_connector(self, mock_delete):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_delete.return_value = mock_response

        result = self.integration.delete_connector("test-connector")

        self.assertTrue(result)


class TestKafkaIntegrationMirrorMaker(unittest.TestCase):
    """Test KafkaIntegration MirrorMaker methods"""

    def setUp(self):
        self.integration = KafkaIntegration.__new__(KafkaIntegration)
        self.integration.bootstrap_servers = "localhost:9092"
        self.integration.schema_registry_url = None
        self.integration.use_confluent = False
        self.integration._admin_client = None
        self.integration._producer = None
        self.integration._schema_registry_client = None
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running_consumers = set()
        self.integration._streams_instances = {}
        self.integration._ksql_clients = {}
        self.integration._metrics = []
        self.integration._metrics_lock = __import__('threading').RLock()
        self.integration._lock = __import__('threading').RLock()

    def test_create_mirror_maker(self):
        config = MirrorMakerConfig(
            source_cluster={"bootstrap_servers": "source:9092"},
            target_cluster={"bootstrap_servers": "target:9092"},
            topics=["topic1"]
        )

        result = self.integration.create_mirror_maker("mm1", config)

        self.assertEqual(result["id"], "mm1")
        self.assertIn("config", result)

    def test_start_mirror_maker(self):
        result = self.integration.start_mirror_maker("mm1")
        self.assertTrue(result)

    def test_check_mirror_maker_health(self):
        health = self.integration.check_mirror_maker_health("mm1")

        self.assertEqual(health["mirror_maker_id"], "mm1")
        self.assertEqual(health["status"], "running")


if __name__ == "__main__":
    unittest.main()
