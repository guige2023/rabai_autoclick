"""
Tests for workflow_activemq module - testing actual implementation
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

# Create mock stomp module before importing workflow_activemq
mock_stomp = types.ModuleType('stomp')
mock_stomp.Connection = MagicMock()

# Create mock exception module
mock_stomp_exception = types.ModuleType('stomp.exception')
mock_stomp_exception.StompConnectionError = Exception
mock_stomp_exception.StompDataError = Exception
mock_stomp.exception = mock_stomp_exception

sys.modules['stomp'] = mock_stomp
sys.modules['stomp.exception'] = mock_stomp_exception

# Now we can import the module
from src.workflow_activemq import (
    ActiveMQIntegration,
    ActiveMQConnection,
    ActiveMQMessage,
    AckMode,
    DestinationType,
    MessageDeliveryMode,
    MessagePriority,
    BrokerConfig,
    QueueConfig,
    TopicConfig,
    ProducerConfig,
    ConsumerConfig,
    NetworkConnectorConfig,
    SecurityConfig,
    PersistenceConfig,
    AdvisoryConfig,
)


class TestAckMode(unittest.TestCase):
    """Test AckMode enum"""

    def test_ack_mode_values(self):
        self.assertEqual(AckMode.AUTO.value, "auto")
        self.assertEqual(AckMode.CLIENT.value, "client")
        self.assertEqual(AckMode.INDIVIDUAL.value, "individual")


class TestDestinationType(unittest.TestCase):
    """Test DestinationType enum"""

    def test_destination_type_values(self):
        self.assertEqual(DestinationType.QUEUE.value, "queue")
        self.assertEqual(DestinationType.TOPIC.value, "topic")
        self.assertEqual(DestinationType.TEMP_QUEUE.value, "temp-queue")
        self.assertEqual(DestinationType.TEMP_TOPIC.value, "temp-topic")


class TestMessageDeliveryMode(unittest.TestCase):
    """Test MessageDeliveryMode enum"""

    def test_message_delivery_mode_values(self):
        self.assertEqual(MessageDeliveryMode.NON_PERSISTENT.value, 1)
        self.assertEqual(MessageDeliveryMode.PERSISTENT.value, 2)


class TestMessagePriority(unittest.TestCase):
    """Test MessagePriority enum"""

    def test_message_priority_values(self):
        self.assertEqual(MessagePriority.LOWEST.value, 0)
        self.assertEqual(MessagePriority.LOW.value, 2)
        self.assertEqual(MessagePriority.NORMAL.value, 4)
        self.assertEqual(MessagePriority.HIGH.value, 7)
        self.assertEqual(MessagePriority.HIGHEST.value, 9)


class TestBrokerConfig(unittest.TestCase):
    """Test BrokerConfig dataclass"""

    def test_broker_config_defaults(self):
        config = BrokerConfig()
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 61613)
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "admin")
        self.assertEqual(config.virtual_host, "/")
        self.assertFalse(config.ssl)
        self.assertEqual(config.timeout, 30.0)
        self.assertEqual(config.heartbeat, 60000)
        self.assertEqual(config.reconnect_attempts, 10)
        self.assertEqual(config.reconnect_delay, 1.0)

    def test_broker_config_custom(self):
        config = BrokerConfig(
            host="broker.example.com",
            port=61614,
            username="user",
            password="pass",
            ssl=True
        )
        self.assertEqual(config.host, "broker.example.com")
        self.assertEqual(config.port, 61614)
        self.assertEqual(config.username, "user")
        self.assertEqual(config.password, "pass")
        self.assertTrue(config.ssl)


class TestQueueConfig(unittest.TestCase):
    """Test QueueConfig dataclass"""

    def test_queue_config_required(self):
        config = QueueConfig(name="test-queue")
        self.assertEqual(config.name, "test-queue")
        self.assertTrue(config.durable)
        self.assertFalse(config.auto_delete)
        self.assertFalse(config.exclusive)
        self.assertEqual(config.maxConsumers, -1)
        self.assertEqual(config.maxMessages, -1)
        self.assertEqual(config.selector, "")
        self.assertEqual(config.arguments, {})

    def test_queue_config_custom(self):
        config = QueueConfig(
            name="custom-queue",
            durable=False,
            auto_delete=True,
            exclusive=True,
            maxConsumers=10,
            selector="priority=high"
        )
        self.assertEqual(config.name, "custom-queue")
        self.assertFalse(config.durable)
        self.assertTrue(config.auto_delete)
        self.assertTrue(config.exclusive)
        self.assertEqual(config.maxConsumers, 10)
        self.assertEqual(config.selector, "priority=high")


class TestTopicConfig(unittest.TestCase):
    """Test TopicConfig dataclass"""

    def test_topic_config_required(self):
        config = TopicConfig(name="test-topic")
        self.assertEqual(config.name, "test-topic")
        self.assertTrue(config.durable)
        self.assertFalse(config.auto_delete)
        self.assertEqual(config.maxSubscribers, -1)
        self.assertEqual(config.selector, "")
        self.assertEqual(config.arguments, {})


class TestProducerConfig(unittest.TestCase):
    """Test ProducerConfig dataclass"""

    def test_producer_config_defaults(self):
        config = ProducerConfig(destination="test-queue")
        self.assertEqual(config.destination, "test-queue")
        self.assertEqual(config.destination_type, DestinationType.QUEUE)
        self.assertEqual(config.delivery_mode, MessageDeliveryMode.PERSISTENT)
        self.assertEqual(config.priority, MessagePriority.NORMAL)
        self.assertEqual(config.expiration, 0)
        self.assertTrue(config.timestamp)
        self.assertEqual(config.properties, {})


class TestConsumerConfig(unittest.TestCase):
    """Test ConsumerConfig dataclass"""

    def test_consumer_config_defaults(self):
        config = ConsumerConfig(destination="test-queue")
        self.assertEqual(config.destination, "test-queue")
        self.assertEqual(config.destination_type, DestinationType.QUEUE)
        self.assertEqual(config.ack_mode, AckMode.AUTO)
        self.assertEqual(config.selector, "")
        self.assertEqual(config.id, "")
        self.assertFalse(config.durable)
        self.assertEqual(config.subscription_name, "")
        self.assertEqual(config.max_pending_messages, 0)
        self.assertEqual(config.arguments, {})


class TestNetworkConnectorConfig(unittest.TestCase):
    """Test NetworkConnectorConfig dataclass"""

    def test_network_connector_config_defaults(self):
        config = NetworkConnectorConfig(name="connector1", uri="tcp://broker2:61616")
        self.assertEqual(config.name, "connector1")
        self.assertEqual(config.uri, "tcp://broker2:61616")
        self.assertFalse(config.duplex)
        self.assertTrue(config.decreasePriority)
        self.assertTrue(config.demandFlowControl)
        self.assertTrue(config.conduitSubscriptions)


class TestSecurityConfig(unittest.TestCase):
    """Test SecurityConfig dataclass"""

    def test_security_config_defaults(self):
        config = SecurityConfig()
        self.assertEqual(config.users, {})
        self.assertEqual(config.roles, {})
        self.assertEqual(config.plugins, [])


class TestPersistenceConfig(unittest.TestCase):
    """Test PersistenceConfig dataclass"""

    def test_persistence_config_defaults(self):
        config = PersistenceConfig()
        self.assertTrue(config.enabled)
        self.assertEqual(config.type, "kahaDB")
        self.assertEqual(config.journal_dir, "data/journal")
        self.assertEqual(config.persistence_dir, "data/kahadb")
        self.assertEqual(config.maxFileLength, 10485760)
        self.assertEqual(config.checkpointInterval, 5000)
        self.assertEqual(config.cleanupInterval, 30000)


class TestAdvisoryConfig(unittest.TestCase):
    """Test AdvisoryConfig dataclass"""

    def test_advisory_config_defaults(self):
        config = AdvisoryConfig()
        self.assertTrue(config.enabled)
        self.assertEqual(config.topic_prefix, "ActiveMQ.Advisory")
        self.assertTrue(config.connection)
        self.assertTrue(config.queue)
        self.assertTrue(config.topic)
        self.assertTrue(config.producer)
        self.assertTrue(config.consumer)
        self.assertTrue(config.slow)
        self.assertTrue(config.fast)


class TestActiveMQMessage(unittest.TestCase):
    """Test ActiveMQMessage class"""

    def test_message_creation(self):
        msg = ActiveMQMessage(body="test body", destination="test-queue")
        self.assertEqual(msg.body, "test body")
        self.assertEqual(msg.destination, "test-queue")
        self.assertIsNotNone(msg.message_id)
        self.assertIsNotNone(msg.timestamp)
        self.assertEqual(msg.headers, {})
        self.assertEqual(msg.properties, {})

    def test_message_to_dict(self):
        msg = ActiveMQMessage(body="test body", destination="test-queue")
        msg_dict = msg.to_dict()
        self.assertIn("message_id", msg_dict)
        self.assertIn("destination", msg_dict)
        self.assertIn("body", msg_dict)
        self.assertIn("headers", msg_dict)
        self.assertIn("timestamp", msg_dict)
        self.assertIn("properties", msg_dict)

    def test_message_from_dict(self):
        data = {
            "body": "test body",
            "destination": "test-queue",
            "message_id": "test-id",
            "headers": {"key": "value"},
            "timestamp": 1234567890,
            "properties": {"prop": "value"}
        }
        msg = ActiveMQMessage.from_dict(data)
        self.assertEqual(msg.body, "test body")
        self.assertEqual(msg.destination, "test-queue")
        self.assertEqual(msg.message_id, "test-id")
        self.assertEqual(msg.headers, {"key": "value"})
        self.assertEqual(msg.timestamp, 1234567890)
        self.assertEqual(msg.properties, {"prop": "value"})


class TestActiveMQConnection(unittest.TestCase):
    """Test ActiveMQConnection class"""

    def test_connection_init(self):
        config = BrokerConfig(host="localhost", port=61613)
        conn = ActiveMQConnection(config)
        self.assertEqual(conn.config, config)
        self.assertIsNone(conn.connection)
        self.assertFalse(conn.connected)

    @patch('src.workflow_activemq.STOMP_AVAILABLE', False)
    def test_connection_without_stomp(self):
        config = BrokerConfig()
        conn = ActiveMQConnection(config)
        result = conn.connect()
        self.assertFalse(result)


class TestActiveMQIntegration(unittest.TestCase):
    """Test ActiveMQIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = BrokerConfig(host="localhost", port=61613)
        self.integration = ActiveMQIntegration(self.config)

    def test_integration_init(self):
        """Test integration initialization"""
        self.assertEqual(self.integration.config, self.config)
        self.assertIsInstance(self.integration.connection, ActiveMQConnection)
        self.assertEqual(self.integration._producers, {})
        self.assertEqual(self.integration._consumers, {})
        self.assertEqual(self.integration._queues, {})
        self.assertEqual(self.integration._topics, {})
        self.assertFalse(self.integration._running)

    def test_get_stats(self):
        """Test getting statistics"""
        stats = self.integration.get_stats()
        self.assertIn("messages_sent", stats)
        self.assertIn("messages_received", stats)
        self.assertIn("messages_acked", stats)
        self.assertIn("errors", stats)
        self.assertIn("reconnects", stats)
        self.assertIn("last_activity", stats)

    def test_reset_stats(self):
        """Test resetting statistics"""
        self.integration._stats["messages_sent"] = 100
        self.integration.reset_stats()
        stats = self.integration.get_stats()
        self.assertEqual(stats["messages_sent"], 0)
        self.assertEqual(stats["messages_received"], 0)

    def test_is_connected_initially_false(self):
        """Test is_connected returns False when not connected"""
        self.assertFalse(self.integration.is_connected())

    @patch.object(ActiveMQConnection, 'connect')
    def test_connect_success(self, mock_connect):
        """Test successful connection"""
        mock_connect.return_value = True
        result = self.integration.connect()
        self.assertTrue(result)
        self.assertTrue(self.integration._running)

    @patch.object(ActiveMQConnection, 'connect')
    def test_connect_failure(self, mock_connect):
        """Test failed connection"""
        mock_connect.return_value = False
        result = self.integration.connect()
        self.assertFalse(result)

    def test_disconnect(self):
        """Test disconnect"""
        self.integration._running = True
        self.integration.disconnect()
        self.assertFalse(self.integration._running)

    def test_get_broker_info_not_connected(self):
        """Test get_broker_info when not connected"""
        info = self.integration.get_broker_info()
        self.assertEqual(info, {"connected": False})

    @patch.object(ActiveMQIntegration, 'is_connected')
    def test_get_broker_info_connected(self, mock_connected):
        """Test get_broker_info when connected"""
        mock_connected.return_value = True
        info = self.integration.get_broker_info()
        self.assertTrue(info["connected"])
        self.assertEqual(info["host"], "localhost")
        self.assertEqual(info["port"], 61613)

    @patch.object(ActiveMQIntegration, 'is_connected')
    def test_check_broker_health(self, mock_connected):
        """Test check_broker_health"""
        mock_connected.return_value = True
        health = self.integration.check_broker_health()
        self.assertTrue(health["healthy"])
        self.assertTrue(health["connected"])
        self.assertEqual(health["host"], "localhost")
        self.assertEqual(health["port"], 61613)

    def test_create_queue(self):
        """Test queue creation"""
        result = self.integration.create_queue("test-queue")
        self.assertTrue(result)
        self.assertIn("test-queue", self.integration._queues)

    def test_create_queue_idempotent(self):
        """Test that creating same queue twice returns True"""
        self.integration.create_queue("test-queue")
        result = self.integration.create_queue("test-queue")
        self.assertTrue(result)

    def test_create_topic(self):
        """Test topic creation"""
        result = self.integration.create_topic("test-topic")
        self.assertTrue(result)
        self.assertIn("test-topic", self.integration._topics)

    @patch.object(ActiveMQIntegration, 'is_connected')
    def test_create_queue_when_connected(self, mock_connected):
        """Test queue creation when connected"""
        mock_connected.return_value = True
        with patch.object(self.integration, '_send_queue_create', return_value=True) as mock_send:
            result = self.integration.create_queue("test-queue")
            self.assertTrue(result)
            mock_send.assert_called_once()

    def test_delete_queue(self):
        """Test queue deletion"""
        self.integration._queues["test-queue"] = QueueConfig(name="test-queue")
        result = self.integration.delete_queue("test-queue")
        self.assertTrue(result)
        self.assertNotIn("test-queue", self.integration._queues)

    def test_delete_queue_not_found(self):
        """Test deleting non-existent queue"""
        result = self.integration.delete_queue("non-existent")
        self.assertFalse(result)

    def test_delete_topic(self):
        """Test topic deletion"""
        self.integration._topics["test-topic"] = TopicConfig(name="test-topic")
        result = self.integration.delete_topic("test-topic")
        self.assertTrue(result)
        self.assertNotIn("test-topic", self.integration._topics)

    def test_create_producer(self):
        """Test producer creation"""
        result = self.integration.create_producer("producer1", "test-queue")
        self.assertTrue(result)
        self.assertIn("producer1", self.integration._producers)

    def test_create_producer_already_exists(self):
        """Test creating duplicate producer"""
        self.integration.create_producer("producer1", "test-queue")
        result = self.integration.create_producer("producer1", "test-queue")
        self.assertTrue(result)  # Idempotent

    def test_delete_producer(self):
        """Test producer deletion"""
        self.integration.create_producer("producer1", "test-queue")
        result = self.integration.delete_producer("producer1")
        self.assertTrue(result)
        self.assertNotIn("producer1", self.integration._producers)

    def test_delete_producer_not_found(self):
        """Test deleting non-existent producer"""
        result = self.integration.delete_producer("non-existent")
        self.assertFalse(result)

    def test_get_producer_info(self):
        """Test getting producer info"""
        self.integration.create_producer("producer1", "test-queue")
        info = self.integration.get_producer_info("producer1")
        self.assertTrue(info["exists"])
        self.assertEqual(info["id"], "producer1")
        self.assertEqual(info["destination"], "test-queue")

    def test_get_producer_info_not_found(self):
        """Test getting info for non-existent producer"""
        info = self.integration.get_producer_info("non-existent")
        self.assertFalse(info["exists"])

    def test_list_producers(self):
        """Test listing producers"""
        self.integration.create_producer("producer1", "queue1")
        self.integration.create_producer("producer2", "queue2")
        producers = self.integration.list_producers()
        self.assertEqual(len(producers), 2)
        self.assertIn("producer1", producers)
        self.assertIn("producer2", producers)

    def test_create_consumer(self):
        """Test consumer creation"""
        result = self.integration.create_consumer("consumer1", "test-queue")
        self.assertTrue(result)
        self.assertIn("consumer1", self.integration._consumers)

    def test_create_consumer_already_exists(self):
        """Test creating duplicate consumer"""
        self.integration.create_consumer("consumer1", "test-queue")
        result = self.integration.create_consumer("consumer1", "test-queue")
        self.assertTrue(result)  # Idempotent

    def test_delete_consumer(self):
        """Test consumer deletion"""
        self.integration.create_consumer("consumer1", "test-queue")
        result = self.integration.delete_consumer("consumer1")
        self.assertTrue(result)
        self.assertNotIn("consumer1", self.integration._consumers)

    def test_delete_consumer_not_found(self):
        """Test deleting non-existent consumer"""
        result = self.integration.delete_consumer("non-existent")
        self.assertFalse(result)

    def test_get_consumer_info(self):
        """Test getting consumer info"""
        self.integration.create_consumer("consumer1", "test-queue")
        info = self.integration.get_consumer_info("consumer1")
        self.assertTrue(info["exists"])
        self.assertEqual(info["id"], "consumer1")
        self.assertEqual(info["destination"], "test-queue")

    def test_get_consumer_info_not_found(self):
        """Test getting info for non-existent consumer"""
        info = self.integration.get_consumer_info("non-existent")
        self.assertFalse(info["exists"])

    def test_list_consumers(self):
        """Test listing consumers"""
        self.integration.create_consumer("consumer1", "queue1")
        self.integration.create_consumer("consumer2", "queue2")
        consumers = self.integration.list_consumers()
        self.assertEqual(len(consumers), 2)
        self.assertIn("consumer1", consumers)
        self.assertIn("consumer2", consumers)

    @patch.object(ActiveMQConnection, 'is_connected')
    def test_send_message(self, mock_connected):
        """Test sending a message"""
        mock_connected.return_value = True
        self.integration.create_producer("producer1", "test-queue")
        # Mock _do_send_message 
        with patch.object(self.integration, '_do_send_message', return_value=True) as mock_send:
            result = self.integration.send_message("producer1", "test message")
            self.assertTrue(result)
            mock_send.assert_called_once()

    def test_send_message_not_connected(self):
        """Test sending message when not connected"""
        result = self.integration.send_message("test-queue", "test message")
        self.assertFalse(result)

    def test_reconnect(self):
        """Test reconnection"""
        self.integration._stats["reconnects"] = 0
        with patch.object(self.integration, 'disconnect'), \
             patch.object(self.integration, 'connect', return_value=True):
            result = self.integration.reconnect()
            self.assertTrue(result)
            self.assertEqual(self.integration._stats["reconnects"], 1)

    def test_error_rate_calculation(self):
        """Test error rate calculation"""
        self.integration._stats["messages_sent"] = 100
        self.integration._stats["messages_received"] = 50
        self.integration._stats["errors"] = 5
        rate = self.integration._calculate_error_rate()
        self.assertAlmostEqual(rate, 0.0333, places=3)

    def test_error_rate_no_messages(self):
        """Test error rate with no messages"""
        rate = self.integration._calculate_error_rate()
        self.assertEqual(rate, 0.0)

    def test_get_queue_info(self):
        """Test getting queue info"""
        self.integration._queues["test-queue"] = QueueConfig(name="test-queue")
        info = self.integration.get_queue_info("test-queue")
        self.assertTrue(info["exists"])
        self.assertEqual(info["name"], "test-queue")

    def test_get_queue_info_not_found(self):
        """Test getting info for non-existent queue"""
        info = self.integration.get_queue_info("non-existent")
        self.assertFalse(info["exists"])

    def test_get_topic_info(self):
        """Test getting topic info"""
        self.integration._topics["test-topic"] = TopicConfig(name="test-topic")
        info = self.integration.get_topic_info("test-topic")
        self.assertTrue(info["exists"])
        self.assertEqual(info["name"], "test-topic")

    def test_get_topic_info_not_found(self):
        """Test getting info for non-existent topic"""
        info = self.integration.get_topic_info("non-existent")
        self.assertFalse(info["exists"])

    def test_list_queues(self):
        """Test listing queues"""
        self.integration._queues["queue1"] = QueueConfig(name="queue1")
        self.integration._queues["queue2"] = QueueConfig(name="queue2")
        queues = self.integration.list_queues()
        self.assertEqual(len(queues), 2)
        self.assertIn("queue1", queues)
        self.assertIn("queue2", queues)

    def test_list_topics(self):
        """Test listing topics"""
        self.integration._topics["topic1"] = TopicConfig(name="topic1")
        self.integration._topics["topic2"] = TopicConfig(name="topic2")
        topics = self.integration.list_topics()
        self.assertEqual(len(topics), 2)
        self.assertIn("topic1", topics)
        self.assertIn("topic2", topics)

    def test_get_queue_stats(self):
        """Test getting queue stats"""
        self.integration._queues["test-queue"] = QueueConfig(name="test-queue")
        stats = self.integration.get_queue_stats("test-queue")
        self.assertNotIn("exists", stats)
        self.assertEqual(stats["name"], "test-queue")
        self.assertEqual(stats["message_count"], 0)

    def test_get_queue_stats_not_found(self):
        """Test getting stats for non-existent queue"""
        stats = self.integration.get_queue_stats("non-existent")
        self.assertFalse(stats["exists"])

    def test_get_topic_stats(self):
        """Test getting topic stats"""
        self.integration._topics["test-topic"] = TopicConfig(name="test-topic")
        stats = self.integration.get_topic_stats("test-topic")
        self.assertNotIn("exists", stats)
        self.assertEqual(stats["name"], "test-topic")

    def test_get_topic_stats_not_found(self):
        """Test getting stats for non-existent topic"""
        stats = self.integration.get_topic_stats("non-existent")
        self.assertFalse(stats["exists"])

    def test_configure_security(self):
        """Test security configuration"""
        security = SecurityConfig(
            users={"admin": {"password": "secret_password"}},
            roles={"admin": ["read", "write"]}
        )
        result = self.integration.configure_security(security)
        self.assertTrue(result)
        # The actual implementation stores the config as-is
        self.assertEqual(self.integration._security.users["admin"]["password"], "secret_password")

    def test_configure_persistence(self):
        """Test persistence configuration"""
        persistence = PersistenceConfig(enabled=True, type="kahaDB")
        result = self.integration.configure_persistence(persistence)
        self.assertTrue(result)
        self.assertEqual(self.integration._persistence.type, "kahaDB")

    def test_configure_advisory(self):
        """Test advisory configuration"""
        advisory = AdvisoryConfig(enabled=False)
        result = self.integration.setup_advisories(advisory)
        self.assertTrue(result)
        self.assertFalse(self.integration._advisory.enabled)

    @patch.object(ActiveMQConnection, 'is_connected')
    def test_purge_queue(self, mock_connected):
        """Test purging a queue when connected"""
        mock_connected.return_value = True
        self.integration._queues["test-queue"] = QueueConfig(name="test-queue")
        # Mock the connection.send to avoid actual network call
        self.integration.connection.connection = MagicMock()
        result = self.integration.purge_queue("test-queue")
        self.assertTrue(result)

    def test_purge_queue_not_found(self):
        """Test purging non-existent queue"""
        result = self.integration.purge_queue("non-existent")
        self.assertFalse(result)

    def test_register_message_handler(self):
        """Test registering a message handler"""
        self.integration.create_consumer("consumer1", "test-queue")
        callback = MagicMock()
        result = self.integration.register_message_handler("consumer1", callback)
        self.assertTrue(result)
        self.assertEqual(self.integration._message_listeners["consumer1"], callback)

    def test_register_message_handler_consumer_not_found(self):
        """Test registering handler for non-existent consumer"""
        callback = MagicMock()
        result = self.integration.register_message_handler("non-existent", callback)
        self.assertFalse(result)

    def test_add_network_connector(self):
        """Test adding a network connector"""
        config = NetworkConnectorConfig(name="conn1", uri="tcp://broker2:61616")
        result = self.integration.add_network_connector(config)
        self.assertTrue(result)
        self.assertIn("conn1", self.integration._network_connectors)

    def test_remove_network_connector(self):
        """Test removing a network connector"""
        config = NetworkConnectorConfig(name="conn1", uri="tcp://broker2:61616")
        self.integration.add_network_connector(config)
        result = self.integration.remove_network_connector("conn1")
        self.assertTrue(result)
        self.assertNotIn("conn1", self.integration._network_connectors)

    def test_remove_network_connector_not_found(self):
        """Test removing non-existent network connector"""
        result = self.integration.remove_network_connector("non-existent")
        self.assertFalse(result)

    def test_get_network_connector_info(self):
        """Test getting network connector info"""
        config = NetworkConnectorConfig(name="conn1", uri="tcp://broker2:61616")
        self.integration.add_network_connector(config)
        info = self.integration.get_network_connector_info("conn1")
        self.assertIsNotNone(info)
        self.assertEqual(info["name"], "conn1")
        self.assertEqual(info["uri"], "tcp://broker2:61616")

    def test_get_network_connector_info_not_found(self):
        """Test getting info for non-existent network connector"""
        info = self.integration.get_network_connector_info("non-existent")
        self.assertIsNone(info)

    def test_list_network_connectors(self):
        """Test listing network connectors"""
        self.integration.add_network_connector(NetworkConnectorConfig(name="conn1", uri="tcp://broker1:61616"))
        self.integration.add_network_connector(NetworkConnectorConfig(name="conn2", uri="tcp://broker2:61616"))
        connectors = self.integration.list_network_connectors()
        self.assertEqual(len(connectors), 2)
        self.assertIn("conn1", connectors)
        self.assertIn("conn2", connectors)

    def test_get_network_info(self):
        """Test getting network info"""
        self.integration.add_network_connector(NetworkConnectorConfig(name="conn1", uri="tcp://broker1:61616"))
        info = self.integration.get_network_info()
        self.assertEqual(info["connector_count"], 1)
        self.assertEqual(len(info["connectors"]), 1)

    def test_get_persistence_info(self):
        """Test getting persistence info"""
        info = self.integration.get_persistence_info()
        self.assertTrue(info["enabled"])
        self.assertEqual(info["type"], "kahaDB")

    def test_set_persistence_enabled(self):
        """Test enabling/disabling persistence"""
        result = self.integration.set_persistence_enabled(False)
        self.assertTrue(result)
        self.assertFalse(self.integration._persistence.enabled)

    def test_get_destination_stats(self):
        """Test getting destination stats"""
        self.integration._queues["q1"] = QueueConfig(name="q1")
        self.integration._topics["t1"] = TopicConfig(name="t1")
        stats = self.integration.get_destination_stats()
        self.assertIn("queues", stats)
        self.assertIn("topics", stats)
        self.assertEqual(len(stats["queues"]), 1)
        self.assertEqual(len(stats["topics"]), 1)

    def test_health_check(self):
        """Test health check"""
        health = self.integration.health_check()
        self.assertIn("connected", health)
        self.assertIn("broker_info", health)
        self.assertIn("health", health)
        self.assertIn("stats", health)

    def test_add_user(self):
        """Test adding a user"""
        result = self.integration.add_user("testuser", "testpass", ["group1"])
        self.assertTrue(result)
        self.assertIn("testuser", self.integration._security.users)
        self.assertEqual(self.integration._security.users["testuser"]["password"], "testpass")

    def test_remove_user(self):
        """Test removing a user"""
        self.integration.add_user("testuser", "testpass", ["group1"])
        result = self.integration.remove_user("testuser")
        self.assertTrue(result)
        self.assertNotIn("testuser", self.integration._security.users)

    def test_remove_user_not_found(self):
        """Test removing non-existent user"""
        result = self.integration.remove_user("non-existent")
        self.assertFalse(result)

    def test_get_user_info(self):
        """Test getting user info"""
        self.integration.add_user("testuser", "testpass", ["group1"])
        info = self.integration.get_user_info("testuser")
        self.assertIsNotNone(info)
        self.assertEqual(info["username"], "testuser")
        self.assertIn("group1", info["groups"])

    def test_get_user_info_not_found(self):
        """Test getting info for non-existent user"""
        info = self.integration.get_user_info("non-existent")
        self.assertIsNone(info)

    def test_list_users(self):
        """Test listing users"""
        self.integration.add_user("user1", "pass1")
        self.integration.add_user("user2", "pass2")
        users = self.integration.list_users()
        self.assertEqual(len(users), 2)
        self.assertIn("user1", users)
        self.assertIn("user2", users)

    def test_add_role(self):
        """Test adding a role"""
        result = self.integration.add_role("admin", ["read", "write", "delete"])
        self.assertTrue(result)
        self.assertIn("admin", self.integration._security.roles)
        self.assertEqual(self.integration._security.roles["admin"], ["read", "write", "delete"])

    def test_get_role_info(self):
        """Test getting role info"""
        self.integration.add_role("admin", ["read", "write"])
        self.integration.add_user("testuser", "testpass", ["admin"])
        info = self.integration.get_role_info("admin")
        self.assertIsNotNone(info)
        self.assertEqual(info["role"], "admin")
        self.assertIn("testuser", info["members"])

    def test_get_role_info_not_found(self):
        """Test getting info for non-existent role"""
        info = self.integration.get_role_info("non-existent")
        self.assertIsNone(info)

    def test_list_roles(self):
        """Test listing roles"""
        self.integration.add_role("role1", ["read"])
        self.integration.add_role("role2", ["write"])
        roles = self.integration.list_roles()
        self.assertEqual(len(roles), 2)
        self.assertIn("role1", roles)
        self.assertIn("role2", roles)

    def test_get_security_info(self):
        """Test getting security info"""
        self.integration.add_user("testuser", "testpass")
        self.integration.add_role("admin", ["read"])
        info = self.integration.get_security_info()
        self.assertEqual(info["user_count"], 1)
        self.assertEqual(info["role_count"], 1)

    def test_setup_advisories(self):
        """Test setting up advisories"""
        advisory = AdvisoryConfig(enabled=True, connection=False)
        result = self.integration.setup_advisories(advisory)
        self.assertTrue(result)
        self.assertFalse(self.integration._advisory.connection)
        self.assertTrue(self.integration._advisory.enabled)

    def test_get_advisory_info(self):
        """Test getting advisory info"""
        info = self.integration.get_advisory_info()
        self.assertIn("enabled", info)
        self.assertIn("topic_prefix", info)
        self.assertTrue(info["enabled"])

    def test_register_advisory_handler(self):
        """Test registering advisory handler"""
        callback = MagicMock()
        self.integration.register_advisory_handler(callback)
        self.assertEqual(len(self.integration._advisory_listeners), 1)

    def test_send_to_destination(self):
        """Test send_to_destination method"""
        with patch.object(self.integration, '_do_send_message', return_value=True) as mock_send:
            result = self.integration.send_to_destination(
                destination="test-queue",
                destination_type=DestinationType.QUEUE,
                body="test message"
            )
            self.assertTrue(result)
            mock_send.assert_called_once()

    def test_receive_message(self):
        """Test receiving a message"""
        # Create a test message and put it in pending
        msg = ActiveMQMessage(body="test", destination="test-queue")
        self.integration._pending_messages.put(msg)
        received = self.integration.receive_message("consumer1", timeout=0.1)
        self.assertIsNotNone(received)
        self.assertEqual(received.body, "test")

    def test_receive_message_empty(self):
        """Test receiving from empty queue"""
        received = self.integration.receive_message("consumer1", timeout=0.1)
        self.assertIsNone(received)

    def test_ack_message(self):
        """Test acknowledging a message"""
        self.integration.create_consumer("consumer1", "test-queue")
        self.integration.connection.connection = MagicMock()
        result = self.integration.ack_message("consumer1", "msg-id-123")
        self.assertTrue(result)

    def test_ack_message_consumer_not_found(self):
        """Test acking with non-existent consumer"""
        result = self.integration.ack_message("non-existent", "msg-id")
        self.assertFalse(result)

    def test_cleanup(self):
        """Test cleanup method"""
        self.integration._running = True
        self.integration.create_producer("p1", "q1")
        self.integration.create_consumer("c1", "q1")
        self.integration.cleanup()
        self.assertFalse(self.integration._running)
        self.assertEqual(len(self.integration._producers), 0)
        self.assertEqual(len(self.integration._consumers), 0)


class TestActiveMQIntegrationAsync(unittest.TestCase):
    """Test async functionality in ActiveMQIntegration"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = BrokerConfig()
        self.integration = ActiveMQIntegration(self.config)

    def test_integration_with_default_config(self):
        """Test integration with default config"""
        integration = ActiveMQIntegration()
        self.assertIsNotNone(integration.config)
        self.assertEqual(integration.config.host, "localhost")


if __name__ == '__main__':
    unittest.main()
