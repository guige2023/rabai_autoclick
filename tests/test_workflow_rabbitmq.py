"""
Tests for workflow_rabbitmq module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from queue import Queue, Empty
import types

# Create mock pika module before importing workflow_rabbitmq
mock_pika = types.ModuleType('pika')
mock_pika.BlockingConnection = MagicMock()
mock_pika.PlainCredentials = MagicMock()
mock_pika.ConnectionParameters = MagicMock()
mock_pika.BasicProperties = MagicMock()

# Create mock exceptions module
mock_exceptions = types.ModuleType('pika.exceptions')
mock_exceptions.AMQPConnectionError = Exception
mock_exceptions.AMQPChannelError = Exception
mock_exceptions.ChannelClosedByBroker = Exception
mock_exceptions.ConnectionClosedByBroker = Exception
mock_pika.exceptions = mock_exceptions

sys.modules['pika'] = mock_pika
sys.modules['pika.exceptions'] = mock_exceptions

# Now we can import the module
from src.workflow_rabbitmq import (
    RabbitMQIntegration,
    ExchangeType,
    QueueMode,
    MessagePriority,
    QueueConfig,
    ExchangeConfig,
    BindingConfig,
    MessageProperties,
    Message,
    DeadLetterConfig,
    ClusterNode,
)


class TestExchangeType(unittest.TestCase):
    """Test ExchangeType enum"""

    def test_exchange_type_values(self):
        self.assertEqual(ExchangeType.DIRECT.value, "direct")
        self.assertEqual(ExchangeType.FANOUT.value, "fanout")
        self.assertEqual(ExchangeType.TOPIC.value, "topic")
        self.assertEqual(ExchangeType.HEADERS.value, "headers")


class TestQueueMode(unittest.TestCase):
    """Test QueueMode enum"""

    def test_queue_mode_values(self):
        self.assertEqual(QueueMode.CLASSIC.value, "classic")
        self.assertEqual(QueueMode.QUORUM.value, "quorum")


class TestMessagePriority(unittest.TestCase):
    """Test MessagePriority enum"""

    def test_message_priority_values(self):
        self.assertEqual(MessagePriority.LOW.value, 0)
        self.assertEqual(MessagePriority.NORMAL.value, 5)
        self.assertEqual(MessagePriority.HIGH.value, 10)


class TestQueueConfig(unittest.TestCase):
    """Test QueueConfig dataclass"""

    def test_queue_config_defaults(self):
        config = QueueConfig(name="test-queue")
        self.assertEqual(config.name, "test-queue")
        self.assertTrue(config.durable)
        self.assertFalse(config.exclusive)
        self.assertFalse(config.auto_delete)
        self.assertEqual(config.mode, QueueMode.CLASSIC)

    def test_queue_config_custom(self):
        config = QueueConfig(
            name="custom-queue",
            durable=False,
            exclusive=True,
            mode=QueueMode.QUORUM,
            arguments={"x-max-length": 1000}
        )
        self.assertEqual(config.durable, False)
        self.assertEqual(config.exclusive, True)
        self.assertEqual(config.mode, QueueMode.QUORUM)
        self.assertEqual(config.arguments["x-max-length"], 1000)


class TestExchangeConfig(unittest.TestCase):
    """Test ExchangeConfig dataclass"""

    def test_exchange_config_defaults(self):
        config = ExchangeConfig(name="test-exchange")
        self.assertEqual(config.name, "test-exchange")
        self.assertEqual(config.exchange_type, ExchangeType.DIRECT)
        self.assertTrue(config.durable)
        self.assertFalse(config.auto_delete)

    def test_exchange_config_custom(self):
        config = ExchangeConfig(
            name="custom-exchange",
            exchange_type=ExchangeType.FANOUT,
            auto_delete=True
        )
        self.assertEqual(config.exchange_type, ExchangeType.FANOUT)
        self.assertTrue(config.auto_delete)


class TestBindingConfig(unittest.TestCase):
    """Test BindingConfig dataclass"""

    def test_binding_config_defaults(self):
        config = BindingConfig(queue="test-queue", exchange="test-exchange")
        self.assertEqual(config.queue, "test-queue")
        self.assertEqual(config.exchange, "test-exchange")
        self.assertEqual(config.routing_key, "")

    def test_binding_config_custom(self):
        config = BindingConfig(
            queue="test-queue",
            exchange="test-exchange",
            routing_key="test.key",
            arguments={"x-match": "all"}
        )
        self.assertEqual(config.routing_key, "test.key")
        self.assertEqual(config.arguments["x-match"], "all")


class TestMessageProperties(unittest.TestCase):
    """Test MessageProperties dataclass"""

    def test_message_properties_defaults(self):
        props = MessageProperties()
        self.assertEqual(props.content_type, "application/json")
        self.assertEqual(props.delivery_mode, 2)
        self.assertEqual(props.priority, 5)
        self.assertIsNone(props.message_id)
        self.assertEqual(props.headers, {})

    def test_message_properties_custom(self):
        props = MessageProperties(
            content_type="text/plain",
            priority=10,
            correlation_id="corr-123",
            reply_to="reply-queue"
        )
        self.assertEqual(props.content_type, "text/plain")
        self.assertEqual(props.priority, 10)
        self.assertEqual(props.correlation_id, "corr-123")


class TestMessage(unittest.TestCase):
    """Test Message dataclass"""

    def test_message_defaults(self):
        msg = Message(body="test body")
        self.assertEqual(msg.body, "test body")
        self.assertEqual(msg.routing_key, "")
        self.assertIsNone(msg.delivery_tag)
        self.assertFalse(msg.redelivered)

    def test_message_with_properties(self):
        props = MessageProperties(correlation_id="corr-123")
        msg = Message(
            body={"key": "value"},
            routing_key="test.key",
            delivery_tag=1,
            properties=props
        )
        self.assertEqual(msg.body, {"key": "value"})
        self.assertEqual(msg.properties.correlation_id, "corr-123")


class TestDeadLetterConfig(unittest.TestCase):
    """Test DeadLetterConfig dataclass"""

    def test_dead_letter_config_defaults(self):
        config = DeadLetterConfig()
        self.assertEqual(config.exchange, "dlx")
        self.assertEqual(config.queue, "dlq")
        self.assertEqual(config.routing_key, "dead")
        self.assertEqual(config.max_retries, 3)

    def test_dead_letter_config_custom(self):
        config = DeadLetterConfig(
            exchange="custom-dlx",
            queue="custom-dlq",
            max_retries=5,
            ttl=60000
        )
        self.assertEqual(config.max_retries, 5)
        self.assertEqual(config.ttl, 60000)


class TestClusterNode(unittest.TestCase):
    """Test ClusterNode dataclass"""

    def test_cluster_node_defaults(self):
        node = ClusterNode(host="localhost")
        self.assertEqual(node.host, "localhost")
        self.assertEqual(node.port, 5672)
        self.assertEqual(node.node_type, "disk")
        self.assertFalse(node.is_master)

    def test_cluster_node_custom(self):
        node = ClusterNode(
            host="rabbitmq-1",
            port=5673,
            node_type="ram",
            is_master=True
        )
        self.assertEqual(node.port, 5673)
        self.assertEqual(node.node_type, "ram")
        self.assertTrue(node.is_master)


class TestRabbitMQIntegrationInit(unittest.TestCase):
    """Test RabbitMQIntegration initialization"""

    def test_init_with_defaults(self):
        integration = RabbitMQIntegration()
        self.assertEqual(integration.host, "localhost")
        self.assertEqual(integration.port, 5672)
        self.assertEqual(integration.username, "guest")
        self.assertEqual(integration.virtual_host, "/")
        self.assertFalse(integration.use_clustering)

    def test_init_with_custom_params(self):
        integration = RabbitMQIntegration(
            host="rabbitmq.example.com",
            port=5673,
            username="admin",
            password="secret",
            virtual_host="/custom",
            use_clustering=True,
            max_channels=200
        )
        self.assertEqual(integration.host, "rabbitmq.example.com")
        self.assertEqual(integration.port, 5673)
        self.assertEqual(integration.username, "admin")
        self.assertEqual(integration.virtual_host, "/custom")
        self.assertTrue(integration.use_clustering)
        self.assertEqual(integration.max_channels, 200)

    def test_init_stores_state(self):
        integration = RabbitMQIntegration()
        self.assertIsNone(integration._connection)
        self.assertEqual(integration._channels, {})
        self.assertEqual(integration._exchanges, set())
        self.assertEqual(integration._queues, set())
        self.assertEqual(integration._bindings, [])


class TestRabbitMQIntegrationConnection(unittest.TestCase):
    """Test RabbitMQIntegration connection management"""

    def test_connect_success(self):
        mock_connection = MagicMock()
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = None

        with patch('src.workflow_rabbitmq.pika.BlockingConnection', return_value=mock_connection):
            with patch.object(integration, '_setup_dead_letter_queue'):
                result = integration.connect()

        self.assertTrue(result)
        self.assertEqual(integration._connection, mock_connection)

    def test_connect_already_connected(self):
        mock_connection = MagicMock()
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        result = integration.connect()

        self.assertTrue(result)

    def test_connect_failure(self):
        from pika.exceptions import AMQPConnectionError

        integration = RabbitMQIntegration()
        integration._connection = None

        with patch('src.workflow_rabbitmq.pika.BlockingConnection', side_effect=AMQPConnectionError("Connection refused")):
            result = integration.connect()

        self.assertFalse(result)

    def test_disconnect(self):
        integration = RabbitMQIntegration()
        mock_connection = MagicMock()
        mock_connection.is_open = True
        integration._connection = mock_connection

        integration.disconnect()

        mock_connection.close.assert_called_once()
        self.assertIsNone(integration._connection)

    def test_is_connected_true(self):
        integration = RabbitMQIntegration()
        mock_connection = MagicMock()
        mock_connection.is_open = True
        integration._connection = mock_connection

        self.assertTrue(integration.is_connected())

    def test_is_connected_false(self):
        integration = RabbitMQIntegration()
        integration._connection = None

        self.assertFalse(integration.is_connected())

    def test_reconnect(self):
        mock_connection = MagicMock()
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = MagicMock()

        with patch.object(integration, 'disconnect') as mock_disconnect:
            with patch.object(integration, 'connect', return_value=True) as mock_connect:
                result = integration.reconnect()

        mock_disconnect.assert_called_once()
        mock_connect.assert_called_once()


class TestRabbitMQIntegrationChannel(unittest.TestCase):
    """Test RabbitMQIntegration channel management"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_create_channel(self):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            channel = integration.create_channel("test-channel")

        self.assertIsNotNone(channel)
        self.assertIn("test-channel", integration._channels)

    def test_create_channel_with_confirm_delivery(self):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            channel = integration.create_channel(confirm_delivery=True)

        mock_channel.confirm_delivery.assert_called_once()

    def test_get_channel(self):
        mock_channel = MagicMock()
        self.integration._channels["test-channel"] = mock_channel

        channel = self.integration.get_channel("test-channel")

        self.assertEqual(channel, mock_channel)

    def test_get_channel_not_found(self):
        channel = self.integration.get_channel("nonexistent")

        self.assertIsNone(channel)

    def test_close_channel(self):
        mock_channel = MagicMock()
        mock_channel.is_open = True
        self.integration._channels["test-channel"] = mock_channel

        self.integration.close_channel("test-channel")

        mock_channel.close.assert_called_once()
        self.assertNotIn("test-channel", self.integration._channels)

    def test_close_all_channels(self):
        mock_channel1 = MagicMock()
        mock_channel1.is_open = True
        mock_channel2 = MagicMock()
        mock_channel2.is_open = True
        self.integration._channels = {"ch1": mock_channel1, "ch2": mock_channel2}

        self.integration.close_all_channels()

        self.assertEqual(len(self.integration._channels), 0)


class TestRabbitMQIntegrationExchange(unittest.TestCase):
    """Test RabbitMQIntegration exchange management"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_declare_exchange(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.declare_exchange("test-exchange", ExchangeType.DIRECT)

        self.assertTrue(result)
        self.assertIn("test-exchange", integration._exchanges)
        mock_channel.exchange_declare.assert_called_once()

    def test_declare_exchange_with_type(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.declare_exchange("fanout-exchange", ExchangeType.FANOUT)

        self.assertTrue(result)
        call_kwargs = mock_channel.exchange_declare.call_args[1]
        self.assertEqual(call_kwargs['exchange_type'], "fanout")

    def test_delete_exchange(self):
        mock_channel = MagicMock()
        self.integration._channels["test-channel"] = mock_channel
        self.integration._exchanges.add("test-exchange")

        with patch.object(self.integration, 'is_connected', return_value=True):
            result = self.integration.delete_exchange("test-exchange")

        self.assertTrue(result)
        self.assertNotIn("test-exchange", self.integration._exchanges)

    def test_get_declared_exchanges(self):
        self.integration._exchanges = {"ex1", "ex2"}

        exchanges = self.integration.get_declared_exchanges()

        self.assertEqual(exchanges, {"ex1", "ex2"})


class TestRabbitMQIntegrationQueue(unittest.TestCase):
    """Test RabbitMQIntegration queue management"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_declare_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.declare_queue("test-queue")

        self.assertTrue(result)
        self.assertIn("test-queue", integration._queues)
        mock_channel.queue_declare.assert_called_once()

    def test_declare_quorum_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.declare_queue("quorum-queue", mode=QueueMode.QUORUM)

        self.assertTrue(result)
        call_kwargs = mock_channel.queue_declare.call_args[1]
        self.assertEqual(call_kwargs['arguments']['x-queue-type'], "quorum")

    def test_delete_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._channels = {"test": mock_channel}
        integration._queues = {"test-queue"}

        with patch.object(integration, 'is_connected', return_value=True):
            result = integration.delete_queue("test-queue")

        self.assertTrue(result)
        self.assertNotIn("test-queue", integration._queues)

    def test_purge_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
            with patch.object(integration, '_release_channel_to_pool'):
                result = integration.purge_queue("test-queue")

        self.assertTrue(result)
        mock_channel.queue_purge.assert_called_once()

    def test_get_declared_queues(self):
        self.integration._queues = {"queue1", "queue2"}

        queues = self.integration.get_declared_queues()

        self.assertEqual(queues, {"queue1", "queue2"})


class TestRabbitMQIntegrationBinding(unittest.TestCase):
    """Test RabbitMQIntegration binding management"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_bind_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.bind_queue(
                        queue="test-queue",
                        exchange="test-exchange",
                        routing_key="test.key"
                    )

        self.assertTrue(result)
        self.assertEqual(len(integration._bindings), 1)
        mock_channel.queue_bind.assert_called_once()

    def test_unbind_queue(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection
        integration._bindings = [BindingConfig(
            queue="test-queue",
            exchange="test-exchange",
            routing_key="test.key"
        )]

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.unbind_queue(
                        queue="test-queue",
                        exchange="test-exchange",
                        routing_key="test.key"
                    )

        self.assertTrue(result)
        self.assertEqual(len(integration._bindings), 0)

    def test_get_bindings(self):
        binding = BindingConfig(queue="q1", exchange="e1", routing_key="key1")
        self.integration._bindings = [binding]

        bindings = self.integration.get_bindings()

        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0].queue, "q1")


class TestRabbitMQIntegrationDeadLetterQueue(unittest.TestCase):
    """Test RabbitMQIntegration dead letter queue methods"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_enable_dlq_for_queue(self):
        with patch.object(self.integration, 'declare_queue', return_value=True) as mock_declare:
            result = self.integration.enable_dlq_for_queue("test-queue", max_retries=5)

        self.assertTrue(result)
        call_kwargs = mock_declare.call_args[1]
        self.assertEqual(call_kwargs['arguments']['x-dead-letter-exchange'], "dlx")

    def test_get_dlq_message_count(self):
        with patch.object(self.integration, 'get_queue_info', return_value={"message_count": 10}):
            count = self.integration.get_dlq_message_count()

        self.assertEqual(count, 10)


class TestRabbitMQIntegrationPublish(unittest.TestCase):
    """Test RabbitMQIntegration message publishing"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_publish_dict_message(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.publish(
                        message={"key": "value"},
                        exchange="test-exchange",
                        routing_key="test.key"
                    )

        self.assertTrue(result)
        mock_channel.basic_publish.assert_called_once()

    def test_publish_string_message(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.publish(
                        message="hello world",
                        exchange="test-exchange"
                    )

        self.assertTrue(result)

    def test_publish_with_properties(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        props = MessageProperties(priority=10, correlation_id="corr-123")

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                with patch.object(integration, '_release_channel_to_pool'):
                    result = integration.publish(
                        message="test",
                        exchange="test-exchange",
                        properties=props
                    )

        self.assertTrue(result)

    def test_publish_batch(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        messages = [
            {"body": "msg1"},
            {"body": "msg2"}
        ]

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, 'publish', return_value=True) as mock_publish:
                with patch.object(integration, 'create_channel', return_value=mock_channel):
                    with patch.object(integration, 'close_channel'):
                        result = integration.publish_batch(messages, "test-exchange")

        self.assertEqual(result, 2)


class TestRabbitMQIntegrationConsume(unittest.TestCase):
    """Test RabbitMQIntegration message consuming"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_consume(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        callback = MagicMock()

        with patch.object(integration, 'is_connected', return_value=True):
            with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
                result = integration.consume("test-queue", callback)

        self.assertIsNotNone(result)
        mock_channel.basic_consume.assert_called_once()

    def test_stop_consuming(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._channels = {"test": mock_channel}
        integration._consumers = {"consumer-tag-1": MagicMock()}

        integration.stop_consuming()

        self.assertFalse(integration._running)
        mock_channel.stop_consuming.assert_called_once()

    def test_acknowledge(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
            with patch.object(integration, '_release_channel_to_pool'):
                result = integration.acknowledge(delivery_tag=1)

        self.assertTrue(result)
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_reject(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        with patch.object(integration, '_acquire_channel_from_pool', return_value=mock_channel):
            with patch.object(integration, '_release_channel_to_pool'):
                result = integration.reject(delivery_tag=1, requeue=True)

        self.assertTrue(result)
        mock_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)


class TestRabbitMQIntegrationRPC(unittest.TestCase):
    """Test RabbitMQIntegration RPC methods"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 0.1
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_rpc_call_timeout(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection
        integration._rpc_timeout = 0.1

        with patch.object(integration, 'declare_queue', return_value=True):
            with patch.object(integration, 'publish', return_value=True):
                with patch.object(integration, 'consume', return_value="tag"):
                    with patch.object(integration, 'stop_consuming'):
                        with patch.object(integration, 'delete_queue'):
                            result = integration.rpc_call(
                                exchange="test-exchange",
                                routing_key="test.key",
                                request={"test": "request"},
                                timeout=0.1
                            )

        self.assertIsNone(result)

    def test_rpc_server(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True

        integration = RabbitMQIntegration()
        integration._connection = mock_connection

        handler = MagicMock(return_value={"status": "ok"})

        with patch.object(integration, 'declare_exchange', return_value=True):
            with patch.object(integration, 'declare_queue', return_value=True):
                with patch.object(integration, 'bind_queue', return_value=True):
                    with patch.object(integration, 'consume') as mock_consume:
                        integration.rpc_server(
                            queue="rpc-queue",
                            handler=handler,
                            exchange="rpc-exchange",
                            routing_key="rpc.key"
                        )

        mock_consume.assert_called_once()


class TestRabbitMQIntegrationClustering(unittest.TestCase):
    """Test RabbitMQIntegration clustering methods"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_add_cluster_node(self):
        node = ClusterNode(host="rabbitmq-2", is_master=True)

        self.integration.add_cluster_node(node)

        self.assertEqual(len(self.integration.cluster_nodes), 1)
        self.assertTrue(self.integration.cluster_nodes[0].is_master)

    def test_remove_cluster_node(self):
        node = ClusterNode(host="rabbitmq-2")
        self.integration.cluster_nodes = [node]

        self.integration.remove_cluster_node(node)

        self.assertEqual(len(self.integration.cluster_nodes), 0)

    def test_get_cluster_nodes(self):
        node1 = ClusterNode(host="node1")
        node2 = ClusterNode(host="node2")
        self.integration.cluster_nodes = [node1, node2]

        nodes = self.integration.get_cluster_nodes()

        self.assertEqual(len(nodes), 2)

    def test_connect_to_cluster(self):
        from pika.exceptions import AMQPConnectionError

        mock_connection = MagicMock()
        mock_connection.is_open = True

        node1 = ClusterNode(host="node1", is_master=True)
        node2 = ClusterNode(host="node2")
        self.integration.cluster_nodes = [node1, node2]
        self.integration.use_clustering = True

        with patch('src.workflow_rabbitmq.pika.BlockingConnection', return_value=mock_connection):
            with patch.object(self.integration, '_setup_dead_letter_queue'):
                result = self.integration.connect_to_cluster()

        self.assertTrue(result)

    def test_get_cluster_status(self):
        mock_connection = MagicMock()
        mock_connection.is_open = True
        mock_channel = MagicMock()
        mock_result = MagicMock()
        mock_result.method = MagicMock(message_count=5, consumer_count=2)
        mock_channel.queue_declare.return_value = mock_result
        mock_connection.channel.return_value = mock_channel

        node = ClusterNode(host="node1", is_master=True)
        self.integration.cluster_nodes = [node]
        self.integration.use_clustering = True
        self.integration._connection = mock_connection

        with patch.object(self.integration, '_acquire_channel_from_pool', return_value=mock_channel):
            with patch.object(self.integration, '_release_channel_to_pool'):
                status = self.integration.get_cluster_status()

        self.assertTrue(status["connected"])
        self.assertTrue(status["clustering_enabled"])

    def test_failover_to_node(self):
        from pika.exceptions import AMQPConnectionError

        mock_connection = MagicMock()
        mock_connection.is_open = True
        self.integration._connection = mock_connection

        node = ClusterNode(host="new-node", port=5673)

        with patch.object(self.integration, 'close_all_channels'):
            with patch.object(self.integration, 'disconnect'):
                with patch.object(self.integration, '_setup_dead_letter_queue'):
                    with patch('src.workflow_rabbitmq.pika.BlockingConnection', return_value=mock_connection):
                        result = self.integration.failover_to_node(node)

    def test_is_master_node(self):
        master_node = ClusterNode(host="master", is_master=True)
        worker_node = ClusterNode(host="worker", is_master=False)

        self.assertTrue(self.integration.is_master_node(master_node))
        self.assertFalse(self.integration.is_master_node(worker_node))

    def test_get_optimal_node(self):
        node1 = ClusterNode(host="node1", is_master=False)
        node2 = ClusterNode(host="master", is_master=True)

        self.integration.cluster_nodes = [node1, node2]

        optimal = self.integration.get_optimal_node()

        self.assertTrue(optimal.is_master)


class TestRabbitMQIntegrationUtility(unittest.TestCase):
    """Test RabbitMQIntegration utility methods"""

    def setUp(self):
        self.integration = RabbitMQIntegration.__new__(RabbitMQIntegration)
        self.integration.host = "localhost"
        self.integration.port = 5672
        self.integration.username = "guest"
        self.integration.password = "guest"
        self.integration.virtual_host = "/"
        self.integration.connection_attempts = 3
        self.integration.retry_delay = 5
        self.integration.heartbeat = 60
        self.integration.blocked_connection_timeout = 30.0
        self.integration.cluster_nodes = []
        self.integration.use_clustering = False
        self.integration.dlq_config = DeadLetterConfig()
        self.integration.max_channels = 100
        self.integration.channel_pool_size = 10
        self.integration._connection = None
        self.integration._connection_lock = __import__('threading').RLock()
        self.integration._channels = {}
        self.integration._channel_lock = __import__('threading').RLock()
        self.integration._channel_pool = Queue(maxsize=10)
        self.integration._consumers = {}
        self.integration._consumer_threads = {}
        self.integration._running = False
        self.integration._exchanges = set()
        self.integration._queues = set()
        self.integration._bindings = []
        self.integration._rpc_callbacks = {}
        self.integration._rpc_lock = __import__('threading').RLock()
        self.integration._rpc_timeout = 30.0
        self.integration._logger = __import__('logging').getLogger(__name__)

    def test_health_check_healthy(self):
        mock_connection = MagicMock()
        mock_connection.is_open = True
        self.integration._connection = mock_connection
        self.integration._channels = {"ch1": MagicMock()}
        self.integration._exchanges = {"ex1"}
        self.integration._queues = {"q1"}
        self.integration._bindings = [MagicMock()]
        self.integration._consumers = {"consumer1": MagicMock()}

        health = self.integration.health_check()

        self.assertEqual(health["status"], "healthy")
        self.assertTrue(health["connection"])

    def test_health_check_unhealthy(self):
        self.integration._connection = None

        health = self.integration.health_check()

        self.assertEqual(health["status"], "unhealthy")
        self.assertFalse(health["connection"])

    def test_reset(self):
        mock_connection = MagicMock()
        self.integration._connection = mock_connection
        self.integration._channels = {"ch1": MagicMock()}
        self.integration._exchanges = {"ex1"}
        self.integration._queues = {"q1"}
        self.integration._bindings = [MagicMock()]
        self.integration._consumers = {"consumer1": MagicMock()}
        self.integration._rpc_callbacks = {"key": "value"}

        self.integration.reset()

        self.assertEqual(len(self.integration._exchanges), 0)
        self.assertEqual(len(self.integration._queues), 0)
        self.assertEqual(len(self.integration._bindings), 0)
        self.assertEqual(len(self.integration._consumers), 0)

    def test_context_manager(self):
        with patch.object(self.integration, 'connect', return_value=True):
            with patch.object(self.integration, 'reset'):
                with self.integration as ctx:
                    pass

    def test_get_connection_parameters(self):
        integration = RabbitMQIntegration(
            host="testhost",
            port=5673,
            username="testuser",
            password="testpass"
        )

        with patch('src.workflow_rabbitmq.pika.ConnectionParameters') as mock_params_class:
            integration._get_connection_parameters()
            mock_params_class.assert_called_once()
            call_kwargs = mock_params_class.call_args[1]
            self.assertEqual(call_kwargs['host'], "testhost")
            self.assertEqual(call_kwargs['port'], 5673)


if __name__ == "__main__":
    unittest.main()
