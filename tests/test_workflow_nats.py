"""
Tests for workflow_nats module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import asyncio
import types

# Create mock nats module before importing workflow_nats
mock_nats = types.ModuleType('nats')
mock_nats.connect = MagicMock()

# Create mock errors module
mock_nats_errors = types.ModuleType('nats.errors')
mock_nats_errors.TimeoutError = Exception
mock_nats_errors.ErrConnectionClosed = Exception
mock_nats_errors.ErrTimeout = Exception
mock_nats.errors = mock_nats_errors

# Mock js.msg module for headers support
mock_js_msg = types.ModuleType('nats.js.msg')
mock_js_msg.Msg = MagicMock()
mock_nats.js = types.ModuleType('nats.js')
mock_nats.js.msg = mock_js_msg

sys.modules['nats'] = mock_nats
sys.modules['nats.errors'] = mock_nats_errors
sys.modules['nats.js'] = mock_nats.js
sys.modules['nats.js.msg'] = mock_js_msg

# Now we can import the module
from src.workflow_nats import (
    NATSIntegration,
    NATSConnectionState,
    DeliveryPolicy,
    StorageType,
    RePublish,
    NATSConfig,
    JetStreamConfig,
    ConsumerConfig as NATSConsumerConfig,
    KVConfig,
    ObjectConfig,
    ServiceInfo,
    ClusterNode,
)


class TestNATSConnectionState(unittest.TestCase):
    """Test NATSConnectionState enum"""

    def test_connection_state_values(self):
        self.assertEqual(NATSConnectionState.DISCONNECTED.value, "disconnected")
        self.assertEqual(NATSConnectionState.CONNECTING.value, "connecting")
        self.assertEqual(NATSConnectionState.CONNECTED.value, "connected")
        self.assertEqual(NATSConnectionState.RECONNECTING.value, "reconnecting")
        self.assertEqual(NATSConnectionState.CLOSED.value, "closed")


class TestDeliveryPolicy(unittest.TestCase):
    """Test DeliveryPolicy enum"""

    def test_delivery_policy_values(self):
        self.assertEqual(DeliveryPolicy.ALL.value, "all")
        self.assertEqual(DeliveryPolicy.NEW.value, "new")
        self.assertEqual(DeliveryPolicy.LAST.value, "last")
        self.assertEqual(DeliveryPolicy.LAST_PER_SUBJECT.value, "last_per_subject")
        self.assertEqual(DeliveryPolicy.SEQUENCE_START.value, "sequence_start")
        self.assertEqual(DeliveryPolicy.TIME_START.value, "time_start")


class TestStorageType(unittest.TestCase):
    """Test StorageType enum"""

    def test_storage_type_values(self):
        self.assertEqual(StorageType.FILE.value, "file")
        self.assertEqual(StorageType.MEMORY.value, "memory")


class TestRePublish(unittest.TestCase):
    """Test RePublish enum"""

    def test_republish_values(self):
        self.assertEqual(RePublish.NO.value, "no")
        self.assertEqual(RePublish.ALL.value, "all")
        self.assertEqual(RePublish.LAST.value, "last")


class TestNATSConfig(unittest.TestCase):
    """Test NATSConfig dataclass"""

    def test_nats_config_defaults(self):
        config = NATSConfig()
        self.assertEqual(config.servers, ["nats://localhost:4222"])
        self.assertEqual(config.name, "nats-integration")
        self.assertIsNone(config.user)
        self.assertIsNone(config.password)
        self.assertIsNone(config.token)
        self.assertIsNone(config.nkeys)
        self.assertIsNone(config.creds)
        self.assertFalse(config.verbose)
        self.assertFalse(config.pedantic)
        self.assertFalse(config.use_stan)
        self.assertTrue(config.retry_on_connect)
        self.assertEqual(config.max_reconnect_attempts, -1)
        self.assertEqual(config.reconnect_time_wait, 2.0)
        self.assertEqual(config.connect_timeout, 10.0)
        self.assertEqual(config.drain_timeout, 30.0)
        self.assertEqual(config.flush_timeout, 10.0)
        self.assertEqual(config.pending_size, 1024 * 1024)
        self.assertEqual(config.max_control_line, 1024)
        self.assertEqual(config.max_payload, 1024 * 1024)
        self.assertEqual(config.max_channels, 1024)

    def test_nats_config_custom(self):
        config = NATSConfig(
            servers=["nats://server1:4222", "nats://server2:4222"],
            name="custom-nats",
            user="admin",
            password="secret",
            verbose=True
        )
        self.assertEqual(len(config.servers), 2)
        self.assertEqual(config.name, "custom-nats")
        self.assertEqual(config.user, "admin")
        self.assertEqual(config.password, "secret")
        self.assertTrue(config.verbose)


class TestJetStreamConfig(unittest.TestCase):
    """Test JetStreamConfig dataclass"""

    def test_jetstream_config_defaults(self):
        config = JetStreamConfig()
        self.assertEqual(config.stream_name, "default")
        self.assertIsNone(config.description)
        self.assertEqual(config.subjects, [])
        self.assertEqual(config.retention, "limits")
        self.assertEqual(config.max_bytes, -1)
        self.assertEqual(config.max_msgs, -1)
        self.assertEqual(config.max_age, 0)
        self.assertEqual(config.storage, StorageType.FILE)
        self.assertEqual(config.replicas, 1)
        self.assertFalse(config.no_ack)
        self.assertEqual(config.duplicates, 0)

    def test_jetstream_config_custom(self):
        config = JetStreamConfig(
            stream_name="test-stream",
            subjects=["test.>", "events.>"],
            storage=StorageType.MEMORY,
            replicas=3
        )
        self.assertEqual(config.stream_name, "test-stream")
        self.assertEqual(len(config.subjects), 2)
        self.assertEqual(config.storage, StorageType.MEMORY)
        self.assertEqual(config.replicas, 3)


class TestNatsConsumerConfig(unittest.TestCase):
    """Test NATS ConsumerConfig dataclass"""

    def test_consumer_config_defaults(self):
        config = NATSConsumerConfig()
        self.assertEqual(config.consumer_name, "")
        self.assertIsNone(config.durable_name)
        self.assertEqual(config.deliver_policy, DeliveryPolicy.ALL)
        self.assertIsNone(config.filter_subject)
        self.assertEqual(config.ack_policy, "explicit")
        self.assertEqual(config.ack_wait, 30)
        self.assertEqual(config.max_deliver, -1)
        self.assertEqual(config.max_ack_pending, -1)
        self.assertEqual(config.max_waiting, 512)
        self.assertFalse(config.headers_only)

    def test_consumer_config_custom(self):
        config = NATSConsumerConfig(
            consumer_name="test-consumer",
            durable_name="durable-consumer",
            deliver_policy=DeliveryPolicy.LAST,
            max_deliver=5
        )
        self.assertEqual(config.consumer_name, "test-consumer")
        self.assertEqual(config.durable_name, "durable-consumer")
        self.assertEqual(config.deliver_policy, DeliveryPolicy.LAST)
        self.assertEqual(config.max_deliver, 5)


class TestKVConfig(unittest.TestCase):
    """Test KVConfig dataclass"""

    def test_kv_config_defaults(self):
        config = KVConfig()
        self.assertEqual(config.bucket, "default")
        self.assertIsNone(config.description)
        self.assertEqual(config.max_bytes, -1)
        self.assertEqual(config.max_value_size, -1)
        self.assertEqual(config.history, 1)
        self.assertEqual(config.ttl, 0)
        self.assertEqual(config.storage, StorageType.FILE)
        self.assertEqual(config.replicas, 1)
        self.assertFalse(config.allow_republish)
        self.assertFalse(config.deny_delete)
        self.assertFalse(config.deny_purge)

    def test_kv_config_custom(self):
        config = KVConfig(
            bucket="my-bucket",
            max_bytes=1024,
            history=5,
            storage=StorageType.MEMORY
        )
        self.assertEqual(config.bucket, "my-bucket")
        self.assertEqual(config.max_bytes, 1024)
        self.assertEqual(config.history, 5)
        self.assertEqual(config.storage, StorageType.MEMORY)


class TestObjectConfig(unittest.TestCase):
    """Test ObjectConfig dataclass"""

    def test_object_config_defaults(self):
        config = ObjectConfig()
        self.assertEqual(config.bucket, "default")
        self.assertIsNone(config.description)
        self.assertEqual(config.max_bytes, -1)
        self.assertEqual(config.storage, StorageType.FILE)
        self.assertEqual(config.replicas, 1)
        self.assertFalse(config.compression)
        self.assertTrue(config.allow_delete)
        self.assertTrue(config.allow_purge)

    def test_object_config_custom(self):
        config = ObjectConfig(
            bucket="my-objects",
            compression=True,
            allow_delete=False
        )
        self.assertEqual(config.bucket, "my-objects")
        self.assertTrue(config.compression)
        self.assertFalse(config.allow_delete)


class TestServiceInfo(unittest.TestCase):
    """Test ServiceInfo dataclass"""

    def test_service_info_required(self):
        info = ServiceInfo(name="test-service", version="1.0", endpoint="http://localhost:8080")
        self.assertEqual(info.name, "test-service")
        self.assertEqual(info.version, "1.0")
        self.assertEqual(info.endpoint, "http://localhost:8080")
        self.assertEqual(info.metadata, {})
        self.assertEqual(info.status, "healthy")
        self.assertIsNotNone(info.last_seen)
        self.assertEqual(info.tags, [])

    def test_service_info_full(self):
        info = ServiceInfo(
            name="full-service",
            version="2.0",
            endpoint="http://localhost:9090",
            metadata={"env": "prod"},
            status="running",
            tags=["production", "v2"]
        )
        self.assertEqual(info.metadata, {"env": "prod"})
        self.assertEqual(info.status, "running")
        self.assertEqual(len(info.tags), 2)


class TestClusterNode(unittest.TestCase):
    """Test ClusterNode dataclass"""

    def test_cluster_node_required(self):
        node = ClusterNode(server_id="server1", address="192.168.1.1", port=4222)
        self.assertEqual(node.server_id, "server1")
        self.assertEqual(node.address, "192.168.1.1")
        self.assertEqual(node.port, 4222)
        self.assertFalse(node.auth_required)
        self.assertFalse(node.tls_required)
        self.assertEqual(node.connect_urls, [])
        self.assertFalse(node.is_leader)
        self.assertFalse(node.is_operator)
        self.assertIsNone(node.cluster)

    def test_cluster_node_full(self):
        node = ClusterNode(
            server_id="leader",
            address="192.168.1.1",
            port=4222,
            auth_required=True,
            tls_required=True,
            is_leader=True,
            cluster="production"
        )
        self.assertTrue(node.auth_required)
        self.assertTrue(node.tls_required)
        self.assertTrue(node.is_leader)
        self.assertEqual(node.cluster, "production")


class TestNATSIntegration(unittest.TestCase):
    """Test NATSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    def test_integration_init(self):
        """Test integration initialization"""
        self.assertEqual(self.integration.config, self.config)
        self.assertIsNone(self.integration._client)
        self.assertIsNone(self.integration._js)
        self.assertEqual(self.integration._state, NATSConnectionState.DISCONNECTED)
        self.assertEqual(self.integration._subscriptions, {})
        self.assertEqual(self.integration._js_contexts, {})
        self.assertEqual(self.integration._kv_stores, {})
        self.assertEqual(self.integration._object_stores, {})
        self.assertEqual(self.integration._services, {})

    def test_is_connected_initially_false(self):
        """Test is_connected returns False initially"""
        self.assertFalse(self.integration.is_connected)

    def test_connection_state_initially_disconnected(self):
        """Test connection_state returns DISCONNECTED initially"""
        self.assertEqual(self.integration.connection_state, NATSConnectionState.DISCONNECTED)

    @patch('src.workflow_nats.NATS_AVAILABLE', False)
    def test_connect_without_nats_library(self):
        """Test connect when NATS library is not available"""
        result = asyncio.run(self.integration.connect())
        self.assertFalse(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', False)
    def test_publish_without_nats_library(self):
        """Test publish when NATS library is not available"""
        result = asyncio.run(self.integration.publish("test.subject", "message"))
        self.assertFalse(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', False)
    def test_subscribe_without_nats_library(self):
        """Test subscribe when NATS library is not available"""
        async def callback(subject, payload):
            pass
        result = asyncio.run(self.integration.subscribe("test.subject", callback))
        self.assertIsNone(result)

    def test_integration_with_default_config(self):
        """Test integration with default config"""
        integration = NATSIntegration()
        self.assertIsNotNone(integration.config)
        self.assertEqual(integration.config.servers, ["nats://localhost:4222"])

    def test_publish_without_connection(self):
        """Test publish fails when not connected"""
        result = asyncio.run(self.integration.publish("test.subject", "message"))
        self.assertFalse(result)

    def test_subscribe_without_connection(self):
        """Test subscribe fails when not connected"""
        async def callback(subject, payload):
            pass
        result = asyncio.run(self.integration.subscribe("test.subject", callback))
        self.assertIsNone(result)


class TestNATSIntegrationSync(unittest.TestCase):
    """Test synchronous methods in NATSIntegration"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', False)
    def test_connect_sync_without_library(self):
        """Test synchronous connect without library"""
        result = self.integration.connect_sync()
        self.assertFalse(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', False)
    def test_publish_sync_without_library(self):
        """Test synchronous publish without library"""
        result = self.integration.publish_sync("test.subject", "message")
        self.assertFalse(result)

    def test_publish_sync_without_connection(self):
        """Test synchronous publish without connection"""
        result = self.integration.publish_sync("test.subject", "message")
        self.assertFalse(result)

    def test_subscribe_sync_without_connection(self):
        """Test synchronous subscribe without connection"""
        def callback(subject, payload):
            pass
        result = self.integration.subscribe_sync("test.subject", callback)
        self.assertIsNone(result)

    def test_disconnect_sync(self):
        """Test synchronous disconnect"""
        # Should not raise even when not connected
        self.integration.disconnect_sync()


class TestNATSIntegrationPubSub(unittest.TestCase):
    """Test publish/subscribe functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_publish_bytes(self, mock_connect):
        """Test publishing bytes payload"""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.publish("test.subject", b"binary data")
        self.assertTrue(result)
        mock_client.publish.assert_called_once()

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_publish_json(self, mock_connect):
        """Test publishing JSON-serializable payload"""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.publish("test.subject", {"key": "value"})
        self.assertTrue(result)
        mock_client.publish.assert_called_once()

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_subscribe(self, mock_connect):
        """Test subscription"""
        mock_client = MagicMock()
        mock_sub = MagicMock()
        mock_client.subscribe.return_value = mock_sub
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        async def callback(subject, payload):
            pass

        result = await self.integration.subscribe("test.subject", callback)
        self.assertIsNotNone(result)
        self.assertIn(result, self.integration._subscriptions)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_subscribe_with_queue(self, mock_connect):
        """Test subscription with queue group"""
        mock_client = MagicMock()
        mock_sub = MagicMock()
        mock_client.subscribe.return_value = mock_sub
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        async def callback(subject, payload):
            pass

        result = await self.integration.subscribe("test.subject", callback, queue="workers")
        self.assertIsNotNone(result)
        mock_client.subscribe.assert_called()

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_unsubscribe(self, mock_connect):
        """Test unsubscribing"""
        mock_client = MagicMock()
        mock_sub = MagicMock()
        mock_client.subscribe.return_value = mock_sub
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        async def callback(subject, payload):
            pass

        sub_id = await self.integration.subscribe("test.subject", callback)
        result = await self.integration.unsubscribe(sub_id)
        self.assertTrue(result)


class TestNATSIntegrationJetStream(unittest.TestCase):
    """Test JetStream functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_create_stream(self, mock_connect):
        """Test stream creation"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED

        config = JetStreamConfig(stream_name="test-stream", subjects=["test.>"])
        result = await self.integration.create_stream(config)
        self.assertTrue(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_delete_stream(self, mock_connect):
        """Test stream deletion"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.delete_stream("test-stream")
        self.assertTrue(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_publish_to_stream(self, mock_connect):
        """Test publishing to JetStream"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.publish("test.subject", "message")
        self.assertTrue(result)


class TestNATSIntegrationKV(unittest.TestCase):
    """Test KV store functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_create_kv_bucket(self, mock_connect):
        """Test KV bucket creation"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED

        config = KVConfig(bucket="my-bucket")
        result = await self.integration.create_kv_bucket(config)
        self.assertTrue(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_kv_put(self, mock_connect):
        """Test KV put operation"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_kv = MagicMock()
        mock_js.kv.return_value = mock_kv
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED
        self.integration._kv_stores["bucket"] = mock_kv

        result = await self.integration.kv_put("bucket", "key", "value")
        self.assertTrue(result)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_kv_get(self, mock_connect):
        """Test KV get operation"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_kv = MagicMock()
        mock_entry = MagicMock()
        mock_entry.value = b'"test-value"'
        mock_kv.get.return_value = mock_entry
        mock_js.kv.return_value = mock_kv
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED
        self.integration._kv_stores["bucket"] = mock_kv

        result = await self.integration.kv_get("bucket", "key")
        self.assertEqual(result, "test-value")

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_kv_delete(self, mock_connect):
        """Test KV delete operation"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_kv = MagicMock()
        mock_js.kv.return_value = mock_kv
        mock_client.jetstream.return_value = mock_js
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED
        self.integration._kv_stores["bucket"] = mock_kv

        result = await self.integration.kv_delete("bucket", "key")
        self.assertTrue(result)


class TestNATSIntegrationServiceDiscovery(unittest.TestCase):
    """Test service discovery functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_register_service(self, mock_connect):
        """Test service registration"""
        mock_client = MagicMock()
        mock_js = MagicMock()
        mock_kv = MagicMock()
        mock_js.kv.return_value = mock_kv
        mock_client.jetstream.return_value = mock_js
        mock_client.publish = MagicMock()
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._js = mock_js
        self.integration._state = NATSConnectionState.CONNECTED
        self.integration._kv_stores["_service_registry"] = mock_kv

        service = ServiceInfo(
            name="test-service",
            version="1.0",
            endpoint="http://localhost:8080"
        )
        result = await self.integration.register_service(service)
        self.assertTrue(result)

    def test_discover_services_not_connected(self):
        """Test discovering services when not connected"""
        result = asyncio.run(self.integration.discover_services())
        self.assertEqual(result, [])


class TestNATSIntegrationRequestReply(unittest.TestCase):
    """Test request/reply functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_request(self, mock_connect):
        """Test making a request"""
        mock_client = MagicMock()
        mock_msg = MagicMock()
        mock_msg.data = b'"response"'
        mock_client.request.return_value = mock_msg
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.request("test.subject", "request data")
        self.assertEqual(result, "response")

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_request_timeout(self, mock_connect):
        """Test request timeout"""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        # Mock request to raise timeout
        import src.workflow_nats as wn
        with patch.object(mock_client, 'request', side_effect=wn.ErrTimeout()):
            result = await self.integration.request("test.subject", "data", timeout=1)
            self.assertIsNone(result)


class TestNATSIntegrationClustering(unittest.TestCase):
    """Test clustering functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = NATSConfig()
        self.integration = NATSIntegration(self.config)

    @patch('src.workflow_nats.NATS_AVAILABLE', True)
    @patch.object(mock_nats, 'connect')
    async def test_get_cluster_info(self, mock_connect):
        """Test getting cluster info"""
        mock_client = MagicMock()
        mock_client.server_info = MagicMock()
        mock_client.server_info.server_id = "server-1"
        mock_client.server_info.cluster = "test-cluster"
        mock_connect.return_value = mock_client

        self.integration._client = mock_client
        self.integration._state = NATSConnectionState.CONNECTED

        result = await self.integration.get_cluster_info()
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
