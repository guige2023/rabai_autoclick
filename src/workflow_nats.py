"""
NATS Messaging System Integration Module for Workflow System

Implements a NATSIntegration class with:
1. Connection management: Manage NATS connections
2. Subscription management: Subscribe to subjects
3. Publish/subscribe: Pub/sub messaging
4. Request/reply: Request/reply pattern
5. JetStream: Stream persistence
6. KV store: Key-value store
7. Object store: Object storage
8. Service infrastructure: Service discovery
9. Micro services: Service monitoring
10. Clustering: Cluster management

Commit: 'feat(nats): add NATS integration with connection management, subscription, pub/sub, request/reply, JetStream, KV store, object store, service infrastructure, clustering'
"""

import uuid
import json
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy

try:
    import nats
    from nats.errors import TimeoutError as NATSTimeoutError, ErrConnectionClosed, ErrTimeout
    NATS_AVAILABLE = True
except ImportError:
    NATS_AVAILABLE = False
    nats = None


logger = logging.getLogger(__name__)


class NATSConnectionState(Enum):
    """NATS connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


class DeliveryPolicy(Enum):
    """Message delivery policies for JetStream."""
    ALL = "all"
    NEW = "new"
    LAST = "last"
    LAST_PER_SUBJECT = "last_per_subject"
    SEQUENCE_START = "sequence_start"
    TIME_START = "time_start"


class StorageType(Enum):
    """Storage types for streams."""
    FILE = "file"
    MEMORY = "memory"


class RePublish(Enum):
    """RePublish policy for JetStream consumers."""
    NO = "no"
    ALL = "all"
    LAST = "last"


@dataclass
class NATSConfig:
    """Configuration for NATS connection."""
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    name: str = "nats-integration"
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    nkeys: Optional[str] = None
    creds: Optional[str] = None
    verbose: bool = False
    pedantic: bool = False
    use_stan: bool = False
    retry_on_connect: bool = True
    max_reconnect_attempts: int = -1
    reconnect_time_wait: float = 2.0
    connect_timeout: float = 10.0
    drain_timeout: float = 30.0
    flush_timeout: float = 10.0
    pending_size: int = 1024 * 1024
    max_control_line: int = 1024
    max_payload: int = 1024 * 1024
    max_channels: int = 1024


@dataclass
class JetStreamConfig:
    """Configuration for JetStream."""
    stream_name: str = "default"
    description: Optional[str] = None
    subjects: List[str] = field(default_factory=list)
    retention: str = "limits"
    max_bytes: int = -1
    max_msgs: int = -1
    max_age: int = 0
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    no_ack: bool = False
    duplicates: int = 0


@dataclass
class ConsumerConfig:
    """Configuration for JetStream consumer."""
    consumer_name: str = ""
    durable_name: Optional[str] = None
    deliver_policy: DeliveryPolicy = DeliveryPolicy.ALL
    filter_subject: Optional[str] = None
    ack_policy: str = "explicit"
    ack_wait: int = 30
    max_deliver: int = -1
    max_ack_pending: int = -1
    max_waiting: int = 512
    headers_only: bool = False


@dataclass
class KVConfig:
    """Configuration for KV store bucket."""
    bucket: str = "default"
    description: Optional[str] = None
    max_bytes: int = -1
    max_value_size: int = -1
    history: int = 1
    ttl: int = 0
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    allow_republish: bool = False
    deny_delete: bool = False
    deny_purge: bool = False


@dataclass
class ObjectConfig:
    """Configuration for object store bucket."""
    bucket: str = "default"
    description: Optional[str] = None
    max_bytes: int = -1
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    compression: bool = False
    allow_delete: bool = True
    allow_purge: bool = True


@dataclass
class ServiceInfo:
    """Service information for discovery."""
    name: str
    version: str
    endpoint: str
    metadata: Dict[str, str] = field(default_factory=dict)
    status: str = "healthy"
    last_seen: datetime = field(default_factory=datetime.now)
    tags: List[str] = field(default_factory=list)


@dataclass
class ClusterNode:
    """NATS cluster node information."""
    server_id: str
    address: str
    port: int
    auth_required: bool = False
    tls_required: bool = False
    connect_urls: List[str] = field(default_factory=list)
    is_leader: bool = False
    is_operator: bool = False
    cluster: Optional[str] = None


class NATSIntegration:
    """
    NATS messaging system integration with support for:
    - Connection management
    - Subscription management
    - Publish/subscribe
    - Request/reply
    - JetStream persistence
    - KV store
    - Object store
    - Service discovery
    - Service monitoring
    - Clustering
    """

    def __init__(self, config: Optional[NATSConfig] = None):
        """
        Initialize NATS integration.

        Args:
            config: NATS configuration
        """
        self.config = config or NATSConfig()
        self._client: Optional[Any] = None
        self._js: Optional[Any] = None
        self._state = NATSConnectionState.DISCONNECTED
        self._subscriptions: Dict[str, Any] = {}
        self._js_contexts: Dict[str, Any] = {}
        self._kv_stores: Dict[str, Any] = {}
        self._object_stores: Dict[str, Any] = {}
        self._services: Dict[str, ServiceInfo] = {}
        self._lock = threading.RLock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connected_event = threading.Event()

        if not NATS_AVAILABLE:
            logger.warning("NATS library not available. Install with: pip install nats-py")

    @property
    def is_connected(self) -> bool:
        """Check if connected to NATS server."""
        return self._state == NATSConnectionState.CONNECTED and self._client is not None

    @property
    def connection_state(self) -> NATSConnectionState:
        """Get current connection state."""
        return self._state

    async def _ensure_async(self):
        """Ensure we have an event loop running."""
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            # Start the loop in a background thread
            def run_loop():
                asyncio.set_event_loop(self._loop)
                self._loop.run_forever()
            threading.Thread(target=run_loop, daemon=True).start()

    async def connect(self) -> bool:
        """
        Connect to NATS server.

        Returns:
            True if connected successfully
        """
        if not NATS_AVAILABLE:
            logger.error("Cannot connect: NATS library not available")
            return False

        with self._lock:
            if self.is_connected:
                return True

            self._state = NATSConnectionState.CONNECTING
            try:
                await self._ensure_async()

                options = {
                    "servers": self.config.servers,
                    "name": self.config.name,
                    "verbose": self.config.verbose,
                    "pedantic": self.config.pedantic,
                    "reconnect_time_wait": self.config.reconnect_time_wait,
                    "max_reconnect_attempts": self.config.max_reconnect_attempts,
                    "connect_timeout": self.config.connect_timeout,
                    "drain_timeout": self.config.drain_timeout,
                    "pending_size": self.config.pending_size,
                }

                if self.config.user and self.config.password:
                    options["user"] = self.config.user
                    options["password"] = self.config.password
                if self.config.token:
                    options["token"] = self.config.token
                if self.config.nkeys:
                    options["nkeys"] = self.config.nkeys
                if self.config.creds:
                    options["creds"] = self.config.creds

                self._client = await nats.connect(**options)
                self._js = self._client.jetstream()
                self._state = NATSConnectionState.CONNECTED
                self._connected_event.set()
                logger.info(f"Connected to NATS at {self.config.servers}")
                return True

            except Exception as e:
                self._state = NATSConnectionState.DISCONNECTED
                logger.error(f"Failed to connect to NATS: {e}")
                return False

    def connect_sync(self, timeout: float = 10.0) -> bool:
        """
        Synchronous connect to NATS server.

        Args:
            timeout: Connection timeout

        Returns:
            True if connected successfully
        """
        if not NATS_AVAILABLE:
            logger.error("Cannot connect: NATS library not available")
            return False

        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        if self._loop.is_running():
            # If loop is already running, we need to run in a separate thread
            result = [False]
            def connect_in_thread():
                result[0] = asyncio.run(self.connect())
            thread = threading.Thread(target=connect_in_thread)
            thread.start()
            thread.join(timeout=timeout)
            return result[0]
        else:
            return asyncio.run(self.connect())

    async def disconnect(self):
        """Disconnect from NATS server."""
        with self._lock:
            if self._client:
                try:
                    await self._client.close()
                except Exception as e:
                    logger.error(f"Error closing NATS connection: {e}")
                finally:
                    self._client = None
                    self._js = None
                    self._state = NATSConnectionState.CLOSED
                    self._connected_event.clear()

    def disconnect_sync(self):
        """Synchronous disconnect from NATS server."""
        if self._loop and not self._loop.is_running():
            asyncio.run(self.disconnect())
        elif self._loop:
            def close_in_thread():
                asyncio.set_event_loop(self._loop)
                asyncio.run(self.disconnect())
            thread = threading.Thread(target=close_in_thread)
            thread.start()
            thread.join(timeout=5.0)

    # =========================================================================
    # Pub/Sub Methods
    # =========================================================================

    async def publish(self, subject: str, payload: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Publish a message to a subject.

        Args:
            subject: Subject to publish to
            payload: Message payload (will be serialized to JSON if not bytes)
            headers: Optional message headers

        Returns:
            True if published successfully
        """
        if not self.is_connected:
            logger.error("Cannot publish: not connected to NATS")
            return False

        try:
            if isinstance(payload, bytes):
                data = payload
            else:
                data = json.dumps(payload).encode("utf-8")

            msg = data
            if headers:
                from nats.js.msg import Msg
                # For messages with headers, we use a different approach
                await self._client.publish(subject, msg, headers=headers)
            else:
                await self._client.publish(subject, msg)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {subject}: {e}")
            return False

    def publish_sync(self, subject: str, payload: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """Synchronous publish."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.publish(subject, payload, headers))
        elif self._loop:
            result = [False]
            def publish_in_thread():
                result[0] = asyncio.run(self.publish(subject, payload, headers))
            thread = threading.Thread(target=publish_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.publish(subject, payload, headers))

    async def subscribe(
        self,
        subject: str,
        callback: Callable[[str, Any], Awaitable[None]],
        queue: Optional[str] = None,
        max_messages: int = -1
    ) -> Optional[str]:
        """
        Subscribe to a subject.

        Args:
            subject: Subject to subscribe to
            callback: Async callback function(subject, payload)
            queue: Optional queue group
            max_messages: Maximum messages to receive (-1 for unlimited)

        Returns:
            Subscription ID if successful
        """
        if not self.is_connected:
            logger.error("Cannot subscribe: not connected to NATS")
            return None

        try:
            sub_id = str(uuid.uuid4())

            async def wrapper(msg):
                try:
                    payload = msg.data
                    try:
                        payload = json.loads(payload.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                    await callback(msg.subject, payload)
                except Exception as e:
                    logger.error(f"Error in subscription callback: {e}")

            if queue:
                sub = await self._client.subscribe(subject, queue=queue, cb=wrapper)
            else:
                sub = await self._client.subscribe(subject, cb=wrapper)

            self._subscriptions[sub_id] = sub
            logger.info(f"Subscribed to {subject} with ID {sub_id}")
            return sub_id
        except Exception as e:
            logger.error(f"Failed to subscribe to {subject}: {e}")
            return None

    def subscribe_sync(
        self,
        subject: str,
        callback: Callable[[str, Any], None],
        queue: Optional[str] = None,
        max_messages: int = -1
    ) -> Optional[str]:
        """
        Synchronous subscribe to a subject.

        Args:
            subject: Subject to subscribe to
            callback: Callback function(subject, payload)
            queue: Optional queue group
            max_messages: Maximum messages to receive

        Returns:
            Subscription ID if successful
        """
        async def async_callback(subj, payload):
            callback(subj, payload)

        if self._loop and not self._loop.is_running():
            return asyncio.run(self.subscribe(subject, async_callback, queue, max_messages))
        elif self._loop:
            result = [None]
            def sub_in_thread():
                result[0] = asyncio.run(self.subscribe(subject, async_callback, queue, max_messages))
            thread = threading.Thread(target=sub_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.subscribe(subject, async_callback, queue, max_messages))

    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a subject.

        Args:
            subscription_id: Subscription ID to unsubscribe

        Returns:
            True if unsubscribed successfully
        """
        with self._lock:
            if subscription_id not in self._subscriptions:
                logger.warning(f"Subscription {subscription_id} not found")
                return False

            try:
                sub = self._subscriptions[subscription_id]
                await sub.unsubscribe()
                del self._subscriptions[subscription_id]
                logger.info(f"Unsubscribed from {subscription_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to unsubscribe: {e}")
                return False

    def unsubscribe_sync(self, subscription_id: str) -> bool:
        """Synchronous unsubscribe."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.unsubscribe(subscription_id))
        elif self._loop:
            result = [False]
            def unsub_in_thread():
                result[0] = asyncio.run(self.unsubscribe(subscription_id))
            thread = threading.Thread(target=unsub_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.unsubscribe(subscription_id))

    # =========================================================================
    # Request/Reply Methods
    # =========================================================================

    async def request(
        self,
        subject: str,
        payload: Any,
        timeout: float = 5.0,
        headers: Optional[Dict[str, str]] = None
    ) -> Optional[Any]:
        """
        Send a request and wait for a reply.

        Args:
            subject: Subject to send request to
            payload: Request payload
            timeout: Request timeout in seconds
            headers: Optional request headers

        Returns:
            Response payload or None
        """
        if not self.is_connected:
            logger.error("Cannot request: not connected to NATS")
            return None

        try:
            if isinstance(payload, bytes):
                data = payload
            else:
                data = json.dumps(payload).encode("utf-8")

            msg = await self._client.request(subject, data, timeout=timeout)
            response = msg.data
            try:
                response = json.loads(response.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            return response
        except Exception as e:
            logger.error(f"Request to {subject} failed: {e}")
            return None

    def request_sync(self, subject: str, payload: Any, timeout: float = 5.0) -> Optional[Any]:
        """Synchronous request."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.request(subject, payload, timeout))
        elif self._loop:
            result = [None]
            def req_in_thread():
                result[0] = asyncio.run(self.request(subject, payload, timeout))
            thread = threading.Thread(target=req_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.request(subject, payload, timeout))

    async def respond(self, subject: str, payload: Any) -> bool:
        """
        Send a response to a reply subject.

        Args:
            subject: Reply subject
            payload: Response payload

        Returns:
            True if sent successfully
        """
        return await self.publish(subject, payload)

    # =========================================================================
    # JetStream Methods
    # =========================================================================

    async def create_stream(self, config: JetStreamConfig) -> bool:
        """
        Create a JetStream stream.

        Args:
            config: Stream configuration

        Returns:
            True if created successfully
        """
        if not self.is_connected:
            logger.error("Cannot create stream: not connected to NATS")
            return False

        try:
            stream_config = {
                "name": config.stream_name,
                "description": config.description,
                "subjects": config.subjects,
                "retention": config.retention,
                "max_bytes": config.max_bytes,
                "max_msgs": config.max_msgs,
                "max_age": config.max_age,
                "storage": config.storage.value,
                "replicas": config.replicas,
                "no_ack": config.no_ack,
                "duplicates": config.duplicates,
            }

            await self._js.add_stream(**stream_config)
            logger.info(f"Created stream: {config.stream_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create stream: {e}")
            return False

    def create_stream_sync(self, config: JetStreamConfig) -> bool:
        """Synchronous create stream."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.create_stream(config))
        elif self._loop:
            result = [False]
            def create_in_thread():
                result[0] = asyncio.run(self.create_stream(config))
            thread = threading.Thread(target=create_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.create_stream(config))

    async def delete_stream(self, stream_name: str) -> bool:
        """
        Delete a JetStream stream.

        Args:
            stream_name: Name of stream to delete

        Returns:
            True if deleted successfully
        """
        if not self.is_connected:
            return False

        try:
            await self._js.delete_stream(stream_name)
            logger.info(f"Deleted stream: {stream_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete stream: {e}")
            return False

    def delete_stream_sync(self, stream_name: str) -> bool:
        """Synchronous delete stream."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.delete_stream(stream_name))
        elif self._loop:
            result = [False]
            def delete_in_thread():
                result[0] = asyncio.run(self.delete_stream(stream_name))
            thread = threading.Thread(target=delete_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.delete_stream(stream_name))

    async def publish_with_ack(
        self,
        subject: str,
        payload: Any,
        stream: Optional[str] = None,
        timeout: float = 5.0
    ) -> Optional[str]:
        """
        Publish a message with JetStream acknowledgment.

        Args:
            subject: Subject to publish to
            payload: Message payload
            stream: Optional stream name for persistence
            timeout: Ack timeout

        Returns:
            Message ID if published successfully
        """
        if not self.is_connected:
            logger.error("Cannot publish: not connected to NATS")
            return None

        try:
            if isinstance(payload, bytes):
                data = payload
            else:
                data = json.dumps(payload).encode("utf-8")

            msg = self._js.publish(subject, data)
            ack = await msg.achunk(timeout=timeout)
            return ack.sequence if hasattr(ack, 'sequence') else str(ack)
        except Exception as e:
            logger.error(f"Failed to publish with ack: {e}")
            return None

    def publish_with_ack_sync(self, subject: str, payload: Any, stream: Optional[str] = None) -> Optional[str]:
        """Synchronous publish with ack."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.publish_with_ack(subject, payload, stream))
        elif self._loop:
            result = [None]
            def publish_in_thread():
                result[0] = asyncio.run(self.publish_with_ack(subject, payload, stream))
            thread = threading.Thread(target=publish_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.publish_with_ack(subject, payload, stream))

    async def subscribe_with_durable_consumer(
        self,
        subject: str,
        callback: Callable[[str, Any], Awaitable[None]],
        consumer_config: ConsumerConfig,
        stream: Optional[str] = None
    ) -> Optional[str]:
        """
        Subscribe with a durable JetStream consumer.

        Args:
            subject: Subject to subscribe to
            callback: Async callback function
            consumer_config: Consumer configuration
            stream: Optional stream name

        Returns:
            Subscription ID if successful
        """
        if not self.is_connected:
            logger.error("Cannot subscribe: not connected to NATS")
            return None

        try:
            sub_id = str(uuid.uuid4())

            async def wrapper(msg):
                try:
                    payload = msg.data
                    try:
                        payload = json.loads(payload.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                    await callback(msg.subject, payload)
                    await msg.ack()
                except Exception as e:
                    logger.error(f"Error in JetStream consumer callback: {e}")

            consumer_options = {
                "durable": consumer_config.durable_name or f"consumer-{sub_id}",
                "deliver_policy": consumer_config.deliver_policy.value,
                "ack_policy": consumer_config.ack_policy,
                "ack_wait": consumer_config.ack_wait,
                "max_deliver": consumer_config.max_deliver,
                "max_ack_pending": consumer_config.max_ack_pending,
                "max_waiting": consumer_config.max_waiting,
                "headers_only": consumer_config.headers_only,
            }

            if stream:
                sub = await self._js.subscribe(
                    subject,
                    stream=stream,
                    cb=wrapper,
                    **consumer_options
                )
            else:
                sub = await self._js.subscribe(
                    subject,
                    cb=wrapper,
                    **consumer_options
                )

            self._subscriptions[sub_id] = sub
            return sub_id
        except Exception as e:
            logger.error(f"Failed to subscribe with durable consumer: {e}")
            return None

    def subscribe_with_durable_consumer_sync(
        self,
        subject: str,
        callback: Callable[[str, Any], None],
        consumer_config: ConsumerConfig,
        stream: Optional[str] = None
    ) -> Optional[str]:
        """Synchronous subscribe with durable consumer."""
        async def async_callback(subj, payload):
            callback(subj, payload)

        if self._loop and not self._loop.is_running():
            return asyncio.run(self.subscribe_with_durable_consumer(subject, async_callback, consumer_config, stream))
        elif self._loop:
            result = [None]
            def sub_in_thread():
                result[0] = asyncio.run(self.subscribe_with_durable_consumer(subject, async_callback, consumer_config, stream))
            thread = threading.Thread(target=sub_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.subscribe_with_durable_consumer(subject, async_callback, consumer_config, stream))

    # =========================================================================
    # KV Store Methods
    # =========================================================================

    async def create_kv_bucket(self, config: KVConfig) -> bool:
        """
        Create a KV store bucket.

        Args:
            config: KV bucket configuration

        Returns:
            True if created successfully
        """
        if not self.is_connected:
            logger.error("Cannot create KV bucket: not connected to NATS")
            return False

        try:
            kv = await self._js.create_key_value(
                bucket=config.bucket,
                description=config.description,
                max_bytes=config.max_bytes,
                max_value_size=config.max_value_size,
                history=config.history,
                ttl=config.ttl,
                storage=config.storage.value,
                replicas=config.replicas,
                allow_republish=config.allow_republish,
                deny_delete=config.deny_delete,
                deny_purge=config.deny_purge,
            )
            self._kv_stores[config.bucket] = kv
            logger.info(f"Created KV bucket: {config.bucket}")
            return True
        except Exception as e:
            logger.error(f"Failed to create KV bucket: {e}")
            return False

    def create_kv_bucket_sync(self, config: KVConfig) -> bool:
        """Synchronous create KV bucket."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.create_kv_bucket(config))
        elif self._loop:
            result = [False]
            def create_in_thread():
                result[0] = asyncio.run(self.create_kv_bucket(config))
            thread = threading.Thread(target=create_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.create_kv_bucket(config))

    async def kv_put(self, bucket: str, key: str, value: Any) -> bool:
        """
        Put a value in KV store.

        Args:
            bucket: Bucket name
            key: Key name
            value: Value to store

        Returns:
            True if stored successfully
        """
        if bucket not in self._kv_stores:
            logger.error(f"KV bucket {bucket} not found")
            return False

        try:
            if isinstance(value, bytes):
                data = value
            else:
                data = json.dumps(value).encode("utf-8")
            await self._kv_stores[bucket].put(key, data)
            return True
        except Exception as e:
            logger.error(f"Failed to put KV value: {e}")
            return False

    def kv_put_sync(self, bucket: str, key: str, value: Any) -> bool:
        """Synchronous KV put."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.kv_put(bucket, key, value))
        elif self._loop:
            result = [False]
            def put_in_thread():
                result[0] = asyncio.run(self.kv_put(bucket, key, value))
            thread = threading.Thread(target=put_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.kv_put(bucket, key, value))

    async def kv_get(self, bucket: str, key: str) -> Optional[Any]:
        """
        Get a value from KV store.

        Args:
            bucket: Bucket name
            key: Key name

        Returns:
            Value or None if not found
        """
        if bucket not in self._kv_stores:
            logger.error(f"KV bucket {bucket} not found")
            return None

        try:
            entry = await self._kv_stores[bucket].get(key)
            if entry is None:
                return None
            value = entry.value
            try:
                value = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            return value
        except Exception as e:
            logger.error(f"Failed to get KV value: {e}")
            return None

    def kv_get_sync(self, bucket: str, key: str) -> Optional[Any]:
        """Synchronous KV get."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.kv_get(bucket, key))
        elif self._loop:
            result = [None]
            def get_in_thread():
                result[0] = asyncio.run(self.kv_get(bucket, key))
            thread = threading.Thread(target=get_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.kv_get(bucket, key))

    async def kv_delete(self, bucket: str, key: str) -> bool:
        """
        Delete a key from KV store.

        Args:
            bucket: Bucket name
            key: Key to delete

        Returns:
            True if deleted successfully
        """
        if bucket not in self._kv_stores:
            return False

        try:
            await self._kv_stores[bucket].delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete KV key: {e}")
            return False

    def kv_delete_sync(self, bucket: str, key: str) -> bool:
        """Synchronous KV delete."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.kv_delete(bucket, key))
        elif self._loop:
            result = [False]
            def delete_in_thread():
                result[0] = asyncio.run(self.kv_delete(bucket, key))
            thread = threading.Thread(target=delete_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.kv_delete(bucket, key))

    async def kv_keys(self, bucket: str) -> List[str]:
        """
        Get all keys in a KV bucket.

        Args:
            bucket: Bucket name

        Returns:
            List of keys
        """
        if bucket not in self._kv_stores:
            return []

        try:
            keys = []
            async for entry in self._kv_stores[bucket].iter_keys():
                keys.append(entry)
            return keys
        except Exception as e:
            logger.error(f"Failed to get KV keys: {e}")
            return []

    def kv_keys_sync(self, bucket: str) -> List[str]:
        """Synchronous KV keys."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.kv_keys(bucket))
        elif self._loop:
            result = [[]]
            def keys_in_thread():
                result[0] = asyncio.run(self.kv_keys(bucket))
            thread = threading.Thread(target=keys_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.kv_keys(bucket))

    # =========================================================================
    # Object Store Methods
    # =========================================================================

    async def create_object_bucket(self, config: ObjectConfig) -> bool:
        """
        Create an object store bucket.

        Args:
            config: Object bucket configuration

        Returns:
            True if created successfully
        """
        if not self.is_connected:
            logger.error("Cannot create object bucket: not connected to NATS")
            return False

        try:
            obj = await self._js.create_object_store(
                bucket=config.bucket,
                description=config.description,
                max_bytes=config.max_bytes,
                storage=config.storage.value,
                replicas=config.replicas,
                compression=config.compression,
                allow_delete=config.allow_delete,
                allow_purge=config.allow_purge,
            )
            self._object_stores[config.bucket] = obj
            logger.info(f"Created object bucket: {config.bucket}")
            return True
        except Exception as e:
            logger.error(f"Failed to create object bucket: {e}")
            return False

    def create_object_bucket_sync(self, config: ObjectConfig) -> bool:
        """Synchronous create object bucket."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.create_object_bucket(config))
        elif self._loop:
            result = [False]
            def create_in_thread():
                result[0] = asyncio.run(self.create_object_bucket(config))
            thread = threading.Thread(target=create_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.create_object_bucket(config))

    async def object_put(self, bucket: str, key: str, data: bytes) -> bool:
        """
        Put an object in store.

        Args:
            bucket: Bucket name
            key: Object key
            data: Object data

        Returns:
            True if stored successfully
        """
        if bucket not in self._object_stores:
            logger.error(f"Object bucket {bucket} not found")
            return False

        try:
            info = await self._object_stores[bucket].put(key, data)
            return info is not None
        except Exception as e:
            logger.error(f"Failed to put object: {e}")
            return False

    def object_put_sync(self, bucket: str, key: str, data: bytes) -> bool:
        """Synchronous object put."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.object_put(bucket, key, data))
        elif self._loop:
            result = [False]
            def put_in_thread():
                result[0] = asyncio.run(self.object_put(bucket, key, data))
            thread = threading.Thread(target=put_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.object_put(bucket, key, data))

    async def object_get(self, bucket: str, key: str) -> Optional[bytes]:
        """
        Get an object from store.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Object data or None
        """
        if bucket not in self._object_stores:
            logger.error(f"Object bucket {bucket} not found")
            return None

        try:
            data = await self._object_stores[bucket].get(key)
            return data
        except Exception as e:
            logger.error(f"Failed to get object: {e}")
            return None

    def object_get_sync(self, bucket: str, key: str) -> Optional[bytes]:
        """Synchronous object get."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.object_get(bucket, key))
        elif self._loop:
            result = [None]
            def get_in_thread():
                result[0] = asyncio.run(self.object_get(bucket, key))
            thread = threading.Thread(target=get_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.object_get(bucket, key))

    async def object_delete(self, bucket: str, key: str) -> bool:
        """
        Delete an object from store.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            True if deleted successfully
        """
        if bucket not in self._object_stores:
            return False

        try:
            await self._object_stores[bucket].delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            return False

    def object_delete_sync(self, bucket: str, key: str) -> bool:
        """Synchronous object delete."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.object_delete(bucket, key))
        elif self._loop:
            result = [False]
            def delete_in_thread():
                result[0] = asyncio.run(self.object_delete(bucket, key))
            thread = threading.Thread(target=delete_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.object_delete(bucket, key))

    # =========================================================================
    # Service Discovery Methods
    # =========================================================================

    async def register_service(self, service: ServiceInfo) -> bool:
        """
        Register a service for discovery.

        Args:
            service: Service information

        Returns:
            True if registered successfully
        """
        try:
            service_key = f"services.{service.name}"
            service.last_seen = datetime.now()

            if service.name not in self._services:
                self._services[service.name] = service

            # Store service info in KV
            bucket = "_service_registry"
            if bucket not in self._kv_stores:
                kv_config = KVConfig(bucket=bucket)
                await self.create_kv_bucket(kv_config)

            await self.kv_put(bucket, service.name, {
                "name": service.name,
                "version": service.version,
                "endpoint": service.endpoint,
                "metadata": service.metadata,
                "status": service.status,
                "last_seen": service.last_seen.isoformat(),
                "tags": service.tags,
            })

            # Publish service registration event
            await self.publish(f"services.{service.name}.register", {
                "name": service.name,
                "version": service.version,
                "endpoint": service.endpoint,
            })

            logger.info(f"Registered service: {service.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to register service: {e}")
            return False

    def register_service_sync(self, service: ServiceInfo) -> bool:
        """Synchronous register service."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.register_service(service))
        elif self._loop:
            result = [False]
            def register_in_thread():
                result[0] = asyncio.run(self.register_service(service))
            thread = threading.Thread(target=register_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.register_service(service))

    async def discover_services(self, name: Optional[str] = None) -> List[ServiceInfo]:
        """
        Discover services.

        Args:
            name: Optional service name to filter

        Returns:
            List of discovered services
        """
        try:
            bucket = "_service_registry"
            if bucket not in self._kv_stores:
                return []

            services = []
            keys = await self.kv_keys(bucket)

            for key in keys:
                if name and key != name:
                    continue

                data = await self.kv_get(bucket, key)
                if data:
                    service = ServiceInfo(
                        name=data.get("name", key),
                        version=data.get("version", "unknown"),
                        endpoint=data.get("endpoint", ""),
                        metadata=data.get("metadata", {}),
                        status=data.get("status", "unknown"),
                        tags=data.get("tags", []),
                    )
                    services.append(service)

            return services
        except Exception as e:
            logger.error(f"Failed to discover services: {e}")
            return []

    def discover_services_sync(self, name: Optional[str] = None) -> List[ServiceInfo]:
        """Synchronous discover services."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.discover_services(name))
        elif self._loop:
            result = [[]]
            def discover_in_thread():
                result[0] = asyncio.run(self.discover_services(name))
            thread = threading.Thread(target=discover_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.discover_services(name))

    async def deregister_service(self, name: str) -> bool:
        """
        Deregister a service.

        Args:
            name: Service name

        Returns:
            True if deregistered successfully
        """
        try:
            if name in self._services:
                del self._services[name]

            bucket = "_service_registry"
            if bucket in self._kv_stores:
                await self.kv_delete(bucket, name)

            await self.publish(f"services.{name}.deregister", {"name": name})
            logger.info(f"Deregistered service: {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to deregister service: {e}")
            return False

    def deregister_service_sync(self, name: str) -> bool:
        """Synchronous deregister service."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.deregister_service(name))
        elif self._loop:
            result = [False]
            def deregister_in_thread():
                result[0] = asyncio.run(self.deregister_service(name))
            thread = threading.Thread(target=deregister_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.deregister_service(name))

    # =========================================================================
    # Service Monitoring Methods
    # =========================================================================

    async def publish_health_status(self, service_name: str, status: str, metadata: Optional[Dict] = None) -> bool:
        """
        Publish service health status.

        Args:
            service_name: Name of the service
            status: Health status (healthy, degraded, unhealthy)
            metadata: Optional additional metadata

        Returns:
            True if published successfully
        """
        return await self.publish(f"services.{service_name}.health", {
            "service": service_name,
            "status": status,
            "metadata": metadata or {},
            "timestamp": datetime.now().isoformat(),
        })

    def publish_health_status_sync(self, service_name: str, status: str, metadata: Optional[Dict] = None) -> bool:
        """Synchronous publish health status."""
        return self.publish_sync(f"services.{service_name}.health", {
            "service": service_name,
            "status": status,
            "metadata": metadata or {},
            "timestamp": datetime.now().isoformat(),
        })

    async def subscribe_to_health_events(
        self,
        callback: Callable[[str, Dict], Awaitable[None]]
    ) -> Optional[str]:
        """
        Subscribe to service health events.

        Args:
            callback: Async callback function(subject, payload)

        Returns:
            Subscription ID
        """
        return await self.subscribe("services.>", callback)

    def subscribe_to_health_events_sync(
        self,
        callback: Callable[[str, Dict], None]
    ) -> Optional[str]:
        """Synchronous subscribe to health events."""
        async def async_callback(subj, payload):
            callback(subj, payload)

        if self._loop and not self._loop.is_running():
            return asyncio.run(self.subscribe_to_health_events(async_callback))
        elif self._loop:
            result = [None]
            def sub_in_thread():
                result[0] = asyncio.run(self.subscribe_to_health_events(async_callback))
            thread = threading.Thread(target=sub_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.subscribe_to_health_events(async_callback))

    # =========================================================================
    # Clustering Methods
    # =========================================================================

    async def get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the NATS cluster.

        Returns:
            Cluster information dictionary
        """
        if not self.is_connected:
            return None

        try:
            info = self._client.server_info
            return {
                "server_id": info.get("server_id", ""),
                "version": info.get("version", ""),
                "go": info.get("go", ""),
                "host": info.get("host", ""),
                "port": info.get("port", 0),
                "auth_required": info.get("auth_required", False),
                "tls_required": info.get("tls_required", False),
                "client_id": info.get("client_id", 0),
                "client_ip": info.get("client_ip", ""),
                "cluster": info.get("cluster", ""),
                "cluster_nodes": info.get("cluster_nodes", []),
            }
        except Exception as e:
            logger.error(f"Failed to get cluster info: {e}")
            return None

    def get_cluster_info_sync(self) -> Optional[Dict[str, Any]]:
        """Synchronous get cluster info."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.get_cluster_info())
        elif self._loop:
            result = [None]
            def info_in_thread():
                result[0] = asyncio.run(self.get_cluster_info())
            thread = threading.Thread(target=info_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.get_cluster_info())

    async def get_server_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get server statistics.

        Returns:
            Server statistics dictionary
        """
        if not self.is_connected:
            return None

        try:
            info = self._client.server_info
            return {
                "server_id": info.get("server_id", ""),
                "uptime": info.get("uptime", 0),
                "mem": info.get("mem", 0),
                "cores": info.get("cores", 0),
                "connections": info.get("connections", 0),
                "subscriptions": info.get("subscriptions", 0),
                "slow_consumers": info.get("slow_consumers", 0),
            }
        except Exception as e:
            logger.error(f"Failed to get server stats: {e}")
            return None

    def get_server_stats_sync(self) -> Optional[Dict[str, Any]]:
        """Synchronous get server stats."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.get_server_stats())
        elif self._loop:
            result = [None]
            def stats_in_thread():
                result[0] = asyncio.run(self.get_server_stats())
            thread = threading.Thread(target=stats_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.get_server_stats())

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def flush(self, timeout: float = 5.0) -> bool:
        """
        Flush pending messages.

        Args:
            timeout: Flush timeout

        Returns:
            True if flushed successfully
        """
        if not self.is_connected:
            return False

        try:
            await self._client.flush(timeout=timeout)
            return True
        except Exception as e:
            logger.error(f"Failed to flush: {e}")
            return False

    def flush_sync(self, timeout: float = 5.0) -> bool:
        """Synchronous flush."""
        if self._loop and not self._loop.is_running():
            return asyncio.run(self.flush(timeout))
        elif self._loop:
            result = [False]
            def flush_in_thread():
                result[0] = asyncio.run(self.flush(timeout))
            thread = threading.Thread(target=flush_in_thread)
            thread.start()
            thread.join()
            return result[0]
        else:
            return asyncio.run(self.flush(timeout))

    async def close(self):
        """Close all connections and cleanup."""
        await self.disconnect()
        with self._lock:
            self._subscriptions.clear()
            self._js_contexts.clear()
            self._kv_stores.clear()
            self._object_stores.clear()
            self._services.clear()

    def close_sync(self):
        """Synchronous close."""
        if self._loop and not self._loop.is_running():
            asyncio.run(self.close())
        elif self._loop:
            def close_in_thread():
                asyncio.set_event_loop(self._loop)
                asyncio.run(self.close())
            thread = threading.Thread(target=close_in_thread)
            thread.start()
            thread.join()
        else:
            asyncio.run(self.close())


# Convenience functions for quick operations

async def quick_publish(servers: List[str], subject: str, payload: Any) -> bool:
    """
    Quick publish without creating an integration instance.

    Args:
        servers: List of NATS server URLs
        subject: Subject to publish to
        payload: Message payload

    Returns:
        True if published successfully
    """
    config = NATSConfig(servers=servers)
    integration = NATSIntegration(config)
    try:
        await integration.connect()
        return await integration.publish(subject, payload)
    finally:
        await integration.disconnect()


async def quick_request(servers: List[str], subject: str, payload: Any, timeout: float = 5.0) -> Optional[Any]:
    """
    Quick request without creating an integration instance.

    Args:
        servers: List of NATS server URLs
        subject: Subject to send request to
        payload: Request payload
        timeout: Request timeout

    Returns:
        Response payload
    """
    config = NATSConfig(servers=servers)
    integration = NATSIntegration(config)
    try:
        await integration.connect()
        return await integration.request(subject, payload, timeout)
    finally:
        await integration.disconnect()
