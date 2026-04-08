"""
Protocol MQTT action for MQTT message queue communication.

This module provides actions for MQTT client operations including
publish, subscribe, and message handling.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class QoSLevel(Enum):
    """MQTT Quality of Service levels."""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class ConnectionStatus(Enum):
    """MQTT connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


@dataclass
class MQTTMessage:
    """Represents an MQTT message."""
    topic: str
    payload: Any
    qos: QoSLevel = QoSLevel.AT_MOST_ONCE
    retain: bool = False
    message_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "topic": self.topic,
            "payload": self.payload,
            "qos": self.qos.value,
            "retain": self.retain,
            "message_id": self.message_id,
            "timestamp": self.timestamp.isoformat(),
            "properties": self.properties,
        }


@dataclass
class MQTTConfig:
    """Configuration for MQTT client."""
    broker_host: str = "localhost"
    broker_port: int = 1883
    client_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    keepalive: int = 60
    clean_session: bool = True
    qos_default: QoSLevel = QoSLevel.AT_MOST_ONCE
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0
    max_reconnect_attempts: int = 10
    tls_enabled: bool = False
    tls_ca_certs: Optional[str] = None
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None


class MQTTClient:
    """
    MQTT client for message publishing and subscribing.

    Provides both callback-based and polling-based message handling.
    """

    def __init__(self, config: Optional[MQTTConfig] = None):
        """
        Initialize the MQTT client.

        Args:
            config: MQTT configuration.
        """
        self.config = config or MQTTConfig()
        self._status = ConnectionStatus.DISCONNECTED
        self._client = None
        self._subscriptions: Dict[str, Tuple[QoSLevel, Callable]] = {}
        self._message_queue: List[MQTTMessage] = []
        self._lock = threading.RLock()
        self._running = False
        self._receive_thread: Optional[threading.Thread] = None

    def connect(self) -> bool:
        """
        Connect to the MQTT broker.

        Returns:
            True if connected successfully.
        """
        try:
            import paho.mqtt.client as mqtt

            self._status = ConnectionStatus.CONNECTING

            client_id = self.config.client_id or f"mqtt_client_{int(time.time())}"
            self._client = mqtt.Client(
                client_id=client_id,
                clean_session=self.config.clean_session,
            )

            if self.config.username and self.config.password:
                self._client.username_pw_set(
                    self.config.username,
                    self.config.password,
                )

            if self.config.tls_enabled:
                self._client.tls_set(
                    ca_certs=self.config.tls_ca_certs,
                    certfile=self.config.tls_certfile,
                    keyfile=self.config.tls_keyfile,
                )

            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message = self._on_message
            self._client.on_subscribe = self._on_subscribe

            self._client.connect(
                self.config.broker_host,
                self.config.broker_port,
                self.config.keepalive,
            )

            self._client.loop_start()
            self._running = True

            timeout = 10
            start = time.time()
            while self._status == ConnectionStatus.CONNECTING and time.time() - start < timeout:
                time.sleep(0.1)

            return self._status == ConnectionStatus.CONNECTED

        except ImportError:
            raise ImportError(
                "paho-mqtt is required for MQTT support. "
                "Install with: pip install paho-mqtt"
            )
        except Exception as e:
            self._status = ConnectionStatus.ERROR
            raise ConnectionError(f"Failed to connect to MQTT broker: {e}")

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        self._running = False
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
        self._status = ConnectionStatus.DISCONNECTED

    def publish(
        self,
        topic: str,
        payload: Any,
        qos: Optional[QoSLevel] = None,
        retain: bool = False,
    ) -> bool:
        """
        Publish a message to a topic.

        Args:
            topic: Topic to publish to.
            payload: Message payload.
            qos: Quality of Service level.
            retain: Whether to retain the message.

        Returns:
            True if published successfully.

        Raises:
            ConnectionError: If not connected.
        """
        if self._status != ConnectionStatus.CONNECTED:
            raise ConnectionError("Not connected to MQTT broker")

        qos = qos or self.config.qos_default

        if isinstance(payload, (dict, list)):
            payload_str = json.dumps(payload)
        else:
            payload_str = str(payload)

        try:
            result = self._client.publish(
                topic,
                payload_str,
                qos=qos.value,
                retain=retain,
            )
            return result.rc == 0
        except Exception as e:
            raise ConnectionError(f"Failed to publish message: {e}")

    def subscribe(
        self,
        topic: str,
        callback: Callable[[MQTTMessage], None],
        qos: Optional[QoSLevel] = None,
    ) -> bool:
        """
        Subscribe to a topic.

        Args:
            topic: Topic to subscribe to.
            callback: Function to call when message received.
            qos: Quality of Service level.

        Returns:
            True if subscribed successfully.
        """
        if self._status != ConnectionStatus.CONNECTED:
            raise ConnectionError("Not connected to MQTT broker")

        qos = qos or self.config.qos_default

        try:
            result, _ = self._client.subscribe(topic, qos.value)
            if result == 0:
                self._subscriptions[topic] = (qos, callback)
                return True
            return False
        except Exception as e:
            raise ConnectionError(f"Failed to subscribe: {e}")

    def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic.

        Args:
            topic: Topic to unsubscribe from.

        Returns:
            True if unsubscribed successfully.
        """
        if self._status != ConnectionStatus.CONNECTED:
            raise ConnectionError("Not connected to MQTT broker")

        try:
            result, _ = self._client.unsubscribe(topic)
            if result == 0:
                self._subscriptions.pop(topic, None)
                return True
            return False
        except Exception:
            return False

    def get_messages(
        self,
        topic: Optional[str] = None,
        timeout: float = 0,
        max_count: int = 100,
    ) -> List[MQTTMessage]:
        """
        Get messages from the queue (polling mode).

        Args:
            topic: Optional topic filter.
            timeout: How long to wait for messages.
            max_count: Maximum number of messages to return.

        Returns:
            List of MQTTMessage objects.
        """
        deadline = time.time() + timeout if timeout > 0 else None

        with self._lock:
            while not self._message_queue:
                if deadline and time.time() >= deadline:
                    return []
                if timeout == 0:
                    break
                self._lock.wait(0.1)

            messages = self._message_queue
            if topic:
                messages = [m for m in messages if self._match_topic(m.topic, topic)]
            messages = messages[:max_count]
            self._message_queue = self._message_queue[len(messages):]

        return messages

    def _match_topic(self, topic: str, pattern: str) -> bool:
        """Check if topic matches a pattern."""
        import fnmatch
        return fnmatch.fnmatch(topic, pattern)

    def _on_connect(
        self,
        client,
        userdata,
        flags,
        rc,
    ) -> None:
        """Handle connect event."""
        if rc == 0:
            self._status = ConnectionStatus.CONNECTED
            for topic, (qos, _) in self._subscriptions.items():
                client.subscribe(topic, qos.value)
        else:
            self._status = ConnectionStatus.ERROR

    def _on_disconnect(self, client, userdata, rc) -> None:
        """Handle disconnect event."""
        self._status = ConnectionStatus.DISCONNECTED
        if self.config.auto_reconnect and self._running:
            self._attempt_reconnect()

    def _on_message(self, client, userdata, msg) -> None:
        """Handle incoming message."""
        try:
            payload = msg.payload.decode("utf-8")
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                pass

            message = MQTTMessage(
                topic=msg.topic,
                payload=payload,
                qos=QoSLevel(msg.qos),
                retain=msg.retain,
                message_id=str(msg.mid),
            )

            with self._lock:
                self._message_queue.append(message)

            if msg.topic in self._subscriptions:
                _, callback = self._subscriptions[msg.topic]
                try:
                    callback(message)
                except Exception:
                    pass

        except Exception:
            pass

    def _on_subscribe(self, client, userdata, mid, granted_qos) -> None:
        """Handle subscribe event."""
        pass

    def _attempt_reconnect(self) -> None:
        """Attempt to reconnect to the broker."""
        for attempt in range(self.config.max_reconnect_attempts):
            if not self._running:
                break
            try:
                time.sleep(self.config.reconnect_delay)
                self._client.reconnect()
                break
            except Exception:
                continue

    @property
    def status(self) -> ConnectionStatus:
        """Get current connection status."""
        return self._status

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def mqtt_publish_action(
    topic: str,
    payload: Any,
    broker_host: str = "localhost",
    broker_port: int = 1883,
    qos: int = 0,
) -> Dict[str, Any]:
    """
    Action function to publish an MQTT message.

    Args:
        topic: Topic to publish to.
        payload: Message payload.
        broker_host: MQTT broker host.
        broker_port: MQTT broker port.
        qos: Quality of Service level (0, 1, or 2).

    Returns:
        Dictionary with publish result.
    """
    config = MQTTConfig(
        broker_host=broker_host,
        broker_port=broker_port,
    )

    qos_level = QoSLevel(qos)

    client = MQTTClient(config)
    try:
        client.connect()
        success = client.publish(topic, payload, qos_level)
        return {
            "success": success,
            "topic": topic,
            "timestamp": datetime.now().isoformat(),
        }
    finally:
        client.disconnect()


def mqtt_subscribe_action(
    topic: str,
    broker_host: str = "localhost",
    broker_port: int = 1883,
    qos: int = 0,
    timeout: float = 5.0,
) -> List[Dict[str, Any]]:
    """
    Action function to subscribe and receive MQTT messages.

    Args:
        topic: Topic to subscribe to.
        broker_host: MQTT broker host.
        broker_port: MQTT broker port.
        qos: Quality of Service level.
        timeout: How long to wait for messages.

    Returns:
        List of received messages.
    """
    config = MQTTConfig(
        broker_host=broker_host,
        broker_port=broker_port,
    )

    qos_level = QoSLevel(qos)
    messages = []

    def on_message(msg: MQTTMessage):
        messages.append(msg.to_dict())

    client = MQTTClient(config)
    try:
        client.connect()
        client.subscribe(topic, on_message, qos_level)
        received = client.get_messages(topic, timeout=timeout)
        return [m.to_dict() for m in received]
    finally:
        client.disconnect()
