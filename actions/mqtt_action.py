"""
MQTT Action Module.

Provides MQTT client capabilities for IoT messaging and pub/sub communication.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class MQTTQOS(Enum):
    """MQTT quality of service levels."""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


@dataclass
class MQTTMessage:
    """MQTT message structure."""
    topic: str
    payload: Any
    qos: MQTTQOS = MQTTQOS.AT_MOST_ONCE
    retain: bool = False
    message_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class Subscription:
    """MQTT subscription record."""
    topic: str
    qos: MQTTQOS
    handler: Callable[[MQTTMessage], None]
    subscriber_id: str


@dataclass
class MQTTConfig:
    """MQTT client configuration."""
    broker_host: str = "localhost"
    broker_port: int = 1883
    client_id: str = ""
    keepalive: int = 60
    clean_session: bool = True
    username: Optional[str] = None
    password: Optional[str] = None
    tls_enabled: bool = False
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0


class MQTTAction:
    """
    MQTT action handler.
    
    Provides MQTT client for IoT messaging and pub/sub.
    
    Example:
        mqtt = MQTTAction(config=cfg)
        mqtt.connect()
        mqtt.subscribe("sensors/#", handler)
        mqtt.publish("sensors/temp", {"value": 25})
    """
    
    def __init__(self, config: Optional[MQTTConfig] = None):
        """
        Initialize MQTT handler.
        
        Args:
            config: MQTT configuration
        """
        self.config = config or MQTTConfig()
        self._connected = False
        self._subscriptions: Dict[str, Subscription] = {}
        self._message_queue: List[MQTTMessage] = []
        self._lock = threading.RLock()
        self._pending_acks: Dict[str, Callable] = {}
    
    def connect(self) -> bool:
        """
        Connect to MQTT broker.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(
                f"Connecting to MQTT broker: "
                f"{self.config.broker_host}:{self.config.broker_port}"
            )
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"MQTT connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from MQTT broker.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            if self._connected:
                self._connected = False
                logger.info("Disconnected from MQTT broker")
                return True
            return False
    
    def is_connected(self) -> bool:
        """Check if connected to broker."""
        return self._connected
    
    def publish(
        self,
        topic: str,
        payload: Any,
        qos: MQTTQOS = MQTTQOS.AT_MOST_ONCE,
        retain: bool = False
    ) -> bool:
        """
        Publish a message to a topic.
        
        Args:
            topic: Target topic
            payload: Message payload
            qos: Quality of service level
            retain: Whether to retain message
            
        Returns:
            True if published successfully
        """
        if not self._connected:
            logger.warning("Not connected to MQTT broker")
            return False
        
        message = MQTTMessage(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            message_id=self._generate_id()
        )
        
        logger.debug(f"Published to {topic}: {payload}")
        return True
    
    def subscribe(
        self,
        topic: str,
        handler: Callable[[MQTTMessage], None],
        qos: MQTTQOS = MQTTQOS.AT_MOST_ONCE,
        subscriber_id: Optional[str] = None
    ) -> bool:
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic pattern (supports wildcards)
            handler: Message handler function
            qos: Quality of service level
            subscriber_id: Optional subscriber ID
            
        Returns:
            True if subscribed successfully
        """
        if not self._connected:
            logger.warning("Not connected to MQTT broker")
            return False
        
        subscriber_id = subscriber_id or self._generate_id()
        
        with self._lock:
            self._subscriptions[topic] = Subscription(
                topic=topic,
                qos=qos,
                handler=handler,
                subscriber_id=subscriber_id
            )
        
        logger.info(f"Subscribed to: {topic}")
        return True
    
    def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: Topic to unsubscribe from
            
        Returns:
            True if unsubscribed
        """
        with self._lock:
            if topic in self._subscriptions:
                del self._subscriptions[topic]
                logger.info(f"Unsubscribed from: {topic}")
                return True
        return False
    
    def publish_multiple(
        self,
        messages: List[Dict[str, Any]]
    ) -> int:
        """
        Publish multiple messages.
        
        Args:
            messages: List of message specifications
            
        Returns:
            Number of messages published
        """
        count = 0
        for msg in messages:
            success = self.publish(
                topic=msg.get("topic", ""),
                payload=msg.get("payload"),
                qos=MQTTQOS(msg.get("qos", 0)),
                retain=msg.get("retain", False)
            )
            if success:
                count += 1
        return count
    
    def get_subscriptions(self) -> List[Dict[str, Any]]:
        """
        Get all active subscriptions.
        
        Returns:
            List of subscription info
        """
        with self._lock:
            return [
                {
                    "topic": sub.topic,
                    "qos": sub.qos.value,
                    "subscriber_id": sub.subscriber_id
                }
                for sub in self._subscriptions.values()
            ]
    
    def message_loop(self, timeout: Optional[float] = None) -> None:
        """
        Start processing messages (blocking).
        
        Args:
            timeout: Optional timeout in seconds
        """
        start = time.time()
        
        while self._connected:
            if timeout and (time.time() - start) > timeout:
                break
            
            with self._lock:
                while self._message_queue:
                    msg = self._message_queue.pop(0)
                    self._deliver_message(msg)
            
            time.sleep(0.1)
    
    def _deliver_message(self, message: MQTTMessage) -> None:
        """Deliver message to matching subscriptions."""
        for sub in self._subscriptions.values():
            if self._topic_matches(message.topic, sub.topic):
                try:
                    sub.handler(message)
                except Exception as e:
                    logger.error(f"Message handler failed: {e}")
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches subscription pattern."""
        if "#" in pattern:
            prefix = pattern.split("#")[0].rstrip("/")
            return topic.startswith(prefix)
        if "+" in pattern:
            parts = pattern.split("/")
            topic_parts = topic.split("/")
            for i, part in enumerate(parts):
                if part == "+":
                    continue
                if i >= len(topic_parts) or part != topic_parts[i]:
                    return False
            return True
        return topic == pattern
    
    def _generate_id(self) -> str:
        """Generate unique ID."""
        return f"{time.time()}_{id(self)}"
    
    def get_message_count(self) -> int:
        """Get number of queued messages."""
        with self._lock:
            return len(self._message_queue)
    
    def clear_message_queue(self) -> int:
        """
        Clear the message queue.
        
        Returns:
            Number of messages cleared
        """
        with self._lock:
            count = len(self._message_queue)
            self._message_queue.clear()
            return count
