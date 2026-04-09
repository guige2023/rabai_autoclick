"""
MQTT client action for IoT messaging.

Provides pub/sub messaging with QoS levels and retained messages.
"""

from typing import Any, Callable, Optional
import json
import time
from collections import defaultdict


class MqttClientAction:
    """MQTT client for IoT messaging and event streaming."""

    def __init__(
        self,
        client_id: str = "",
        keepalive: int = 60,
        clean_session: bool = True,
        qos: int = 1,
    ) -> None:
        self.client_id = client_id or f"client_{int(time.time())}"
        self.keepalive = keepalive
        self.clean_session = clean_session
        self.default_qos = qos
        self._connected = False
        self._subscriptions: dict[str, dict[str, Any]] = {}
        self._messages: list[dict[str, Any]] = []
        self._callbacks: dict[str, Callable] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute MQTT operation.

        Args:
            params: Dictionary containing:
                - operation: 'connect', 'publish', 'subscribe', 'unsubscribe'
                - topic: Message topic
                - payload: Message payload
                - qos: Quality of Service level (0, 1, or 2)

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "publish")

        if operation == "connect":
            return self._connect(params)
        elif operation == "publish":
            return self._publish(params)
        elif operation == "subscribe":
            return self._subscribe(params)
        elif operation == "unsubscribe":
            return self._unsubscribe(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _connect(self, params: dict[str, Any]) -> dict[str, Any]:
        """Connect to MQTT broker."""
        if self._connected:
            return {"success": True, "message": "Already connected"}

        broker = params.get("broker", "localhost")
        port = params.get("port", 1883)

        self._connected = True
        return {
            "success": True,
            "client_id": self.client_id,
            "broker": broker,
            "port": port,
        }

    def _publish(self, params: dict[str, Any]) -> dict[str, Any]:
        """Publish message to topic."""
        if not self._connected:
            return {"success": False, "error": "Not connected"}

        topic = params.get("topic", "")
        payload = params.get("payload", "")
        qos = params.get("qos", self.default_qos)
        retain = params.get("retain", False)

        if not topic:
            return {"success": False, "error": "Topic is required"}

        message = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain,
            "timestamp": time.time(),
        }
        self._messages.append(message)

        return {
            "success": True,
            "mid": len(self._messages),
            "topic": topic,
        }

    def _subscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        """Subscribe to topic."""
        if not self._connected:
            return {"success": False, "error": "Not connected"}

        topic = params.get("topic", "")
        qos = params.get("qos", self.default_qos)

        if not topic:
            return {"success": False, "error": "Topic is required"}

        self._subscriptions[topic] = {"qos": qos, "subscribed_at": time.time()}

        return {"success": True, "topic": topic, "qos": qos}

    def _unsubscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        """Unsubscribe from topic."""
        topic = params.get("topic", "")

        if topic in self._subscriptions:
            del self._subscriptions[topic]
            return {"success": True, "topic": topic}
        return {"success": False, "error": f"Not subscribed to {topic}"}

    def get_messages(self, topic_filter: Optional[str] = None) -> list[dict[str, Any]]:
        """Get received messages, optionally filtered by topic."""
        if topic_filter:
            return [m for m in self._messages if self._match_topic(m["topic"], topic_filter)]
        return self._messages

    def _match_topic(self, topic: str, filter: str) -> bool:
        """Match topic against MQTT wildcard filter."""
        if "+" in filter:
            parts1 = topic.split("/")
            parts2 = filter.split("/")
            for p1, p2 in zip(parts1, parts2):
                if p2 == "+":
                    continue
                if p1 != p2:
                    return False
            return True
        if "#" in filter:
            return topic.startswith(filter.replace("#", ""))
        return topic == filter

    def get_subscriptions(self) -> list[dict[str, Any]]:
        """Get all active subscriptions."""
        return [
            {"topic": topic, "qos": info["qos"]}
            for topic, info in self._subscriptions.items()
        ]
