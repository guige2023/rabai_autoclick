"""MQTT action module for RabAI AutoClick.

Provides MQTT client operations for IoT messaging.
"""

import json
import sys
import os
import time
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MQTTAction(BaseAction):
    """MQTT client for IoT pub/sub messaging.
    
    Supports publishing messages, subscribing to topics,
    QoS levels, retained messages, and last-will testimonials.
    """
    action_type = "mqtt"
    display_name = "MQTT客户端"
    description = "MQTT物联网消息发布与订阅"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[Any] = None
        self._messages: List[Dict] = []
    
    def _get_mqtt(self):
        """Import paho.mqtt."""
        try:
            import paho.mqtt.client as mqtt
            return mqtt
        except ImportError:
            return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute MQTT operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'connect', 'disconnect', 'publish', 'subscribe', 'unsubscribe', 'messages'
                - broker: MQTT broker URL (e.g., 'mqtt://localhost:1883')
                - client_id: MQTT client ID
                - topic: Topic for publish/subscribe
                - message: Message payload (for publish)
                - qos: QoS level 0/1/2 (default 0)
                - retain: Retain message flag (default False)
                - keepalive: Keepalive interval in seconds
                - username: Broker username
                - password: Broker password
        
        Returns:
            ActionResult with operation result.
        """
        mqtt_lib = self._get_mqtt()
        if mqtt_lib is None:
            return ActionResult(
                success=False,
                message="Requires paho-mqtt. Install: pip install paho-mqtt"
            )
        
        command = params.get('command', 'connect')
        broker = params.get('broker', 'mqtt://localhost:1883')
        client_id = params.get('client_id', f'rabai_{int(time.time())}')
        topic = params.get('topic')
        message = params.get('message')
        qos = params.get('qos', 0)
        retain = params.get('retain', False)
        keepalive = params.get('keepalive', 60)
        username = params.get('username')
        password = params.get('password')
        
        broker_host = broker.replace('mqtt://', '').replace('tcp://', '').split(':')[0]
        broker_port = int(broker.split(':')[-1]) if ':' in broker else 1883
        
        if command == 'connect':
            return self._mqtt_connect(mqtt_lib, broker_host, broker_port, client_id, keepalive, username, password)
        
        if command == 'disconnect':
            return self._mqtt_disconnect()
        
        if command == 'publish':
            if not topic:
                return ActionResult(success=False, message="topic required for publish")
            return self._mqtt_publish(topic, message, qos, retain)
        
        if command == 'subscribe':
            if not topic:
                return ActionResult(success=False, message="topic required for subscribe")
            return self._mqtt_subscribe(mqtt_lib, broker_host, broker_port, client_id, keepalive, username, password, topic, qos)
        
        if command == 'unsubscribe':
            if not topic:
                return ActionResult(success=False, message="topic required for unsubscribe")
            return self._mqtt_unsubscribe(topic)
        
        if command == 'messages':
            return ActionResult(
                success=True,
                message=f"Received {len(self._messages)} messages",
                data={'messages': self._messages[-50:], 'count': len(self._messages)}
            )
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _mqtt_connect(self, mqtt_lib: Any, host: str, port: int, client_id: str, keepalive: int, username: Optional[str], password: Optional[str]) -> ActionResult:
        """Connect to MQTT broker."""
        try:
            client = mqtt_lib.Client(client_id=client_id, clean_session=True)
            if username and password:
                client.username_pw_set(username, password)
            
            client.on_message = self._on_message
            client.connect(host, port, keepalive=keepalive)
            client.loop_start()
            self._client = client
            
            return ActionResult(
                success=True,
                message=f"Connected to {host}:{port}",
                data={'broker': f'{host}:{port}', 'client_id': client_id}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to connect: {e}")
    
    def _mqtt_disconnect(self) -> ActionResult:
        """Disconnect from MQTT broker."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
            return ActionResult(success=True, message="Disconnected")
        return ActionResult(success=True, message="Not connected")
    
    def _mqtt_publish(self, topic: str, message: Any, qos: int, retain: bool) -> ActionResult:
        """Publish message to topic."""
        if not self._client:
            return ActionResult(success=False, message="Not connected. Run 'connect' first.")
        
        try:
            if not isinstance(message, str):
                payload = json.dumps(message) if isinstance(message, (dict, list)) else str(message)
            else:
                payload = message
            
            info = self._client.publish(topic, payload, qos, retain)
            return ActionResult(
                success=True,
                message=f"Published to {topic} (QoS {qos})",
                data={'topic': topic, 'qos': info.qos, 'mid': info.mid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to publish: {e}")
    
    def _mqtt_subscribe(self, mqtt_lib: Any, host: str, port: int, client_id: str, keepalive: int, username: Optional[str], password: Optional[str], topic: str, qos: int) -> ActionResult:
        """Subscribe to topic."""
        if not self._client:
            client = mqtt_lib.Client(client_id=client_id, clean_session=True)
            if username and password:
                client.username_pw_set(username, password)
            client.on_message = self._on_message
            client.connect(host, port, keepalive=keepalive)
            client.loop_start()
            self._client = client
        
        try:
            result = self._client.subscribe(topic, qos)
            return ActionResult(
                success=True,
                message=f"Subscribed to {topic} (QoS {qos})",
                data={'topic': topic, 'qos': qos}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to subscribe: {e}")
    
    def _mqtt_unsubscribe(self, topic: str) -> ActionResult:
        """Unsubscribe from topic."""
        if not self._client:
            return ActionResult(success=False, message="Not connected")
        
        try:
            self._client.unsubscribe(topic)
            return ActionResult(success=True, message=f"Unsubscribed from {topic}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to unsubscribe: {e}")
    
    def _on_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Callback for incoming messages."""
        try:
            payload = msg.payload.decode('utf-8')
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass
            self._messages.append({
                'topic': msg.topic,
                'payload': payload,
                'qos': msg.qos,
                'retain': msg.retain,
                'timestamp': time.time()
            })
            if len(self._messages) > 1000:
                self._messages = self._messages[-500:]
        except Exception:
            pass
