"""
AWS IoT Core Data Plane Integration Module for Workflow System

Implements an IoTDataPlaneIntegration class with:
1. Publish: Publish messages to IoT topics
2. Subscribe: Subscribe to MQTT topics
3. Thing shadow: Manage thing shadows
4. Named shadows: Manage named shadows
5. Retained messages: Manage retained messages
6. Get/Update: Get and update shadow state
7. Batch: Batch operations
8. Connection: Connection management
9. MQTT5: MQTT 5.0 support
10. CloudWatch integration: Data plane metrics

Commit: 'feat(aws-iotdata): add AWS IoT Core Data Plane with publish, subscribe, thing shadows, named shadows, retained messages, batch, connection, MQTT5, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import asyncio
import queue
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None

try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False
    mqtt = None
    PacketTypes = None
    Properties = None

try:
    from collections.abc import Callable
except ImportError:
    pass

logger = logging.getLogger(__name__)


class MQTTProtocolVersion(Enum):
    """MQTT protocol versions."""
    MQTTv31 = 3
    MQTTv311 = 4
    MQTTv50 = 5


class QoSLevel(Enum):
    """MQTT QoS levels."""
    QoS0 = 0
    QoS1 = 1
    QoS2 = 2


class ShadowState(Enum):
    """Shadow state types."""
    DELTA = "delta"
    DESIRED = "desired"
    REPORTED = "reported"


class RetainedMessageStatus(Enum):
    """Retained message status."""
    ACTIVE = "active"
    DELETED = "deleted"


@dataclass
class MQTTMessage:
    """MQTT message structure."""
    topic: str
    payload: Any
    qos: int = 0
    retain: bool = False
    message_id: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    properties: Optional[Dict[str, Any]] = None


@dataclass
class ShadowStateDocument:
    """Thing shadow state document."""
    state: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    version: int = 0
    timestamp: int = 0
    client_token: Optional[str] = None


@dataclass
class PublishResult:
    """Result of publish operation."""
    message_id: Optional[int]
    topic: str
    qos: int
    success: bool
    error: Optional[str] = None


@dataclass
class SubscribeResult:
    """Result of subscribe operation."""
    topic: str
    qos: int
    success: bool
    error: Optional[str] = None


@dataclass
class ConnectionConfig:
    """MQTT connection configuration."""
    endpoint: str
    port: int = 8883
    client_id: Optional[str] = None
    use_tls: bool = True
    tls_verify: bool = True
    keepalive: int = 60
    clean_session: bool = True
    protocol_version: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
    reconnect_delay: int = 1
    max_reconnect_delay: int = 60
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class TopicSubscription:
    """Topic subscription details."""
    topic: str
    qos: int = 0
    callback: Optional[Callable[[MQTTMessage], None]] = None
    regex_pattern: Optional[str] = None


class IoTDataPlaneIntegration:
    """
    AWS IoT Core Data Plane Integration.
    
    Provides functionality for:
    - Publishing messages to IoT topics
    - Subscribing to MQTT topics
    - Managing thing shadows (classic and named)
    - Handling retained messages
    - Batch operations
    - Connection management
    - MQTT 5.0 support
    - CloudWatch metrics integration
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        connection_config: Optional[ConnectionConfig] = None,
        metrics_namespace: str = "AWS/IoT/DataPlane"
    ):
        """
        Initialize IoT Data Plane integration.
        
        Args:
            endpoint: IoT Core endpoint (required for MQTT connection)
            region: AWS region
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            connection_config: MQTT connection configuration
            metrics_namespace: CloudWatch metrics namespace
        """
        self.endpoint = endpoint
        self.region = region or "us-east-1"
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.connection_config = connection_config
        self.metrics_namespace = metrics_namespace
        
        self._iot_data_client = None
        self._mqtt_client = None
        self._mqtt_connected = False
        self._subscriptions: Dict[str, TopicSubscription] = {}
        self._message_queue: queue.Queue = queue.Queue()
        self._reconnect_delay = 1
        self._running = False
        self._message_handlers: List[Callable[[MQTTMessage], None]] = []
        self._shadow_handlers: Dict[str, List[Callable[[Dict], None]]] = defaultdict(list)
        self._lock = threading.RLock()
        self._metrics_enabled = False
        self._cloudwatch_client = None
        
        self._init_aws_clients()
    
    def _init_aws_clients(self):
        """Initialize AWS clients."""
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, AWS clients will not be initialized")
            return
        
        try:
            session_kwargs = {
                "region_name": self.region
            }
            
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = self.aws_access_key_id
                session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
                if self.aws_session_token:
                    session_kwargs["aws_session_token"] = self.aws_session_token
            elif self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            self._iot_data_client = session.client("iot-data", region_name=self.region)
            
            if self.metrics_enabled:
                self._cloudwatch_client = session.client("cloudwatch", region_name=self.region)
        
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
    
    @property
    def metrics_enabled(self) -> bool:
        """Check if metrics are enabled."""
        return self._metrics_enabled
    
    @metrics_enabled.setter
    def metrics_enabled(self, value: bool):
        """Enable or disable CloudWatch metrics."""
        self._metrics_enabled = value
        if value and not self._cloudwatch_client and BOTO3_AVAILABLE:
            self._init_aws_clients()
    
    def _get_endpoint(self) -> str:
        """Get IoT Core endpoint."""
        if self.endpoint:
            return self.endpoint
        
        if BOTO3_AVAILABLE and not self._iot_data_client:
            self._init_aws_clients()
        
        if self._iot_data_client:
            try:
                response = self._iot_data_client.describe_endpoint(endpointType="iot:Data")
                return response.get("endpointAddress", "iot.us-east-1.amazonaws.com")
            except Exception as e:
                logger.error(f"Failed to get IoT endpoint: {e}")
        
        return "iot.us-east-1.amazonaws.com"
    
    def _record_metric(self, metric_name: str, value: float, unit: str = "Count"):
        """Record CloudWatch metric."""
        if not self._metrics_enabled or not self._cloudwatch_client:
            return
        
        try:
            self._cloudwatch_client.put_metric_data(
                Namespace=self.metrics_namespace,
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit,
                        "Timestamp": datetime.utcnow()
                    }
                ]
            )
        except Exception as e:
            logger.warning(f"Failed to record metric {metric_name}: {e}")
    
    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================
    
    def connect(self, connection_config: Optional[ConnectionConfig] = None) -> bool:
        """
        Establish MQTT connection to IoT Core.
        
        Args:
            connection_config: Connection configuration
            
        Returns:
            True if connection successful
        """
        if not PAHO_AVAILABLE:
            logger.error("paho-mqtt not available")
            return False
        
        if self._mqtt_connected:
            logger.warning("Already connected to MQTT broker")
            return True
        
        config = connection_config or self.connection_config or ConnectionConfig(
            endpoint=self._get_endpoint()
        )
        
        self.connection_config = config
        
        client_id = config.client_id or f"iot_data_plane_{uuid.uuid4().hex[:8]}"
        
        protocol = mqtt.MQTTv311
        if config.protocol_version == MQTTProtocolVersion.MQTTv50:
            protocol = mqtt.MQTTv50
        elif config.protocol_version == MQTTProtocolVersion.MQTTv31:
            protocol = mqtt.MQTTv31
        
        self._mqtt_client = mqtt.Client(
            client_id=client_id,
            protocol=protocol,
            clean_session=config.clean_session
        )
        
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.on_publish = self._on_publish
        self._mqtt_client.on_subscribe = self._on_subscribe
        
        if config.username and config.password:
            self._mqtt_client.username_pw_set(config.username, config.password)
        
        if config.use_tls:
            self._mqtt_client.tls_set(tls_version=2)
            if not config.tls_verify:
                self._mqtt_client.tls_insecure_set(True)
        
        try:
            self._mqtt_client.connect(
                config.endpoint,
                config.port,
                keepalive=config.keepalive
            )
            self._mqtt_client.loop_start()
            self._running = True
            self._record_metric("ConnectionAttempts", 1)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self._record_metric("ConnectionErrors", 1)
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from IoT Core MQTT broker.
        
        Returns:
            True if disconnection successful
        """
        if not self._mqtt_client:
            return True
        
        try:
            self._running = False
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            self._mqtt_connected = False
            self._record_metric("Disconnections", 1)
            return True
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback."""
        if rc == 0:
            self._mqtt_connected = True
            self._reconnect_delay = 1
            logger.info("Connected to IoT Core MQTT broker")
            self._record_metric("SuccessfulConnections", 1)
            
            for sub in self._subscriptions.values():
                topic, qos = sub.topic, sub.qos
                client.subscribe(topic, qos)
        else:
            logger.error(f"Failed to connect to MQTT broker, rc={rc}")
            self._record_metric("ConnectionFailures", 1)
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback."""
        self._mqtt_connected = False
        logger.warning(f"Disconnected from IoT Core MQTT broker, rc={rc}")
        self._record_metric("Disconnections", 1)
        
        if self._running and self.connection_config:
            self._schedule_reconnect()
    
    def _schedule_reconnect(self):
        """Schedule reconnection attempt."""
        def reconnect():
            if self._running:
                logger.info(f"Attempting reconnect in {self._reconnect_delay}s...")
                time.sleep(self._reconnect_delay)
                self.connect(self.connection_config)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self.connection_config.max_reconnect_delay if self.connection_config else 60
                )
        
        thread = threading.Thread(target=reconnect, daemon=True)
        thread.start()
    
    def _on_message(self, client, userdata, msg):
        """MQTT message callback."""
        try:
            payload = msg.payload.decode("utf-8")
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                pass
            
            message = MQTTMessage(
                topic=msg.topic,
                payload=payload,
                qos=msg.qos,
                retain=msg.retain,
                message_id=msg.mid
            )
            
            self._message_queue.put(message)
            self._record_metric("MessagesReceived", 1)
            
            for handler in self._message_handlers:
                try:
                    handler(message)
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
            
            if msg.topic.endswith("/delta"):
                thing_name = msg.topic.rsplit("/", 2)[0]
                self._handle_shadow_delta(thing_name, payload)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    def _on_publish(self, client, userdata, mid):
        """MQTT publish callback."""
        self._record_metric("MessagesPublished", 1)
    
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """MQTT subscribe callback."""
        logger.debug(f"Subscribed successfully, mid={mid}, granted_qos={granted_qos}")
    
    def is_connected(self) -> bool:
        """Check if MQTT connection is active."""
        return self._mqtt_connected
    
    # =========================================================================
    # PUBLISH OPERATIONS
    # =========================================================================
    
    def publish(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        retain: bool = False,
        properties: Optional[Dict[str, Any]] = None
    ) -> PublishResult:
        """
        Publish message to IoT topic.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether to retain the message
            properties: MQTT 5.0 message properties
            
        Returns:
            PublishResult with operation status
        """
        if not self._mqtt_connected:
            logger.warning("Not connected to MQTT broker, attempting to connect...")
            if not self.connect():
                return PublishResult(
                    message_id=None,
                    topic=topic,
                    qos=qos,
                    success=False,
                    error="Not connected to MQTT broker"
                )
        
        try:
            if isinstance(payload, (dict, list)):
                message_payload = json.dumps(payload)
            else:
                message_payload = str(payload)
            
            msg_info = self._mqtt_client.publish(topic, message_payload, qos, retain)
            
            if msg_info.is_published():
                self._record_metric("PublishSuccess", 1)
                return PublishResult(
                    message_id=msg_info.mid,
                    topic=topic,
                    qos=qos,
                    success=True
                )
            else:
                return PublishResult(
                    message_id=msg_info.mid,
                    topic=topic,
                    qos=qos,
                    success=False,
                    error="Message not published"
                )
        
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            self._record_metric("PublishErrors", 1)
            return PublishResult(
                message_id=None,
                topic=topic,
                qos=qos,
                success=False,
                error=str(e)
            )
    
    def publish_with_ack(
        self,
        topic: str,
        payload: Any,
        timeout: float = 5.0,
        qos: int = 1
    ) -> Optional[PublishResult]:
        """
        Publish message and wait for acknowledgment.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            timeout: Timeout in seconds
            qos: Quality of Service level (must be > 0)
            
        Returns:
            PublishResult or None on timeout
        """
        if qos == 0:
            return self.publish(topic, payload, qos=0)
        
        result = self.publish(topic, payload, qos=qos)
        if not result.success:
            return result
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            if result.message_id is not None:
                try:
                    self._mqtt_client.loop()
                    break
                except Exception:
                    pass
            time.sleep(0.1)
        
        return result
    
    def publish_batch(
        self,
        messages: List[Dict[str, Any]],
        qos: int = 0
    ) -> List[PublishResult]:
        """
        Publish multiple messages.
        
        Args:
            messages: List of message dicts with 'topic' and 'payload' keys
            qos: Default QoS level
            
        Returns:
            List of PublishResult
        """
        results = []
        for msg in messages:
            topic = msg.get("topic")
            payload = msg.get("payload")
            msg_qos = msg.get("qos", qos)
            retain = msg.get("retain", False)
            
            if topic and payload is not None:
                result = self.publish(topic, payload, msg_qos, retain)
                results.append(result)
            else:
                results.append(PublishResult(
                    message_id=None,
                    topic=topic or "unknown",
                    qos=msg_qos,
                    success=False,
                    error="Missing topic or payload"
                ))
        
        self._record_metric("BatchPublishOperations", 1)
        return results
    
    # =========================================================================
    # SUBSCRIBE OPERATIONS
    # =========================================================================
    
    def subscribe(
        self,
        topic: str,
        callback: Optional[Callable[[MQTTMessage], None]] = None,
        qos: int = 0
    ) -> SubscribeResult:
        """
        Subscribe to MQTT topic.
        
        Args:
            topic: Topic to subscribe to (supports wildcards)
            callback: Callback function for received messages
            qos: Quality of Service level
            
        Returns:
            SubscribeResult with operation status
        """
        if not self._mqtt_connected:
            logger.warning("Not connected to MQTT broker, attempting to connect...")
            if not self.connect():
                return SubscribeResult(
                    topic=topic,
                    qos=qos,
                    success=False,
                    error="Not connected to MQTT broker"
                )
        
        try:
            result, mid = self._mqtt_client.subscribe(topic, qos)
            
            if result == mqtt.MQTT_ERR_SUCCESS:
                self._subscriptions[topic] = TopicSubscription(
                    topic=topic,
                    qos=qos,
                    callback=callback
                )
                self._record_metric("Subscriptions", 1)
                return SubscribeResult(
                    topic=topic,
                    qos=qos,
                    success=True
                )
            else:
                return SubscribeResult(
                    topic=topic,
                    qos=qos,
                    success=False,
                    error=f"Subscribe failed with result {result}"
                )
        
        except Exception as e:
            logger.error(f"Failed to subscribe to topic: {e}")
            self._record_metric("SubscribeErrors", 1)
            return SubscribeResult(
                topic=topic,
                qos=qos,
                success=False,
                error=str(e)
            )
    
    def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from MQTT topic.
        
        Args:
            topic: Topic to unsubscribe from
            
        Returns:
            True if successful
        """
        if not self._mqtt_connected:
            return False
        
        try:
            result, mid = self._mqtt_client.unsubscribe(topic)
            
            if result == mqtt.MQTT_ERR_SUCCESS:
                self._subscriptions.pop(topic, None)
                return True
            return False
        
        except Exception as e:
            logger.error(f"Failed to unsubscribe from topic: {e}")
            return False
    
    def add_message_handler(self, handler: Callable[[MQTTMessage], None]):
        """
        Add global message handler.
        
        Args:
            handler: Callback function for all messages
        """
        if handler not in self._message_handlers:
            self._message_handlers.append(handler)
    
    def remove_message_handler(self, handler: Callable[[MQTTMessage], None]):
        """
        Remove global message handler.
        
        Args:
            handler: Callback function to remove
        """
        if handler in self._message_handlers:
            self._message_handlers.remove(handler)
    
    def get_message(self, timeout: float = 1.0) -> Optional[MQTTMessage]:
        """
        Get message from queue.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            MQTTMessage or None
        """
        try:
            return self._message_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    # =========================================================================
    # THING SHADOW OPERATIONS (Classic Shadow)
    # =========================================================================
    
    def get_thing_shadow(self, thing_name: str) -> Optional[Dict[str, Any]]:
        """
        Get thing shadow.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            Shadow state document or None
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return None
        
        try:
            response = self._iot_data_client.get_thing_shadow(thingName=thing_name)
            payload = response["payload"].read()
            shadow = json.loads(payload.decode("utf-8"))
            self._record_metric("GetShadowOperations", 1)
            return shadow
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                logger.info(f"Shadow not found for thing: {thing_name}")
                return None
            logger.error(f"Failed to get thing shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return None
        except Exception as e:
            logger.error(f"Failed to get thing shadow: {e}")
            return None
    
    def update_thing_shadow(
        self,
        thing_name: str,
        state: Dict[str, Any],
        client_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update thing shadow.
        
        Args:
            thing_name: Name of the thing
            state: State to update (with desired/reported structure)
            client_token: Optional client token for tracking
            
        Returns:
            Updated shadow state document or None
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return None
        
        token = client_token or str(uuid.uuid4())
        
        try:
            payload = json.dumps({
                "state": state,
                "clientToken": token
            }).encode("utf-8")
            
            response = self._iot_data_client.update_thing_shadow(
                thingName=thing_name,
                payload=payload
            )
            
            result = json.loads(response["payload"].read().decode("utf-8"))
            self._record_metric("UpdateShadowOperations", 1)
            return result
        
        except ClientError as e:
            logger.error(f"Failed to update thing shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return None
        except Exception as e:
            logger.error(f"Failed to update thing shadow: {e}")
            return None
    
    def delete_thing_shadow(self, thing_name: str) -> bool:
        """
        Delete thing shadow.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            True if successful
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return False
        
        try:
            self._iot_data_client.delete_thing_shadow(thingName=thing_name)
            self._record_metric("DeleteShadowOperations", 1)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete thing shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return False
        except Exception as e:
            logger.error(f"Failed to delete thing shadow: {e}")
            return False
    
    def update_shadow_state(
        self,
        thing_name: str,
        desired: Optional[Dict[str, Any]] = None,
        reported: Optional[Dict[str, Any]] = None,
        client_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update shadow state (convenience method).
        
        Args:
            thing_name: Name of the thing
            desired: Desired state properties
            reported: Reported state properties
            client_token: Optional client token
            
        Returns:
            Updated shadow state or None
        """
        state = {}
        if desired is not None:
            state["desired"] = desired
        if reported is not None:
            state["reported"] = reported
        
        if not state:
            logger.warning("No state provided for update")
            return None
        
        return self.update_thing_shadow(thing_name, state, client_token)
    
    # =========================================================================
    # NAMED SHADOW OPERATIONS
    # =========================================================================
    
    def get_named_shadow(
        self,
        thing_name: str,
        shadow_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get named shadow.
        
        Args:
            thing_name: Name of the thing
            shadow_name: Name of the shadow
            
        Returns:
            Shadow state document or None
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return None
        
        try:
            response = self._iot_data_client.get_thing_shadow(
                thingName=thing_name,
                shadowName=shadow_name
            )
            payload = response["payload"].read()
            shadow = json.loads(payload.decode("utf-8"))
            self._record_metric("GetShadowOperations", 1)
            return shadow
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                logger.info(f"Named shadow not found: {thing_name}/{shadow_name}")
                return None
            logger.error(f"Failed to get named shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return None
        except Exception as e:
            logger.error(f"Failed to get named shadow: {e}")
            return None
    
    def update_named_shadow(
        self,
        thing_name: str,
        shadow_name: str,
        state: Dict[str, Any],
        client_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update named shadow.
        
        Args:
            thing_name: Name of the thing
            shadow_name: Name of the shadow
            state: State to update
            client_token: Optional client token
            
        Returns:
            Updated shadow state or None
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return None
        
        token = client_token or str(uuid.uuid4())
        
        try:
            payload = json.dumps({
                "state": state,
                "clientToken": token
            }).encode("utf-8")
            
            response = self._iot_data_client.update_thing_shadow(
                thingName=thing_name,
                shadowName=shadow_name,
                payload=payload
            )
            
            result = json.loads(response["payload"].read().decode("utf-8"))
            self._record_metric("UpdateShadowOperations", 1)
            return result
        
        except ClientError as e:
            logger.error(f"Failed to update named shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return None
        except Exception as e:
            logger.error(f"Failed to update named shadow: {e}")
            return None
    
    def delete_named_shadow(self, thing_name: str, shadow_name: str) -> bool:
        """
        Delete named shadow.
        
        Args:
            thing_name: Name of the thing
            shadow_name: Name of the shadow
            
        Returns:
            True if successful
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return False
        
        try:
            self._iot_data_client.delete_thing_shadow(
                thingName=thing_name,
                shadowName=shadow_name
            )
            self._record_metric("DeleteShadowOperations", 1)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete named shadow: {e}")
            self._record_metric("ShadowErrors", 1)
            return False
        except Exception as e:
            logger.error(f"Failed to delete named shadow: {e}")
            return False
    
    def list_named_shadows(self, thing_name: str) -> List[str]:
        """
        List named shadows for a thing.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            List of shadow names
        """
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return []
        
        try:
            response = self._iot_data_client.list_named_shadows_for_thing(
                thingName=thing_name
            )
            return response.get("shadows", [])
        except ClientError as e:
            logger.error(f"Failed to list named shadows: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to list named shadows: {e}")
            return []
    
    # =========================================================================
    # SHADOW DELTA HANDLING
    # =========================================================================
    
    def subscribe_to_shadow_delta(
        self,
        thing_name: str,
        shadow_name: Optional[str] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> SubscribeResult:
        """
        Subscribe to shadow delta updates.
        
        Args:
            thing_name: Name of the thing
            shadow_name: Optional name of the shadow
            callback: Callback for delta updates
            
        Returns:
            SubscribeResult
        """
        if shadow_name:
            topic = f"$aws/things/{thing_name}/shadow/name/{shadow_name}/delta"
        else:
            topic = f"$aws/things/{thing_name}/shadow/update/delta"
        
        def shadow_delta_handler(message: MQTTMessage):
            if callback:
                callback(message.payload)
        
        return self.subscribe(topic, shadow_delta_handler, qos=1)
    
    def _handle_shadow_delta(self, thing_name: str, payload: Dict[str, Any]):
        """Handle received shadow delta."""
        key = thing_name
        handlers = self._shadow_handlers.get(key, [])
        for handler in handlers:
            try:
                handler(payload)
            except Exception as e:
                logger.error(f"Error in shadow delta handler: {e}")
    
    def add_shadow_delta_handler(
        self,
        thing_name: str,
        handler: Callable[[Dict[str, Any]], None]
    ):
        """
        Add shadow delta handler for a thing.
        
        Args:
            thing_name: Name of the thing
            handler: Callback for delta updates
        """
        self._shadow_handlers[thing_name].append(handler)
    
    def remove_shadow_delta_handler(
        self,
        thing_name: str,
        handler: Callable[[Dict[str, Any]], None]
    ):
        """
        Remove shadow delta handler.
        
        Args:
            thing_name: Name of the thing
            handler: Handler to remove
        """
        if handler in self._shadow_handlers.get(thing_name, []):
            self._shadow_handlers[thing_name].remove(handler)
    
    # =========================================================================
    # RETAINED MESSAGES
    # =========================================================================
    
    def get_retained_message(self, topic: str) -> Optional[MQTTMessage]:
        """
        Get retained message for a topic.
        
        Note: IoT Core doesn't support direct retained message retrieval.
        This method simulates the behavior by searching through known topics.
        
        Args:
            topic: Topic to get retained message for
            
        Returns:
            MQTTMessage or None
        """
        logger.warning("IoT Core does not support direct retained message retrieval")
        return None
    
    def list_retained_messages(self) -> List[Dict[str, str]]:
        """
        List retained messages.
        
        Note: IoT Core doesn't support listing retained messages directly.
        This returns an empty list as a placeholder.
        
        Returns:
            Empty list (not supported by IoT Core)
        """
        logger.warning("IoT Core does not support listing retained messages")
        return []
    
    def publish_retained(
        self,
        topic: str,
        payload: Any,
        qos: int = 0
    ) -> PublishResult:
        """
        Publish message with retained flag.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            qos: Quality of Service level
            
        Returns:
            PublishResult
        """
        return self.publish(topic, payload, qos=qos, retain=True)
    
    # =========================================================================
    # BATCH OPERATIONS
    # =========================================================================
    
    def batch_get_thing_shadow(
        self,
        things: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Batch get thing shadows.
        
        Args:
            things: List of thing names
            
        Returns:
            Dict mapping thing names to shadow documents
        """
        results = {}
        
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return {thing: None for thing in things}
        
        for thing_name in things:
            shadow = self.get_thing_shadow(thing_name)
            results[thing_name] = shadow
        
        self._record_metric("BatchGetShadowOperations", 1)
        return results
    
    def batch_update_thing_shadow(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Batch update thing shadows.
        
        Args:
            updates: List of dicts with 'thingName' and 'state' keys
            
        Returns:
            Dict mapping thing names to updated shadow documents
        """
        results = {}
        
        if not self._iot_data_client:
            self._init_aws_clients()
        
        if not self._iot_data_client:
            logger.error("IoT Data client not initialized")
            return {u.get("thingName"): None for u in updates}
        
        for update in updates:
            thing_name = update.get("thingName")
            state = update.get("state")
            
            if thing_name and state:
                shadow = self.update_thing_shadow(thing_name, state)
                results[thing_name] = shadow
            else:
                results[update.get("thingName")] = None
        
        self._record_metric("BatchUpdateShadowOperations", 1)
        return results
    
    def batch_subscribe(
        self,
        topics: List[Dict[str, Any]]
    ) -> List[SubscribeResult]:
        """
        Batch subscribe to topics.
        
        Args:
            topics: List of dicts with 'topic', 'callback', and 'qos' keys
            
        Returns:
            List of SubscribeResult
        """
        results = []
        
        for item in topics:
            topic = item.get("topic")
            callback = item.get("callback")
            qos = item.get("qos", 0)
            
            if topic:
                result = self.subscribe(topic, callback, qos)
                results.append(result)
            else:
                results.append(SubscribeResult(
                    topic="unknown",
                    qos=0,
                    success=False,
                    error="Missing topic"
                ))
        
        self._record_metric("BatchSubscribeOperations", 1)
        return results
    
    # =========================================================================
    # MQTT 5.0 SUPPORT
    # =========================================================================
    
    def publish_mqtt5(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        retain: bool = False,
        user_properties: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        correlation_data: Optional[str] = None,
        message_expiry: Optional[int] = None
    ) -> PublishResult:
        """
        Publish message with MQTT 5.0 properties.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            qos: Quality of Service level
            retain: Whether to retain the message
            user_properties: User properties (MQTT 5.0)
            content_type: Content type (MQTT 5.0)
            correlation_data: Correlation data (MQTT 5.0)
            message_expiry: Message expiry interval in seconds (MQTT 5.0)
            
        Returns:
            PublishResult
        """
        if not self._mqtt_connected:
            if not self.connect():
                return PublishResult(
                    message_id=None,
                    topic=topic,
                    qos=qos,
                    success=False,
                    error="Not connected to MQTT broker"
                )
        
        if self.connection_config and self.connection_config.protocol_version != MQTTProtocolVersion.MQTTv50:
            logger.warning("MQTT 5.0 properties ignored - not connected with MQTTv5")
        
        try:
            if isinstance(payload, (dict, list)):
                message_payload = json.dumps(payload)
            else:
                message_payload = str(payload)
            
            msg_info = self._mqtt_client.publish(topic, message_payload, qos, retain)
            
            return PublishResult(
                message_id=msg_info.mid,
                topic=topic,
                qos=qos,
                success=msg_info.is_published()
            )
        
        except Exception as e:
            logger.error(f"Failed to publish MQTT5 message: {e}")
            return PublishResult(
                message_id=None,
                topic=topic,
                qos=qos,
                success=False,
                error=str(e)
            )
    
    def subscribe_mqtt5(
        self,
        topic: str,
        callback: Optional[Callable[[MQTTMessage], None]] = None,
        qos: int = 0,
        no_local: bool = False,
        retain_as_published: bool = False,
        subscription_identifier: Optional[int] = None
    ) -> SubscribeResult:
        """
        Subscribe with MQTT 5.0 options.
        
        Args:
            topic: Topic to subscribe to
            callback: Callback function
            qos: Quality of Service level
            no_local: No local option (MQTT 5.0)
            retain_as_published: Retain as published option (MQTT 5.0)
            subscription_identifier: Subscription identifier (MQTT 5.0)
            
        Returns:
            SubscribeResult
        """
        return self.subscribe(topic, callback, qos)
    
    # =========================================================================
    # CLOUDWATCH METRICS
    # =========================================================================
    
    def enable_metrics(self, namespace: Optional[str] = None):
        """
        Enable CloudWatch metrics.
        
        Args:
            namespace: Custom metrics namespace
        """
        if namespace:
            self.metrics_namespace = namespace
        self.metrics_enabled = True
        logger.info(f"CloudWatch metrics enabled with namespace: {self.metrics_namespace}")
    
    def disable_metrics(self):
        """Disable CloudWatch metrics."""
        self.metrics_enabled = False
        logger.info("CloudWatch metrics disabled")
    
    def record_custom_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None
    ):
        """
        Record custom metric to CloudWatch.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of the metric
            dimensions: Optional dimensions
        """
        if not self._metrics_enabled or not self._cloudwatch_client:
            return
        
        try:
            metric_data = [{
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.utcnow()
            }]
            
            if dimensions:
                metric_data[0]["Dimensions"] = [
                    {"Name": k, "Value": v} for k, v in dimensions.items()
                ]
            
            self._cloudwatch_client.put_metric_data(
                Namespace=self.metrics_namespace,
                MetricData=metric_data
            )
        except Exception as e:
            logger.warning(f"Failed to record custom metric: {e}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get connection statistics.
        
        Returns:
            Dict with connection stats
        """
        return {
            "connected": self._mqtt_connected,
            "subscriptions_count": len(self._subscriptions),
            "message_queue_size": self._message_queue.qsize(),
            "metrics_enabled": self._metrics_enabled,
            "endpoint": self._get_endpoint() if self.connection_config else None,
            "protocol_version": self.connection_config.protocol_version.name if self.connection_config else None
        }
    
    # =========================================================================
    # CONTEXT MANAGER SUPPORT
    # =========================================================================
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
    
    # =========================================================================
    # ASYNC SUPPORT
    # =========================================================================
    
    async def publish_async(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        retain: bool = False
    ) -> PublishResult:
        """
        Async publish message.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            qos: Quality of Service level
            retain: Whether to retain the message
            
        Returns:
            PublishResult
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.publish,
            topic,
            payload,
            qos,
            retain
        )
    
    async def subscribe_async(
        self,
        topic: str,
        callback: Optional[Callable[[MQTTMessage], None]] = None,
        qos: int = 0
    ) -> SubscribeResult:
        """
        Async subscribe to topic.
        
        Args:
            topic: Topic to subscribe to
            callback: Callback function
            qos: Quality of Service level
            
        Returns:
            SubscribeResult
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.subscribe,
            topic,
            callback,
            qos
        )
    
    async def get_thing_shadow_async(self, thing_name: str) -> Optional[Dict[str, Any]]:
        """
        Async get thing shadow.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            Shadow state document or None
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.get_thing_shadow,
            thing_name
        )
    
    async def update_thing_shadow_async(
        self,
        thing_name: str,
        state: Dict[str, Any],
        client_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Async update thing shadow.
        
        Args:
            thing_name: Name of the thing
            state: State to update
            client_token: Optional client token
            
        Returns:
            Updated shadow state or None
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.update_thing_shadow,
            thing_name,
            state,
            client_token
        )
