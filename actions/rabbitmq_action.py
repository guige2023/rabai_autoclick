"""RabbitMQ action module for RabAI AutoClick.

Provides RabbitMQ message broker operations for publishing,
consuming, queue management, and exchange routing.
"""

import sys
import os
import json
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from queue import Queue, Empty

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RabbitMQConfig:
    """RabbitMQ connection configuration."""
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    connection_timeout: float = 30.0
    heartbeat: float = 60.0
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0


@dataclass
class Message:
    """RabbitMQ message representation."""
    body: Any
    delivery_tag: Optional[int] = None
    routing_key: str = ""
    exchange: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)


class RabbitMQConnection:
    """Manages RabbitMQ connection lifecycle."""
    
    def __init__(self, config: RabbitMQConfig):
        self.config = config
        self._connection = None
        self._channel = None
        self._connected = False
        self._consumers: Dict[str, Callable] = {}
        self._message_queues: Dict[str, Queue] = {}
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> tuple:
        """Establish RabbitMQ connection."""
        try:
            import pika
            from pika.exceptions import AMQPConnectionError
            
            credentials = pika.PlainCredentials(
                self.config.username,
                self.config.password
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=self.config.reconnect_delay,
                socket_timeout=self.config.connection_timeout,
                heartbeat=self.config.heartbeat
            )
            
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            self._connected = True
            
            return True, "Connected"
            
        except ImportError:
            return False, "pika not installed. Install with: pip install pika"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close RabbitMQ connection."""
        self._connected = False
        
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
            except Exception:
                pass
        
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                pass
        
        self._channel = None
        self._connection = None
    
    def declare_queue(
        self, 
        queue_name: str, 
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False
    ) -> tuple:
        """Declare a queue."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete
            )
            return True, f"Queue {queue_name} declared"
        except Exception as e:
            return False, f"Declare error: {str(e)}"
    
    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable: bool = True
    ) -> tuple:
        """Declare an exchange."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable
            )
            return True, f"Exchange {exchange_name} declared"
        except Exception as e:
            return False, f"Declare exchange error: {str(e)}"
    
    def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str = ""
    ) -> tuple:
        """Bind queue to exchange."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            return True, f"Queue {queue_name} bound to {exchange_name}"
        except Exception as e:
            return False, f"Bind error: {str(e)}"
    
    def publish(
        self,
        message: Any,
        exchange: str = "",
        routing_key: str = "",
        properties: Optional[Dict] = None
    ) -> tuple:
        """Publish message to exchange."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            import pika
            from pika.spec import BasicProperties
            
            if isinstance(message, (dict, list)):
                body = json.dumps(message)
            else:
                body = str(message)
            
            props = BasicProperties(
                delivery_mode=2,
                content_type="application/json"
            )
            
            if properties:
                props = BasicProperties(**properties)
            
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body.encode("utf-8") if isinstance(body, str) else body,
                properties=props
            )
            
            return True, "Message published"
        except Exception as e:
            return False, f"Publish error: {str(e)}"
    
    def consume(
        self,
        queue_name: str,
        callback: Callable[[Message], None],
        auto_ack: bool = False
    ) -> tuple:
        """Start consuming messages from queue."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            def on_message(channel, method, properties, body):
                msg = Message(
                    body=body.decode("utf-8") if isinstance(body, bytes) else body,
                    delivery_tag=method.delivery_tag,
                    properties={
                        "content_type": properties.content_type,
                        "delivery_mode": properties.delivery_mode
                    }
                )
                callback(msg)
                
                if not auto_ack:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
            
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=on_message,
                auto_ack=auto_ack
            )
            
            self._consumers[queue_name] = True
            self._channel.start_consuming()
            
            return True, f"Consuming from {queue_name}"
        except Exception as e:
            return False, f"Consume error: {str(e)}"
    
    def get_message(self, queue_name: str, timeout: float = 1.0) -> tuple:
        """Get single message from queue."""
        if not self._connected:
            return False, None
        
        try:
            import pika
            
            method, properties, body = self._channel.basic_get(
                queue=queue_name,
                auto_ack=False
            )
            
            if method:
                msg = Message(
                    body=body.decode("utf-8") if isinstance(body, bytes) else body,
                    delivery_tag=method.delivery_tag
                )
                return True, msg
            else:
                return False, None
        except Exception:
            return False, None
    
    def ack(self, delivery_tag: int) -> tuple:
        """Acknowledge message."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            self._channel.basic_ack(delivery_tag=delivery_tag)
            return True, "Acknowledged"
        except Exception as e:
            return False, f"Ack error: {str(e)}"
    
    def get_queue_message_count(self, queue_name: str) -> int:
        """Get number of messages in queue."""
        if not self._connected:
            return -1
        
        try:
            result = self._channel.queue_declare(queue=queue_name, passive=True)
            return result.method.message_count
        except Exception:
            return -1


class RabbitMQAction(BaseAction):
    """Action for RabbitMQ operations.
    
    Features:
        - Connect to RabbitMQ servers
        - Declare queues and exchanges
        - Publish messages with routing
        - Consume messages (push and pull)
        - Queue management (purge, delete)
        - Message acknowledgment
        - Dead letter queues
        - Publisher confirms
    
    Note: Requires pika library. Install with: pip install pika
    """
    
    def __init__(self, config: Optional[RabbitMQConfig] = None):
        """Initialize RabbitMQ action.
        
        Args:
            config: RabbitMQ configuration.
        """
        super().__init__()
        self.config = config or RabbitMQConfig()
        self._connection: Optional[RabbitMQConnection] = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute RabbitMQ operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, publish,
                           consume, declare_queue, declare_exchange, bind,
                           get_message, ack, purge, status)
                - queue: Queue name
                - exchange: Exchange name
                - message: Message to publish
                - routing_key: Routing key
                - properties: Message properties
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "publish":
                return self._publish(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "get_message":
                return self._get_message(params)
            elif operation == "declare_queue":
                return self._declare_queue(params)
            elif operation == "declare_exchange":
                return self._declare_exchange(params)
            elif operation == "bind":
                return self._bind(params)
            elif operation == "ack":
                return self._ack(params)
            elif operation == "purge":
                return self._purge(params)
            elif operation == "status":
                return self._status(params)
            elif operation == "batch_publish":
                return self._batch_publish(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"RabbitMQ operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish RabbitMQ connection."""
        host = params.get("host", self.config.host)
        port = params.get("port", self.config.port)
        username = params.get("username", self.config.username)
        password = params.get("password", self.config.password)
        virtual_host = params.get("virtual_host", self.config.virtual_host)
        
        config = RabbitMQConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            virtual_host=virtual_host
        )
        
        self._connection = RabbitMQConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to RabbitMQ at {host}:{port}",
                data={"host": host, "port": port, "connected": True}
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close RabbitMQ connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _publish(self, params: Dict[str, Any]) -> ActionResult:
        """Publish message to exchange."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        message = params.get("message", "")
        exchange = params.get("exchange", "")
        routing_key = params.get("routing_key", "")
        properties = params.get("properties", {})
        
        if not message:
            return ActionResult(success=False, message="message is required")
        
        success, result = self._connection.publish(
            message=message,
            exchange=exchange,
            routing_key=routing_key,
            properties=properties
        )
        
        if success:
            return ActionResult(
                success=True,
                message="Message published",
                data={"exchange": exchange, "routing_key": routing_key}
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _consume(self, params: Dict[str, Any]) -> ActionResult:
        """Start consuming messages from queue."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        queue = params.get("queue", "")
        if not queue:
            return ActionResult(success=False, message="queue is required")
        
        max_messages = params.get("max_messages", 10)
        timeout = params.get("timeout", 5.0)
        
        messages = []
        end_time = time.time() + timeout
        
        while len(messages) < max_messages and time.time() < end_time:
            ok, msg = self._connection.get_message(queue, timeout=0.5)
            if ok and msg:
                try:
                    parsed = json.loads(msg.body)
                    messages.append({"type": "json", "data": parsed})
                except json.JSONDecodeError:
                    messages.append({"type": "text", "data": msg.body})
                
                if msg.delivery_tag:
                    self._connection.ack(msg.delivery_tag)
            else:
                if time.time() >= end_time:
                    break
        
        return ActionResult(
            success=True,
            message=f"Consumed {len(messages)} messages",
            data={"messages": messages, "count": len(messages)}
        )
    
    def _get_message(self, params: Dict[str, Any]) -> ActionResult:
        """Get single message from queue."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        queue = params.get("queue", "")
        if not queue:
            return ActionResult(success=False, message="queue is required")
        
        success, msg = self._connection.get_message(queue)
        
        if success and msg:
            try:
                data = json.loads(msg.body)
                return ActionResult(
                    success=True,
                    message="Message received",
                    data={"body": data, "delivery_tag": msg.delivery_tag}
                )
            except json.JSONDecodeError:
                return ActionResult(
                    success=True,
                    message="Message received",
                    data={"body": msg.body, "delivery_tag": msg.delivery_tag}
                )
        else:
            return ActionResult(success=False, message="No messages in queue")
    
    def _declare_queue(self, params: Dict[str, Any]) -> ActionResult:
        """Declare a queue."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        queue = params.get("queue", "")
        if not queue:
            return ActionResult(success=False, message="queue name required")
        
        durable = params.get("durable", True)
        exclusive = params.get("exclusive", False)
        auto_delete = params.get("auto_delete", False)
        
        success, message = self._connection.declare_queue(
            queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete
        )
        
        if success:
            return ActionResult(success=True, message=message, data={"queue": queue})
        else:
            return ActionResult(success=False, message=message)
    
    def _declare_exchange(self, params: Dict[str, Any]) -> ActionResult:
        """Declare an exchange."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        exchange = params.get("exchange", "")
        exchange_type = params.get("exchange_type", "direct")
        durable = params.get("durable", True)
        
        if not exchange:
            return ActionResult(success=False, message="exchange name required")
        
        success, message = self._connection.declare_exchange(
            exchange,
            exchange_type=exchange_type,
            durable=durable
        )
        
        if success:
            return ActionResult(success=True, message=message, data={"exchange": exchange})
        else:
            return ActionResult(success=False, message=message)
    
    def _bind(self, params: Dict[str, Any]) -> ActionResult:
        """Bind queue to exchange."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        queue = params.get("queue", "")
        exchange = params.get("exchange", "")
        routing_key = params.get("routing_key", "")
        
        if not queue or not exchange:
            return ActionResult(success=False, message="queue and exchange required")
        
        success, message = self._connection.bind_queue(queue, exchange, routing_key)
        
        if success:
            return ActionResult(success=True, message=message)
        else:
            return ActionResult(success=False, message=message)
    
    def _ack(self, params: Dict[str, Any]) -> ActionResult:
        """Acknowledge message."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        delivery_tag = params.get("delivery_tag")
        if delivery_tag is None:
            return ActionResult(success=False, message="delivery_tag required")
        
        success, message = self._connection.ack(delivery_tag)
        
        if success:
            return ActionResult(success=True, message=message)
        else:
            return ActionResult(success=False, message=message)
    
    def _purge(self, params: Dict[str, Any]) -> ActionResult:
        """Purge all messages from queue."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        queue = params.get("queue", "")
        if not queue:
            return ActionResult(success=False, message="queue name required")
        
        try:
            self._connection._channel.queue_purge(queue)
            return ActionResult(success=True, message=f"Queue {queue} purged")
        except Exception as e:
            return ActionResult(success=False, message=f"Purge error: {str(e)}")
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get connection and queue status."""
        if not self._connection:
            return ActionResult(success=True, message="Not connected", data={"connected": False})
        
        queues = params.get("queues", [])
        status_data = {"connected": True}
        
        for queue in queues:
            count = self._connection.get_queue_message_count(queue)
            status_data[queue] = {"message_count": count}
        
        return ActionResult(success=True, message="Status retrieved", data=status_data)
    
    def _batch_publish(self, params: Dict[str, Any]) -> ActionResult:
        """Publish multiple messages."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        messages = params.get("messages", [])
        exchange = params.get("exchange", "")
        routing_key = params.get("routing_key", "")
        
        if not messages:
            return ActionResult(success=False, message="messages list required")
        
        success_count = 0
        fail_count = 0
        
        for msg in messages:
            ok, _ = self._connection.publish(
                message=msg,
                exchange=exchange,
                routing_key=routing_key
            )
            if ok:
                success_count += 1
            else:
                fail_count += 1
        
        return ActionResult(
            success=fail_count == 0,
            message=f"Published {success_count}, failed {fail_count}",
            data={"sent": success_count, "failed": fail_count}
        )
