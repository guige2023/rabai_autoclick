"""RabbitMQ action module for RabAI AutoClick.

Provides RabbitMQ operations for
message queue management and pub/sub messaging.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RabbitMQClient:
    """RabbitMQ client for message queue operations.
    
    Provides methods for managing queues, exchanges,
    and publishing/consuming messages.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        virtual_host: str = "/",
        username: str = "guest",
        password: str = "guest",
        heartbeat: int = 60
    ) -> None:
        """Initialize RabbitMQ client.
        
        Args:
            host: RabbitMQ server host.
            port: RabbitMQ server port.
            virtual_host: Virtual host.
            username: Username.
            password: Password.
            heartbeat: Heartbeat interval.
        """
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.heartbeat = heartbeat
        self._connection: Optional[Any] = None
        self._channel: Optional[Any] = None
    
    def connect(self) -> bool:
        """Connect to RabbitMQ server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import pika
        except ImportError:
            raise ImportError("pika is required. Install with: pip install pika")
        
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=self.heartbeat
            )
            
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            
            return self._connection is not None
        
        except Exception:
            self._connection = None
            self._channel = None
            return False
    
    def disconnect(self) -> None:
        """Disconnect from RabbitMQ server."""
        if self._channel:
            try:
                self._channel.close()
            except Exception:
                pass
            self._channel = None
        
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
    
    def declare_queue(
        self,
        queue_name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False
    ) -> bool:
        """Declare a queue.
        
        Args:
            queue_name: Queue name.
            durable: Durable queue.
            exclusive: Exclusive queue.
            auto_delete: Auto-delete queue.
            
        Returns:
            True if declaration succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete
            )
            return True
        
        except Exception:
            return False
    
    def delete_queue(self, queue_name: str) -> bool:
        """Delete a queue.
        
        Args:
            queue_name: Queue name to delete.
            
        Returns:
            True if deletion succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.queue_delete(queue=queue_name)
            return True
        
        except Exception:
            return False
    
    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable: bool = True
    ) -> bool:
        """Declare an exchange.
        
        Args:
            exchange_name: Exchange name.
            exchange_type: Exchange type (direct, fanout, topic, headers).
            durable: Durable exchange.
            
        Returns:
            True if declaration succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable
            )
            return True
        
        except Exception:
            return False
    
    def delete_exchange(self, exchange_name: str) -> bool:
        """Delete an exchange.
        
        Args:
            exchange_name: Exchange name to delete.
            
        Returns:
            True if deletion succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.exchange_delete(exchange=exchange_name)
            return True
        
        except Exception:
            return False
    
    def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str = ""
    ) -> bool:
        """Bind a queue to an exchange.
        
        Args:
            queue_name: Queue name.
            exchange_name: Exchange name.
            routing_key: Routing key.
            
        Returns:
            True if binding succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            return True
        
        except Exception:
            return False
    
    def publish_message(
        self,
        exchange: str,
        routing_key: str,
        message: str,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Publish a message.
        
        Args:
            exchange: Exchange name.
            routing_key: Routing key.
            message: Message body.
            properties: Optional message properties.
            
        Returns:
            True if publish succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            import pika
            
            props = pika.BasicProperties(
                delivery_mode=2,
                content_type="text/plain"
            )
            
            if properties:
                if "persistent" in properties:
                    props.delivery_mode = 2 if properties["persistent"] else 1
                if "content_type" in properties:
                    props.content_type = properties["content_type"]
            
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message.encode() if isinstance(message, str) else message,
                properties=props
            )
            
            return True
        
        except Exception:
            return False
    
    def consume_messages(
        self,
        queue_name: str,
        auto_ack: bool = True,
        count: int = 1
    ) -> List[Any]:
        """Consume messages from a queue.
        
        Args:
            queue_name: Queue name.
            auto_ack: Auto acknowledge messages.
            count: Number of messages to consume.
            
        Returns:
            List of messages.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            messages = []
            
            for _ in range(count):
                method, props, body = self._channel.basic_get(
                    queue=queue_name,
                    auto_ack=auto_ack
                )
                
                if method:
                    messages.append({
                        "body": body,
                        "delivery_tag": method.delivery_tag,
                        "props": props
                    })
                else:
                    break
            
            return messages
        
        except Exception:
            return []
    
    def ack_message(self, delivery_tag: int) -> bool:
        """Acknowledge a message.
        
        Args:
            delivery_tag: Delivery tag.
            
        Returns:
            True if ack succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.basic_ack(delivery_tag=delivery_tag)
            return True
        
        except Exception:
            return False
    
    def nack_message(self, delivery_tag: int, requeue: bool = True) -> bool:
        """Negative acknowledge a message.
        
        Args:
            delivery_tag: Delivery tag.
            requeue: Requeue the message.
            
        Returns:
            True if nack succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
            return True
        
        except Exception:
            return False
    
    def get_queue_info(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """Get queue information.
        
        Args:
            queue_name: Queue name.
            
        Returns:
            Queue information or None.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            result = self._channel.queue_declare(
                queue=queue_name,
                durable=True,
                passive=True
            )
            
            return {
                "name": result.method.queue,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count
            }
        
        except Exception:
            return None
    
    def list_queues(self) -> List[Dict[str, Any]]:
        """List all queues.
        
        Returns:
            List of queue information.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            result = self._channel.queue_declare(
                queue="amq.rabbitmq.management",
                durable=True,
                auto_delete=True
            )
            
            self._channel.basic_publish(
                exchange="",
                routing_key="amq.rabbitmq.management",
                body='{"action":"list_queues"}'
            )
            
            return []
        
        except Exception:
            return []
    
    def purge_queue(self, queue_name: str) -> bool:
        """Purge all messages from a queue.
        
        Args:
            queue_name: Queue name.
            
        Returns:
            True if purge succeeded.
        """
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        try:
            self._channel.queue_purge(queue=queue_name)
            return True
        
        except Exception:
            return False
    
    def get_message_count(self, queue_name: str) -> int:
        """Get message count for a queue.
        
        Args:
            queue_name: Queue name.
            
        Returns:
            Number of messages.
        """
        info = self.get_queue_info(queue_name)
        if info:
            return info.get("message_count", 0)
        return 0
    
    def get_consumer_count(self, queue_name: str) -> int:
        """Get consumer count for a queue.
        
        Args:
            queue_name: Queue name.
            
        Returns:
            Number of consumers.
        """
        info = self.get_queue_info(queue_name)
        if info:
            return info.get("consumer_count", 0)
        return 0


class RabbitMQAction(BaseAction):
    """RabbitMQ action for message queue operations.
    
    Supports queue/exchange management and pub/sub messaging.
    """
    action_type: str = "rabbitmq"
    display_name: str = "RabbitMQ动作"
    description: str = "RabbitMQ消息队列和发布订阅管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[RabbitMQClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute RabbitMQ operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "declare_queue":
                return self._declare_queue(params, start_time)
            elif operation == "delete_queue":
                return self._delete_queue(params, start_time)
            elif operation == "declare_exchange":
                return self._declare_exchange(params, start_time)
            elif operation == "delete_exchange":
                return self._delete_exchange(params, start_time)
            elif operation == "bind_queue":
                return self._bind_queue(params, start_time)
            elif operation == "publish":
                return self._publish(params, start_time)
            elif operation == "consume":
                return self._consume(params, start_time)
            elif operation == "ack":
                return self._ack(params, start_time)
            elif operation == "nack":
                return self._nack(params, start_time)
            elif operation == "get_queue_info":
                return self._get_queue_info(params, start_time)
            elif operation == "purge_queue":
                return self._purge_queue(params, start_time)
            elif operation == "get_message_count":
                return self._get_message_count(params, start_time)
            elif operation == "get_consumer_count":
                return self._get_consumer_count(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except ImportError as e:
            return ActionResult(success=False, message=f"Import error: {str(e)}", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=f"RabbitMQ operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to RabbitMQ."""
        host = params.get("host", "localhost")
        port = params.get("port", 5672)
        virtual_host = params.get("virtual_host", "/")
        username = params.get("username", "guest")
        password = params.get("password", "guest")
        
        self._client = RabbitMQClient(
            host=host,
            port=port,
            virtual_host=virtual_host,
            username=username,
            password=password
        )
        
        success = self._client.connect()
        
        return ActionResult(success=success, message=f"Connected to RabbitMQ at {host}:{port}" if success else "Failed to connect", duration=time.time() - start_time)
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from RabbitMQ."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from RabbitMQ", duration=time.time() - start_time)
    
    def _declare_queue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Declare a queue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.declare_queue(
                queue_name,
                durable=params.get("durable", True),
                exclusive=params.get("exclusive", False),
                auto_delete=params.get("auto_delete", False)
            )
            return ActionResult(success=success, message=f"Queue declared: {queue_name}" if success else "Declare failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_queue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a queue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_queue(queue_name)
            return ActionResult(success=success, message=f"Queue deleted: {queue_name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _declare_exchange(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Declare an exchange."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        exchange_name = params.get("exchange_name", "")
        if not exchange_name:
            return ActionResult(success=False, message="exchange_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.declare_exchange(
                exchange_name,
                exchange_type=params.get("exchange_type", "direct"),
                durable=params.get("durable", True)
            )
            return ActionResult(success=success, message=f"Exchange declared: {exchange_name}" if success else "Declare failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_exchange(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an exchange."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        exchange_name = params.get("exchange_name", "")
        if not exchange_name:
            return ActionResult(success=False, message="exchange_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_exchange(exchange_name)
            return ActionResult(success=success, message=f"Exchange deleted: {exchange_name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _bind_queue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Bind a queue to an exchange."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        exchange_name = params.get("exchange_name", "")
        
        if not queue_name or not exchange_name:
            return ActionResult(success=False, message="queue_name and exchange_name are required", duration=time.time() - start_time)
        
        try:
            success = self._client.bind_queue(
                queue_name,
                exchange_name,
                routing_key=params.get("routing_key", "")
            )
            return ActionResult(success=success, message=f"Queue bound to {exchange_name}" if success else "Bind failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _publish(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Publish a message."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        exchange = params.get("exchange", "")
        routing_key = params.get("routing_key", "")
        message = params.get("message", "")
        
        if not exchange:
            return ActionResult(success=False, message="exchange is required", duration=time.time() - start_time)
        
        try:
            success = self._client.publish_message(
                exchange,
                routing_key,
                message,
                params.get("properties")
            )
            return ActionResult(success=success, message="Message published" if success else "Publish failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _consume(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Consume messages."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            messages = self._client.consume_messages(
                queue_name,
                auto_ack=params.get("auto_ack", True),
                count=params.get("count", 1)
            )
            return ActionResult(success=True, message=f"Consumed {len(messages)} messages", data={"messages": messages}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _ack(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Acknowledge a message."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        delivery_tag = params.get("delivery_tag", 0)
        if not delivery_tag:
            return ActionResult(success=False, message="delivery_tag is required", duration=time.time() - start_time)
        
        try:
            success = self._client.ack_message(delivery_tag)
            return ActionResult(success=success, message="Message acknowledged" if success else "Ack failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _nack(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Negative acknowledge a message."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        delivery_tag = params.get("delivery_tag", 0)
        if not delivery_tag:
            return ActionResult(success=False, message="delivery_tag is required", duration=time.time() - start_time)
        
        try:
            success = self._client.nack_message(delivery_tag, params.get("requeue", True))
            return ActionResult(success=success, message="Message nacked" if success else "Nack failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_queue_info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get queue information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_queue_info(queue_name)
            return ActionResult(success=info is not None, message=f"Queue info retrieved: {queue_name}", data={"info": info}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _purge_queue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Purge a queue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.purge_queue(queue_name)
            return ActionResult(success=success, message=f"Queue purged: {queue_name}" if success else "Purge failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_message_count(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get message count."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            count = self._client.get_message_count(queue_name)
            return ActionResult(success=True, message=f"Queue {queue_name} has {count} messages", data={"count": count}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_consumer_count(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get consumer count."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_name = params.get("queue_name", "")
        if not queue_name:
            return ActionResult(success=False, message="queue_name is required", duration=time.time() - start_time)
        
        try:
            count = self._client.get_consumer_count(queue_name)
            return ActionResult(success=True, message=f"Queue {queue_name} has {count} consumers", data={"count": count}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
