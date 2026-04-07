"""RabbitMQ action module for RabAI AutoClick.

Provides RabbitMQ messaging operations:
- RabbitMQPublishAction: Publish message to exchange
- RabbitMQConsumeAction: Consume messages from queue
- RabbitMQDeclareQueueAction: Declare queue
- RabbitMQDeclareExchangeAction: Declare exchange
- RabbitMQBindAction: Bind queue to exchange
- RabbitMQDeleteQueueAction: Delete queue
- RabbitMQPurgeAction: Purge queue
- RabbitMQHealthAction: Check RabbitMQ health
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def get_rabbitmq_connection(host='localhost', port=5672, user='guest', password='guest', vhost='/'):
    """Get RabbitMQ connection."""
    try:
        import pika
        credentials = pika.PlainCredentials(user, password)
        params = pika.ConnectionParameters(
            host=host, port=port,
            virtual_host=vhost,
            credentials=credentials
        )
        return pika.BlockingConnection(params)
    except ImportError:
        return None
    except Exception:
        return None


class RabbitMQPublishAction(BaseAction):
    """Publish message to exchange."""
    action_type = "rabbitmq_publish"
    display_name = "RabbitMQ发布"
    description = "向RabbitMQ交换机发布消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute publish.

        Args:
            context: Execution context.
            params: Dict with exchange, routing_key, message, host, port, user, password.

        Returns:
            ActionResult indicating success.
        """
        exchange = params.get('exchange', '')
        routing_key = params.get('routing_key', '')
        message = params.get('message', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_exchange = context.resolve_value(exchange)
            resolved_routing = context.resolve_value(routing_key)
            resolved_message = context.resolve_value(message)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="pika未安装或无法连接: pip install pika"
                )

            channel = conn.channel()

            if isinstance(resolved_message, dict):
                body = json.dumps(resolved_message).encode('utf-8')
            else:
                body = str(resolved_message).encode('utf-8')

            channel.basic_publish(
                exchange=resolved_exchange,
                routing_key=resolved_routing,
                body=body
            )

            conn.close()

            return ActionResult(
                success=True,
                message=f"消息已发布: {resolved_routing}",
                data={'exchange': resolved_exchange, 'routing_key': resolved_routing}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ发布失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['exchange', 'routing_key', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/'}


class RabbitMQConsumeAction(BaseAction):
    """Consume messages from queue."""
    action_type = "rabbitmq_consume"
    display_name = "RabbitMQ消费"
    description = "从RabbitMQ队列消费消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute consume.

        Args:
            context: Execution context.
            params: Dict with queue, count, auto_ack, host, port, user, password, output_var.

        Returns:
            ActionResult with messages.
        """
        queue = params.get('queue', '')
        count = params.get('count', 1)
        auto_ack = params.get('auto_ack', True)
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')
        output_var = params.get('output_var', 'rabbitmq_messages')

        valid, msg = self.validate_type(queue, str, 'queue')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_count = context.resolve_value(count)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()

            messages = []
            for _ in range(int(resolved_count)):
                method, props, body = channel.basic_get(queue=resolved_queue, auto_ack=auto_ack)
                if method:
                    try:
                        data = json.loads(body.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        data = body.decode('utf-8', errors='replace')

                    messages.append({
                        'body': data,
                        'delivery_tag': method.delivery_tag
                    })

            conn.close()

            context.set(output_var, messages)

            return ActionResult(
                success=True,
                message=f"消费 {len(messages)} 条消息",
                data={'count': len(messages), 'messages': messages, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ消费失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'count': 1, 'auto_ack': True, 'host': 'localhost', 'port': 5672,
            'user': 'guest', 'password': 'guest', 'vhost': '/',
            'output_var': 'rabbitmq_messages'
        }


class RabbitMQDeclareQueueAction(BaseAction):
    """Declare queue."""
    action_type = "rabbitmq_declare_queue"
    display_name = "RabbitMQ声明队列"
    description = "声明RabbitMQ队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute declare.

        Args:
            context: Execution context.
            params: Dict with queue, durable, host, port, user, password.

        Returns:
            ActionResult indicating success.
        """
        queue = params.get('queue', '')
        durable = params.get('durable', True)
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')

        valid, msg = self.validate_type(queue, str, 'queue')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_durable = context.resolve_value(durable)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()
            result = channel.queue_declare(queue=resolved_queue, durable=resolved_durable)

            conn.close()

            return ActionResult(
                success=True,
                message=f"队列已声明: {resolved_queue} (消息数: {result.method.message_count})",
                data={'queue': resolved_queue, 'message_count': result.method.message_count, 'consumer_count': result.method.consumer_count}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ声明队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'durable': True, 'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/'}


class RabbitMQDeclareExchangeAction(BaseAction):
    """Declare exchange."""
    action_type = "rabbitmq_declare_exchange"
    display_name = "RabbitMQ声明交换机"
    description = "声明RabbitMQ交换机"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute declare.

        Args:
            context: Execution context.
            params: Dict with exchange, type, durable, host, port, user, password.

        Returns:
            ActionResult indicating success.
        """
        exchange = params.get('exchange', '')
        exchange_type = params.get('type', 'direct')
        durable = params.get('durable', True)
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')

        valid, msg = self.validate_type(exchange, str, 'exchange')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_exchange = context.resolve_value(exchange)
            resolved_type = context.resolve_value(exchange_type)
            resolved_durable = context.resolve_value(durable)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()
            channel.exchange_declare(
                exchange=resolved_exchange,
                exchange_type=resolved_type,
                durable=resolved_durable
            )

            conn.close()

            return ActionResult(
                success=True,
                message=f"交换机已声明: {resolved_exchange} ({resolved_type})",
                data={'exchange': resolved_exchange, 'type': resolved_type}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ声明交换机失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['exchange']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'type': 'direct', 'durable': True, 'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/'}


class RabbitMQBindAction(BaseAction):
    """Bind queue to exchange."""
    action_type = "rabbitmq_bind"
    display_name = "RabbitMQ绑定"
    description = "将队列绑定到交换机"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bind.

        Args:
            context: Execution context.
            params: Dict with queue, exchange, routing_key, host, port, user, password.

        Returns:
            ActionResult indicating success.
        """
        queue = params.get('queue', '')
        exchange = params.get('exchange', '')
        routing_key = params.get('routing_key', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')

        valid, msg = self.validate_type(queue, str, 'queue')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_exchange = context.resolve_value(exchange)
            resolved_routing = context.resolve_value(routing_key)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()
            channel.queue_bind(
                queue=resolved_queue,
                exchange=resolved_exchange,
                routing_key=resolved_routing
            )

            conn.close()

            return ActionResult(
                success=True,
                message=f"已绑定: {resolved_queue} -> {resolved_exchange} ({resolved_routing})",
                data={'queue': resolved_queue, 'exchange': resolved_exchange, 'routing_key': resolved_routing}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ绑定失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue', 'exchange', 'routing_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/'}


class RabbitMQDeleteQueueAction(BaseAction):
    """Delete queue."""
    action_type = "rabbitmq_delete_queue"
    display_name = "RabbitMQ删除队列"
    description = "删除RabbitMQ队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with queue, host, port, user, password.

        Returns:
            ActionResult indicating success.
        """
        queue = params.get('queue', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')

        valid, msg = self.validate_type(queue, str, 'queue')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_queue = context.resolve_value(queue)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()
            channel.queue_delete(queue=resolved_queue)

            conn.close()

            return ActionResult(
                success=True,
                message=f"队列已删除: {resolved_queue}",
                data={'queue': resolved_queue}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ删除队列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['queue']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/'}


class RabbitMQHealthAction(BaseAction):
    """Check RabbitMQ health."""
    action_type = "rabbitmq_health"
    display_name = "RabbitMQ健康检查"
    description = "检查RabbitMQ健康状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute health.

        Args:
            context: Execution context.
            params: Dict with host, port, user, password, output_var.

        Returns:
            ActionResult with health status.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 5672)
        user = params.get('user', 'guest')
        password = params.get('password', 'guest')
        vhost = params.get('vhost', '/')
        output_var = params.get('output_var', 'rabbitmq_health')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_user = context.resolve_value(user)
            resolved_pwd = context.resolve_value(password)
            resolved_vhost = context.resolve_value(vhost)

            conn = get_rabbitmq_connection(resolved_host, int(resolved_port), resolved_user, resolved_pwd, resolved_vhost)
            if conn is None:
                return ActionResult(
                    success=False,
                    message="无法连接RabbitMQ"
                )

            channel = conn.channel()
            result = channel.queue_declare(queue='health_check', durable=False, passive=True)
            channel.queue_delete(queue='health_check')

            health = {
                'connected': True,
                'host': resolved_host,
                'port': resolved_port,
                'vhost': resolved_vhost,
                'open_connections': conn.channel_count()
            }

            conn.close()

            context.set(output_var, health)

            return ActionResult(
                success=True,
                message=f"RabbitMQ健康: {conn.channel_count()} 通道开放",
                data=health
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RabbitMQ健康检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 5672, 'user': 'guest', 'password': 'guest', 'vhost': '/', 'output_var': 'rabbitmq_health'}
