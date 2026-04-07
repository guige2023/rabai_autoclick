"""
RabbitMQ message broker actions.
"""
from __future__ import annotations

import json
import subprocess
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse


def run_rabbitmqctl(
    args: List[str],
    node: str = 'rabbit@localhost'
) -> Dict[str, Any]:
    """
    Execute rabbitmqctl command.

    Args:
        args: rabbitmqctl arguments.
        node: RabbitMQ node name.

    Returns:
        Command result.
    """
    cmd = ['rabbitmqctl', '-n', node] + args

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        return {
            'success': result.returncode == 0,
            'output': result.stdout,
            'error': result.stderr,
        }
    except FileNotFoundError:
        return {
            'success': False,
            'output': '',
            'error': 'rabbitmqctl not found',
        }
    except Exception as e:
        return {
            'success': False,
            'output': '',
            'error': str(e),
        }


def list_queues(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all queues.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of queue information.
    """
    result = run_rabbitmqctl(['list_queues', 'name', 'messages', 'consumers', 'vhost'], node)

    if not result['success']:
        return []

    queues = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if len(parts) >= 4:
            queues.append({
                'name': parts[0],
                'messages': int(parts[1]) if parts[1] else 0,
                'consumers': int(parts[2]) if parts[2] else 0,
                'vhost': parts[3],
            })

    return queues


def list_exchanges(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all exchanges.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of exchange information.
    """
    result = run_rabbitmqctl(
        ['list_exchanges', 'name', 'type', 'durable', 'auto_delete', 'vhost'],
        node
    )

    if not result['success']:
        return []

    exchanges = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if len(parts) >= 5:
            exchanges.append({
                'name': parts[0],
                'type': parts[1],
                'durable': parts[2] == 'true',
                'auto_delete': parts[3] == 'true',
                'vhost': parts[4],
            })

    return exchanges


def list_bindings(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all bindings.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of binding information.
    """
    result = run_rabbitmqctl(
        ['list_bindings', 'source_name', 'destination_name', 'routing_key', 'vhost'],
        node
    )

    if not result['success']:
        return []

    bindings = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if len(parts) >= 4:
            bindings.append({
                'source': parts[0],
                'destination': parts[1],
                'routing_key': parts[2],
                'vhost': parts[3],
            })

    return bindings


def list_connections(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all connections.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of connection information.
    """
    result = run_rabbitmqctl(
        ['list_connections', 'name', 'port', 'user', 'state', 'vhost'],
        node
    )

    if not result['success']:
        return []

    connections = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if len(parts) >= 5:
            connections.append({
                'name': parts[0],
                'port': parts[1],
                'user': parts[2],
                'state': parts[3],
                'vhost': parts[4],
            })

    return connections


def list_channels(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all channels.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of channel information.
    """
    result = run_rabbitmqctl(
        ['list_channels', 'pid', 'name', 'user', 'messages', 'consumers'],
        node
    )

    if not result['success']:
        return []

    channels = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if len(parts) >= 5:
            channels.append({
                'pid': parts[0],
                'name': parts[1],
                'user': parts[2],
                'messages': int(parts[3]) if parts[3] else 0,
                'consumers': int(parts[4]) if len(parts) > 4 and parts[4] else 0,
            })

    return channels


def get_queue_info(queue_name: str, vhost: str = '/', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Get information about a specific queue.

    Args:
        queue_name: Queue name.
        vhost: Virtual host.
        node: RabbitMQ node name.

    Returns:
        Queue information.
    """
    result = run_rabbitmqctl(
        ['list_queues', 'name', 'messages', 'consumers', 'durable', 'vhost'],
        node
    )

    if not result['success']:
        return {'error': result['error']}

    for line in result['output'].splitlines():
        if not line or line.startswith('...'):
            continue

        parts = line.split('\t')
        if len(parts) >= 5 and parts[0] == queue_name and parts[4] == vhost:
            return {
                'name': parts[0],
                'messages': int(parts[1]) if parts[1] else 0,
                'consumers': int(parts[2]) if parts[2] else 0,
                'durable': parts[3] == 'true',
                'vhost': parts[4],
            }

    return {'error': 'Queue not found'}


def purge_queue(queue_name: str, vhost: str = '/', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Purge all messages from a queue.

    Args:
        queue_name: Queue name.
        vhost: Virtual host.
        node: RabbitMQ node name.

    Returns:
        Purge result.
    """
    vhost_escaped = vhost.replace('/', '%2f')
    return run_rabbitmqctl(['purge_queue', '-p', vhost_escaped, queue_name], node)


def delete_queue(queue_name: str, vhost: str = '/', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Delete a queue.

    Args:
        queue_name: Queue name.
        vhost: Virtual host.
        node: RabbitMQ node name.

    Returns:
        Deletion result.
    """
    vhost_escaped = vhost.replace('/', '%2f')
    return run_rabbitmqctl(['delete_queue', '-p', vhost_escaped, queue_name], node)


def delete_exchange(exchange_name: str, vhost: str = '/', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Delete an exchange.

    Args:
        exchange_name: Exchange name.
        vhost: Virtual host.
        node: RabbitMQ node name.

    Returns:
        Deletion result.
    """
    vhost_escaped = vhost.replace('/', '%2f')
    return run_rabbitmqctl(['delete_exchange', '-p', vhost_escaped, exchange_name], node)


def add_user(username: str, password: str, node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Add a user.

    Args:
        username: Username.
        password: Password.
        node: RabbitMQ node name.

    Returns:
        Result.
    """
    return run_rabbitmqctl(['add_user', username, password], node)


def set_user_tags(username: str, tags: List[str], node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Set user tags (roles).

    Args:
        username: Username.
        tags: List of tags (e.g., ['administrator', 'monitoring']).
        node: RabbitMQ node name.

    Returns:
        Result.
    """
    return run_rabbitmqctl(['set_user_tags', username] + tags, node)


def list_users(node: str = 'rabbit@localhost') -> List[Dict[str, Any]]:
    """
    List all users.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of user information.
    """
    result = run_rabbitmqctl(['list_users'], node)

    if not result['success']:
        return []

    users = []
    for line in result['output'].splitlines():
        if not line or line.startswith('...') or line.startswith('Listing'):
            continue

        parts = line.split('\t')
        if parts:
            users.append({
                'name': parts[0],
                'tags': parts[1].split(' ') if len(parts) > 1 and parts[1] else [],
            })

    return users


def set_permissions(
    username: str,
    vhost: str = '/',
    configure: str = '.*',
    write: str = '.*',
    read: str = '.*',
    node: str = 'rabbit@localhost'
) -> Dict[str, Any]:
    """
    Set user permissions for a vhost.

    Args:
        username: Username.
        vhost: Virtual host.
        configure: Configure pattern.
        write: Write pattern.
        read: Read pattern.
        node: RabbitMQ node name.

    Returns:
        Result.
    """
    vhost_escaped = vhost.replace('/', '%2f')
    return run_rabbitmqctl(
        ['set_permissions', '-p', vhost_escaped, username, configure, write, read],
        node
    )


def list_permissions(username: str, vhost: str = '/', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    List permissions for a user.

    Args:
        username: Username.
        vhost: Virtual host.
        node: RabbitMQ node name.

    Returns:
        Permission information.
    """
    vhost_escaped = vhost.replace('/', '%2f')
    result = run_rabbitmqctl(['list_user_permissions', '-p', vhost_escaped, username], node)

    if not result['success']:
        return {'error': result['error']}

    for line in result['output'].splitlines():
        if not line or line.startswith('...'):
            continue

        parts = line.split('\t')
        if len(parts) >= 4:
            return {
                'username': username,
                'vhost': vhost,
                'configure': parts[0],
                'write': parts[1],
                'read': parts[2],
            }

    return {'error': 'User not found'}


def get_cluster_status(node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Get cluster status.

    Args:
        node: RabbitMQ node name.

    Returns:
        Cluster status information.
    """
    result = run_rabbitmqctl(['cluster_status'], node)

    if result['success']:
        return {
            'success': True,
            'status': result['output'],
        }

    return {'success': False, 'error': result['error']}


def get_vhosts(node: str = 'rabbit@localhost') -> List[str]:
    """
    List all virtual hosts.

    Args:
        node: RabbitMQ node name.

    Returns:
        List of vhost names.
    """
    result = run_rabbitmqctl(['list_vhosts', 'name'], node)

    if not result['success']:
        return []

    return [line.strip() for line in result['output'].splitlines() if line.strip()]


def close_all_connections(reason: str = 'administrative shutdown', node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Close all connections.

    Args:
        reason: Reason for closing.
        node: RabbitMQ node name.

    Returns:
        Result.
    """
    return run_rabbitmqctl(['close_all_connections', reason], node)


def get_memory_usage(node: str = 'rabbit@localhost') -> Dict[str, Any]:
    """
    Get memory usage information.

    Args:
        node: RabbitMQ node name.

    Returns:
        Memory usage breakdown.
    """
    result = run_rabbitmqctl(['list_users'], node)

    mem_result = run_rabbitmqctl(['report'], node)

    if mem_result['success']:
        lines = mem_result['output'].splitlines()
        memory_info = {}

        for line in lines:
            if ':' in line and 'memory' in line.lower():
                parts = line.split(':')
                if len(parts) >= 2:
                    memory_info[parts[0].strip()] = parts[1].strip()

        return {
            'success': True,
            'memory': memory_info,
        }

    return {'success': False, 'error': mem_result.get('error', 'Failed to get memory')}


class RabbitMQPublisher:
    """Simple RabbitMQ publisher using pika-like interface."""

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5672,
        username: str = 'guest',
        password: str = 'guest',
        vhost: str = '/'
    ):
        """
        Initialize publisher.

        Args:
            host: RabbitMQ host.
            port: RabbitMQ port.
            username: Username.
            password: Password.
            vhost: Virtual host.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost


def publish_message(
    host: str,
    queue: str,
    message: str,
    exchange: str = '',
    routing_key: str = '',
    username: str = 'guest',
    password: str = 'guest',
    vhost: str = '/'
) -> Dict[str, Any]:
    """
    Publish a message to a queue.

    Args:
        host: RabbitMQ host.
        queue: Target queue name.
        message: Message content.
        exchange: Exchange name (optional).
        routing_key: Routing key (optional).
        username: Username.
        password: Password.
        vhost: Virtual host.

    Returns:
        Publish result.
    """
    try:
        import pika
    except ImportError:
        return {
            'success': False,
            'error': 'pika not installed. Install with: pip install pika',
        }

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        virtual_host=vhost,
        credentials=credentials
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.queue_declare(queue=queue, durable=True)

        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key or queue,
            body=message.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
            )
        )

        connection.close()

        return {'success': True, 'queue': queue}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def consume_messages(
    host: str,
    queue: str,
    callback: str = 'print',
    username: str = 'guest',
    password: str = 'guest',
    vhost: str = '/',
    max_messages: int = 0
) -> Dict[str, Any]:
    """
    Consume messages from a queue.

    Args:
        host: RabbitMQ host.
        queue: Queue name.
        callback: Callback function name.
        username: Username.
        password: Password.
        vhost: Virtual host.
        max_messages: Max messages to consume (0 for unlimited).

    Returns:
        Consumer result.
    """
    try:
        import pika
    except ImportError:
        return {
            'success': False,
            'error': 'pika not installed',
        }

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        virtual_host=vhost,
        credentials=credentials
    )

    def callback_func(ch, method, properties, body):
        print(f"Message: {body.decode('utf-8')}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.queue_declare(queue=queue, durable=True)

        if max_messages > 0:
            for _ in range(max_messages):
                method, properties, body = channel.basic_get(queue=queue)
                if method:
                    callback_func(channel, method, properties, body)
        else:
            channel.basic_consume(queue=queue, on_message_callback=callback_func)
            channel.start_consuming()

        connection.close()
        return {'success': True}
    except Exception as e:
        return {'success': False, 'error': str(e)}
