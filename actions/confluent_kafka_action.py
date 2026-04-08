"""Confluent Kafka integration for RabAI AutoClick.

Provides actions to produce and consume messages from Confluent Kafka.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ConfluentKafkaProduceAction(BaseAction):
    """Produce messages to Confluent Kafka topics.

    Supports JSON, Avro, and raw message encoding.
    """
    action_type = "confluent_kafka_produce"
    display_name = "Confluent Kafka生产者"
    description = "向Confluent Kafka主题发送消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Produce messages to Kafka.

        Args:
            context: Execution context.
            params: Dict with keys:
                - bootstrap_servers: Kafka bootstrap servers
                - api_key: Confluent API key
                - api_secret: Confluent API secret
                - topic: Target topic name
                - messages: List of messages (dicts with key/value) or single dict
                - key: Optional message key
                - value: Message value (str or dict)
                - partition: Optional partition number
                - use_avro: Whether to use Avro encoding

        Returns:
            ActionResult with produce result.
        """
        bootstrap_servers = params.get('bootstrap_servers') or os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS')
        api_key = params.get('api_key') or os.environ.get('CONFLUENT_API_KEY')
        api_secret = params.get('api_secret') or os.environ.get('CONFLUENT_API_SECRET')
        topic = params.get('topic')

        if not all([bootstrap_servers, api_key, api_secret, topic]):
            return ActionResult(success=False, message="bootstrap_servers, api_key, api_secret, and topic are required")

        try:
            from confluent_kafka import Producer
        except ImportError:
            return ActionResult(success=False, message="confluent-kafka not installed. Run: pip install confluent-kafka")

        try:
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': api_key,
                'sasl.password': api_secret,
                'acks': 'all',
            })

            def delivery_callback(err, msg):
                pass  # In production, track delivery here

            messages = params.get('messages', [params.get('value', {})])
            if isinstance(messages, dict):
                messages = [messages]

            for msg in messages:
                key = msg.get('key', params.get('key', ''))
                value = msg.get('value', msg)
                if isinstance(value, dict):
                    value = json.dumps(value).encode('utf-8')
                elif isinstance(value, str):
                    value = value.encode('utf-8')

                producer.produce(
                    topic=topic,
                    key=str(key).encode('utf-8') if key else None,
                    value=value,
                    callback=delivery_callback
                )

            producer.flush(timeout=30)

            return ActionResult(
                success=True,
                message=f"Produced {len(messages)} messages to {topic}",
                data={'topic': topic, 'count': len(messages)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Kafka produce error: {str(e)}")


class ConfluentKafkaConsumeAction(BaseAction):
    """Consume messages from Confluent Kafka topics.

    Supports consumer groups and offset management.
    """
    action_type = "confluent_kafka_consume"
    display_name = "Confluent Kafka消费者"
    description = "从Confluent Kafka主题消费消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Consume messages from Kafka.

        Args:
            context: Execution context.
            params: Dict with keys:
                - bootstrap_servers: Kafka bootstrap servers
                - api_key: Confluent API key
                - api_secret: Confluent API secret
                - topic: Topic to consume from
                - group_id: Consumer group ID
                - max_messages: Max messages to consume (default 10)
                - timeout: Poll timeout in seconds (default 5)
                - auto_offset_reset: earliest | latest

        Returns:
            ActionResult with consumed messages.
        """
        bootstrap_servers = params.get('bootstrap_servers') or os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS')
        api_key = params.get('api_key') or os.environ.get('CONFLUENT_API_KEY')
        api_secret = params.get('api_secret') or os.environ.get('CONFLUENT_API_SECRET')
        topic = params.get('topic')
        group_id = params.get('group_id', 'rabai-consumer-group')
        max_messages = params.get('max_messages', 10)
        timeout = params.get('timeout', 5)

        if not all([bootstrap_servers, api_key, api_secret, topic]):
            return ActionResult(success=False, message="bootstrap_servers, api_key, api_secret, and topic are required")

        try:
            from confluent_kafka import Consumer
        except ImportError:
            return ActionResult(success=False, message="confluent-kafka not installed")

        try:
            consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': api_key,
                'sasl.password': api_secret,
                'group.id': group_id,
                'auto.offset.reset': params.get('auto_offset_reset', 'earliest'),
                'enable.auto.commit': True,
            })

            consumer.subscribe([topic])

            messages = []
            for _ in range(max_messages):
                msg = consumer.poll(timeout=timeout)
                if msg is None:
                    break
                if msg.error():
                    continue
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    value = msg.value().decode('utf-8', errors='replace')

                messages.append({
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': value,
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                })

            consumer.close()

            return ActionResult(
                success=True,
                message=f"Consumed {len(messages)} messages from {topic}",
                data={'topic': topic, 'messages': messages, 'count': len(messages)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Kafka consume error: {str(e)}")


class ConfluentKafkaAdminAction(BaseAction):
    """Admin operations for Confluent Kafka - topics, schemas, ACLs.

    Provides topic management and schema registry operations.
    """
    action_type = "confluent_kafka_admin"
    display_name = "Confluent Kafka管理"
    description = "Confluent Kafka主题和Schema管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Admin operations on Kafka.

        Args:
            context: Execution context.
            params: Dict with keys:
                - bootstrap_servers: Kafka bootstrap servers
                - api_key: Confluent API key
                - api_secret: Confluent API secret
                - operation: list_topics | create_topic | delete_topic | list_schemas
                - topic: Topic name (for create/delete)
                - num_partitions: Partition count (for create)
                - replication_factor: Replication factor (for create)
                - schema_registry_url: Schema Registry URL
                - schema_subject: Schema subject name

        Returns:
            ActionResult with admin result.
        """
        bootstrap_servers = params.get('bootstrap_servers') or os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS')
        api_key = params.get('api_key') or os.environ.get('CONFLUENT_API_KEY')
        api_secret = params.get('api_secret') or os.environ.get('CONFLUENT_API_SECRET')
        operation = params.get('operation', 'list_topics')

        if not all([bootstrap_servers, api_key, api_secret]):
            return ActionResult(success=False, message="bootstrap_servers, api_key, and api_secret are required")

        import urllib.request
        import urllib.error

        try:
            if operation == 'list_topics':
                from confluent_kafka.admin import AdminClient
                admin = AdminClient({
                    'bootstrap.servers': bootstrap_servers,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': api_key,
                    'sasl.password': api_secret,
                })
                metadata = admin.list_topics(timeout=10)
                topics = list(metadata.topics.keys())
                return ActionResult(success=True, message=f"Found {len(topics)} topics", data={'topics': topics})

            elif operation == 'create_topic':
                from confluent_kafka.admin import AdminClient, NewTopic
                topic_name = params.get('topic')
                if not topic_name:
                    return ActionResult(success=False, message="topic is required for create_topic")
                admin = AdminClient({
                    'bootstrap.servers': bootstrap_servers,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': api_key,
                    'sasl.password': api_secret,
                })
                new_topic = NewTopic(
                    topic_name,
                    num_partitions=params.get('num_partitions', 3),
                    replication_factor=params.get('replication_factor', 3),
                )
                fs = admin.create_topics([new_topic])
                for f in fs.values():
                    f.result()  # Wait for creation
                return ActionResult(success=True, message=f"Topic {topic_name} created")

            elif operation == 'delete_topic':
                from confluent_kafka.admin import AdminClient
                topic_name = params.get('topic')
                if not topic_name:
                    return ActionResult(success=False, message="topic is required for delete_topic")
                admin = AdminClient({
                    'bootstrap.servers': bootstrap_servers,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': api_key,
                    'sasl.password': api_secret,
                })
                fs = admin.delete_topics([topic_name])
                for f in fs.values():
                    f.result()
                return ActionResult(success=True, message=f"Topic {topic_name} deleted")

            elif operation == 'list_schemas':
                schema_registry_url = params.get('schema_registry_url') or os.environ.get('CONFLUENT_SCHEMA_REGISTRY_URL')
                schema_key = params.get('api_key') or os.environ.get('CONFLUENT_SCHEMA_REGISTRY_KEY')
                schema_secret = params.get('api_secret') or os.environ.get('CONFLUENT_SCHEMA_REGISTRY_SECRET')

                if not schema_registry_url:
                    return ActionResult(success=False, message="schema_registry_url is required for list_schemas")

                req = urllib.request.Request(
                    f'{schema_registry_url}/subjects',
                    headers={
                        'Authorization': self._basic_auth(schema_key, schema_secret),
                    }
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    subjects = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(subjects)} schema subjects", data={'subjects': subjects})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError as e:
            return ActionResult(success=False, message=f"Missing dependency: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Kafka admin error: {str(e)}")

    def _basic_auth(self, username: str, password: str) -> str:
        import base64
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        return f"Basic {token}"
