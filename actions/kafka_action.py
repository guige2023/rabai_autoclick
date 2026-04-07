"""Kafka action module for RabAI AutoClick.

Provides Kafka messaging operations:
- KafkaProduceAction: Produce message to topic
- KafkaConsumeAction: Consume messages from topic
- KafkaListTopicsAction: List topics
- KafkaCreateTopicAction: Create topic
- KafkaDeleteTopicAction: Delete topic
- KafkaOffsetsAction: Get consumer offsets
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class KafkaProduceAction(BaseAction):
    """Produce message to Kafka topic."""
    action_type = "kafka_produce"
    display_name = "Kafka生产消息"
    description = "向Kafka主题生产消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute produce.

        Args:
            context: Execution context.
            params: Dict with brokers, topic, message, key.

        Returns:
            ActionResult indicating success.
        """
        brokers = params.get('brokers', 'localhost:9092')
        topic = params.get('topic', '')
        message = params.get('message', '')
        key = params.get('key', '')

        valid, msg = self.validate_type(topic, str, 'topic')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)
            resolved_topic = context.resolve_value(topic)
            resolved_message = context.resolve_value(message)
            resolved_key = context.resolve_value(key) if key else None

            try:
                from confluent_kafka import Producer
                producer = Producer({'bootstrap.servers': resolved_brokers})

                if isinstance(resolved_message, dict):
                    msg_value = json.dumps(resolved_message).encode('utf-8')
                else:
                    msg_value = str(resolved_message).encode('utf-8')

                msg_key = resolved_key.encode('utf-8') if resolved_key else None

                producer.produce(
                    resolved_topic,
                    key=msg_key,
                    value=msg_value
                )
                producer.flush(timeout=10)

                return ActionResult(
                    success=True,
                    message=f"消息已生产到: {resolved_topic}",
                    data={'topic': resolved_topic, 'key': resolved_key}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装: pip install confluent-kafka"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka生产失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka生产失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['topic', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092', 'key': ''}


class KafkaConsumeAction(BaseAction):
    """Consume messages from Kafka topic."""
    action_type = "kafka_consume"
    display_name = "Kafka消费消息"
    description = "从Kafka主题消费消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute consume.

        Args:
            context: Execution context.
            params: Dict with brokers, topic, group_id, count, output_var.

        Returns:
            ActionResult with messages.
        """
        brokers = params.get('brokers', 'localhost:9092')
        topic = params.get('topic', '')
        group_id = params.get('group_id', 'rabai_consumer')
        count = params.get('count', 10)
        output_var = params.get('output_var', 'kafka_messages')

        valid, msg = self.validate_type(topic, str, 'topic')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)
            resolved_topic = context.resolve_value(topic)
            resolved_group = context.resolve_value(group_id)
            resolved_count = context.resolve_value(count)

            try:
                from confluent_kafka import Consumer

                consumer = Consumer({
                    'bootstrap.servers': resolved_brokers,
                    'group.id': resolved_group,
                    'auto.offset.reset': 'earliest'
                })

                consumer.subscribe([resolved_topic])

                messages = []
                for _ in range(int(resolved_count)):
                    msg = consumer.poll(timeout=1.0)
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
                        'offset': msg.offset()
                    })

                consumer.close()

                context.set(output_var, messages)

                return ActionResult(
                    success=True,
                    message=f"消费 {len(messages)} 条消息",
                    data={'count': len(messages), 'messages': messages, 'output_var': output_var}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka消费失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka消费失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['topic']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092', 'group_id': 'rabai_consumer', 'count': 10, 'output_var': 'kafka_messages'}


class KafkaListTopicsAction(BaseAction):
    """List Kafka topics."""
    action_type = "kafka_list_topics"
    display_name = "Kafka列出主题"
    description = "列出所有Kafka主题"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list topics.

        Args:
            context: Execution context.
            params: Dict with brokers, output_var.

        Returns:
            ActionResult with topic list.
        """
        brokers = params.get('brokers', 'localhost:9092')
        output_var = params.get('output_var', 'kafka_topics')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)

            try:
                from confluent_kafka.admin import AdminClient

                admin = AdminClient({'bootstrap.servers': resolved_brokers})
                topics = admin.list_topics(timeout=5).topics

                topic_names = [t for t in topics.keys() if not t.startswith('__')]

                context.set(output_var, topic_names)

                return ActionResult(
                    success=True,
                    message=f"Kafka主题: {len(topic_names)} 个",
                    data={'count': len(topic_names), 'topics': topic_names, 'output_var': output_var}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka列出主题失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka列出主题失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092', 'output_var': 'kafka_topics'}


class KafkaCreateTopicAction(BaseAction):
    """Create Kafka topic."""
    action_type = "kafka_create_topic"
    display_name = "Kafka创建主题"
    description = "创建Kafka主题"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create topic.

        Args:
            context: Execution context.
            params: Dict with brokers, topic, partitions, replication_factor.

        Returns:
            ActionResult indicating success.
        """
        brokers = params.get('brokers', 'localhost:9092')
        topic = params.get('topic', '')
        partitions = params.get('partitions', 3)
        replication_factor = params.get('replication_factor', 1)

        valid, msg = self.validate_type(topic, str, 'topic')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)
            resolved_topic = context.resolve_value(topic)
            resolved_parts = context.resolve_value(partitions)
            resolved_replication = context.resolve_value(replication_factor)

            try:
                from confluent_kafka.admin import AdminClient, NewTopic

                admin = AdminClient({'bootstrap.servers': resolved_brokers})

                new_topic = NewTopic(
                    resolved_topic,
                    num_partitions=int(resolved_parts),
                    replication_factor=int(resolved_replication)
                )

                fs = admin.create_topics([new_topic])

                # Wait for result
                for f in fs.values():
                    f.result()

                return ActionResult(
                    success=True,
                    message=f"主题已创建: {resolved_topic} (partitions={resolved_parts}, rf={resolved_replication})",
                    data={'topic': resolved_topic, 'partitions': resolved_parts, 'replication_factor': resolved_replication}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka创建主题失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka创建主题失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['topic']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092', 'partitions': 3, 'replication_factor': 1}


class KafkaDeleteTopicAction(BaseAction):
    """Delete Kafka topic."""
    action_type = "kafka_delete_topic"
    display_name = "Kafka删除主题"
    description = "删除Kafka主题"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete topic.

        Args:
            context: Execution context.
            params: Dict with brokers, topic.

        Returns:
            ActionResult indicating success.
        """
        brokers = params.get('brokers', 'localhost:9092')
        topic = params.get('topic', '')

        valid, msg = self.validate_type(topic, str, 'topic')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)
            resolved_topic = context.resolve_value(topic)

            try:
                from confluent_kafka.admin import AdminClient

                admin = AdminClient({'bootstrap.servers': resolved_brokers})
                fs = admin.delete_topics([resolved_topic])

                for f in fs.values():
                    f.result()

                return ActionResult(
                    success=True,
                    message=f"主题已删除: {resolved_topic}",
                    data={'topic': resolved_topic}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka删除主题失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka删除主题失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['topic']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092'}


class KafkaOffsetsAction(BaseAction):
    """Get consumer group offsets."""
    action_type = "kafka_offsets"
    display_name = "Kafka获取偏移量"
    description = "获取Kafka消费者组偏移量"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute offsets.

        Args:
            context: Execution context.
            params: Dict with brokers, group_id, topic, output_var.

        Returns:
            ActionResult with offsets.
        """
        brokers = params.get('brokers', 'localhost:9092')
        group_id = params.get('group_id', '')
        topic = params.get('topic', '')
        output_var = params.get('output_var', 'kafka_offsets')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_brokers = context.resolve_value(brokers)
            resolved_group = context.resolve_value(group_id) if group_id else None
            resolved_topic = context.resolve_value(topic) if topic else None

            try:
                from confluent_kafka.admin import AdminClient

                admin = AdminClient({'bootstrap.servers': resolved_brokers})

                if resolved_group:
                    # List consumer group offsets
                    try:
                        # Use list_consumer_group_offsets
                        offsets = admin.list_consumer_group_offsets(resolved_group)
                        result = offsets.result()

                        offsets_data = []
                        for tp, offset_info in result.items():
                            offsets_data.append({
                                'topic': tp.topic,
                                'partition': tp.partition,
                                'offset': offset_info.offset,
                                'lag': offset_info.lag if hasattr(offset_info, 'lag') else None
                            })

                        context.set(output_var, offsets_data)

                        return ActionResult(
                            success=True,
                            message=f"消费者组 {resolved_group} 偏移量: {len(offsets_data)}",
                            data={'offsets': offsets_data, 'output_var': output_var}
                        )
                    except Exception:
                        context.set(output_var, [])
                        return ActionResult(
                            success=True,
                            message=f"无法获取消费者组偏移量",
                            data={'offsets': [], 'output_var': output_var}
                        )
                else:
                    # List topics
                    topics = admin.list_topics(timeout=5).topics
                    topic_names = [t for t in topics.keys() if not t.startswith('__')]
                    context.set(output_var, topic_names)
                    return ActionResult(
                        success=True,
                        message=f"Kafka主题: {len(topic_names)} 个",
                        data={'topics': topic_names, 'output_var': output_var}
                    )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="confluent-kafka未安装"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Kafka获取偏移量失败: {str(e)}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka获取偏移量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'brokers': 'localhost:9092', 'group_id': '', 'topic': '', 'output_var': 'kafka_offsets'}
