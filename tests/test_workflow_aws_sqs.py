"""
Tests for workflow_aws_sqs module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_sqs
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now we can import the module
from src.workflow_aws_sqs import (
    SQSIntegration,
    QueueType,
    MessageAttribute,
    QueueConfig,
    MessageConfig,
    ReceiveConfig,
    DeadLetterQueueConfig,
)


class TestQueueType(unittest.TestCase):
    """Test QueueType enum"""

    def test_queue_type_values(self):
        self.assertEqual(QueueType.STANDARD.value, "standard")
        self.assertEqual(QueueType.FIFO.value, "fifo")


class TestMessageAttribute(unittest.TestCase):
    """Test MessageAttribute enum"""

    def test_message_attribute_values(self):
        self.assertEqual(MessageAttribute.ALL.value, "All")
        self.assertEqual(MessageAttribute.SENT_TIMESTAMP.value, "SentTimestamp")
        self.assertEqual(MessageAttribute.APPROXIMATE_RECEIVE_COUNT.value, "ApproximateReceiveCount")
        self.assertEqual(MessageAttribute.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP.value, "ApproximateFirstReceiveTimestamp")


class TestQueueConfig(unittest.TestCase):
    """Test QueueConfig dataclass"""

    def test_queue_config_defaults(self):
        config = QueueConfig(name="test-queue")
        self.assertEqual(config.name, "test-queue")
        self.assertEqual(config.queue_type, QueueType.STANDARD)
        self.assertEqual(config.visibility_timeout, 30)
        self.assertEqual(config.message_retention_period, 345600)
        self.assertEqual(config.maximum_message_size, 262144)
        self.assertEqual(config.delay_seconds, 0)
        self.assertEqual(config.receive_message_wait_time_seconds, 0)
        self.assertEqual(config.redrive_policy, {})
        self.assertEqual(config.tags, {})
        self.assertIsNone(config.kms_master_key_id)
        self.assertEqual(config.kms_data_key_reuse_period_seconds, 300)
        self.assertIsNone(config.policy)
        self.assertEqual(config.content_based_deduplication, False)

    def test_queue_config_fifo(self):
        config = QueueConfig(
            name="test.fifo",
            queue_type=QueueType.FIFO,
            content_based_deduplication=True
        )
        self.assertEqual(config.queue_type, QueueType.FIFO)
        self.assertTrue(config.content_based_deduplication)


class TestMessageConfig(unittest.TestCase):
    """Test MessageConfig dataclass"""

    def test_message_config_defaults(self):
        config = MessageConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            message_body="test message"
        )
        self.assertEqual(config.queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        self.assertEqual(config.message_body, "test message")
        self.assertEqual(config.delay_seconds, 0)
        self.assertEqual(config.message_attributes, {})
        self.assertEqual(config.message_system_attributes, {})
        self.assertIsNone(config.deduplication_id)
        self.assertIsNone(config.group_id)
        self.assertIsNone(config.reply_to)
        self.assertEqual(config.message_structure, "json")

    def test_message_config_dict_body(self):
        config = MessageConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            message_body={"key": "value", "number": 42}
        )
        self.assertIsInstance(config.message_body, dict)


class TestReceiveConfig(unittest.TestCase):
    """Test ReceiveConfig dataclass"""

    def test_receive_config_defaults(self):
        config = ReceiveConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        )
        self.assertEqual(config.queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        self.assertEqual(config.max_number_of_messages, 1)
        self.assertIsNone(config.visibility_timeout)
        self.assertEqual(config.wait_time_seconds, 0)
        self.assertIsNone(config.receive_request_attempt_id)
        self.assertEqual(config.message_attribute_names, [])
        self.assertEqual(config.attribute_names, [])
        self.assertEqual(config.generic_attribute_names, [])


class TestDeadLetterQueueConfig(unittest.TestCase):
    """Test DeadLetterQueueConfig dataclass"""

    def test_dlq_config_defaults(self):
        config = DeadLetterQueueConfig(name="test-dlq")
        self.assertEqual(config.name, "test-dlq")
        self.assertEqual(config.max_receive_count, 10)
        self.assertEqual(config.visibility_timeout, 30)
        self.assertEqual(config.message_retention_period, 1209600)


class TestSQSIntegration(unittest.TestCase):
    """Test SQSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        # Create integration instance with mocked clients
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret",
                region_name="us-east-1",
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_init_without_boto3(self):
        """Test initialization raises ImportError when boto3 not available"""
        with patch.dict('sys.modules', {'boto3': None}):
            with self.assertRaises(ImportError):
                SQSIntegration()

    def test_sqs_client_property(self):
        """Test sqs_client property returns the client"""
        self.assertEqual(self.integration.sqs_client, self.mock_sqs_client)

    def test_cloudwatch_client_property(self):
        """Test cloudwatch_client property returns the client"""
        self.assertEqual(self.integration.cloudwatch_client, self.mock_cloudwatch_client)

    def test_generate_deduplication_id_string(self):
        """Test deduplication ID generation for string message"""
        dedup_id = self.integration._generate_deduplication_id("test message")
        self.assertEqual(len(dedup_id), 64)

    def test_generate_deduplication_id_dict(self):
        """Test deduplication ID generation for dict message"""
        dedup_id = self.integration._generate_deduplication_id({"key": "value"})
        self.assertEqual(len(dedup_id), 64)

    def test_record_metric(self):
        """Test metric recording"""
        self.integration._record_metric("TestMetric", 1.0, "Count", {"Dimension": "Value"})
        self.assertEqual(len(self.integration._metrics_buffer), 1)
        self.assertEqual(self.integration._metrics_buffer[0]["MetricName"], "TestMetric")

    def test_flush_metrics_success(self):
        """Test flushing metrics to CloudWatch"""
        self.integration._record_metric("TestMetric", 1.0, "Count")
        
        self.mock_cloudwatch_client.put_metric_data.return_value = {}
        
        self.integration.flush_metrics()
        
        self.mock_cloudwatch_client.put_metric_data.assert_called_once()
        self.assertEqual(len(self.integration._metrics_buffer), 0)

    def test_flush_metrics_empty(self):
        """Test flushing empty metrics buffer"""
        # Should not raise
        self.integration.flush_metrics()
        self.mock_cloudwatch_client.put_metric_data.assert_not_called()


class TestSQSIntegrationQueueOperations(unittest.TestCase):
    """Test SQSIntegration queue operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_create_queue_standard(self):
        """Test creating a standard queue"""
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        }
        
        config = QueueConfig(name="test-queue")
        queue_url = self.integration.create_queue(config)
        
        self.assertEqual(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        self.mock_sqs_client.create_queue.assert_called_once()
        
        call_kwargs = self.mock_sqs_client.create_queue.call_args[1]
        self.assertEqual(call_kwargs["QueueName"], "test-queue")
        self.assertNotIn("FifoQueue", call_kwargs["Attributes"])

    def test_create_queue_fifo(self):
        """Test creating a FIFO queue"""
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test.fifo"
        }
        
        config = QueueConfig(name="test", queue_type=QueueType.FIFO)
        queue_url = self.integration.create_queue(config)
        
        self.assertEqual(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test.fifo")
        
        call_kwargs = self.mock_sqs_client.create_queue.call_args[1]
        self.assertEqual(call_kwargs["QueueName"], "test.fifo")
        self.assertEqual(call_kwargs["Attributes"]["FifoQueue"], "true")

    def test_create_queue_with_dlq(self):
        """Test creating a queue with dead letter queue"""
        self.mock_sqs_client.create_queue.side_effect = [
            {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"},
            {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test-dlq"}
        ]
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123456789:test-dlq"}
        }
        
        dlq_config = DeadLetterQueueConfig(name="test-dlq", max_receive_count=5)
        config = QueueConfig(name="test-queue")
        
        queue_url = self.integration.create_queue(config, create_dlq=True, dlq_config=dlq_config)
        
        self.assertEqual(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")

    def test_create_queue_cached(self):
        """Test queue creation returns cached URL"""
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        }
        
        config = QueueConfig(name="test-queue")
        
        # First call
        queue_url1 = self.integration.create_queue(config)
        # Second call should use cache
        queue_url2 = self.integration.create_queue(config)
        
        self.assertEqual(queue_url1, queue_url2)
        self.assertEqual(self.mock_sqs_client.create_queue.call_count, 1)

    def test_get_queue_url_found(self):
        """Test getting queue URL when queue exists"""
        self.mock_sqs_client.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        }
        
        queue_url = self.integration.get_queue_url("test-queue")
        
        self.assertEqual(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")

    def test_get_queue_url_not_found(self):
        """Test getting queue URL when queue doesn't exist"""
        from botocore.exceptions import ClientError
        
        error_response = {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue"}}
        self.mock_sqs_client.get_queue_url.side_effect = ClientError(error_response, "GetQueueUrl")
        
        queue_url = self.integration.get_queue_url("nonexistent-queue")
        
        self.assertIsNone(queue_url)

    def test_get_queue_arn(self):
        """Test getting queue ARN"""
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123456789:test-queue"}
        }
        
        arn = self.integration.get_queue_arn("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        
        self.assertEqual(arn, "arn:aws:sqs:us-east-1:123456789:test-queue")

    def test_delete_queue_success(self):
        """Test deleting a queue successfully"""
        self.mock_sqs_client.delete_queue.return_value = {}
        
        result = self.integration.delete_queue("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        
        self.assertTrue(result)
        self.mock_sqs_client.delete_queue.assert_called_once()

    def test_delete_queue_failure(self):
        """Test deleting a queue failure"""
        self.mock_sqs_client.delete_queue.side_effect = Exception("Delete failed")
        
        result = self.integration.delete_queue("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        
        self.assertFalse(result)

    def test_list_queues(self):
        """Test listing queues"""
        self.mock_sqs_client.list_queues.return_value = {
            "QueueUrls": [
                "https://sqs.us-east-1.amazonaws.com/123456789/queue1",
                "https://sqs.us-east-1.amazonaws.com/123456789/queue2"
            ]
        }
        
        queues = self.integration.list_queues()
        
        self.assertEqual(len(queues), 2)
        self.mock_sqs_client.list_queues.assert_called_once()

    def test_list_queues_with_prefix(self):
        """Test listing queues with prefix filter"""
        self.mock_sqs_client.list_queues.return_value = {
            "QueueUrls": ["https://sqs.us-east-1.amazonaws.com/123456789/test-queue"]
        }
        
        queues = self.integration.list_queues(prefix="test")
        
        self.assertEqual(len(queues), 1)
        self.mock_sqs_client.list_queues.assert_called_with(QueueNamePrefix="test")

    def test_set_queue_attributes(self):
        """Test setting queue attributes"""
        self.mock_sqs_client.set_queue_attributes.return_value = {}
        
        result = self.integration.set_queue_attributes(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            {"VisibilityTimeout": "60"}
        )
        
        self.assertTrue(result)
        self.mock_sqs_client.set_queue_attributes.assert_called_once()

    def test_get_queue_attributes(self):
        """Test getting queue attributes"""
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {
                "VisibilityTimeout": "30",
                "QueueArn": "arn:aws:sqs:us-east-1:123456789:test-queue"
            }
        }
        
        attrs = self.integration.get_queue_attributes(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ["VisibilityTimeout", "QueueArn"]
        )
        
        self.assertEqual(attrs["VisibilityTimeout"], "30")
        self.assertEqual(attrs["QueueArn"], "arn:aws:sqs:us-east-1:123456789:test-queue")


class TestSQSIntegrationMessageOperations(unittest.TestCase):
    """Test SQSIntegration message operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_send_message_string(self):
        """Test sending a string message"""
        self.mock_sqs_client.send_message.return_value = {
            "MessageId": "test-message-id",
            "MD5OfMessageBody": "test-checksum"
        }
        
        config = MessageConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            message_body="test message"
        )
        
        result = self.integration.send_message(config)
        
        self.assertEqual(result["MessageId"], "test-message-id")
        self.mock_sqs_client.send_message.assert_called_once()

    def test_send_message_dict(self):
        """Test sending a dict message"""
        self.mock_sqs_client.send_message.return_value = {
            "MessageId": "test-message-id",
            "MD5OfMessageBody": "test-checksum"
        }
        
        config = MessageConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            message_body={"key": "value"}
        )
        
        result = self.integration.send_message(config)
        
        self.assertEqual(result["MessageId"], "test-message-id")

    def test_send_message_fifo_with_deduplication_id(self):
        """Test sending message to FIFO queue with deduplication ID"""
        self.mock_sqs_client.send_message.return_value = {
            "MessageId": "test-message-id",
            "MD5OfMessageBody": "test-checksum"
        }
        
        config = MessageConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test.fifo",
            message_body="test message",
            deduplication_id="test-dedup-id",
            group_id="test-group"
        )
        
        result = self.integration.send_message(config)
        
        self.assertEqual(result["MessageId"], "test-message-id")
        
        call_kwargs = self.mock_sqs_client.send_message.call_args[1]
        self.assertEqual(call_kwargs["MessageDeduplicationId"], "test-dedup-id")
        self.assertEqual(call_kwargs["MessageGroupId"], "test-group")

    def test_send_message_batch(self):
        """Test sending batch messages"""
        self.mock_sqs_client.send_message_batch.return_value = {
            "Successful": [{"Id": "0", "MessageId": "msg-1"}, {"Id": "1", "MessageId": "msg-2"}],
            "Failed": []
        }
        
        messages = [
            MessageConfig(queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue", message_body="msg1"),
            MessageConfig(queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue", message_body="msg2")
        ]
        
        result = self.integration.send_message_batch(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            messages
        )
        
        self.assertEqual(len(result["Successful"]), 2)
        self.assertEqual(len(result["Failed"]), 0)

    def test_receive_messages(self):
        """Test receiving messages"""
        self.mock_sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "msg-1",
                    "Body": '{"key": "value"}',
                    "ReceiptHandle": "receipt-handle-1",
                    "MD5OfBody": "test-md5",
                    "Attributes": {"ApproximateReceiveCount": "1"}
                }
            ]
        }
        
        config = ReceiveConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            max_number_of_messages=10
        )
        
        messages = self.integration.receive_messages(config)
        
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["MessageId"], "msg-1")
        self.assertEqual(messages[0]["BodyParsed"], {"key": "value"})

    def test_receive_messages_empty(self):
        """Test receiving messages when queue is empty"""
        self.mock_sqs_client.receive_message.return_value = {"Messages": []}
        
        config = ReceiveConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        )
        
        messages = self.integration.receive_messages(config)
        
        self.assertEqual(len(messages), 0)

    def test_delete_message(self):
        """Test deleting a message"""
        self.mock_sqs_client.delete_message.return_value = {}
        
        result = self.integration.delete_message(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            "receipt-handle-123"
        )
        
        self.assertTrue(result)
        self.mock_sqs_client.delete_message.assert_called_once()

    def test_delete_message_batch(self):
        """Test batch deleting messages"""
        self.mock_sqs_client.delete_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": []
        }
        
        result = self.integration.delete_message_batch(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ["receipt-1", "receipt-2"]
        )
        
        self.assertEqual(len(result["Successful"]), 2)

    def test_purge_queue(self):
        """Test purging a queue"""
        self.mock_sqs_client.purge_queue.return_value = {}
        
        result = self.integration.purge_queue(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        )
        
        self.assertTrue(result)
        self.mock_sqs_client.purge_queue.assert_called_once()

    def test_change_message_visibility(self):
        """Test changing message visibility"""
        self.mock_sqs_client.change_message_visibility.return_value = {}
        
        result = self.integration.change_message_visibility(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            "receipt-handle-123",
            60
        )
        
        self.assertTrue(result)

    def test_change_message_visibility_batch(self):
        """Test batch changing message visibility"""
        self.mock_sqs_client.change_message_visibility_batch.return_value = {
            "Successful": [{"Id": "0"}],
            "Failed": []
        }
        
        entries = [
            {"Id": "0", "ReceiptHandle": "rh1", "VisibilityTimeout": 60},
            {"Id": "1", "ReceiptHandle": "rh2", "VisibilityTimeout": 60}
        ]
        
        result = self.integration.change_message_visibility_batch(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            entries
        )
        
        self.assertIn("Successful", result)


class TestSQSIntegrationQueueManagement(unittest.TestCase):
    """Test SQSIntegration queue management features"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_add_permission(self):
        """Test adding queue permission"""
        self.mock_sqs_client.add_permission.return_value = {}
        
        result = self.integration.add_permission(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            "test-label",
            ["123456789012"],
            ["sqs:SendMessage", "sqs:ReceiveMessage"]
        )
        
        self.assertTrue(result)

    def test_remove_permission(self):
        """Test removing queue permission"""
        self.mock_sqs_client.remove_permission.return_value = {}
        
        result = self.integration.remove_permission(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            "test-label"
        )
        
        self.assertTrue(result)

    def test_tag_queue(self):
        """Test tagging a queue"""
        self.mock_sqs_client.tag_queue.return_value = {}
        
        result = self.integration.tag_queue(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            {"Environment": "test", "Team": "dev"}
        )
        
        self.assertTrue(result)

    def test_untag_queue(self):
        """Test untagging a queue"""
        self.mock_sqs_client.untag_queue.return_value = {}
        
        result = self.integration.untag_queue(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ["Environment"]
        )
        
        self.assertTrue(result)

    def test_list_queue_tags(self):
        """Test listing queue tags"""
        self.mock_sqs_client.list_queue_tags.return_value = {
            "Tags": {"Environment": "test", "Team": "dev"}
        }
        
        tags = self.integration.list_queue_tags(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        )
        
        self.assertEqual(tags["Environment"], "test")
        self.assertEqual(tags["Team"], "dev")

    def test_get_dead_letter_source_queues(self):
        """Test getting dead letter source queues"""
        self.mock_sqs_client.list_dead_letter_source_queues.return_value = {
            "queueUrls": ["https://sqs.us-east-1.amazonaws.com/123456789/source-queue"]
        }
        
        result = self.integration.get_dead_letter_source_queues(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-dlq"
        )
        
        self.assertEqual(len(result), 1)

    def test_configure_redrive_policy(self):
        """Test configuring redrive policy"""
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123456789:test-dlq"}
        }
        self.mock_sqs_client.set_queue_attributes.return_value = {}
        
        result = self.integration.configure_redrive_policy(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            "https://sqs.us-east-1.amazonaws.com/123456789/test-dlq",
            max_receive_count=5
        )
        
        self.assertTrue(result)

    def test_set_long_polling(self):
        """Test setting long polling"""
        self.mock_sqs_client.set_queue_attributes.return_value = {}
        
        result = self.integration.set_long_polling(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            wait_time_seconds=20
        )
        
        self.assertTrue(result)

    def test_set_visibility_timeout(self):
        """Test setting visibility timeout"""
        self.mock_sqs_client.set_queue_attributes.return_value = {}
        
        result = self.integration.set_visibility_timeout(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            visibility_timeout=60
        )
        
        self.assertTrue(result)


class TestSQSIntegrationMetrics(unittest.TestCase):
    """Test SQSIntegration metrics and monitoring"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_get_queue_metrics(self):
        """Test getting queue metrics"""
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "10",
                "ApproximateNumberOfMessagesDelayed": "2",
                "ApproximateNumberOfMessagesNotVisible": "3",
                "MaximumMessageSize": "262144",
                "MessageRetentionPeriod": "345600",
                "ReceiveMessageWaitTimeSeconds": "0",
                "VisibilityTimeout": "30"
            }
        }
        
        metrics = self.integration.get_queue_metrics(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
        )
        
        self.assertEqual(metrics["ApproximateNumberOfMessages"], 10)
        self.assertEqual(metrics["ApproximateNumberOfMessagesDelayed"], 2)
        self.assertEqual(metrics["ApproximateNumberOfMessagesNotVisible"], 3)

    def test_get_cloudwatch_metrics(self):
        """Test getting CloudWatch metrics"""
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123456789:test-queue"}
        }
        
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Timestamp": datetime.utcnow(), "Sum": 100, "Average": 10, "Maximum": 50}
            ]
        }
        
        start_time = datetime.utcnow() - timedelta(hours=1)
        end_time = datetime.utcnow()
        
        metrics = self.integration.get_cloudwatch_metrics(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            start_time,
            end_time
        )
        
        self.assertIsInstance(metrics, dict)


class TestSQSIntegrationHelperMethods(unittest.TestCase):
    """Test SQSIntegration helper methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_create_fifo_queue(self):
        """Test creating a FIFO queue using helper method"""
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/test.fifo"
        }
        
        queue_url = self.integration.create_fifo_queue(
            name="test",
            content_based_deduplication=True
        )
        
        self.assertEqual(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789/test.fifo")

    def test_create_dlq_with_redrive(self):
        """Test creating DLQ with redrive policy"""
        self.mock_sqs_client.create_queue.side_effect = [
            {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/source"},
            {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/dlq"}
        ]
        self.mock_sqs_client.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/dlq"
        }
        self.mock_sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123456789:dlq"}
        }
        
        source_url, dlq_url = self.integration.create_dlq_with_redrive(
            source_queue_name="source",
            dlq_name="dlq",
            max_receive_count=5
        )
        
        self.assertEqual(source_url, "https://sqs.us-east-1.amazonaws.com/123456789/source")

    def test_process_messages(self):
        """Test processing messages with callback"""
        self.mock_sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "msg-1",
                    "Body": "test",
                    "ReceiptHandle": "rh1"
                }
            ]
        }
        self.mock_sqs_client.delete_message.return_value = {}
        
        processed = []
        def callback(msg):
            processed.append(msg)
            return True
        
        count = self.integration.process_messages(
            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            callback,
            auto_delete=True
        )
        
        self.assertEqual(count, 1)
        self.assertEqual(len(processed), 1)

    def test_parse_message_body_json(self):
        """Test parsing JSON message body"""
        result = self.integration._parse_message_body('{"key": "value"}')
        self.assertEqual(result, {"key": "value"})

    def test_parse_message_body_string(self):
        """Test parsing non-JSON message body"""
        result = self.integration._parse_message_body("plain text")
        self.assertEqual(result, "plain text")

    def test_parse_message_body_empty(self):
        """Test parsing empty message body"""
        result = self.integration._parse_message_body("")
        self.assertIsNone(result)


class TestSQSIntegrationContextManager(unittest.TestCase):
    """Test SQSIntegration context manager"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sqs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        with patch('src.workflow_aws_sqs.boto3') as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.side_effect = [self.mock_sqs_client, self.mock_cloudwatch_client]
            
            self.integration = SQSIntegration(
                sqs_client=self.mock_sqs_client,
                cloudwatch_client=self.mock_cloudwatch_client
            )

    def test_context_manager(self):
        """Test using integration as context manager"""
        self.mock_cloudwatch_client.put_metric_data.return_value = {}
        
        with self.integration as integ:
            self.assertIsNotNone(integ)
        
        # After context exit, clients should be None
        self.assertIsNone(self.integration._sqs_client)

    def test_close(self):
        """Test close method"""
        self.mock_cloudwatch_client.put_metric_data.return_value = {}
        
        self.integration.close()
        
        self.assertIsNone(self.integration._sqs_client)
        self.assertIsNone(self.integration._cloudwatch_client)


if __name__ == '__main__':
    unittest.main()
