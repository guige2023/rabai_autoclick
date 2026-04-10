"""
Tests for workflow_aws_sns module
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

# Create mock boto3 module before importing workflow_aws_sns
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
from src.workflow_aws_sns import (
    SNSIntegration,
    SNSTopicType,
    MessageStructure,
    DeliveryProtocol,
    FilterPolicyMatchType,
    SNSConfig,
    SNSTopic,
    SNSSubscription,
    PlatformApplication,
    MessageAttributes,
    PublishResult,
    DeliveryStatus,
    CloudWatchMetrics,
)


class TestSNSTopicType(unittest.TestCase):
    """Test SNSTopicType enum"""

    def test_topic_type_values(self):
        self.assertEqual(SNSTopicType.STANDARD.value, "standard")
        self.assertEqual(SNSTopicType.FIFO.value, "fifo")


class TestMessageStructure(unittest.TestCase):
    """Test MessageStructure enum"""

    def test_message_structure_values(self):
        self.assertEqual(MessageStructure.JSON.value, "json")
        self.assertEqual(MessageStructure.STRING.value, "string")
        self.assertEqual(MessageStructure.RAW.value, "raw")
        self.assertEqual(MessageStructure.MULTI_FORMAT.value, "multi")


class TestDeliveryProtocol(unittest.TestCase):
    """Test DeliveryProtocol enum"""

    def test_delivery_protocol_values(self):
        self.assertEqual(DeliveryProtocol.HTTP.value, "http")
        self.assertEqual(DeliveryProtocol.HTTPS.value, "https")
        self.assertEqual(DeliveryProtocol.EMAIL.value, "email")
        self.assertEqual(DeliveryProtocol.EMAIL_JSON.value, "email-json")
        self.assertEqual(DeliveryProtocol.SMS.value, "sms")
        self.assertEqual(DeliveryProtocol.SQS.value, "sqs")
        self.assertEqual(DeliveryProtocol.LAMBDA.value, "lambda")
        self.assertEqual(DeliveryProtocol.PLATFORM_APPLICATION.value, "application")
        self.assertEqual(DeliveryProtocol.FIREHOSE.value, "firehose")


class TestFilterPolicyMatchType(unittest.TestCase):
    """Test FilterPolicyMatchType enum"""

    def test_filter_policy_match_type_values(self):
        self.assertEqual(FilterPolicyMatchType.EXACT.value, "exact")
        self.assertEqual(FilterPolicyMatchType.STARTS_WITH.value, "starts-with")
        self.assertEqual(FilterPolicyMatchType.CONTAINS.value, "contains")
        self.assertEqual(FilterPolicyMatchType.ANY.value, "exists")


class TestSNSConfig(unittest.TestCase):
    """Test SNSConfig dataclass"""

    def test_sns_config_defaults(self):
        config = SNSConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.aws_session_token)
        self.assertIsNone(config.profile_name)
        self.assertIsNone(config.endpoint_url)
        self.assertIsNone(config.config)
        self.assertEqual(config.verify_ssl, True)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)

    def test_sns_config_custom(self):
        config = SNSConfig(
            region_name="eu-west-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            timeout=60
        )
        self.assertEqual(config.region_name, "eu-west-1")
        self.assertEqual(config.timeout, 60)


class TestSNSTopic(unittest.TestCase):
    """Test SNSTopic dataclass"""

    def test_sns_topic_defaults(self):
        topic = SNSTopic(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            name="test-topic"
        )
        self.assertEqual(topic.topic_arn, "arn:aws:sns:us-east-1:123456789012:test-topic")
        self.assertEqual(topic.name, "test-topic")
        self.assertIsNone(topic.display_name)
        self.assertEqual(topic.topic_type, SNSTopicType.STANDARD)
        self.assertIsNone(topic.owner)
        self.assertIsNone(topic.region)
        self.assertIsNone(topic.account_id)
        self.assertIsNone(topic.created_timestamp)
        self.assertFalse(topic.fifo_topic)
        self.assertFalse(topic.content_based_deduplication)
        self.assertEqual(topic.tags, {})


class TestSNSSubscription(unittest.TestCase):
    """Test SNSSubscription dataclass"""

    def test_sns_subscription_defaults(self):
        sub = SNSSubscription(
            subscription_arn="arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id",
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            protocol="sqs",
            endpoint="arn:aws:sqs:us-east-1:123456789012:test-queue"
        )
        self.assertEqual(sub.subscription_arn, "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id")
        self.assertEqual(sub.topic_arn, "arn:aws:sns:us-east-1:123456789012:test-topic")
        self.assertEqual(sub.protocol, "sqs")
        self.assertEqual(sub.endpoint, "arn:aws:sqs:us-east-1:123456789012:test-queue")
        self.assertFalse(sub.confirmation_authenticated)
        self.assertFalse(sub.raw_message_delivery)


class TestPlatformApplication(unittest.TestCase):
    """Test PlatformApplication dataclass"""

    def test_platform_application_defaults(self):
        app = PlatformApplication(
            application_arn="arn:aws:sns:us-east-1:123456789012:app/test-app",
            name="test-app",
            platform="GCM"
        )
        self.assertEqual(app.application_arn, "arn:aws:sns:us-east-1:123456789012:app/test-app")
        self.assertEqual(app.name, "test-app")
        self.assertEqual(app.platform, "GCM")


class TestMessageAttributes(unittest.TestCase):
    """Test MessageAttributes dataclass"""

    def test_message_attributes_defaults(self):
        attrs = MessageAttributes()
        self.assertIsNone(attrs.title)
        self.assertEqual(attrs.message_structure, MessageStructure.JSON)
        self.assertIsNone(attrs.subject)
        self.assertIsNone(attrs.message_group_id)
        self.assertIsNone(attrs.message_deduplication_id)
        self.assertFalse(attrs.content_based_deduplication)


class TestPublishResult(unittest.TestCase):
    """Test PublishResult dataclass"""

    def test_publish_result_defaults(self):
        result = PublishResult(message_id="test-message-id")
        self.assertEqual(result.message_id, "test-message-id")
        self.assertIsNone(result.topic_arn)
        self.assertIsNone(result.target_arn)
        self.assertIsNone(result.sequence_number)
        self.assertIsInstance(result.timestamp, datetime)
        self.assertEqual(result.message_attributes, {})


class TestDeliveryStatus(unittest.TestCase):
    """Test DeliveryStatus dataclass"""

    def test_delivery_status_defaults(self):
        status = DeliveryStatus(message_id="test-id", status="success")
        self.assertEqual(status.message_id, "test-id")
        self.assertEqual(status.status, "success")
        self.assertIsNone(status.provider_response)
        self.assertIsNone(status.delivery_latency)
        self.assertEqual(status.attempts, 1)
        self.assertIsInstance(status.timestamp, datetime)


class TestCloudWatchMetrics(unittest.TestCase):
    """Test CloudWatchMetrics dataclass"""

    def test_cloudwatch_metrics_defaults(self):
        metrics = CloudWatchMetrics(
            topic_name="test-topic",
            metric_name="PublishMode",
            value=100.0,
            unit="Count"
        )
        self.assertEqual(metrics.topic_name, "test-topic")
        self.assertEqual(metrics.metric_name, "PublishMode")
        self.assertEqual(metrics.value, 100.0)
        self.assertEqual(metrics.unit, "Count")
        self.assertIsInstance(metrics.timestamp, datetime)
        self.assertEqual(metrics.dimensions, {})


class TestSNSIntegration(unittest.TestCase):
    """Test SNSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(
                region_name="us-east-1",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret"
            )
            self.integration = SNSIntegration(config=config)

    def test_client_property(self):
        """Test client property returns the client"""
        self.assertEqual(self.integration.client, self.mock_client)

    def test_parse_topic_arn(self):
        """Test parsing topic ARN"""
        result = self.integration._parse_topic_arn(
            "arn:aws:sns:us-east-1:123456789012:my-topic"
        )
        self.assertEqual(result["partition"], "aws")
        self.assertEqual(result["service"], "sns")
        self.assertEqual(result["region"], "us-east-1")
        self.assertEqual(result["account_id"], "123456789012")
        self.assertEqual(result["topic_name"], "my-topic")

    def test_parse_subscription_arn(self):
        """Test parsing subscription ARN"""
        result = self.integration._parse_subscription_arn(
            "arn:aws:sns:us-east-1:123456789012:my-topic:subscription-id"
        )
        self.assertEqual(result["partition"], "aws")
        self.assertEqual(result["service"], "sns")
        self.assertEqual(result["region"], "us-east-1")
        self.assertEqual(result["account_id"], "123456789012")
        self.assertEqual(result["subscription_id"], "subscription-id")


class TestSNSIntegrationTopicManagement(unittest.TestCase):
    """Test SNSIntegration topic management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_create_topic_standard(self):
        """Test creating a standard topic"""
        self.mock_client.create_topic.return_value = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"
        }
        
        topic = self.integration.create_topic(name="test-topic")
        
        self.assertEqual(topic.topic_arn, "arn:aws:sns:us-east-1:123456789012:test-topic")
        self.assertEqual(topic.topic_type, SNSTopicType.STANDARD)
        self.assertFalse(topic.fifo_topic)

    def test_create_topic_fifo(self):
        """Test creating a FIFO topic"""
        self.mock_client.create_topic.return_value = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic.fifo"
        }
        
        topic = self.integration.create_topic(
            name="test-topic",
            fifo_topic=True,
            content_based_deduplication=True
        )
        
        self.assertEqual(topic.topic_type, SNSTopicType.FIFO)
        self.assertTrue(topic.fifo_topic)
        self.assertTrue(topic.content_based_deduplication)

    def test_create_topic_with_display_name(self):
        """Test creating topic with display name"""
        self.mock_client.create_topic.return_value = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"
        }
        
        topic = self.integration.create_topic(
            name="test-topic",
            display_name="Test Topic Display"
        )
        
        self.assertEqual(topic.display_name, "Test Topic Display")

    def test_create_topic_with_tags(self):
        """Test creating topic with tags"""
        self.mock_client.create_topic.return_value = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"
        }
        
        topic = self.integration.create_topic(
            name="test-topic",
            tags={"Environment": "test", "Team": "dev"}
        )
        
        self.assertEqual(topic.tags, {"Environment": "test", "Team": "dev"})
        self.mock_client.tag_resource.assert_called()

    def test_get_topic(self):
        """Test getting topic attributes"""
        self.mock_client.get_topic_attributes.return_value = {
            "Attributes": {
                "DisplayName": "Test Topic",
                "FifoTopic": "false",
                "ContentBasedDeduplication": "false",
                "Owner": "123456789012"
            }
        }
        
        topic = self.integration.get_topic(
            "arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertEqual(topic.topic_arn, "arn:aws:sns:us-east-1:123456789012:test-topic")
        self.assertEqual(topic.display_name, "Test Topic")

    def test_delete_topic(self):
        """Test deleting a topic"""
        self.mock_client.delete_topic.return_value = {}
        
        result = self.integration.delete_topic(
            "arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertTrue(result)
        self.mock_client.delete_topic.assert_called_once()

    def test_set_topic_attributes(self):
        """Test setting topic attributes"""
        self.mock_client.set_topic_attributes.return_value = {}
        
        result = self.integration.set_topic_attributes(
            "arn:aws:sns:us-east-1:123456789012:test-topic",
            "DisplayName",
            "New Display Name"
        )
        
        self.assertTrue(result)

    def test_add_topic_tags(self):
        """Test adding topic tags"""
        result = self.integration.add_topic_tags(
            "arn:aws:sns:us-east-1:123456789012:test-topic",
            {"Environment": "prod"}
        )
        
        self.assertTrue(result)

    def test_remove_topic_tags(self):
        """Test removing topic tags"""
        result = self.integration.remove_topic_tags(
            "arn:aws:sns:us-east-1:123456789012:test-topic",
            ["Environment"]
        )
        
        self.assertTrue(result)


class TestSNSIntegrationSubscriptionManagement(unittest.TestCase):
    """Test SNSIntegration subscription management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_subscribe_sqs(self):
        """Test subscribing an SQS endpoint"""
        self.mock_client.subscribe.return_value = {
            "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        }
        
        sub = self.integration.subscribe(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            protocol=DeliveryProtocol.SQS,
            endpoint="arn:aws:sqs:us-east-1:123456789012:test-queue"
        )
        
        self.assertEqual(sub.subscription_arn, "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id")
        self.assertEqual(sub.protocol, "sqs")

    def test_subscribe_with_filter_policy(self):
        """Test subscribing with filter policy"""
        self.mock_client.subscribe.return_value = {
            "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        }
        self.mock_client.set_subscription_attributes.return_value = {}
        
        filter_policy = {"attribute1": ["value1", "value2"]}
        sub = self.integration.subscribe(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            protocol=DeliveryProtocol.LAMBDA,
            endpoint="arn:aws:lambda:us-east-1:123456789012:function:test",
            filter_policy=filter_policy
        )
        
        self.assertEqual(sub.filter_policy, filter_policy)

    def test_subscribe_with_raw_delivery(self):
        """Test subscribing with raw message delivery"""
        self.mock_client.subscribe.return_value = {
            "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        }
        self.mock_client.set_subscription_attributes.return_value = {}
        
        sub = self.integration.subscribe(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            protocol=DeliveryProtocol.HTTP,
            endpoint="https://example.com/webhook",
            raw_message_delivery=True
        )
        
        self.assertTrue(sub.raw_message_delivery)

    def test_confirm_subscription(self):
        """Test confirming a subscription"""
        self.mock_client.confirm_subscription.return_value = {
            "SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        }
        
        arn = self.integration.confirm_subscription(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            token="test-token"
        )
        
        self.assertEqual(arn, "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id")

    def test_unsubscribe(self):
        """Test unsubscribing"""
        self.mock_client.unsubscribe.return_value = {}
        
        result = self.integration.unsubscribe(
            "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        )
        
        self.assertTrue(result)

    def test_get_subscription(self):
        """Test getting subscription attributes"""
        self.mock_client.get_subscription_attributes.return_value = {
            "Attributes": {
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:test-queue",
                "ConfirmationWasAuthenticated": "true",
                "DeliveryPolicy": "{}",
                "FilterPolicy": "{}",
                "RawMessageDelivery": "true"
            }
        }
        
        sub = self.integration.get_subscription(
            "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        )
        
        self.assertEqual(sub.subscription_arn, "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id")
        self.assertEqual(sub.protocol, "sqs")

    def test_set_subscription_attributes(self):
        """Test setting subscription attributes"""
        self.mock_client.set_subscription_attributes.return_value = {}
        
        result = self.integration.set_subscription_attributes(
            "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id",
            "RawMessageDelivery",
            "true"
        )
        
        self.assertTrue(result)

    def test_set_subscription_filter_policy(self):
        """Test setting subscription filter policy"""
        self.mock_client.set_subscription_attributes.return_value = {}
        
        filter_policy = {"key": ["value1", "value2"]}
        result = self.integration.set_subscription_filter_policy(
            "arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id",
            filter_policy
        )
        
        self.assertTrue(result)


class TestSNSIntegrationMessagePublishing(unittest.TestCase):
    """Test SNSIntegration message publishing"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_publish_string_message(self):
        """Test publishing a string message"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id",
            "SequenceNumber": "1234567890"
        }
        
        result = self.integration.publish(
            message="Hello, World!",
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertEqual(result.message_id, "test-message-id")
        self.assertEqual(result.topic_arn, "arn:aws:sns:us-east-1:123456789012:test-topic")

    def test_publish_dict_message(self):
        """Test publishing a dict message"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish(
            message={"key": "value", "number": 42},
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertEqual(result.message_id, "test-message-id")

    def test_publish_with_subject(self):
        """Test publishing with subject"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish(
            message="Test message",
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            subject="Test Subject"
        )
        
        call_kwargs = self.mock_client.publish.call_args[1]
        self.assertEqual(call_kwargs["Subject"], "Test Subject")

    def test_publish_with_message_attributes(self):
        """Test publishing with message attributes"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish(
            message="Test message",
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            message_attributes={"type": "test", "version": "1.0"}
        )
        
        self.assertEqual(result.message_id, "test-message-id")

    def test_publish_to_topic(self):
        """Test publish_to_topic helper method"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish_to_topic(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            message="Test message",
            subject="Test Subject"
        )
        
        self.assertEqual(result.message_id, "test-message-id")

    def test_publish_fifo(self):
        """Test publishing to FIFO topic"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id",
            "SequenceNumber": "1234567890"
        }
        
        result = self.integration.publish_fifo(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic.fifo",
            message="Test message",
            message_group_id="test-group"
        )
        
        self.assertEqual(result.message_id, "test-message-id")
        
        call_kwargs = self.mock_client.publish.call_args[1]
        self.assertEqual(call_kwargs["MessageGroupId"], "test-group")

    def test_publish_to_phone(self):
        """Test publishing SMS to phone number"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish_to_phone(
            phone_number="+1234567890",
            message="Test SMS"
        )
        
        call_kwargs = self.mock_client.publish.call_args[1]
        self.assertEqual(call_kwargs["PhoneNumber"], "+1234567890")
        self.assertEqual(call_kwargs["MessageStructure"], "string")

    def test_publish_cross_region(self):
        """Test cross-region publishing"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish_cross_region(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            message="Test message",
            target_region="eu-west-1"
        )
        
        self.assertEqual(result.message_id, "test-message-id")

    def test_batch_publish(self):
        """Test batch publishing"""
        self.mock_client.publish_batch.return_value = {
            "Successful": [
                {"Id": "0", "MessageId": "msg-1", "SequenceNumber": "1"},
                {"Id": "1", "MessageId": "msg-2", "SequenceNumber": "2"}
            ],
            "Failed": []
        }
        
        messages = [
            {"body": "message 1", "subject": "Subject 1"},
            {"body": "message 2", "subject": "Subject 2"}
        ]
        
        results = self.integration.batch_publish(
            messages=messages,
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertEqual(len(results), 2)


class TestSNSIntegrationPlatformApplications(unittest.TestCase):
    """Test SNSIntegration platform application management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_create_platform_application(self):
        """Test creating platform application"""
        self.mock_client.create_platform_application.return_value = {
            "PlatformApplicationArn": "arn:aws:sns:us-east-1:123456789012:app/GCM/test-app"
        }
        
        app = self.integration.create_platform_application(
            name="test-app",
            platform="GCM"
        )
        
        self.assertEqual(app.application_arn, "arn:aws:sns:us-east-1:123456789012:app/GCM/test-app")
        self.assertEqual(app.platform, "GCM")

    def test_get_platform_application(self):
        """Test getting platform application attributes"""
        self.mock_client.get_platform_application_attributes.return_value = {
            "Attributes": {
                "PlatformCredential": "test-credential",
                "Platform": "GCM"
            }
        }
        
        app = self.integration.get_platform_application(
            "arn:aws:sns:us-east-1:123456789012:app/GCM/test-app"
        )
        
        self.assertEqual(app.platform, "GCM")

    def test_delete_platform_application(self):
        """Test deleting platform application"""
        self.mock_client.delete_platform_application.return_value = {}
        
        result = self.integration.delete_platform_application(
            "arn:aws:sns:us-east-1:123456789012:app/GCM/test-app"
        )
        
        self.assertTrue(result)

    def test_create_platform_endpoint(self):
        """Test creating platform endpoint"""
        self.mock_client.create_platform_endpoint.return_value = {
            "EndpointArn": "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/test-app/device-token"
        }
        
        arn = self.integration.create_platform_endpoint(
            application_arn="arn:aws:sns:us-east-1:123456789012:app/GCM/test-app",
            token="device-token",
            user_data="test-user-data"
        )
        
        self.assertEqual(arn, "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/test-app/device-token")

    def test_publish_to_endpoint(self):
        """Test publishing to platform endpoint"""
        self.mock_client.publish.return_value = {
            "MessageId": "test-message-id"
        }
        
        result = self.integration.publish_to_endpoint(
            endpoint_arn="arn:aws:sns:us-east-1:123456789012:endpoint/GCM/test-app/device-token",
            message="Test push notification"
        )
        
        self.assertEqual(result.message_id, "test-message-id")


class TestSNSIntegrationMessageFiltering(unittest.TestCase):
    """Test SNSIntegration message filtering"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_create_filter_policy_subscription(self):
        """Test creating filter policy for subscription"""
        self.mock_client.set_subscription_attributes.return_value = {}
        
        policy = {"attribute1": ["value1", "value2"]}
        result = self.integration.create_filter_policy(
            policy=policy,
            subscription_arn="arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id"
        )
        
        self.assertTrue(result)

    def test_create_filter_policy_topic(self):
        """Test creating filter policy for topic"""
        self.mock_client.set_topic_attributes.return_value = {}
        
        policy = {"attribute1": ["value1"]}
        result = self.integration.create_filter_policy(
            policy=policy,
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertTrue(result)

    def test_validate_filter_policy_valid(self):
        """Test validating a valid filter policy"""
        policy = {
            "key1": ["value1", "value2"],
            "key2": [{"exists": True}]
        }
        
        result = self.integration.validate_filter_policy(policy)
        self.assertTrue(result)

    def test_validate_filter_policy_invalid_dict(self):
        """Test validating invalid filter policy (not a dict)"""
        result = self.integration.validate_filter_policy("not a dict")
        self.assertFalse(result)

    def test_validate_filter_policy_invalid_key(self):
        """Test validating filter policy with invalid key"""
        policy = {123: ["value"]}
        
        result = self.integration.validate_filter_policy(policy)
        self.assertFalse(result)

    def test_validate_filter_policy_invalid_value(self):
        """Test validating filter policy with invalid value"""
        policy = {"key": "not a list"}
        
        result = self.integration.validate_filter_policy(policy)
        self.assertFalse(result)

    def test_test_filter_policy_match(self):
        """Test filter policy matching"""
        policy = {"key1": ["value1", "value2"]}
        message_attrs = {"key1": "value1"}
        
        result = self.integration.test_filter_policy(policy, message_attrs)
        self.assertTrue(result)

    def test_test_filter_policy_no_match(self):
        """Test filter policy no match"""
        policy = {"key1": ["value1"]}
        message_attrs = {"key1": "wrong_value"}
        
        result = self.integration.test_filter_policy(policy, message_attrs)
        self.assertFalse(result)

    def test_test_filter_policy_missing_attribute(self):
        """Test filter policy with missing attribute"""
        policy = {"key1": ["value1"]}
        message_attrs = {"key2": "value2"}
        
        result = self.integration.test_filter_policy(policy, message_attrs)
        self.assertFalse(result)

    def test_test_filter_policy_exists_true(self):
        """Test filter policy with exists: true"""
        policy = {"key1": [{"exists": True}]}
        message_attrs = {"key1": "value1"}
        
        result = self.integration.test_filter_policy(policy, message_attrs)
        self.assertTrue(result)

    def test_test_filter_policy_exists_false(self):
        """Test filter policy with exists: false"""
        policy = {"key1": [{"exists": False}]}
        message_attrs = {}
        
        result = self.integration.test_filter_policy(policy, message_attrs)
        self.assertTrue(result)


class TestSNSIntegrationDeliveryStatus(unittest.TestCase):
    """Test SNSIntegration delivery status tracking"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_record_delivery_status(self):
        """Test recording delivery status"""
        status = self.integration.record_delivery_status(
            message_id="test-message-id",
            status="success",
            delivery_latency=100,
            attempts=1
        )
        
        self.assertEqual(status.message_id, "test-message-id")
        self.assertEqual(status.status, "success")
        self.assertEqual(status.delivery_latency, 100)

    def test_get_delivery_status(self):
        """Test getting delivery status"""
        self.integration.record_delivery_status(
            message_id="test-message-id",
            status="success"
        )
        
        status = self.integration.get_delivery_status("test-message-id")
        
        self.assertIsNotNone(status)
        self.assertEqual(status.message_id, "test-message-id")

    def test_get_delivery_status_not_found(self):
        """Test getting non-existent delivery status"""
        status = self.integration.get_delivery_status("nonexistent-id")
        
        self.assertIsNone(status)

    def test_list_delivery_statuses(self):
        """Test listing delivery statuses"""
        self.integration.record_delivery_status("msg-1", "success")
        self.integration.record_delivery_status("msg-2", "failure")
        
        statuses = self.integration.list_delivery_statuses()
        
        self.assertEqual(len(statuses), 2)

    def test_list_delivery_statuses_filtered(self):
        """Test listing filtered delivery statuses"""
        self.integration.record_delivery_status("msg-1", "success")
        self.integration.record_delivery_status("msg-2", "failure")
        
        statuses = self.integration.list_delivery_statuses(status_filter="success")
        
        self.assertEqual(len(statuses), 1)
        self.assertEqual(statuses[0].status, "success")


class TestSNSIntegrationCloudWatch(unittest.TestCase):
    """Test SNSIntegration CloudWatch integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.mock_cw_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.side_effect = [self.mock_client, self.mock_cw_client]
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_set_delivery_status_logging_subscription(self):
        """Test setting delivery status logging for subscription"""
        self.mock_client.set_subscription_attributes.return_value = {}
        
        result = self.integration.set_delivery_status_logging(
            resource_arn="arn:aws:sns:us-east-1:123456789012:test-topic:subscription-id",
            success_sample_rate="50"
        )
        
        self.assertTrue(result)

    def test_set_delivery_status_logging_topic(self):
        """Test setting delivery status logging for topic"""
        self.mock_client.set_topic_attributes.return_value = {}
        
        result = self.integration.set_delivery_status_logging(
            resource_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            success_sample_rate="100"
        )
        
        self.assertTrue(result)

    def test_get_cloudwatch_metrics(self):
        """Test getting CloudWatch metrics"""
        self.mock_cw_client.get_metric_data.return_value = {
            "MetricDataResults": [
                {"Id": "publish", "Values": [100.0]},
                {"Id": "notifications", "Values": [95.0]},
                {"Id": "failures", "Values": [5.0]}
            ]
        }
        
        metrics = self.integration.get_cloudwatch_metrics(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic"
        )
        
        self.assertIsInstance(metrics, list)

    def test_create_cloudwatch_alarm(self):
        """Test creating CloudWatch alarm"""
        self.mock_cw_client.put_metric_alarm.return_value = {}
        
        alarm_arn = self.integration.create_cloudwatch_alarm(
            alarm_name="test-alarm",
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            metric_name="NumberOfNotificationsFailed",
            threshold=10
        )
        
        self.assertIn("alarm:test-alarm", alarm_arn)


class TestSNSIntegrationCrossRegion(unittest.TestCase):
    """Test SNSIntegration cross-region operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.mock_cross_region_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.side_effect = [self.mock_client, self.mock_cross_region_client]
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_publish_cross_region_multi(self):
        """Test publishing to multiple regions"""
        self.mock_client.publish.return_value = {"MessageId": "msg-us"}
        self.mock_cross_region_client.publish.return_value = {"MessageId": "msg-eu"}
        
        results = self.integration.publish_cross_region(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test-topic",
            message="Test message",
            target_regions=["us-east-1", "eu-west-1"]
        )
        
        self.assertIn("us-east-1", results)
        self.assertIn("eu-west-1", results)

    def test_get_topic_arn(self):
        """Test getting topic ARN from name"""
        with patch.object(self.integration, '_get_account_id', return_value='123456789012'):
            arn = self.integration.get_topic_arn("test-topic")
            
            self.assertEqual(arn, "arn:aws:sns:us-east-1:123456789012:test-topic")

    def test_get_topic_arn_with_region(self):
        """Test getting topic ARN with specific region"""
        with patch.object(self.integration, '_get_account_id', return_value='123456789012'):
            arn = self.integration.get_topic_arn("test-topic", region="eu-west-1")
            
            self.assertEqual(arn, "arn:aws:sns:eu-west-1:123456789012:test-topic")

    def test_find_topic_by_name(self):
        """Test finding topic by name"""
        self.mock_client.get_paginator.return_value.paginate.return_value = [
            {"Topics": [{"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"}]}
        ]
        self.mock_client.get_topic_attributes.return_value = {
            "Attributes": {"FifoTopic": "false"}
        }
        
        topic = self.integration.find_topic_by_name("test-topic")
        
        self.assertIsNotNone(topic)
        self.assertEqual(topic.name, "test-topic")


class TestSNSIntegrationUtilityMethods(unittest.TestCase):
    """Test SNSIntegration utility methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        
        with patch('src.workflow_aws_sns.boto3') as mock_boto3:
            mock_boto3.client.return_value = self.mock_client
            
            config = SNSConfig(region_name="us-east-1")
            self.integration = SNSIntegration(config=config)

    def test_close(self):
        """Test close method"""
        self.integration.close()
        
        self.assertIsNone(self.integration._client)
        self.assertEqual(len(self.integration._cross_region_clients), 0)


if __name__ == '__main__':
    unittest.main()
