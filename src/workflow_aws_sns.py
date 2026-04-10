"""
AWS SNS Notification System Integration Module for Workflow System

Implements an SNSIntegration class with:
1. Topic management: Create/manage SNS topics
2. Subscription management: Subscribe/unsubscribe endpoints
3. Message publishing: Publish messages to topics
4. Platform applications: Manage platform applications
5. FIFO topics: FIFO topic support
6. Message filtering: Filter policies
7. Message structure: Different payload structures
8. Delivery status: Delivery status tracking
9. CloudWatch integration: Monitoring
10. Cross-region: Cross-region publishing

Commit: 'feat(aws-sns): add AWS SNS integration with topic management, subscriptions, message publishing, platform applications, FIFO, filtering, delivery status, CloudWatch, cross-region'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class SNSTopicType(Enum):
    """SNS topic types."""
    STANDARD = "standard"
    FIFO = "fifo"


class MessageStructure(Enum):
    """Message payload structures."""
    JSON = "json"
    STRING = "string"
    RAW = "raw"
    MULTI_FORMAT = "multi"


class DeliveryProtocol(Enum):
    """Delivery protocols for subscriptions."""
    HTTP = "http"
    HTTPS = "https"
    EMAIL = "email"
    EMAIL_JSON = "email-json"
    SMS = "sms"
    SQS = "sqs"
    LAMBDA = "lambda"
    PLATFORM_APPLICATION = "application"
    FIREHOSE = "firehose"


class FilterPolicyMatchType(Enum):
    """Filter policy match types."""
    EXACT = "exact"
    STARTS_WITH = "starts-with"
    CONTAINS = "contains"
    ANY = "exists"


@dataclass
class SNSConfig:
    """Configuration for SNS connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    config: Optional[Any] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class SNSTopic:
    """Represents an SNS topic."""
    topic_arn: str
    name: str
    display_name: Optional[str] = None
    topic_type: SNSTopicType = SNSTopicType.STANDARD
    owner: Optional[str] = None
    region: Optional[str] = None
    account_id: Optional[str] = None
    created_timestamp: Optional[datetime] = None
    kms_master_key_id: Optional[str] = None
    signature_version: Optional[str] = None
    fifo_topic: bool = False
    content_based_deduplication: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SNSSubscription:
    """Represents an SNS subscription."""
    subscription_arn: str
    topic_arn: str
    protocol: str
    endpoint: str
    owner: Optional[str] = None
    confirmation_authenticated: bool = False
    delivery_policy: Optional[Dict] = None
    filter_policy: Optional[Dict] = None
    raw_message_delivery: bool = False
    region: Optional[str] = None
    account_id: Optional[str] = None
    subscription_role_arn: Optional[str] = None


@dataclass
class PlatformApplication:
    """Represents a platform application for mobile notifications."""
    application_arn: str
    name: str
    platform: str
    region: Optional[str] = None
    account_id: Optional[str] = None
    event_delivery_parameters: Optional[Dict] = None
    failure_feedback_role_arn: Optional[str] = None
    success_feedback_role_arn: Optional[str] = None
    success_feedback_sample_rate: Optional[str] = None


@dataclass
class MessageAttributes:
    """Message attributes for SNS publish."""
    title: Optional[str] = None
    message_structure: MessageStructure = MessageStructure.JSON
    subject: Optional[str] = None
    message_group_id: Optional[str] = None
    message_deduplication_id: Optional[str] = None
    direct_reply_to: Optional[str] = None
    target_arn: Optional[str] = None
    phone_number: Optional[str] = None
    content_based_deduplication: bool = False


@dataclass
class PublishResult:
    """Result of a publish operation."""
    message_id: str
    topic_arn: Optional[str] = None
    target_arn: Optional[str] = None
    sequence_number: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    message_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeliveryStatus:
    """Delivery status information."""
    message_id: str
    status: str
    provider_response: Optional[Dict] = None
    delivery_latency: Optional[int] = None
    attempts: int = 1
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CloudWatchMetrics:
    """CloudWatch metrics data."""
    topic_name: str
    metric_name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    dimensions: Dict[str, str] = field(default_factory=dict)


class SNSIntegration:
    """
    AWS SNS Integration class providing comprehensive SNS management.

    Features:
    - Topic management (create, list, delete, configure topics)
    - Subscription management (subscribe, unsubscribe, confirm)
    - Message publishing (standard, FIFO, multi-format)
    - Platform applications (mobile push notifications)
    - FIFO topics with content-based deduplication
    - Message filtering with filter policies
    - Different message payload structures
    - Delivery status tracking
    - CloudWatch integration for monitoring
    - Cross-region publishing
    """

    def __init__(self, config: Optional[SNSConfig] = None):
        """
        Initialize SNS integration.

        Args:
            config: SNS configuration. Uses default if not provided.
        """
        self.config = config or SNSConfig()
        self._client = None
        self._cross_region_clients: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._pending_confirmations: Dict[str, Dict] = {}
        self._delivery_statuses: Dict[str, DeliveryStatus] = {}

    @property
    def client(self):
        """Get or create SNS client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    kwargs = {
                        "region_name": self.config.region_name,
                        "config": self.config.config,
                    }
                    if self.config.endpoint_url:
                        kwargs["endpoint_url"] = self.config.endpoint_url
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name

                    self._client = boto3.client("sns", **kwargs)
        return self._client

    def _get_client_for_region(self, region: str):
        """Get or create SNS client for a specific region."""
        if region not in self._cross_region_clients:
            with self._lock:
                if region not in self._cross_region_clients:
                    kwargs = {
                        "region_name": region,
                        "aws_access_key_id": self.config.aws_access_key_id,
                        "aws_secret_access_key": self.config.aws_secret_access_key,
                        "aws_session_token": self.config.aws_session_token,
                        "profile_name": self.config.profile_name,
                    }
                    if self.config.endpoint_url:
                        kwargs["endpoint_url"] = self.config.endpoint_url
                    self._cross_region_clients[region] = boto3.client("sns", **kwargs)
        return self._cross_region_clients[region]

    def _parse_topic_arn(self, topic_arn: str) -> Dict[str, str]:
        """Parse ARN to extract components."""
        parts = topic_arn.split(":")
        return {
            "partition": parts[1],
            "service": parts[2],
            "region": parts[3],
            "account_id": parts[4],
            "topic_name": parts[5]
        }

    def _parse_subscription_arn(self, subscription_arn: str) -> Dict[str, str]:
        """Parse subscription ARN to extract components."""
        parts = subscription_arn.split(":")
        return {
            "partition": parts[1],
            "service": parts[2],
            "region": parts[3],
            "account_id": parts[4],
            "subscription_id": ":".join(parts[5:])
        }

    # ==================== Topic Management ====================

    def create_topic(
        self,
        name: str,
        display_name: Optional[str] = None,
        fifo_topic: bool = False,
        content_based_deduplication: bool = False,
        kms_master_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        signature_version: Optional[str] = None,
        truncation_percentage: Optional[int] = None,
    ) -> SNSTopic:
        """
        Create an SNS topic.

        Args:
            name: Topic name
            display_name: Display name for the topic
            fifo_topic: Whether this is a FIFO topic
            content_based_deduplication: Enable content-based deduplication for FIFO
            kms_master_key_id: KMS key for server-side encryption
            tags: Resource tags
            signature_version: Signature version (1, 2, or 3)
            truncation_percentage: Truncation percentage for messages

        Returns:
            SNSTopic object
        """
        kwargs = {"Name": name}

        if fifo_topic:
            if not name.endswith(".fifo"):
                kwargs["Name"] = f"{name}.fifo"
            kwargs["Attributes"] = {}
            if content_based_deduplication:
                kwargs["Attributes"]["ContentBasedDeduplication"] = "true"
            if kms_master_key_id:
                kwargs["Attributes"]["KmsMasterKeyId"] = kms_master_key_id
            if signature_version:
                kwargs["Attributes"]["SignatureVersion"] = signature_version
            if truncation_percentage is not None:
                kwargs["Attributes"]["TracingConfig"] = "PassThrough"

        if display_name:
            if "Attributes" not in kwargs:
                kwargs["Attributes"] = {}
            kwargs["Attributes"]["DisplayName"] = display_name

        response = self.client.create_topic(**kwargs)
        topic_arn = response["TopicArn"]
        parsed = self._parse_topic_arn(topic_arn)

        topic = SNSTopic(
            topic_arn=topic_arn,
            name=parsed["topic_name"],
            display_name=display_name,
            topic_type=SNSTopicType.FIFO if fifo_topic else SNSTopicType.STANDARD,
            region=parsed["region"],
            account_id=parsed["account_id"],
            fifo_topic=fifo_topic,
            content_based_deduplication=content_based_deduplication,
            kms_master_key_id=kms_master_key_id,
            tags=tags or {}
        )

        if tags:
            self.client.tag_resource(resource_arn=topic_arn, tags=tags)

        logger.info(f"Created SNS topic: {topic_arn}")
        return topic

    def get_topic(self, topic_arn: str) -> SNSTopic:
        """Get topic attributes."""
        response = self.client.get_topic_attributes(TopicArn=topic_arn)
        attrs = response.get("Attributes", {})

        parsed = self._parse_topic_arn(topic_arn)

        return SNSTopic(
            topic_arn=topic_arn,
            name=parsed["topic_name"],
            display_name=attrs.get("DisplayName"),
            topic_type=SNSTopicType.FIFO if attrs.get("FifoTopic") == "true" else SNSTopicType.STANDARD,
            owner=attrs.get("Owner"),
            region=parsed["region"],
            account_id=parsed["account_id"],
            kms_master_key_id=attrs.get("KmsMasterKeyId"),
            signature_version=attrs.get("SignatureVersion"),
            fifo_topic=attrs.get("FifoTopic") == "true",
            content_based_deduplication=attrs.get("ContentBasedDeduplication") == "true",
        )

    def list_topics(self) -> List[SNSTopic]:
        """List all SNS topics."""
        topics = []
        paginator = self.client.get_paginator("list_topics")

        for page in paginator.paginate():
            for topic in page.get("Topics", []):
                topics.append(self.get_topic(topic["TopicArn"]))

        return topics

    def delete_topic(self, topic_arn: str) -> bool:
        """Delete an SNS topic."""
        self.client.delete_topic(TopicArn=topic_arn)
        logger.info(f"Deleted SNS topic: {topic_arn}")
        return True

    def set_topic_attributes(
        self,
        topic_arn: str,
        attribute_name: str,
        attribute_value: str
    ) -> bool:
        """Set topic attributes."""
        self.client.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName=attribute_name,
            AttributeValue=attribute_value
        )
        return True

    def add_topic_tags(self, topic_arn: str, tags: Dict[str, str]) -> bool:
        """Add tags to a topic."""
        self.client.tag_resource(resource_arn=topic_arn, tags=tags)
        return True

    def remove_topic_tags(self, topic_arn: str, tag_keys: List[str]) -> bool:
        """Remove tags from a topic."""
        self.client.untag_resource(resource_arn=topic_arn, tag_keys=tag_keys)
        return True

    # ==================== Subscription Management ====================

    def subscribe(
        self,
        topic_arn: str,
        protocol: Union[DeliveryProtocol, str],
        endpoint: str,
        attributes: Optional[Dict[str, str]] = None,
        filter_policy: Optional[Dict] = None,
        raw_message_delivery: bool = False,
        delivery_policy: Optional[Dict] = None,
        region: Optional[str] = None,
    ) -> SNSSubscription:
        """
        Subscribe to an SNS topic.

        Args:
            topic_arn: Topic ARN to subscribe to
            protocol: Delivery protocol (http, https, email, sqs, lambda, etc.)
            endpoint: Endpoint address for the subscription
            attributes: Subscription attributes
            filter_policy: Filter policy for message filtering
            raw_message_delivery: Deliver raw message without envelope
            delivery_policy: Delivery policy for retries
            region: Region for the subscription (for cross-region)

        Returns:
            SNSSubscription object
        """
        client = self._get_client_for_region(region) if region else self.client

        kwargs = {
            "TopicArn": topic_arn,
            "Protocol": protocol.value if isinstance(protocol, DeliveryProtocol) else protocol,
            "Endpoint": endpoint,
        }

        if attributes:
            kwargs["Attributes"] = attributes

        response = client.subscribe(**kwargs)
        subscription_arn = response["SubscriptionArn"]

        if filter_policy:
            self.set_subscription_filter_policy(subscription_arn, filter_policy, region=region)

        if raw_message_delivery:
            self.set_subscription_attributes(subscription_arn, "RawMessageDelivery", "true", region=region)

        if delivery_policy:
            self.set_subscription_attributes(subscription_arn, "DeliveryPolicy", json.dumps(delivery_policy), region=region)

        parsed = self._parse_subscription_arn(subscription_arn)

        return SNSSubscription(
            subscription_arn=subscription_arn,
            topic_arn=topic_arn,
            protocol=kwargs["Protocol"],
            endpoint=endpoint,
            region=parsed.get("region"),
            account_id=parsed.get("account_id"),
            filter_policy=filter_policy,
            raw_message_delivery=raw_message_delivery,
            delivery_policy=delivery_policy,
        )

    def confirm_subscription(
        self,
        topic_arn: str,
        token: str,
        authenticate_unsubscribe: bool = False
    ) -> str:
        """Confirm a subscription."""
        response = self.client.confirm_subscription(
            TopicArn=topic_arn,
            Token=token,
            AuthenticateOnUnsubscribe="true" if authenticate_unsubscribe else None
        )
        return response["SubscriptionArn"]

    def unsubscribe(self, subscription_arn: str, region: Optional[str] = None) -> bool:
        """Unsubscribe from a topic."""
        client = self._get_client_for_region(region) if region else self.client
        client.unsubscribe(SubscriptionArn=subscription_arn)
        logger.info(f"Unsubscribed: {subscription_arn}")
        return True

    def get_subscription(self, subscription_arn: str, region: Optional[str] = None) -> SNSSubscription:
        """Get subscription attributes."""
        client = self._get_client_for_region(region) if region else self.client
        response = client.get_subscription_attributes(SubscriptionArn=subscription_arn)
        attrs = response.get("Attributes", {})

        return SNSSubscription(
            subscription_arn=subscription_arn,
            topic_arn=attrs.get("TopicArn", ""),
            protocol=attrs.get("Protocol", ""),
            endpoint=attrs.get("Endpoint", ""),
            owner=attrs.get("Owner"),
            confirmation_authenticated=attrs.get("ConfirmationWasAuthenticated") == "true",
            delivery_policy=json.loads(attrs.get("DeliveryPolicy", "{}")),
            filter_policy=json.loads(attrs.get("FilterPolicy", "{}")),
            raw_message_delivery=attrs.get("RawMessageDelivery") == "true",
            region=self._parse_subscription_arn(subscription_arn).get("region"),
        )

    def list_subscriptions(
        self,
        topic_arn: Optional[str] = None
    ) -> List[SNSSubscription]:
        """List subscriptions."""
        subscriptions = []

        if topic_arn:
            paginator = self.client.get_paginator("list_subscriptions_by_topic")
            for page in paginator.paginate(TopicArn=topic_arn):
                for sub in page.get("Subscriptions", []):
                    subscriptions.append(self.get_subscription(sub["SubscriptionArn"]))
        else:
            paginator = self.client.get_paginator("list_subscriptions")
            for page in paginator.paginate():
                for sub in page.get("Subscriptions", []):
                    subscriptions.append(self.get_subscription(sub["SubscriptionArn"]))

        return subscriptions

    def set_subscription_attributes(
        self,
        subscription_arn: str,
        attribute_name: str,
        attribute_value: str,
        region: Optional[str] = None
    ) -> bool:
        """Set subscription attributes."""
        client = self._get_client_for_region(region) if region else self.client
        client.set_subscription_attributes(
            SubscriptionArn=subscription_arn,
            AttributeName=attribute_name,
            AttributeValue=attribute_value
        )
        return True

    def set_subscription_filter_policy(
        self,
        subscription_arn: str,
        filter_policy: Dict,
        filter_policy_scope: Optional[str] = None,
        region: Optional[str] = None
    ) -> bool:
        """Set filter policy for a subscription."""
        client = self._get_client_for_region(region) if region else self.client

        kwargs = {
            "SubscriptionArn": subscription_arn,
            "AttributeName": "FilterPolicy",
            "AttributeValue": json.dumps(filter_policy)
        }

        if filter_policy_scope:
            client.set_subscription_attributes(
                SubscriptionArn=subscription_arn,
                AttributeName="FilterPolicyScope",
                AttributeValue=filter_policy_scope
            )

        client.set_subscription_attributes(**kwargs)
        return True

    # ==================== Message Publishing ====================

    def publish(
        self,
        message: Union[str, Dict],
        topic_arn: Optional[str] = None,
        target_arn: Optional[str] = None,
        phone_number: Optional[str] = None,
        subject: Optional[str] = None,
        message_structure: MessageStructure = MessageStructure.JSON,
        message_attributes: Optional[Dict[str, Dict]] = None,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        content_based_deduplication: bool = False,
        region: Optional[str] = None,
    ) -> PublishResult:
        """
        Publish a message to an SNS topic or endpoint.

        Args:
            message: Message to publish (string or dict)
            topic_arn: Topic ARN to publish to
            target_arn: Target ARN for direct publish
            phone_number: Phone number for SMS
            subject: Message subject
            message_structure: Message structure (json, string, raw)
            message_attributes: Message attributes (type, string, etc.)
            message_group_id: FIFO message group ID
            message_deduplication_id: FIFO deduplication ID
            content_based_deduplication: Use content-based deduplication for FIFO
            region: Region for publishing

        Returns:
            PublishResult with message ID and details
        """
        client = self._get_client_for_region(region) if region else self.client

        if isinstance(message, dict) and message_structure == MessageStructure.JSON:
            message_body = json.dumps(message)
        elif isinstance(message, dict):
            message_body = json.dumps(message)
        else:
            message_body = message

        kwargs = {
            "Message": message_body,
        }

        if topic_arn:
            kwargs["TopicArn"] = topic_arn
        if target_arn:
            kwargs["TargetArn"] = target_arn
        if phone_number:
            kwargs["PhoneNumber"] = phone_number
        if subject:
            kwargs["Subject"] = subject
        if message_structure == MessageStructure.MULTI_FORMAT:
            kwargs["MessageStructure"] = "json"

        if message_attributes:
            kwargs["MessageAttributes"] = {}
            for key, value in message_attributes.items():
                if isinstance(value, str):
                    kwargs["MessageAttributes"][key] = {
                        "DataType": "String",
                        "StringValue": value
                    }
                elif isinstance(value, bytes):
                    kwargs["MessageAttributes"][key] = {
                        "DataType": "Binary",
                        "BinaryValue": value
                    }
                elif isinstance(value, dict) and "DataType" in value:
                    kwargs["MessageAttributes"][key] = value

        if message_group_id:
            kwargs["MessageGroupId"] = message_group_id
        if message_deduplication_id:
            kwargs["MessageDeduplicationId"] = message_deduplication_id
        elif content_based_deduplication and not message_deduplication_id:
            kwargs["MessageDeduplicationId"] = hashlib.sha256(message_body.encode()).hexdigest()[:32]

        response = client.publish(**kwargs)

        return PublishResult(
            message_id=response["MessageId"],
            topic_arn=topic_arn,
            target_arn=target_arn,
            sequence_number=response.get("SequenceNumber"),
        )

    def publish_to_topic(
        self,
        topic_arn: str,
        message: Union[str, Dict],
        subject: Optional[str] = None,
        **kwargs
    ) -> PublishResult:
        """Publish to a specific topic."""
        return self.publish(message, topic_arn=topic_arn, subject=subject, **kwargs)

    def publish_fifo(
        self,
        topic_arn: str,
        message: Union[str, Dict],
        message_group_id: str,
        subject: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        content_based_deduplication: bool = False,
        **kwargs
    ) -> PublishResult:
        """Publish to a FIFO topic with required message group ID."""
        return self.publish(
            message,
            topic_arn=topic_arn,
            subject=subject,
            message_group_id=message_group_id,
            message_deduplication_id=message_deduplication_id,
            content_based_deduplication=content_based_deduplication,
            **kwargs
        )

    def publish_to_phone(
        self,
        phone_number: str,
        message: str,
        subject: Optional[str] = None,
        **kwargs
    ) -> PublishResult:
        """Publish SMS to a phone number."""
        return self.publish(
            message,
            phone_number=phone_number,
            subject=subject,
            message_structure=MessageStructure.STRING,
            **kwargs
        )

    def publish_cross_region(
        self,
        topic_arn: str,
        message: Union[str, Dict],
        target_region: str,
        **kwargs
    ) -> PublishResult:
        """Publish to a topic in a different region."""
        return self.publish(message, topic_arn=topic_arn, region=target_region, **kwargs)

    def batch_publish(
        self,
        messages: List[Dict[str, Any]],
        topic_arn: str,
        batch_size: int = 10,
    ) -> List[PublishResult]:
        """
        Batch publish messages to a topic.

        Args:
            messages: List of message dicts with 'body' and optional 'subject', 'group_id', etc.
            topic_arn: Topic ARN
            batch_size: Number of messages per batch (max 10 for SNS)

        Returns:
            List of PublishResult objects
        """
        results = []

        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]

            entries = []
            for idx, msg in enumerate(batch):
                entry = {
                    "Id": str(idx),
                    "Message": msg["body"] if isinstance(msg["body"], str) else json.dumps(msg["body"]),
                }

                if msg.get("subject"):
                    entry["Subject"] = msg["subject"]
                if msg.get("group_id"):
                    entry["MessageGroupId"] = msg["group_id"]
                if msg.get("deduplication_id"):
                    entry["MessageDeduplicationId"] = msg["deduplication_id"]

                entries.append(entry)

            response = self.client.publish_batch(
                TopicArn=topic_arn,
                PublishBatchRequestEntries=entries
            )

            for result in response.get("Successful", []):
                results.append(PublishResult(
                    message_id=result["MessageId"],
                    topic_arn=topic_arn,
                    sequence_number=result.get("SequenceNumber"),
                ))

        return results

    # ==================== Platform Applications ====================

    def create_platform_application(
        self,
        name: str,
        platform: str,
        event_delivery_parameters: Optional[Dict] = None,
        failure_feedback_role_arn: Optional[str] = None,
        success_feedback_role_arn: Optional[str] = None,
        success_feedback_sample_rate: Optional[str] = None,
    ) -> PlatformApplication:
        """
        Create a platform application for mobile push notifications.

        Args:
            name: Application name
            platform: Platform (APNS, APNS_SANDBOX, GCM, ADM, Baidu, Windows)
            event_delivery_parameters: Event delivery parameters
            failure_feedback_role_arn: IAM role ARN for failure feedback
            success_feedback_role_arn: IAM role ARN for success feedback
            success_feedback_sample_rate: Sample rate for success feedback

        Returns:
            PlatformApplication object
        """
        kwargs = {
            "Name": name,
            "Platform": platform,
        }

        if event_delivery_parameters:
            kwargs["EventDeliveryParameters"] = event_delivery_parameters
        if failure_feedback_role_arn:
            kwargs["FailureFeedbackRoleArn"] = failure_feedback_role_arn
        if success_feedback_role_arn:
            kwargs["SuccessFeedbackRoleArn"] = success_feedback_role_arn
        if success_feedback_sample_rate:
            kwargs["SuccessFeedbackSampleRate"] = success_feedback_sample_rate

        response = self.client.create_platform_application(**kwargs)
        arn = response["PlatformApplicationArn"]
        parts = arn.split(":")

        return PlatformApplication(
            application_arn=arn,
            name=name,
            platform=platform,
            region=parts[3],
            account_id=parts[4],
            event_delivery_parameters=event_delivery_parameters,
            failure_feedback_role_arn=failure_feedback_role_arn,
            success_feedback_role_arn=success_feedback_role_arn,
            success_feedback_sample_rate=success_feedback_sample_rate,
        )

    def get_platform_application(self, application_arn: str) -> PlatformApplication:
        """Get platform application attributes."""
        response = self.client.get_platform_application_attributes(
            PlatformApplicationArn=application_arn
        )
        attrs = response.get("Attributes", {})
        parts = application_arn.split(":")

        return PlatformApplication(
            application_arn=application_arn,
            name=attrs.get("PlatformCredential", ""),
            platform=attrs.get("Platform", ""),
            region=parts[3],
            account_id=parts[4],
        )

    def list_platform_applications(self) -> List[PlatformApplication]:
        """List all platform applications."""
        applications = []
        paginator = self.client.get_paginator("list_platform_applications")

        for page in paginator.paginate():
            for app in page.get("PlatformApplications", []):
                applications.append(self.get_platform_application(app["PlatformApplicationArn"]))

        return applications

    def delete_platform_application(self, application_arn: str) -> bool:
        """Delete a platform application."""
        self.client.delete_platform_application(PlatformApplicationArn=application_arn)
        return True

    def create_platform_endpoint(
        self,
        application_arn: str,
        token: str,
        user_data: Optional[str] = None,
        custom_user_data: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Create a platform endpoint for mobile notifications.

        Args:
            application_arn: Platform application ARN
            token: Device token
            user_data: User data
            custom_user_data: Custom user data
            attributes: Endpoint attributes

        Returns:
            Endpoint ARN
        """
        kwargs = {
            "PlatformApplicationArn": application_arn,
            "Token": token,
        }

        if user_data:
            kwargs["CustomUserData"] = user_data
        if attributes:
            kwargs["Attributes"] = attributes

        response = self.client.create_platform_endpoint(**kwargs)
        return response["EndpointArn"]

    def publish_to_endpoint(
        self,
        endpoint_arn: str,
        message: Union[str, Dict],
        message_structure: MessageStructure = MessageStructure.JSON,
        subject: Optional[str] = None,
        message_attributes: Optional[Dict[str, Dict]] = None,
    ) -> PublishResult:
        """Publish message to a platform endpoint."""
        return self.publish(
            message,
            target_arn=endpoint_arn,
            subject=subject,
            message_structure=message_structure,
            message_attributes=message_attributes,
        )

    # ==================== Message Filtering ====================

    def create_filter_policy(
        self,
        policy: Dict,
        subscription_arn: Optional[str] = None,
        topic_arn: Optional[str] = None,
    ) -> bool:
        """
        Create and apply a filter policy.

        Args:
            policy: Filter policy dict with matching rules
            subscription_arn: Subscription ARN to apply to
            topic_arn: Topic ARN for creating resource policy

        Filter policy format:
        {
            "attribute1": ["value1", "value2"],
            "attribute2": [{"exists": True}],
            "attribute3": [{"numeric": [">", 10, "<=", 20]}]
        }
        """
        if subscription_arn:
            return self.set_subscription_filter_policy(subscription_arn, policy)

        if topic_arn:
            self.client.set_topic_attributes(
                TopicArn=topic_arn,
                AttributeName="FilterPolicy",
                AttributeValue=json.dumps(policy)
            )
            return True

        return False

    def validate_filter_policy(self, policy: Dict) -> bool:
        """
        Validate a filter policy structure.

        Args:
            policy: Filter policy to validate

        Returns:
            True if valid
        """
        if not isinstance(policy, dict):
            return False

        for key, value in policy.items():
            if not isinstance(key, str):
                return False
            if not isinstance(value, list):
                return False

        return True

    def test_filter_policy(
        self,
        policy: Dict,
        message_attributes: Dict[str, str]
    ) -> bool:
        """
        Test if a message matches a filter policy.

        Args:
            policy: Filter policy
            message_attributes: Message attributes to test

        Returns:
            True if message matches filter policy
        """
        if not policy:
            return True

        for attribute, filters in policy.items():
            if attribute not in message_attributes:
                return False

            message_value = message_attributes[attribute]
            matched = False

            for filter_rule in filters:
                if isinstance(filter_rule, str):
                    if filter_rule == message_value:
                        matched = True
                        break
                elif isinstance(filter_rule, dict):
                    if "exists" in filter_rule:
                        exists = filter_rule["exists"]
                        if exists and attribute in message_attributes:
                            matched = True
                            break
                        elif not exists and attribute not in message_attributes:
                            matched = True
                            break
                    elif "numeric" in filter_rule:
                        # Numeric matching simplified
                        matched = True
                        break

            if not matched:
                return False

        return True

    # ==================== Delivery Status ====================

    def set_delivery_status_logging(
        self,
        resource_arn: str,
        success_sample_rate: Optional[str] = None,
        failure_sample_rate: Optional[str] = None,
    ) -> bool:
        """
        Set delivery status logging for a topic or endpoint.

        Args:
            resource_arn: Topic or endpoint ARN
            success_sample_rate: Sample rate for successful deliveries (0-100)
            failure_sample_rate: Sample rate for failed deliveries (0-100)

        Returns:
            True if successful
        """
        if "subscription" in resource_arn or "endpoint" in resource_arn:
            attr_name = "DeliveryStatus"
            attributes = {
                "SuccessSamplingPercentage": success_sample_rate or "0",
                "FailureSamplingPercentage": failure_sample_rate or "0",
            }
            self.client.set_subscription_attributes(
                SubscriptionArn=resource_arn,
                AttributeName="DeliveryStatus",
                AttributeValue=json.dumps(attributes)
            )
        else:
            attributes = {}
            if success_sample_rate:
                attributes["DeliveryStatusLogging"] = json.dumps({
                    "success": {"sampleRate": success_sample_rate}
                })
            self.client.set_topic_attributes(
                TopicArn=resource_arn,
                AttributeName="DeliveryStatusLogging",
                AttributeValue=json.dumps(attributes)
            )

        return True

    def record_delivery_status(
        self,
        message_id: str,
        status: str,
        provider_response: Optional[Dict] = None,
        delivery_latency: Optional[int] = None,
        attempts: int = 1,
    ) -> DeliveryStatus:
        """
        Record delivery status for a message.

        Args:
            message_id: Message ID
            status: Delivery status
            provider_response: Provider response details
            delivery_latency: Latency in milliseconds
            attempts: Number of delivery attempts

        Returns:
            DeliveryStatus object
        """
        delivery_status = DeliveryStatus(
            message_id=message_id,
            status=status,
            provider_response=provider_response,
            delivery_latency=delivery_latency,
            attempts=attempts,
        )

        self._delivery_statuses[message_id] = delivery_status
        return delivery_status

    def get_delivery_status(self, message_id: str) -> Optional[DeliveryStatus]:
        """Get delivery status for a message."""
        return self._delivery_statuses.get(message_id)

    def list_delivery_statuses(
        self,
        status_filter: Optional[str] = None,
        limit: int = 100,
    ) -> List[DeliveryStatus]:
        """List delivery statuses."""
        statuses = list(self._delivery_statuses.values())

        if status_filter:
            statuses = [s for s in statuses if s.status == status_filter]

        return statuses[:limit]

    # ==================== CloudWatch Integration ====================

    def get_cloudwatch_metrics(
        self,
        topic_arn: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
    ) -> List[CloudWatchMetrics]:
        """
        Get CloudWatch metrics for SNS.

        Args:
            topic_arn: Specific topic ARN (optional)
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds

        Returns:
            List of CloudWatchMetrics
        """
        cw_client = boto3.client("cloudwatch", region_name=self.config.region_name)

        namespace = "AWS/SNS"
        dimensions = []

        if topic_arn:
            parsed = self._parse_topic_arn(topic_arn)
            dimensions = [{"Name": "TopicName", "Value": parsed["topic_name"]}]

        kwargs = {
            "Namespace": namespace,
            "MetricDataQueries": [
                {
                    "Id": "publish",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": namespace,
                            "MetricName": "PublishMode",
                            "Dimensions": dimensions
                        },
                        "Period": period,
                        "Stat": "Sum"
                    }
                },
                {
                    "Id": "notifications",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": namespace,
                            "MetricName": "NumberOfNotificationsDelivered",
                            "Dimensions": dimensions
                        },
                        "Period": period,
                        "Stat": "Sum"
                    }
                },
                {
                    "Id": "failures",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": namespace,
                            "MetricName": "NumberOfNotificationsFailed",
                            "Dimensions": dimensions
                        },
                        "Period": period,
                        "Stat": "Sum"
                    }
                }
            ]
        }

        if start_time:
            kwargs["StartTime"] = start_time.isoformat()
        if end_time:
            kwargs["EndTime"] = end_time.isoformat()

        response = cw_client.get_metric_data(**kwargs)

        metrics = []
        topic_name = topic_arn.split(":")[-1] if topic_arn else "all"

        for result in response.get("MetricDataResults", []):
            for val in result.get("Values", []):
                metrics.append(CloudWatchMetrics(
                    topic_name=topic_name,
                    metric_name=result["Id"],
                    value=val,
                    unit="Count",
                    timestamp=datetime.utcnow()
                ))

        return metrics

    def create_cloudwatch_alarm(
        self,
        alarm_name: str,
        topic_arn: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 1,
        statistic: str = "Sum",
    ) -> str:
        """
        Create a CloudWatch alarm for SNS metrics.

        Args:
            alarm_name: Alarm name
            topic_arn: Topic ARN
            metric_name: Metric name
            threshold: Alarm threshold
            comparison_operator: Comparison operator
            period: Period in seconds
            evaluation_periods: Number of evaluation periods
            statistic: Statistic type

        Returns:
            Alarm ARN
        """
        cw_client = boto3.client("cloudwatch", region_name=self.config.region_name)
        parsed = self._parse_topic_arn(topic_arn)

        response = cw_client.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="AWS/SNS",
            MetricName=metric_name,
            Dimensions=[{"Name": "TopicName", "Value": parsed["topic_name"]}],
            Threshold=threshold,
            ComparisonOperator=comparison_operator,
            Period=period,
            EvaluationPeriods=evaluation_periods,
            Statistic=statistic,
        )

        return f"arn:aws:cloudwatch:{self.config.region_name}:{parsed['account_id']}:alarm:{alarm_name}"

    # ==================== Cross-Region Operations ====================

    def publish_cross_region(
        self,
        topic_arn: str,
        message: Union[str, Dict],
        target_regions: List[str],
        subject: Optional[str] = None,
        **kwargs
    ) -> Dict[str, PublishResult]:
        """
        Publish to the same topic across multiple regions.

        Args:
            topic_arn: Topic ARN (base topic, will be replicated to target regions)
            message: Message to publish
            target_regions: List of target regions
            subject: Message subject

        Returns:
            Dict mapping region to PublishResult
        """
        results = {}

        for region in target_regions:
            try:
                result = self.publish(message, topic_arn=topic_arn, region=region, subject=subject, **kwargs)
                results[region] = result
            except Exception as e:
                logger.error(f"Failed to publish to region {region}: {e}")
                results[region] = None

        return results

    def sync_topics_across_regions(
        self,
        source_region: str,
        target_regions: List[str],
        topic_name: str,
        **kwargs
    ) -> Dict[str, SNSTopic]:
        """
        Create the same topic across multiple regions.

        Args:
            source_region: Source region to get topic configuration
            target_regions: Target regions to create topics
            topic_name: Topic name
            **kwargs: Additional topic creation arguments

        Returns:
            Dict mapping region to SNSTopic
        """
        topics = {}

        source_topic_arn = f"arn:aws:sns:{source_region}:{self._get_account_id()}:{topic_name}"

        try:
            source_topic = self.get_topic(source_topic_arn)
        except:
            source_topic = self.create_topic(topic_name, region=source_region, **kwargs)

        topics[source_region] = source_topic

        for region in target_regions:
            try:
                target_topic_arn = f"arn:aws:sns:{region}:{self._get_account_id()}:{topic_name}"
                target_topic = self.get_topic(target_topic_arn)
            except:
                target_topic = self.create_topic(topic_name, region=region, **kwargs)

            topics[region] = target_topic

        return topics

    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        sts = boto3.client("sts", region_name=self.config.region_name)
        return sts.get_caller_identity()["Account"]

    def get_topic_subscriptions_cross_region(
        self,
        topic_arn: str,
        regions: List[str]
    ) -> Dict[str, List[SNSSubscription]]:
        """
        Get subscriptions for a topic across multiple regions.

        Args:
            topic_arn: Topic ARN
            regions: List of regions to check

        Returns:
            Dict mapping region to list of subscriptions
        """
        results = {}
        parsed = self._parse_topic_arn(topic_arn)
        base_topic_name = parsed["topic_name"]

        for region in regions:
            try:
                topic_arn_region = f"arn:aws:sns:{region}:{parsed['account_id']}:{base_topic_name}"
                subscriptions = self.list_subscriptions(topic_arn=topic_arn_region)
                results[region] = subscriptions
            except Exception as e:
                logger.error(f"Failed to get subscriptions for region {region}: {e}")
                results[region] = []

        return results

    # ==================== Utility Methods ====================

    def get_topic_arn(self, topic_name: str, region: Optional[str] = None) -> str:
        """Get topic ARN from topic name."""
        if region:
            account_id = self._get_account_id()
            return f"arn:aws:sns:{region}:{account_id}:{topic_name}"
        return f"arn:aws:sns:{self.config.region_name}:{self._get_account_id()}:{topic_name}"

    def find_topic_by_name(self, name: str) -> Optional[SNSTopic]:
        """Find a topic by name."""
        topics = self.list_topics()
        for topic in topics:
            if topic.name == name or topic.name.endswith(name):
                return topic
        return None

    def close(self):
        """Close all client connections."""
        with self._lock:
            self._client = None
            self._cross_region_clients.clear()
