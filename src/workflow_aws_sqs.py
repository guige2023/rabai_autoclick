"""
AWS SQS Message Queue Integration Module for Workflow System

Implements an SQSIntegration class with:
1. Queue management: Create/manage SQS queues
2. Message sending: Send messages to queues
3. Message receiving: Receive messages from queues
4. Message deletion: Delete messages after processing
5. Dead letter queue: DLQ support
6. FIFO queues: FIFO queue support
7. Long polling: Configure long polling
8. Visibility timeout: Manage visibility timeout
9. Redrive policy: Configure redrive policies
10. CloudWatch integration: Monitoring and metrics

Commit: 'feat(aws-sqs): add AWS SQS integration with queue management, message sending/receiving, DLQ, FIFO, long polling, visibility timeout, redrive policy, CloudWatch'
"""

import uuid
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union
from dataclasses import dataclass, field
from collections import defaultdict
from queue import Queue, Empty
from enum import Enum
import copy
import hashlib

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None


logger = logging.getLogger(__name__)


class QueueType(Enum):
    """SQS queue types."""
    STANDARD = "standard"
    FIFO = "fifo"


class MessageAttribute(Enum):
    """SQS message attribute types."""
    ALL = "All"
    SENT_TIMESTAMP = "SentTimestamp"
    APPROXIMATE RECEIVE_COUNT = "ApproximateReceiveCount"
    APPROXIMATE_FIRST_RECEIVE_TIMESTAMP = "ApproximateFirstReceiveTimestamp"


@dataclass
class QueueConfig:
    """Configuration for an SQS queue."""
    name: str
    queue_type: QueueType = QueueType.STANDARD
    visibility_timeout: int = 30
    message_retention_period: int = 345600
    maximum_message_size: int = 262144
    delay_seconds: int = 0
    receive_message_wait_time_seconds: int = 0
    redrive_policy: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    kms_master_key_id: Optional[str] = None
    kms_data_key_reuse_period_seconds: int = 300
    policy: Optional[str] = None
    content_based_deduplication: bool = False


@dataclass
class MessageConfig:
    """Configuration for sending a message."""
    queue_url: str
    message_body: Union[str, Dict, Any]
    delay_seconds: int = 0
    message_attributes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    message_system_attributes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    deduplication_id: Optional[str] = None
    group_id: Optional[str] = None
    reply_to: Optional[str] = None
    message_structure: str = "json"


@dataclass
class ReceiveConfig:
    """Configuration for receiving messages."""
    queue_url: str
    max_number_of_messages: int = 1
    visibility_timeout: Optional[int] = None
    wait_time_seconds: int = 0
    receive_request_attempt_id: Optional[str] = None
    message_attribute_names: List[str] = field(default_factory=list)
    attribute_names: List[str] = field(default_factory=list)
    generic_attribute_names: List[str] = field(default_factory=list)


@dataclass
class DeadLetterQueueConfig:
    """Configuration for a dead letter queue."""
    name: str
    max_receive_count: int = 10
    visibility_timeout: int = 30
    message_retention_period: int = 1209600


class SQSIntegration:
    """
    AWS SQS integration class for message queue operations.
    
    Supports:
    - Standard and FIFO queues
    - Message publishing and consuming
    - Dead letter queues with redrive policies
    - Long polling for efficient message retrieval
    - Visibility timeout management
    - CloudWatch metrics integration
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        sqs_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None
    ):
        """
        Initialize SQS integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: SQS endpoint URL (for testing with LocalStack, etc.)
            sqs_client: Pre-configured SQS client (overrides boto3 creation)
            cloudwatch_client: Pre-configured CloudWatch client for metrics
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SQS integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._sqs_client = sqs_client
        self._cloudwatch_client = cloudwatch_client
        self._cloudwatch_namespace = "SQS/Integration"
        self._queue_cache: Dict[str, str] = {}
        self._lock = threading.RLock()
        
        session_kwargs = {
            "region_name": region_name
        }
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self._session = boto3.Session(**session_kwargs)
        
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
    
    @property
    def sqs_client(self):
        """Get or create SQS client."""
        if self._sqs_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._sqs_client = self._session.client("sqs", **kwargs)
        return self._sqs_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._cloudwatch_client = self._session.client("cloudwatch", **kwargs)
        return self._cloudwatch_client
    
    def _generate_deduplication_id(self, message_body: Union[str, Dict, Any]) -> str:
        """Generate deduplication ID for FIFO queues based on message body."""
        if isinstance(message_body, dict):
            body_str = json.dumps(message_body, sort_keys=True)
        else:
            body_str = str(message_body)
        return hashlib.sha256(body_str.encode()).hexdigest()[:64]
    
    def _record_metric(self, metric_name: str, value: float, unit: str = "Count", dimensions: Dict[str, str] = None):
        """Record a metric for CloudWatch."""
        metric = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow().isoformat()
        }
        if dimensions:
            metric["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        
        with self._metrics_lock:
            self._metrics_buffer.append(metric)
    
    def flush_metrics(self):
        """Flush buffered metrics to CloudWatch."""
        with self._metrics_lock:
            if not self._metrics_buffer:
                return
            
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self._cloudwatch_namespace,
                    MetricData=self._metrics_buffer
                )
                logger.info(f"Flushed {len(self._metrics_buffer)} metrics to CloudWatch")
                self._metrics_buffer.clear()
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to flush metrics to CloudWatch: {e}")
    
    def create_queue(
        self,
        config: QueueConfig,
        create_dlq: bool = False,
        dlq_config: Optional[DeadLetterQueueConfig] = None
    ) -> str:
        """
        Create an SQS queue.
        
        Args:
            config: Queue configuration
            create_dlq: Whether to create a dead letter queue
            dlq_config: Configuration for the dead letter queue
            
        Returns:
            Queue URL
        """
        with self._lock:
            queue_name = config.name
            
            if config.queue_type == QueueType.FIFO:
                if not queue_name.endswith(".fifo"):
                    queue_name = f"{queue_name}.fifo"
            
            if queue_name in self._queue_cache:
                return self._queue_cache[queue_name]
            
            attributes = {
                "VisibilityTimeout": str(config.visibility_timeout),
                "MessageRetentionPeriod": str(config.message_retention_period),
                "MaximumMessageSize": str(config.maximum_message_size),
                "DelaySeconds": str(config.delay_seconds),
                "ReceiveMessageWaitTimeSeconds": str(config.receive_message_wait_time_seconds)
            }
            
            if config.queue_type == QueueType.FIFO:
                attributes["FifoQueue"] = "true"
                if config.content_based_deduplication:
                    attributes["ContentBasedDeduplication"] = "true"
            
            if config.kms_master_key_id:
                attributes["KmsMasterKeyId"] = config.kms_master_key_id
                attributes["KmsDataKeyReusePeriodSeconds"] = str(config.kms_data_key_reuse_period_seconds)
            
            if config.policy:
                attributes["Policy"] = config.policy
            
            if create_dlq and dlq_config:
                dlq_name = dlq_config.name
                if config.queue_type == QueueType.FIFO and not dlq_name.endswith(".fifo"):
                    dlq_name = f"{dlq_name}.fifo"
                
                dlq_url = self.create_queue(QueueConfig(
                    name=dlq_name,
                    queue_type=config.queue_type,
                    visibility_timeout=dlq_config.visibility_timeout,
                    message_retention_period=dlq_config.message_retention_period
                ))
                
                dlq_arn = self.get_queue_arn(dlq_url)
                attributes["RedrivePolicy"] = json.dumps({
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": str(dlq_config.max_receive_count)
                })
            elif config.redrive_policy:
                attributes["RedrivePolicy"] = json.dumps(config.redrive_policy)
            
            try:
                create_params = {
                    "QueueName": queue_name,
                    "Attributes": attributes
                }
                
                if config.tags:
                    create_params["tags"] = config.tags
                
                response = self.sqs_client.create_queue(**create_params)
                queue_url = response["QueueUrl"]
                self._queue_cache[queue_name] = queue_url
                self._record_metric("QueuesCreated", 1, "Count", {"QueueType": config.queue_type.value})
                logger.info(f"Created queue: {queue_name} -> {queue_url}")
                return queue_url
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to create queue {queue_name}: {e}")
                raise
    
    def get_queue_url(self, queue_name: str) -> Optional[str]:
        """
        Get queue URL by name.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            Queue URL or None if not found
        """
        with self._lock:
            if queue_name in self._queue_cache:
                return self._queue_cache[queue_name]
            
            try:
                response = self.sqs_client.get_queue_url(QueueName=queue_name)
                queue_url = response["QueueUrl"]
                self._queue_cache[queue_name] = queue_url
                return queue_url
            except ClientError as e:
                if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                    return None
                raise
    
    def get_queue_arn(self, queue_url: str) -> str:
        """
        Get queue ARN.
        
        Args:
            queue_url: Queue URL
            
        Returns:
            Queue ARN
        """
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"]
        )
        return response["Attributes"]["QueueArn"]
    
    def delete_queue(self, queue_url: str) -> bool:
        """
        Delete a queue.
        
        Args:
            queue_url: Queue URL
            
        Returns:
            True if deleted successfully
        """
        try:
            self.sqs_client.delete_queue(QueueUrl=queue_url)
            with self._lock:
                for name, url in list(self._queue_cache.items()):
                    if url == queue_url:
                        del self._queue_cache[name]
            self._record_metric("QueuesDeleted", 1, "Count")
            logger.info(f"Deleted queue: {queue_url}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete queue {queue_url}: {e}")
            return False
    
    def list_queues(self, prefix: Optional[str] = None) -> List[str]:
        """
        List queues.
        
        Args:
            prefix: Filter queues by name prefix
            
        Returns:
            List of queue URLs
        """
        try:
            kwargs = {}
            if prefix:
                kwargs["QueueNamePrefix"] = prefix
            
            response = self.sqs_client.list_queues(**kwargs)
            return response.get("QueueUrls", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list queues: {e}")
            return []
    
    def set_queue_attributes(self, queue_url: str, attributes: Dict[str, str]) -> bool:
        """
        Set queue attributes.
        
        Args:
            queue_url: Queue URL
            attributes: Dictionary of attribute names and values
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes=attributes
            )
            logger.info(f"Set attributes for queue: {queue_url}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set queue attributes: {e}")
            return False
    
    def get_queue_attributes(self, queue_url: str, attribute_names: List[str]) -> Dict[str, str]:
        """
        Get queue attributes.
        
        Args:
            queue_url: Queue URL
            attribute_names: List of attribute names to retrieve
            
        Returns:
            Dictionary of attributes
        """
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=attribute_names
            )
            return response.get("Attributes", {})
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get queue attributes: {e}")
            return {}
    
    def send_message(self, config: MessageConfig) -> Dict[str, Any]:
        """
        Send a message to a queue.
        
        Args:
            config: Message configuration
            
        Returns:
            Response containing MessageId, MD5Checksum, etc.
        """
        try:
            message_body = config.message_body
            if isinstance(message_body, dict):
                message_body = json.dumps(message_body)
            
            kwargs = {
                "QueueUrl": config.queue_url,
                "MessageBody": message_body,
                "DelaySeconds": config.delay_seconds
            }
            
            if config.message_attributes:
                kwargs["MessageAttributes"] = config.message_attributes
            
            if config.message_system_attributes:
                kwargs["MessageSystemAttributes"] = config.message_system_attributes
            
            is_fifo = config.queue_url.endswith(".fifo")
            
            if is_fifo:
                if config.deduplication_id:
                    kwargs["MessageDeduplicationId"] = config.deduplication_id
                elif not config.deduplication_id:
                    kwargs["MessageDeduplicationId"] = self._generate_deduplication_id(config.message_body)
                
                if config.group_id:
                    kwargs["MessageGroupId"] = config.group_id
            
            response = self.sqs_client.send_message(**kwargs)
            
            self._record_metric("MessagesSent", 1, "Count", {"QueueUrl": config.queue_url})
            
            logger.debug(f"Sent message to {config.queue_url}: {response.get('MessageId')}")
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send message to {config.queue_url}: {e}")
            self._record_metric("MessageSendErrors", 1, "Count", {"QueueUrl": config.queue_url})
            raise
    
    def send_message_batch(
        self,
        queue_url: str,
        messages: List[MessageConfig],
        batch_size: int = 10
    ) -> Dict[str, Any]:
        """
        Send multiple messages in a batch.
        
        Args:
            queue_url: Queue URL
            messages: List of message configurations
            batch_size: Number of messages per batch (max 10)
            
        Returns:
            Response with successful and failed messages
        """
        results = {"Successful": [], "Failed": []}
        is_fifo = queue_url.endswith(".fifo")
        
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            entries = []
            
            for idx, msg_config in enumerate(batch):
                message_body = msg_config.message_body
                if isinstance(message_body, dict):
                    message_body = json.dumps(message_body)
                
                entry = {
                    "Id": str(idx),
                    "MessageBody": message_body,
                    "DelaySeconds": msg_config.delay_seconds
                }
                
                if msg_config.message_attributes:
                    entry["MessageAttributes"] = msg_config.message_attributes
                
                if is_fifo:
                    dedup_id = msg_config.deduplication_id or self._generate_deduplication_id(msg_config.message_body)
                    entry["MessageDeduplicationId"] = dedup_id
                    if msg_config.group_id:
                        entry["MessageGroupId"] = msg_config.group_id
                
                entries.append(entry)
            
            try:
                response = self.sqs_client.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries
                )
                
                if response.get("Successful"):
                    results["Successful"].extend(response["Successful"])
                    self._record_metric("MessagesSent", len(response["Successful"]), "Count", {"QueueUrl": queue_url})
                
                if response.get("Failed"):
                    results["Failed"].extend(response["Failed"])
                    self._record_metric("MessageBatchErrors", len(response["Failed"]), "Count", {"QueueUrl": queue_url})
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to send message batch: {e}")
                results["Failed"].extend([{"Id": str(i), "Error": str(e)} for i in range(len(batch))])
        
        return results
    
    def receive_messages(self, config: ReceiveConfig) -> List[Dict[str, Any]]:
        """
        Receive messages from a queue.
        
        Args:
            config: Receive configuration
            
        Returns:
            List of messages with body, attributes, receipt handle, etc.
        """
        try:
            kwargs = {
                "QueueUrl": config.queue_url,
                "MaxNumberOfMessages": min(config.max_number_of_messages, 10),
                "WaitTimeSeconds": config.wait_time_seconds
            }
            
            if config.visibility_timeout is not None:
                kwargs["VisibilityTimeout"] = config.visibility_timeout
            
            if config.message_attribute_names:
                kwargs["MessageAttributeNames"] = config.message_attribute_names
            
            if config.attribute_names:
                kwargs["AttributeNames"] = config.attribute_names
            
            if config.generic_attribute_names:
                kwargs["MessageSystemAttributeNames"] = config.generic_attribute_names
            
            if config.receive_request_attempt_id:
                kwargs["ReceiveRequestAttemptId"] = config.receive_request_attempt_id
            
            response = self.sqs_client.receive_message(**kwargs)
            messages = response.get("Messages", [])
            
            self._record_metric("MessagesReceived", len(messages), "Count", {"QueueUrl": config.queue_url})
            
            result = []
            for msg in messages:
                result.append({
                    "MessageId": msg.get("MessageId"),
                    "Body": msg.get("Body"),
                    "BodyParsed": self._parse_message_body(msg.get("Body")),
                    "ReceiptHandle": msg.get("ReceiptHandle"),
                    "MD5OfBody": msg.get("MD5OfBody"),
                    "MD5OfMessageAttributes": msg.get("MD5OfMessageAttributes"),
                    "Attributes": msg.get("Attributes", {}),
                    "MessageAttributes": msg.get("MessageAttributes", {}),
                    "MessageSystemAttributes": msg.get("MessageSystemAttributes", {})
                })
            
            return result
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to receive messages from {config.queue_url}: {e}")
            self._record_metric("MessageReceiveErrors", 1, "Count", {"QueueUrl": config.queue_url})
            raise
    
    def _parse_message_body(self, body: str) -> Any:
        """Parse message body, attempting JSON deserialization."""
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body
    
    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Delete a message from a queue.
        
        Args:
            queue_url: Queue URL
            receipt_handle: Receipt handle from received message
            
        Returns:
            True if deleted successfully
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            self._record_metric("MessagesDeleted", 1, "Count", {"QueueUrl": queue_url})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete message from {queue_url}: {e}")
            return False
    
    def delete_message_batch(
        self,
        queue_url: str,
        receipt_handles: List[str]
    ) -> Dict[str, Any]:
        """
        Delete multiple messages in a batch.
        
        Args:
            queue_url: Queue URL
            receipt_handles: List of receipt handles
            
        Returns:
            Response with successful and failed deletions
        """
        results = {"Successful": [], "Failed": []}
        
        for i in range(0, len(receipt_handles), 10):
            batch = receipt_handles[i:i + 10]
            entries = [
                {"Id": str(idx), "ReceiptHandle": handle}
                for idx, handle in enumerate(batch)
            ]
            
            try:
                response = self.sqs_client.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries
                )
                
                if response.get("Successful"):
                    results["Successful"].extend(response["Successful"])
                    self._record_metric("MessagesDeleted", len(response["Successful"]), "Count", {"QueueUrl": queue_url})
                
                if response.get("Failed"):
                    results["Failed"].extend(response["Failed"])
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to delete message batch: {e}")
                results["Failed"].extend([{"Id": str(i), "Error": str(e)} for i in range(len(batch))])
        
        return results
    
    def purge_queue(self, queue_url: str) -> bool:
        """
        Purge all messages from a queue.
        
        Args:
            queue_url: Queue URL
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.purge_queue(QueueUrl=queue_url)
            self._record_metric("QueuesPurged", 1, "Count", {"QueueUrl": queue_url})
            logger.info(f"Purged queue: {queue_url}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to purge queue {queue_url}: {e}")
            return False
    
    def change_message_visibility(
        self,
        queue_url: str,
        receipt_handle: str,
        visibility_timeout: int
    ) -> bool:
        """
        Change message visibility timeout.
        
        Args:
            queue_url: Queue URL
            receipt_handle: Receipt handle
            visibility_timeout: New visibility timeout in seconds (0-43200)
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout
            )
            self._record_metric("VisibilityChanged", 1, "Count", {"QueueUrl": queue_url})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to change message visibility: {e}")
            return False
    
    def change_message_visibility_batch(
        self,
        queue_url: str,
        entries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Change visibility timeout for multiple messages.
        
        Args:
            queue_url: Queue URL
            entries: List of dicts with Id, ReceiptHandle, VisibilityTimeout
            
        Returns:
            Response with successful and failed entries
        """
        try:
            response = self.sqs_client.change_message_visibility_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to change message visibility batch: {e}")
            raise
    
    def add_permission(self, queue_url: str, label: str, aws_account_ids: List[str], actions: List[str]) -> bool:
        """
        Add permissions to a queue.
        
        Args:
            queue_url: Queue URL
            label: Unique label for this permission
            aws_account_ids: List of AWS account IDs
            actions: List of actions (e.g., "sqs:SendMessage", "sqs:ReceiveMessage")
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.add_permission(
                QueueUrl=queue_url,
                Label=label,
                AWSAccountIds=aws_account_ids,
                Actions=actions
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add permission: {e}")
            return False
    
    def remove_permission(self, queue_url: str, label: str) -> bool:
        """
        Remove permissions from a queue.
        
        Args:
            queue_url: Queue URL
            label: Label of the permission to remove
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.remove_permission(
                QueueUrl=queue_url,
                Label=label
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to remove permission: {e}")
            return False
    
    def tag_queue(self, queue_url: str, tags: Dict[str, str]) -> bool:
        """
        Add tags to a queue.
        
        Args:
            queue_url: Queue URL
            tags: Dictionary of tag key-value pairs
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.tag_queue(
                QueueUrl=queue_url,
                Tags=tags
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to tag queue: {e}")
            return False
    
    def untag_queue(self, queue_url: str, tag_keys: List[str]) -> bool:
        """
        Remove tags from a queue.
        
        Args:
            queue_url: Queue URL
            tag_keys: List of tag keys to remove
            
        Returns:
            True if successful
        """
        try:
            self.sqs_client.untag_queue(
                QueueUrl=queue_url,
                TagKeys=tag_keys
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to untag queue: {e}")
            return False
    
    def list_queue_tags(self, queue_url: str) -> Dict[str, str]:
        """
        List tags for a queue.
        
        Args:
            queue_url: Queue URL
            
        Returns:
            Dictionary of tag key-value pairs
        """
        try:
            response = self.sqs_client.list_queue_tags(QueueUrl=queue_url)
            return response.get("Tags", {})
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list queue tags: {e}")
            return {}
    
    def get_dead_letter_source_queues(self, queue_url: str) -> List[str]:
        """
        Get queues that have the specified queue as their dead letter queue.
        
        Args:
            queue_url: Dead letter queue URL
            
        Returns:
            List of source queue URLs
        """
        try:
            response = self.sqs_client.list_dead_letter_source_queues(
                QueueUrl=queue_url
            )
            return response.get("queueUrls", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list dead letter source queues: {e}")
            return []
    
    def configure_redrive_policy(
        self,
        queue_url: str,
        dead_letter_queue_url: str,
        max_receive_count: int = 10
    ) -> bool:
        """
        Configure redrive policy for a queue.
        
        Args:
            queue_url: Source queue URL
            dead_letter_queue_url: Dead letter queue URL
            max_receive_count: Maximum receive count before moving to DLQ
            
        Returns:
            True if successful
        """
        try:
            dlq_arn = self.get_queue_arn(dead_letter_queue_url)
            redrive_policy = {
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": str(max_receive_count)
            }
            
            return self.set_queue_attributes(queue_url, {
                "RedrivePolicy": json.dumps(redrive_policy)
            })
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to configure redrive policy: {e}")
            return False
    
    def set_long_polling(self, queue_url: str, wait_time_seconds: int = 20) -> bool:
        """
        Configure long polling for a queue.
        
        Args:
            queue_url: Queue URL
            wait_time_seconds: Wait time for long polling (0-20, or 0 to disable)
            
        Returns:
            True if successful
        """
        wait_time = max(0, min(wait_time_seconds, 20))
        return self.set_queue_attributes(queue_url, {
            "ReceiveMessageWaitTimeSeconds": str(wait_time)
        })
    
    def set_visibility_timeout(self, queue_url: str, visibility_timeout: int) -> bool:
        """
        Set default visibility timeout for a queue.
        
        Args:
            queue_url: Queue URL
            visibility_timeout: Visibility timeout in seconds (0-43200)
            
        Returns:
            True if successful
        """
        timeout = max(0, min(visibility_timeout, 43200))
        return self.set_queue_attributes(queue_url, {
            "VisibilityTimeout": str(timeout)
        })
    
    def get_queue_metrics(self, queue_url: str) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for a queue.
        
        Args:
            queue_url: Queue URL
            
        Returns:
            Dictionary of metric values
        """
        queue_name = queue_url.split("/")[-1]
        
        metrics = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesDelayed",
            "ApproximateNumberOfMessagesNotVisible",
            "CreatedTimestamp",
            "DelayFirstDelivery",
            "LastModifiedTimestamp",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "QueueArn",
            "ReceiveMessageWaitTimeSeconds",
            "VisibilityTimeout"
        ]
        
        try:
            attributes = self.get_queue_attributes(queue_url, metrics)
            
            result = {
                "ApproximateNumberOfMessages": int(attributes.get("ApproximateNumberOfMessages", 0)),
                "ApproximateNumberOfMessagesDelayed": int(attributes.get("ApproximateNumberOfMessagesDelayed", 0)),
                "ApproximateNumberOfMessagesNotVisible": int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
                "MaximumMessageSize": int(attributes.get("MaximumMessageSize", 262144)),
                "MessageRetentionPeriod": int(attributes.get("MessageRetentionPeriod", 345600)),
                "ReceiveMessageWaitTimeSeconds": int(attributes.get("ReceiveMessageWaitTimeSeconds", 0)),
                "VisibilityTimeout": int(attributes.get("VisibilityTimeout", 30))
            }
            
            self._record_metric("QueueSize", result["ApproximateNumberOfMessages"], "Count", {"QueueName": queue_name})
            
            return result
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get queue metrics: {e}")
            return {}
    
    def get_cloudwatch_metrics(
        self,
        queue_url: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get CloudWatch metrics for a queue over a time period.
        
        Args:
            queue_url: Queue URL
            start_time: Start of time range
            end_time: End of time range
            period: Metric period in seconds
            
        Returns:
            Dictionary of metric data series
        """
        queue_arn = self.get_queue_arn(queue_url)
        queue_name = queue_url.split("/")[-1]
        
        metric_names = [
            "NumberOfMessagesSent",
            "NumberOfMessagesReceived",
            "NumberOfMessagesDeleted",
            "ApproximateNumberOfMessagesVisible",
            "ApproximateAgeOfOldestMessage",
            "ApproximateNumberOfMessagesNotVisible",
            "SentMessageSize"
        ]
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/SQS",
                MetricName="NumberOfMessagesSent",
                Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Sum", "Average", "Maximum"]
            )
            
            result = {}
            for metric_name in metric_names:
                try:
                    metric_response = self.cloudwatch_client.get_metric_statistics(
                        Namespace="AWS/SQS",
                        MetricName=metric_name,
                        Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=period,
                        Statistics=["Sum", "Average", "Maximum", "Minimum"]
                    )
                    result[metric_name] = metric_response.get("Datapoints", [])
                except (ClientError, BotoCoreError):
                    pass
            
            return result
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            return {}
    
    def create_fifo_queue(
        self,
        name: str,
        visibility_timeout: int = 30,
        message_retention_period: int = 345600,
        content_based_deduplication: bool = False,
        kms_master_key_id: Optional[str] = None
    ) -> str:
        """
        Create a FIFO queue.
        
        Args:
            name: Queue name (will have .fifo appended if not present)
            visibility_timeout: Visibility timeout in seconds
            message_retention_period: Message retention period in seconds
            content_based_deduplication: Enable content-based deduplication
            kms_master_key_id: KMS key ID for server-side encryption
            
        Returns:
            Queue URL
        """
        config = QueueConfig(
            name=name,
            queue_type=QueueType.FIFO,
            visibility_timeout=visibility_timeout,
            message_retention_period=message_retention_period,
            content_based_deduplication=content_based_deduplication,
            kms_master_key_id=kms_master_key_id
        )
        return self.create_queue(config)
    
    def create_dlq_with_redrive(
        self,
        source_queue_name: str,
        dlq_name: str,
        max_receive_count: int = 10,
        visibility_timeout: int = 30
    ) -> tuple:
        """
        Create a source queue with dead letter queue and redrive policy.
        
        Args:
            source_queue_name: Name of the source queue
            dlq_name: Name of the dead letter queue
            max_receive_count: Max receives before moving to DLQ
            visibility_timeout: Visibility timeout for both queues
            
        Returns:
            Tuple of (source_queue_url, dlq_url)
        """
        dlq_config = DeadLetterQueueConfig(
            name=dlq_name,
            max_receive_count=max_receive_count,
            visibility_timeout=visibility_timeout
        )
        
        source_queue_url = self.create_queue(
            QueueConfig(name=source_queue_name),
            create_dlq=True,
            dlq_config=dlq_config
        )
        
        dlq_url = self.get_queue_url(
            dlq_name if dlq_name.endswith(".fifo") else dlq_name
        )
        
        return source_queue_url, dlq_url
    
    def process_messages(
        self,
        queue_url: str,
        callback: Callable[[Dict[str, Any]], bool],
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
        auto_delete: bool = True
    ) -> int:
        """
        Process messages from a queue with a callback.
        
        Args:
            queue_url: Queue URL
            callback: Function to process each message, returns True to delete
            max_messages: Maximum messages to retrieve per batch
            wait_time_seconds: Long polling wait time
            visibility_timeout: Override visibility timeout
            auto_delete: Automatically delete successfully processed messages
            
        Returns:
            Number of messages processed
        """
        config = ReceiveConfig(
            queue_url=queue_url,
            max_number_of_messages=max_messages,
            visibility_timeout=visibility_timeout,
            wait_time_seconds=wait_time_seconds
        )
        
        messages = self.receive_messages(config)
        processed = 0
        
        for msg in messages:
            try:
                success = callback(msg)
                if success and auto_delete:
                    self.delete_message(queue_url, msg["ReceiptHandle"])
                    processed += 1
                elif success:
                    self.change_message_visibility(queue_url, msg["ReceiptHandle"], 0)
                    processed += 1
            except Exception as e:
                logger.error(f"Error processing message {msg.get('MessageId')}: {e}")
        
        return processed
    
    def close(self):
        """Close and cleanup resources."""
        self.flush_metrics()
        self._sqs_client = None
        self._cloudwatch_client = None
        with self._lock:
            self._queue_cache.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
