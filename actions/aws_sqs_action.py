"""AWS SQS integration for message queue operations.

Handles SQS operations including sending, receiving,
deleting messages, and queue management.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
import json
import hashlib

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError:
    boto3 = None
    ClientError = None
    BotoCoreError = Exception

logger = logging.getLogger(__name__)


@dataclass
class SQSConfig:
    """Configuration for AWS SQS."""
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region_name: str = "us-east-1"
    endpoint_url: Optional[str] = None


@dataclass
class SQSMessage:
    """Represents an SQS message."""
    message_id: str
    receipt_handle: str
    body: str
    attributes: dict = field(default_factory=dict)
    md5_of_body: str = ""
    message_attributes: dict = field(default_factory=dict)


@dataclass
class SQSQueue:
    """Represents an SQS queue."""
    url: str
    name: str
    arn: str
    created_timestamp: Optional[datetime] = None
    approximate_message_count: int = 0


class SQSAPIError(Exception):
    """Raised when SQS operations fail."""
    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class SQSAction:
    """AWS SQS client for message queue operations."""

    def __init__(self, config: SQSConfig):
        """Initialize SQS client with configuration.

        Args:
            config: SQSConfig with AWS credentials and settings

        Raises:
            ImportError: If boto3 is not installed
        """
        if boto3 is None:
            raise ImportError("boto3 required: pip install boto3")

        self.config = config
        self._client = None

    def _get_client(self):
        """Get or create SQS client."""
        if self._client is None:
            kwargs: dict[str, Any] = {
                "region_name": self.config.region_name
            }

            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url

            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            self._client = boto3.client("sqs", **kwargs)

        return self._client

    def create_queue(self, queue_name: str,
                    visibility_timeout: int = 30,
                    message_retention_period: int = 345600,
                    delay_seconds: int = 0,
                    max_message_size: int = 262144,
                    receive_message_wait_time_seconds: int = 0) -> Optional[str]:
        """Create an SQS queue.

        Args:
            queue_name: Name of the queue
            visibility_timeout: Seconds before message becomes visible
            message_retention_period: Seconds to retain messages
            delay_seconds: Seconds to delay message delivery
            max_message_size: Maximum message size in bytes
            receive_message_wait_time_seconds: Long polling wait time

        Returns:
            Queue URL or None if failed
        """
        try:
            client = self._get_client()

            attributes = {
                "VisibilityTimeout": str(visibility_timeout),
                "MessageRetentionPeriod": str(message_retention_period),
                "DelaySeconds": str(delay_seconds),
                "MaximumMessageSize": str(max_message_size),
                "ReceiveMessageWaitTimeSeconds": str(receive_message_wait_time_seconds)
            }

            response = client.create_queue(
                QueueName=queue_name,
                Attributes=attributes
            )

            return response["QueueUrl"]

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Create queue failed: {e}")
            return None

    def get_queue_url(self, queue_name: str) -> Optional[str]:
        """Get the URL of an SQS queue.

        Args:
            queue_name: Name of the queue

        Returns:
            Queue URL or None if not found
        """
        try:
            client = self._get_client()
            response = client.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Get queue URL failed: {e}")
            return None

    def send_message(self, queue_url: str, message_body: str,
                    delay_seconds: int = 0,
                    message_attributes: Optional[dict] = None) -> Optional[str]:
        """Send a message to a queue.

        Args:
            queue_url: Queue URL
            message_body: Message content
            delay_seconds: Delivery delay
            message_attributes: Optional message attributes

        Returns:
            Message ID or None if failed
        """
        try:
            client = self._get_client()

            kwargs: dict[str, Any] = {
                "QueueUrl": queue_url,
                "MessageBody": message_body
            }

            if delay_seconds > 0:
                kwargs["DelaySeconds"] = delay_seconds

            if message_attributes:
                kwargs["MessageAttributes"] = self._format_message_attributes(message_attributes)

            response = client.send_message(**kwargs)
            return response["MessageId"]

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Send message failed: {e}")
            return None

    def send_message_batch(self, queue_url: str,
                          messages: list[tuple[str, Optional[int]]]) -> list[str]:
        """Send multiple messages to a queue.

        Args:
            queue_url: Queue URL
            messages: List of (message_body, delay_seconds) tuples

        Returns:
            List of message IDs
        """
        if not messages:
            return []

        try:
            client = self._get_client()

            entries = []
            for i, (body, delay) in enumerate(messages):
                entry: dict[str, Any] = {
                    "Id": str(i),
                    "MessageBody": body
                }
                if delay is not None and delay > 0:
                    entry["DelaySeconds"] = delay
                entries.append(entry)

            response = client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )

            return [m["MessageId"] for m in response.get("Successful", [])]

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Send message batch failed: {e}")
            return []

    def receive_messages(self, queue_url: str,
                        max_number: int = 10,
                        wait_time_seconds: int = 0,
                        visibility_timeout: Optional[int] = None) -> list[SQSMessage]:
        """Receive messages from a queue.

        Args:
            queue_url: Queue URL
            max_number: Maximum messages to receive (1-10)
            wait_time_seconds: Long polling duration (0-20)
            visibility_timeout: Override visibility timeout

        Returns:
            List of SQSMessage objects
        """
        try:
            client = self._get_client()

            kwargs: dict[str, Any] = {
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": min(max_number, 10),
                "WaitTimeSeconds": wait_time_seconds,
                "AttributeNames": ["All"],
                "MessageAttributeNames": ["All"]
            }

            if visibility_timeout is not None:
                kwargs["VisibilityTimeout"] = visibility_timeout

            response = client.receive_message(**kwargs)

            messages = []
            for msg in response.get("Messages", []):
                messages.append(SQSMessage(
                    message_id=msg["MessageId"],
                    receipt_handle=msg["ReceiptHandle"],
                    body=msg["Body"],
                    attributes=msg.get("Attributes", {}),
                    md5_of_body=msg.get("MD5OfBody", ""),
                    message_attributes=msg.get("MessageAttributes", {})
                ))

            return messages

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Receive messages failed: {e}")
            return []

    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """Delete a message from a queue.

        Args:
            queue_url: Queue URL
            receipt_handle: Receipt handle from received message

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Delete message failed: {e}")
            return False

    def delete_message_batch(self, queue_url: str,
                            receipt_handles: list[str]) -> int:
        """Delete multiple messages from a queue.

        Args:
            queue_url: Queue URL
            receipt_handles: List of receipt handles

        Returns:
            Number of deleted messages
        """
        if not receipt_handles:
            return 0

        try:
            client = self._get_client()

            entries = [
                {"Id": str(i), "ReceiptHandle": handle}
                for i, handle in enumerate(receipt_handles)
            ]

            response = client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )

            return len(response.get("Successful", []))

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Delete message batch failed: {e}")
            return 0

    def purge_queue(self, queue_url: str) -> bool:
        """Purge all messages from a queue.

        Args:
            queue_url: Queue URL

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.purge_queue(QueueUrl=queue_url)
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Purge queue failed: {e}")
            return False

    def change_message_visibility(self, queue_url: str,
                                  receipt_handle: str,
                                  visibility_timeout: int) -> bool:
        """Change message visibility timeout.

        Args:
            queue_url: Queue URL
            receipt_handle: Receipt handle
            visibility_timeout: New timeout in seconds

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout
            )
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Change visibility failed: {e}")
            return False

    def get_queue_attributes(self, queue_url: str) -> dict:
        """Get queue attributes.

        Args:
            queue_url: Queue URL

        Returns:
            Dict of queue attributes
        """
        try:
            client = self._get_client()
            response = client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["All"]
            )
            return response.get("Attributes", {})

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Get queue attributes failed: {e}")
            return {}

    def list_queues(self, prefix: str = "") -> list[SQSQueue]:
        """List SQS queues.

        Args:
            prefix: Queue name prefix filter

        Returns:
            List of SQSQueue objects
        """
        try:
            client = self._get_client()
            kwargs: dict[str, Any] = {}
            if prefix:
                kwargs["QueueNamePrefix"] = prefix

            response = client.list_queues(**kwargs)

            queues = []
            for url in response.get("QueueUrls", []):
                name = url.split("/")[-1]
                attr_response = client.get_queue_attributes(
                    QueueUrl=url,
                    AttributeNames=["QueueArn", "CreatedTimestamp", "ApproximateNumberOfMessages"]
                )
                attrs = attr_response.get("Attributes", {})

                queues.append(SQSQueue(
                    url=url,
                    name=name,
                    arn=attrs.get("QueueArn", ""),
                    created_timestamp=datetime.fromtimestamp(float(attrs.get("CreatedTimestamp", 0))) if attrs.get("CreatedTimestamp") else None,
                    approximate_message_count=int(attrs.get("ApproximateNumberOfMessages", 0))
                ))

            return queues

        except (ClientError, BotoCoreError) as e:
            logger.error(f"List queues failed: {e}")
            return []

    def delete_queue(self, queue_url: str) -> bool:
        """Delete an SQS queue.

        Args:
            queue_url: Queue URL

        Returns:
            True if deleted
        """
        try:
            client = self._get_client()
            client.delete_queue(QueueUrl=queue_url)
            return True

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Delete queue failed: {e}")
            return False

    def send_json(self, queue_url: str, data: Any,
                 delay_seconds: int = 0) -> Optional[str]:
        """Send JSON-serializable data to a queue.

        Args:
            queue_url: Queue URL
            data: JSON-serializable data
            delay_seconds: Delivery delay

        Returns:
            Message ID or None if failed
        """
        try:
            body = json.dumps(data, default=str)
            return self.send_message(queue_url, body, delay_seconds)

        except (TypeError, ValueError) as e:
            logger.error(f"JSON serialize failed: {e}")
            return None

    def receive_json(self, queue_url: str,
                    max_number: int = 10) -> list[tuple[str, Any]]:
        """Receive messages and parse as JSON.

        Args:
            queue_url: Queue URL
            max_number: Maximum messages to receive

        Returns:
            List of (receipt_handle, parsed_json) tuples
        """
        messages = self.receive_messages(queue_url, max_number)
        results = []

        for msg in messages:
            try:
                data = json.loads(msg.body)
                results.append((msg.receipt_handle, data))
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse message {msg.message_id} as JSON")
                results.append((msg.receipt_handle, msg.body))

        return results

    def _format_message_attributes(self, attrs: dict) -> dict:
        """Format message attributes for SQS."""
        formatted = {}

        for key, value in attrs.items():
            if isinstance(value, str):
                formatted[key] = {
                    "DataType": "String",
                    "StringValue": value
                }
            elif isinstance(value, bytes):
                formatted[key] = {
                    "DataType": "Binary",
                    "BinaryValue": value
                }
            elif isinstance(value, (int, float)):
                formatted[key] = {
                    "DataType": "Number",
                    "StringValue": str(value)
                }

        return formatted
