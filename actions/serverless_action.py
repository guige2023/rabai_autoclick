"""Serverless action for cloud function execution.

This module provides serverless computing support:
- AWS Lambda integration
- Google Cloud Functions
- Azure Functions
- Vercel/Netlify functions
- Generic HTTP function wrapper
- Cold start optimization
- Concurrency management

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import parse_qs, urlparse

try:
    import boto3
    from botocore.config import Config as BotoConfig
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    BotoConfig = None

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

logger = logging.getLogger(__name__)


class CloudProvider(Enum):
    """Supported cloud providers."""
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"
    VERCEL = "vercel"
    NETLIFY = "netlify"
    GENERIC = "generic"


class InvocationType(Enum):
    """Lambda invocation types."""
    REQUEST_RESPONSE = "RequestResponse"
    EVENT = "Event"
    DRY_RUN = "DryRun"


@dataclass
class FunctionConfig:
    """Serverless function configuration."""
    name: str
    provider: CloudProvider = CloudProvider.AWS
    runtime: Optional[str] = "python3.11"
    handler: Optional[str] = None
    memory_size: int = 256
    timeout_seconds: int = 30
    environment: Dict[str, str] = field(default_factory=dict)
    layers: List[str] = field(default_factory=list)
    vpc_config: Optional[Dict[str, Any]] = None
    dead_letter_config: Optional[Dict[str, str]] = None
    reserved_concurrency: Optional[int] = None


@dataclass
class InvocationRequest:
    """Function invocation request."""
    function_name: str
    payload: Any
    invocation_type: InvocationType = InvocationRequest
    qualifier: Optional[str] = None
    client_context: Optional[Dict[str, str]] = None


@dataclass
class InvocationResult:
    """Function invocation result."""
    status_code: int
    payload: Any
    executed_version: Optional[str] = None
    logs: Optional[str] = None
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class ScheduledEvent:
    """Scheduled event from CloudWatch Events/EventBridge."""
    id: str
    source: str
    detail_type: str
    time: str
    detail: Dict[str, Any]


@dataclass
class SQSEvent:
    """SQS queue event."""
    records: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SNSEvent:
    """SNS topic event."""
    records: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class APIEvent:
    """API Gateway event."""
    http_method: str
    path: str
    query_params: Dict[str, str] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    path_params: Dict[str, str] = field(default_factory=dict)
    stage_variables: Dict[str, str] = field(default_factory=dict)
    is_base64_encoded: bool = False


@dataclass
class APIResponse:
    """API Gateway response."""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: str = ""
    is_base64_encoded: bool = False


class AWSLambdaClient:
    """AWS Lambda client for function invocation.

    Provides Lambda function management and invocation:
    - Synchronous and asynchronous invocations
    - Batch processing
    - Alias/version management
    - Cold start tracking
    """

    def __init__(
        self,
        region: str = "us-east-1",
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        config: Optional[BotoConfig] = None,
    ):
        """Initialize AWS Lambda client.

        Args:
            region: AWS region
            access_key_id: AWS access key
            secret_access_key: AWS secret key
            session_token: Session token for temp credentials
            config: Botocore configuration
        """
        self.region = region
        self._lambda_client = None
        self._config = config

        if BOTO3_AVAILABLE:
            self._init_client(access_key_id, secret_access_key, session_token)

    def _init_client(
        self,
        access_key_id: Optional[str],
        secret_access_key: Optional[str],
        session_token: Optional[str],
    ) -> None:
        """Initialize boto3 client."""
        boto_kwargs = {
            "region_name": self.region,
        }

        if access_key_id and secret_access_key:
            boto_kwargs["aws_access_key_id"] = access_key_id
            boto_kwargs["aws_secret_access_key"] = secret_access_key

        if session_token:
            boto_kwargs["aws_session_token"] = session_token

        if self._config:
            boto_kwargs["config"] = self._config

        self._lambda_client = boto3.client("lambda", **boto_kwargs)

    def invoke(
        self,
        function_name: str,
        payload: Any,
        invocation_type: InvocationType = InvocationType.REQUEST_RESPONSE,
        qualifier: Optional[str] = None,
        wait: bool = True,
    ) -> InvocationResult:
        """Invoke Lambda function.

        Args:
            function_name: Function name or ARN
            payload: Invocation payload
            invocation_type: Invocation type
            qualifier: Version or alias
            wait: Wait for result (only for RequestResponse)

        Returns:
            Invocation result
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        start_time = time.time()

        if isinstance(payload, dict):
            payload_bytes = json.dumps(payload).encode("utf-8")
        elif isinstance(payload, str):
            payload_bytes = payload.encode("utf-8")
        else:
            payload_bytes = payload

        invoke_kwargs = {
            "FunctionName": function_name,
            "InvocationType": invocation_type.value,
            "Payload": payload_bytes,
        }

        if qualifier:
            invoke_kwargs["Qualifier"] = qualifier

        try:
            response = self._lambda_client.invoke(**invoke_kwargs)

            duration_ms = (time.time() - start_time) * 1000

            status_code = response.get("StatusCode", 0)
            payload = response.get("Payload", b"").read()

            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")

            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                pass

            logs = None
            if "LogResult" in response:
                logs = base64.b64decode(response["LogResult"]).decode("utf-8")

            return InvocationResult(
                status_code=status_code,
                payload=payload,
                executed_version=response.get("ExecutedVersion"),
                logs=logs,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            return InvocationResult(
                status_code=500,
                payload=None,
                duration_ms=duration_ms,
                error=str(e),
            )

    def invoke_async(
        self,
        function_name: str,
        payload: Any,
    ) -> InvocationResult:
        """Asynchronously invoke Lambda function.

        Args:
            function_name: Function name
            payload: Invocation payload

        Returns:
            Invocation result
        """
        return self.invoke(
            function_name,
            payload,
            InvocationType.EVENT,
        )

    def batch_invoke(
        self,
        items: List[Any],
        function_name: str,
        batch_size: int = 10,
        parallel: bool = True,
    ) -> List[InvocationResult]:
        """Batch invoke Lambda function.

        Args:
            items: List of payloads
            function_name: Function name
            batch_size: Items per batch
            parallel: Execute batches in parallel

        Returns:
            List of invocation results
        """
        batches = [
            items[i:i + batch_size]
            for i in range(0, len(items), batch_size)
        ]

        results = []

        for batch in batches:
            batch_payload = {"batch": batch}
            result = self.invoke(function_name, batch_payload)
            results.append(result)

        return results

    def create_function(
        self,
        config: FunctionConfig,
        zip_path: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create Lambda function.

        Args:
            config: Function configuration
            zip_path: Local zip file path
            s3_bucket: S3 bucket for code
            s3_key: S3 key for code

        Returns:
            Function details
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        if zip_path and s3_bucket and s3_key:
            code = {"S3Bucket": s3_bucket, "S3Key": s3_key}
        elif zip_path:
            with open(zip_path, "rb") as f:
                code = {"ZipFile": f.read()}
        else:
            raise ValueError("Either zip_path with s3 info or zip_path required")

        kwargs = {
            "FunctionName": config.name,
            "Runtime": config.runtime,
            "Role": "required",
            "Handler": config.handler or f"{config.name}.handler",
            "Code": code,
            "Timeout": config.timeout_seconds,
            "MemorySize": config.memory_size,
            "Environment": {"Variables": config.environment},
        }

        if config.layers:
            kwargs["Layers"] = config.layers

        if config.vpc_config:
            kwargs["VpcConfig"] = config.vpc_config

        if config.dead_letter_config:
            kwargs["DeadLetterConfig"] = config.dead_letter_config

        if config.reserved_concurrency is not None:
            kwargs["ReservedConcurrentExecutions"] = config.reserved_concurrency

        return self._lambda_client.create_function(**kwargs)

    def delete_function(self, function_name: str) -> bool:
        """Delete Lambda function.

        Args:
            function_name: Function name

        Returns:
            True if deleted
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        try:
            self._lambda_client.delete_function(FunctionName=function_name)
            return True
        except Exception:
            return False

    def update_function_code(
        self,
        function_name: str,
        zip_path: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update function code.

        Args:
            function_name: Function name
            zip_path: Local zip file
            s3_bucket: S3 bucket
            s3_key: S3 key

        Returns:
            Updated function details
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        kwargs = {"FunctionName": function_name}

        if s3_bucket and s3_key:
            kwargs["S3Bucket"] = s3_bucket
            kwargs["S3Key"] = s3_key
        elif zip_path:
            with open(zip_path, "rb") as f:
                kwargs["ZipFile"] = f.read()
        else:
            raise ValueError("s3 info or zip_path required")

        return self._lambda_client.update_function_code(**kwargs)

    def update_function_config(
        self,
        function_name: str,
        memory_size: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        environment: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Update function configuration.

        Args:
            function_name: Function name
            memory_size: Memory in MB
            timeout_seconds: Timeout in seconds
            environment: Environment variables

        Returns:
            Updated function details
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        kwargs = {"FunctionName": function_name}

        if memory_size is not None:
            kwargs["MemorySize"] = memory_size

        if timeout_seconds is not None:
            kwargs["Timeout"] = timeout_seconds

        if environment is not None:
            kwargs["Environment"] = {"Variables": environment}

        return self._lambda_client.update_function_configuration(**kwargs)

    def publish_version(self, function_name: str) -> str:
        """Publish new version.

        Args:
            function_name: Function name

        Returns:
            Version number
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        response = self._lambda_client.publish_version(FunctionName=function_name)
        return response["Version"]

    def create_alias(
        self,
        function_name: str,
        alias_name: str,
        version: str,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create function alias.

        Args:
            function_name: Function name
            alias_name: Alias name
            version: Version number
            description: Alias description

        Returns:
            Alias details
        """
        if not self._lambda_client:
            raise RuntimeError("Lambda client not initialized")

        kwargs = {
            "FunctionName": function_name,
            "Name": alias_name,
            "FunctionVersion": version,
        }

        if description:
            kwargs["Description"] = description

        return self._lambda_client.create_alias(**kwargs)


class ServerlessFunction:
    """Base serverless function handler.

    Provides common functionality for serverless functions:
    - Event parsing
    - Response formatting
    - Error handling
    - Cold start optimization
    """

    def __init__(
        self,
        name: str,
        provider: CloudProvider = CloudProvider.GENERIC,
    ):
        """Initialize serverless function.

        Args:
            name: Function name
            provider: Cloud provider
        """
        self.name = name
        self.provider = provider
        self._cold_start = True
        self._invocation_count = 0
        self._total_duration = 0.0

    def parse_event(self, event: Dict[str, Any]) -> Any:
        """Parse incoming event.

        Args:
            event: Raw event

        Returns:
            Parsed event
        """
        if self.provider == CloudProvider.AWS:
            if "Records" in event:
                if "s3" in event["Records"][0]:
                    return self._parse_s3_event(event)
                elif "sns" in event["Records"][0]:
                    return self._parse_sns_event(event)
                elif "sqs" in event["Records"][0]:
                    return self._parse_sqs_event(event)

            if "httpMethod" in event:
                return self._parse_api_gateway_event(event)

            if "detail" in event:
                return self._parse_eventbridge_event(event)

        return event

    def _parse_api_gateway_event(self, event: Dict[str, Any]) -> APIEvent:
        """Parse API Gateway event."""
        return APIEvent(
            http_method=event.get("httpMethod", "GET"),
            path=event.get("path", "/"),
            query_params=event.get("queryStringParameters") or {},
            headers=event.get("headers") or {},
            body=event.get("body"),
            path_params=event.get("pathParameters") or {},
            stage_variables=event.get("stageVariables") or {},
            is_base64_encoded=event.get("isBase64Encoded", False),
        )

    def _parse_sqs_event(self, event: Dict[str, Any]) -> SQSEvent:
        """Parse SQS event."""
        records = []
        for record in event.get("Records", []):
            records.append({
                "message_id": record.get("messageId"),
                "receipt_handle": record.get("receiptHandle"),
                "body": record.get("body"),
                "attributes": record.get("messageAttributes", {}),
            })
        return SQSEvent(records=records)

    def _parse_sns_event(self, event: Dict[str, Any]) -> SNSEvent:
        """Parse SNS event."""
        records = []
        for record in event.get("Records", []):
            records.append({
                "topic_arn": record.get("Sns", {}).get("TopicArn"),
                "message_id": record.get("Sns", {}).get("MessageId"),
                "subject": record.get("Sns", {}).get("Subject"),
                "message": record.get("Sns", {}).get("Message"),
                "timestamp": record.get("Sns", {}).get("Timestamp"),
            })
        return SNSEvent(records=records)

    def _parse_s3_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Parse S3 event."""
        return event

    def _parse_eventbridge_event(self, event: Dict[str, Any]) -> ScheduledEvent:
        """Parse EventBridge event."""
        return ScheduledEvent(
            id=event.get("id"),
            source=event.get("source"),
            detail_type=event.get("detail-type"),
            time=event.get("time"),
            detail=event.get("detail", {}),
        )

    def format_response(
        self,
        body: Any,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Format API Gateway response.

        Args:
            body: Response body
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Formatted response
        """
        return APIResponse(
            status_code=status_code,
            headers=headers or {"Content-Type": "application/json"},
            body=json.dumps(body) if not isinstance(body, str) else body,
        ).__dict__

    def format_error(
        self,
        error: str,
        status_code: int = 500,
    ) -> Dict[str, Any]:
        """Format error response.

        Args:
            error: Error message
            status_code: HTTP status code

        Returns:
            Formatted error response
        """
        return self.format_response(
            {"error": error},
            status_code=status_code,
        )

    def handle(self, event: Any, context: Any = None) -> Any:
        """Handle function invocation.

        Override this method in subclasses.

        Args:
            event: Invocation event
            context: Lambda context

        Returns:
            Function result
        """
        raise NotImplementedError

    def _track_invocation(self, duration_ms: float) -> None:
        """Track invocation metrics.

        Args:
            duration_ms: Invocation duration
        """
        self._invocation_count += 1
        self._total_duration += duration_ms
        self._cold_start = False

    def get_metrics(self) -> Dict[str, Any]:
        """Get function metrics.

        Returns:
            Metrics dictionary
        """
        avg_duration = (
            self._total_duration / self._invocation_count
            if self._invocation_count > 0
            else 0
        )

        return {
            "invocation_count": self._invocation_count,
            "cold_starts": 1 if self._cold_start else 0,
            "avg_duration_ms": avg_duration,
            "total_duration_ms": self._total_duration,
        }


class GenericServerlessClient:
    """Generic HTTP-based serverless function client.

    Works with Vercel, Netlify, or any HTTP-accessible function.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
    ):
        """Initialize generic serverless client.

        Args:
            endpoint: Function endpoint URL
            api_key: API key for authentication
            headers: Default headers
            timeout: Request timeout
        """
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.headers = headers or {}
        self.timeout = timeout

        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

    async def invoke(
        self,
        payload: Any,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
    ) -> InvocationResult:
        """Invoke serverless function.

        Args:
            payload: Function payload
            method: HTTP method
            headers: Additional headers

        Returns:
            Invocation result
        """
        if not AIOHTTP_AVAILABLE:
            raise ImportError("aiohttp required")

        start_time = time.time()

        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        body = None
        if method in ("POST", "PUT", "PATCH"):
            body = json.dumps(payload)
            request_headers["Content-Type"] = "application/json"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method,
                    self.endpoint,
                    headers=request_headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    duration_ms = (time.time() - start_time) * 1000

                    response_body = await response.text()

                    try:
                        response_body = json.loads(response_body)
                    except (json.JSONDecodeError, TypeError):
                        pass

                    return InvocationResult(
                        status_code=response.status,
                        payload=response_body,
                        duration_ms=duration_ms,
                    )

        except asyncio.TimeoutError:
            duration_ms = (time.time() - start_time) * 1000

            return InvocationResult(
                status_code=408,
                payload=None,
                duration_ms=duration_ms,
                error="Request timeout",
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            return InvocationResult(
                status_code=500,
                payload=None,
                duration_ms=duration_ms,
                error=str(e),
            )

    async def invoke_get(self, params: Optional[Dict[str, str]] = None) -> InvocationResult:
        """Invoke function with GET.

        Args:
            params: Query parameters

        Returns:
            Invocation result
        """
        return await self.invoke(None, method="GET", headers=params)

    async def invoke_post(self, payload: Any) -> InvocationResult:
        """Invoke function with POST.

        Args:
            payload: Request payload

        Returns:
            Invocation result
        """
        return await self.invoke(payload, method="POST")


class WebhookHandler:
    """Webhook handler for serverless functions.

    Provides signature verification and event parsing.
    """

    def __init__(
        self,
        secret: Optional[str] = None,
        provider: Optional[str] = None,
    ):
        """Initialize webhook handler.

        Args:
            secret: Webhook secret for signature verification
            provider: Provider name (github, stripe, etc.)
        """
        self.secret = secret
        self.provider = provider

    def verify_signature(
        self,
        payload: bytes,
        signature: str,
        algorithm: str = "sha256",
    ) -> bool:
        """Verify webhook signature.

        Args:
            payload: Raw payload bytes
            signature: Signature from header
            algorithm: Hash algorithm

        Returns:
            True if signature valid
        """
        if not self.secret:
            return True

        if algorithm == "sha256":
            expected = hmac.new(
                self.secret.encode("utf-8"),
                payload,
                hashlib.sha256
            ).hexdigest()
            expected = f"sha256={expected}"
        elif algorithm == "sha1":
            expected = hmac.new(
                self.secret.encode("utf-8"),
                payload,
                hashlib.sha1
            ).hexdigest()
            expected = f"sha1={expected}"
        else:
            return False

        return hmac.compare_digest(expected, signature)

    def parse_github_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Parse GitHub webhook event.

        Args:
            event: GitHub webhook payload

        Returns:
            Normalized event
        """
        return {
            "action": event.get("action"),
            "event_type": event.get("event"),
            "repository": event.get("repository", {}).get("full_name"),
            "sender": event.get("sender", {}).get("login"),
            "payload": event,
        }

    def parse_stripe_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Stripe webhook event.

        Args:
            event: Stripe webhook payload

        Returns:
            Normalized event
        """
        return {
            "event_id": event.get("id"),
            "event_type": event.get("type"),
            "created": event.get("created"),
            "data": event.get("data", {}).get("object"),
        }


# Factory functions

def create_lambda_client(region: str = "us-east-1", **kwargs) -> AWSLambdaClient:
    """Create AWS Lambda client.

    Args:
        region: AWS region
        **kwargs: Additional arguments

    Returns:
        AWSLambdaClient instance
    """
    return AWSLambdaClient(region=region, **kwargs)


async def create_generic_client(
    endpoint: str,
    **kwargs
) -> GenericServerlessClient:
    """Create generic serverless client.

    Args:
        endpoint: Function endpoint
        **kwargs: Additional arguments

    Returns:
        GenericServerlessClient instance
    """
    return GenericServerlessClient(endpoint=endpoint, **kwargs)


def create_webhook_handler(
    secret: Optional[str] = None,
    provider: Optional[str] = None,
) -> WebhookHandler:
    """Create webhook handler.

    Args:
        secret: Webhook secret
        provider: Provider name

    Returns:
        WebhookHandler instance
    """
    return WebhookHandler(secret=secret, provider=provider)
