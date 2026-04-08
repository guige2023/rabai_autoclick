"""
Serverless function executor for cloud function platforms.

Supports AWS Lambda, Google Cloud Functions, Azure Functions,
and generic HTTP-based serverless backends.
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ServerlessProvider(Enum):
    """Supported serverless providers."""
    AWS_LAMBDA = "aws_lambda"
    GOOGLE_CLOUD_FUNCTIONS = "gcp_functions"
    AZURE_FUNCTIONS = "azure_functions"
    GENERIC_HTTP = "generic_http"
    VERCEL = "vercel"
    NETLIFY = "netlify"


class InvocationMode(Enum):
    """How the function is invoked."""
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    BATCH = "batch"


@dataclass
class FunctionConfig:
    """Configuration for a serverless function."""
    name: str
    provider: ServerlessProvider
    region: str = "us-east-1"
    runtime: str = "python3.11"
    memory_mb: int = 256
    timeout_seconds: int = 30
    environment: dict = field(default_factory=dict)
    layers: list[str] = field(default_factory=list)
    iam_role: Optional[str] = None
    vpc_config: Optional[dict] = None


@dataclass
class InvocationResult:
    """Result of a serverless function invocation."""
    request_id: str
    status_code: int
    payload: Any
    duration_ms: float
    billed_duration_ms: Optional[int] = None
    memory_used_mb: Optional[int] = None
    logs: Optional[str] = None
    error: Optional[str] = None
    invoked_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None


@dataclass
class BatchResult:
    """Result of a batch invocation."""
    batch_id: str
    total_count: int
    success_count: int
    failure_count: int
    results: list[InvocationResult]
    duration_ms: float


class ServerlessExecutor:
    """
    Executor for serverless functions across multiple providers.

    Provides a unified interface for invoking functions regardless
    of the underlying cloud provider.
    """

    def __init__(
        self,
        credentials: Optional[dict] = None,
        default_config: Optional[FunctionConfig] = None,
    ):
        self.credentials = credentials or {}
        self.default_config = default_config
        self._functions: dict[str, FunctionConfig] = {}
        self._invocation_history: list[InvocationResult] = []

    def register_function(
        self,
        name: str,
        config: FunctionConfig,
    ) -> None:
        """Register a function configuration."""
        self._functions[name] = config

    def invoke(
        self,
        function_name: str,
        payload: Any,
        mode: InvocationMode = InvocationMode.SYNCHRONOUS,
        config: Optional[FunctionConfig] = None,
    ) -> InvocationResult:
        """Invoke a serverless function."""
        cfg = config or self._functions.get(function_name)
        if not cfg:
            return InvocationResult(
                request_id=str(uuid.uuid4()),
                status_code=404,
                payload=None,
                duration_ms=0,
                error=f"Function not found: {function_name}",
            )

        request_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            if cfg.provider == ServerlessProvider.AWS_LAMBDA:
                result = self._invoke_aws_lambda(cfg, payload, mode)
            elif cfg.provider == ServerlessProvider.GOOGLE_CLOUD_FUNCTIONS:
                result = self._invoke_gcp_functions(cfg, payload, mode)
            elif cfg.provider == ServerlessProvider.AZURE_FUNCTIONS:
                result = self._invoke_azure_functions(cfg, payload, mode)
            elif cfg.provider == ServerlessProvider.GENERIC_HTTP:
                result = self._invoke_generic_http(cfg, payload, mode)
            else:
                result = self._invoke_generic_http(cfg, payload, mode)

            result.request_id = request_id
            result.invoked_at = start_time
            result.completed_at = time.time()
            result.duration_ms = (result.completed_at - start_time) * 1000

            self._invocation_history.append(result)
            return result

        except Exception as e:
            return InvocationResult(
                request_id=request_id,
                status_code=500,
                payload=None,
                duration_ms=(time.time() - start_time) * 1000,
                error=str(e),
                invoked_at=start_time,
                completed_at=time.time(),
            )

    def invoke_batch(
        self,
        function_name: str,
        payloads: list[Any],
        config: Optional[FunctionConfig] = None,
    ) -> BatchResult:
        """Invoke a function with multiple payloads."""
        batch_id = str(uuid.uuid4())
        start_time = time.time()
        results = []

        for payload in payloads:
            result = self.invoke(function_name, payload, config=config)
            results.append(result)

        duration_ms = (time.time() - start_time) * 1000
        success_count = sum(1 for r in results if r.status_code < 400)
        failure_count = len(results) - success_count

        return BatchResult(
            batch_id=batch_id,
            total_count=len(results),
            success_count=success_count,
            failure_count=failure_count,
            results=results,
            duration_ms=duration_ms,
        )

    def _invoke_aws_lambda(
        self,
        config: FunctionConfig,
        payload: Any,
        mode: InvocationMode,
    ) -> InvocationResult:
        """Invoke AWS Lambda function."""
        import boto3
        import json

        client = boto3.client(
            "lambda",
            region_name=config.region,
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
        )

        payload_bytes = json.dumps(payload).encode("utf-8")

        if mode == InvocationMode.ASYNCHRONOUS:
            response = client.invoke(
                FunctionName=config.name,
                InvocationType="Event",
                Payload=payload_bytes,
            )
        else:
            response = client.invoke(
                FunctionName=config.name,
                InvocationType="RequestResponse",
                Payload=payload_bytes,
            )

        response_payload = json.loads(response["Payload"].read().decode("utf-8"))
        status_code = response.get("StatusCode", 200)
        error = None
        if "FunctionError" in response:
            error = response_payload.get("errorMessage", "Lambda error")

        return InvocationResult(
            request_id="",
            status_code=status_code,
            payload=response_payload,
            duration_ms=0,
            billed_duration_ms=response.get("BilledDuration"),
        )

    def _invoke_gcp_functions(
        self,
        config: FunctionConfig,
        payload: Any,
        mode: InvocationMode,
    ) -> InvocationResult:
        """Invoke Google Cloud Function."""
        import requests

        url = (
            f"https://{config.region}-{config.name}.cloudfunctions.net/{config.name}"
        )
        headers = {"Content-Type": "application/json"}
        if self.credentials.get("gcp_token"):
            headers["Authorization"] = f"Bearer {self.credentials['gcp_token']}"

        response = requests.post(url, json=payload, headers=headers, timeout=config.timeout_seconds)

        return InvocationResult(
            request_id=response.headers.get("X-Google-Cloud-Resource", ""),
            status_code=response.status_code,
            payload=response.json() if response.ok else response.text,
            duration_ms=0,
            error=None if response.ok else response.text,
        )

    def _invoke_azure_functions(
        self,
        config: FunctionConfig,
        payload: Any,
        mode: InvocationMode,
    ) -> InvocationResult:
        """Invoke Azure Function."""
        import requests

        url = (
            f"https://{config.name}-{config.region}.azurewebsites.net/api/{config.name}"
        )
        headers = {"Content-Type": "application/json"}
        if self.credentials.get("azure_token"):
            headers["x-functions-key"] = self.credentials["azure_token"]

        response = requests.post(url, json=payload, headers=headers, timeout=config.timeout_seconds)

        return InvocationResult(
            request_id=response.headers.get("X-Azure-Functions-RequestId", ""),
            status_code=response.status_code,
            payload=response.json() if response.ok else response.text,
            duration_ms=0,
            error=None if response.ok else response.text,
        )

    def _invoke_generic_http(
        self,
        config: FunctionConfig,
        payload: Any,
        mode: InvocationMode,
    ) -> InvocationResult:
        """Invoke a generic HTTP endpoint as a serverless function."""
        import requests

        url = config.environment.get("FUNCTION_URL", f"http://localhost:8080/{config.name}")
        headers = {"Content-Type": "application/json"}
        headers.update(config.environment.get("HEADERS", {}))

        response = requests.post(url, json=payload, headers=headers, timeout=config.timeout_seconds)

        return InvocationResult(
            request_id=str(uuid.uuid4()),
            status_code=response.status_code,
            payload=response.json() if response.ok else response.text,
            duration_ms=0,
            error=None if response.ok else response.text,
        )

    def create_local_emulator(
        self,
        function_name: str,
        handler: Callable,
        config: Optional[FunctionConfig] = None,
    ) -> None:
        """Create a local emulator for testing."""
        cfg = config or self._functions.get(function_name)
        if not cfg:
            cfg = FunctionConfig(
                name=function_name,
                provider=ServerlessProvider.GENERIC_HTTP,
            )

        self._local_handlers = getattr(self, "_local_handlers", {})
        self._local_handlers[function_name] = handler

    def get_invocation_stats(self) -> dict:
        """Get invocation statistics."""
        if not self._invocation_history:
            return {"total": 0, "success": 0, "failure": 0, "avg_duration_ms": 0}

        total = len(self._invocation_history)
        success = sum(1 for r in self._invocation_history if r.status_code < 400)
        failure = total - success
        avg_duration = sum(r.duration_ms for r in self._invocation_history) / total

        return {
            "total": total,
            "success": success,
            "failure": failure,
            "avg_duration_ms": avg_duration,
            "total_duration_ms": sum(r.duration_ms for r in self._invocation_history),
        }
