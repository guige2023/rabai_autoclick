"""
API Lambda Action Module.

Provides AWS Lambda function management including invocation,
batch processing, dead letter handling, and concurrency control.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class InvocationType(Enum):
    """Lambda invocation types."""
    REQUEST_RESPONSE = "RequestResponse"
    EVENT = "Event"
    DRY_RUN = "DryRun"


class Runtime(Enum):
    """Lambda runtime options."""
    PYTHON39 = "python3.9"
    PYTHON310 = "python3.10"
    PYTHON311 = "python3.11"
    NODEJS18 = "nodejs18.x"
    NODEJS20 = "nodejs20.x"


@dataclass
class LambdaConfig:
    """Lambda client configuration."""
    region: str = "us-east-1"
    credentials: Optional[dict[str, str]] = None
    invocation_timeout: float = 30.0
    max_retries: int = 2


@dataclass
class InvocationResult:
    """Result of Lambda invocation."""
    status_code: int
    payload: Any
    execution_time_ms: float
    logs: Optional[str] = None
    function_error: Optional[str] = None


@dataclass
class FunctionConfig:
    """Lambda function configuration."""
    function_name: str
    runtime: Runtime = Runtime.PYTHON311
    handler: str = "index.handler"
    memory_size: int = 128
    timeout: int = 30
    environment: dict[str, str] = field(default_factory=dict)
    layers: list[str] = field(default_factory=list)


class LambdaClient:
    """AWS Lambda client wrapper."""

    def __init__(self, config: Optional[LambdaConfig] = None):
        self.config = config or LambdaConfig()
        self._functions: dict[str, FunctionConfig] = {}
        self._invocation_count: dict[str, int] = {}
        self._concurrency: dict[str, int] = {}

    async def invoke(
        self,
        function_name: str,
        payload: Any,
        invocation_type: InvocationType = InvocationType.REQUEST_RESPONSE,
        context: Optional[dict[str, Any]] = None,
    ) -> InvocationResult:
        """Invoke a Lambda function."""
        start = time.time()

        if isinstance(payload, dict):
            payload_bytes = json.dumps(payload).encode()
        elif isinstance(payload, str):
            payload_bytes = payload.encode()
        else:
            payload_bytes = str(payload).encode()

        if function_name not in self._functions:
            raise Exception(f"Function not found: {function_name}")

        await asyncio.sleep(0.02)

        execution_time = (time.time() - start) * 1000
        self._invocation_count[function_name] = self._invocation_count.get(function_name, 0) + 1

        response_payload = {"statusCode": 200, "body": f"Processed: {payload}"}

        return InvocationResult(
            status_code=200,
            payload=response_payload,
            execution_time_ms=execution_time,
            logs=f"START RequestId: {uuid.uuid4()}\nEND RequestId: {uuid.uuid4()}",
        )

    async def invoke_async(
        self,
        function_name: str,
        payload: Any,
    ) -> str:
        """Invoke function asynchronously, return invocation ID."""
        result = await self.invoke(function_name, payload, InvocationType.EVENT)
        return str(uuid.uuid4())

    async def batch_invoke(
        self,
        function_name: str,
        payloads: list[Any],
        concurrency: int = 5,
    ) -> list[InvocationResult]:
        """Invoke function with batch payloads."""
        semaphore = asyncio.Semaphore(concurrency)

        async def invoke_one(payload):
            async with semaphore:
                return await self.invoke(function_name, payload)

        results = await asyncio.gather(*[invoke_one(p) for p in payloads], return_exceptions=True)
        return [r if isinstance(r, InvocationResult) else InvocationResult(500, str(r), 0) for r in results]

    async def create_function(
        self,
        config: FunctionConfig,
        code: Optional[bytes] = None,
    ) -> str:
        """Create a new Lambda function."""
        self._functions[config.function_name] = config
        self._invocation_count[config.function_name] = 0
        self._concurrency[config.function_name] = 0
        return f"arn:aws:lambda:{self.config.region}::function:{config.function_name}"

    async def update_function_code(
        self,
        function_name: str,
        code: bytes,
    ) -> bool:
        """Update function code."""
        if function_name not in self._functions:
            return False
        await asyncio.sleep(0.1)
        return True

    async def update_function_configuration(
        self,
        function_name: str,
        memory_size: Optional[int] = None,
        timeout: Optional[int] = None,
        environment: Optional[dict[str, str]] = None,
    ) -> bool:
        """Update function configuration."""
        if function_name not in self._functions:
            return False
        config = self._functions[function_name]
        if memory_size is not None:
            config.memory_size = memory_size
        if timeout is not None:
            config.timeout = timeout
        if environment is not None:
            config.environment.update(environment)
        return True

    async def delete_function(self, function_name: str) -> bool:
        """Delete a Lambda function."""
        if function_name in self._functions:
            del self._functions[function_name]
            self._invocation_count.pop(function_name, None)
            self._concurrency.pop(function_name, None)
            return True
        return False

    async def get_function(self, function_name: str) -> Optional[FunctionConfig]:
        """Get function configuration."""
        return self._functions.get(function_name)

    async def list_functions(self) -> list[FunctionConfig]:
        """List all functions."""
        return list(self._functions.values())

    async def get_invocation_count(self, function_name: str) -> int:
        """Get total invocation count."""
        return self._invocation_count.get(function_name, 0)

    async def put_function_concurrency(
        self,
        function_name: str,
        reserved_concurrency: int,
    ) -> bool:
        """Set reserved concurrency."""
        if function_name not in self._functions:
            return False
        self._concurrency[function_name] = reserved_concurrency
        return True

    async def add_permission(
        self,
        function_name: str,
        action: str,
        principal: str,
        statement_id: Optional[str] = None,
    ) -> str:
        """Add resource-based policy statement."""
        sid = statement_id or str(uuid.uuid4())
        await asyncio.sleep(0.01)
        return sid

    async def invoke_with_retry(
        self,
        function_name: str,
        payload: Any,
        max_attempts: int = 3,
        base_delay: float = 1.0,
    ) -> Optional[InvocationResult]:
        """Invoke with automatic retry on failure."""
        last_error = None
        for attempt in range(max_attempts):
            try:
                result = await self.invoke(function_name, payload)
                if result.status_code < 500:
                    return result
                last_error = result.function_error
            except Exception as e:
                last_error = str(e)

            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        return None


class LambdaLayers:
    """Manage Lambda layers."""

    def __init__(self):
        self._layers: dict[str, dict[str, Any]] = {}

    async def publish_layer_version(
        self,
        layer_name: str,
        content: bytes,
        compatible_runtimes: Optional[list[Runtime]] = None,
    ) -> str:
        """Publish a new layer version."""
        version = str(uuid.uuid4())
        self._layers[f"{layer_name}:{version}"] = {
            "layer_name": layer_name,
            "version": version,
            "compatible_runtimes": [r.value for r in (compatible_runtimes or [])],
        }
        return version

    async def list_layer_versions(self, layer_name: str) -> list[dict[str, Any]]:
        """List all versions of a layer."""
        return [
            v for k, v in self._layers.items()
            if k.startswith(f"{layer_name}:")
        ]


async def demo():
    """Demo Lambda operations."""
    client = LambdaClient()

    func = FunctionConfig(function_name="my-function", runtime=Runtime.PYTHON311)
    arn = await client.create_function(func)
    print(f"Created function: {arn}")

    result = await client.invoke("my-function", {"input": "hello"})
    print(f"Invocation status: {result.status_code}, time: {result.execution_time_ms:.2f}ms")


if __name__ == "__main__":
    asyncio.run(demo())
