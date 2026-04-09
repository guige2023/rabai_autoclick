"""Automation Adaptor Action Module.

Provides protocol and interface adaptation for automation workflows,
translating between different input/output formats and protocols.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ProtocolType(Enum):
    """Supported protocol types."""
    HTTP = "http"
    WEBSOCKET = "websocket"
    GRPC = "grpc"
    STREAM = "stream"
    BATCH = "batch"
    EVENT = "event"


@dataclass
class AdaptorConfig:
    """Configuration for an adaptor."""
    input_protocol: ProtocolType
    output_protocol: ProtocolType
    timeout: float = 30.0
    buffer_size: int = 1024


@dataclass
class AdaptationResult:
    """Result of an adaptation operation."""
    success: bool
    adapted: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ProtocolAdapter:
    """Base class for protocol adapters."""

    def __init__(self, config: AdaptorConfig):
        self._config = config

    async def adapt(self, data: Any) -> AdaptationResult:
        """Adapt data from input to output protocol."""
        raise NotImplementedError


class HTTPToWebSocketAdapter(ProtocolAdapter):
    """Adapt HTTP requests to WebSocket messages."""

    def __init__(self, config: AdaptorConfig):
        super().__init__(config)

    async def adapt(self, data: Any) -> AdaptationResult:
        """Convert HTTP request to WebSocket frame."""
        try:
            adapted = {
                "type": "text",
                "data": str(data),
                "opcode": 1  # Text frame
            }
            return AdaptationResult(success=True, adapted=adapted)
        except Exception as e:
            return AdaptationResult(success=False, error=str(e))


class BatchToStreamAdapter(ProtocolAdapter):
    """Adapt batch operations to stream."""

    def __init__(self, config: AdaptorConfig):
        super().__init__(config)

    async def adapt(self, data: Any) -> AdaptationResult:
        """Convert batch to stream of items."""
        try:
            if isinstance(data, list):
                return AdaptationResult(
                    success=True,
                    adapted=data,
                    metadata={"stream_count": len(data)}
                )
            return AdaptationResult(
                success=True,
                adapted=[data],
                metadata={"stream_count": 1}
            )
        except Exception as e:
            return AdaptationResult(success=False, error=str(e))


class StreamToBatchAdapter(ProtocolAdapter):
    """Adapt stream to batch operations."""

    def __init__(self, config: AdaptorConfig, batch_size: int = 100):
        super().__init__(config)
        self._batch_size = batch_size
        self._buffer: List[Any] = []

    async def adapt(self, data: Any) -> AdaptationResult:
        """Buffer stream items until batch is ready."""
        self._buffer.append(data)

        if len(self._buffer) >= self._batch_size:
            batch = list(self._buffer)
            self._buffer.clear()
            return AdaptationResult(
                success=True,
                adapted=batch,
                metadata={"batch_size": len(batch), "flushed": True}
            )

        return AdaptationResult(
            success=True,
            adapted=[],
            metadata={"buffered": len(self._buffer), "flushed": False}
        )

    async def flush(self) -> AdaptationResult:
        """Flush remaining buffered items."""
        if self._buffer:
            batch = list(self._buffer)
            self._buffer.clear()
            return AdaptationResult(
                success=True,
                adapted=batch,
                metadata={"batch_size": len(batch), "flushed": True}
            )
        return AdaptationResult(success=True, adapted=[], metadata={"flushed": True})


class InterfaceAdapter:
    """Adapt between different function interfaces."""

    def __init__(self):
        self._transforms: Dict[str, Callable] = {}

    def register_transform(
        self,
        from_interface: str,
        to_interface: str,
        transform: Callable
    ) -> None:
        """Register an interface transformation."""
        key = f"{from_interface}->{to_interface}"
        self._transforms[key] = transform

    def adapt_call(
        self,
        from_interface: str,
        to_interface: str,
        args: tuple,
        kwargs: Dict[str, Any]
    ) -> tuple:
        """Adapt function call from one interface to another."""
        key = f"{from_interface}->{to_interface}"
        transform = self._transforms.get(key)

        if transform:
            return transform(args, kwargs)
        return args, kwargs


class FormatAdapter:
    """Adapt between different data formats."""

    def __init__(self):
        self._converters: Dict[str, Callable] = {}

    def register_converter(
        self,
        from_format: str,
        to_format: str,
        converter: Callable
    ) -> None:
        """Register a format converter."""
        key = f"{from_format}->{to_format}"
        self._converters[key] = converter

    def convert(self, data: Any, from_format: str, to_format: str) -> Any:
        """Convert data from one format to another."""
        key = f"{from_format}->{to_format}"
        converter = self._converters.get(key)

        if converter:
            return converter(data)
        return data


class AutomationAdaptorAction:
    """Main action class for automation adaptation."""

    def __init__(self):
        self._interface_adapter = InterfaceAdapter()
        self._format_adapter = FormatAdapter()
        self._stream_adapters: Dict[str, StreamToBatchAdapter] = {}

    def register_converter(
        self,
        from_format: str,
        to_format: str,
        converter: Callable
    ) -> None:
        """Register a format converter."""
        self._format_adapter.register_converter(from_format, to_format, converter)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation adaptor action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with adaptation results.
        """
        operation = context.get("operation", "adapt")

        if operation == "adapt":
            data = context.get("data")
            from_format = context.get("from_format", "")
            to_format = context.get("to_format", "")

            if from_format and to_format:
                adapted = self._format_adapter.convert(data, from_format, to_format)
                return {"success": True, "adapted": adapted, "from": from_format, "to": to_format}

            return {"success": False, "error": "Formats not specified"}

        elif operation == "register_converter":
            self.register_converter(
                context.get("from_format", ""),
                context.get("to_format", ""),
                lambda x: x  # Placeholder
            )
            return {"success": True}

        elif operation == "adapt_interface":
            args, kwargs = self._interface_adapter.adapt_call(
                context.get("from_interface", ""),
                context.get("to_interface", ""),
                tuple(context.get("args", [])),
                context.get("kwargs", {})
            )
            return {"success": True, "args": list(args), "kwargs": kwargs}

        elif operation == "create_stream_adapter":
            key = context.get("key", "")
            batch_size = context.get("batch_size", 100)
            self._stream_adapters[key] = StreamToBatchAdapter(
                AdaptorConfig(ProtocolType.BATCH, ProtocolType.STREAM),
                batch_size
            )
            return {"success": True, "key": key}

        elif operation == "stream_to_batch":
            key = context.get("key", "")
            data = context.get("data")

            adapter = self._stream_adapters.get(key)
            if adapter:
                result = await adapter.adapt(data)
                return {
                    "success": result.success,
                    "adapted": result.adapted,
                    "metadata": result.metadata
                }
            return {"success": False, "error": "Adapter not found"}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
