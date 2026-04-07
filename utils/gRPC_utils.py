"""
gRPC utilities for high-performance RPC communication.

Provides service definition helpers, channel management,
interceptor chains, streaming, and load balancing.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """gRPC compression algorithms."""
    NONE = "identity"
    DEFLATE = "deflate"
    GZIP = "gzip"


@dataclass
class GrpcChannelConfig:
    """Configuration for a gRPC channel."""
    target: str = "localhost:50051"
    credentials: Optional[Any] = None
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    max_receive_message_length: int = 100 * 1024 * 1024
    max_send_message_length: int = 100 * 1024 * 1024
    keepalive_time_ms: int = 7200000
    keepalive_timeout_ms: int = 20000
    http2_min_sent_ping_interval_ms: int = 30000
    http2_max_pings_without_data: int = 0
    initial_window_size: int = 65535
    initial_conn_window_size: int = 65535


@dataclass
class MethodDescriptor:
    """Descriptor for a gRPC method."""
    service: str
    method: str
    request_serializer: Callable[[Any], bytes]
    response_deserializer: Callable[[bytes], Any]
    request_type: str = ""
    response_type: str = ""


@dataclass
class InterceptorConfig:
    """Configuration for a gRPC interceptor."""
    name: str
    interceptors: list[Any] = field(default_factory=list)


class GrpcChannelPool:
    """Pool of gRPC channels with connection reuse."""

    def __init__(self, config: Optional[GrpcChannelConfig] = None) -> None:
        self.config = config or GrpcChannelConfig()
        self._channels: dict[str, Any] = {}
        self._stubs: dict[str, Any] = {}
        self._lock = False

    def get_channel(self, target: Optional[str] = None) -> Any:
        """Get or create a gRPC channel."""
        target = target or self.config.target
        if target not in self._channels:
            try:
                import grpc
                options = [
                    ("grpc.max_receive_message_length", self.config.max_receive_message_length),
                    ("grpc.max_send_message_length", self.config.max_send_message_length),
                    ("grpc.keepalive_time_ms", self.config.keepalive_time_ms),
                    ("grpc.keepalive_timeout_ms", self.config.keepalive_timeout_ms),
                    ("grpc.http2.min_sent_ping_interval_without_data_ms", self.config.http2_min_sent_ping_interval_ms),
                    ("grpc.http2.max_pings_without_data", self.config.http2_max_pings_without_data),
                    ("grpc.initial_window_size", self.config.initial_window_size),
                    ("grpc.initial_conn_window_size", self.config.initial_conn_window_size),
                ]
                if self.config.compression != CompressionAlgorithm.NONE:
                    options.append(("grpc.default_compression_algorithm", self.config.compression.value))

                if self.config.credentials:
                    self._channels[target] = grpc.secure_channel(target, self.config.credentials, options=options)
                else:
                    self._channels[target] = grpc.insecure_channel(target, options=options)

                logger.info("Created gRPC channel to %s", target)
            except ImportError:
                logger.warning("grpcio not installed")
                return None
        return self._channels[target]

    def get_stub(self, stub_class: type, target: Optional[str] = None) -> Any:
        """Get a stub for a gRPC service."""
        target = target or self.config.target
        key = f"{target}:{stub_class.__name__}"
        if key not in self._stubs:
            channel = self.get_channel(target)
            if channel:
                self._stubs[key] = stub_class(channel)
        return self._stubs.get(key)

    def close_all(self) -> None:
        """Close all channels in the pool."""
        for channel in self._channels.values():
            try:
                channel.close()
            except Exception as e:
                logger.error("Error closing channel: %s", e)
        self._channels.clear()
        self._stubs.clear()


class UnaryUnaryInterceptor:
    """Interceptor for unary-unary RPC calls."""

    def __init__(self, name: str) -> None:
        self.name = name

    def intercept(
        self,
        method: Callable[..., Any],
        request: Any,
        metadata: Optional[list[tuple[str, str]]] = None,
    ) -> Any:
        """Override to implement interception logic."""
        return method(request, metadata=metadata)


class UnaryStreamInterceptor:
    """Interceptor for unary-stream RPC calls."""

    def __init__(self, name: str) -> None:
        self.name = name

    def intercept_stream(
        self,
        method: Callable[..., Any],
        request: Any,
        metadata: Optional[list[tuple[str, str]]] = None,
    ) -> Any:
        """Override to implement streaming interception logic."""
        return method(request, metadata=metadata)


class GrpcClient:
    """High-level gRPC client with interceptors and error handling."""

    def __init__(self, config: Optional[GrpcChannelConfig] = None) -> None:
        self.config = config or GrpcChannelConfig()
        self._pool = GrpcChannelPool(config)
        self._interceptors: list[UnaryUnaryInterceptor] = []

    def add_interceptor(self, interceptor: UnaryUnaryInterceptor) -> None:
        self._interceptors.append(interceptor)

    def call_unary(
        self,
        stub: Any,
        method_name: str,
        request: Any,
        timeout: float = 30.0,
        metadata: Optional[list[tuple[str, str]]] = None,
    ) -> Any:
        """Make a unary RPC call with interceptors."""
        method = getattr(stub, method_name)
        for interceptor in self._interceptors:
            original = method
            def make_wrapped(m: Callable, i: UnaryUnaryInterceptor) -> Callable:
                def wrapped(req: Any, metadata: Optional[list] = None) -> Any:
                    return i.intercept(m, req, metadata)
                return wrapped
            method = make_wrapped(method, interceptor)

        try:
            start = time.perf_counter()
            response = method(request, timeout=timeout, metadata=metadata or [])
            duration = time.perf_counter() - start
            logger.debug("gRPC call %s completed in %.3fs", method_name, duration)
            return response
        except Exception as e:
            logger.error("gRPC call %s failed: %s", method_name, e)
            raise


class RetryPolicy:
    """gRPC retry policy configuration."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_backoff_ms: int = 100,
        max_backoff_ms: int = 30000,
        backoff_multiplier: float = 2.0,
        retryable_status_codes: Optional[list[int]] = None,
    ) -> None:
        self.max_attempts = max_attempts
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.retryable_status_codes = retryable_status_codes or [1, 4, 8, 10, 13, 14]

    def to_grpc_options(self) -> list[tuple[str, Any]]:
        """Convert retry policy to gRPC channel options."""
        return [
            ("grpc.service_config", self._build_service_config()),
        ]

    def _build_service_config(self) -> str:
        import json
        config = {
            "methodConfig": [{
                "name": [{"service": ""}],
                "retryPolicy": {
                    "maxAttempts": self.max_attempts,
                    "initialBackoff": f"{self.initial_backoff_ms}ms",
                    "maxBackoff": f"{self.max_backoff_ms}ms",
                    "backoffMultiplier": self.backoff_multiplier,
                    "retryableStatusCodes": ["OK" if c == 0 else str(c) for c in self.retryable_status_codes],
                },
            }],
        }
        return json.dumps(config)


class GrpcServer:
    """gRPC server builder and manager."""

    def __init__(self, port: int = 50051) -> None:
        self.port = port
        self._server: Any = None
        self._servicers: list[Any] = []
        self._interceptors: list[Any] = []

    def add_service(self, servicer: Any, add_servicer_func: Callable) -> "GrpcServer":
        """Add a service to the server."""
        self._servicers.append((servicer, add_servicer_func))
        return self

    def add_insecure_port(self, port: Optional[int] = None) -> "GrpcServer":
        """Configure insecure port binding."""
        self.port = port or self.port
        return self

    def start(self) -> bool:
        """Start the gRPC server."""
        try:
            import grpc
            from concurrent import futures

            self._server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=10),
                options=[
                    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                    ("grpc.max_send_message_length", 100 * 1024 * 1024),
                ],
            )

            for servicer, add_func in self._servicers:
                add_func(servicer, self._server)

            self._server.add_insecure_port(f"[::]:{self.port}")
            self._server.start()
            logger.info("gRPC server started on port %d", self.port)
            return True
        except ImportError:
            logger.warning("grpcio not installed")
            return False
        except Exception as e:
            logger.error("Failed to start gRPC server: %s", e)
            return False

    def wait_for_termination(self) -> None:
        """Wait for the server to terminate."""
        if self._server:
            self._server.wait_for_termination()

    def stop(self, grace: float = 5.0) -> None:
        """Stop the gRPC server."""
        if self._server:
            self._server.stop(grace=grace)
            logger.info("gRPC server stopped")
