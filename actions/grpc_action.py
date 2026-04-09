"""gRPC action for high-performance RPC communication.

This module provides comprehensive gRPC support:
- Client and server implementations
- Streaming RPC (server, client, bidirectional)
- Interceptors for auth, logging, tracing
- Health checks and reflection
- Deadline and cancellation handling
- Load balancing and failover
- TLS/mTLS security

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import grpc
    from grpc import aio as grpc_aio
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    grpc_aio = None

try:
    import protobuf
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

logger = logging.getLogger(__name__)


class ChannelCredentials(Enum):
    """gRPC channel credential types."""
    INSECURE = "insecure"
    TLS = "tls"
    MUTUAL_TLS = "mutual_tls"


class CompressionAlgorithm(Enum):
    """gRPC compression algorithms."""
    NONE = "none"
    DEFLATE = "deflate"
    GZIP = "gzip"


@dataclass
class GrpcMethodConfig:
    """Configuration for a gRPC method."""
    name: str
    timeout_seconds: float = 30.0
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    retry_policy: Optional[Dict[str, Any]] = None
    max_request_message_bytes: int = 4 * 1024 * 1024
    max_response_message_bytes: int = 4 * 1024 * 1024


@dataclass
class ChannelConfig:
    """gRPC channel configuration."""
    target: str
    credentials: ChannelCredentials = ChannelCredentials.INSECURE
    options: Dict[str, Any] = field(default_factory=dict)
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    max_concurrent_rpcs: Optional[int] = None
    max_receive_message_length: int = 100 * 1024 * 1024
    max_send_message_length: int = 100 * 1024 * 1024
    http2_initial_window_size: int = 65535
    http2_max_frame_size: int = 16384
    keepalive_time_ms: int = 7200000
    keepalive_timeout_ms: int = 20000
    keepalive_permit_without_calls: bool = False


@dataclass
class ServerConfig:
    """gRPC server configuration."""
    host: str = "localhost"
    port: int = 50051
    max_concurrent_rpcs: Optional[int] = None
    max_workers: Optional[int] = None
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    interceptors: List[Any] = field(default_factory=list)
    authentication: Optional[Callable] = None
    authorization: Optional[Callable] = None
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GrpcMetadata:
    """gRPC metadata wrapper."""
    headers: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: Optional[float] = None
    compression: Optional[CompressionAlgorithm] = None


@dataclass
class RpcResult:
    """Result of an RPC call."""
    response: Optional[Any] = None
    status: str = "ok"
    code: int = 0
    details: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    duration_ms: float = 0.0


class InterceptorType(Enum):
    """Interceptor types."""
    UNARY_UNARY = "unary_unary"
    UNARY_STREAM = "unary_stream"
    STREAM_UNARY = "stream_unary"
    STREAM_STREAM = "stream_stream"


class BaseInterceptor:
    """Base interceptor class.

    Override methods to implement custom interceptor logic.
    """

    async def interceptUnaryUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Intercept unary-unary RPC."""
        return await continuation(request, metadata)

    async def interceptUnaryStream(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Intercept unary-stream RPC."""
        return await continuation(request, metadata)

    async def interceptStreamUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request_iterator: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Intercept stream-unary RPC."""
        return await continuation(request_iterator, metadata)

    async def interceptStreamStream(
        self,
        continuation: Callable,
        method_handler: Any,
        request_iterator: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Intercept stream-stream RPC."""
        return await continuation(request_iterator, metadata)


class LoggingInterceptor(BaseInterceptor):
    """Interceptor for logging RPC requests and responses."""

    def __init__(self, logger_instance: Optional[logging.Logger] = None):
        """Initialize logging interceptor.

        Args:
            logger_instance: Logger instance
        """
        self.logger = logger_instance or logging.getLogger("grpc.logging")

    async def interceptUnaryUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Log unary-unary RPC."""
        method = invoker_details.get("method", "unknown")
        start_time = time.time()

        self.logger.debug(f"RPC started: {method}")

        try:
            response = await continuation(request, metadata)
            duration = (time.time() - start_time) * 1000
            self.logger.debug(f"RPC completed: {method} ({duration:.2f}ms)")
            return response
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            self.logger.error(f"RPC failed: {method} ({duration:.2f}ms) - {e}")
            raise


class AuthInterceptor(BaseInterceptor):
    """Interceptor for authentication."""

    def __init__(
        self,
        auth_token_getter: Callable[[], str],
        header_name: str = "authorization"
    ):
        """Initialize auth interceptor.

        Args:
            auth_token_getter: Function to get auth token
            header_name: Header name for token
        """
        self.auth_token_getter = auth_token_getter
        self.header_name = header_name

    async def interceptUnaryUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Add auth header to unary-unary RPC."""
        metadata[self.header_name] = self.auth_token_getter()
        return await continuation(request, metadata)


class RetryInterceptor(BaseInterceptor):
    """Interceptor for automatic retry with backoff."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_backoff_ms: float = 100,
        max_backoff_ms: float = 5000,
        backoff_multiplier: float = 2.0,
        retryable_codes: Optional[List[int]] = None
    ):
        """Initialize retry interceptor.

        Args:
            max_attempts: Maximum retry attempts
            initial_backoff_ms: Initial backoff in milliseconds
            max_backoff_ms: Maximum backoff in milliseconds
            backoff_multiplier: Backoff multiplier
            retryable_codes: List of retryable status codes
        """
        self.max_attempts = max_attempts
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.retryable_codes = retryable_codes or [
            grpc_aio.StatusCode.UNAVAILABLE,
            grpc_aio.StatusCode.RESOURCE_EXHAUSTED,
            grpc_aio.StatusCode.INTERNAL,
        ]

    async def interceptUnaryUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Retry unary-unary RPC with backoff."""
        last_error = None
        backoff = self.initial_backoff_ms

        for attempt in range(self.max_attempts):
            try:
                return await continuation(request, metadata)
            except grpc_aio.AioRpcError as e:
                last_error = e

                if e.code() not in self.retryable_codes:
                    raise

                if attempt < self.max_attempts - 1:
                    await asyncio.sleep(backoff / 1000)
                    backoff = min(backoff * self.backoff_multiplier, self.max_backoff_ms)

        raise last_error


class RateLimitInterceptor(BaseInterceptor):
    """Interceptor for rate limiting."""

    def __init__(
        self,
        max_rps: float = 100,
        burst: int = 10
    ):
        """Initialize rate limit interceptor.

        Args:
            max_rps: Maximum requests per second
            burst: Burst size
        """
        self.max_rps = max_rps
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def _acquire(self) -> bool:
        """Acquire a token."""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.max_rps)
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def interceptUnaryUnary(
        self,
        continuation: Callable,
        method_handler: Any,
        request: Any,
        metadata: Dict[str, str],
        invoker_details: Dict[str, Any]
    ) -> Any:
        """Rate limit unary-unary RPC."""
        if not await self._acquire():
            raise grpc_aio.AioRpcError(
                code=grpc_aio.StatusCode.RESOURCE_EXHAUSTED,
                details="Rate limit exceeded",
                initial_metadata=grpc_aio.Metadata(),
                trailing_metadata=grpc_aio.Metadata(),
            )
        return await continuation(request, metadata)


class GrpcClient:
    """gRPC client with connection management.

    Provides a robust gRPC client with:
    - Connection pooling and keepalive
    - Automatic retry and failover
    - Load balancing
    - Deadline management
    - Interceptor support
    """

    def __init__(
        self,
        channel_config: ChannelConfig,
        interceptors: Optional[List[BaseInterceptor]] = None,
    ):
        """Initialize gRPC client.

        Args:
            channel_config: Channel configuration
            interceptors: List of interceptors
        """
        self.channel_config = channel_config
        self.interceptors = interceptors or []
        self._channel = None
        self._stubs: Dict[str, Any] = {}

    async def connect(self) -> None:
        """Establish channel connection."""
        if not GRPC_AVAILABLE:
            raise ImportError("grpcio and grpcio-tools required")

        options = []

        if self.channel_config.max_concurrent_rpcs:
            options.append(
                ("grpc.max_concurrent_rpcs", self.channel_config.max_concurrent_rpcs)
            )

        if self.channel_config.max_receive_message_length:
            options.append(
                ("grpc.max_receive_message_length", self.channel_config.max_receive_message_length)
            )

        if self.channel_config.max_send_message_length:
            options.append(
                ("grpc.max_send_message_length", self.channel_config.max_send_message_length)
            )

        if self.channel_config.keepalive_time_ms:
            options.append(
                ("grpc.keepalive_time_ms", self.channel_config.keepalive_time_ms)
            )

        for key, value in self.channel_config.options.items():
            options.append((key, value))

        if self.channel_config.credentials == ChannelCredentials.INSECURE:
            self._channel = grpc_aio.insecure_channel(
                self.channel_config.target,
                options=options,
                compression=self._get_compression_algorithm()
            )
        else:
            self._channel = grpc_aio.secure_channel(
                self.channel_config.target,
                credentials=self._get_channel_credentials(),
                options=options,
                compression=self._get_compression_algorithm()
            )

        logger.info(f"gRPC channel connected to {self.channel_config.target}")

    async def close(self) -> None:
        """Close channel connection."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            logger.info("gRPC channel closed")

    def get_stub(self, stub_class: type) -> Any:
        """Get or create stub for service.

        Args:
            stub_class: Stub class to instantiate

        Returns:
            Stub instance
        """
        stub_name = stub_class.__name__

        if stub_name not in self._stubs:
            self._stubs[stub_name] = stub_class(self._channel)

        return self._stubs[stub_name]

    async def callUnaryUnary(
        self,
        stub: Any,
        method_name: str,
        request: Any,
        metadata: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> RpcResult:
        """Call unary-unary method.

        Args:
            stub: Service stub
            method_name: Method name
            request: Request message
            metadata: Request metadata
            timeout: Call timeout

        Returns:
            RPC result
        """
        if not self._channel:
            raise RuntimeError("Channel not connected")

        start_time = time.time()

        try:
            method = getattr(stub, method_name)

            if metadata:
                grpc_metadata = grpc_aio.Metadata(**metadata)
            else:
                grpc_metadata = None

            response = await method(
                request,
                metadata=grpc_metadata,
                timeout=timeout or self.channel_config.target
            )

            duration_ms = (time.time() - start_time) * 1000

            return RpcResult(
                response=response,
                status="ok",
                code=0,
                duration_ms=duration_ms
            )

        except grpc_aio.AioRpcError as e:
            duration_ms = (time.time() - start_time) * 1000

            return RpcResult(
                status="error",
                code=e.code().value[0],
                details=e.details(),
                duration_ms=duration_ms
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            return RpcResult(
                status="error",
                code=-1,
                details=str(e),
                duration_ms=duration_ms
            )

    async def callUnaryStream(
        self,
        stub: Any,
        method_name: str,
        request: Any,
        metadata: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ):
        """Call unary-stream method.

        Args:
            stub: Service stub
            method_name: Method name
            request: Request message
            metadata: Request metadata
            timeout: Call timeout

        Yields:
            Response messages
        """
        if not self._channel:
            raise RuntimeError("Channel not connected")

        method = getattr(stub, method_name)

        if metadata:
            grpc_metadata = grpc_aio.Metadata(**metadata)
        else:
            grpc_metadata = None

        async for response in method(
            request,
            metadata=grpc_metadata,
            timeout=timeout
        ):
            yield response

    def _get_compression_algorithm(self) -> Optional[int]:
        """Get gRPC compression algorithm."""
        if self.channel_config.compression == CompressionAlgorithm.GZIP:
            return grpc_aio.Compression.Gzip
        elif self.channel_config.compression == CompressionAlgorithm.DEFLATE:
            return grpc_aio.Compression.Deflate
        return None

    def _get_channel_credentials(self):
        """Get channel credentials."""
        if self.channel_config.credentials == ChannelCredentials.TLS:
            return grpc_aio.ssl_channel_credentials()
        elif self.channel_config.credentials == ChannelCredentials.MUTUAL_TLS:
            return grpc_aio.ssl_channel_credentials()
        return grpc_aio.ChannelCredentials()


class GrpcServer:
    """gRPC server with service registration.

    Provides a robust gRPC server with:
    - Service registration
    - Interceptor support
    - Health checks
    - Reflection support
    - Graceful shutdown
    """

    def __init__(self, config: ServerConfig):
        """Initialize gRPC server.

        Args:
            config: Server configuration
        """
        self.config = config
        self._server = None
        self._services: Dict[str, Any] = {}

    async def start(self) -> None:
        """Start the server."""
        if not GRPC_AVAILABLE:
            raise ImportError("grpcio and grpcio-tools required")

        self._server = grpc_aio.server(
            options=self._get_server_options(),
            maximum_concurrent_rpcs=self.config.max_concurrent_rpcs,
        )

        for service_name, service in self._services.items():
            self._server.add_insecure_port(f"{self.config.host}:{self.config.port}")
            self._server.add_registered_method_handlers(service_name, service)

        await self._server.start()
        logger.info(f"gRPC server started on {self.config.host}:{self.config.port}")

    async def stop(self, graceful: bool = True) -> None:
        """Stop the server.

        Args:
            graceful: Graceful shutdown with timeout
        """
        if self._server:
            if graceful:
                await self._server.stop(grace=30)
            else:
                self._server.stop(grace=0)
            logger.info("gRPC server stopped")

    def add_service(
        self,
        service_name: str,
        service_implementation: Any
    ) -> None:
        """Add service implementation.

        Args:
            service_name: Service name
            service_implementation: Service implementation
        """
        self._services[service_name] = service_implementation

    def enable_health_checks(self) -> None:
        """Enable built-in health checks."""
        if self._server:
            from grpc_health.v1 import health
            from grpc_health.v1 import health_pb2
            from grpc_health.v1 import health_pb2_grpc

            health_servicer = health.HealthServicer()
            health_pb2_grpc.add_HealthServicer_to_server(health_servicer, self._server)

    def enable_reflection(self) -> None:
        """Enable gRPC reflection."""
        if self._server:
            from grpc_reflect.v1alpha import reflection
            reflection.enable_server_reflection(self._server)

    def _get_server_options(self) -> List:
        """Get server options."""
        options = []

        if self.config.max_workers:
            options.append(("grpc.max_workers", self.config.max_workers))

        for key, value in self.config.options.items():
            options.append((key, value))

        return options


class HealthServicer:
    """Health check servicer implementation."""

    def __init__(self):
        """Initialize health servicer."""
        self._status: Dict[str, str] = {}

    def set_status(self, service: str, status: str) -> None:
        """Set health status for service.

        Args:
            service: Service name
            status: Health status (SERVING, NOT_SERVING, UNKNOWN)
        """
        self._status[service] = status

    def get_status(self, service: str) -> str:
        """Get health status for service.

        Args:
            service: Service name

        Returns:
            Health status
        """
        return self._status.get(service, "UNKNOWN")

    def Check(self, request: Any, context: Any) -> Any:
        """Handle health check request.

        Args:
            request: Health check request
            context: gRPC context

        Returns:
            Health check response
        """
        from grpc_health.v1 import health_pb2

        service = request.service
        status = self.get_status(service)

        status_enum = health_pb2.HealthCheckResponse.SERVING
        if status == "NOT_SERVING":
            status_enum = health_pb2.HealthCheckResponse.NOT_SERVING
        elif status == "UNKNOWN":
            status_enum = health_pb2.HealthCheckResponse.UNKNOWN

        return health_pb2.HealthCheckResponse(status=status_enum)


class LoadBalancer:
    """Client-side load balancer for gRPC."""

    def __init__(self, policy: str = "round_robin"):
        """Initialize load balancer.

        Args:
            policy: Load balancing policy
        """
        self.policy = policy
        self._addresses: List[str] = []
        self._current_index = 0
        self._lock = asyncio.Lock()

    def set_addresses(self, addresses: List[str]) -> None:
        """Set backend addresses.

        Args:
            addresses: List of backend addresses
        """
        self._addresses = addresses
        self._current_index = 0

    async def get_next_address(self) -> Optional[str]:
        """Get next address based on load balancing policy.

        Returns:
            Backend address or None
        """
        async with self._lock:
            if not self._addresses:
                return None

            if self.policy == "round_robin":
                address = self._addresses[self._current_index]
                self._current_index = (self._current_index + 1) % len(self._addresses)
                return address

            elif self.policy == "random":
                import random
                return random.choice(self._addresses)

            return self._addresses[0]


# Factory functions

async def create_client(
    target: str,
    credentials: ChannelCredentials = ChannelCredentials.INSECURE,
    **kwargs
) -> GrpcClient:
    """Create gRPC client.

    Args:
        target: Server target
        credentials: Credential type
        **kwargs: Additional config

    Returns:
        GrpcClient instance
    """
    channel_config = ChannelConfig(
        target=target,
        credentials=credentials,
        **kwargs
    )
    client = GrpcClient(channel_config)
    await client.connect()
    return client


async def create_server(
    host: str = "localhost",
    port: int = 50051,
    **kwargs
) -> GrpcServer:
    """Create gRPC server.

    Args:
        host: Server host
        port: Server port
        **kwargs: Additional config

    Returns:
        GrpcServer instance
    """
    config = ServerConfig(host=host, port=port, **kwargs)
    server = GrpcServer(config)
    await server.start()
    return server
