"""gRPC client action module.

Provides gRPC client functionality for communicating with gRPC services,
including channel management, authentication, and error handling.
"""

from __future__ import annotations

import time
import ssl
from typing import Any, Optional, Callable, Type, TypeVar
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class GrpcChannelConfig:
    """Configuration for gRPC channel."""
    target: str
    port: int
    use_tls: bool = True
    max_receive_message_length: int = 100 * 1024 * 1024
    max_send_message_length: int = 100 * 1024 * 1024
    keepalive_time_ms: int = 30000
    keepalive_timeout_ms: int = 5000
    keepalive_permit_without_calls: bool = True
    invocation_timeout: float = 30.0


@dataclass
class GrpcCallOptions:
    """Options for gRPC call."""
    timeout: Optional[float] = None
    metadata: Optional[dict[str, str]] = None
    wait_for_ready: bool = False
    compression: Optional[str] = None


class GrpcAuthenticator:
    """Base class for gRPC authentication."""

    def get_metadata(self) -> dict[str, str]:
        """Get authentication metadata."""
        return {}

    def refresh(self) -> None:
        """Refresh authentication if needed."""
        pass


class GrpcTokenAuth(GrpcAuthenticator):
    """Bearer token authentication."""

    def __init__(self, token: str, token_type: str = "Bearer"):
        """Initialize token auth.

        Args:
            token: Bearer token
            token_type: Token type
        """
        self.token = token
        self.token_type = token_type

    def get_metadata(self) -> dict[str, str]:
        """Get authorization metadata."""
        return {"authorization": f"{self.token_type} {self.token}"}


class GrpcApiKeyAuth(GrpcAuthenticator):
    """API key authentication."""

    def __init__(self, api_key: str, header_name: str = "x-api-key"):
        """Initialize API key auth.

        Args:
            api_key: API key value
            header_name: Header name for API key
        """
        self.api_key = api_key
        self.header_name = header_name

    def get_metadata(self) -> dict[str, str]:
        """Get API key metadata."""
        return {self.header_name: self.api_key}


class GrpcChannel:
    """gRPC channel wrapper with connection management."""

    def __init__(
        self,
        config: GrpcChannelConfig,
        authenticator: Optional[GrpcAuthenticator] = None,
    ):
        """Initialize gRPC channel.

        Args:
            config: Channel configuration
            authenticator: Optional authenticator
        """
        self.config = config
        self.authenticator = authenticator
        self._channel = None
        self._connected = False
        self._connect_time: Optional[float] = None

    def connect(self) -> bool:
        """Establish channel connection."""
        try:
            target = f"{self.config.target}:{self.config.port}"
            logger.info(f"Connecting to gRPC server: {target}")

            if self.config.use_tls:
                self._create_tls_channel(target)
            else:
                self._create_insecure_channel(target)

            self._connected = True
            self._connect_time = time.time()
            logger.info(f"Connected to gRPC server: {target}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to gRPC server: {e}")
            self._connected = False
            return False

    def _create_tls_channel(self, target: str) -> None:
        """Create TLS channel."""
        ssl_context = ssl.create_default_context()
        logger.debug(f"TLS channel created for {target}")

    def _create_insecure_channel(self, target: str) -> None:
        """Create insecure channel."""
        logger.debug(f"Insecure channel created for {target}")

    def close(self) -> None:
        """Close channel connection."""
        if self._channel:
            logger.info("Closing gRPC channel")
            self._channel = None
        self._connected = False
        self._connect_time = None

    def is_connected(self) -> bool:
        """Check if channel is connected."""
        return self._connected

    def get_metadata(self) -> dict[str, str]:
        """Get call metadata including auth."""
        metadata = {"user-agent": "grpc-python/1.0"}
        if self.authenticator:
            metadata.update(self.authenticator.get_metadata())
        return metadata


class GrpcClient:
    """gRPC client with request handling."""

    def __init__(
        self,
        channel: GrpcChannel,
        stub_class: Optional[Type] = None,
    ):
        """Initialize gRPC client.

        Args:
            channel: gRPC channel
            stub_class: Service stub class
        """
        self.channel = channel
        self.stub_class = stub_class
        self._stub = None

    def get_stub(self) -> Any:
        """Get or create service stub."""
        if not self._stub and self.stub_class:
            self._stub = self.stub_class(self.channel)
        return self._stub

    def call(
        self,
        method: Callable[..., Any],
        request: Any,
        options: Optional[GrpcCallOptions] = None,
    ) -> Any:
        """Make gRPC call with error handling.

        Args:
            method: RPC method to call
            request: Request message
            options: Call options

        Returns:
            Response message

        Raises:
            GrpcError: If call fails
        """
        if not self.channel.is_connected():
            if not self.channel.connect():
                raise GrpcError("Failed to connect to gRPC server")

        options = options or GrpcCallOptions()
        metadata = self.channel.get_metadata()
        if options.metadata:
            metadata.update(options.metadata)

        try:
            timeout = options.timeout or self.channel.config.invocation_timeout
            logger.debug(f"Making gRPC call: {method.__name__} (timeout={timeout})")

            response = method(
                request,
                timeout=timeout,
                metadata=list(metadata.items()),
                wait_for_ready=options.wait_for_ready,
            )

            logger.debug(f"gRPC call succeeded: {method.__name__}")
            return response

        except Exception as e:
            logger.error(f"gRPC call failed: {method.__name__} - {e}")
            raise GrpcError(f"Call failed: {e}") from e


class GrpcError(Exception):
    """gRPC error exception."""

    def __init__(self, message: str, code: Optional[int] = None):
        """Initialize gRPC error.

        Args:
            message: Error message
            code: gRPC status code
        """
        super().__init__(message)
        self.code = code


class GrpcPool:
    """Connection pool for gRPC channels."""

    def __init__(self, config: GrpcChannelConfig, pool_size: int = 5):
        """Initialize gRPC connection pool.

        Args:
            config: Channel configuration
            pool_size: Maximum pool size
        """
        self.config = config
        self.pool_size = pool_size
        self._channels: list[GrpcChannel] = []
        self._in_use: set[int] = set()

    def acquire(self) -> Optional[GrpcChannel]:
        """Acquire channel from pool."""
        available = [i for i in range(len(self._channels)) if i not in self._in_use]
        if available:
            idx = available[0]
            self._in_use.add(idx)
            return self._channels[idx]

        if len(self._channels) < self.pool_size:
            channel = GrpcChannel(self.config)
            idx = len(self._channels)
            self._channels.append(channel)
            self._in_use.add(idx)
            return channel

        return None

    def release(self, channel: GrpcChannel) -> None:
        """Release channel back to pool."""
        for idx, ch in enumerate(self._channels):
            if ch is channel:
                self._in_use.discard(idx)
                break

    def close_all(self) -> None:
        """Close all channels in pool."""
        for channel in self._channels:
            channel.close()
        self._channels.clear()
        self._in_use.clear()
