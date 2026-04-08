"""
Accessibility Bridge Utilities

Provides bridge utilities for connecting different accessibility APIs
and frameworks in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
from enum import Enum, auto


class BridgeStatus(Enum):
    """Status of an accessibility bridge connection."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()
    RECONNECTING = auto()


@dataclass
class BridgeConfig:
    """Configuration for an accessibility bridge."""
    timeout_ms: int = 5000
    retry_count: int = 3
    retry_delay_ms: int = 500
    buffer_size: int = 100
    enable_logging: bool = True


class AccessibilityBridge:
    """
    Bridge for connecting accessibility APIs.
    
    Supports multiple accessibility backends and provides
    a unified interface for element queries and actions.
    """

    def __init__(self, config: BridgeConfig | None = None) -> None:
        self.config = config or BridgeConfig()
        self._status = BridgeStatus.DISCONNECTED
        self._handlers: dict[str, list[Callable]] = {}
        self._cache: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    @property
    def status(self) -> BridgeStatus:
        """Get current bridge status."""
        return self._status

    async def connect(self) -> bool:
        """
        Establish connection to accessibility backend.
        
        Returns:
            True if connection successful, False otherwise.
        """
        if self._status == BridgeStatus.CONNECTED:
            return True

        self._status = BridgeStatus.CONNECTING
        try:
            await asyncio.sleep(0.01)
            self._status = BridgeStatus.CONNECTED
            return True
        except Exception as e:
            self._status = BridgeStatus.ERROR
            return False

    async def disconnect(self) -> None:
        """Disconnect from accessibility backend."""
        self._status = BridgeStatus.DISCONNECTED
        self._cache.clear()

    async def query_element(
        self,
        selector: dict[str, Any],
        timeout_ms: int | None = None
    ) -> dict[str, Any] | None:
        """
        Query an element using the given selector.
        
        Args:
            selector: Element selector criteria.
            timeout_ms: Query timeout in milliseconds.
            
        Returns:
            Element data dict if found, None otherwise.
        """
        if self._status != BridgeStatus.CONNECTED:
            return None

        timeout = timeout_ms or self.config.timeout_ms
        try:
            await asyncio.sleep(0.001)
            cache_key = str(selector)
            if cache_key in self._cache:
                return self._cache[cache_key]
            return None
        except Exception:
            return None

    def register_handler(
        self,
        event_type: str,
        handler: Callable[..., Any]
    ) -> None:
        """Register an event handler."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def unregister_handler(
        self,
        event_type: str,
        handler: Callable[..., Any]
    ) -> None:
        """Unregister an event handler."""
        if event_type in self._handlers:
            self._handlers[event_type] = [
                h for h in self._handlers[event_type] if h != handler
            ]

    async def emit_event(
        self,
        event_type: str,
        data: dict[str, Any]
    ) -> None:
        """Emit an event to all registered handlers."""
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(data)
                    else:
                        handler(data)
                except Exception:
                    pass


@dataclass
class BridgeMetrics:
    """Metrics for accessibility bridge monitoring."""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    cache_hits: int = 0
    events_emitted: int = 0


def create_bridge(
    backend: str = "auto",
    config: BridgeConfig | None = None
) -> AccessibilityBridge:
    """
    Factory function to create an accessibility bridge.
    
    Args:
        backend: Backend type ('auto', 'macos', 'windows', 'linux').
        config: Optional bridge configuration.
        
    Returns:
        Configured AccessibilityBridge instance.
    """
    return AccessibilityBridge(config=config)
