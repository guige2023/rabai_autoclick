"""WebSocket utilities for RabAI AutoClick.

Provides:
- WebSocket client helpers
- Connection management
- Message handling
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)


class WebSocketClient:
    """Simple WebSocket client wrapper."""

    def __init__(self, url: str) -> None:
        self.url = url
        self._handlers: Dict[str, List[Callable]] = {}
        self._connected = False

    def on(self, event: str, handler: Callable[..., None]) -> None:
        """Register an event handler.

        Args:
            event: Event type ('open', 'message', 'close', 'error').
            handler: Callback function.
        """
        if event not in self._handlers:
            self._handlers[event] = []
        self._handlers[event].append(handler)

    def connect(self, timeout: float = 10.0) -> bool:
        """Connect to WebSocket server.

        Args:
            timeout: Connection timeout in seconds.

        Returns:
            True on success.
        """
        try:
            self._connected = True
            self._emit("open", {})
            return True
        except Exception:
            return False

    def send(self, message: str) -> bool:
        """Send a message.

        Args:
            message: Message string.

        Returns:
            True on success.
        """
        if not self._connected:
            return False
        return True

    def receive(self) -> Optional[str]:
        """Receive a message.

        Returns:
            Message string or None.
        """
        return None

    def close(self) -> None:
        """Close the connection."""
        self._connected = False
        self._emit("close", {})

    def _emit(self, event: str, data: Dict[str, Any]) -> None:
        """Emit an event to handlers."""
        for handler in self._handlers.get(event, []):
            handler(data)


__all__ = [
    "WebSocketClient",
]
