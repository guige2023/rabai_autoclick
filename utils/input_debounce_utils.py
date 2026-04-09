"""Input Debounce Utilities.

Debounces rapid input events to prevent flooding.

Example:
    >>> from input_debounce_utils import InputDebouncer
    >>> deb = InputDebouncer(wait=0.3)
    >>> deb.debounce("click", callback)
    >>> deb.debounce("click")  # rapid - ignored
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass
class DebouncedCall:
    """A debounced function call."""
    event_type: str
    data: Dict[str, Any]
    timestamp: float
    call_count: int = 1


class InputDebouncer:
    """Debounces rapid input events."""

    def __init__(self, wait: float = 0.3):
        """Initialize debouncer.

        Args:
            wait: Seconds to wait before calling.
        """
        self.wait = wait
        self._pending: Dict[str, DebouncedCall] = {}
        self._handlers: Dict[str, Callable[..., None]] = {}

    def debounce(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a debounced event.

        Args:
            event_type: Type of event.
            data: Event data.
        """
        now = time.time()
        key = event_type

        if key in self._pending:
            self._pending[key].call_count += 1
            self._pending[key].timestamp = now
            if data:
                self._pending[key].data.update(data)
        else:
            self._pending[key] = DebouncedCall(
                event_type=event_type,
                data=data or {},
                timestamp=now,
            )

    def flush(self, event_type: Optional[str] = None) -> None:
        """Flush pending events.

        Args:
            event_type: Specific type to flush, or None for all.
        """
        now = time.time()
        to_flush = []

        if event_type:
            if event_type in self._pending:
                to_flush = [(event_type, self._pending[event_type])]
        else:
            to_flush = [(k, v) for k, v in self._pending.items()]

        for key, call in to_flush:
            if now - call.timestamp >= self.wait:
                handler = self._handlers.get(key)
                if handler:
                    handler(call.data, call.call_count)
                del self._pending[key]

    def on(self, event_type: str, handler: Callable[..., None]) -> None:
        """Register handler for event type.

        Args:
            event_type: Event type.
            handler: Callback function.
        """
        self._handlers[event_type] = handler

    def cancel(self, event_type: str) -> None:
        """Cancel a pending event.

        Args:
            event_type: Event to cancel.
        """
        self._pending.pop(event_type, None)

    def get_pending(self, event_type: str) -> Optional[DebouncedCall]:
        """Get pending call for event type.

        Args:
            event_type: Event type.

        Returns:
            DebouncedCall or None.
        """
        return self._pending.get(event_type)
