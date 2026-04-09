"""Automation Signal Action Module.

Provides signal-based coordination for automation workflows including
event signaling, condition variables, and pub/sub messaging.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """Signal types."""
    EVENT = "event"
    CONDITION = "condition"
    BROADCAST = "broadcast"
    TOPIC = "topic"


@dataclass
class Signal:
    """Represents a signal/event."""
    signal_id: str
    name: str
    signal_type: SignalType
    data: Any = None
    sender_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class SignalHandler:
    """Handler for processing signals."""

    def __init__(
        self,
        name: str,
        callback: Callable[[Signal], Any],
        filter_fn: Optional[Callable[[Signal], bool]] = None
    ):
        self.name = name
        self.callback = callback
        self.filter_fn = filter_fn or (lambda s: True)
        self.call_count = 0
        self.last_called: Optional[float] = None

    async def handle(self, signal: Signal) -> Any:
        """Handle a signal."""
        if not self.filter_fn(signal):
            return None

        self.call_count += 1
        self.last_called = time.time()

        result = self.callback(signal)
        if asyncio.iscoroutine(result):
            result = await result
        return result


class SignalBus:
    """Signal bus for pub/sub messaging."""

    def __init__(self):
        self._handlers: Dict[str, List[SignalHandler]] = {}
        self._signal_history: List[Signal] = []
        self._max_history = 1000
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        signal_name: str,
        handler: SignalHandler
    ) -> None:
        """Subscribe a handler to a signal."""
        async with self._lock:
            if signal_name not in self._handlers:
                self._handlers[signal_name] = []
            self._handlers[signal_name].append(handler)

    async def unsubscribe(
        self,
        signal_name: str,
        handler_name: str
    ) -> bool:
        """Unsubscribe a handler from a signal."""
        async with self._lock:
            handlers = self._handlers.get(signal_name, [])
            for i, h in enumerate(handlers):
                if h.name == handler_name:
                    handlers.pop(i)
                    return True
        return False

    async def emit(
        self,
        name: str,
        signal_type: SignalType,
        data: Any = None,
        sender_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Emit a signal to all subscribed handlers."""
        signal = Signal(
            signal_id=str(uuid.uuid4())[:8],
            name=name,
            signal_type=signal_type,
            data=data,
            sender_id=sender_id,
            metadata=metadata or {}
        )

        async with self._lock:
            self._signal_history.append(signal)
            if len(self._signal_history) > self._max_history:
                self._signal_history.pop(0)

            handlers = self._handlers.get(name, []).copy()

        results = []
        for handler in handlers:
            try:
                result = await handler.handle(signal)
                results.append(result)
            except Exception as e:
                logger.exception(f"Signal handler {handler.name} error: {e}")

        return results

    async def emit_broadcast(
        self,
        data: Any,
        sender_id: Optional[str] = None
    ) -> int:
        """Emit a broadcast signal to all handlers."""
        async with self._lock:
            all_handlers = sum(self._handlers.values(), [])

        count = 0
        for handler in all_handlers:
            try:
                signal = Signal(
                    signal_id=str(uuid.uuid4())[:8],
                    name="*",
                    signal_type=SignalType.BROADCAST,
                    data=data,
                    sender_id=sender_id
                )
                await handler.handle(signal)
                count += 1
            except Exception as e:
                logger.exception(f"Broadcast handler {handler.name} error: {e}")

        return count

    async def get_history(
        self,
        signal_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get signal history."""
        async with self._lock:
            history = self._signal_history[-limit:]
            if signal_name:
                history = [s for s in history if s.name == signal_name]

            return [
                {
                    "signal_id": s.signal_id,
                    "name": s.name,
                    "signal_type": s.signal_type.value,
                    "sender_id": s.sender_id,
                    "timestamp": s.timestamp
                }
                for s in history
            ]

    async def get_subscribers(self, signal_name: str) -> List[str]:
        """Get list of subscriber names for a signal."""
        async with self._lock:
            handlers = self._handlers.get(signal_name, [])
            return [h.name for h in handlers]


class ConditionVariable:
    """Async condition variable for synchronization."""

    def __init__(self, name: str):
        self._name = name
        self._condition = asyncio.Condition()
        self._waiters: Set[asyncio.Future] = set()
        self._notified_count = 0

    async def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for the condition to be notified."""
        async with self._condition:
            try:
                if timeout:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=timeout
                    )
                else:
                    await self._condition.wait()
                return True
            except asyncio.TimeoutError:
                return False

    async def notify(self, n: int = 1) -> None:
        """Notify n waiters."""
        async with self._condition:
            self._notified_count += n
            self._condition.notify(n)

    async def notify_all(self) -> None:
        """Notify all waiters."""
        async with self._condition:
            self._notified_count += len(self._waiters)
            self._condition.notify_all()

    @property
    def notified_count(self) -> int:
        """Return number of times notified."""
        return self._notified_count


class EventBus:
    """Simple async event bus."""

    def __init__(self):
        self._events: Dict[str, asyncio.Event] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def create_event(self, name: str) -> None:
        """Create a named event."""
        async with self._lock:
            if name not in self._events:
                self._events[name] = asyncio.Event()
                self._locks[name] = asyncio.Lock()

    async def set_event(self, name: str) -> bool:
        """Set a named event."""
        event = self._events.get(name)
        if event:
            event.set()
            return True
        return False

    async def clear_event(self, name: str) -> bool:
        """Clear a named event."""
        event = self._events.get(name)
        if event:
            event.clear()
            return True
        return False

    async def wait_for(
        self,
        name: str,
        timeout: Optional[float] = None
    ) -> bool:
        """Wait for a named event."""
        event = self._events.get(name)
        if not event:
            return False

        try:
            if timeout:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
            return True
        except asyncio.TimeoutError:
            return False


class AutomationSignalAction:
    """Main action class for signal-based automation."""

    def __init__(self):
        self._bus = SignalBus()
        self._conditions: Dict[str, ConditionVariable] = {}
        self._event_bus = EventBus()

    async def create_signal(
        self,
        name: str,
        signal_type: SignalType = SignalType.EVENT,
        handler_name: Optional[str] = None,
        handler_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Create a named signal with optional handler."""
        if handler_name and handler_callback:
            handler = SignalHandler(
                name=handler_name,
                callback=handler_callback
            )
            await self._bus.subscribe(name, handler)
            return {"success": True, "signal": name, "handler": handler_name}
        return {"success": True, "signal": name}

    async def emit_signal(
        self,
        name: str,
        data: Any = None,
        signal_type: SignalType = SignalType.EVENT,
        sender_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Emit a signal."""
        results = await self._bus.emit(
            name=name,
            signal_type=signal_type,
            data=data,
            sender_id=sender_id
        )
        return {
            "success": True,
            "signal": name,
            "handler_count": len(results)
        }

    async def subscribe(
        self,
        signal_name: str,
        handler_name: str,
        callback: Callable
    ) -> Dict[str, Any]:
        """Subscribe to a signal."""
        handler = SignalHandler(name=handler_name, callback=callback)
        await self._bus.subscribe(signal_name, handler)
        return {"success": True}

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation signal action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "emit")

        if operation == "emit":
            return await self.emit_signal(
                name=context.get("signal", ""),
                data=context.get("data"),
                signal_type=SignalType(context.get("signal_type", "event"))
            )

        elif operation == "broadcast":
            count = await self._bus.emit_broadcast(
                data=context.get("data"),
                sender_id=context.get("sender_id")
            )
            return {"success": True, "handler_count": count}

        elif operation == "subscribe":
            return await self.subscribe(
                signal_name=context.get("signal", ""),
                handler_name=context.get("handler_name", ""),
                callback=lambda s: s
            )

        elif operation == "unsubscribe":
            success = await self._bus.unsubscribe(
                signal_name=context.get("signal", ""),
                handler_name=context.get("handler_name", "")
            )
            return {"success": success}

        elif operation == "history":
            history = await self._bus.get_history(
                signal_name=context.get("signal"),
                limit=context.get("limit", 100)
            )
            return {"success": True, "history": history}

        elif operation == "subscribers":
            subs = await self._bus.get_subscribers(
                signal_name=context.get("signal", "")
            )
            return {"success": True, "subscribers": subs}

        elif operation == "wait":
            cond_name = context.get("condition", "default")
            if cond_name not in self._conditions:
                self._conditions[cond_name] = ConditionVariable(cond_name)

            timeout = context.get("timeout")
            result = await self._conditions[cond_name].wait(timeout=timeout)
            return {"success": True, "notified": result}

        elif operation == "notify":
            cond_name = context.get("condition", "default")
            n = context.get("n", 1)
            if cond_name in self._conditions:
                await self._conditions[cond_name].notify(n=n)
            return {"success": True}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
