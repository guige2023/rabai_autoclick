"""Event emitter pattern utilities.

Provides a generic event emitter for pub/sub communication
in automation workflows and state management.
"""

from typing import Any, Callable, Dict, List


EventHandler = Callable[..., None]


class EventEmitter:
    """Generic event emitter with support for once handlers.

    Example:
        emitter = EventEmitter()
        emitter.on("click", lambda x: print(f"Clicked: {x}"))
        emitter.emit("click", "button1")
        emitter.off("click", handler)
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._once_handlers: Dict[str, List[EventHandler]] = {}

    def on(self, event: str, handler: EventHandler) -> None:
        """Register an event handler.

        Args:
            event: Event name.
            handler: Callback function.
        """
        if event not in self._handlers:
            self._handlers[event] = []
        if handler not in self._handlers[event]:
            self._handlers[event].append(handler)

    def once(self, event: str, handler: EventHandler) -> None:
        """Register a one-time event handler.

        Args:
            event: Event name.
            handler: Callback function.
        """
        if event not in self._once_handlers:
            self._once_handlers[event] = []
        if handler not in self._once_handlers[event]:
            self._once_handlers[event].append(handler)

    def off(self, event: str, handler: EventHandler) -> None:
        """Remove an event handler.

        Args:
            event: Event name.
            handler: Callback function to remove.
        """
        if event in self._handlers:
            self._handlers[event] = [h for h in self._handlers[event] if h != handler]
        if event in self._once_handlers:
            self._once_handlers[event] = [h for h in self._once_handlers[event] if h != handler]

    def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Emit an event to all registered handlers.

        Args:
            event: Event name.
            *args: Positional arguments to pass to handlers.
            **kwargs: Keyword arguments to pass to handlers.
        """
        if event in self._handlers:
            for handler in self._handlers[event]:
                handler(*args, **kwargs)
        if event in self._once_handlers:
            once = self._once_handlers.pop(event, [])
            for handler in once:
                handler(*args, **kwargs)

    def clear(self, event: Optional[str] = None) -> None:
        """Clear handlers for an event or all events.

        Args:
            event: Event name. If None, clear all events.
        """
        if event is None:
            self._handlers.clear()
            self._once_handlers.clear()
        else:
            self._handlers.pop(event, None)
            self._once_handlers.pop(event, None)

    def listener_count(self, event: str) -> int:
        """Count handlers for an event.

        Args:
            event: Event name.

        Returns:
            Number of registered handlers.
        """
        return len(self._handlers.get(event, [])) + len(self._once_handlers.get(event, []))

    def event_names(self) -> List[str]:
        """List all events with registered handlers.

        Returns:
            List of event names.
        """
        return list(set(list(self._handlers.keys()) + list(self._once_handlers.keys())))
