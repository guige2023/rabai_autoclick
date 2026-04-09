"""Automation Signal Action Module.

Provides signal-based automation with event emission,
handler registration, and signal routing for workflow
coordination and state propagation.

Example:
    >>> from actions.automation.automation_signal_action import SignalBus, Signal
    >>> bus = SignalBus()
    >>> bus.emit("task_complete", data={"task_id": "123"})
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import threading
import uuid


class SignalPriority(Enum):
    """Signal handler priorities."""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class Signal:
    """Signal event representation.
    
    Attributes:
        name: Signal name
        data: Signal payload data
        source: Signal source identifier
        timestamp: Signal emission time
        signal_id: Unique signal identifier
        metadata: Additional metadata
    """
    name: str
    data: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalHandler:
    """Signal handler registration.
    
    Attributes:
        handler_id: Unique handler identifier
        signal_name: Signal to handle
        callback: Handler callback function
        priority: Handler priority
        filter_func: Optional filter function
        description: Handler description
    """
    handler_id: str
    signal_name: str
    callback: Callable[[Signal], None]
    priority: SignalPriority = SignalPriority.NORMAL
    filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None
    description: str = ""
    is_active: bool = True


@dataclass
class SignalRoute:
    """Signal routing rule.
    
    Attributes:
        route_id: Unique route identifier
        source_signal: Source signal name
        target_signal: Target signal name
        transform: Optional data transform function
        condition: Optional routing condition
    """
    route_id: str
    source_signal: str
    target_signal: str
    transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    is_active: bool = True


@dataclass
class SignalStats:
    """Signal statistics.
    
    Attributes:
        total_emitted: Total signals emitted
        total_handled: Total signals handled
        total_routed: Total signals routed
        handlers_registered: Number of registered handlers
        routes_active: Number of active routes
    """
    total_emitted: int = 0
    total_handled: int = 0
    total_routed: int = 0
    handlers_registered: int = 0
    routes_active: int = 0


class SignalBus:
    """Signal-based event bus for automation.
    
    Manages signal emission, handler registration, and
    routing for coordinated automation workflows.
    
    Attributes:
        _handlers: Registered signal handlers
        _routes: Signal routing rules
        _signal_history: Recent signal history
        _stats: Signal statistics
        _lock: Thread safety lock
        _max_history: Maximum history size
    """
    
    def __init__(self, max_history: int = 1000) -> None:
        """Initialize signal bus.
        
        Args:
            max_history: Maximum signal history size
        """
        self._handlers: Dict[str, List[SignalHandler]] = {}
        self._routes: Dict[str, List[SignalRoute]] = {}
        self._signal_history: List[Signal] = []
        self._stats = SignalStats()
        self._lock = threading.RLock()
        self._max_history = max_history
        self._global_handlers: List[SignalHandler] = []
    
    def register_handler(
        self,
        signal_name: str,
        callback: Callable[[Signal], None],
        priority: SignalPriority = SignalPriority.NORMAL,
        filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
        description: str = "",
    ) -> str:
        """Register a signal handler.
        
        Args:
            signal_name: Signal name to handle
            callback: Handler callback
            priority: Handler priority
            filter_func: Optional filter function
            description: Handler description
            
        Returns:
            Handler ID
        """
        handler_id = str(uuid.uuid4())[:8]
        handler = SignalHandler(
            handler_id=handler_id,
            signal_name=signal_name,
            callback=callback,
            priority=priority,
            filter_func=filter_func,
            description=description,
        )
        
        with self._lock:
            if signal_name not in self._handlers:
                self._handlers[signal_name] = []
            self._handlers[signal_name].append(handler)
            self._handlers[signal_name].sort(key=lambda h: h.priority.value, reverse=True)
            self._stats.handlers_registered += 1
        
        return handler_id
    
    def register_global_handler(
        self,
        callback: Callable[[Signal], None],
        priority: SignalPriority = SignalPriority.NORMAL,
        signal_names: Optional[List[str]] = None,
        description: str = "",
    ) -> str:
        """Register a global handler for all signals.
        
        Args:
            callback: Handler callback
            priority: Handler priority
            signal_names: Optional filter to specific signals
            description: Handler description
            
        Returns:
            Handler ID
        """
        handler_id = str(uuid.uuid4())[:8]
        handler = SignalHandler(
            handler_id=handler_id,
            signal_name="*",
            callback=callback,
            priority=priority,
            description=description,
        )
        
        with self._lock:
            self._global_handlers.append(handler)
            self._global_handlers.sort(key=lambda h: h.priority.value, reverse=True)
            self._stats.handlers_registered += 1
        
        return handler_id
    
    def unregister_handler(self, handler_id: str) -> bool:
        """Unregister a handler.
        
        Args:
            handler_id: Handler ID to remove
            
        Returns:
            True if handler was found and removed
        """
        with self._lock:
            # Check specific handlers
            for signal_name, handlers in self._handlers.items():
                for handler in handlers:
                    if handler.handler_id == handler_id:
                        handlers.remove(handler)
                        self._stats.handlers_registered -= 1
                        return True
            
            # Check global handlers
            for handler in self._global_handlers:
                if handler.handler_id == handler_id:
                    self._global_handlers.remove(handler)
                    self._stats.handlers_registered -= 1
                    return True
            
            return False
    
    def add_route(
        self,
        source_signal: str,
        target_signal: str,
        transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> str:
        """Add a signal routing rule.
        
        Args:
            source_signal: Source signal name
            target_signal: Target signal name
            transform: Optional data transform
            condition: Optional routing condition
            
        Returns:
            Route ID
        """
        route_id = str(uuid.uuid4())[:8]
        route = SignalRoute(
            route_id=route_id,
            source_signal=source_signal,
            target_signal=target_signal,
            transform=transform,
            condition=condition,
        )
        
        with self._lock:
            if source_signal not in self._routes:
                self._routes[source_signal] = []
            self._routes[source_signal].append(route)
            self._stats.routes_active += 1
        
        return route_id
    
    def remove_route(self, route_id: str) -> bool:
        """Remove a routing rule.
        
        Args:
            route_id: Route ID to remove
            
        Returns:
            True if route was found and removed
        """
        with self._lock:
            for source_signal, routes in self._routes.items():
                for route in routes:
                    if route.route_id == route_id:
                        routes.remove(route)
                        self._stats.routes_active -= 1
                        return True
            return False
    
    def emit(
        self,
        signal_name: str,
        data: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Signal:
        """Emit a signal.
        
        Args:
            signal_name: Signal name
            data: Signal data
            source: Signal source
            metadata: Additional metadata
            
        Returns:
            Emitted signal
        """
        signal = Signal(
            name=signal_name,
            data=data or {},
            source=source,
            metadata=metadata or {},
        )
        
        with self._lock:
            self._signal_history.append(signal)
            if len(self._signal_history) > self._max_history:
                self._signal_history = self._signal_history[-self._max_history // 2:]
            self._stats.total_emitted += 1
        
        # Handle direct handlers
        self._dispatch_to_handlers(signal)
        
        # Handle routed signals
        self._dispatch_routes(signal)
        
        return signal
    
    def _dispatch_to_handlers(self, signal: Signal) -> None:
        """Dispatch signal to registered handlers.
        
        Args:
            signal: Signal to dispatch
        """
        with self._lock:
            handlers = list(self._handlers.get(signal.name, []))
            global_handlers = list(self._global_handlers)
        
        all_handlers = handlers + [h for h in global_handlers if self._match_global_handler(h, signal)]
        
        for handler in all_handlers:
            if not handler.is_active:
                continue
            if handler.filter_func and not handler.filter_func(signal.data):
                continue
            try:
                handler.callback(signal)
                with self._lock:
                    self._stats.total_handled += 1
            except Exception:
                pass  # Handler errors don't stop other handlers
    
    def _match_global_handler(self, handler: SignalHandler, signal: Signal) -> bool:
        """Check if global handler should handle signal.
        
        Args:
            handler: Global handler
            signal: Signal to check
            
        Returns:
            True if handler should handle signal
        """
        # Handler with signal_name="*" handles all
        return True
    
    def _dispatch_routes(self, signal: Signal) -> None:
        """Dispatch signal through routing rules.
        
        Args:
            signal: Signal to route
        """
        with self._lock:
            routes = list(self._routes.get(signal.name, []))
        
        for route in routes:
            if not route.is_active:
                continue
            if route.condition and not route.condition(signal.data):
                continue
            
            new_data = signal.data
            if route.transform:
                new_data = route.transform(signal.data)
            
            new_signal = Signal(
                name=route.target_signal,
                data=new_data,
                source=signal.source,
                metadata={"routed_from": signal.signal_id},
            )
            
            with self._lock:
                self._stats.total_routed += 1
            
            self._dispatch_to_handlers(new_signal)
    
    def get_history(
        self,
        signal_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Signal]:
        """Get signal history.
        
        Args:
            signal_name: Filter by signal name
            limit: Maximum entries to return
            
        Returns:
            List of historical signals
        """
        with self._lock:
            history = self._signal_history
            if signal_name:
                history = [s for s in history if s.name == signal_name]
            return list(history[-limit:])
    
    def get_stats(self) -> SignalStats:
        """Get signal statistics.
        
        Returns:
            Signal statistics
        """
        with self._lock:
            return SignalStats(
                total_emitted=self._stats.total_emitted,
                total_handled=self._stats.total_handled,
                total_routed=self._stats.total_routed,
                handlers_registered=self._stats.handlers_registered,
                routes_active=self._stats.routes_active,
            )
    
    def clear_history(self) -> None:
        """Clear signal history."""
        with self._lock:
            self._signal_history.clear()
