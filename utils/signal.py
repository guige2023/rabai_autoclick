"""Signal handling utilities for RabAI AutoClick.

Provides:
- Signal handlers
- Signal routing
- Signal masking
"""

import signal
import threading
from enum import Enum
from typing import Callable, Dict, List, Optional


class SignalType(Enum):
    """Signal types."""
    HUP = "hup"
    INT = "int"
    TERM = "term"
    USR1 = "usr1"
    USR2 = "usr2"
    CHILD = "child"
    WINCH = "winch"


class SignalHandler:
    """Handle OS signals."""

    def __init__(self) -> None:
        """Initialize handler."""
        self._handlers: Dict[SignalType, List[Callable]] = {}
        self._original_handlers: Dict[signal.Signals, Callable] = {}
        self._setup = False
        self._lock = threading.Lock()

    def register(
        self,
        signal_type: SignalType,
        handler: Callable[[], None],
    ) -> None:
        """Register signal handler.

        Args:
            signal_type: Type of signal.
            handler: Handler function.
        """
        with self._lock:
            if signal_type not in self._handlers:
                self._handlers[signal_type] = []
            self._handlers[signal_type].append(handler)

            if not self._setup:
                self._setup_signals()

    def unregister(
        self,
        signal_type: SignalType,
        handler: Callable[[], None],
    ) -> bool:
        """Unregister signal handler.

        Args:
            signal_type: Type of signal.
            handler: Handler to remove.

        Returns:
            True if removed.
        """
        with self._lock:
            if signal_type in self._handlers:
                if handler in self._handlers[signal_type]:
                    self._handlers[signal_type].remove(handler)
                    return True
        return False

    def _setup_signals(self) -> None:
        """Set up signal handlers."""
        self._setup = True
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: signal.Signals, frame) -> None:
        """Handle incoming signal.

        Args:
            signum: Signal number.
            frame: Current stack frame.
        """
        sig_type = self._signum_to_type(signum)
        if sig_type and sig_type in self._handlers:
            for handler in self._handlers[sig_type]:
                try:
                    handler()
                except Exception:
                    pass

    def _signum_to_type(self, signum: int) -> Optional[SignalType]:
        """Convert signal number to type.

        Args:
            signum: Signal number.

        Returns:
            SignalType or None.
        """
        mapping = {
            signal.SIGHUP: SignalType.HUP,
            signal.SIGINT: SignalType.INT,
            signal.SIGTERM: SignalType.TERM,
            signal.SIGUSR1: SignalType.USR1,
            signal.SIGUSR2: SignalType.USR2,
            signal.SIGCHLD: SignalType.CHILD,
            signal.SIGWINCH: SignalType.WINCH,
        }
        return mapping.get(signum)


class SignalEmitter:
    """Emit custom signals within the application."""

    def __init__(self) -> None:
        """Initialize emitter."""
        self._handlers: Dict[str, List[Callable]] = {}

    def on(self, event: str, handler: Callable[..., None]) -> None:
        """Register event handler.

        Args:
            event: Event name.
            handler: Handler function.
        """
        if event not in self._handlers:
            self._handlers[event] = []
        self._handlers[event].append(handler)

    def off(self, event: str, handler: Callable[..., None]) -> bool:
        """Unregister event handler.

        Args:
            event: Event name.
            handler: Handler to remove.

        Returns:
            True if removed.
        """
        if event in self._handlers:
            if handler in self._handlers[event]:
                self._handlers[event].remove(handler)
                return True
        return False

    def emit(self, event: str, *args: any, **kwargs: any) -> None:
        """Emit event to handlers.

        Args:
            event: Event name.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        if event in self._handlers:
            for handler in self._handlers[event]:
                try:
                    handler(*args, **kwargs)
                except Exception:
                    pass

    def clear(self, event: Optional[str] = None) -> None:
        """Clear handlers.

        Args:
            event: Event to clear (None = all).
        """
        if event:
            self._handlers.pop(event, None)
        else:
            self._handlers.clear()


class SignalRouter:
    """Route signals to handlers based on conditions."""

    def __init__(self) -> None:
        """Initialize router."""
        self._routes: Dict[str, Callable] = {}
        self._lock = threading.Lock()

    def add_route(
        self,
        signal_type: str,
        handler: Callable[[], None],
    ) -> None:
        """Add signal route.

        Args:
            signal_type: Signal type identifier.
            handler: Handler function.
        """
        with self._lock:
            self._routes[signal_type] = handler

    def remove_route(self, signal_type: str) -> bool:
        """Remove signal route.

        Args:
            signal_type: Signal type identifier.

        Returns:
            True if removed.
        """
        with self._lock:
            if signal_type in self._routes:
                del self._routes[signal_type]
                return True
        return False

    def route(self, signal_type: str) -> bool:
        """Route signal to handler.

        Args:
            signal_type: Signal type identifier.

        Returns:
            True if routed.
        """
        with self._lock:
            handler = self._routes.get(signal_type)
            if handler:
                try:
                    handler()
                    return True
                except Exception:
                    return False
        return False


class SignalMask:
    """Mask/unmask signals in current thread."""

    @staticmethod
    def mask(*signal_types: int) -> None:
        """Mask signals in current thread.

        Args:
            *signal_types: Signal numbers to mask.
        """
        try:
            signal.pthread_sigmask(signal.SIG_BLOCK, set(signal_types))
        except Exception:
            pass

    @staticmethod
    def unmask(*signal_types: int) -> None:
        """Unmask signals in current thread.

        Args:
            *signal_types: Signal numbers to unmask.
        """
        try:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, set(signal_types))
        except Exception:
            pass

    @staticmethod
    def get_masked() -> set:
        """Get currently masked signals.

        Returns:
            Set of masked signal numbers.
        """
        try:
            return signal.pthread_sigmask(signal.SIG_BLOCK, set())
        except Exception:
            return set()


class SignalWaiter:
    """Wait for signals."""

    def __init__(self) -> None:
        """Initialize waiter."""
        self._signals: Dict[int, threading.Event] = {}

    def wait_for(
        self,
        signal_num: int,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait for signal.

        Args:
            signal_num: Signal number to wait for.
            timeout: Optional timeout.

        Returns:
            True if signal received.
        """
        if signal_num not in self._signals:
            self._signals[signal_num] = threading.Event()

        return self._signals[signal_num].wait(timeout=timeout)

    def signal(self, signal_num: int) -> None:
        """Signal a signal number.

        Args:
            signal_num: Signal number.
        """
        if signal_num in self._signals:
            self._signals[signal_num].set()

    def clear(self, signal_num: int) -> None:
        """Clear signal event.

        Args:
            signal_num: Signal number.
        """
        if signal_num in self._signals:
            self._signals[signal_num].clear()
