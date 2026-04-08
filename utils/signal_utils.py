"""Signal and signal handler utilities.

Provides signal handling for graceful shutdown and
inter-process communication in automation workflows.
"""

import os
import signal
import sys
import threading
from typing import Callable, Dict, List, Optional


SignalHandler = Callable[[int, Any], None]


class SignalManager:
    """Manages signal handlers for graceful shutdown.

    Example:
        sm = SignalManager()
        sm.on_sigint(lambda: print("Shutting down..."))
        sm.on_sigterm(lambda: save_state())
        sm.wait()
    """

    def __init__(self) -> None:
        self._handlers: Dict[int, List[SignalHandler]] = {}
        self._original_handlers: Dict[int, Optional[SignalHandler]] = {}
        self._lock = threading.Lock()
        self._done = threading.Event()

    def on_signal(self, sig: int, handler: SignalHandler) -> None:
        """Register a signal handler.

        Args:
            sig: Signal number (e.g., signal.SIGINT).
            handler: Handler function (sig, frame).
        """
        with self._lock:
            if sig not in self._handlers:
                self._handlers[sig] = []
                self._original_handlers[sig] = signal.signal(sig, self._dispatch)
            self._handlers[sig].append(handler)

    def on_sigint(self, handler: SignalHandler) -> None:
        """Register SIGINT (Ctrl+C) handler."""
        self.on_signal(signal.SIGINT, handler)

    def on_sigterm(self, handler: SignalHandler) -> None:
        """Register SIGTERM handler."""
        self.on_signal(signal.SIGTERM, handler)

    def on_sighup(self, handler: SignalHandler) -> None:
        """Register SIGHUP handler."""
        self.on_signal(signal.SIGHUP, handler)

    def _dispatch(self, sig: int, frame: Any) -> None:
        with self._lock:
            handlers = list(self._handlers.get(sig, []))
        for handler in handlers:
            try:
                handler(sig, frame)
            except Exception as e:
                print(f"Signal handler error: {e}", file=sys.stderr)

    def send_signal(self, sig: int, pid: Optional[int] = None) -> None:
        """Send a signal to a process.

        Args:
            sig: Signal number.
            pid: Process ID. Current process if None.
        """
        os.kill(pid or os.getpid(), sig)

    def set_exit(self) -> None:
        """Signal that shutdown should begin."""
        self._done.set()

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for exit signal.

        Args:
            timeout: Maximum wait time in seconds.

        Returns:
            True if exit was signaled.
        """
        return self._done.wait(timeout)

    def restore_defaults(self) -> None:
        """Restore original signal handlers."""
        with self._lock:
            for sig, handler in self._original_handlers.items():
                if handler is not None:
                    signal.signal(sig, handler)
            self._handlers.clear()
            self._original_handlers.clear()


class DelayedSignal:
    """Send signal after a delay.

    Example:
        ds = DelayedSignal(signal.SIGTERM, delay=5.0)
        ds.start()
        ds.cancel()
    """

    def __init__(self, sig: int, delay: float, pid: Optional[int] = None) -> None:
        self._sig = sig
        self._delay = delay
        self._pid = pid or os.getpid()
        self._timer: Optional[threading.Timer] = None

    def start(self) -> None:
        """Start the delayed signal timer."""
        self._timer = threading.Timer(self._delay, self._send)
        self._timer.daemon = True
        self._timer.start()

    def _send(self) -> None:
        os.kill(self._pid, self._sig)

    def cancel(self) -> None:
        """Cancel the delayed signal."""
        if self._timer:
            self._timer.cancel()
            self._timer = None


def ignore_signal(sig: int) -> None:
    """Ignore a signal (set to SIG_IGN).

    Args:
        sig: Signal number.
    """
    signal.signal(sig, signal.SIG_IGN)


def default_signal(sig: int) -> None:
    """Restore default signal handler.

    Args:
        sig: Signal number.
    """
    signal.signal(sig, signal.SIG_DFL)


def is_signal_supported(sig: int) -> bool:
    """Check if signal is supported on this platform.

    Args:
        sig: Signal number.

    Returns:
        True if supported.
    """
    return sig in signal.Signals
