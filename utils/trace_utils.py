"""
Trace utilities for debugging and tracing automation workflow execution.

Provides function tracing, call stack visualization, and execution
profiling for diagnosing automation issues.

Example:
    >>> from trace_utils import trace_calls, TraceLogger
    >>> traced = trace_calls(my_function)
    >>> logger = TraceLogger()
    >>> logger.start()
"""

from __future__ import annotations

import functools
import os
import sys
import threading
import time
import traceback as tb_module
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


# =============================================================================
# Types
# =============================================================================


class TraceEvent(Enum):
    """Trace event types."""
    CALL = "call"
    RETURN = "return"
    EXCEPTION = "exception"
    LINE = "line"


@dataclass
class TraceRecord:
    """A single trace event record."""
    event: TraceEvent
    timestamp: float
    thread_id: int
    thread_name: str
    function_name: str
    filename: str
    lineno: int
    locals: Optional[Dict[str, Any]] = None
    return_value: Any = None
    exception: Optional[str] = None


# =============================================================================
# Function Tracer
# =============================================================================


def trace_calls(func: Callable) -> Callable:
    """
    Decorator to trace function calls.

    Example:
        >>> @trace_calls
        >>> def my_function(x, y):
        ...     return x + y
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        thread = threading.current_thread()
        record = TraceRecord(
            event=TraceEvent.CALL,
            timestamp=time.monotonic(),
            thread_id=threading.get_ident(),
            thread_name=thread.name,
            function_name=func.__name__,
            filename=func.__code__.co_filename,
            lineno=func.__code__.co_firstlineno,
        )

        print(f"[TRACE] {record.function_name} called from {record.filename}:{record.lineno}")

        try:
            result = func(*args, **kwargs)

            print(f"[TRACE] {record.function_name} -> returned")

            return result

        except Exception as e:
            print(f"[TRACE] {record.function_name} -> raised {type(e).__name__}: {e}")
            raise

    return wrapper


class TracedClass:
    """
    Base class that traces all methods.

    Example:
        >>> class MyClass(TracedClass):
        ...     def my_method(self):
        ...         pass
    """

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        for name, method in cls.__dict__.items():
            if callable(method) and not name.startswith("_"):
                setattr(cls, name, trace_calls(method))


# =============================================================================
# Call Stack Tracer
# =============================================================================


class CallStackTracer:
    """
    Traces the call stack for debugging.

    Example:
        >>> tracer = CallStackTracer()
        >>> tracer.start()
        >>> # code runs
        >>> trace = tracer.stop()
        >>> print(trace.format())
    """

    def __init__(self, max_depth: int = 100):
        self.max_depth = max_depth
        self._records: List[TraceRecord] = []
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start tracing."""
        self._running = True
        self._records.clear()

    def stop(self) -> List[TraceRecord]:
        """Stop tracing and return records."""
        self._running = False
        with self._lock:
            return list(self._records)

    def record(self, event: TraceEvent, frame: Any) -> None:
        """Record a trace event from a frame."""
        if not self._running:
            return

        thread = threading.current_thread()

        try:
            code = frame.f_code
            filename = code.co_filename
            func_name = code.co_name
            lineno = frame.f_lineno

            record = TraceRecord(
                event=event,
                timestamp=time.monotonic(),
                thread_id=threading.get_ident(),
                thread_name=thread.name,
                function_name=func_name,
                filename=filename,
                lineno=lineno,
            )

            with self._lock:
                self._records.append(record)

        except Exception:
            pass

    def get_trace(self) -> "TraceFormatter":
        """Get a formatter for the trace."""
        with self._lock:
            return TraceFormatter(list(self._records))


class TraceFormatter:
    """Formats trace records for display."""

    def __init__(self, records: List[TraceRecord]):
        self.records = records

    def format(self, max_width: int = 120) -> str:
        """Format trace as a string."""
        lines = []
        indent_level = 0

        for record in self.records:
            if record.event == TraceEvent.CALL:
                indent = "  " * indent_level
                lines.append(f"{indent}→ {record.function_name}() at {record.filename}:{record.lineno}")
                indent_level += 1

            elif record.event == TraceEvent.RETURN:
                indent_level = max(0, indent_level - 1)
                indent = "  " * indent_level
                ret_val = f" -> {record.return_value}" if record.return_value else ""
                lines.append(f"{indent}← {record.function_name}{ret_val}")

            elif record.event == TraceEvent.EXCEPTION:
                indent = "  " * indent_level
                lines.append(f"{indent}! {record.exception}")

        return "\n".join(lines)

    def to_html(self) -> str:
        """Format trace as HTML."""
        html = ['<div class="trace">']

        for record in self.records:
            color = {
                TraceEvent.CALL: "#2e7d32",
                TraceEvent.RETURN: "#1565c0",
                TraceEvent.EXCEPTION: "#c62828",
            }.get(record.event, "#666")

            indent = 20 * self.records[:self.records.index(record)].count(
                TraceEvent.CALL
            ) - 20 * self.records[:self.records.index(record)].count(
                TraceEvent.RETURN
            )

            html.append(
                f'<div style="color:{color};margin-left:{max(0, indent)}px">'
                f"{record.event.value}: {record.function_name}() at {record.filename}:{record.lineno}"
                f"</div>"
            )

        html.append("</div>")
        return "\n".join(html)


# =============================================================================
# Execution Tracer
# =============================================================================


class ExecutionTracer:
    """
    Tracer that captures execution flow with timing.

    Example:
        >>> tracer = ExecutionTracer()
        >>> with tracer.trace("operation"):
        ...     do_work()
        >>> print(tracer.report())
    """

    def __init__(self):
        self._events: List[Dict[str, Any]] = []
        self._stack: List[str] = []
        self._lock = threading.Lock()

    def trace(self, name: str) -> "_TraceContext":
        """Context manager for tracing a block."""
        return _TraceContext(self, name)

    def _enter(self, name: str) -> None:
        with self._lock:
            self._events.append({
                "type": "enter",
                "name": name,
                "timestamp": time.monotonic(),
                "thread": threading.current_thread().name,
            })
            self._stack.append(name)

    def _exit(self, name: str, duration: float) -> None:
        with self._lock:
            self._events.append({
                "type": "exit",
                "name": name,
                "duration_ms": duration * 1000,
                "timestamp": time.monotonic(),
                "thread": threading.current_thread().name,
            })
            if self._stack and self._stack[-1] == name:
                self._stack.pop()

    def _event(self, name: str, data: Any = None) -> None:
        with self._lock:
            self._events.append({
                "type": "event",
                "name": name,
                "data": data,
                "timestamp": time.monotonic(),
                "thread": threading.current_thread().name,
            })

    def report(self) -> str:
        """Generate a text report."""
        lines = ["Execution Trace", "=" * 50]

        for event in self._events:
            indent = len(self._stack) * 2
            prefix = "  " * indent

            if event["type"] == "enter":
                lines.append(f"{prefix}→ {event['name']}")
            elif event["type"] == "exit":
                lines.append(f"{prefix}← {event['name']} ({event['duration_ms']:.2f}ms)")
            elif event["type"] == "event":
                lines.append(f"{prefix}• {event['name']}: {event['data']}")

        return "\n".join(lines)


class _TraceContext:
    """Context manager for ExecutionTracer.trace()."""

    def __init__(self, tracer: ExecutionTracer, name: str):
        self.tracer = tracer
        self.name = name
        self.start_time = 0.0

    def __enter__(self) -> None:
        self.start_time = time.monotonic()
        self.tracer._enter(self.name)

    def __exit__(self, *args: Any) -> None:
        duration = time.monotonic() - self.start_time
        self.tracer._exit(self.name, duration)


# =============================================================================
# Stack Dumper
# =============================================================================


def dump_stack() -> str:
    """
    Get current stack trace as a string.

    Returns:
        Formatted stack trace.
    """
    return tb_module.format_exc()


def dump_stacks() -> Dict[int, str]:
    """
    Get stack traces for all threads.

    Returns:
        Dict mapping thread ID to stack trace.
    """
    import traceback

    stacks = {}
    for thread_id, frame in sys._current_frames().items():
        stacks[thread_id] = "".join(tb_module.format_stack(frame))

    return stacks


# =============================================================================
# Decorators
# =============================================================================


def timing(func: Callable) -> Callable:
    """
    Decorator to measure and print function execution time.

    Example:
        >>> @timing
        >>> def slow_function():
        ...     time.sleep(1)
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            print(f"[TIMING] {func.__name__}: {elapsed:.2f}ms")

    return wrapper


def log_exceptions(func: Callable) -> Callable:
    """
    Decorator to log any exceptions raised by a function.

    Example:
        >>> @log_exceptions
        >>> def risky_function():
        ...     pass
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            print(f"[ERROR] {func.__name__} raised:")
            tb_module.print_exc()
            raise

    return wrapper


# =============================================================================
# Debug Utilities
# =============================================================================


def get_callers_name(depth: int = 2) -> str:
    """
    Get the name of the calling function.

    Args:
        depth: Stack depth (1 = immediate caller, 2 = caller's caller, etc.)
    """
    try:
        frame = sys._getframe(depth)
        return frame.f_code.co_name
    except Exception:
        return "<unknown>"


def get_local(name: str, depth: int = 2) -> Any:
    """
    Get a local variable by name from a caller's frame.

    Args:
        name: Variable name.
        depth: Stack depth.
    """
    try:
        frame = sys._getframe(depth)
        return frame.f_locals.get(name)
    except Exception:
        return None


def set_local(name: str, value: Any, depth: int = 2) -> None:
    """
    Set a local variable by name in a caller's frame.

    Args:
        name: Variable name.
        value: New value.
        depth: Stack depth.
    """
    try:
        frame = sys._getframe(depth)
        frame.f_locals[name] = value
    except Exception:
        pass
