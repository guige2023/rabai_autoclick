"""
Breakpoint utilities for debugging automation workflows.

Provides programmatic breakpoints, conditional breakpoints, and
breakpoint management for diagnosing automation issues.

Example:
    >>> from breakpoint_utils import breakpoint, set_breakpoint, watch
    >>> breakpoint(condition="x > 10")
    >>> watch("variable_name")
"""

from __future__ import annotations

import inspect
import os
import sys
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


# =============================================================================
# Types
# =============================================================================


class BreakpointType(Enum):
    """Types of breakpoints."""
    NORMAL = "normal"
    CONDITIONAL = "conditional"
    WATCH = "watch"
    CALL = "call"
    RETURN = "return"


@dataclass
class Breakpoint:
    """Represents a breakpoint."""
    id: int
    condition: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None
    enabled: bool = True
    hit_count: int = 0
    hit_limit: Optional[int] = None
    ignore_count: int = 0
    filename: str = ""
    lineno: int = 0
    func_name: str = ""


@dataclass
class WatchPoint:
    """Represents a watch point."""
    id: int
    expression: str
    value: Any = None
    previous_value: Any = None
    changed: bool = False
    enabled: bool = True


# =============================================================================
# Breakpoint Manager
# =============================================================================


class BreakpointManager:
    """
    Manages all breakpoints and watch points.

    Example:
        >>> manager = BreakpointManager()
        >>> bp = manager.add_breakpoint("my_function", lineno=10)
        >>> bp.condition = lambda: x > 5
        >>> manager.enable_all()
    """

    def __init__(self):
        self._breakpoints: Dict[int, Breakpoint] = {}
        self._watchpoints: Dict[int, WatchPoint] = {}
        self._next_bp_id: int = 1
        self._next_wp_id: int = 1
        self._lock = threading.Lock()
        self._global_breakpoint_set = False

    def add_breakpoint(
        self,
        filename: Optional[str] = None,
        lineno: Optional[int] = None,
        func_name: Optional[str] = None,
        condition: Optional[Callable[[], bool]] = None,
        action: Optional[Callable[[], None]] = None,
    ) -> Breakpoint:
        """
        Add a new breakpoint.

        Args:
            filename: File to break in.
            lineno: Line number.
            func_name: Function name.
            condition: Optional condition function.
            action: Optional action to run when hit.

        Returns:
            The created Breakpoint.
        """
        with self._lock:
            bp = Breakpoint(
                id=self._next_bp_id,
                condition=condition,
                action=action,
                filename=filename or "",
                lineno=lineno or 0,
                func_name=func_name or "",
            )
            self._breakpoints[self._next_bp_id] = bp
            self._next_bp_id += 1

            self._set_global_breakpoint()

            return bp

    def add_watchpoint(
        self,
        expression: str,
        initial_value: Any = None,
    ) -> WatchPoint:
        """
        Add a watch point.

        Args:
            expression: Variable or expression to watch.
            initial_value: Initial value.

        Returns:
            The created WatchPoint.
        """
        with self._lock:
            wp = WatchPoint(
                id=self._next_wp_id,
                expression=expression,
                value=initial_value,
            )
            self._watchpoints[self._next_wp_id] = wp
            self._next_wp_id += 1

            return wp

    def remove_breakpoint(self, bp_id: int) -> bool:
        """Remove a breakpoint by ID."""
        with self._lock:
            return self._breakpoints.pop(bp_id, None) is not None

    def remove_watchpoint(self, wp_id: int) -> bool:
        """Remove a watchpoint by ID."""
        with self._lock:
            return self._watchpoints.pop(wp_id, None) is not None

    def enable_breakpoint(self, bp_id: int) -> None:
        """Enable a breakpoint."""
        with self._lock:
            if bp_id in self._breakpoints:
                self._breakpoints[bp_id].enabled = True

    def disable_breakpoint(self, bp_id: int) -> None:
        """Disable a breakpoint."""
        with self._lock:
            if bp_id in self._breakpoints:
                self._breakpoints[bp_id].enabled = False

    def enable_all(self) -> None:
        """Enable all breakpoints."""
        with self._lock:
            for bp in self._breakpoints.values():
                bp.enabled = True

    def disable_all(self) -> None:
        """Disable all breakpoints."""
        with self._lock:
            for bp in self._breakpoints.values():
                bp.enabled = False

    def get_breakpoint(self, bp_id: int) -> Optional[Breakpoint]:
        """Get a breakpoint by ID."""
        with self._lock:
            return self._breakpoints.get(bp_id)

    def list_breakpoints(self) -> List[Breakpoint]:
        """List all breakpoints."""
        with self._lock:
            return list(self._breakpoints.values())

    def _set_global_breakpoint(self) -> None:
        """Set up the global breakpoint handler."""
        if self._global_breakpoint_set:
            return

        self._global_breakpoint_set = True

        old_excepthook = sys.excepthook

        def breakpoint_hook(type, value, tb):
            import traceback
            if type == KeyboardInterrupt:
                print("\nBreakpoint hit!")
                traceback.print_stack()
                return
            old_excepthook(type, value, tb)

        sys.excepthook = breakpoint_hook

    def check_breakpoint(
        self,
        filename: str,
        lineno: int,
        func_name: str,
    ) -> bool:
        """
        Check if a breakpoint should trigger.

        Args:
            filename: Current file.
            lineno: Current line.
            func_name: Current function.

        Returns:
            True if breakpoint should stop.
        """
        with self._lock:
            for bp in self._breakpoints.values():
                if not bp.enabled:
                    continue

                if bp.ignore_count > 0:
                    bp.ignore_count -= 1
                    continue

                # Check if breakpoint matches
                matches = False

                if bp.filename and bp.filename in filename:
                    matches = True
                elif bp.func_name and bp.func_name == func_name:
                    matches = True
                elif bp.lineno and bp.lineno == lineno:
                    matches = True
                elif not bp.filename and not bp.lineno and not bp.func_name:
                    # Global breakpoint
                    matches = True

                if matches:
                    bp.hit_count += 1

                    if bp.hit_limit and bp.hit_count > bp.hit_limit:
                        continue

                    # Check condition
                    if bp.condition:
                        try:
                            if not bp.condition():
                                continue
                        except Exception:
                            continue

                    # Execute action
                    if bp.action:
                        try:
                            bp.action()
                        except Exception:
                            pass

                    return True

        return False

    def update_watchpoints(self, frame: Any) -> List[WatchPoint]:
        """
        Update all watchpoints with current frame values.

        Args:
            frame: Current execution frame.

        Returns:
            List of watchpoints that changed.
        """
        changed = []

        with self._lock:
            for wp in self._watchpoints.values():
                if not wp.enabled:
                    continue

                try:
                    wp.previous_value = wp.value
                    wp.value = eval(wp.expression, frame.f_globals, frame.f_locals)
                    wp.changed = wp.previous_value != wp.value

                    if wp.changed:
                        changed.append(wp)

                except Exception:
                    pass

        return changed


# =============================================================================
# Global breakpoint manager
# =============================================================================


_manager: Optional[BreakpointManager] = None


def get_manager() -> BreakpointManager:
    """Get the global breakpoint manager."""
    global _manager
    if _manager is None:
        _manager = BreakpointManager()
    return _manager


def breakpoint(
    condition: Optional[str] = None,
    action: Optional[Callable] = None,
) -> None:
    """
    Set a breakpoint at the current location.

    Args:
        condition: Optional condition expression.
        action: Optional action to run.
    """
    frame = sys._getframe(1)
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno
    func_name = frame.f_code.co_name

    cond_fn = None
    if condition:
        def make_cond(expr=condition):
            def cond():
                try:
                    return eval(expr, frame.f_globals, frame.f_locals)
                except Exception:
                    return False
            return cond
        cond_fn = make_cond()

    bp = get_manager().add_breakpoint(
        filename=filename,
        lineno=lineno,
        func_name=func_name,
        condition=cond_fn,
        action=action,
    )

    print(f"Breakpoint {bp.id} set at {filename}:{lineno}")


def set_breakpoint(
    func_name: str,
    condition: Optional[Callable] = None,
) -> Breakpoint:
    """
    Set a breakpoint at a function.

    Args:
        func_name: Function name to break on.
        condition: Optional condition.

    Returns:
        The created Breakpoint.
    """
    return get_manager().add_breakpoint(func_name=func_name, condition=condition)


def watch(expression: str) -> WatchPoint:
    """
    Watch a variable or expression.

    Args:
        expression: Variable name or expression to watch.

    Returns:
        The created WatchPoint.
    """
    return get_manager().add_watchpoint(expression)


def watch_changes(expression: str) -> None:
    """
    Watch a variable and print when it changes.

    Args:
        expression: Variable name.
    """
    frame = sys._getframe(1)

    try:
        initial = eval(expression, frame.f_globals, frame.f_locals)
    except Exception:
        initial = None

    wp = watch(expression)

    def check_change():
        changed = get_manager().update_watchpoints(frame)
        for w in changed:
            if w.expression == expression:
                print(f"[WATCH] {expression}: {w.previous_value} -> {w.value}")

    # This would need to be called periodically
    # In practice, use a debugger or profiling tool


# =============================================================================
# Convenience Functions
# =============================================================================


def break_on_exception():
    """Set a breakpoint that triggers on any exception."""
    def exception_breakpoint():
        import traceback
        traceback.print_exc()

    # This hooks into sys.excepthook
    old_hook = sys.excepthook

    def hook(type, value, tb):
        print(f"\nException: {type.__name__}: {value}")
        traceback.print_tb(tb)
        old_hook(type, value, tb)

    sys.excepthook = hook


def break_on_variable(variable_name: str, condition: Optional[str] = None):
    """
    Break when a variable changes or meets a condition.

    Args:
        variable_name: Name of variable to watch.
        condition: Optional condition expression.
    """
    frame = sys._getframe(1)

    wp = watch(variable_name)

    def check_and_break():
        try:
            current = eval(variable_name, frame.f_globals, frame.f_locals)
        except Exception:
            return

        if condition:
            try:
                if not eval(condition, frame.f_globals, frame.f_locals):
                    return
            except Exception:
                return

        import traceback
        print(f"\nBreak on {variable_name} = {current}")
        traceback.print_stack()

    # Store for later checking
    if not hasattr(sys, "_breakpoint_watches"):
        sys._breakpoint_watches = []
    sys._breakpoint_watches.append((variable_name, check_and_break))


# =============================================================================
# Interactive Debugger Trigger
# =============================================================================


def trigger_debugger():
    """
    Trigger an interactive debugger (pdb).

    Usage:
        >>> trigger_debugger()  # drops into pdb
    """
    import pdb
    pdb.set_trace()


def trigger_ipdb():
    """
    Trigger IPython debugger if available.

    Falls back to pdb if IPython not installed.
    """
    try:
        import ipdb
        ipdb.pm()
    except ImportError:
        import pdb
        pdb.pm()
