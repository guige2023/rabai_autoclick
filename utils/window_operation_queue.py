"""
Window Operation Queue.

Queue and batch multiple window operations (move, resize, minimize, etc.)
to avoid redundant state changes and optimize window management.

Usage:
    from utils.window_operation_queue import WindowOpQueue, WindowOp

    queue = WindowOpQueue()
    queue.add_move(win_id, x=100, y=200)
    queue.add_resize(win_id, width=800, height=600)
    queue.execute_all()
"""

from __future__ import annotations

from typing import Optional, Dict, List, Any, Callable, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto
import time

if TYPE_CHECKING:
    pass


class WindowOperation(Enum):
    """Types of window operations."""
    MOVE = auto()
    RESIZE = auto()
    MINIMIZE = auto()
    MAXIMIZE = auto()
    RESTORE = auto()
    RAISE = auto()
    LOWER = auto()
    SET_FULLSCREEN = auto()
    CENTER = auto()
    SET_ALPHA = auto()


@dataclass
class WindowOp:
    """A single window operation."""
    op_type: WindowOperation
    window_id: int
    params: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    priority: int = 0

    def __repr__(self) -> str:
        return f"WindowOp({self.op_type.name}, win={self.window_id}, {self.params})"


@dataclass
class WindowState:
    """Represents the current state of a window."""
    window_id: int
    bounds: Tuple[int, int, int, int] = (0, 0, 0, 0)  # x, y, w, h
    is_minimized: bool = False
    is_maximized: bool = False
    is_fullscreen: bool = False
    alpha: float = 1.0
    z_order: int = 0


class WindowOpQueue:
    """
    Queue and batch window operations.

    Supports deduplication (merging multiple moves to the same
    window), prioritization, and batch execution.

    Example:
        queue = WindowOpQueue()
        queue.add_move(win_id, x=100, y=200)
        queue.add_resize(win_id, width=800, height=600)
        queue.execute_all()
    """

    def __init__(
        self,
        dedup_window_ops: bool = True,
        batch_interval: float = 0.1,
    ) -> None:
        """
        Initialize the window operation queue.

        Args:
            dedup_window_ops: Merge duplicate ops to the same window.
            batch_interval: Seconds to wait before flushing queue.
        """
        self._queue: List[WindowOp] = []
        self._dedup = dedup_window_ops
        self._batch_interval = batch_interval
        self._last_flush = time.time()
        self._callbacks: List[Callable[[WindowOp], None]] = []
        self._state_cache: Dict[int, WindowState] = {}
        self._executor: Optional[Callable[[WindowOp], None]] = None

    def set_executor(
        self,
        executor: Callable[[WindowOp], None],
    ) -> None:
        """
        Set the function that executes individual operations.

        Args:
            executor: Function that takes a WindowOp and executes it.
        """
        self._executor = executor

    def add_move(
        self,
        window_id: int,
        x: Optional[int] = None,
        y: Optional[int] = None,
        priority: int = 0,
    ) -> "WindowOpQueue":
        """
        Queue a window move operation.

        Args:
            window_id: ID of the window to move.
            x: New X coordinate (None to keep current).
            y: New Y coordinate (None to keep current).
            priority: Operation priority (higher = earlier execution).

        Returns:
            Self for chaining.
        """
        params = {}
        if x is not None:
            params["x"] = x
        if y is not None:
            params["y"] = y

        op = WindowOp(
            op_type=WindowOperation.MOVE,
            window_id=window_id,
            params=params,
            priority=priority,
        )
        self._add_op(op)
        return self

    def add_resize(
        self,
        window_id: int,
        width: Optional[int] = None,
        height: Optional[int] = None,
        priority: int = 0,
    ) -> "WindowOpQueue":
        """
        Queue a window resize operation.

        Args:
            window_id: ID of the window to resize.
            width: New width (None to keep current).
            height: New height (None to keep current).
            priority: Operation priority.

        Returns:
            Self for chaining.
        """
        params = {}
        if width is not None:
            params["width"] = width
        if height is not None:
            params["height"] = height

        op = WindowOp(
            op_type=WindowOperation.RESIZE,
            window_id=window_id,
            params=params,
            priority=priority,
        )
        self._add_op(op)
        return self

    def add_minimize(
        self,
        window_id: int,
    ) -> "WindowOpQueue":
        """Queue a window minimize operation."""
        self._add_op(WindowOp(
            op_type=WindowOperation.MINIMIZE,
            window_id=window_id,
        ))
        return self

    def add_maximize(
        self,
        window_id: int,
    ) -> "WindowOpQueue":
        """Queue a window maximize operation."""
        self._add_op(WindowOp(
            op_type=WindowOperation.MAXIMIZE,
            window_id=window_id,
        ))
        return self

    def add_restore(
        self,
        window_id: int,
    ) -> "WindowOpQueue":
        """Queue a window restore operation."""
        self._add_op(WindowOp(
            op_type=WindowOperation.RESTORE,
            window_id=window_id,
        ))
        return self

    def add_raise(
        self,
        window_id: int,
    ) -> "WindowOpQueue":
        """Queue a window raise (bring to front) operation."""
        self._add_op(WindowOp(
            op_type=WindowOperation.RAISE,
            window_id=window_id,
        ))
        return self

    def add_center(
        self,
        window_id: int,
    ) -> "WindowOpQueue":
        """Queue a window center operation."""
        self._add_op(WindowOp(
            op_type=WindowOperation.CENTER,
            window_id=window_id,
        ))
        return self

    def _add_op(self, op: WindowOp) -> None:
        """Add an operation to the queue."""
        if self._dedup:
            self._merge_or_add(op)
        else:
            self._queue.append(op)

    def _merge_or_add(self, new_op: WindowOp) -> None:
        """Merge with existing operation or add new one."""
        for existing in self._queue:
            if (existing.window_id == new_op.window_id and
                    existing.op_type == new_op.op_type):
                existing.params.update(new_op.params)
                return
        self._queue.append(new_op)

    def should_flush(self) -> bool:
        """Return True if queue should be flushed."""
        if not self._queue:
            return False
        return time.time() - self._last_flush >= self._batch_interval

    def execute_all(self) -> List[WindowOp]:
        """
        Execute all queued operations.

        Returns:
            List of executed WindowOp objects.
        """
        if not self._queue:
            return []

        self._queue.sort(key=lambda o: -o.priority)
        executed: List[WindowOp] = []

        while self._queue:
            op = self._queue.pop(0)
            if self._executor:
                self._executor(op)
            executed.append(op)

        self._last_flush = time.time()
        return executed

    def on_execute(
        self,
        callback: Callable[[WindowOp], None],
    ) -> None:
        """
        Register a callback for operation execution.

        Args:
            callback: Function called with each executed WindowOp.
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    @property
    def queue_size(self) -> int:
        """Return the current queue size."""
        return len(self._queue)

    def clear(self) -> None:
        """Clear the queue without executing."""
        self._queue.clear()
