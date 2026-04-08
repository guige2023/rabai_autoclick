"""
Input Batch Controller.

Control and coordinate batched input operations including
debouncing, rate limiting, and sequential execution of input sequences.

Usage:
    from utils.input_batch_controller import InputBatchController

    controller = InputBatchController()
    controller.add_mouse_move(x, y)
    controller.add_mouse_click()
    controller.execute_all()
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto
import time

if TYPE_CHECKING:
    pass


class InputActionType(Enum):
    """Types of input actions."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DRAG = auto()
    KEY_PRESS = auto()
    KEY_TYPE = auto()
    SCROLL = auto()
    WAIT = auto()


@dataclass
class InputAction:
    """A single input action."""
    action_type: InputActionType
    params: Dict[str, Any] = field(default_factory=dict)
    delay_after: float = 0.0
    timestamp: float = field(default_factory=time.time)

    def __repr__(self) -> str:
        return f"InputAction({self.action_type.name}, {self.params})"


class InputBatchController:
    """
    Controller for batching and executing input actions.

    Supports queuing multiple input operations and executing
    them in sequence with optional debouncing and rate limiting.

    Example:
        controller = InputBatchController(bridge)
        controller.add_mouse_move(100, 200)
        controller.add_mouse_click()
        controller.execute_all()
    """

    def __init__(
        self,
        bridge: Optional[Any] = None,
        default_delay: float = 0.05,
        enable_debounce: bool = True,
        debounce_threshold: float = 0.1,
    ) -> None:
        """
        Initialize the batch controller.

        Args:
            bridge: Optional AccessibilityBridge for executing actions.
            default_delay: Default delay between actions.
            enable_debounce: Whether to debounce similar consecutive actions.
            debounce_threshold: Time window for debouncing.
        """
        self._bridge = bridge
        self._default_delay = default_delay
        self._enable_debounce = enable_debounce
        self._debounce_threshold = debounce_threshold
        self._queue: List[InputAction] = []
        self._last_action_time: Dict[InputActionType, float] = {}
        self._executor: Optional[Callable[[InputAction], None]] = None

    def set_bridge(self, bridge: Any) -> None:
        """Set the bridge for executing actions."""
        self._bridge = bridge

    def set_executor(
        self,
        executor: Callable[[InputAction], None],
    ) -> None:
        """
        Set a custom executor function for actions.

        Args:
            executor: Function that takes an InputAction and executes it.
        """
        self._executor = executor

    def add_mouse_move(
        self,
        x: int,
        y: int,
        delay_after: Optional[float] = None,
    ) -> "InputBatchController":
        """
        Add a mouse move action.

        Args:
            x: Target X coordinate.
            y: Target Y coordinate.
            delay_after: Optional delay after this action.

        Returns:
            Self for chaining.
        """
        action = InputAction(
            action_type=InputActionType.MOUSE_MOVE,
            params={"x": x, "y": y},
            delay_after=delay_after if delay_after is not None else self._default_delay,
        )
        self._add_action(action)
        return self

    def add_mouse_click(
        self,
        button: str = "left",
        delay_after: Optional[float] = None,
    ) -> "InputBatchController":
        """Add a mouse click action."""
        action = InputAction(
            action_type=InputActionType.MOUSE_CLICK,
            params={"button": button},
            delay_after=delay_after if delay_after is not None else self._default_delay,
        )
        self._add_action(action)
        return self

    def add_mouse_double_click(
        self,
        button: str = "left",
    ) -> "InputBatchController":
        """Add a mouse double click action."""
        self._add_action(InputAction(
            action_type=InputActionType.MOUSE_DOUBLE_CLICK,
            params={"button": button},
            delay_after=self._default_delay,
        ))
        return self

    def add_mouse_right_click(
        self,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> "InputBatchController":
        """Add a right click action."""
        params = {"button": "right"}
        if x is not None:
            params["x"] = x
        if y is not None:
            params["y"] = y

        self._add_action(InputAction(
            action_type=InputActionType.MOUSE_RIGHT_CLICK,
            params=params,
            delay_after=self._default_delay,
        ))
        return self

    def add_mouse_drag(
        self,
        start: Tuple[int, int],
        end: Tuple[int, int],
        duration: float = 0.5,
    ) -> "InputBatchController":
        """Add a mouse drag action."""
        self._add_action(InputAction(
            action_type=InputActionType.MOUSE_DRAG,
            params={"start": start, "end": end, "duration": duration},
            delay_after=self._default_delay,
        ))
        return self

    def add_key_press(
        self,
        key: str,
        modifiers: Optional[List[str]] = None,
    ) -> "InputBatchController":
        """Add a key press action."""
        self._add_action(InputAction(
            action_type=InputActionType.KEY_PRESS,
            params={"key": key, "modifiers": modifiers or []},
            delay_after=self._default_delay,
        ))
        return self

    def add_key_type(
        self,
        text: str,
    ) -> "InputBatchController":
        """Add a text typing action."""
        self._add_action(InputAction(
            action_type=InputActionType.KEY_TYPE,
            params={"text": text},
            delay_after=0.01,
        ))
        return self

    def add_scroll(
        self,
        dx: int = 0,
        dy: int = 0,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> "InputBatchController":
        """Add a scroll action."""
        self._add_action(InputAction(
            action_type=InputActionType.SCROLL,
            params={"dx": dx, "dy": dy, "x": x, "y": y},
            delay_after=self._default_delay,
        ))
        return self

    def add_wait(
        self,
        seconds: float,
    ) -> "InputBatchController":
        """Add a wait/delay action."""
        self._add_action(InputAction(
            action_type=InputActionType.WAIT,
            params={"seconds": seconds},
            delay_after=0,
        ))
        return self

    def _add_action(self, action: InputAction) -> None:
        """Add an action to the queue with optional debouncing."""
        if self._enable_debounce and self._should_debounce(action):
            return

        self._queue.append(action)
        self._last_action_time[action.action_type] = time.time()

    def _should_debounce(self, action: InputAction) -> bool:
        """Check if an action should be debounced."""
        last_time = self._last_action_time.get(action.action_type)
        if last_time is None:
            return False

        elapsed = time.time() - last_time
        return elapsed < self._debounce_threshold

    def execute_all(self) -> List[InputAction]:
        """
        Execute all queued actions in sequence.

        Returns:
            List of executed InputAction objects.
        """
        executed: List[InputAction] = []

        while self._queue:
            action = self._queue.pop(0)
            self._execute_action(action)
            executed.append(action)

            if action.delay_after > 0:
                time.sleep(action.delay_after)

        return executed

    def _execute_action(self, action: InputAction) -> None:
        """Execute a single input action."""
        if self._executor:
            self._executor(action)
            return

        if self._bridge is None:
            return

        try:
            at = action.action_type
            p = action.params

            if at == InputActionType.MOUSE_MOVE:
                self._bridge.move_mouse_to(p["x"], p["y"])
            elif at == InputActionType.MOUSE_CLICK:
                self._bridge.click_at(p.get("x", 0), p.get("y", 0))
            elif at == InputActionType.MOUSE_DOUBLE_CLICK:
                self._bridge.double_click_at(p.get("x", 0), p.get("y", 0))
            elif at == InputActionType.MOUSE_RIGHT_CLICK:
                x = p.get("x", 0)
                y = p.get("y", 0)
                self._bridge.right_click_at(x, y)
            elif at == InputActionType.MOUSE_DRAG:
                self._bridge.drag(p["start"], p["end"], p.get("duration", 0.5))
            elif at == InputActionType.KEY_PRESS:
                modifiers = p.get("modifiers", [])
                key = p["key"]
                if modifiers:
                    self._bridge.send_key(key, modifiers=modifiers)
                else:
                    self._bridge.send_key(key)
            elif at == InputActionType.KEY_TYPE:
                self._bridge.type_text(p["text"])
            elif at == InputActionType.SCROLL:
                self._bridge.scroll(p.get("dx", 0), p.get("dy", 0),
                                   p.get("x"), p.get("y"))
            elif at == InputActionType.WAIT:
                time.sleep(p["seconds"])
        except Exception:
            pass

    @property
    def queue_size(self) -> int:
        """Return the current queue size."""
        return len(self._queue)

    def clear(self) -> None:
        """Clear the queue without executing."""
        self._queue.clear()
