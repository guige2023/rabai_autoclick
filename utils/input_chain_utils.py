"""
Input chain utilities for composing complex input sequences.

This module provides utilities for building and executing
chained input sequences combining keyboard, mouse, and touch events.
"""

from __future__ import annotations

import time
from typing import List, Tuple, Optional, Dict, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from contextlib import contextmanager


class InputActionType(Enum):
    """Types of input actions in a chain."""
    KEY_PRESS = auto()
    KEY_RELEASE = auto()
    KEY_TAP = auto()
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DRAG = auto()
    MOUSE_SCROLL = auto()
    WAIT = auto()
    CONDITIONAL = auto()
    REPEAT = auto()
    LABEL = auto()
    GOTO = auto()


@dataclass
class InputAction:
    """
    A single input action in a chain.

    Attributes:
        action_type: Type of action.
        x: X coordinate for mouse/touch actions.
        y: Y coordinate for mouse/touch actions.
        keycode: Key code for keyboard actions.
        key: Key name for keyboard actions.
        button: Mouse button for click actions.
        delta: Scroll delta for scroll actions.
        duration: Duration in seconds for waits/delays.
        times: Number of times to repeat.
        label: Label for goto/conditional actions.
        condition: Callable condition for conditional actions.
        metadata: Additional action data.
    """
    action_type: InputActionType
    x: int = 0
    y: int = 0
    keycode: Optional[int] = None
    key: Optional[str] = None
    button: str = 'left'
    delta: float = 0.0
    duration: float = 0.0
    times: int = 1
    label: Optional[str] = None
    condition: Optional[Callable[[], bool]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"InputAction({self.action_type.name}, x={self.x}, y={self.y}, key={self.key})"


@dataclass
class InputChain:
    """
    A chain of input actions to be executed sequentially.

    Attributes:
        name: Name of the chain.
        actions: List of input actions.
    """
    name: str = "unnamed"
    actions: List[InputAction] = field(default_factory=list)

    def key_press(self, key: Union[str, int]) -> InputChain:
        """Add a key press action."""
        if isinstance(key, str):
            self.actions.append(InputAction(
                InputActionType.KEY_PRESS, key=key
            ))
        else:
            self.actions.append(InputAction(
                InputActionType.KEY_PRESS, keycode=key
            ))
        return self

    def key_release(self, key: Union[str, int]) -> InputChain:
        """Add a key release action."""
        if isinstance(key, str):
            self.actions.append(InputAction(
                InputActionType.KEY_RELEASE, key=key
            ))
        else:
            self.actions.append(InputAction(
                InputActionType.KEY_RELEASE, keycode=key
            ))
        return self

    def key_tap(self, key: Union[str, int]) -> InputChain:
        """Add a key tap (press + release) action."""
        if isinstance(key, str):
            self.actions.append(InputAction(
                InputActionType.KEY_TAP, key=key
            ))
        else:
            self.actions.append(InputAction(
                InputActionType.KEY_TAP, keycode=key
            ))
        return self

    def mouse_move(self, x: int, y: int) -> InputChain:
        """Add a mouse move action."""
        self.actions.append(InputAction(
            InputActionType.MOUSE_MOVE, x=x, y=y
        ))
        return self

    def mouse_click(
        self, x: int, y: int, button: str = 'left'
    ) -> InputChain:
        """Add a mouse click action."""
        self.actions.append(InputAction(
            InputActionType.MOUSE_CLICK, x=x, y=y, button=button
        ))
        return self

    def mouse_drag(
        self, x1: int, y1: int, x2: int, y2: int,
        button: str = 'left'
    ) -> InputChain:
        """Add a mouse drag action (press, move, release)."""
        self.actions.append(InputAction(
            InputActionType.MOUSE_DRAG, x=x1, y=y1, button=button,
            metadata={'end_x': x2, 'end_y': y2}
        ))
        return self

    def mouse_scroll(self, x: int, y: int, delta: float) -> InputChain:
        """Add a mouse scroll action."""
        self.actions.append(InputAction(
            InputActionType.MOUSE_SCROLL, x=x, y=y, delta=delta
        ))
        return self

    def wait(self, duration: float) -> InputChain:
        """Add a wait action."""
        self.actions.append(InputAction(
            InputActionType.WAIT, duration=duration
        ))
        return self

    def repeat(self, times: int) -> InputChain:
        """Add a repeat marker (affects previous action)."""
        if self.actions:
            self.actions[-1].times = times
        return self

    def label(self, name: str) -> InputChain:
        """Add a label for GOTO."""
        self.actions.append(InputAction(
            InputActionType.LABEL, label=name
        ))
        return self

    def goto(self, label: str) -> InputChain:
        """Add a GOTO action."""
        self.actions.append(InputAction(
            InputActionType.GOTO, label=label
        ))
        return self

    def conditional(
        self, condition: Callable[[], bool], label: str
    ) -> InputChain:
        """Add a conditional goto action."""
        self.actions.append(InputAction(
            InputActionType.CONDITIONAL,
            condition=condition,
            label=label
        ))
        return self

    def then(self, other: InputChain) -> InputChain:
        """Concatenate another chain."""
        self.actions.extend(other.actions)
        return self

    def clear(self) -> None:
        """Clear all actions."""
        self.actions.clear()

    @property
    def duration_estimate(self) -> float:
        """Estimate total duration in seconds."""
        total = 0.0
        for action in self.actions:
            if action.action_type == InputActionType.WAIT:
                total += action.duration
            elif action.action_type == InputActionType.MOUSE_DRAG:
                total += 0.5  # Assume 500ms drag
        return total


class InputChainExecutor:
    """
    Executes InputChain sequences with support for
    pause, resume, and interruption.
    """

    def __init__(self):
        """Initialize the executor."""
        self._is_paused = False
        self._is_running = False
        self._labels: Dict[str, int] = {}
        self._log: List[str] = []
        self._step_delay: float = 0.01

    def set_step_delay(self, delay: float) -> None:
        """Set delay between steps."""
        self._step_delay = delay

    def execute(self, chain: InputChain) -> None:
        """
        Execute an input chain.

        Args:
            chain: The InputChain to execute.
        """
        self._is_running = True
        self._is_paused = False
        self._build_labels(chain)

        idx = 0
        while idx < len(chain.actions) and self._is_running:
            if self._is_paused:
                time.sleep(0.05)
                continue

            action = chain.actions[idx]
            self._log_action(action)

            if action.action_type == InputActionType.GOTO:
                idx = self._labels.get(action.label, idx + 1)
            elif action.action_type == InputActionType.CONDITIONAL:
                if action.condition and action.condition():
                    idx = self._labels.get(action.label, idx + 1)
                else:
                    idx += 1
            else:
                self._execute_action(action)
                idx += 1

            time.sleep(self._step_delay)

        self._is_running = False

    def _build_labels(self, chain: InputChain) -> None:
        """Build label index for goto."""
        self._labels.clear()
        for i, action in enumerate(chain.actions):
            if action.action_type == InputActionType.LABEL:
                if action.label:
                    self._labels[action.label] = i

    def _execute_action(self, action: InputAction) -> None:
        """Execute a single action."""
        if action.action_type == InputActionType.KEY_TAP:
            self._do_key(action, press=True)
            time.sleep(0.05)
            self._do_key(action, press=False)
        elif action.action_type == InputActionType.KEY_PRESS:
            self._do_key(action, press=True)
        elif action.action_type == InputActionType.KEY_RELEASE:
            self._do_key(action, press=False)
        elif action.action_type == InputActionType.MOUSE_MOVE:
            self._do_mouse_move(action.x, action.y)
        elif action.action_type == InputActionType.MOUSE_CLICK:
            self._do_mouse_click(action.x, action.y, action.button)
        elif action.action_type == InputActionType.MOUSE_DRAG:
            self._do_mouse_drag(action, action.times)
        elif action.action_type == InputActionType.MOUSE_SCROLL:
            self._do_scroll(action.x, action.y, action.delta)
        elif action.action_type == InputActionType.WAIT:
            time.sleep(action.duration)

    def _do_key(self, action: InputAction, press: bool) -> None:
        """Execute a key action."""
        import platform
        if platform.system() == 'Darwin':
            import Quartz
            keycode = action.keycode or self._key_name_to_code(action.key)
            if keycode:
                e_type = Quartz.kCGEventKeyDown if press else Quartz.kCGEventKeyUp
                e = Quartz.CGEventCreateKeyboardEvent(None, keycode, press)
                if e:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)

    def _key_name_to_code(self, key: Optional[str]) -> Optional[int]:
        """Convert key name to keycode."""
        if not key:
            return None
        key_map = {
            'return': 36, 'tab': 48, 'escape': 53,
            'space': 49, 'delete': 51,
            'up': 126, 'down': 125, 'left': 123, 'right': 124,
        }
        return key_map.get(key.lower())

    def _do_mouse_move(self, x: int, y: int) -> None:
        """Execute mouse move."""
        import platform
        if platform.system() == 'Darwin':
            import Quartz
            e = Quartz.CGEventCreateMouseEvent(
                None, Quartz.kCGEventMouseMoved, (x, y),
                Quartz.kCGMouseButtonLeft
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)

    def _do_mouse_click(self, x: int, y: int, button: str) -> None:
        """Execute mouse click."""
        import platform
        if platform.system() == 'Darwin':
            import Quartz
            btn = {
                'left': Quartz.kCGEventLeftMouseDown,
                'right': Quartz.kCGEventRightMouseDown,
            }.get(button, Quartz.kCGEventLeftMouseDown)
            up = {
                'left': Quartz.kCGEventLeftMouseUp,
                'right': Quartz.kCGEventRightMouseUp,
            }.get(button, Quartz.kCGEventLeftMouseUp)
            e1 = Quartz.CGEventCreateMouseEvent(None, btn, (x, y), Quartz.kCGMouseButtonLeft)
            e2 = Quartz.CGEventCreateMouseEvent(None, up, (x, y), Quartz.kCGMouseButtonLeft)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e1)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e2)

    def _do_mouse_drag(self, action: InputAction, times: int) -> None:
        """Execute mouse drag."""
        import platform
        if platform.system() == 'Darwin':
            import Quartz
            end_x = action.metadata.get('end_x', action.x)
            end_y = action.metadata.get('end_y', action.y)
            for _ in range(times):
                e1 = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDown,
                    (action.x, action.y), Quartz.kCGMouseButtonLeft
                )
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e1)
                time.sleep(0.05)
                e2 = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDragged,
                    (end_x, end_y), Quartz.kCGMouseButtonLeft
                )
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e2)
                time.sleep(0.1)
                e3 = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseUp,
                    (end_x, end_y), Quartz.kCGMouseButtonLeft
                )
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e3)

    def _do_scroll(self, x: int, y: int, delta: float) -> None:
        """Execute scroll."""
        import platform
        if platform.system() == 'Darwin':
            import Quartz
            e = Quartz.CGEventCreateScrollMouseEvent(
                None, Quartz.kCGScrollMouseEventVersion,
                Quartz.kCGEventSourceStateID, int(delta), 0, 0
            )
            if e:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)

    def _log_action(self, action: InputAction) -> None:
        """Log an action."""
        self._log.append(f"{action.action_type.name}: {action}")

    def pause(self) -> None:
        """Pause execution."""
        self._is_paused = True

    def resume(self) -> None:
        """Resume execution."""
        self._is_paused = False

    def stop(self) -> None:
        """Stop execution."""
        self._is_running = False
        self._is_paused = False

    def get_log(self) -> List[str]:
        """Get action log."""
        return self._log.copy()

    @contextmanager
    def paused(self):
        """Context manager for temporarily pausing."""
        self.pause()
        try:
            yield
        finally:
            self.resume()


def create_standard_shortcut_chain(
    keys: List[str],
    modifiers: Optional[List[str]] = None
) -> InputChain:
    """
    Create a chain for a keyboard shortcut.

    Args:
        keys: List of key names.
        modifiers: List of modifier keys ('cmd', 'shift', 'alt', 'ctrl').

    Returns:
        InputChain for the shortcut.
    """
    chain = InputChain(name=f"shortcut-{'+'.join(keys)}")
    if modifiers:
        for mod in modifiers:
            chain.key_press(mod)
    for key in keys:
        chain.key_tap(key)
    if modifiers:
        for mod in reversed(modifiers):
            chain.key_release(mod)
    return chain
