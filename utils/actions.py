"""Action utilities for RabAI AutoClick.

Provides:
- Action definitions
- Action execution
- Action sequences
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class ActionType(Enum):
    """Action types."""
    MOUSE_CLICK = "mouse_click"
    MOUSE_MOVE = "mouse_move"
    MOUSE_DRAG = "mouse_drag"
    KEYBOARD_PRESS = "keyboard_press"
    KEYBOARD_TYPE = "keyboard_type"
    WAIT = "wait"
    SCREENSHOT = "screenshot"
    IMAGE_MATCH = "image_match"
    REGION = "region"
    CUSTOM = "custom"


@dataclass
class ActionResult:
    """Result of action execution."""
    success: bool
    data: Any = None
    error: Optional[str] = None


class Action(ABC):
    """Base action class."""

    @property
    @abstractmethod
    def action_type(self) -> ActionType:
        """Get action type."""
        pass

    @abstractmethod
    def execute(self) -> ActionResult:
        """Execute action.

        Returns:
            Action result.
        """
        pass

    def validate(self) -> List[str]:
        """Validate action.

        Returns:
            List of validation errors.
        """
        return []


@dataclass
class MouseClickAction(Action):
    """Mouse click action."""
    x: int
    y: int
    button: str = "left"
    clicks: int = 1

    @property
    def action_type(self) -> ActionType:
        return ActionType.MOUSE_CLICK

    def execute(self) -> ActionResult:
        try:
            from utils.mouse import MouseSimulator, MouseButton
            btn = MouseButton.LEFT if self.button == "left" else MouseButton.RIGHT
            if self.clicks == 2:
                MouseSimulator.double_click(self.x, self.y, btn)
            else:
                MouseSimulator.click(self.x, self.y, btn)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def validate(self) -> List[str]:
        errors = []
        if self.x < 0:
            errors.append("X coordinate must be non-negative")
        if self.y < 0:
            errors.append("Y coordinate must be non-negative")
        if self.button not in ("left", "right", "middle"):
            errors.append("Invalid button type")
        return errors


@dataclass
class MouseMoveAction(Action):
    """Mouse move action."""
    x: int
    y: int

    @property
    def action_type(self) -> ActionType:
        return ActionType.MOUSE_MOVE

    def execute(self) -> ActionResult:
        try:
            from utils.mouse import MouseSimulator
            MouseSimulator.move(self.x, self.y)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


@dataclass
class KeyboardPressAction(Action):
    """Keyboard press action."""
    key_code: int
    modifiers: List[int] = field(default_factory=list)

    @property
    def action_type(self) -> ActionType:
        return ActionType.KEYBOARD_PRESS

    def execute(self) -> ActionResult:
        try:
            from utils.keyboard import KeySequence
            seq = KeySequence()
            if self.modifiers:
                seq.hold(*self.modifiers)
            seq.type(self.key_code)
            if self.modifiers:
                seq.release_all(*self.modifiers)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


@dataclass
class KeyboardTypeAction(Action):
    """Keyboard type text action."""
    text: str

    @property
    def action_type(self) -> ActionType:
        return ActionType.KEYBOARD_TYPE

    def execute(self) -> ActionResult:
        try:
            from utils.clipboard import Clipboard
            Clipboard.set_text(self.text)
            # Would need to simulate Ctrl+V
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


@dataclass
class WaitAction(Action):
    """Wait action."""
    duration: float

    @property
    def action_type(self) -> ActionType:
        return ActionType.WAIT

    def execute(self) -> ActionResult:
        import time
        try:
            time.sleep(self.duration)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def validate(self) -> List[str]:
        if self.duration < 0:
            return ["Duration must be non-negative"]
        return []


@dataclass
class ScreenshotAction(Action):
    """Screenshot action."""
    path: str
    region: Optional[tuple] = None

    @property
    def action_type(self) -> ActionType:
        return ActionType.SCREENSHOT

    def execute(self) -> ActionResult:
        try:
            from utils.image import Screenshot
            Screenshot.save(self.path, self.region)
            return ActionResult(success=True, data=self.path)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


@dataclass
class ImageMatchAction(Action):
    """Image match action."""
    template_path: str
    region: Optional[tuple] = None
    confidence: float = 0.8

    @property
    def action_type(self) -> ActionType:
        return ActionType.IMAGE_MATCH

    def execute(self) -> ActionResult:
        try:
            from utils.image import ImageMatcher, ImageLoader
            matcher = ImageMatcher(threshold=self.confidence)
            img = ImageLoader.load(self.template_path)
            if img is None:
                return ActionResult(success=False, error="Failed to load template")
            match = matcher.find_on_screen(img, self.region)
            if match:
                return ActionResult(success=True, data=(match.x, match.y))
            return ActionResult(success=False, error="No match found")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


@dataclass
class CustomAction(Action):
    """Custom action with callable."""
    name: str
    func: Callable[[], Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @property
    def action_type(self) -> ActionType:
        return ActionType.CUSTOM

    def execute(self) -> ActionResult:
        try:
            result = self.func(*self.args, **self.kwargs)
            return ActionResult(success=True, data=result)
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class ActionSequence:
    """Sequence of actions."""

    def __init__(self, name: str) -> None:
        """Initialize sequence.

        Args:
            name: Sequence name.
        """
        self.name = name
        self._actions: List[Action] = []

    def add(self, action: Action) -> "ActionSequence":
        """Add action to sequence.

        Args:
            action: Action to add.

        Returns:
            Self for chaining.
        """
        self._actions.append(action)
        return self

    def execute_all(self) -> List[ActionResult]:
        """Execute all actions.

        Returns:
            List of results.
        """
        results = []
        for action in self._actions:
            result = action.execute()
            results.append(result)
            if not result.success:
                break
        return results

    def validate_all(self) -> List[str]:
        """Validate all actions.

        Returns:
            List of validation errors.
        """
        errors = []
        for i, action in enumerate(self._actions):
            action_errors = action.validate()
            for error in action_errors:
                errors.append(f"Action {i}: {error}")
        return errors


class ActionBuilder:
    """Build actions programmatically."""

    @staticmethod
    def click(x: int, y: int, button: str = "left", clicks: int = 1) -> MouseClickAction:
        """Build mouse click action."""
        return MouseClickAction(x=x, y=y, button=button, clicks=clicks)

    @staticmethod
    def move(x: int, y: int) -> MouseMoveAction:
        """Build mouse move action."""
        return MouseMoveAction(x=x, y=y)

    @staticmethod
    def press(key_code: int, modifiers: List[int] = None) -> KeyboardPressAction:
        """Build keyboard press action."""
        return KeyboardPressAction(key_code=key_code, modifiers=modifiers or [])

    @staticmethod
    def type_text(text: str) -> KeyboardTypeAction:
        """Build type text action."""
        return KeyboardTypeAction(text=text)

    @staticmethod
    def wait(duration: float) -> WaitAction:
        """Build wait action."""
        return WaitAction(duration=duration)

    @staticmethod
    def screenshot(path: str, region: tuple = None) -> ScreenshotAction:
        """Build screenshot action."""
        return ScreenshotAction(path=path, region=region)

    @staticmethod
    def image_match(template_path: str, confidence: float = 0.8) -> ImageMatchAction:
        """Build image match action."""
        return ImageMatchAction(template_path=template_path, confidence=confidence)

    @staticmethod
    def custom(name: str, func: Callable, *args, **kwargs) -> CustomAction:
        """Build custom action."""
        return CustomAction(name=name, func=func, args=args, kwargs=kwargs)
