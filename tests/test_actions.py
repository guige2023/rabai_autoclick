"""Tests for action utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.actions import (
    ActionType,
    ActionResult,
    Action,
    MouseClickAction,
    MouseMoveAction,
    KeyboardPressAction,
    KeyboardTypeAction,
    WaitAction,
    ScreenshotAction,
    ImageMatchAction,
    CustomAction,
    ActionSequence,
    ActionBuilder,
)


class TestActionType:
    """Tests for ActionType."""

    def test_values(self) -> None:
        """Test action type values."""
        assert ActionType.MOUSE_CLICK.value == "mouse_click"
        assert ActionType.KEYBOARD_PRESS.value == "keyboard_press"


class TestActionResult:
    """Tests for ActionResult."""

    def test_success(self) -> None:
        """Test successful result."""
        result = ActionResult(success=True, data="test")
        assert result.success is True
        assert result.data == "test"

    def test_failure(self) -> None:
        """Test failed result."""
        result = ActionResult(success=False, error="fail")
        assert result.success is False
        assert result.error == "fail"


class TestMouseClickAction:
    """Tests for MouseClickAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = MouseClickAction(x=100, y=200)
        assert action.x == 100
        assert action.y == 200
        assert action.action_type == ActionType.MOUSE_CLICK

    def test_validate(self) -> None:
        """Test validation."""
        action = MouseClickAction(x=-1, y=-1)
        errors = action.validate()
        assert len(errors) == 2


class TestMouseMoveAction:
    """Tests for MouseMoveAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = MouseMoveAction(x=100, y=200)
        assert action.action_type == ActionType.MOUSE_MOVE


class TestKeyboardPressAction:
    """Tests for KeyboardPressAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = KeyboardPressAction(key_code=65)
        assert action.key_code == 65
        assert action.action_type == ActionType.KEYBOARD_PRESS


class TestKeyboardTypeAction:
    """Tests for KeyboardTypeAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = KeyboardTypeAction(text="hello")
        assert action.text == "hello"
        assert action.action_type == ActionType.KEYBOARD_TYPE


class TestWaitAction:
    """Tests for WaitAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = WaitAction(duration=1.0)
        assert action.duration == 1.0
        assert action.action_type == ActionType.WAIT

    def test_validate_negative(self) -> None:
        """Test validation of negative duration."""
        action = WaitAction(duration=-1.0)
        errors = action.validate()
        assert len(errors) == 1


class TestScreenshotAction:
    """Tests for ScreenshotAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = ScreenshotAction(path="/tmp/test.png")
        assert action.path == "/tmp/test.png"
        assert action.action_type == ActionType.SCREENSHOT


class TestImageMatchAction:
    """Tests for ImageMatchAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = ImageMatchAction(template_path="/tmp/template.png")
        assert action.template_path == "/tmp/template.png"
        assert action.action_type == ActionType.IMAGE_MATCH


class TestCustomAction:
    """Tests for CustomAction."""

    def test_create(self) -> None:
        """Test creating action."""
        def my_func():
            return 42
        action = CustomAction(name="test", func=my_func)
        assert action.name == "test"
        assert action.action_type == ActionType.CUSTOM

    def test_execute(self) -> None:
        """Test executing custom action."""
        def my_func(x):
            return x * 2
        action = CustomAction(name="test", func=my_func, args=(21,))
        result = action.execute()
        assert result.success is True
        assert result.data == 42


class TestActionSequence:
    """Tests for ActionSequence."""

    def test_create(self) -> None:
        """Test creating sequence."""
        seq = ActionSequence("test")
        assert seq.name == "test"
        assert len(seq._actions) == 0

    def test_add(self) -> None:
        """Test adding action."""
        seq = ActionSequence("test")
        seq.add(WaitAction(duration=0.1))
        assert len(seq._actions) == 1

    def test_execute_all(self) -> None:
        """Test executing all."""
        seq = ActionSequence("test")
        seq.add(WaitAction(duration=0.01))
        seq.add(WaitAction(duration=0.01))
        results = seq.execute_all()
        assert len(results) == 2
        assert all(r.success for r in results)

    def test_validate_all(self) -> None:
        """Test validating all."""
        seq = ActionSequence("test")
        seq.add(MouseClickAction(x=-1, y=-1))
        errors = seq.validate_all()
        assert len(errors) == 2


class TestActionBuilder:
    """Tests for ActionBuilder."""

    def test_click(self) -> None:
        """Test building click action."""
        action = ActionBuilder.click(100, 200)
        assert isinstance(action, MouseClickAction)
        assert action.x == 100

    def test_move(self) -> None:
        """Test building move action."""
        action = ActionBuilder.move(100, 200)
        assert isinstance(action, MouseMoveAction)

    def test_press(self) -> None:
        """Test building press action."""
        action = ActionBuilder.press(65)
        assert isinstance(action, KeyboardPressAction)

    def test_type_text(self) -> None:
        """Test building type action."""
        action = ActionBuilder.type_text("hello")
        assert isinstance(action, KeyboardTypeAction)

    def test_wait(self) -> None:
        """Test building wait action."""
        action = ActionBuilder.wait(1.0)
        assert isinstance(action, WaitAction)

    def test_screenshot(self) -> None:
        """Test building screenshot action."""
        action = ActionBuilder.screenshot("/tmp/test.png")
        assert isinstance(action, ScreenshotAction)

    def test_image_match(self) -> None:
        """Test building image match action."""
        action = ActionBuilder.image_match("/tmp/template.png")
        assert isinstance(action, ImageMatchAction)

    def test_custom(self) -> None:
        """Test building custom action."""
        action = ActionBuilder.custom("test", lambda: None)
        assert isinstance(action, CustomAction)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])