"""Tests for action recording utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.recorder import (
    ActionType,
    RecordedAction,
    Recording,
    ActionRecorder,
    RecordingPlayer,
    RecordingManager,
)


class TestActionType:
    """Tests for ActionType."""

    def test_values(self) -> None:
        """Test action type values."""
        assert ActionType.MOUSE_MOVE.value == "mouse_move"
        assert ActionType.MOUSE_CLICK.value == "mouse_click"
        assert ActionType.KEYBOARD_TYPE.value == "keyboard_type"


class TestRecordedAction:
    """Tests for RecordedAction."""

    def test_create(self) -> None:
        """Test creating action."""
        action = RecordedAction(
            type=ActionType.MOUSE_CLICK,
            timestamp=1.0,
            data={"x": 100, "y": 200},
        )
        assert action.type == ActionType.MOUSE_CLICK
        assert action.data["x"] == 100

    def test_to_dict(self) -> None:
        """Test converting to dict."""
        action = RecordedAction(
            type=ActionType.MOUSE_CLICK,
            timestamp=1.0,
            data={"x": 100},
        )
        d = action.to_dict()
        assert d["type"] == "mouse_click"

    def test_from_dict(self) -> None:
        """Test creating from dict."""
        d = {
            "type": "mouse_click",
            "timestamp": 1.0,
            "data": {"x": 100},
        }
        action = RecordedAction.from_dict(d)
        assert action.type == ActionType.MOUSE_CLICK


class TestRecording:
    """Tests for Recording."""

    def test_create(self) -> None:
        """Test creating recording."""
        recording = Recording("test")
        assert recording.name == "test"
        assert recording.action_count == 0

    def test_add_action(self) -> None:
        """Test adding action."""
        recording = Recording("test")
        recording.add_action(RecordedAction(
            type=ActionType.MOUSE_CLICK,
            timestamp=1.0,
        ))
        assert recording.action_count == 1

    def test_duration(self) -> None:
        """Test duration calculation."""
        recording = Recording("test")
        recording.add_action(RecordedAction(type=ActionType.WAIT, timestamp=0))
        recording.add_action(RecordedAction(type=ActionType.WAIT, timestamp=2.0))
        assert recording.duration == 2.0

    def test_clear(self) -> None:
        """Test clearing."""
        recording = Recording("test")
        recording.add_action(RecordedAction(type=ActionType.WAIT, timestamp=0))
        recording.clear()
        assert recording.action_count == 0

    def test_to_dict(self) -> None:
        """Test converting to dict."""
        recording = Recording("test")
        recording.add_action(RecordedAction(type=ActionType.WAIT, timestamp=1.0))
        d = recording.to_dict()
        assert d["name"] == "test"
        assert len(d["actions"]) == 1


class TestActionRecorder:
    """Tests for ActionRecorder."""

    def test_start_stop(self) -> None:
        """Test starting and stopping recording."""
        recorder = ActionRecorder()
        recorder.start_recording("test")
        assert recorder.is_recording() is True
        recording = recorder.stop_recording()
        assert recording is not None
        assert recorder.is_recording() is False

    def test_record_mouse_move(self) -> None:
        """Test recording mouse move."""
        recorder = ActionRecorder()
        recorder.start_recording("test")
        recorder.record_mouse_move(100, 200)
        recording = recorder.stop_recording()
        assert recording.action_count == 1

    def test_record_mouse_click(self) -> None:
        """Test recording mouse click."""
        recorder = ActionRecorder()
        recorder.start_recording("test")
        recorder.record_mouse_click(100, 200)
        recording = recorder.stop_recording()
        assert recording.action_count == 1

    def test_record_wait(self) -> None:
        """Test recording wait."""
        recorder = ActionRecorder()
        recorder.start_recording("test")
        recorder.record_wait(1.0)
        recording = recorder.stop_recording()
        assert recording.action_count == 1


class TestRecordingPlayer:
    """Tests for RecordingPlayer."""

    def test_create(self) -> None:
        """Test creating player."""
        player = RecordingPlayer()
        assert player.is_running is False

    def test_play_empty(self) -> None:
        """Test playing empty recording."""
        player = RecordingPlayer()
        recording = Recording("test")
        result = player.play(recording)
        assert result is True


class TestRecordingManager:
    """Tests for RecordingManager."""

    def test_add_get(self) -> None:
        """Test adding and getting recording."""
        manager = RecordingManager()
        recording = Recording("test")
        manager.add(recording)
        assert manager.get("test") is recording

    def test_remove(self) -> None:
        """Test removing recording."""
        manager = RecordingManager()
        recording = Recording("test")
        manager.add(recording)
        assert manager.remove("test") is True
        assert manager.get("test") is None

    def test_list_recordings(self) -> None:
        """Test listing recordings."""
        manager = RecordingManager()
        manager.add(Recording("a"))
        manager.add(Recording("b"))
        names = manager.list_recordings()
        assert "a" in names
        assert "b" in names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])