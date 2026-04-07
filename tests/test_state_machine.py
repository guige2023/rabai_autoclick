"""Tests for state machine utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.state_machine import (
    StateMachine,
    create_simple_state_machine,
)


class TestStateMachine:
    """Tests for StateMachine."""

    def test_initial_state(self) -> None:
        """Test initial state is set."""
        sm = StateMachine("idle")
        assert sm.current_state == "idle"

    def test_add_state(self) -> None:
        """Test adding states."""
        sm = StateMachine("idle")
        sm.add_state("running")
        sm.add_state("stopped")

    def test_add_transition(self) -> None:
        """Test adding transitions."""
        sm = StateMachine("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")

    def test_send_event(self) -> None:
        """Test sending events."""
        sm = StateMachine("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")

        result = sm.send_event("start")
        assert result is True
        assert sm.current_state == "running"

    def test_invalid_event(self) -> None:
        """Test invalid event returns False."""
        sm = StateMachine("idle")
        result = sm.send_event("unknown_event")
        assert result is False

    def test_reset(self) -> None:
        """Test resetting state machine."""
        sm = StateMachine("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")

        sm.send_event("start")
        sm.reset()
        assert sm.current_state == "idle"

    def test_get_available_events(self) -> None:
        """Test getting available events."""
        sm = StateMachine("idle")
        sm.add_state("running")
        sm.add_transition("idle", "running", "start")
        sm.add_transition("running", "idle", "stop")

        events = sm.get_available_events()
        assert "start" in events

    def test_is_in_state(self) -> None:
        """Test state checking."""
        sm = StateMachine("idle")
        assert sm.is_in_state("idle")
        assert not sm.is_in_state("running")


class TestCreateSimpleStateMachine:
    """Tests for create_simple_state_machine."""

    def test_create_simple(self) -> None:
        """Test creating simple state machine."""
        sm = create_simple_state_machine(
            states=["idle", "running"],
            transitions={("idle", "start"): "running"},
            initial="idle",
        )
        assert sm.current_state == "idle"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])