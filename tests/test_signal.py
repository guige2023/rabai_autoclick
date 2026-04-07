"""Tests for signal handling utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.signal import (
    SignalType,
    SignalHandler,
    SignalEmitter,
    SignalRouter,
    SignalMask,
    SignalWaiter,
)


class TestSignalType:
    """Tests for SignalType."""

    def test_values(self) -> None:
        """Test signal type values."""
        assert SignalType.HUP.value == "hup"
        assert SignalType.INT.value == "int"


class TestSignalHandler:
    """Tests for SignalHandler."""

    def test_create(self) -> None:
        """Test creating handler."""
        handler = SignalHandler()
        assert len(handler._handlers) == 0

    def test_register(self) -> None:
        """Test registering handler."""
        handler = SignalHandler()
        handler.register(SignalType.INT, lambda: None)
        assert SignalType.INT in handler._handlers

    def test_unregister(self) -> None:
        """Test unregistering handler."""
        handler = SignalHandler()

        def h():
            pass

        handler.register(SignalType.INT, h)
        handler.unregister(SignalType.INT, h)
        assert SignalType.INT not in handler._handlers


class TestSignalEmitter:
    """Tests for SignalEmitter."""

    def test_create(self) -> None:
        """Test creating emitter."""
        emitter = SignalEmitter()
        assert len(emitter._handlers) == 0

    def test_on_off(self) -> None:
        """Test registering handlers."""
        emitter = SignalEmitter()

        def handler():
            pass

        emitter.on("event", handler)
        assert "event" in emitter._handlers

        emitter.off("event", handler)
        assert "event" not in emitter._handlers

    def test_emit(self) -> None:
        """Test emitting events."""
        emitter = SignalEmitter()
        called = []

        def handler():
            called.append(1)

        emitter.on("event", handler)
        emitter.emit("event")
        assert called == [1]

    def test_clear(self) -> None:
        """Test clearing handlers."""
        emitter = SignalEmitter()
        emitter.on("event1", lambda: None)
        emitter.on("event2", lambda: None)
        emitter.clear()
        assert len(emitter._handlers) == 0


class TestSignalRouter:
    """Tests for SignalRouter."""

    def test_create(self) -> None:
        """Test creating router."""
        router = SignalRouter()
        assert len(router._routes) == 0

    def test_add_route(self) -> None:
        """Test adding route."""
        router = SignalRouter()
        router.add_route("test", lambda: None)
        assert "test" in router._routes

    def test_remove_route(self) -> None:
        """Test removing route."""
        router = SignalRouter()
        router.add_route("test", lambda: None)
        router.remove_route("test")
        assert "test" not in router._routes

    def test_route(self) -> None:
        """Test routing signal."""
        router = SignalRouter()
        called = []

        def handler():
            called.append(1)

        router.add_route("test", handler)
        result = router.route("test")
        assert result is True
        assert called == [1]


class TestSignalMask:
    """Tests for SignalMask."""

    def test_get_masked(self) -> None:
        """Test getting masked signals."""
        masked = SignalMask.get_masked()
        assert isinstance(masked, set)


class TestSignalWaiter:
    """Tests for SignalWaiter."""

    def test_create(self) -> None:
        """Test creating waiter."""
        waiter = SignalWaiter()
        assert len(waiter._signals) == 0

    def test_wait_for_timeout(self) -> None:
        """Test waiting with timeout."""
        waiter = SignalWaiter()
        result = waiter.wait_for(1, timeout=0.1)
        assert result is False

    def test_signal(self) -> None:
        """Test signaling."""
        waiter = SignalWaiter()
        waiter._signals[1] = __import__("threading").Event()
        waiter.signal(1)
        assert waiter._signals[1].is_set()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])