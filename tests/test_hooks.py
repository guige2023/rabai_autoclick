"""Tests for hook utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.hooks import HookManager, get_hooks, hook, Hooks


class TestHookManager:
    """Tests for HookManager."""

    def test_register(self) -> None:
        """Test registering a hook."""
        manager = HookManager()
        called = []

        def my_hook():
            called.append(True)

        manager.register("test", my_hook)
        assert manager.has_hooks("test")

    def test_trigger(self) -> None:
        """Test triggering hooks."""
        manager = HookManager()
        results = []

        def my_hook(data):
            results.append(data)

        manager.register("test", my_hook)
        manager.trigger("test", "hello")

        assert len(results) == 1
        assert results[0] == "hello"

    def test_unregister(self) -> None:
        """Test unregistering hooks."""
        manager = HookManager()

        def my_hook():
            pass

        manager.register("test", my_hook)
        manager.unregister("test", my_hook)

        assert not manager.has_hooks("test")

    def test_priority(self) -> None:
        """Test hook priority."""
        manager = HookManager()
        order = []

        def hook1():
            order.append(1)

        def hook2():
            order.append(2)

        manager.register("test", hook2, priority=0)
        manager.register("test", hook1, priority=1)
        manager.trigger("test")

        assert order == [1, 2]

    def test_once(self) -> None:
        """Test once-only hooks."""
        manager = HookManager()
        count = []

        def my_hook():
            count.append(1)

        manager.register("test", my_hook, once=True)
        manager.trigger("test")
        manager.trigger("test")

        assert len(count) == 1


class TestHooks:
    """Tests for Hooks constants."""

    def test_workflow_hooks(self) -> None:
        """Test workflow hook names exist."""
        assert Hooks.WORKFLOW_START == "workflow.start"
        assert Hooks.WORKFLOW_END == "workflow.end"

    def test_action_hooks(self) -> None:
        """Test action hook names exist."""
        assert Hooks.ACTION_BEFORE == "action.before"
        assert Hooks.ACTION_AFTER == "action.after"


class TestGetHooks:
    """Tests for global hooks."""

    def test_get_hooks(self) -> None:
        """Test getting global hook manager."""
        hooks = get_hooks()
        assert isinstance(hooks, HookManager)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])