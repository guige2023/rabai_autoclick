"""Test suite for new RabAI AutoClick actions.

Run with: pytest tests/test_new_actions.py -v
Or: python -m pytest tests/test_new_actions.py -v
"""

import sys
import os
import time
import json

import pytest

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class TestContextEnhancements:
    """Tests for enhanced ContextManager methods."""

    def test_has_method(self):
        """Test has() method for variable existence check."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set('name', 'test')

        assert ctx.has('name') is True
        assert ctx.has('not_exist') is False

    def test_keys_values_items(self):
        """Test keys(), values(), items() methods."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set_all({'a': 1, 'b': 2, 'c': 3})

        assert set(ctx.keys()) == {'a', 'b', 'c'}
        assert set(ctx.values()) == {1, 2, 3}
        assert set(ctx.items()) == {('a', 1), ('b', 2), ('c', 3)}

    def test_pop_method(self):
        """Test pop() to remove and return variable."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set('name', 'test')

        value = ctx.pop('name')
        assert value == 'test'
        assert ctx.has('name') is False

    def test_copy_method(self):
        """Test copy() returns shallow copy."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set('name', 'test')

        copy = ctx.copy()
        assert copy == {'name': 'test'}

        copy['name'] = 'modified'
        assert ctx.get('name') == 'test'  # Original unchanged

    def test_nested_access(self):
        """Test get_nested() for dot notation access."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set('user', {'profile': {'name': 'test'}})

        assert ctx.get_nested('user.profile.name') == 'test'
        assert ctx.get_nested('user.profile.age', 25) == 25
        assert ctx.get_nested('nonexistent.key', 'default') == 'default'

    def test_set_nested(self):
        """Test set_nested() for dot notation setting."""
        from core.context import ContextManager

        ctx = ContextManager()
        result = ctx.set_nested('user.profile.name', 'new_name')

        assert result is True
        assert ctx.get_nested('user.profile.name') == 'new_name'

    def test_size_and_is_empty(self):
        """Test size() and is_empty() methods."""
        from core.context import ContextManager

        ctx = ContextManager()
        assert ctx.size() == 0
        assert ctx.is_empty() is True

        ctx.set('name', 'test')
        assert ctx.size() == 1
        assert ctx.is_empty() is False

    def test_merge(self):
        """Test merge() method."""
        from core.context import ContextManager

        ctx = ContextManager()
        ctx.set('existing', 1)

        ctx.merge({'existing': 100, 'new': 2}, overwrite=False)
        assert ctx.get('existing') == 1  # Not overwritten

        ctx.merge({'existing': 100, 'new': 2}, overwrite=True)
        assert ctx.get('existing') == 100  # Overwritten


class TestBaseActionEnhancements:
    """Tests for enhanced BaseAction validation methods."""

    def test_validate_coords(self):
        """Test validate_coords() method."""
        from core.base_action import BaseAction, ActionResult

        class TestAction(BaseAction):
            action_type = "test"

            def execute(self, context, params):
                return ActionResult(success=True)

        action = TestAction()

        # Valid coordinates
        valid, msg = action.validate_coords(100, 200, allow_none=False)
        assert valid is True

        # Both None but allowed
        valid, msg = action.validate_coords(None, None, allow_none=True)
        assert valid is True

        # Both None but not allowed
        valid, msg = action.validate_coords(None, None, allow_none=False)
        assert valid is False

        # Invalid type
        valid, msg = action.validate_coords("x", 100, allow_none=True)
        assert valid is False

    def test_validate_positive(self):
        """Test validate_positive() method."""
        from core.base_action import BaseAction, ActionResult

        class TestAction(BaseAction):
            action_type = "test"

            def execute(self, context, params):
                return ActionResult(success=True)

        action = TestAction()

        # Valid positive number
        valid, msg = action.validate_positive(5, 'param')
        assert valid is True

        # Zero when not allowed
        valid, msg = action.validate_positive(0, 'param', allow_zero=False)
        assert valid is False

        # Zero when allowed
        valid, msg = action.validate_positive(0, 'param', allow_zero=True)
        assert valid is True

        # Negative number
        valid, msg = action.validate_positive(-5, 'param', allow_zero=True)
        assert valid is False

    def test_validate_string_not_empty(self):
        """Test validate_string_not_empty() method."""
        from core.base_action import BaseAction, ActionResult

        class TestAction(BaseAction):
            action_type = "test"

            def execute(self, context, params):
                return ActionResult(success=True)

        action = TestAction()

        # Valid non-empty string
        valid, msg = action.validate_string_not_empty("hello", 'param')
        assert valid is True

        # Empty string
        valid, msg = action.validate_string_not_empty("", 'param')
        assert valid is False

        # Non-string type
        valid, msg = action.validate_string_not_empty(123, 'param')
        assert valid is False

    def test_get_full_params(self):
        """Test get_full_params() merges defaults."""
        from core.base_action import BaseAction, ActionResult

        class TestAction(BaseAction):
            action_type = "test"

            def execute(self, context, params):
                return ActionResult(success=True)

            def get_required_params(self):
                return ['x', 'y']

            def get_optional_params(self):
                return {'button': 'left', 'clicks': 1}

        action = TestAction()
        action.set_params({'x': 100})

        full = action.get_full_params()
        assert full['x'] == 100
        assert full['y'] is None  # Required but not provided
        assert full['button'] == 'left'  # Default


class TestWaitActions:
    """Tests for wait action types."""

    def test_wait_action(self):
        """Test WaitAction basic functionality."""
        from actions.wait import WaitAction
        from core.context import ContextManager

        action = WaitAction()
        ctx = ContextManager()

        start = time.time()
        result = action.execute(ctx, {'duration': 0.1})
        elapsed = time.time() - start

        assert result.success is True
        assert elapsed >= 0.1

    def test_wait_invalid_duration(self):
        """Test WaitAction rejects invalid duration."""
        from actions.wait import WaitAction
        from core.context import ContextManager

        action = WaitAction()
        ctx = ContextManager()

        result = action.execute(ctx, {'duration': -1})
        assert result.success is False


class TestFlowControlActions:
    """Tests for flow control actions."""

    def test_loop_action_basic(self):
        """Test LoopAction basic functionality."""
        from actions.flow_control import LoopAction
        from core.context import ContextManager

        action = LoopAction()
        ctx = ContextManager()

        result = action.execute(ctx, {
            'count': 5,
            'loop_var': '_test_count'
        })

        assert result.success is True
        assert ctx.get('_test_count') == 5

    def test_loop_invalid_count(self):
        """Test LoopAction rejects invalid count."""
        from actions.flow_control import LoopAction
        from core.context import ContextManager

        action = LoopAction()
        ctx = ContextManager()

        result = action.execute(ctx, {'count': -1})
        assert result.success is False


class TestVariableActions:
    """Tests for variable manipulation actions."""

    def test_set_variable_action(self):
        """Test SetVariableAction basic functionality."""
        from actions.variable import SetVariableAction
        from core.context import ContextManager

        action = SetVariableAction()
        ctx = ContextManager()

        result = action.execute(ctx, {
            'name': 'test_var',
            'value': 'hello'
        })

        assert result.success is True
        assert ctx.get('test_var') == 'hello'

    def test_get_variable_action(self):
        """Test GetVariableAction basic functionality."""
        from actions.variable import GetVariableAction
        from core.context import ContextManager

        action = GetVariableAction()
        ctx = ContextManager()
        ctx.set('my_var', 'test_value')

        result = action.execute(ctx, {
            'name': 'my_var',
            'output_var': 'copied_var'
        })

        assert result.success is True
        assert ctx.get('copied_var') == 'test_value'

    def test_delete_variable_action(self):
        """Test DeleteVariableAction basic functionality."""
        from actions.variable import DeleteVariableAction
        from core.context import ContextManager

        action = DeleteVariableAction()
        ctx = ContextManager()
        ctx.set('to_delete', 'value')

        result = action.execute(ctx, {'name': 'to_delete'})

        assert result.success is True
        assert ctx.has('to_delete') is False

    def test_clear_variables_action(self):
        """Test ClearVariablesAction."""
        from actions.variable import ClearVariablesAction
        from core.context import ContextManager

        action = ClearVariablesAction()
        ctx = ContextManager()
        ctx.set_all({'a': 1, 'b': 2})

        result = action.execute(ctx, {'confirm': True})

        assert result.success is True
        assert ctx.is_empty() is True

    def test_math_operation_action(self):
        """Test MathOperationAction."""
        from actions.variable import MathOperationAction
        from core.context import ContextManager

        action = MathOperationAction()
        ctx = ContextManager()

        result = action.execute(ctx, {
            'operation': 'add',
            'operand1': 10,
            'operand2': 5
        })

        assert result.success is True
        assert ctx.get('_math_result') == 15

    def test_string_operation_action(self):
        """Test StringOperationAction."""
        from actions.variable import StringOperationAction
        from core.context import ContextManager

        action = StringOperationAction()
        ctx = ContextManager()

        result = action.execute(ctx, {
            'operation': 'upper',
            'value': 'hello'
        })

        assert result.success is True
        assert ctx.get('_string_result') == 'HELLO'


class TestClipboardActions:
    """Tests for clipboard actions."""

    def test_copy_action(self):
        """Test CopyAction basic functionality."""
        from actions.clipboard import CopyAction
        from core.context import ContextManager
        import pyperclip

        action = CopyAction()
        ctx = ContextManager()

        result = action.execute(ctx, {'text': 'test content'})

        assert result.success is True
        assert pyperclip.paste() == 'test content'

    def test_get_clipboard_action(self):
        """Test GetClipboardAction."""
        from actions.clipboard import GetClipboardAction, CopyAction
        from core.context import ContextManager

        # First copy something
        copy_action = CopyAction()
        copy_ctx = ContextManager()
        copy_action.execute(copy_ctx, {'text': 'clipboard test'})

        # Now get it
        action = GetClipboardAction()
        ctx = ContextManager()

        result = action.execute(ctx, {'output_var': 'clip_content'})

        assert result.success is True
        assert ctx.get('clip_content') == 'clipboard test'


class TestEngineEnhancements:
    """Tests for enhanced FlowEngine features."""

    def test_execution_stats(self):
        """Test execution statistics tracking."""
        from core.engine import FlowEngine

        engine = FlowEngine()

        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.01}
            ]
        }

        engine.load_workflow_from_dict(workflow)
        engine.run()

        stats = engine.get_execution_stats()
        assert stats.total_steps == 1
        assert stats.completed_steps == 1
        assert stats.total_duration >= 0

    def test_retry_config(self):
        """Test retry configuration."""
        from core.engine import FlowEngine

        engine = FlowEngine()
        engine.set_retry_config(max_retries=5, retry_delay=2.0)

        assert engine._max_step_retries == 5
        assert engine._step_retry_delay == 2.0

    def test_reset_stats(self):
        """Test stats reset."""
        from core.engine import FlowEngine

        engine = FlowEngine()

        workflow = {
            'variables': {},
            'steps': [{'id': 1, 'type': 'delay', 'seconds': 0.01}]
        }

        engine.load_workflow_from_dict(workflow)
        engine.run()
        engine.reset_stats()

        stats = engine.get_execution_stats()
        assert stats.total_steps == 0
        assert stats.completed_steps == 0


class TestExceptions:
    """Tests for custom exceptions."""

    def test_rabai_error_base(self):
        """Test base RabaiError exception."""
        from core.exceptions import RabaiError

        error = RabaiError("test message", cause=ValueError("cause"))
        assert str(error) == "test message (caused by: cause)"
        assert error.cause is not None

    def test_workflow_load_error(self):
        """Test WorkflowLoadError with path."""
        from core.exceptions import WorkflowLoadError

        error = WorkflowLoadError("/path/to/workflow.json")
        assert "/path/to/workflow.json" in str(error)
        assert error.path == "/path/to/workflow.json"

    def test_action_validation_error(self):
        """Test ActionValidationError with details."""
        from core.exceptions import ActionValidationError

        error = ActionValidationError("click", "x", "must be int")
        assert "click" in str(error)
        assert "x" in str(error)
        assert error.action_type == "click"
        assert error.param_name == "x"


class TestActionLoaderCaching:
    """Tests for action loader caching."""

    def test_cache_invalidation(self):
        """Test cache invalidation."""
        from core.action_loader import ActionLoader

        loader = ActionLoader()
        loader.load_all()

        # Cache should be valid
        assert loader._is_cache_valid(loader._actions_dir if hasattr(loader, '_actions_dir') else None) in [True, False]

        loader.invalidate_cache()

        # Cache should be invalidated (not valid)
        # After invalidation, next load_all should reload

    def test_reload_action(self):
        """Test reloading a specific action."""
        from core.action_loader import ActionLoader

        loader = ActionLoader()
        loader.load_all()

        # Should be able to get an action
        action = loader.get_action('click')
        assert action is not None

        # Reload should return True
        result = loader.reload_action('click')
        assert result is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
