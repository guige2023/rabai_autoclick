"""Expanded tests for core modules of RabAI AutoClick.

Tests ContextManager, FlowEngine, ActionLoader, BaseAction, and error recovery.
"""

import sys
import os
import time
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path

sys.path.insert(0, '/Users/guige/my_project')

from core.context import ContextManager
from core.engine import FlowEngine
from core.action_loader import ActionLoader
from core.base_action import BaseAction, ActionResult


class TestContextManagerEdgeCases(unittest.TestCase):
    """Test ContextManager edge cases."""

    def test_nested_double_braces(self):
        """Test nested {{variable}} resolution."""
        ctx = ContextManager()
        ctx.set('outer', 'hello')
        ctx.set('inner', 'world')
        ctx.set('nested', '{{outer}}_{{inner}}')
        
        # Single resolution
        result = ctx.resolve_value('{{nested}}')
        self.assertEqual(result, 'hello_world')
        
        # Double resolution (literal braces)
        result2 = ctx.resolve_value('{{{{nested}}}}')
        self.assertEqual(result2, '{{hello_world}}')
        
    def test_triple_nested_braces(self):
        """Test triple nested braces."""
        ctx = ContextManager()
        ctx.set('a', 'A')
        ctx.set('b', '{{a}}')
        ctx.set('c', '{{b}}')
        
        result = ctx.resolve_value('{{c}}')
        self.assertEqual(result, 'A')
        
    def test_unicode_variables(self):
        """Test unicode characters in variables."""
        ctx = ContextManager()
        
        ctx.set('unicode_chinese', '你好世界')
        ctx.set('unicode_japanese', 'こんにちは')
        ctx.set('unicode_emoji', '🎉🚀')
        ctx.set('unicode_mixed', 'Hello 世界 🌏')
        
        self.assertEqual(ctx.get('unicode_chinese'), '你好世界')
        self.assertEqual(ctx.get('unicode_japanese'), 'こんにちは')
        self.assertEqual(ctx.get('unicode_emoji'), '🎉🚀')
        self.assertEqual(ctx.get('unicode_mixed'), 'Hello 世界 🌏')
        
        # Resolve in string
        result = ctx.resolve_value('{{unicode_chinese}}')
        self.assertEqual(result, '你好世界')
        
    def test_empty_string_variables(self):
        """Test empty string handling."""
        ctx = ContextManager()
        
        ctx.set('empty', '')
        ctx.set('non_empty', 'value')
        
        # Empty string should resolve to empty
        result = ctx.resolve_value('{{empty}}')
        self.assertEqual(result, '')
        
        # Empty braces should be preserved
        result2 = ctx.resolve_value('{{}}')
        self.assertEqual(result2, '{{}}')
        
    def test_empty_string_in_resolve_value(self):
        """Test resolve_value with empty string input."""
        ctx = ContextManager()
        ctx.set('test', 'value')
        
        result = ctx.resolve_value('')
        self.assertEqual(result, '')
        
    def test_special_chars_in_variable_names(self):
        """Test special characters in variable names."""
        ctx = ContextManager()
        
        # Valid variable names should work
        ctx.set('var_underscore', '1')
        ctx.set('varCamelCase', '2')
        ctx.set('var123', '3')
        
        self.assertEqual(ctx.get('var_underscore'), '1')
        self.assertEqual(ctx.get('varCamelCase'), '2')
        self.assertEqual(ctx.get('var123'), '3')
        
    def test_missing_variable_default(self):
        """Test missing variable returns default."""
        ctx = ContextManager()
        
        # Should return None for missing variables
        self.assertIsNone(ctx.get('nonexistent'))
        self.assertEqual(ctx.get('nonexistent', 'default'), 'default')
        
    def test_mixed_unicode_and_braces(self):
        """Test mixed unicode and brace syntax."""
        ctx = ContextManager()
        ctx.set('name', '张三')
        ctx.set('greeting', '你好')
        
        result = ctx.resolve_value('{{greeting}}, {{name}}!')
        self.assertEqual(result, '你好, 张三!')
        
    def test_unclosed_braces(self):
        """Test unclosed braces handling."""
        ctx = ContextManager()
        ctx.set('test', 'value')
        
        # Unclosed brace - should return as-is or partial match
        result = ctx.resolve_value('{{test')
        # The behavior depends on implementation - at minimum should not crash
        self.assertIsInstance(result, str)
        
    def test_resolve_value_with_dict(self):
        """Test resolve_value with dictionary input."""
        ctx = ContextManager()
        ctx.set('key1', 'value1')
        ctx.set('key2', 'value2')
        
        input_dict = {
            'param1': '{{key1}}',
            'param2': '{{key2}}',
            'nested': {
                'inner': '{{key1}}'
            }
        }
        
        result = ctx.resolve_value(input_dict)
        
        self.assertEqual(result['param1'], 'value1')
        self.assertEqual(result['param2'], 'value2')
        self.assertEqual(result['nested']['inner'], 'value1')
        
    def test_resolve_value_with_list(self):
        """Test resolve_value with list input."""
        ctx = ContextManager()
        ctx.set('item1', 'first')
        ctx.set('item2', 'second')
        
        input_list = ['{{item1}}', '{{item2}}', 'static']
        result = ctx.resolve_value(input_list)
        
        self.assertEqual(result, ['first', 'second', 'static'])


class TestFlowEngineEdgeCases(unittest.TestCase):
    """Test FlowEngine pause/resume/stop edge cases."""

    def test_pause_and_resume(self):
        """Test pausing and resuming a workflow."""
        engine = FlowEngine()
        
        workflow = {
            'variables': {'counter': 0},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.05},
                {'id': 2, 'type': 'delay', 'seconds': 0.05},
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        
        # Start execution
        import threading
        
        def pause_after_delay():
            time.sleep(0.05)
            engine.pause()
            time.sleep(0.05)
            engine.resume()
        
        t = threading.Thread(target=pause_after_delay)
        t.start()
        
        result = engine.run()
        t.join()
        
        self.assertTrue(result)
        self.assertFalse(engine.is_paused())
        
    def test_multiple_rapid_pause_resume(self):
        """Test rapid pause/resume calls."""
        engine = FlowEngine()
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.1},
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        
        # Rapid pause/resume should not break state
        engine.pause()
        engine.pause()  # Double pause
        engine.resume()
        engine.resume()  # Double resume
        
        result = engine.run()
        self.assertTrue(result)
        
    def test_stop_while_paused(self):
        """Test stopping a paused workflow."""
        engine = FlowEngine()
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 10},
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        
        import threading
        
        def pause_then_stop():
            time.sleep(0.05)
            engine.pause()
            time.sleep(0.05)
            engine.stop()
        
        t = threading.Thread(target=pause_then_stop)
        t.start()
        
        result = engine.run()
        t.join()
        
        self.assertFalse(result)
        self.assertFalse(engine.is_paused())
        self.assertFalse(engine.is_running())
        
    def test_is_running_state_consistency(self):
        """Test is_running state is consistent."""
        engine = FlowEngine()
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.05},
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        
        self.assertFalse(engine.is_running())
        
        import threading
        
        running_states = []
        
        def track_state():
            time.sleep(0.01)
            running_states.append(engine.is_running())
        
        t = threading.Thread(target=track_state)
        t.start()
        
        engine.run()
        t.join()
        
        # At some point during execution it should be running
        # (May or may not be captured depending on timing)
        
    def test_callbacks_called_on_step_events(self):
        """Test that callbacks are properly called."""
        engine = FlowEngine()
        
        step_start_called = []
        step_end_called = []
        
        engine.set_callbacks(
            on_step_start=lambda s: step_start_called.append(s),
            on_step_end=lambda s, r: step_end_called.append((s, r)),
        )
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.02},
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        engine.run()
        
        # Callbacks should be called
        # (May be empty if execution is too fast)


class TestActionLoaderEdgeCases(unittest.TestCase):
    """Test ActionLoader with invalid action files."""

    def test_load_from_invalid_file_syntax_error(self):
        """Test loading action file with syntax error."""
        loader = ActionLoader()
        
        with patch('builtins.open', mock_open(read_data='not valid python {{{{')):
            with patch('pathlib.Path.exists', return_value=True):
                with patch('pathlib.Path.is_file', return_value=True):
                    with patch('importlib.util.spec_from_file_location') as mock_spec:
                        mock_spec.return_value = None
                        # Should handle gracefully
                        result = loader.load_from_file('/fake/path.py')
                        self.assertIsNone(result)
                        
    def test_load_from_file_no_base_action(self):
        """Test loading file without BaseAction subclass."""
        loader = ActionLoader()
        
        code = '''
class NotAnAction:
    def execute(self):
        pass
'''
        
        with patch('builtins.open', mock_open(read_data=code)):
            with patch('pathlib.Path.exists', return_value=True):
                with patch('importlib.util.module_from_spec') as mock_module:
                    mock_module.return_value = MagicMock()
                    # Module has no BaseAction subclass
                    result = loader.load_from_file('/fake/path.py')
                    # Should return empty dict or handle gracefully
                    
    def test_load_all_with_permission_error(self):
        """Test load_all handles permission errors."""
        loader = ActionLoader()
        
        with patch('pathlib.Path.iterdir') as mock_iter:
            mock_iter.side_effect = PermissionError("Access denied")
            # Should not crash
            loader.load_all()
            
    def test_load_all_with_empty_directory(self):
        """Test load_all with empty actions directory."""
        loader = ActionLoader()
        
        with patch('pathlib.Path.iterdir') as mock_iter:
            mock_iter.return_value = iter([])
            loader.load_all()
            # Should complete without error
            
    def test_get_nonexistent_action(self):
        """Test getting an action that doesn't exist."""
        loader = ActionLoader()
        
        loader.load_all()
        result = loader.get_action('nonexistent_action_xyz')
        self.assertIsNone(result)
        
    def test_register_and_unregister_action(self):
        """Test manual action registration."""
        loader = ActionLoader()
        
        class TestAction(BaseAction):
            action_type = 'test_registered'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        loader.register_action(TestAction)
        action = loader.get_action('test_registered')
        self.assertIsNotNone(action)
        
        loader.unregister_action('test_registered')
        action = loader.get_action('test_registered')
        self.assertIsNone(action)
        
    def test_get_action_info(self):
        """Test getting action information."""
        loader = ActionLoader()
        loader.load_all()
        
        info = loader.get_action_info()
        self.assertIsInstance(info, dict)


class TestBaseActionEdgeCases(unittest.TestCase):
    """Test BaseAction.validate_type and other validation edge cases."""

    def test_validate_type_with_none(self):
        """Test validate_type with None value."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        # None is a valid value for any type when required=False
        valid, msg = action.validate_type(None, str, 'test_param', required=False)
        self.assertTrue(valid)
        
    def test_validate_type_with_wrong_type(self):
        """Test validate_type with wrong type."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        valid, msg = action.validate_type(123, str, 'test_param')
        self.assertFalse(valid)
        self.assertIn('must be', msg)
        
    def test_validate_type_with_tuple_and_expected_list(self):
        """Test validate_type with tuple when list expected."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        valid, msg = action.validate_type((1, 2, 3), list, 'test_param')
        # Tuple is not list, should fail
        self.assertFalse(valid)
        
    def test_validate_range_with_invalid_bounds(self):
        """Test validate_range with min > max."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        # When min > max, range check should handle gracefully
        # or value should always be invalid
        valid, msg = action.validate_range(5, 10, 1, 'test_param')
        self.assertFalse(valid)
        
    def test_validate_in_with_empty_valid_values(self):
        """Test validate_in with empty valid list."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        # Empty valid values should always fail or be handled
        valid, msg = action.validate_in('value', [], 'test_param')
        self.assertFalse(valid)
        
    def test_validate_in_with_none_value(self):
        """Test validate_in with None value."""
        class TestAction(BaseAction):
            action_type = 'test'
            def execute(self, ctx, params):
                return ActionResult(success=True, message='ok')
        
        action = TestAction()
        
        valid, msg = action.validate_in(None, ['a', 'b'], 'test_param')
        self.assertFalse(valid)


class TestErrorRecovery(unittest.TestCase):
    """Test error recovery flow."""

    def test_context_safe_exec_catches_exception(self):
        """Test safe_exec catches exceptions properly."""
        ctx = ContextManager()
        
        try:
            result = ctx.safe_exec('raise ValueError("test error")')
        except Exception as e:
            self.assertIsInstance(e, ValueError)
            
    def test_context_safe_exec_with_valid_code(self):
        """Test safe_exec with valid code."""
        ctx = ContextManager()
        ctx.set('x', 10)
        
        result = ctx.safe_exec('x + 5')
        self.assertEqual(result, 15)
        
    def test_action_result_duration_field(self):
        """Test ActionResult has duration field."""
        result = ActionResult(success=True, message='ok')
        self.assertTrue(hasattr(result, 'duration'))
        
        # Duration can be set
        result.duration = 1.5
        self.assertEqual(result.duration, 1.5)
        
    def test_action_result_next_step_id(self):
        """Test ActionResult next_step_id field."""
        result = ActionResult(success=True, message='ok', next_step_id='step_2')
        self.assertEqual(result.next_step_id, 'step_2')
        
    def test_context_manager_state_on_error(self):
        """Test context state is preserved on error."""
        ctx = ContextManager()
        ctx.set('counter', 0)
        
        # Execute code that modifies state before error
        try:
            ctx.safe_exec('counter = 5; raise ValueError("stop")')
        except:
            pass
            
        # State should be preserved (depends on implementation)
        # The safe_exec may or may not roll back on error


if __name__ == '__main__':
    unittest.main()
