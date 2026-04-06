"""Test suite for RabAI AutoClick core modules.

Run with: pytest tests/test_core.py -v
Or: python -m pytest tests/test_core.py -v
"""

import sys
import os
import time
import tempfile
import shutil
import json

import pytest

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class TestContextManager:
    """Tests for ContextManager class."""
    
    def test_basic_operations(self):
        """Test basic get/set/delete operations."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        
        # set and get
        ctx.set('name', 'test')
        assert ctx.get('name') == 'test'
        
        # default value
        assert ctx.get('not_exist', 'default') == 'default'
        assert ctx.get('not_exist') is None
        
        # delete
        assert ctx.delete('name') is True
        assert ctx.get('name') is None
        
        # delete non-existent
        assert ctx.delete('not_exist') is False
    
    def test_set_all(self):
        """Test batch variable setting."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set_all({'a': 1, 'b': 2, 'c': 3})
        
        assert ctx.get('a') == 1
        assert ctx.get('b') == 2
        assert ctx.get('c') == 3
    
    def test_variable_resolution(self):
        """Test {{variable}} resolution in strings."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set('name', 'world')
        
        result = ctx.resolve_value('Hello {{name}}!')
        assert result == 'Hello world!'
    
    def test_expression_evaluation(self):
        """Test arithmetic expression evaluation."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set('a', 10)
        ctx.set('b', 20)
        
        result = ctx.resolve_value('{{a + b}}')
        assert result == 30
    
    def test_nested_dict_access(self):
        """Test dot notation for nested dict access."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set('obj', {'key': 'value', 'nested': {'inner': 'deep'}})
        
        assert ctx.resolve_value('{{obj.key}}') == 'value'
        assert ctx.resolve_value('{{obj.nested.inner}}') == 'deep'
    
    def test_safe_exec_simple(self):
        """Test safe code execution."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        result = ctx.safe_exec("return_value = 1 + 1")
        assert result == 2
    
    def test_safe_exec_with_variables(self):
        """Test safe exec with context variables."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set('x', 10)
        result = ctx.safe_exec("return_value = x * 2")
        assert result == 20
    
    def test_safe_exec_blocks_dangerous(self):
        """Test that dangerous code is blocked in safe exec."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        
        # __import__ should be blocked
        with pytest.raises(Exception):
            ctx.safe_exec("__import__('os').system('echo test')")
    
    def test_json_serialization(self):
        """Test JSON export/import round-trip."""
        from core.context import ContextManager
        
        ctx = ContextManager()
        ctx.set('name', 'test')
        ctx.set('value', 123)
        
        json_str = ctx.to_json()
        data = json.loads(json_str)
        assert data['name'] == 'test'
        assert data['value'] == 123
        
        ctx2 = ContextManager()
        ctx2.from_json(json_str)
        assert ctx2.get('name') == 'test'
        assert ctx2.get('value') == 123


class TestBaseAction:
    """Tests for BaseAction class and validation methods."""
    
    def test_action_result_dataclass(self):
        """Test ActionResult creation."""
        from core.base_action import ActionResult
        
        result = ActionResult(success=True, message="ok", data={'key': 'value'})
        assert result.success is True
        assert result.message == "ok"
        assert result.data == {'key': 'value'}
        assert result.next_step_id is None
        assert result.duration == 0.0
    
    def test_validate_type_int(self):
        """Test validate_type with int."""
        from core.base_action import BaseAction, ActionResult
        
        class TestAction(BaseAction):
            action_type = "test"
            
            def execute(self, context, params):
                return ActionResult(success=True)
        
        action = TestAction()
        
        # Valid int
        valid, msg = action.validate_type(123, int, 'param')
        assert valid is True
        
        # Invalid string for int
        valid, msg = action.validate_type("str", int, 'param')
        assert valid is False
        assert 'param' in msg and 'int' in msg
    
    def test_validate_type_tuple(self):
        """Test validate_type with tuple type."""
        from core.base_action import BaseAction, ActionResult
        
        class TestAction(BaseAction):
            action_type = "test"
            
            def execute(self, context, params):
                return ActionResult(success=True)
        
        action = TestAction()
        
        valid, msg = action.validate_type((1, 2, 3, 4), tuple, 'region')
        assert valid is True
        
        valid, msg = action.validate_type([1, 2, 3, 4], tuple, 'region')
        assert valid is False
    
    def test_validate_range(self):
        """Test validate_range numeric bounds."""
        from core.base_action import BaseAction, ActionResult
        
        class TestAction(BaseAction):
            action_type = "test"
            
            def execute(self, context, params):
                return ActionResult(success=True)
        
        action = TestAction()
        
        # Within range
        valid, msg = action.validate_range(5, 0, 10, 'param')
        assert valid is True
        
        # Below range
        valid, msg = action.validate_range(-1, 0, 10, 'param')
        assert valid is False
        
        # Above range
        valid, msg = action.validate_range(15, 0, 10, 'param')
        assert valid is False
    
    def test_validate_in(self):
        """Test validate_in with valid values list."""
        from core.base_action import BaseAction, ActionResult
        
        class TestAction(BaseAction):
            action_type = "test"
            
            def execute(self, context, params):
                return ActionResult(success=True)
        
        action = TestAction()
        
        valid, msg = action.validate_in('left', ['left', 'right', 'middle'], 'button')
        assert valid is True
        
        valid, msg = action.validate_in('invalid', ['left', 'right'], 'button')
        assert valid is False
    
    def test_validate_params_required(self):
        """Test default validate_params checks required params."""
        from core.base_action import BaseAction, ActionResult
        
        class TestAction(BaseAction):
            action_type = "test"
            
            def execute(self, context, params):
                return ActionResult(success=True)
            
            def get_required_params(self):
                return ['required1', 'required2']
        
        action = TestAction()
        
        # All required present
        valid, msg = action.validate_params({'required1': 'a', 'required2': 'b'})
        assert valid is True
        
        # Missing required2
        valid, msg = action.validate_params({'required1': 'a'})
        assert valid is False
        assert 'required2' in msg


class TestActionLoader:
    """Tests for ActionLoader class."""
    
    def test_load_all_actions(self):
        """Test loading all built-in actions."""
        from core.action_loader import ActionLoader
        
        loader = ActionLoader()
        actions = loader.load_all()
        
        # Check some expected actions are loaded
        assert 'click' in actions
        assert 'delay' in actions
        assert 'type_text' in actions
        assert 'key_press' in actions
        assert 'screenshot' in actions
        assert 'ocr' in actions
    
    def test_get_action_info(self):
        """Test getting action information."""
        from core.action_loader import ActionLoader
        
        loader = ActionLoader()
        loader.load_all()
        info = loader.get_action_info()
        
        assert 'click' in info
        assert 'display_name' in info['click']
        assert 'description' in info['click']
        assert 'required_params' in info['click']
        assert 'optional_params' in info['click']


class TestClickAction:
    """Tests for ClickAction."""
    
    def test_required_params(self):
        """Test that x and y are required."""
        from actions.click import ClickAction
        
        action = ClickAction()
        assert action.get_required_params() == ['x', 'y']
    
    def test_optional_params(self):
        """Test default optional params."""
        from actions.click import ClickAction
        
        action = ClickAction()
        opts = action.get_optional_params()
        
        assert opts['button'] == 'left'
        assert opts['clicks'] == 1
        assert opts['interval'] == 0.1
        assert opts['move_duration'] == 0.2
        assert opts['relative'] is False
    
    def test_validate_invalid_button(self):
        """Test validation rejects invalid button."""
        from actions.click import ClickAction
        from core.context import ContextManager
        
        action = ClickAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'x': 100, 'y': 200, 'button': 'invalid'
        })
        assert result.success is False
        assert 'button' in result.message.lower()
    
    def test_validate_negative_clicks(self):
        """Test validation rejects negative clicks."""
        from actions.click import ClickAction
        from core.context import ContextManager
        
        action = ClickAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'x': 100, 'y': 200, 'clicks': -1
        })
        assert result.success is False
        assert 'clicks' in result.message.lower()


class TestDelayAction:
    """Tests for DelayAction."""
    
    def test_required_params(self):
        """Test that seconds is required."""
        from actions.script import DelayAction
        
        action = DelayAction()
        assert action.get_required_params() == ['seconds']
    
    def test_delay_execution(self):
        """Test delay actually waits."""
        from actions.script import DelayAction
        from core.context import ContextManager
        
        action = DelayAction()
        ctx = ContextManager()
        
        start = time.time()
        result = action.execute(ctx, {'seconds': 0.1})
        elapsed = time.time() - start
        
        assert result.success is True
        assert elapsed >= 0.1
    
    def test_validate_negative_seconds(self):
        """Test validation rejects negative seconds."""
        from actions.script import DelayAction
        from core.context import ContextManager
        
        action = DelayAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {'seconds': -1})
        assert result.success is False


class TestConditionAction:
    """Tests for ConditionAction."""
    
    def test_true_condition(self):
        """Test condition evaluates to true."""
        from actions.script import ConditionAction
        from core.context import ContextManager
        
        action = ConditionAction()
        ctx = ContextManager()
        ctx.set('value', 10)
        
        result = action.execute(ctx, {
            'condition': 'value > 5',
            'true_next': 2,
            'false_next': 3
        })
        
        assert result.success is True
        assert result.data['result'] is True
        assert result.next_step_id == 2
    
    def test_false_condition(self):
        """Test condition evaluates to false."""
        from actions.script import ConditionAction
        from core.context import ContextManager
        
        action = ConditionAction()
        ctx = ContextManager()
        ctx.set('value', 3)
        
        result = action.execute(ctx, {
            'condition': 'value > 5',
            'true_next': 2,
            'false_next': 3
        })
        
        assert result.success is True
        assert result.data['result'] is False
        assert result.next_step_id == 3
    
    def test_empty_condition_fails(self):
        """Test empty condition returns error."""
        from actions.script import ConditionAction
        from core.context import ContextManager
        
        action = ConditionAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {'condition': ''})
        assert result.success is False


class TestLoopAction:
    """Tests for LoopAction."""
    
    def test_loop_iterations(self):
        """Test loop counts iterations correctly."""
        from actions.script import LoopAction
        from core.context import ContextManager
        
        action = LoopAction()
        ctx = ContextManager()
        
        # Run 3 iterations of a 3-count loop
        for i in range(3):
            result = action.execute(ctx, {
                'loop_id': 'test_loop',
                'count': 3,
                'loop_start': 1,
                'loop_end': 4
            })
            assert result.success is True
        
        # After 3 iterations, loop should end
        assert ctx.get('_loop_test_loop') == 3
    
    def test_loop_zero_count_invalid(self):
        """Test loop with count < 1 is rejected."""
        from actions.script import LoopAction
        from core.context import ContextManager
        
        action = LoopAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'loop_id': 'test',
            'count': 0
        })
        assert result.success is False


class TestSetVariableAction:
    """Tests for SetVariableAction."""
    
    def test_set_int_variable(self):
        """Test setting an integer variable."""
        from actions.script import SetVariableAction
        from core.context import ContextManager
        
        action = SetVariableAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'name': 'my_var',
            'value': '123',
            'value_type': 'int'
        })
        
        assert result.success is True
        assert ctx.get('my_var') == 123
    
    def test_set_bool_variable(self):
        """Test setting a boolean variable."""
        from actions.script import SetVariableAction
        from core.context import ContextManager
        
        action = SetVariableAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'name': 'flag',
            'value': 'true',
            'value_type': 'bool'
        })
        
        assert result.success is True
        assert ctx.get('flag') is True
    
    def test_invalid_value_type(self):
        """Test invalid value_type is rejected."""
        from actions.script import SetVariableAction
        from core.context import ContextManager
        
        action = SetVariableAction()
        ctx = ContextManager()
        
        result = action.execute(ctx, {
            'name': 'var',
            'value': 'test',
            'value_type': 'invalid'
        })
        
        assert result.success is False


class TestFlowEngine:
    """Tests for FlowEngine class."""
    
    def test_load_workflow_from_dict(self):
        """Test loading workflow from dictionary."""
        from core.engine import FlowEngine
        
        engine = FlowEngine()
        
        workflow = {
            'variables': {'count': 0},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.1}
            ]
        }
        
        result = engine.load_workflow_from_dict(workflow)
        assert result is True
        assert engine.context.get('count') == 0
    
    def test_run_simple_workflow(self):
        """Test running a simple delay workflow."""
        from core.engine import FlowEngine
        
        engine = FlowEngine()
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 0.05},
                {'id': 2, 'type': 'set_variable', 'name': 'done', 'value': 'yes', 'value_type': 'string'}
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        result = engine.run()
        
        assert result is True
        assert engine.context.get('done') == 'yes'
    
    def test_stop_requested(self):
        """Test that stop() prevents further execution."""
        from core.engine import FlowEngine
        
        engine = FlowEngine()
        
        workflow = {
            'variables': {},
            'steps': [
                {'id': 1, 'type': 'delay', 'seconds': 10}
            ]
        }
        
        engine.load_workflow_from_dict(workflow)
        
        # Start async execution
        import threading
        
        def stop_after_delay():
            time.sleep(0.1)
            engine.stop()
        
        t = threading.Thread(target=stop_after_delay)
        t.start()
        
        result = engine.run()
        t.join()
        
        assert result is False  # Stopped, not completed
        assert engine.is_running() is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
