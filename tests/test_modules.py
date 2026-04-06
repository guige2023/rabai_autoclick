"""Test modules for RabAI AutoClick.

Provides comprehensive unit tests for core modules, actions, and utilities.
Can be run standalone (python tests/test_modules.py) or with pytest.
"""

import json
import os
import sys
import traceback
from typing import Any, Callable, Dict, List, Tuple

# Add project root to path
project_root = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
sys.path.insert(0, project_root)


# Test results storage
test_results: Dict[str, List[Any]] = {
    'passed': [],
    'failed': [],
    'warnings': []
}


def test(name: str = "") -> Callable:
    """Decorator to mark and run a test function.
    
    Args:
        name: Test name (uses function name if empty).
        
    Returns:
        Decorator function that wraps the test.
    """
    def decorator(func: Callable) -> Callable:
        test_name = name or func.__name__
        
        def wrapper() -> None:
            try:
                func()
                test_results['passed'].append(test_name)
                print(f"[PASS] {test_name}")
            except AssertionError as e:
                test_results['failed'].append({
                    'name': test_name, 
                    'error': str(e)
                })
                print(f"[FAIL] {test_name}: {e}")
            except Exception as e:
                test_results['failed'].append({
                    'name': test_name, 
                    'error': str(e)
                })
                print(f"[ERROR] {test_name}: {e}")
                traceback.print_exc()
        
        return wrapper
    
    return decorator


@test("ContextManager - 基本操作")
def test_context_basic() -> None:
    """Test basic ContextManager operations."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('name', 'test')
    assert ctx.get('name') == 'test', "set/get failed"
    
    assert ctx.get('not_exist', 'default') == 'default', "default value failed"
    
    assert ctx.delete('name') == True, "delete failed"
    assert ctx.get('name') is None, "value still exists after delete"
    
    ctx.set('a', 1)
    ctx.set('b', 2)
    all_vars = ctx.get_all()
    assert all_vars == {'a': 1, 'b': 2}, "get_all failed"


@test("ContextManager - 变量解析")
def test_context_resolve() -> None:
    """Test ContextManager variable resolution."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('name', 'world')
    result = ctx.resolve_value('Hello {{name}}!')
    assert result == 'Hello world!', f"variable resolution failed: {result}"
    
    ctx.set('a', 10)
    ctx.set('b', 20)
    result = ctx.resolve_value('{{a + b}}')
    assert result == '30', f"expression resolution failed: {result}"
    
    ctx.set('obj', {'key': 'value'})
    result = ctx.resolve_value('{{obj.key}}')
    assert result == 'value', f"attribute access failed: {result}"


@test("ContextManager - 安全执行")
def test_context_safe_exec() -> None:
    """Test ContextManager safe execution."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    result = ctx.safe_exec("return_value = 1 + 1")
    assert result == 2, f"simple exec failed: {result}"
    
    ctx.set('x', 10)
    result = ctx.safe_exec("return_value = x * 2")
    assert result == 20, f"variable access failed: {result}"
    
    try:
        ctx.safe_exec("__import__('os').system('echo test')")
        assert False, "dangerous code should not execute"
    except Exception:
        pass  # Expected - dangerous code should be blocked


@test("ContextManager - JSON序列化")
def test_context_json() -> None:
    """Test ContextManager JSON serialization."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('name', 'test')
    ctx.set('value', 123)
    
    json_str = ctx.to_json()
    data = json.loads(json_str)
    assert data['name'] == 'test', "JSON serialization failed"
    
    ctx2 = ContextManager()
    ctx2.from_json(json_str)
    assert ctx2.get('name') == 'test', "JSON deserialization failed"


@test("BaseAction - 基类功能")
def test_base_action() -> None:
    """Test BaseAction class functionality."""
    from core.base_action import BaseAction, ActionResult
    
    result = ActionResult(
        success=True, 
        message="test", 
        data={'key': 'value'}
    )
    assert result.success == True
    assert result.message == "test"
    assert result.data == {'key': 'value'}
    
    class TestAction(BaseAction):
        action_type = "test"
        display_name = "测试动作"
        
        def execute(self, context, params):
            return ActionResult(success=True, message="ok")
        
        def get_required_params(self):
            return ['param1']
        
        def get_optional_params(self):
            return {'param2': 'default'}
    
    action = TestAction()
    assert action.action_type == "test"
    assert action.get_required_params() == ['param1']
    assert action.get_optional_params() == {'param2': 'default'}


@test("BaseAction - 验证方法")
def test_base_action_validation() -> None:
    """Test BaseAction validation methods."""
    from core.base_action import BaseAction, ActionResult
    
    class TestAction(BaseAction):
        action_type = "test"
        display_name = "Test"
        
        def execute(self, context, params):
            return ActionResult(success=True)
        
        def get_required_params(self):
            return ['required1', 'required2']
    
    action = TestAction()
    
    # Test validate_type
    valid, msg = action.validate_type(123, int, 'param')
    assert valid == True
    
    valid, msg = action.validate_type("str", int, 'param')
    assert valid == False
    assert 'param' in msg and 'int' in msg
    
    # Test validate_range
    valid, msg = action.validate_range(5, 0, 10, 'param')
    assert valid == True
    
    valid, msg = action.validate_range(15, 0, 10, 'param')
    assert valid == False
    assert 'param' in msg and '0' in msg and '10' in msg
    
    # Test validate_in
    valid, msg = action.validate_in('left', ['left', 'right'], 'button')
    assert valid == True
    
    valid, msg = action.validate_in('middle', ['left', 'right'], 'button')
    assert valid == False
    
    # Test validate_params (checks required params)
    valid, msg = action.validate_params({
        'required1': 'a', 
        'required2': 'b'
    })
    assert valid == True
    
    valid, msg = action.validate_params({'required1': 'a'})
    assert valid == False
    assert 'required2' in msg


@test("ActionLoader - 加载动作")
def test_action_loader() -> None:
    """Test ActionLoader functionality."""
    from core.action_loader import ActionLoader
    import tempfile
    import shutil
    
    temp_dir = tempfile.mkdtemp()
    try:
        action_code = '''
from core.base_action import BaseAction, ActionResult

class TempAction(BaseAction):
    action_type = "temp_action"
    display_name = "临时动作"
    
    def execute(self, context, params):
        return ActionResult(success=True)
'''
        with open(
            os.path.join(temp_dir, 'temp_action.py'), 
            'w', 
            encoding='utf-8'
        ) as f:
            f.write(action_code)
        
        loader = ActionLoader(temp_dir)
        actions = loader.load_all()
        
        assert 'temp_action' in actions, "action loading failed"
        
        info = loader.get_action_info()
        assert 'temp_action' in info, "action info retrieval failed"
    finally:
        shutil.rmtree(temp_dir)


@test("FlowEngine - 工作流加载")
def test_engine_load() -> None:
    """Test FlowEngine workflow loading."""
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow = {
        'variables': {'count': 0},
        'steps': [
            {'id': 1, 'type': 'delay', 'seconds': 0.1}
        ]
    }
    
    result = engine.load_workflow_from_dict(workflow)
    assert result == True, "dict loading failed"
    assert engine.context.get('count') == 0, "variables not loaded"


@test("FlowEngine - 执行流程")
def test_engine_run() -> None:
    """Test FlowEngine workflow execution."""
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow = {
        'variables': {},
        'steps': [
            {'id': 1, 'type': 'delay', 'seconds': 0.1, 'output_var': 'delay_result'},
            {'id': 2, 'type': 'set_variable', 'name': 'test_var', 'value': 'hello'}
        ]
    }
    
    engine.load_workflow_from_dict(workflow)
    result = engine.run()
    
    assert result == True, "execution failed"
    assert engine.context.get('test_var') == 'hello', "variable setting failed"


@test("ClickAction - 鼠标点击")
def test_click_action() -> None:
    """Test ClickAction functionality."""
    from actions.click import ClickAction
    from core.context import ContextManager
    
    action = ClickAction()
    ctx = ContextManager()
    
    assert action.action_type == "click"
    assert action.get_required_params() == ['x', 'y']
    
    params = {
        'x': 100, 
        'y': 200, 
        'button': 'left', 
        'clicks': 1
    }
    assert action.validate_params(params) == (True, "")


@test("TypeAction - 键盘输入")
def test_type_action() -> None:
    """Test TypeAction functionality."""
    from actions.keyboard import TypeAction
    from core.context import ContextManager
    
    action = TypeAction()
    ctx = ContextManager()
    
    assert action.action_type == "type_text"
    assert action.get_required_params() == ['text']
    
    result = action.execute(ctx, {'text': ''})
    assert result.success == False, "empty text should fail"


@test("DelayAction - 延时")
def test_delay_action() -> None:
    """Test DelayAction functionality."""
    from actions.script import DelayAction
    from core.context import ContextManager
    import time
    
    action = DelayAction()
    ctx = ContextManager()
    
    start = time.time()
    result = action.execute(ctx, {'seconds': 0.1})
    elapsed = time.time() - start
    
    assert result.success == True
    assert elapsed >= 0.1, "delay time insufficient"


@test("ConditionAction - 条件判断")
def test_condition_action() -> None:
    """Test ConditionAction functionality."""
    from actions.script import ConditionAction
    from core.context import ContextManager
    
    action = ConditionAction()
    ctx = ContextManager()
    
    ctx.set('value', 10)
    result = action.execute(
        ctx, 
        {'condition': 'value > 5', 'true_next': 2, 'false_next': 3}
    )
    
    assert result.success == True
    assert result.data['result'] == True
    assert result.next_step_id == 2


@test("LoopAction - 循环控制")
def test_loop_action() -> None:
    """Test LoopAction functionality."""
    from actions.script import LoopAction
    from core.context import ContextManager
    
    action = LoopAction()
    ctx = ContextManager()
    
    for i in range(3):
        result = action.execute(
            ctx, 
            {'loop_id': 'test', 'count': 3, 'loop_start': 1}
        )
        assert result.success == True
    
    assert ctx.get('_loop_test') == 3


@test("SetVariableAction - 设置变量")
def test_set_variable_action() -> None:
    """Test SetVariableAction functionality."""
    from actions.script import SetVariableAction
    from core.context import ContextManager
    
    action = SetVariableAction()
    ctx = ContextManager()
    
    result = action.execute(
        ctx, 
        {'name': 'test', 'value': '123', 'value_type': 'int'}
    )
    assert result.success == True
    assert ctx.get('test') == 123
    
    result = action.execute(
        ctx, 
        {'name': 'flag', 'value': 'true', 'value_type': 'bool'}
    )
    assert result.success == True
    assert ctx.get('flag') == True


@test("ScreenshotAction - 截图")
def test_screenshot_action() -> None:
    """Test ScreenshotAction functionality."""
    from actions.system import ScreenshotAction
    from core.context import ContextManager
    
    action = ScreenshotAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success == True, "screenshot failed"
    assert 'image' in result.data, "no image data returned"


@test("GetMousePosAction - 获取鼠标位置")
def test_get_mouse_pos_action() -> None:
    """Test GetMousePosAction functionality."""
    from actions.system import GetMousePosAction
    from core.context import ContextManager
    
    action = GetMousePosAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success == True, "get mouse position failed"
    assert 'x' in result.data and 'y' in result.data


@test("HotkeyManager - 热键管理")
def test_hotkey_manager() -> None:
    """Test HotkeyManager functionality."""
    from utils.hotkey import HotkeyManager
    
    manager = HotkeyManager()
    
    assert manager.DEFAULT_HOTKEYS == {
        'start': 'f6', 
        'stop': 'f7', 
        'pause': 'f8'
    }
    
    parsed = HotkeyManager.parse_hotkey('Ctrl + Shift + A')
    assert 'ctrl' in parsed and 'shift' in parsed and 'a' in parsed
    
    formatted = HotkeyManager.format_hotkey('ctrl+shift+a')
    assert 'CTRL' in formatted and 'SHIFT' in formatted


@test("ImageMatchAction - 参数验证")
def test_image_match_action() -> None:
    """Test ImageMatchAction parameter validation."""
    from actions.image_match import ImageMatchAction
    from core.context import ContextManager
    
    action = ImageMatchAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'template': ''})
    assert result.success == False, "empty template should fail"
    
    result = action.execute(ctx, {'template': '/nonexistent/path.png'})
    assert result.success == False, "nonexistent file should fail"


@test("OCRAction - 可用性检查")
def test_ocr_action() -> None:
    """Test OCRAction availability."""
    from actions.ocr import OCRAction, _get_ocr
    from core.context import ContextManager
    
    action = OCRAction()
    ctx = ContextManager()
    
    ocr_engine, backend = _get_ocr()
    if ocr_engine is None:
        result = action.execute(ctx, {})
        assert result.success == False
        assert "未安装" in result.message


def run_all_tests() -> Dict[str, List[Any]]:
    """Run all tests and generate report.
    
    Returns:
        Dictionary with test results (passed, failed, warnings).
    """
    print("=" * 60)
    print("RabAI AutoClick Module Test Report")
    print("=" * 60)
    print()
    
    test_functions: List[Callable] = [
        test_context_basic,
        test_context_resolve,
        test_context_safe_exec,
        test_context_json,
        test_base_action,
        test_action_loader,
        test_engine_load,
        test_engine_run,
        test_click_action,
        test_type_action,
        test_delay_action,
        test_condition_action,
        test_loop_action,
        test_set_variable_action,
        test_screenshot_action,
        test_get_mouse_pos_action,
        test_hotkey_manager,
        test_image_match_action,
        test_ocr_action,
    ]
    
    for func in test_functions:
        func()
    
    print()
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Passed: {len(test_results['passed'])}")
    print(f"Failed: {len(test_results['failed'])}")
    
    if test_results['failed']:
        print("\nFailed Tests:")
        for item in test_results['failed']:
            print(f"  - {item['name']}: {item['error']}")
    
    return test_results


if __name__ == '__main__':
    run_all_tests()
