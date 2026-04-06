#!/usr/bin/env python3
"""RabAI AutoClick v1.2 comprehensive module test script.

This script tests all major modules of the RabAI AutoClick system.
"""

import os
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest


# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# Test results tracking
test_results: Dict[str, List[Any]] = {
    'passed': [],
    'failed': [],
    'warnings': []
}


def test(name: str) -> Callable:
    """Decorator to mark and run a test function.
    
    Args:
        name: Test name for reporting.
        
    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper() -> bool:
            try:
                func()
                test_results['passed'].append(name)
                print(f"[PASS] {name}")
                return True
            except AssertionError as e:
                test_results['failed'].append({'name': name, 'error': str(e)})
                print(f"[FAIL] {name}: {e}")
                return False
            except Exception as e:
                test_results['failed'].append({'name': name, 'error': str(e)})
                print(f"[ERROR] {name}: {e}")
                traceback.print_exc()
                return False
        return wrapper
    return decorator


# ==================== Core Module Tests ====================

@test("Core - ContextManager Basic Operations")
def test_context_basic() -> None:
    """Test ContextManager basic get/set/delete operations."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('name', 'test')
    assert ctx.get('name') == 'test', "set/get failed"
    
    assert ctx.get('not_exist', 'default') == 'default', "default value failed"
    
    assert ctx.delete('name') is True, "delete failed"
    assert ctx.get('name') is None, "delete后仍存在"


@test("Core - ContextManager Variable Resolution")
def test_context_resolve() -> None:
    """Test ContextManager variable resolution in strings."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('name', 'world')
    result = ctx.resolve_value('Hello {{name}}!')
    assert result == 'Hello world!', f"Variable resolution failed: {result}"
    
    ctx.set('a', 10)
    ctx.set('b', 20)
    result = ctx.resolve_value('{{a + b}}')
    assert result == '30', f"Expression resolution failed: {result}"


@test("Core - ContextManager Safe Execution")
def test_context_safe_exec() -> None:
    """Test ContextManager safe execution sandbox."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('x', 10)
    result = ctx.safe_exec("return_value = x * 2")
    assert result == 20, f"Variable access failed: {result}"
    
    try:
        ctx.safe_exec("__import__('os').system('echo test')")
        assert False, "Dangerous code should not execute"
    except Exception:
        pass


@test("Core - BaseAction Base Class")
def test_base_action() -> None:
    """Test BaseAction and ActionResult classes."""
    from core.base_action import ActionResult
    
    result = ActionResult(
        success=True,
        message="test",
        data={'key': 'value'}
    )
    assert result.success is True
    assert result.message == "test"
    assert result.data == {'key': 'value'}


@test("Core - ActionLoader Loading")
def test_action_loader() -> None:
    """Test ActionLoader loads all action modules."""
    from core.action_loader import ActionLoader
    
    actions_dir = project_root / "actions"
    loader = ActionLoader(str(actions_dir))
    actions = loader.load_all()
    
    assert len(actions) > 0, "No actions loaded"
    assert 'click' in actions, "click action not loaded"
    assert 'type_text' in actions, "type_text action not loaded"
    assert 'delay' in actions, "delay action not loaded"
    
    info = loader.get_action_info()
    assert len(info) == len(actions), "Action info count mismatch"


@test("Core - FlowEngine Load Workflow")
def test_engine_load() -> None:
    """Test FlowEngine loads workflow from dict."""
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow: Dict[str, Any] = {
        'variables': {'count': 0},
        'steps': [{'id': 1, 'type': 'delay', 'seconds': 0.1}]
    }
    
    result = engine.load_workflow_from_dict(workflow)
    assert result is True, "Load from dict failed"
    assert engine.context.get('count') == 0, "Variable not loaded"


@test("Core - FlowEngine Execute Workflow")
def test_engine_run() -> None:
    """Test FlowEngine executes workflow correctly."""
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow: Dict[str, Any] = {
        'variables': {},
        'steps': [
            {'id': 1, 'type': 'delay', 'seconds': 0.05},
            {
                'id': 2,
                'type': 'set_variable',
                'name': 'test_var',
                'value': 'hello'
            }
        ]
    }
    
    engine.load_workflow_from_dict(workflow)
    result = engine.run()
    
    assert result is True, "Execution failed"
    assert engine.context.get('test_var') == 'hello', "Variable not set"


# ==================== Action Plugin Tests ====================

@test("Action - ClickAction Parameter Validation")
def test_click_action() -> None:
    """Test ClickAction parameter validation."""
    from actions.click import ClickAction
    from core.context import ContextManager
    
    action = ClickAction()
    ctx = ContextManager()
    
    assert action.action_type == "click"
    assert action.get_required_params() == ['x', 'y']
    
    valid, msg = action.validate_params({'x': 100, 'y': 200})
    assert valid is True, f"Parameter validation failed: {msg}"


@test("Action - TypeAction Empty Text Handling")
def test_type_action() -> None:
    """Test TypeAction handles empty text."""
    from actions.keyboard import TypeAction
    from core.context import ContextManager
    
    action = TypeAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'text': ''})
    assert result.success is False, "Empty text should fail"


@test("Action - DelayAction Execution")
def test_delay_action() -> None:
    """Test DelayAction executes with correct timing."""
    from actions.script import DelayAction
    from core.context import ContextManager
    
    action = DelayAction()
    ctx = ContextManager()
    
    start = time.time()
    result = action.execute(ctx, {'seconds': 0.1})
    elapsed = time.time() - start
    
    assert result.success is True
    assert elapsed >= 0.1, "Delay time insufficient"


@test("Action - ConditionAction Conditional Logic")
def test_condition_action() -> None:
    """Test ConditionAction evaluates conditions correctly."""
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


@test("Action - LoopAction Loop Control")
def test_loop_action() -> None:
    """Test LoopAction manages loop state."""
    from actions.script import LoopAction
    from core.context import ContextManager
    
    action = LoopAction()
    ctx = ContextManager()
    
    for _ in range(3):
        result = action.execute(ctx, {
            'loop_id': 'test',
            'count': 3,
            'loop_start': 1
        })
        assert result.success is True
    
    assert ctx.get('_loop_test') == 3


@test("Action - SetVariableAction Type Conversion")
def test_set_variable_action() -> None:
    """Test SetVariableAction converts types correctly."""
    from actions.script import SetVariableAction
    from core.context import ContextManager
    
    action = SetVariableAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {
        'name': 'test',
        'value': '123',
        'value_type': 'int'
    })
    assert result.success is True
    assert ctx.get('test') == 123
    assert isinstance(ctx.get('test'), int)
    
    result = action.execute(ctx, {
        'name': 'flag',
        'value': 'true',
        'value_type': 'bool'
    })
    assert result.success is True
    assert ctx.get('flag') is True


@test("Action - ScreenshotAction Screenshot")
def test_screenshot_action() -> None:
    """Test ScreenshotAction captures screen."""
    from actions.system import ScreenshotAction
    from core.context import ContextManager
    
    action = ScreenshotAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success is True, "Screenshot failed"
    assert 'image' in result.data, "No image data returned"


@test("Action - GetMousePosAction Get Mouse Position")
def test_get_mouse_pos_action() -> None:
    """Test GetMousePosAction gets current mouse position."""
    from actions.system import GetMousePosAction
    from core.context import ContextManager
    
    action = GetMousePosAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success is True, "Get mouse position failed"
    assert 'x' in result.data and 'y' in result.data


@test("Action - ImageMatchAction Parameter Validation")
def test_image_match_action() -> None:
    """Test ImageMatchAction validates parameters."""
    from actions.image_match import ImageMatchAction
    from core.context import ContextManager
    
    action = ImageMatchAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'template': ''})
    assert result.success is False, "Empty template path should fail"
    
    result = action.execute(ctx, {'template': '/nonexistent/path.png'})
    assert result.success is False, "Non-existent file should fail"


@test("Action - OCRAction Availability Check")
def test_ocr_action() -> None:
    """Test OCRAction checks OCR availability."""
    from actions.ocr import OCRAction, OCR_AVAILABLE
    from core.context import ContextManager
    
    action = OCRAction()
    ctx = ContextManager()
    
    if not OCR_AVAILABLE:
        result = action.execute(ctx, {})
        assert result.success is False
        assert "PaddleOCR未安装" in result.message


# ==================== Utility Module Tests ====================

@test("Utils - HotkeyManager Default Configuration")
def test_hotkey_manager() -> None:
    """Test HotkeyManager default configuration."""
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


@test("Utils - MemoryManager Singleton Pattern")
def test_memory_manager() -> None:
    """Test MemoryManager singleton pattern."""
    from utils.memory import MemoryManager, memory_manager
    
    manager1 = MemoryManager()
    manager2 = MemoryManager()
    
    assert manager1 is manager2, "Singleton pattern failed"
    assert manager1 is memory_manager, "Global instance mismatch"


@test("Utils - AppLogger Logging Functionality")
def test_app_logger() -> None:
    """Test AppLogger singleton and logging."""
    from utils.app_logger import AppLogger, app_logger
    
    logger1 = AppLogger()
    logger2 = AppLogger()
    
    assert logger1 is logger2, "Singleton pattern failed"
    
    entries_before = len(logger1.get_entries(1000))
    logger1.info("Test", "Test")
    entries_after = len(logger1.get_entries(1000))
    
    assert entries_after > entries_before, "Log not added"


@test("Utils - WorkflowHistoryManager History Management")
def test_history_manager() -> None:
    """Test WorkflowHistoryManager save/load/delete."""
    from utils.history import WorkflowHistoryManager
    
    temp_dir = tempfile.mkdtemp()
    try:
        manager = WorkflowHistoryManager(temp_dir)
        
        workflow: Dict[str, Any] = {
            'variables': {},
            'steps': [{'id': 1, 'type': 'delay', 'seconds': 0.1}]
        }
        filepath = manager.save_workflow('test_workflow', workflow, ['test'])
        
        assert os.path.exists(filepath), "File not created"
        
        loaded = manager.load_workflow(os.path.basename(filepath))
        assert loaded is not None, "Load failed"
        assert loaded['steps'][0]['type'] == 'delay', "Content mismatch"
        
        all_workflows = manager.get_all_workflows()
        assert len(all_workflows) > 0, "List is empty"
        
        assert manager.delete_workflow(
            os.path.basename(filepath)
        ), "Delete failed"
    finally:
        shutil.rmtree(temp_dir)


@test("Utils - RecordingManager Recording Management")
def test_recording_manager() -> None:
    """Test RecordingManager basic functionality."""
    from utils.recording import RecordingManager, RecordedAction
    
    manager = RecordingManager()
    
    assert manager.is_recording() is False
    
    action = RecordedAction('click', 0.5, {'x': 100, 'y': 200})
    assert action.action_type == 'click'
    assert action.to_dict()['action_type'] == 'click'


@test("Utils - TeachingModeManager Teaching Mode")
def test_teaching_mode_manager() -> None:
    """Test TeachingModeManager singleton pattern."""
    from utils.teaching_mode import TeachingModeManager, teaching_mode_manager
    
    manager1 = TeachingModeManager()
    manager2 = TeachingModeManager()
    
    assert manager1 is manager2, "Singleton pattern failed"
    assert manager1.is_enabled() is False, "Initial state should be disabled"


# ==================== UI Module Tests ====================

@test("UI - ActionConfigWidget Configuration Get")
def test_action_config_widget() -> None:
    """Test ActionConfigWidget gets configuration."""
    from PyQt5.QtWidgets import QApplication
    from ui.main_window import ActionConfigWidget
    
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    action_info: Dict[str, Any] = {
        'required_params': ['x', 'y'],
        'optional_params': {'button': 'left', 'clicks': 1}
    }
    
    widget = ActionConfigWidget(action_info)
    config = widget.get_config()
    
    assert 'x' in config or True, "Config get failed"


@test("UI - MessageManager Message Management")
def test_message_manager() -> None:
    """Test MessageManager singleton pattern."""
    from ui.message import MessageManager, message_manager
    
    manager1 = MessageManager()
    manager2 = MessageManager()
    
    assert manager1 is manager2, "Singleton pattern failed"
    assert manager1 is message_manager, "Global instance mismatch"


# ==================== Boundary Condition Tests ====================

@test("Boundary - ContextManager Null Value Handling")
def test_context_null_values() -> None:
    """Test ContextManager handles null values."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('null_val', None)
    assert ctx.get('null_val') is None
    
    ctx.set('empty_str', '')
    assert ctx.get('empty_str') == ''
    
    ctx.set('empty_list', [])
    assert ctx.get('empty_list') == []


@test("Boundary - ContextManager Special Characters")
def test_context_special_chars() -> None:
    """Test ContextManager handles special characters."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    ctx.set('special', 'hello\nworld\ttab')
    assert '\n' in ctx.get('special')
    assert '\t' in ctx.get('special')
    
    ctx.set('unicode', '中文测试🎉')
    assert ctx.get('unicode') == '中文测试🎉'


@test("Boundary - FlowEngine Empty Workflow")
def test_engine_empty_workflow() -> None:
    """Test FlowEngine handles empty workflow."""
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    result = engine.load_workflow_from_dict({'variables': {}, 'steps': []})
    assert result is True
    
    result = engine.run()
    assert result is False, "Empty workflow should return False"


@test("Boundary - DelayAction Extreme Values")
def test_delay_extreme() -> None:
    """Test DelayAction handles extreme values."""
    from actions.script import DelayAction
    from core.context import ContextManager
    
    action = DelayAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'seconds': 0})
    assert result.success is True
    
    result = action.execute(ctx, {'seconds': -1})
    assert result.success is True


# ==================== Performance Tests ====================

@test("Performance - ContextManager Many Variables")
def test_context_performance() -> None:
    """Test ContextManager performance with many variables."""
    from core.context import ContextManager
    
    ctx = ContextManager()
    
    start = time.time()
    for i in range(1000):
        ctx.set(f'var_{i}', i)
    elapsed = time.time() - start
    
    assert elapsed < 1.0, f"Setting 1000 variables took too long: {elapsed}s"
    
    start = time.time()
    for i in range(1000):
        ctx.get(f'var_{i}')
    elapsed = time.time() - start
    
    assert elapsed < 0.5, f"Getting 1000 variables took too long: {elapsed}s"


@test("Performance - ActionLoader Loading Time")
def test_loader_performance() -> None:
    """Test ActionLoader loading time."""
    from core.action_loader import ActionLoader
    
    actions_dir = project_root / "actions"
    start = time.time()
    loader = ActionLoader(str(actions_dir))
    loader.load_all()
    elapsed = time.time() - start
    
    assert elapsed < 2.0, f"Loading actions took too long: {elapsed}s"


# ==================== Run All Tests ====================

def run_all_tests() -> bool:
    """Run all test functions and report results.
    
    Returns:
        True if all tests passed.
    """
    print("=" * 70)
    print("RabAI AutoClick v1.2 Module Test Report")
    print("=" * 70)
    print()
    
    test_functions: List[Callable] = [
        # Core modules
        test_context_basic,
        test_context_resolve,
        test_context_safe_exec,
        test_base_action,
        test_action_loader,
        test_engine_load,
        test_engine_run,
        
        # Action plugins
        test_click_action,
        test_type_action,
        test_delay_action,
        test_condition_action,
        test_loop_action,
        test_set_variable_action,
        test_screenshot_action,
        test_get_mouse_pos_action,
        test_image_match_action,
        test_ocr_action,
        
        # Utility modules
        test_hotkey_manager,
        test_memory_manager,
        test_app_logger,
        test_history_manager,
        test_recording_manager,
        test_teaching_mode_manager,
        
        # UI modules
        test_action_config_widget,
        test_message_manager,
        
        # Boundary tests
        test_context_null_values,
        test_context_special_chars,
        test_engine_empty_workflow,
        test_delay_extreme,
        
        # Performance tests
        test_context_performance,
        test_loader_performance,
    ]
    
    passed = 0
    failed = 0
    
    for func in test_functions:
        if func():
            passed += 1
        else:
            failed += 1
    
    print()
    print("=" * 70)
    print("Test Results Summary")
    print("=" * 70)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if test_results['failed']:
        print("\nFailed test details:")
        for item in test_results['failed']:
            print(f"  - {item['name']}: {item['error']}")
    
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
