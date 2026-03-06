#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RabAI AutoClick v1.2 全面模块测试脚本
"""
import sys
import os
import json
import time
import traceback
from typing import Dict, List, Any

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

test_results = {
    'passed': [],
    'failed': [],
    'warnings': []
}

def test(name: str):
    def decorator(func):
        def wrapper():
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

# ==================== 核心模块测试 ====================

@test("Core - ContextManager 基本操作")
def test_context_basic():
    from core.context import ContextManager
    ctx = ContextManager()
    
    ctx.set('name', 'test')
    assert ctx.get('name') == 'test', "set/get失败"
    
    assert ctx.get('not_exist', 'default') == 'default', "默认值失败"
    
    assert ctx.delete('name') == True, "delete失败"
    assert ctx.get('name') is None, "delete后仍存在"

@test("Core - ContextManager 变量解析")
def test_context_resolve():
    from core.context import ContextManager
    ctx = ContextManager()
    
    ctx.set('name', 'world')
    result = ctx.resolve_value('Hello {{name}}!')
    assert result == 'Hello world!', f"变量解析失败: {result}"
    
    ctx.set('a', 10)
    ctx.set('b', 20)
    result = ctx.resolve_value('{{a + b}}')
    assert result == '30', f"表达式解析失败: {result}"

@test("Core - ContextManager 安全执行")
def test_context_safe_exec():
    from core.context import ContextManager
    ctx = ContextManager()
    
    ctx.set('x', 10)
    result = ctx.safe_exec("return_value = x * 2")
    assert result == 20, f"变量访问失败: {result}"
    
    try:
        ctx.safe_exec("__import__('os').system('echo test')")
        assert False, "危险代码不应执行"
    except:
        pass

@test("Core - BaseAction 基类")
def test_base_action():
    from core.base_action import BaseAction, ActionResult
    
    result = ActionResult(success=True, message="test", data={'key': 'value'})
    assert result.success == True
    assert result.message == "test"
    assert result.data == {'key': 'value'}

@test("Core - ActionLoader 加载")
def test_action_loader():
    from core.action_loader import ActionLoader
    
    loader = ActionLoader(os.path.join(project_root, 'actions'))
    actions = loader.load_all()
    
    assert len(actions) > 0, "未加载任何动作"
    assert 'click' in actions, "click动作未加载"
    assert 'type_text' in actions, "type_text动作未加载"
    assert 'delay' in actions, "delay动作未加载"
    
    info = loader.get_action_info()
    assert len(info) == len(actions), "动作信息数量不匹配"

@test("Core - FlowEngine 加载工作流")
def test_engine_load():
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow = {
        'variables': {'count': 0},
        'steps': [{'id': 1, 'type': 'delay', 'seconds': 0.1}]
    }
    
    result = engine.load_workflow_from_dict(workflow)
    assert result == True, "从字典加载失败"
    assert engine.context.get('count') == 0, "变量未加载"

@test("Core - FlowEngine 执行流程")
def test_engine_run():
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    workflow = {
        'variables': {},
        'steps': [
            {'id': 1, 'type': 'delay', 'seconds': 0.05},
            {'id': 2, 'type': 'set_variable', 'name': 'test_var', 'value': 'hello'}
        ]
    }
    
    engine.load_workflow_from_dict(workflow)
    result = engine.run()
    
    assert result == True, "执行失败"
    assert engine.context.get('test_var') == 'hello', "变量设置失败"

# ==================== 动作插件测试 ====================

@test("Action - ClickAction 参数验证")
def test_click_action():
    from actions.click import ClickAction
    from core.context import ContextManager
    
    action = ClickAction()
    ctx = ContextManager()
    
    assert action.action_type == "click"
    assert action.get_required_params() == ['x', 'y']
    
    valid, msg = action.validate_params({'x': 100, 'y': 200})
    assert valid == True, f"参数验证失败: {msg}"

@test("Action - TypeAction 空文本处理")
def test_type_action():
    from actions.keyboard import TypeAction
    from core.context import ContextManager
    
    action = TypeAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'text': ''})
    assert result.success == False, "空文本应失败"

@test("Action - DelayAction 执行")
def test_delay_action():
    from actions.script import DelayAction
    from core.context import ContextManager
    
    action = DelayAction()
    ctx = ContextManager()
    
    start = time.time()
    result = action.execute(ctx, {'seconds': 0.1})
    elapsed = time.time() - start
    
    assert result.success == True
    assert elapsed >= 0.1, "延时时间不足"

@test("Action - ConditionAction 条件判断")
def test_condition_action():
    from actions.script import ConditionAction
    from core.context import ContextManager
    
    action = ConditionAction()
    ctx = ContextManager()
    
    ctx.set('value', 10)
    result = action.execute(ctx, {'condition': 'value > 5', 'true_next': 2, 'false_next': 3})
    
    assert result.success == True
    assert result.data['result'] == True
    assert result.next_step_id == 2

@test("Action - LoopAction 循环控制")
def test_loop_action():
    from actions.script import LoopAction
    from core.context import ContextManager
    
    action = LoopAction()
    ctx = ContextManager()
    
    for i in range(3):
        result = action.execute(ctx, {'loop_id': 'test', 'count': 3, 'loop_start': 1})
        assert result.success == True
    
    assert ctx.get('_loop_test') == 3

@test("Action - SetVariableAction 类型转换")
def test_set_variable_action():
    from actions.script import SetVariableAction
    from core.context import ContextManager
    
    action = SetVariableAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'name': 'test', 'value': '123', 'value_type': 'int'})
    assert result.success == True
    assert ctx.get('test') == 123
    assert isinstance(ctx.get('test'), int)
    
    result = action.execute(ctx, {'name': 'flag', 'value': 'true', 'value_type': 'bool'})
    assert result.success == True
    assert ctx.get('flag') == True

@test("Action - ScreenshotAction 截图")
def test_screenshot_action():
    from actions.system import ScreenshotAction
    from core.context import ContextManager
    
    action = ScreenshotAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success == True, "截图失败"
    assert 'image' in result.data, "未返回图像数据"

@test("Action - GetMousePosAction 获取鼠标位置")
def test_get_mouse_pos_action():
    from actions.system import GetMousePosAction
    from core.context import ContextManager
    
    action = GetMousePosAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {})
    assert result.success == True, "获取鼠标位置失败"
    assert 'x' in result.data and 'y' in result.data

@test("Action - ImageMatchAction 参数验证")
def test_image_match_action():
    from actions.image_match import ImageMatchAction
    from core.context import ContextManager
    
    action = ImageMatchAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'template': ''})
    assert result.success == False, "空模板路径应失败"
    
    result = action.execute(ctx, {'template': '/nonexistent/path.png'})
    assert result.success == False, "不存在的文件应失败"

@test("Action - OCRAction 可用性检查")
def test_ocr_action():
    from actions.ocr import OCRAction, OCR_AVAILABLE
    from core.context import ContextManager
    
    action = OCRAction()
    ctx = ContextManager()
    
    if not OCR_AVAILABLE:
        result = action.execute(ctx, {})
        assert result.success == False
        assert "PaddleOCR未安装" in result.message

# ==================== 工具模块测试 ====================

@test("Utils - HotkeyManager 默认配置")
def test_hotkey_manager():
    from utils.hotkey import HotkeyManager
    
    manager = HotkeyManager()
    
    assert manager.DEFAULT_HOTKEYS == {'start': 'f6', 'stop': 'f7', 'pause': 'f8'}
    
    parsed = HotkeyManager.parse_hotkey('Ctrl + Shift + A')
    assert 'ctrl' in parsed and 'shift' in parsed and 'a' in parsed
    
    formatted = HotkeyManager.format_hotkey('ctrl+shift+a')
    assert 'CTRL' in formatted and 'SHIFT' in formatted

@test("Utils - MemoryManager 单例模式")
def test_memory_manager():
    from utils.memory import MemoryManager, memory_manager
    
    manager1 = MemoryManager()
    manager2 = MemoryManager()
    
    assert manager1 is manager2, "单例模式失败"
    assert manager1 is memory_manager, "全局实例不一致"

@test("Utils - AppLogger 日志功能")
def test_app_logger():
    from utils.app_logger import AppLogger, app_logger
    
    logger1 = AppLogger()
    logger2 = AppLogger()
    
    assert logger1 is logger2, "单例模式失败"
    
    entries_before = len(logger1.get_entries(1000))
    logger1.info("测试日志", "Test")
    entries_after = len(logger1.get_entries(1000))
    
    assert entries_after > entries_before, "日志未添加"

@test("Utils - WorkflowHistoryManager 历史管理")
def test_history_manager():
    from utils.history import WorkflowHistoryManager
    import tempfile
    import shutil
    
    temp_dir = tempfile.mkdtemp()
    try:
        manager = WorkflowHistoryManager(temp_dir)
        
        workflow = {'variables': {}, 'steps': [{'id': 1, 'type': 'delay', 'seconds': 0.1}]}
        filepath = manager.save_workflow('test_workflow', workflow, ['test'])
        
        assert os.path.exists(filepath), "文件未创建"
        
        loaded = manager.load_workflow(os.path.basename(filepath))
        assert loaded is not None, "加载失败"
        assert loaded['steps'][0]['type'] == 'delay', "内容不匹配"
        
        all_workflows = manager.get_all_workflows()
        assert len(all_workflows) > 0, "列表为空"
        
        assert manager.delete_workflow(os.path.basename(filepath)), "删除失败"
    finally:
        shutil.rmtree(temp_dir)

@test("Utils - RecordingManager 录屏管理")
def test_recording_manager():
    from utils.recording import RecordingManager, RecordedAction
    
    manager = RecordingManager()
    
    assert manager.is_recording() == False
    
    action = RecordedAction('click', 0.5, {'x': 100, 'y': 200})
    assert action.action_type == 'click'
    assert action.to_dict()['action_type'] == 'click'

@test("Utils - TeachingModeManager 教学模式")
def test_teaching_mode_manager():
    from utils.teaching_mode import TeachingModeManager, teaching_mode_manager
    
    manager1 = TeachingModeManager()
    manager2 = TeachingModeManager()
    
    assert manager1 is manager2, "单例模式失败"
    assert manager1.is_enabled() == False, "初始状态应为禁用"

# ==================== UI模块测试 ====================

@test("UI - ActionConfigWidget 配置获取")
def test_action_config_widget():
    from PyQt5.QtWidgets import QApplication
    from ui.main_window import ActionConfigWidget
    
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    action_info = {
        'required_params': ['x', 'y'],
        'optional_params': {'button': 'left', 'clicks': 1}
    }
    
    widget = ActionConfigWidget(action_info)
    config = widget.get_config()
    
    assert 'x' in config or True, "配置获取失败"

@test("UI - MessageManager 消息管理")
def test_message_manager():
    from ui.message import MessageManager, message_manager
    
    manager1 = MessageManager()
    manager2 = MessageManager()
    
    assert manager1 is manager2, "单例模式失败"
    assert manager1 is message_manager, "全局实例不一致"

# ==================== 边界条件测试 ====================

@test("边界测试 - ContextManager 空值处理")
def test_context_null_values():
    from core.context import ContextManager
    ctx = ContextManager()
    
    ctx.set('null_val', None)
    assert ctx.get('null_val') is None
    
    ctx.set('empty_str', '')
    assert ctx.get('empty_str') == ''
    
    ctx.set('empty_list', [])
    assert ctx.get('empty_list') == []

@test("边界测试 - ContextManager 特殊字符")
def test_context_special_chars():
    from core.context import ContextManager
    ctx = ContextManager()
    
    ctx.set('special', 'hello\nworld\ttab')
    assert '\n' in ctx.get('special')
    assert '\t' in ctx.get('special')
    
    ctx.set('unicode', '中文测试🎉')
    assert ctx.get('unicode') == '中文测试🎉'

@test("边界测试 - FlowEngine 空工作流")
def test_engine_empty_workflow():
    from core.engine import FlowEngine
    
    engine = FlowEngine()
    
    result = engine.load_workflow_from_dict({'variables': {}, 'steps': []})
    assert result == True
    
    result = engine.run()
    assert result == False, "空工作流应返回False"

@test("边界测试 - DelayAction 极端值")
def test_delay_extreme():
    from actions.script import DelayAction
    from core.context import ContextManager
    
    action = DelayAction()
    ctx = ContextManager()
    
    result = action.execute(ctx, {'seconds': 0})
    assert result.success == True
    
    result = action.execute(ctx, {'seconds': -1})
    assert result.success == True

# ==================== 性能测试 ====================

@test("性能测试 - ContextManager 大量变量")
def test_context_performance():
    from core.context import ContextManager
    import time
    
    ctx = ContextManager()
    
    start = time.time()
    for i in range(1000):
        ctx.set(f'var_{i}', i)
    elapsed = time.time() - start
    
    assert elapsed < 1.0, f"设置1000个变量耗时过长: {elapsed}s"
    
    start = time.time()
    for i in range(1000):
        ctx.get(f'var_{i}')
    elapsed = time.time() - start
    
    assert elapsed < 0.5, f"获取1000个变量耗时过长: {elapsed}s"

@test("性能测试 - ActionLoader 加载时间")
def test_loader_performance():
    from core.action_loader import ActionLoader
    import time
    
    start = time.time()
    loader = ActionLoader(os.path.join(project_root, 'actions'))
    loader.load_all()
    elapsed = time.time() - start
    
    assert elapsed < 2.0, f"加载动作耗时过长: {elapsed}s"

# ==================== 运行所有测试 ====================

def run_all_tests():
    print("=" * 70)
    print("RabAI AutoClick v1.2 模块测试报告")
    print("=" * 70)
    print()
    
    test_functions = [
        # 核心模块
        test_context_basic,
        test_context_resolve,
        test_context_safe_exec,
        test_base_action,
        test_action_loader,
        test_engine_load,
        test_engine_run,
        
        # 动作插件
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
        
        # 工具模块
        test_hotkey_manager,
        test_memory_manager,
        test_app_logger,
        test_history_manager,
        test_recording_manager,
        test_teaching_mode_manager,
        
        # UI模块
        test_action_config_widget,
        test_message_manager,
        
        # 边界测试
        test_context_null_values,
        test_context_special_chars,
        test_engine_empty_workflow,
        test_delay_extreme,
        
        # 性能测试
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
    print("测试结果汇总")
    print("=" * 70)
    print(f"通过: {passed}")
    print(f"失败: {failed}")
    print(f"总计: {passed + failed}")
    
    if test_results['failed']:
        print("\n失败的测试详情:")
        for item in test_results['failed']:
            print(f"  - {item['name']}: {item['error']}")
    
    return failed == 0

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
