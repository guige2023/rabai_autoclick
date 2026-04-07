"""Action registry for RabAI AutoClick.

This module provides a registry of all available actions with their
metadata and documentation.
"""

from typing import Any, Dict, List, Optional, Type

from core.base_action import BaseAction


# Action categories
ACTION_CATEGORIES = {
    'mouse': 'Mouse Actions',
    'keyboard': 'Keyboard Actions',
    'image': 'Image Recognition Actions',
    'ocr': 'OCR Actions',
    'system': 'System Actions',
    'clipboard': 'Clipboard Actions',
    'variable': 'Variable Actions',
    'flow': 'Flow Control Actions',
    'wait': 'Wait Actions',
    'window': 'Window Management Actions',
    'notification': 'Notification Actions',
    'sound': 'Sound Actions',
    'screenshot': 'Screenshot Actions',
}


# Action registry with category and description
ACTION_REGISTRY: Dict[str, Dict[str, Any]] = {
    # Mouse actions (from actions/mouse.py)
    'mouse_click': {
        'category': 'mouse',
        'display_name': '鼠标单击',
        'description': '模拟鼠标单击操作',
        'module': 'actions.mouse',
        'class': 'MouseClickAction',
    },
    'double_click': {
        'category': 'mouse',
        'display_name': '鼠标双击',
        'description': '模拟鼠标双击操作',
        'module': 'actions.mouse',
        'class': 'DoubleClickAction',
    },
    'scroll': {
        'category': 'mouse',
        'display_name': '鼠标滚轮',
        'description': '模拟鼠标滚轮滚动操作',
        'module': 'actions.mouse',
        'class': 'ScrollAction',
    },
    'mouse_move': {
        'category': 'mouse',
        'display_name': '鼠标移动',
        'description': '移动鼠标到指定位置，不执行点击',
        'module': 'actions.mouse',
        'class': 'MouseMoveAction',
    },
    'drag': {
        'category': 'mouse',
        'display_name': '鼠标拖拽',
        'description': '从起点拖拽到终点',
        'module': 'actions.mouse',
        'class': 'DragAction',
    },

    # Keyboard actions (from actions/keyboard.py)
    'type_text': {
        'category': 'keyboard',
        'display_name': '键盘输入',
        'description': '模拟键盘输入文本内容',
        'module': 'actions.keyboard',
        'class': 'TypeAction',
    },
    'key_press': {
        'category': 'keyboard',
        'display_name': '按键操作',
        'description': '模拟按下特定按键或组合键',
        'module': 'actions.keyboard',
        'class': 'KeyPressAction',
    },

    # Image actions (from actions/image_match.py)
    'click_image': {
        'category': 'image',
        'display_name': '图像识别点击',
        'description': '通过图像模板匹配定位并点击目标',
        'module': 'actions.image_match',
        'class': 'ImageMatchAction',
    },
    'find_image': {
        'category': 'image',
        'display_name': '查找图像',
        'description': '查找屏幕上的图像并返回坐标，不执行点击',
        'module': 'actions.image_match',
        'class': 'FindImageAction',
    },

    # OCR actions (from actions/ocr.py)
    'ocr': {
        'category': 'ocr',
        'display_name': 'OCR文字识别',
        'description': '识别屏幕指定区域的文字内容，支持精确匹配和模糊匹配',
        'module': 'actions.ocr',
        'class': 'OCRAction',
    },

    # System actions (from actions/system.py)
    'screenshot': {
        'category': 'system',
        'display_name': '截图',
        'description': '截取屏幕指定区域并保存',
        'module': 'actions.system',
        'class': 'ScreenshotAction',
    },
    'get_mouse_pos': {
        'category': 'system',
        'display_name': '获取鼠标位置',
        'description': '获取当前鼠标位置坐标',
        'module': 'actions.system',
        'class': 'GetMousePosAction',
    },
    'alert': {
        'category': 'system',
        'display_name': '弹出提示',
        'description': '显示提示对话框',
        'module': 'actions.system',
        'class': 'AlertAction',
    },

    # Clipboard actions (from actions/clipboard.py)
    'clipboard_copy': {
        'category': 'clipboard',
        'display_name': '复制到剪贴板',
        'description': '将文本复制到系统剪贴板',
        'module': 'actions.clipboard',
        'class': 'CopyAction',
    },
    'clipboard_paste': {
        'category': 'clipboard',
        'display_name': '粘贴',
        'description': '模拟键盘粘贴剪贴板内容',
        'module': 'actions.clipboard',
        'class': 'PasteAction',
    },
    'get_clipboard': {
        'category': 'clipboard',
        'display_name': '获取剪贴板',
        'description': '获取当前剪贴板的内容',
        'module': 'actions.clipboard',
        'class': 'GetClipboardAction',
    },
    'clear_clipboard': {
        'category': 'clipboard',
        'display_name': '清空剪贴板',
        'description': '清空系统剪贴板的内容',
        'module': 'actions.clipboard',
        'class': 'ClearClipboardAction',
    },

    # Variable actions (from actions/variable.py)
    'set_variable': {
        'category': 'variable',
        'display_name': '设置变量',
        'description': '设置工作流上下文中的变量值',
        'module': 'actions.variable',
        'class': 'SetVariableAction',
    },
    'get_variable': {
        'category': 'variable',
        'display_name': '获取变量',
        'description': '获取工作流上下文中的变量值',
        'module': 'actions.variable',
        'class': 'GetVariableAction',
    },
    'delete_variable': {
        'category': 'variable',
        'display_name': '删除变量',
        'description': '删除工作流上下文中的变量',
        'module': 'actions.variable',
        'class': 'DeleteVariableAction',
    },
    'clear_variables': {
        'category': 'variable',
        'display_name': '清除变量',
        'description': '清除工作流上下文中的所有变量',
        'module': 'actions.variable',
        'class': 'ClearVariablesAction',
    },
    'math_op': {
        'category': 'variable',
        'display_name': '数学运算',
        'description': '对变量执行数学运算',
        'module': 'actions.variable',
        'class': 'MathOperationAction',
    },
    'string_op': {
        'category': 'variable',
        'display_name': '字符串操作',
        'description': '对字符串执行操作',
        'module': 'actions.variable',
        'class': 'StringOperationAction',
    },

    # Flow control actions (from actions/flow_control.py)
    'loop': {
        'category': 'flow',
        'display_name': '循环',
        'description': '循环执行指定次数',
        'module': 'actions.flow_control',
        'class': 'LoopAction',
    },
    'while_loop': {
        'category': 'flow',
        'display_name': '条件循环',
        'description': '当条件满足时循环执行',
        'module': 'actions.flow_control',
        'class': 'WhileAction',
    },
    'condition': {
        'category': 'flow',
        'display_name': '条件分支',
        'description': '根据条件表达式的结果选择执行路径',
        'module': 'actions.flow_control',
        'class': 'ConditionAction',
    },
    'break': {
        'category': 'flow',
        'display_name': '跳出循环',
        'description': '立即跳出当前循环',
        'module': 'actions.flow_control',
        'class': 'BreakAction',
    },
    'continue': {
        'category': 'flow',
        'display_name': '继续循环',
        'description': '跳到下一次循环迭代',
        'module': 'actions.flow_control',
        'class': 'ContinueAction',
    },

    # Wait actions (from actions/wait.py)
    'wait': {
        'category': 'wait',
        'display_name': '等待',
        'description': '等待指定的时间长度',
        'module': 'actions.wait',
        'class': 'WaitAction',
    },
    'wait_for_image': {
        'category': 'wait',
        'display_name': '等待图像',
        'description': '等待图像出现在屏幕上或从屏幕消失',
        'module': 'actions.wait',
        'class': 'WaitForImageAction',
    },
    'wait_for_condition': {
        'category': 'wait',
        'display_name': '等待条件',
        'description': '等待条件表达式满足',
        'module': 'actions.wait',
        'class': 'WaitForConditionAction',
    },

    # Window actions (from actions/window.py)
    'window_focus': {
        'category': 'window',
        'display_name': '聚焦窗口',
        'description': '通过窗口标题聚焦指定窗口',
        'module': 'actions.window',
        'class': 'WindowFocusAction',
    },
    'window_move': {
        'category': 'window',
        'display_name': '移动窗口',
        'description': '将窗口移动到指定位置',
        'module': 'actions.window',
        'class': 'WindowMoveAction',
    },
    'window_resize': {
        'category': 'window',
        'display_name': '调整窗口大小',
        'description': '调整窗口到指定尺寸',
        'module': 'actions.window',
        'class': 'WindowResizeAction',
    },
    'window_minimize': {
        'category': 'window',
        'display_name': '最小化窗口',
        'description': '最小化指定窗口',
        'module': 'actions.window',
        'class': 'WindowMinimizeAction',
    },
    'window_maximize': {
        'category': 'window',
        'display_name': '最大化窗口',
        'description': '最大化指定窗口',
        'module': 'actions.window',
        'class': 'WindowMaximizeAction',
    },
    'window_close': {
        'category': 'window',
        'display_name': '关闭窗口',
        'description': '关闭指定窗口',
        'module': 'actions.window',
        'class': 'WindowCloseAction',
    },

    # Notification actions (from actions/notification.py)
    'notify': {
        'category': 'notification',
        'display_name': '系统通知',
        'description': '发送系统通知',
        'module': 'actions.notification',
        'class': 'NotifyAction',
    },
    'log_message': {
        'category': 'notification',
        'display_name': '记录日志',
        'description': '向应用日志写入消息',
        'module': 'actions.notification',
        'class': 'LogMessageAction',
    },

    # Sound actions (from actions/sound.py)
    'play_sound': {
        'category': 'sound',
        'display_name': '播放声音',
        'description': '播放指定的声音文件',
        'module': 'actions.sound',
        'class': 'PlaySoundAction',
    },
    'beep': {
        'category': 'sound',
        'display_name': '蜂鸣声',
        'description': '播放蜂鸣提示音',
        'module': 'actions.sound',
        'class': 'BeepAction',
    },
    'system_sound': {
        'category': 'sound',
        'display_name': '系统声音',
        'description': '播放macOS系统声音',
        'module': 'actions.sound',
        'class': 'SystemSoundAction',
    },

    # Screenshot actions (from actions/screenshot.py)
    'screen_capture': {
        'category': 'screenshot',
        'display_name': '屏幕截图',
        'description': '截取屏幕或指定区域的图像',
        'module': 'actions.screenshot',
        'class': 'ScreenCaptureAction',
    },
    'capture_region': {
        'category': 'screenshot',
        'display_name': '区域截图',
        'description': '截取指定区域的屏幕图像并保存',
        'module': 'actions.screenshot',
        'class': 'ScreenCaptureRegionAction',
    },
    'compare_images': {
        'category': 'screenshot',
        'display_name': '图像对比',
        'description': '比较两张图像的相似度',
        'module': 'actions.screenshot',
        'class': 'CompareImagesAction',
    },
}


def get_all_actions() -> List[str]:
    """Get list of all registered action types.

    Returns:
        List of action type strings.
    """
    return list(ACTION_REGISTRY.keys())


def get_action_info(action_type: str) -> Optional[Dict[str, Any]]:
    """Get information about a specific action.

    Args:
        action_type: The action type string.

    Returns:
        Dictionary with action info or None if not found.
    """
    return ACTION_REGISTRY.get(action_type)


def get_actions_by_category(category: str) -> List[Dict[str, Any]]:
    """Get all actions in a specific category.

    Args:
        category: The category name.

    Returns:
        List of action info dictionaries.
    """
    return [
        info for info in ACTION_REGISTRY.values()
        if info.get('category') == category
    ]


def get_categories() -> Dict[str, str]:
    """Get all action categories.

    Returns:
        Dictionary mapping category names to display names.
    """
    return ACTION_CATEGORIES.copy()


def get_action_count() -> int:
    """Get total number of registered actions.

    Returns:
        Number of registered actions.
    """
    return len(ACTION_REGISTRY)