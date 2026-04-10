"""
宏录制和播放系统 v22
支持动作录制、回放、编辑、变量、宏库、定时播放、条件执行、导入导出
"""
import json
import time
import uuid
import os
import re
import threading
import subprocess
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timedelta
from croniter import croniter
import hashlib
import shutil
import tempfile


class MacroActionType(Enum):
    """宏动作类型"""
    MOUSE_CLICK = "mouse_click"
    MOUSE_DOUBLE_CLICK = "mouse_double_click"
    MOUSE_RIGHT_CLICK = "mouse_right_click"
    MOUSE_MOVE = "mouse_move"
    MOUSE_DRAG = "mouse_drag"
    KEYBOARD_PRESS = "keyboard_press"
    KEYBOARD_TYPE = "keyboard_type"
    KEYBOARD_COMBINATION = "keyboard_combination"
    DELAY = "delay"
    SCREENSHOT = "screenshot"
    WAIT_FOR_IMAGE = "wait_for_image"
    WAIT_FOR_WINDOW = "wait_for_window"
    CUSTOM = "custom"


class MouseButton(Enum):
    """鼠标按钮"""
    LEFT = "left"
    RIGHT = "right"
    MIDDLE = "middle"


class MacroConditionType(Enum):
    """宏条件类型"""
    FILE_EXISTS = "file_exists"
    FILE_NOT_EXISTS = "file_not_exists"
    IMAGE_FOUND = "image_found"
    IMAGE_NOT_FOUND = "image_not_found"
    VARIABLE_EQUALS = "variable_equals"
    VARIABLE_NOT_EQUALS = "variable_not_equals"
    VARIABLE_CONTAINS = "variable_contains"
    TIME_BETWEEN = "time_between"
    DAY_OF_WEEK = "day_of_week"
    SCREEN_REGION_MATCH = "screen_region_match"
    CUSTOM = "custom"


class PlaybackResult(Enum):
    """播放结果"""
    SUCCESS = "success"
    STOPPED = "stopped"
    SKIPPED = "skipped"
    FAILED = "failed"
    ERROR = "error"


@dataclass
class MacroPosition:
    """宏位置坐标"""
    x: int
    y: int
    screen: int = 0  # 多屏幕支持


@dataclass
class MacroAction:
    """宏动作"""
    action_id: str
    action_type: MacroActionType
    params: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    delay_after: float = 0.0  # 动作后延迟
    delay_before: float = 0.0  # 动作前延迟
    description: str = ""
    enabled: bool = True
    repeat_count: int = 1
    repeat_interval: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value if isinstance(self.action_type, MacroActionType) else self.action_type,
            "params": self.params,
            "timestamp": self.timestamp,
            "delay_after": self.delay_after,
            "delay_before": self.delay_before,
            "description": self.description,
            "enabled": self.enabled,
            "repeat_count": self.repeat_count,
            "repeat_interval": self.repeat_interval
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MacroAction":
        action_type = data.get("action_type")
        if isinstance(action_type, str):
            action_type = MacroActionType(action_type)
        return cls(
            action_id=data.get("action_id", str(uuid.uuid4())),
            action_type=action_type,
            params=data.get("params", {}),
            timestamp=data.get("timestamp", time.time()),
            delay_after=data.get("delay_after", 0.0),
            delay_before=data.get("delay_before", 0.0),
            description=data.get("description", ""),
            enabled=data.get("enabled", True),
            repeat_count=data.get("repeat_count", 1),
            repeat_interval=data.get("repeat_interval", 0.0)
        )


@dataclass
class MacroCondition:
    """宏条件"""
    condition_id: str
    condition_type: MacroConditionType
    params: Dict[str, Any] = field(default_factory=dict)
    negate: bool = False  # 取反条件

    def check(self, context: Dict[str, Any]) -> bool:
        """检查条件是否满足"""
        result = self._evaluate(context)
        return not result if self.negate else result

    def _evaluate(self, context: Dict[str, Any]) -> bool:
        if self.condition_type == MacroConditionType.FILE_EXISTS:
            path = self.params.get("path", "")
            return os.path.exists(path)
        elif self.condition_type == MacroConditionType.FILE_NOT_EXISTS:
            path = self.params.get("path", "")
            return not os.path.exists(path)
        elif self.condition_type == MacroConditionType.VARIABLE_EQUALS:
            var_name = self.params.get("name", "")
            expected = self.params.get("value", "")
            return context.get(var_name) == expected
        elif self.condition_type == MacroConditionType.VARIABLE_NOT_EQUALS:
            var_name = self.params.get("name", "")
            unexpected = self.params.get("value", "")
            return context.get(var_name) != unexpected
        elif self.condition_type == MacroConditionType.VARIABLE_CONTAINS:
            var_name = self.params.get("name", "")
            substring = self.params.get("value", "")
            value = context.get(var_name, "")
            return substring in str(value)
        elif self.condition_type == MacroConditionType.TIME_BETWEEN:
            now = datetime.now().time()
            start = datetime.strptime(self.params.get("start", "00:00"), "%H:%M").time()
            end = datetime.strptime(self.params.get("end", "23:59"), "%H:%M").time()
            return start <= now <= end
        elif self.condition_type == MacroConditionType.DAY_OF_WEEK:
            days = self.params.get("days", [])
            today = datetime.now().weekday()
            return today in days
        elif self.condition_type == MacroConditionType.CUSTOM:
            func = self.params.get("func")
            if callable(func):
                return func(context)
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "condition_type": self.condition_type.value if isinstance(self.condition_type, MacroConditionType) else self.condition_type,
            "params": self.params,
            "negate": self.negate
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MacroCondition":
        cond_type = data.get("condition_type")
        if isinstance(cond_type, str):
            cond_type = MacroConditionType(cond_type)
        return cls(
            condition_id=data.get("condition_id", str(uuid.uuid4())),
            condition_type=cond_type,
            params=data.get("params", {}),
            negate=data.get("negate", False)
        )


@dataclass
class MacroVariable:
    """宏变量"""
    name: str
    value: Any
    var_type: str = "string"  # string, int, float, bool, list, dict
    description: str = ""
    default_value: Any = None
    is_secret: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "var_type": self.var_type,
            "description": self.description,
            "default_value": self.default_value,
            "is_secret": self.is_secret
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MacroVariable":
        return cls(
            name=data["name"],
            value=data.get("value"),
            var_type=data.get("var_type", "string"),
            description=data.get("description", ""),
            default_value=data.get("default_value"),
            is_secret=data.get("is_secret", False)
        )


@dataclass
class Macro:
    """宏"""
    macro_id: str
    name: str
    description: str = ""
    version: str = "22.0.0"
    actions: List[MacroAction] = field(default_factory=list)
    conditions: List[MacroCondition] = field(default_factory=list)
    variables: List[MacroVariable] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    category: str = "default"
    author: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    usage_count: int = 0
    last_used_at: Optional[float] = None
    settings: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "macro_id": self.macro_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "actions": [a.to_dict() if isinstance(a, MacroAction) else a for a in self.actions],
            "conditions": [c.to_dict() if isinstance(c, MacroCondition) else c for c in self.conditions],
            "variables": [v.to_dict() if isinstance(v, MacroVariable) else v for v in self.variables],
            "tags": self.tags,
            "category": self.category,
            "author": self.author,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage_count": self.usage_count,
            "last_used_at": self.last_used_at,
            "settings": self.settings,
            "metadata": self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Macro":
        actions = []
        for a in data.get("actions", []):
            if isinstance(a, MacroAction):
                actions.append(a)
            elif isinstance(a, dict):
                actions.append(MacroAction.from_dict(a))

        conditions = []
        for c in data.get("conditions", []):
            if isinstance(c, MacroCondition):
                conditions.append(c)
            elif isinstance(c, dict):
                conditions.append(MacroCondition.from_dict(c))

        variables = []
        for v in data.get("variables", []):
            if isinstance(v, MacroVariable):
                variables.append(v)
            elif isinstance(v, dict):
                variables.append(MacroVariable.from_dict(v))

        return cls(
            macro_id=data["macro_id"],
            name=data["name"],
            description=data.get("description", ""),
            version=data.get("version", "22.0.0"),
            actions=actions,
            conditions=conditions,
            variables=variables,
            tags=data.get("tags", []),
            category=data.get("category", "default"),
            author=data.get("author", ""),
            created_at=data.get("created_at", time.time()),
            updated_at=data.get("updated_at", time.time()),
            usage_count=data.get("usage_count", 0),
            last_used_at=data.get("last_used_at"),
            settings=data.get("settings", {}),
            metadata=data.get("metadata", {})
        )


class MacroRecorder:
    """宏录制器"""

    def __init__(self):
        self.recording = False
        self.actions: List[MacroAction] = []
        self.start_time: Optional[float] = None
        self.last_action_time: Optional[float] = None
        self._lock = threading.Lock()
        self._callbacks: List[Callable] = []

    def start_recording(self):
        """开始录制"""
        with self._lock:
            self.recording = True
            self.actions = []
            self.start_time = time.time()
            self.last_action_time = self.start_time

    def stop_recording(self) -> List[MacroAction]:
        """停止录制"""
        with self._lock:
            self.recording = False
            return self.actions.copy()

    def record_action(self, action: MacroAction):
        """录制动作"""
        with self._lock:
            if self.recording:
                if self.last_action_time:
                    action.delay_after = action.timestamp - self.last_action_time
                self.actions.append(action)
                self.last_action_time = action.timestamp
                for callback in self._callbacks:
                    try:
                        callback(action)
                    except Exception:
                        pass

    def record_mouse_click(self, x: int, y: int, button: MouseButton = MouseButton.LEFT, screen: int = 0):
        """录制鼠标点击"""
        action_type = {
            MouseButton.LEFT: MacroActionType.MOUSE_CLICK,
            MouseButton.RIGHT: MacroActionType.MOUSE_RIGHT_CLICK,
            MouseButton.MIDDLE: MacroActionType.MOUSE_CLICK
        }[button]
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=action_type,
            params={"x": x, "y": y, "button": button.value, "screen": screen},
            description=f"Mouse {button.value} click at ({x}, {y})"
        )
        self.record_action(action)
        return action

    def record_mouse_double_click(self, x: int, y: int, button: MouseButton = MouseButton.LEFT, screen: int = 0):
        """录制鼠标双击"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.MOUSE_DOUBLE_CLICK,
            params={"x": x, "y": y, "button": button.value, "screen": screen},
            description=f"Mouse double {button.value} click at ({x}, {y})"
        )
        self.record_action(action)
        return action

    def record_mouse_move(self, x: int, y: int, screen: int = 0):
        """录制鼠标移动"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.MOUSE_MOVE,
            params={"x": x, "y": y, "screen": screen},
            description=f"Mouse move to ({x}, {y})"
        )
        self.record_action(action)
        return action

    def record_mouse_drag(self, start_x: int, start_y: int, end_x: int, end_y: int, button: MouseButton = MouseButton.LEFT):
        """录制鼠标拖拽"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.MOUSE_DRAG,
            params={
                "start_x": start_x, "start_y": start_y,
                "end_x": end_x, "end_y": end_y,
                "button": button.value
            },
            description=f"Mouse drag from ({start_x}, {start_y}) to ({end_x}, {end_y})"
        )
        self.record_action(action)
        return action

    def record_keyboard_press(self, key: str):
        """录制键盘按键"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.KEYBOARD_PRESS,
            params={"key": key},
            description=f"Keyboard press: {key}"
        )
        self.record_action(action)
        return action

    def record_keyboard_type(self, text: str):
        """录制键盘输入文本"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.KEYBOARD_TYPE,
            params={"text": text},
            description=f"Keyboard type: {text[:50]}..."
        )
        self.record_action(action)
        return action

    def record_keyboard_combination(self, keys: List[str]):
        """录制键盘组合键"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.KEYBOARD_COMBINATION,
            params={"keys": keys},
            description=f"Keyboard combo: {'+'.join(keys)}"
        )
        self.record_action(action)
        return action

    def record_delay(self, duration: float):
        """录制延迟"""
        action = MacroAction(
            action_id=str(uuid.uuid4()),
            action_type=MacroActionType.DELAY,
            params={"duration": duration},
            description=f"Delay: {duration}s"
        )
        self.record_action(action)
        return action

    def add_callback(self, callback: Callable):
        """添加录制回调"""
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable):
        """移除录制回调"""
        if callback in self._callbacks:
            self._callback.remove(callback)


class MacroPlayer:
    """宏播放器"""

    def __init__(self, macro: Macro):
        self.macro = macro
        self.playing = False
        self.paused = False
        self.stopped = False
        self.current_action_index = 0
        self._lock = threading.Lock()
        self._action_callbacks: List[Callable] = []
        self._condition_callbacks: Dict[str, Callable] = {}
        self.variables: Dict[str, Any] = {}
        self.context: Dict[str, Any] = {}

    def set_variable(self, name: str, value: Any):
        """设置变量"""
        self.variables[name] = value

    def get_variable(self, name: str, default: Any = None) -> Any:
        """获取变量"""
        return self.variables.get(name, default)

    def set_context(self, key: str, value: Any):
        """设置上下文"""
        self.context[key] = value

    def add_action_callback(self, callback: Callable):
        """添加动作回调"""
        self._action_callbacks.append(callback)

    def add_condition_callback(self, condition_type: str, callback: Callable):
        """添加条件回调"""
        self._condition_callbacks[condition_type] = callback

    def play(self) -> Tuple[PlaybackResult, List[Dict]]:
        """播放宏"""
        with self._lock:
            self.playing = True
            self.paused = False
            self.stopped = False
            self.current_action_index = 0

        results = []
        start_time = time.time()

        # 初始化变量
        for var in self.macro.variables:
            self.variables[var.name] = var.value

        # 执行前置条件检查
        for condition in self.macro.conditions:
            if not condition.check(self.context):
                return PlaybackResult.SKIPPED, [{"condition": condition.condition_id, "result": "skipped"}]

        try:
            for i, action in enumerate(self.macro.actions):
                with self._lock:
                    if self.stopped:
                        return PlaybackResult.STOPPED, results
                    while self.paused:
                        time.sleep(0.1)

                self.current_action_index = i

                if not action.enabled:
                    results.append({"action_id": action.action_id, "result": "disabled", "skipped": True})
                    continue

                # 处理变量替换
                params = self._replace_variables(action.params)

                # 执行动作
                result = self._execute_action(action, params)
                results.append(result)

                if result.get("result") == "error":
                    return PlaybackResult.ERROR, results

                # 动作间延迟
                if action.delay_after > 0:
                    time.sleep(action.delay_after)

                # 处理重复
                for r in range(action.repeat_count - 1):
                    if self.stopped:
                        return PlaybackResult.STOPPED, results
                    time.sleep(action.repeat_interval)
                    result = self._execute_action(action, params)
                    results.append(result)

                # 通知回调
                for callback in self._action_callbacks:
                    try:
                        callback(action, result)
                    except Exception:
                        pass

            elapsed = time.time() - start_time
            return PlaybackResult.SUCCESS, results

        except Exception as e:
            return PlaybackResult.ERROR, [{"error": str(e)}]

    def _replace_variables(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """替换参数中的变量"""
        result = {}
        for key, value in params.items():
            if isinstance(value, str):
                # 替换 ${var_name} 格式的变量
                for var_name, var_value in self.variables.items():
                    placeholder = f"${{{var_name}}}"
                    if placeholder in value:
                        value = value.replace(placeholder, str(var_value))
                result[key] = value
            elif isinstance(value, dict):
                result[key] = self._replace_variables(value)
            elif isinstance(value, list):
                result[key] = [self._replace_variables({i: v})[i] if isinstance(v, (dict, str)) else v for v in value]
            else:
                result[key] = value
        return result

    def _execute_action(self, action: MacroAction, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行单个动作"""
        try:
            # 这里应该调用实际的自动化执行模块
            # 为了模块独立性，返回执行信息
            return {
                "action_id": action.action_id,
                "action_type": action.action_type.value if isinstance(action.action_type, MacroActionType) else action.action_type,
                "params": params,
                "result": "executed",
                "timestamp": time.time()
            }
        except Exception as e:
            return {
                "action_id": action.action_id,
                "result": "error",
                "error": str(e)
            }

    def pause(self):
        """暂停播放"""
        with self._lock:
            self.paused = True

    def resume(self):
        """继续播放"""
        with self._lock:
            self.paused = False

    def stop(self):
        """停止播放"""
        with self._lock:
            self.stopped = True
            self.playing = False

    def skip_to_action(self, action_index: int):
        """跳转到指定动作"""
        with self._lock:
            if 0 <= action_index < len(self.macro.actions):
                self.current_action_index = action_index


class MacroEditor:
    """宏编辑器"""

    def __init__(self, macro: Macro):
        self.macro = macro

    def add_action(self, action: MacroAction, index: Optional[int] = None):
        """添加动作"""
        if index is None or index >= len(self.macro.actions):
            self.macro.actions.append(action)
        else:
            self.macro.actions.insert(index, action)
        self.macro.updated_at = time.time()

    def remove_action(self, action_id: str) -> bool:
        """删除动作"""
        for i, action in enumerate(self.macro.actions):
            if action.action_id == action_id:
                del self.macro.actions[i]
                self.macro.updated_at = time.time()
                return True
        return False

    def update_action(self, action_id: str, updates: Dict[str, Any]) -> bool:
        """更新动作"""
        for action in self.macro.actions:
            if action.action_id == action_id:
                for key, value in updates.items():
                    if hasattr(action, key):
                        setattr(action, key, value)
                self.macro.updated_at = time.time()
                return True
        return False

    def insert_action(self, action: MacroAction, after_action_id: str):
        """在指定动作后插入"""
        for i, action_obj in enumerate(self.macro.actions):
            if action_obj.action_id == after_action_id:
                self.macro.actions.insert(i + 1, action)
                self.macro.updated_at = time.time()
                return True
        return False

    def adjust_action_timing(self, action_id: str, delay_after: Optional[float] = None, delay_before: Optional[float] = None):
        """调整动作时间"""
        for action in self.macro.actions:
            if action.action_id == action_id:
                if delay_after is not None:
                    action.delay_after = delay_after
                if delay_before is not None:
                    action.delay_before = delay_before
                self.macro.updated_at = time.time()
                return True
        return False

    def adjust_all_timings(self, multiplier: float):
        """批量调整所有动作的时间"""
        for action in self.macro.actions:
            action.delay_after *= multiplier
            action.delay_before *= multiplier
        self.macro.updated_at = time.time()

    def delete_actions_between(self, start_action_id: str, end_action_id: str) -> List[str]:
        """删除区间内的动作"""
        deleted_ids = []
        start_idx = end_idx = None
        for i, action in enumerate(self.macro.actions):
            if action.action_id == start_action_id:
                start_idx = i
            if action.action_id == end_action_id:
                end_idx = i

        if start_idx is not None and end_idx is not None:
            if start_idx <= end_idx:
                for i in range(start_idx, end_idx + 1):
                    deleted_ids.append(self.macro.actions[i].action_id)
                del self.macro.actions[start_idx:end_idx + 1]
            else:
                for i in range(end_idx, start_idx + 1):
                    deleted_ids.append(self.macro.actions[i].action_id)
                del self.macro.actions[end_idx:start_idx + 1]
            self.macro.updated_at = time.time()
        return deleted_ids

    def move_action(self, action_id: str, new_index: int):
        """移动动作位置"""
        for i, action in enumerate(self.macro.actions):
            if action.action_id == action_id:
                action_obj = self.macro.actions.pop(i)
                self.macro.actions.insert(new_index, action_obj)
                self.macro.updated_at = time.time()
                return True
        return False

    def duplicate_action(self, action_id: str) -> Optional[MacroAction]:
        """复制动作"""
        for action in self.macro.actions:
            if action.action_id == action_id:
                new_action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=action.action_type,
                    params=action.params.copy(),
                    delay_after=action.delay_after,
                    delay_before=action.delay_before,
                    description=action.description + " (copy)",
                    enabled=action.enabled,
                    repeat_count=action.repeat_count,
                    repeat_interval=action.repeat_interval
                )
                self.add_action(new_action)
                return new_action
        return None

    def add_variable(self, variable: MacroVariable):
        """添加变量"""
        self.macro.variables.append(variable)

    def remove_variable(self, name: str) -> bool:
        """删除变量"""
        for i, var in enumerate(self.macro.variables):
            if var.name == name:
                del self.macro.variables[i]
                return True
        return False

    def update_variable(self, name: str, updates: Dict[str, Any]) -> bool:
        """更新变量"""
        for var in self.macro.variables:
            if var.name == name:
                for key, value in updates.items():
                    if hasattr(var, key):
                        setattr(var, key, value)
                return True
        return False

    def add_condition(self, condition: MacroCondition):
        """添加条件"""
        self.macro.conditions.append(condition)

    def remove_condition(self, condition_id: str) -> bool:
        """删除条件"""
        for i, cond in enumerate(self.macro.conditions):
            if cond.condition_id == condition_id:
                del self.macro.conditions[i]
                return True
        return False


class MacroLibrary:
    """宏库"""

    def __init__(self, library_path: Optional[str] = None):
        self.library_path = library_path or os.path.join(os.path.expanduser("~"), ".rabai", "macros")
        os.makedirs(self.library_path, exist_ok=True)
        self._macros: Dict[str, Macro] = {}
        self._load_index()

    def _load_index(self):
        """加载索引"""
        index_file = os.path.join(self.library_path, "index.json")
        if os.path.exists(index_file):
            try:
                with open(index_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # 懒加载：只记录ID和基本信息
                    for macro_data in data.get("macros", []):
                        self._macros[macro_data["macro_id"]] = None
            except Exception:
                pass

    def _save_index(self):
        """保存索引"""
        index_file = os.path.join(self.library_path, "index.json")
        macros_data = []
        for macro_id, macro in self._macros.items():
            if macro is not None:
                macros_data.append({
                    "macro_id": macro.macro_id,
                    "name": macro.name,
                    "category": macro.category,
                    "tags": macro.tags,
                    "updated_at": macro.updated_at
                })
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump({"macros": macros_data, "version": "22.0.0"}, f, indent=2)

    def _get_macro_path(self, macro_id: str) -> str:
        """获取宏文件路径"""
        return os.path.join(self.library_path, f"{macro_id}.json")

    def save_macro(self, macro: Macro) -> str:
        """保存宏"""
        macro.updated_at = time.time()
        file_path = self._get_macro_path(macro.macro_id)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(macro.to_dict(), f, indent=2, ensure_ascii=False)
        self._macros[macro.macro_id] = macro
        self._save_index()
        return macro.macro_id

    def load_macro(self, macro_id: str) -> Optional[Macro]:
        """加载宏"""
        if macro_id in self._macros and self._macros[macro_id] is not None:
            return self._macros[macro_id]

        file_path = self._get_macro_path(macro_id)
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    macro = Macro.from_dict(data)
                    self._macros[macro_id] = macro
                    return macro
            except Exception:
                return None
        return None

    def delete_macro(self, macro_id: str) -> bool:
        """删除宏"""
        file_path = self._get_macro_path(macro_id)
        if os.path.exists(file_path):
            os.remove(file_path)
            if macro_id in self._macros:
                del self._macros[macro_id]
            self._save_index()
            return True
        return False

    def list_macros(self, category: Optional[str] = None, tags: Optional[List[str]] = None) -> List[Macro]:
        """列出宏"""
        result = []
        for macro_id in self._macros:
            macro = self.load_macro(macro_id)
            if macro:
                if category and macro.category != category:
                    continue
                if tags and not any(tag in macro.tags for tag in tags):
                    continue
                result.append(macro)
        return sorted(result, key=lambda m: m.updated_at, reverse=True)

    def search_macros(self, query: str) -> List[Macro]:
        """搜索宏"""
        results = []
        query_lower = query.lower()
        for macro_id in self._macros:
            macro = self.load_macro(macro_id)
            if macro:
                if (query_lower in macro.name.lower() or
                    query_lower in macro.description.lower() or
                    any(query_lower in tag.lower() for tag in macro.tags)):
                    results.append(macro)
        return results

    def get_categories(self) -> List[str]:
        """获取所有分类"""
        categories = set()
        for macro_id in self._macros:
            macro = self.load_macro(macro_id)
            if macro and macro.category:
                categories.add(macro.category)
        return sorted(list(categories))

    def get_tags(self) -> List[str]:
        """获取所有标签"""
        tags = set()
        for macro_id in self._macros:
            macro = self.load_macro(macro_id)
            if macro:
                tags.update(macro.tags)
        return sorted(list(tags))

    def export_macro(self, macro_id: str, format: str, output_path: str) -> bool:
        """导出宏"""
        macro = self.load_macro(macro_id)
        if not macro:
            return False

        if format == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(macro.to_dict(), f, indent=2, ensure_ascii=False)
            return True
        elif format == "autohotkey":
            return self._export_autohotkey(macro, output_path)
        elif format == "applescript":
            return self._export_applescript(macro, output_path)
        return False

    def import_macro(self, source_path: str, source_format: str) -> Optional[Macro]:
        """导入宏"""
        if source_format == "json":
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    macro = Macro.from_dict(data)
                    macro.macro_id = str(uuid.uuid4())
                    self.save_macro(macro)
                    return macro
            except Exception:
                return None
        elif source_format == "autohotkey":
            return self._import_autohotkey(source_path)
        elif source_format == "autoit":
            return self._import_autoit(source_path)
        elif source_format == "selenium":
            return self._import_selenium_ide(source_path)
        return None

    def _export_autohotkey(self, macro: Macro, output_path: str) -> bool:
        """导出为AutoHotkey格式"""
        lines = ["; RabAI AutoClick Macro Export", f"; Macro: {macro.name}", f"; Generated: {datetime.now()}", ""]
        lines.append("#SingleInstance Force")
        lines.append("SetWorkingDir %A_ScriptDir%")
        lines.append("")
        lines.append("#IfWinActive")
        lines.append("")

        for action in macro.actions:
            if not action.enabled:
                continue
            lines.append(f"    ; {action.description}")
            if action.action_type == MacroActionType.MOUSE_CLICK:
                x = action.params.get("x", 0)
                y = action.params.get("y", 0)
                button = action.params.get("button", "left")
                if button == "right":
                    lines.append(f"    Click, Right, {x}, {y}")
                else:
                    lines.append(f"    Click, {x}, {y}")
            elif action.action_type == MacroActionType.KEYBOARD_TYPE:
                text = action.params.get("text", "").replace('"', '\\"')
                lines.append(f'    SendInput, "{text}"')
            elif action.action_type == MacroActionType.KEYBOARD_PRESS:
                key = action.params.get("key", "")
                lines.append(f"    SendInput, {{{key}}}")
            elif action.action_type == MacroActionType.DELAY:
                duration = action.params.get("duration", 1)
                lines.append(f"    Sleep, {int(duration * 1000)}")
            lines.append("")

        lines.append("#IfWinActive")
        lines.append("")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return True

    def _export_applescript(self, macro: Macro, output_path: str) -> bool:
        """导出为AppleScript格式"""
        lines = ["-- RabAI AutoClick Macro Export", f"-- Macro: {macro.name}", f"-- Generated: {datetime.now()}", ""]
        lines.append("tell application \"System Events\"")

        for action in macro.actions:
            if not action.enabled:
                continue
            lines.append(f"    -- {action.description}")
            if action.action_type == MacroActionType.MOUSE_CLICK:
                x = action.params.get("x", 0)
                y = action.params.get("y", 0)
                lines.append(f"    set theClipboard to \"{x},{y}\"")
                lines.append(f"    click at {{x: {x}, y: {y}}}")
            elif action.action_type == MacroActionType.KEYBOARD_TYPE:
                text = action.params.get("text", "").replace('"', '\\"')
                lines.append(f'    keystroke "{text}"')
            elif action.action_type == MacroActionType.KEYBOARD_PRESS:
                key = action.params.get("key", "")
                key_map = {"enter": "return", "escape": "escape", "tab": "tab"}
                mapped_key = key_map.get(key.lower(), key)
                lines.append(f"    key code {mapped_key}")
            elif action.action_type == MacroActionType.DELAY:
                duration = action.params.get("duration", 1)
                lines.append(f"    delay {duration}")
            lines.append("")

        lines.append("end tell")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return True

    def _import_autohotkey(self, source_path: str) -> Optional[Macro]:
        """从AutoHotkey导入"""
        try:
            with open(source_path, "r", encoding="utf-8") as f:
                content = f.read()

            macro = Macro(
                macro_id=str(uuid.uuid4()),
                name=os.path.splitext(os.path.basename(source_path))[0],
                description="Imported from AutoHotkey"
            )

            # 解析Click命令
            click_pattern = r'Click[,\s]*(?:(?:Right|Down|Up|Move|WheelDown|WheelUp)?[,\s]*)?(\d+)?[,\s]*(\d+)?'
            for match in re.finditer(click_pattern, content, re.IGNORECASE):
                x, y = match.groups()
                if x and y:
                    action = MacroAction(
                        action_id=str(uuid.uuid4()),
                        action_type=MacroActionType.MOUSE_CLICK,
                        params={"x": int(x), "y": int(y), "button": "left"},
                        description=f"Click at ({x}, {y})"
                    )
                    macro.actions.append(action)

            # 解析SendInput/Send命令
            send_pattern = r'Send(?:Input)?[,\s]*["\'](.+?)["\']'
            for match in re.finditer(send_pattern, content, re.IGNORECASE):
                text = match.group(1)
                text = text.replace("\\n", "\n").replace("\\t", "\t")
                action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=MacroActionType.KEYBOARD_TYPE,
                    params={"text": text},
                    description=f"Send: {text[:30]}..."
                )
                macro.actions.append(action)

            # 解析Sleep命令
            sleep_pattern = r'Sleep[,\s]*(\d+)'
            for match in re.finditer(sleep_pattern, content, re.IGNORECASE):
                duration = int(match.group(1)) / 1000
                action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=MacroActionType.DELAY,
                    params={"duration": duration},
                    description=f"Delay: {duration}s"
                )
                macro.actions.append(action)

            self.save_macro(macro)
            return macro

        except Exception:
            return None

    def _import_autoit(self, source_path: str) -> Optional[Macro]:
        """从AutoIt导入"""
        try:
            with open(source_path, "r", encoding="utf-8") as f:
                content = f.read()

            macro = Macro(
                macro_id=str(uuid.uuid4()),
                name=os.path.splitext(os.path.basename(source_path))[0],
                description="Imported from AutoIt"
            )

            # 解析MouseClick
            mouse_pattern = r'MouseClick\s*\(\s*["\'](\w+)["\']\s*,\s*(\d+)\s*,\s*(\d+)'
            for match in re.finditer(mouse_pattern, content, re.IGNORECASE):
                button, x, y = match.groups()
                action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=MacroActionType.MOUSE_CLICK,
                    params={"x": int(x), "y": int(y), "button": button.lower()},
                    description=f"MouseClick {button} at ({x}, {y})"
                )
                macro.actions.append(action)

            # 解析Send
            send_pattern = r'Send\s*\(\s*["\'](.+?)["\']'
            for match in re.finditer(send_pattern, content, re.IGNORECASE):
                text = match.group(1)
                action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=MacroActionType.KEYBOARD_TYPE,
                    params={"text": text},
                    description=f"Send: {text[:30]}..."
                )
                macro.actions.append(action)

            # 解析Sleep
            sleep_pattern = r'Sleep\s*\(\s*(\d+)'
            for match in re.finditer(sleep_pattern, content, re.IGNORECASE):
                duration = int(match.group(1)) / 1000
                action = MacroAction(
                    action_id=str(uuid.uuid4()),
                    action_type=MacroActionType.DELAY,
                    params={"duration": duration},
                    description=f"Delay: {duration}s"
                )
                macro.actions.append(action)

            self.save_macro(macro)
            return macro

        except Exception:
            return None

    def _import_selenium_ide(self, source_path: str) -> Optional[Macro]:
        """从Selenium IDE导入"""
        try:
            with open(source_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            macro = Macro(
                macro_id=str(uuid.uuid4()),
                name=data.get("name", os.path.splitext(os.path.basename(source_path))[0]),
                description="Imported from Selenium IDE"
            )

            for command in data.get("commands", []):
                cmd = command.get("command", "").lower()
                target = command.get("target", "")
                value = command.get("value", "")

                if cmd in ["click", "clickAndWait"]:
                    action = MacroAction(
                        action_id=str(uuid.uuid4()),
                        action_type=MacroActionType.MOUSE_CLICK,
                        params={"target": target, "value": value},
                        description=f"Click: {target}"
                    )
                    macro.actions.append(action)
                elif cmd in ["type", "sendKeys", "typeAndWait"]:
                    action = MacroAction(
                        action_id=str(uuid.uuid4()),
                        action_type=MacroActionType.KEYBOARD_TYPE,
                        params={"target": target, "text": value},
                        description=f"Type: {value[:30]}..."
                    )
                    macro.actions.append(action)
                elif cmd == "pause":
                    try:
                        duration = float(value) / 1000
                    except ValueError:
                        duration = 1
                    action = MacroAction(
                        action_id=str(uuid.uuid4()),
                        action_type=MacroActionType.DELAY,
                        params={"duration": duration},
                        description=f"Pause: {duration}s"
                    )
                    macro.actions.append(action)

            self.save_macro(macro)
            return macro

        except Exception:
            return None


class MacroScheduler:
    """宏调度器"""

    def __init__(self, macro_library: MacroLibrary):
        self.macro_library = macro_library
        self.scheduled_tasks: Dict[str, Dict] = {}
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self):
        """启动调度器"""
        with self._lock:
            if not self._running:
                self._running = True
                self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
                self._scheduler_thread.start()

    def stop(self):
        """停止调度器"""
        with self._lock:
            self._running = False
            if self._scheduler_thread:
                self._scheduler_thread.join(timeout=5)

    def _scheduler_loop(self):
        """调度循环"""
        while self._running:
            now = time.time()
            with self._lock:
                for task_id, task in list(self.scheduled_tasks.items()):
                    if not task.get("enabled", True):
                        continue

                    next_run = task.get("next_run_time")
                    if next_run and now >= next_run:
                        self._execute_scheduled_task(task_id, task)
                        self._advance_next_run(task_id, task)

            time.sleep(1)

    def _execute_scheduled_task(self, task_id: str, task: Dict):
        """执行调度任务"""
        macro_id = task.get("macro_id")
        macro = self.macro_library.load_macro(macro_id)
        if not macro:
            return

        player = MacroPlayer(macro)
        result, _ = player.play()
        task["last_run_time"] = time.time()
        task["last_result"] = result.value

    def _advance_next_run(self, task_id: str, task: Dict):
        """推进下次运行时间"""
        schedule_type = task.get("schedule_type")
        schedule_config = task.get("schedule_config", {})

        if schedule_type == "cron":
            cron_expr = schedule_config.get("cron")
            if cron_expr:
                try:
                    cron = croniter(cron_expr, datetime.now())
                    task["next_run_time"] = cron.get_next()
                except Exception:
                    pass
        elif schedule_type == "interval":
            interval = schedule_config.get("interval_seconds", 60)
            task["next_run_time"] = time.time() + interval
        elif schedule_type == "one_time":
            run_time = schedule_config.get("run_time")
            if run_time:
                task["next_run_time"] = run_time
                task["enabled"] = False

    def schedule_macro(self, macro_id: str, schedule_type: str, schedule_config: Dict, task_name: Optional[str] = None) -> str:
        """调度宏"""
        task_id = str(uuid.uuid4())
        task = {
            "task_id": task_id,
            "macro_id": macro_id,
            "task_name": task_name or f"Task-{task_id[:8]}",
            "schedule_type": schedule_type,
            "schedule_config": schedule_config,
            "enabled": True,
            "next_run_time": None,
            "last_run_time": None,
            "last_result": None,
            "created_at": time.time()
        }
        self._advance_next_run(task_id, task)
        self.scheduled_tasks[task_id] = task
        return task_id

    def unschedule_task(self, task_id: str) -> bool:
        """取消调度"""
        if task_id in self.scheduled_tasks:
            del self.scheduled_tasks[task_id]
            return True
        return False

    def enable_task(self, task_id: str) -> bool:
        """启用任务"""
        if task_id in self.scheduled_tasks:
            self.scheduled_tasks[task_id]["enabled"] = True
            return True
        return False

    def disable_task(self, task_id: str) -> bool:
        """禁用任务"""
        if task_id in self.scheduled_tasks:
            self.scheduled_tasks[task_id]["enabled"] = False
            return True
        return False

    def list_scheduled_tasks(self) -> List[Dict]:
        """列出所有调度任务"""
        return list(self.scheduled_tasks.values())


class MacroToWorkflowConverter:
    """宏转工作流转换器"""

    @staticmethod
    def convert(macro: Macro) -> Dict[str, Any]:
        """将宏转换为工作流格式"""
        workflow = {
            "workflow_id": str(uuid.uuid4()),
            "name": f"Converted: {macro.name}",
            "description": macro.description,
            "version": "22.0.0",
            "steps": [],
            "triggers": [],
            "settings": macro.settings.copy(),
            "created_at": time.time(),
            "updated_at": time.time(),
            "metadata": {
                "source": "macro",
                "source_macro_id": macro.macro_id,
                **macro.metadata
            }
        }

        action_type_to_action = {
            MacroActionType.MOUSE_CLICK: "click",
            MacroActionType.MOUSE_DOUBLE_CLICK: "double_click",
            MacroActionType.MOUSE_RIGHT_CLICK: "right_click",
            MacroActionType.MOUSE_MOVE: "move_mouse",
            MacroActionType.MOUSE_DRAG: "drag_mouse",
            MacroActionType.KEYBOARD_PRESS: "key_press",
            MacroActionType.KEYBOARD_TYPE: "type_text",
            MacroActionType.KEYBOARD_COMBINATION: "key_combo",
            MacroActionType.DELAY: "wait",
            MacroActionType.SCREENSHOT: "screenshot",
            MacroActionType.WAIT_FOR_IMAGE: "wait_for_image",
            MacroActionType.WAIT_FOR_WINDOW: "wait_for_window",
            MacroActionType.CUSTOM: "custom_action"
        }

        for action in macro.actions:
            if not action.enabled:
                continue

            action_str = action.action_type
            if isinstance(action_str, MacroActionType):
                action_str = action_str.value

            workflow_action = action_type_to_action.get(action.action_type, "custom_action")

            step = {
                "step_id": str(uuid.uuid4()),
                "action": workflow_action,
                "params": action.params.copy(),
                "timeout": 30.0,
                "retry": 0
            }

            # 添加延迟参数
            if action.delay_after > 0:
                step["params"]["post_delay"] = action.delay_after
            if action.delay_before > 0:
                step["params"]["pre_delay"] = action.delay_before

            # 添加描述
            if action.description:
                step["description"] = action.description

            workflow["steps"].append(step)

        # 添加前置条件
        for condition in macro.conditions:
            condition_dict = {
                "condition_id": condition.condition_id,
                "condition_type": condition.condition_type.value if isinstance(condition.condition_type, MacroConditionType) else condition.condition_type,
                "params": condition.params,
                "negate": condition.negate
            }
            workflow["steps"].insert(0, {
                "step_id": str(uuid.uuid4()),
                "action": "check_condition",
                "params": {"condition": condition_dict},
                "timeout": 10.0
            })

        return workflow

    @staticmethod
    def convert_to_yaml(macro: Macro) -> str:
        """转换为YAML格式"""
        import yaml
        workflow = MacroToWorkflowConverter.convert(macro)
        return yaml.dump(workflow, allow_unicode=True, default_flow_style=False)


class WorkflowMacro:
    """宏系统主类"""

    def __init__(self, library_path: Optional[str] = None):
        self.library = MacroLibrary(library_path)
        self.scheduler = MacroScheduler(self.library)
        self._recorder: Optional[MacroRecorder] = None

    def create_macro(self, name: str, description: str = "") -> Macro:
        """创建新宏"""
        macro = Macro(
            macro_id=str(uuid.uuid4()),
            name=name,
            description=description
        )
        self.library.save_macro(macro)
        return macro

    def get_recorder(self) -> MacroRecorder:
        """获取录制器"""
        if self._recorder is None:
            self._recorder = MacroRecorder()
        return self._recorder

    def record_new_macro(self, name: str, description: str = "") -> Tuple[Macro, MacroRecorder]:
        """创建并开始录制新宏"""
        macro = self.create_macro(name, description)
        recorder = self.get_recorder()
        recorder.start_recording()
        return macro, recorder

    def stop_and_save_recording(self, macro: Macro) -> Macro:
        """停止录制并保存"""
        recorder = self.get_recorder()
        actions = recorder.stop_recording()
        macro.actions = actions
        self.library.save_macro(macro)
        return macro

    def play_macro(self, macro_id: str, variables: Optional[Dict[str, Any]] = None) -> Tuple[PlaybackResult, List[Dict]]:
        """播放宏"""
        macro = self.library.load_macro(macro_id)
        if not macro:
            return PlaybackResult.ERROR, [{"error": "Macro not found"}]

        macro.usage_count += 1
        macro.last_used_at = time.time()
        self.library.save_macro(macro)

        player = MacroPlayer(macro)
        if variables:
            for name, value in variables.items():
                player.set_variable(name, value)
        return player.play()

    def edit_macro(self, macro_id: str) -> Optional[Tuple[Macro, MacroEditor]]:
        """获取宏编辑器"""
        macro = self.library.load_macro(macro_id)
        if macro:
            return macro, MacroEditor(macro)
        return None

    def delete_macro(self, macro_id: str) -> bool:
        """删除宏"""
        return self.library.delete_macro(macro_id)

    def list_macros(self, category: Optional[str] = None, tags: Optional[List[str]] = None) -> List[Macro]:
        """列出宏"""
        return self.library.list_macros(category, tags)

    def search_macros(self, query: str) -> List[Macro]:
        """搜索宏"""
        return self.library.search_macros(query)

    def convert_to_workflow(self, macro_id: str) -> Optional[Dict[str, Any]]:
        """转换为工作流"""
        macro = self.library.load_macro(macro_id)
        if macro:
            return MacroToWorkflowConverter.convert(macro)
        return None

    def import_macro(self, source_path: str, source_format: str) -> Optional[Macro]:
        """导入宏"""
        return self.library.import_macro(source_path, source_format)

    def export_macro(self, macro_id: str, output_path: str, format: str = "json") -> bool:
        """导出宏"""
        return self.library.export_macro(macro_id, format, output_path)

    def schedule_macro(self, macro_id: str, schedule_type: str, schedule_config: Dict, task_name: Optional[str] = None) -> str:
        """调度宏"""
        return self.scheduler.schedule_macro(macro_id, schedule_type, schedule_config, task_name)

    def start_scheduler(self):
        """启动调度器"""
        self.scheduler.start()

    def stop_scheduler(self):
        """停止调度器"""
        self.scheduler.stop()


def create_macro_system(library_path: Optional[str] = None) -> WorkflowMacro:
    """创建宏系统实例"""
    return WorkflowMacro(library_path)
