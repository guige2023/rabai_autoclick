"""
工作流领域特定语言 (DSL) v23
P0级功能 - Python DSL定义工作流，支持链式调用、类型安全构建器、装饰器语法、IDE提示、YAML/JSON编译
"""
import json
import yaml
import re
import inspect
from typing import Dict, List, Optional, Any, Callable, TypeVar, Generic, Union, get_type_hints
from dataclasses import dataclass, field, is_dataclass
from enum import Enum
from functools import wraps
from collections import defaultdict


# ========== 类型定义 ==========

class ActionType(Enum):
    """动作类型"""
    CLICK = "click"
    TYPE = "type"
    PRESS = "press"
    WAIT = "wait"
    SCREENSHOT = "screenshot"
    IF = "if"
    WHILE = "while"
    FOR_EACH = "for_each"
    TRY = "try"
    CALL = "call"
    RETURN = "return"
    COMMENT = "comment"
    GROUP = "group"


class VariableType(Enum):
    """变量类型"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    DICT = "dict"
    ANY = "any"


@dataclass
class ActionRef:
    """动作引用"""
    name: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class ConditionRef:
    """条件引用"""
    lambda_ref: Callable[[], bool]
    source: str = ""


@dataclass
class WorkflowStep:
    """工作流步骤"""
    id: str
    action: ActionType
    params: Dict[str, Any] = field(default_factory=dict)
    conditions: List[ConditionRef] = field(default_factory=list)
    next_on_success: Optional[str] = None
    next_on_failure: Optional[str] = None
    retry_count: int = 0
    timeout: Optional[float] = None


@dataclass
class WorkflowDef:
    """工作流定义"""
    name: str
    description: str = ""
    version: str = "1.0.0"
    steps: List[WorkflowStep] = field(default_factory=list)
    variables: Dict[str, VariableType] = field(default_factory=dict)
    triggers: List[Dict[str, Any]] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TemplateDef:
    """工作流模板"""
    name: str
    description: str
    parameters: Dict[str, VariableType]
    body: str  # DSL源代码
    category: str = "general"


# ========== 类型安全构建器 ==========

T = TypeVar('T')

class Builder(Generic[T]):
    """基础构建器"""
    
    def __init__(self):
        self._errors: List[str] = []
    
    def validate(self) -> 'Builder[T]':
        """验证构建器状态"""
        if self._errors:
            raise ValueError(f"Validation failed: {'; '.join(self._errors)}")
        return self
    
    def errors(self) -> List[str]:
        return self._errors


class ClickBuilder(Builder['ClickBuilder']):
    """点击动作构建器"""
    
    def __init__(self):
        super().__init__()
        self._x: Optional[int] = None
        self._y: Optional[int] = None
        self._button: str = "left"
        self._double: bool = False
        self._element: Optional[str] = None
    
    def at(self, x: int, y: int) -> 'ClickBuilder':
        """设置点击坐标"""
        if x < 0 or y < 0:
            self._errors.append(f"Invalid coordinates: ({x}, {y})")
        self._x = x
        self._y = y
        return self
    
    def element(self, selector: str) -> 'ClickBuilder':
        """通过选择器指定元素"""
        self._element = selector
        return self
    
    def button(self, btn: str) -> 'ClickBuilder':
        """设置鼠标按钮"""
        if btn not in ("left", "right", "middle"):
            self._errors.append(f"Invalid button: {btn}")
        self._button = btn
        return self
    
    def double(self) -> 'ClickBuilder':
        """双击"""
        self._double = True
        return self
    
    def build(self) -> Dict[str, Any]:
        """构建动作参数"""
        self.validate()
        result = {"action": "click", "button": self._button, "double": self._double}
        if self._x is not None and self._y is not None:
            result["x"] = self._x
            result["y"] = self._y
        if self._element:
            result["element"] = self._element
        return result


class TypeBuilder(Builder['TypeBuilder']):
    """输入动作构建器"""
    
    def __init__(self):
        super().__init__()
        self._text: str = ""
        self._delay: float = 0.05
        self._clear: bool = False
    
    def text(self, value: str) -> 'TypeBuilder':
        """设置输入文本"""
        self._text = str(value)
        return self
    
    def clear(self) -> 'TypeBuilder':
        """清除现有文本"""
        self._clear = True
        return self
    
    def delay(self, seconds: float) -> 'TypeBuilder':
        """设置按键延迟"""
        if seconds < 0:
            self._errors.append("Delay cannot be negative")
        self._delay = seconds
        return self
    
    def build(self) -> Dict[str, Any]:
        self.validate()
        return {
            "action": "type",
            "text": self._text,
            "clear": self._clear,
            "delay": self._delay
        }


class PressBuilder(Builder['PressBuilder']):
    """按键动作构建器"""
    
    def __init__(self):
        super().__init__()
        self._keys: List[str] = []
    
    def key(self, key: str) -> 'PressBuilder':
        """添加按键"""
        self._keys.append(key)
        return self
    
    def keys(self, *keys: str) -> 'PressBuilder':
        """批量添加按键"""
        self._keys.extend(keys)
        return self
    
    def build(self) -> Dict[str, Any]:
        self.validate()
        return {"action": "press", "keys": self._keys}


class WaitBuilder(Builder['WaitBuilder']):
    """等待动作构建器"""
    
    def __init__(self):
        super().__init__()
        self._seconds: float = 1.0
        self._for_element: Optional[str] = None
    
    def seconds(self, secs: float) -> 'WaitBuilder':
        """设置等待秒数"""
        if secs < 0:
            self._errors.append("Wait time cannot be negative")
        self._seconds = secs
        return self
    
    def for_element(self, selector: str, timeout: float = 10.0) -> 'WaitBuilder':
        """等待元素出现"""
        self._for_element = selector
        self._timeout = timeout
        return self
    
    def build(self) -> Dict[str, Any]:
        self.validate()
        result: Dict[str, Any] = {"action": "wait", "seconds": self._seconds}
        if self._for_element:
            result["for_element"] = self._for_element
            result["timeout"] = getattr(self, '_timeout', 10.0)
        return result


class IfBuilder(Builder['IfBuilder']):
    """条件动作构建器"""
    
    def __init__(self, condition: Callable[[], bool]):
        super().__init__()
        self._condition = condition
        self._condition_source: str = ""
        self._then_actions: List[Dict[str, Any]] = []
        self._else_actions: List[Dict[str, Any]] = []
    
    def condition(self, cond: Callable[[], bool]) -> 'IfBuilder':
        self._condition = cond
        return self
    
    def then(self, *actions: Dict[str, Any]) -> 'IfBuilder':
        self._then_actions.extend(actions)
        return self
    
    def else_(self, *actions: Dict[str, Any]) -> 'IfBuilder':
        self._else_actions.extend(actions)
        return self
    
    def build(self) -> Dict[str, Any]:
        self.validate()
        return {
            "action": "if",
            "condition": self._condition_source or "<lambda>",
            "then": self._then_actions,
            "else": self._else_actions
        }


class WhileBuilder(Builder['WhileBuilder']):
    """循环动作构建器"""
    
    def __init__(self, condition: Callable[[], bool]):
        super().__init__()
        self._condition = condition
        self._condition_source: str = ""
        self._max_iterations: int = 1000
        self._body_actions: List[Dict[str, Any]] = []
    
    def condition(self, cond: Callable[[], bool]) -> 'WhileBuilder':
        self._condition = cond
        return self
    
    def do(self, *actions: Dict[str, Any]) -> 'WhileBuilder':
        self._body_actions.extend(actions)
        return self
    
    def max_iterations(self, n: int) -> 'WhileBuilder':
        self._max_iterations = n
        return self
    
    def build(self) -> Dict[str, Any]:
        self.validate()
        return {
            "action": "while",
            "condition": self._condition_source or "<lambda>",
            "max_iterations": self._max_iterations,
            "body": self._body_actions
        }


# ========== 工作流DSL主类 ==========

class WorkflowDSL:
    """
    工作流领域特定语言
    
    提供流畅的API来定义工作流，支持:
    - 链式方法调用
    - 类型安全构建器
    - Lambda条件
    - 装饰器语法
    - IDE自动完成提示
    """
    
    _registry: Dict[str, 'WorkflowDef'] = {}
    _templates: Dict[str, TemplateDef] = {}
    _type_cache: Dict[str, VariableType] = {}
    
    def __init__(self, name: str = ""):
        self._name = name
        self._description = ""
        self._version = "1.0.0"
        self._steps: List[WorkflowStep] = []
        self._variables: Dict[str, VariableType] = {}
        self._current_step_id = 0
        self._variable_usage: Dict[str, List[Any]] = defaultdict(list)
    
    # ===== 流畅API - 链式调用 =====
    
    def named(self, name: str) -> 'WorkflowDSL':
        """设置工作流名称"""
        self._name = name
        return self
    
    def described(self, desc: str) -> 'WorkflowDSL':
        """设置工作流描述"""
        self._description = desc
        return self
    
    def version(self, ver: str) -> 'WorkflowDSL':
        """设置版本"""
        self._version = ver
        return self
    
    def with_variable(self, name: str, vtype: VariableType = VariableType.ANY) -> 'WorkflowDSL':
        """声明变量"""
        self._variables[name] = vtype
        return self
    
    def step(self, action: ActionType, **params) -> 'WorkflowDSL':
        """添加步骤"""
        step = WorkflowStep(
            id=f"step_{self._current_step_id}",
            action=action,
            params=params
        )
        self._steps.append(step)
        self._current_step_id += 1
        return self
    
    def click(self, x: int, y: int, button: str = "left", double: bool = False) -> 'WorkflowDSL':
        """添加点击步骤"""
        return self.step(ActionType.CLICK, x=x, y=y, button=button, double=double)
    
    def type(self, text: str, clear: bool = False, delay: float = 0.05) -> 'WorkflowDSL':
        """添加输入步骤"""
        self._track_variable_usage(text)
        return self.step(ActionType.TYPE, text=text, clear=clear, delay=delay)
    
    def press(self, *keys: str) -> 'WorkflowDSL':
        """添加按键步骤"""
        return self.step(ActionType.PRESS, keys=list(keys))
    
    def wait(self, seconds: float = 1.0) -> 'WorkflowDSL':
        """添加等待步骤"""
        return self.step(ActionType.WAIT, seconds=seconds)
    
    def screenshot(self, path: str = "screenshot.png") -> 'WorkflowDSL':
        """添加截图步骤"""
        return self.step(ActionType.SCREENSHOT, path=path)
    
    def if_step(self, condition: Callable[[], bool], source: str = "") -> 'IfStepBuilder':
        """添加条件步骤"""
        return IfStepBuilder(self, condition, source)
    
    def while_step(self, condition: Callable[[], bool], source: str = "") -> 'WhileStepBuilder':
        """添加循环步骤"""
        return WhileStepBuilder(self, condition, source)
    
    def try_step(self) -> 'TryStepBuilder':
        """添加try块"""
        return TryStepBuilder(self)
    
    def comment(self, text: str) -> 'WorkflowDSL':
        """添加注释"""
        return self.step(ActionType.COMMENT, text=text)
    
    def group(self, name: str) -> 'GroupStepBuilder':
        """添加步骤组"""
        return GroupStepBuilder(self, name)
    
    def on_success(self, next_step_id: str) -> 'WorkflowDSL':
        """设置成功时的下一步"""
        if self._steps:
            self._steps[-1].next_on_success = next_step_id
        return self
    
    def on_failure(self, next_step_id: str) -> 'WorkflowDSL':
        """设置失败时的下一步"""
        if self._steps:
            self._steps[-1].next_on_failure = next_step_id
        return self
    
    def retry(self, count: int) -> 'WorkflowDSL':
        """设置重试次数"""
        if self._steps:
            self._steps[-1].retry_count = count
        return self
    
    def timeout(self, seconds: float) -> 'WorkflowDSL':
        """设置超时时间"""
        if self._steps:
            self._steps[-1].timeout = seconds
        return self
    
    def build(self) -> WorkflowDef:
        """构建工作流定义"""
        self._infer_variable_types()
        return WorkflowDef(
            name=self._name,
            description=self._description,
            version=self._version,
            steps=self._steps,
            variables=self._variables,
            triggers=[],
            settings={}
        )
    
    def to_json(self) -> str:
        """导出为JSON"""
        wf = self.build()
        return json.dumps(asdict(wf), indent=2, ensure_ascii=False)
    
    def to_yaml(self) -> str:
        """导出为YAML"""
        wf = self.build()
        return yaml.dump(asdict(wf), allow_unicode=True, default_flow_style=False)
    
    # ===== 变量类型推断 =====
    
    def _track_variable_usage(self, value: Any) -> None:
        """跟踪变量使用情况"""
        if isinstance(value, str) and '{{' in value:
            matches = re.findall(r'\{\{(\w+)\}\}', value)
            for var in matches:
                self._variable_usage[var].append(value)
    
    def _infer_variable_types(self) -> None:
        """从使用情况推断变量类型"""
        for var_name, usages in self._variable_usage.items():
            if var_name not in self._variables or self._variables[var_name] == VariableType.ANY:
                inferred = self._infer_type_from_usage(usages)
                self._variables[var_name] = inferred
    
    def _infer_type_from_usage(self, usages: List[Any]) -> VariableType:
        """从使用情况推断类型"""
        if not usages:
            return VariableType.ANY
        
        sample = usages[0]
        if isinstance(sample, bool):
            return VariableType.BOOLEAN
        elif isinstance(sample, int):
            return VariableType.INTEGER
        elif isinstance(sample, float):
            return VariableType.FLOAT
        elif isinstance(sample, str):
            return VariableType.STRING
        elif isinstance(sample, list):
            return VariableType.LIST
        elif isinstance(sample, dict):
            return VariableType.DICT
        return VariableType.ANY
    
    # ===== 导入/导出 =====
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WorkflowDSL':
        """从JSON导入"""
        data = json.loads(json_str)
        return cls._from_dict(data)
    
    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'WorkflowDSL':
        """从YAML导入"""
        data = yaml.safe_load(yaml_str)
        return cls._from_dict(data)
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> 'WorkflowDSL':
        """从字典导入"""
        wf = cls(data.get('name', ''))
        wf._description = data.get('description', '')
        wf._version = data.get('version', '1.0.0')
        wf._variables = {k: VariableType(v) for k, v in data.get('variables', {}).items()}
        
        for step_data in data.get('steps', []):
            action = ActionType(step_data['action'])
            step = WorkflowStep(
                id=step_data.get('id', ''),
                action=action,
                params=step_data.get('params', {})
            )
            wf._steps.append(step)
        
        return wf
    
    def to_dsl(self) -> str:
        """导出为DSL语法"""
        lines = [f"# Workflow: {self._name}", f"# Version: {self._version}", ""]
        
        if self._description:
            lines.append(f'"""{self._description}"""')
            lines.append("")
        
        if self._variables:
            lines.append("# Variables")
            for name, vtype in self._variables.items():
                lines.append(f"var {name}: {vtype.value}")
            lines.append("")
        
        lines.append(f"workflow {self._name}() {{")
        
        for step in self._steps:
            lines.append(f"  {step.action.value}({self._format_params(step.params)})")
        
        lines.append("}")
        return "\n".join(lines)
    
    def _format_params(self, params: Dict[str, Any]) -> str:
        """格式化参数"""
        parts = []
        for k, v in params.items():
            if isinstance(v, str):
                parts.append(f'{k}="{v}"')
            elif isinstance(v, list):
                parts.append(f"{k}={v}")
            else:
                parts.append(f"{k}={v}")
        return ", ".join(parts)
    
    @classmethod
    def parse_dsl(cls, dsl_code: str) -> 'WorkflowDSL':
        """解析DSL代码"""
        lines = dsl_code.strip().split('\n')
        wf = cls()
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('#') or line.startswith('"""'):
                i += 1
                continue
            
            if line.startswith('var '):
                # 变量声明
                match = re.match(r'var (\w+): (\w+)', line)
                if match:
                    var_name, var_type = match.groups()
                    wf._variables[var_name] = VariableType(var_type)
            
            elif line.startswith('workflow '):
                # 工作流定义开始
                match = re.match(r'workflow (\w+)\(\) \{', line)
                if match:
                    wf._name = match.group(1)
            
            elif '(' in line and ')' in line:
                # 动作调用
                match = re.match(r'(\w+)\((.*)\)', line)
                if match:
                    action_name, params_str = match.groups()
                    try:
                        action = ActionType(action_name)
                        params = cls._parse_params(params_str)
                        wf.step(action, **params)
                    except ValueError:
                        pass  # 未知动作
            
            elif line == '}':
                # 工作流定义结束
                break
            
            i += 1
        
        return wf
    
    @classmethod
    def _parse_params(cls, params_str: str) -> Dict[str, Any]:
        """解析参数字符串"""
        params = {}
        # 简单的参数解析（实际实现可能更复杂）
        matches = re.findall(r'(\w+)="([^"]*)"', params_str)
        for name, value in matches:
            params[name] = value
        matches = re.findall(r'(\w+)=(\[.*?\])', params_str)
        for name, value in matches:
            try:
                params[name] = json.loads(value)
            except:
                params[name] = value
        return params
    
    # ===== 注册与模板 =====
    
    @classmethod
    def register(cls, name: str, workflow: 'WorkflowDSL') -> None:
        """注册工作流"""
        cls._registry[name] = workflow.build()
    
    @classmethod
    def get(cls, name: str) -> Optional['WorkflowDef']:
        """获取工作流"""
        return cls._registry.get(name)
    
    @classmethod
    def create_template(cls, name: str, description: str, parameters: Dict[str, VariableType]) -> 'TemplateBuilder':
        """创建模板"""
        return TemplateBuilder(name, description, parameters)
    
    @classmethod
    def register_template(cls, template: TemplateDef) -> None:
        """注册模板"""
        cls._templates[template.name] = template
    
    @classmethod
    def instantiate_template(cls, template_name: str, **params) -> 'WorkflowDSL':
        """实例化模板"""
        template = cls._templates.get(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")
        
        wf = cls.parse_dsl(template.body)
        
        for param_name, param_value in params.items():
            wf._variables[param_name] = template.parameters.get(param_name, VariableType.ANY)
            wf._replace_parameters(param_name, param_value)
        
        return wf
    
    def _replace_parameters(self, name: str, value: Any) -> None:
        """替换参数"""
        for step in self._steps:
            for key, val in step.params.items():
                if isinstance(val, str) and f'{{{name}}}' in val:
                    step.params[key] = val.replace(f'{{{name}}}', str(value))
    
    # ===== IDE支持提示 =====
    
    @classmethod
    def get_autocomplete_hints(cls) -> Dict[str, List[str]]:
        """获取自动完成提示"""
        return {
            "actions": [a.value for a in ActionType],
            "variable_types": [v.value for v in VariableType],
            "methods": [
                "named(name)", "described(desc)", "version(ver)",
                "with_variable(name, type)", "step(action, **params)",
                "click(x, y)", "type(text)", "press(*keys)", "wait(seconds)",
                "if_step(condition)", "while_step(condition)",
                "try_step()", "comment(text)", "group(name)",
                "on_success(step_id)", "on_failure(step_id)",
                "retry(count)", "timeout(seconds)",
                "build()", "to_json()", "to_yaml()", "to_dsl()"
            ],
            "builders": [
                "ClickBuilder", "TypeBuilder", "PressBuilder", 
                "WaitBuilder", "IfBuilder", "WhileBuilder"
            ]
        }
    
    @classmethod
    def get_type_hints(cls) -> Dict[str, str]:
        """获取类型提示"""
        return {
            "WorkflowDSL": "WorkflowDSL",
            "WorkflowDef": "WorkflowDef", 
            "WorkflowStep": "WorkflowStep",
            "ActionType": "ActionType",
            "VariableType": "VariableType",
            "TemplateDef": "TemplateDef"
        }
    
    @classmethod
    def generate_stub_file(cls) -> str:
        """生成类型存根文件"""
        return '''"""
WorkflowDSL Type Stubs
自动生成用于IDE支持的类型提示
"""
from typing import Callable, Dict, List, Optional, Any

class WorkflowDSL:
    def __init__(self, name: str = "") -> None: ...
    def named(self, name: str) -> "WorkflowDSL": ...
    def described(self, desc: str) -> "WorkflowDSL": ...
    def version(self, ver: str) -> "WorkflowDSL": ...
    def with_variable(self, name: str, vtype: VariableType = ...) -> "WorkflowDSL": ...
    def step(self, action: ActionType, **params: Any) -> "WorkflowDSL": ...
    def click(self, x: int, y: int, button: str = "left", double: bool = False) -> "WorkflowDSL": ...
    def type(self, text: str, clear: bool = False, delay: float = 0.05) -> "WorkflowDSL": ...
    def press(self, *keys: str) -> "WorkflowDSL": ...
    def wait(self, seconds: float = 1.0) -> "WorkflowDSL": ...
    def screenshot(self, path: str = "screenshot.png") -> "WorkflowDSL": ...
    def if_step(self, condition: Callable[[], bool], source: str = "") -> "IfStepBuilder": ...
    def while_step(self, condition: Callable[[], bool], source: str = "") -> "WhileStepBuilder": ...
    def try_step(self) -> "TryStepBuilder": ...
    def comment(self, text: str) -> "WorkflowDSL": ...
    def group(self, name: str) -> "GroupStepBuilder": ...
    def build(self) -> WorkflowDef: ...
    def to_json(self) -> str: ...
    def to_yaml(self) -> str: ...
    def to_dsl(self) -> str: ...
    @classmethod
    def from_json(cls, json_str: str) -> "WorkflowDSL": ...
    @classmethod
    def from_yaml(cls, yaml_str: str) -> "WorkflowDSL": ...
    @classmethod
    def parse_dsl(cls, dsl_code: str) -> "WorkflowDSL": ...
    @classmethod
    def register(cls, name: str, workflow: "WorkflowDSL") -> None: ...
    @classmethod
    def get(cls, name: str) -> Optional[WorkflowDef]: ...

# ... more stubs
'''


# ===== 子构建器 =====

class IfStepBuilder:
    """条件步骤构建器"""
    
    def __init__(self, dsl: WorkflowDSL, condition: Callable[[], bool], source: str = ""):
        self._dsl = dsl
        self._condition = condition
        self._condition_source = source
        self._then_actions: List[Dict[str, Any]] = []
        self._else_actions: List[Dict[str, Any]] = []
    
    def then(self, *actions: Any) -> 'IfStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._then_actions.append(action)
            elif hasattr(action, 'build'):
                self._then_actions.append(action.build())
        return self
    
    def else_(self, *actions: Any) -> 'IfStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._else_actions.append(action)
            elif hasattr(action, 'build'):
                self._else_actions.append(action.build())
        return self
    
    def end(self) -> WorkflowDSL:
        condition_ref = ConditionRef(self._condition, self._condition_source)
        step = WorkflowStep(
            id=f"step_{self._dsl._current_step_id}",
            action=ActionType.IF,
            params={
                "condition_source": self._condition_source or "<lambda>",
                "then": self._then_actions,
                "else": self._else_actions
            },
            conditions=[condition_ref]
        )
        self._dsl._steps.append(step)
        self._dsl._current_step_id += 1
        return self._dsl


class WhileStepBuilder:
    """循环步骤构建器"""
    
    def __init__(self, dsl: WorkflowDSL, condition: Callable[[], bool], source: str = ""):
        self._dsl = dsl
        self._condition = condition
        self._condition_source = source
        self._max_iterations = 1000
        self._body_actions: List[Dict[str, Any]] = []
    
    def do(self, *actions: Any) -> 'WhileStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._body_actions.append(action)
            elif hasattr(action, 'build'):
                self._body_actions.append(action.build())
        return self
    
    def max_iterations(self, n: int) -> 'WhileStepBuilder':
        self._max_iterations = n
        return self
    
    def end(self) -> WorkflowDSL:
        condition_ref = ConditionRef(self._condition, self._condition_source)
        step = WorkflowStep(
            id=f"step_{self._dsl._current_step_id}",
            action=ActionType.WHILE,
            params={
                "condition_source": self._condition_source or "<lambda>",
                "max_iterations": self._max_iterations,
                "body": self._body_actions
            },
            conditions=[condition_ref]
        )
        self._dsl._steps.append(step)
        self._dsl._current_step_id += 1
        return self._dsl


class TryStepBuilder:
    """Try块构建器"""
    
    def __init__(self, dsl: WorkflowDSL):
        self._dsl = dsl
        self._try_actions: List[Dict[str, Any]] = []
        self._catch_actions: List[Dict[str, Any]] = []
        self._finally_actions: List[Dict[str, Any]] = []
        self._exception_var = "e"
    
    def do(self, *actions: Any) -> 'TryStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._try_actions.append(action)
            elif hasattr(action, 'build'):
                self._try_actions.append(action.build())
        return self
    
    def catch(self, var: str = "e") -> 'TryStepBuilder':
        self._exception_var = var
        return self
    
    def catch_do(self, *actions: Any) -> 'TryStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._catch_actions.append(action)
            elif hasattr(action, 'build'):
                self._catch_actions.append(action.build())
        return self
    
    def finally_(self, *actions: Any) -> 'TryStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._finally_actions.append(action)
            elif hasattr(action, 'build'):
                self._finally_actions.append(action.build())
        return self
    
    def end(self) -> WorkflowDSL:
        step = WorkflowStep(
            id=f"step_{self._dsl._current_step_id}",
            action=ActionType.TRY,
            params={
                "try": self._try_actions,
                "catch": self._catch_actions,
                "finally": self._finally_actions,
                "exception_var": self._exception_var
            }
        )
        self._dsl._steps.append(step)
        self._dsl._current_step_id += 1
        return self._dsl


class GroupStepBuilder:
    """步骤组构建器"""
    
    def __init__(self, dsl: WorkflowDSL, name: str):
        self._dsl = dsl
        self._name = name
        self._actions: List[Dict[str, Any]] = []
    
    def add(self, *actions: Any) -> 'GroupStepBuilder':
        for action in actions:
            if isinstance(action, dict):
                self._actions.append(action)
            elif hasattr(action, 'build'):
                self._actions.append(action.build())
        return self
    
    def end(self) -> WorkflowDSL:
        step = WorkflowStep(
            id=f"step_{self._dsl._current_step_id}",
            action=ActionType.GROUP,
            params={
                "name": self._name,
                "actions": self._actions
            }
        )
        self._dsl._steps.append(step)
        self._dsl._current_step_id += 1
        return self._dsl


class TemplateBuilder:
    """模板构建器"""
    
    def __init__(self, name: str, description: str, parameters: Dict[str, VariableType]):
        self._name = name
        self._description = description
        self._parameters = parameters
        self._category = "general"
        self._body_dsl: Optional[WorkflowDSL] = None
    
    def category(self, cat: str) -> 'TemplateBuilder':
        self._category = cat
        return self
    
    def body(self, dsl: WorkflowDSL) -> 'TemplateBuilder':
        self._body_dsl = dsl
        return self
    
    def build(self) -> TemplateDef:
        if not self._body_dsl:
            raise ValueError("Template body is required")
        return TemplateDef(
            name=self._name,
            description=self._description,
            parameters=self._parameters,
            body=self._body_dsl.to_dsl(),
            category=self._category
        )


# ===== 装饰器语法 =====

_workflow_decorator_registry: Dict[str, WorkflowDSL] = {}


def workflow(name: str = "", description: str = ""):
    """
    工作流装饰器
    
    用法:
        @workflow("my_workflow", "描述")
        def my_workflow():
            return (WorkflowDSL("my_workflow")
                .click(100, 200)
                .type("hello")
                .wait(1.0))
    """
    def decorator(func: Callable[[], WorkflowDSL]):
        @wraps(func)
        def wrapper(*args, **kwargs) -> WorkflowDef:
            dsl = func(*args, **kwargs)
            wf = dsl.build()
            _workflow_decorator_registry[name or func.__name__] = dsl
            return wf
        return wrapper
    return decorator


# ===== 辅助函数 =====

def asdict(obj):
    """将对象转换为字典（处理特殊类型）"""
    if is_dataclass(obj):
        result = {}
        for name, value in obj.__dict__.items():
            if name.startswith('_'):
                continue
            if isinstance(value, list):
                result[name] = [asdict(v) if is_dataclass(v) else v for v in value]
            elif is_dataclass(value):
                result[name] = asdict(value)
            elif isinstance(value, Enum):
                result[name] = value.value
            else:
                result[name] = value
        return result
    return obj


# ===== 快捷函数 =====

def create_workflow(name: str) -> WorkflowDSL:
    """创建工作流"""
    return WorkflowDSL(name)


def workflow_from_json(path: str) -> WorkflowDSL:
    """从文件加载工作流"""
    with open(path, 'r', encoding='utf-8') as f:
        return WorkflowDSL.from_json(f.read())


def workflow_to_json(wf: WorkflowDSL, path: str) -> None:
    """保存工作流到文件"""
    with open(path, 'w', encoding='utf-8') as f:
        f.write(wf.to_json())


# ===== 编译为标准格式 =====

def compile_to_json(dsl_code: str) -> str:
    """编译DSL为JSON"""
    wf = WorkflowDSL.parse_dsl(dsl_code)
    return wf.to_json()


def compile_to_yaml(dsl_code: str) -> str:
    """编译DSL为YAML"""
    wf = WorkflowDSL.parse_dsl(dsl_code)
    return wf.to_yaml()


# ===== 示例用法 =====

if __name__ == "__main__":
    # 示例1: 流畅API
    wf = (WorkflowDSL("example_workflow")
        .described("示例工作流")
        .with_variable("count", VariableType.INTEGER)
        .click(100, 200)
        .type("hello {{name}}")
        .press("enter")
        .wait(1.0)
        .if_step(lambda: True, "always_true")
            .then({"action": "click", "x": 100, "y": 200})
        .end()
        .while_step(lambda: True, "always_true")
            .do({"action": "wait", "seconds": 1.0})
            .max_iterations(10)
        .end()
        .build())
    
    print("=== JSON Export ===")
    print(wf.to_json())
    
    print("\n=== YAML Export ===")
    print(wf.to_yaml())
    
    print("\n=== DSL Export ===")
    print(wf.to_dsl())
    
    # 示例2: 装饰器语法
    @workflow("decorated_workflow", "装饰器定义的工作流")
    def my_workflow():
        return (WorkflowDSL("decorated_workflow")
            .click(300, 400)
            .wait(0.5))
    
    decorated_wf = my_workflow()
    print("\n=== Decorated Workflow ===")
    print(json.dumps(asdict(decorated_wf), indent=2))
    
    # 示例3: 类型安全构建器
    click_action = (ClickBuilder()
        .at(100, 200)
        .button("left")
        .double()
        .build())
    print("\n=== Click Builder ===")
    print(click_action)
    
    # 示例4: 模板
    template = (WorkflowDSL.create_template("click_template", "点击模板", 
                                              {"x": VariableType.INTEGER, "y": VariableType.INTEGER})
        .category("actions")
        .body(WorkflowDSL("").click(0, 0)))
    WorkflowDSL.register_template(template)
    
    print("\n=== Template ===")
    print(f"Template: {template.name}")
    
    # 示例5: 从DSL解析
    dsl_code = '''
workflow test_workflow() {
    click(x=100, y=200)
    type(text="hello")
    wait(seconds=1.0)
}
'''
    parsed = WorkflowDSL.parse_dsl(dsl_code)
    print("\n=== Parsed DSL ===")
    print(parsed.to_json())
    
    # 示例6: IDE提示
    print("\n=== IDE Autocomplete Hints ===")
    print(json.dumps(WorkflowDSL.get_autocomplete_hints(), indent=2))
