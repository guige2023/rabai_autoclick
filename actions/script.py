import time
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


DANGEROUS_KEYWORDS = [
    'import os', 'import sys', 'import subprocess', 'import shutil',
    'exec(', 'eval(', 'compile(', '__import__',
    'open(', 'file(', 'input(',
    'os.system', 'os.popen', 'os.spawn',
    'subprocess.', 'shutil.',
    'globals()', 'locals()', 'vars()',
    'getattr(', 'setattr(', 'delattr(',
    '__class__', '__bases__', '__subclasses__',
    '__builtins__', '__import__',
]


class ScriptAction(BaseAction):
    action_type = "script"
    display_name = "执行脚本"
    description = "执行Python代码片段"
    
    def _check_safety(self, code: str) -> tuple:
        for keyword in DANGEROUS_KEYWORDS:
            if keyword in code:
                return False, f"安全限制: 不允许使用 '{keyword}'"
        return True, ""
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        code = params.get('code', '')
        output_var = params.get('output_var', None)
        
        if not code:
            return ActionResult(
                success=False,
                message="脚本代码为空"
            )
        
        is_safe, safety_msg = self._check_safety(code)
        if not is_safe:
            return ActionResult(
                success=False,
                message=safety_msg
            )
        
        try:
            result = context.safe_exec(code, output_var)
            
            return ActionResult(
                success=True,
                message="脚本执行成功",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"脚本执行失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['code']
    
    def get_optional_params(self) -> dict:
        return {
            'output_var': None
        }


class DelayAction(BaseAction):
    action_type = "delay"
    display_name = "延时等待"
    description = "等待指定时间"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        seconds = params.get('seconds', 1)
        random_deviation = params.get('random_deviation', 0)
        
        try:
            if random_deviation > 0:
                import random
                actual_delay = seconds + random.uniform(-random_deviation, random_deviation)
                actual_delay = max(0, actual_delay)
            else:
                actual_delay = max(0, seconds)
            
            time.sleep(actual_delay)
            
            return ActionResult(
                success=True,
                message=f"等待 {actual_delay:.2f} 秒",
                data={'delay': actual_delay}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"延时失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['seconds']
    
    def get_optional_params(self) -> dict:
        return {
            'random_deviation': 0
        }


class ConditionAction(BaseAction):
    action_type = "condition"
    display_name = "条件判断"
    description = "根据条件跳转到不同步骤"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        condition = params.get('condition', '')
        true_next = params.get('true_next', None)
        false_next = params.get('false_next', None)
        
        if not condition:
            return ActionResult(
                success=False,
                message="条件表达式为空"
            )
        
        try:
            result = bool(context._evaluate_expression(condition))
            
            next_step = true_next if result else false_next
            
            return ActionResult(
                success=True,
                message=f"条件结果: {result}",
                data={'result': result},
                next_step_id=next_step
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条件判断失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['condition']
    
    def get_optional_params(self) -> dict:
        return {
            'true_next': None,
            'false_next': None
        }


class LoopAction(BaseAction):
    action_type = "loop"
    display_name = "循环控制"
    description = "控制循环次数和跳转"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        loop_id = params.get('loop_id', 'default')
        count = params.get('count', 1)
        loop_start = params.get('loop_start', None)
        loop_end = params.get('loop_end', None)
        
        current = context.get(f'_loop_{loop_id}', 0)
        current += 1
        context.set(f'_loop_{loop_id}', current)
        
        if current <= count:
            return ActionResult(
                success=True,
                message=f"循环 {current}/{count}",
                data={'current': current, 'total': count},
                next_step_id=loop_start
            )
        else:
            context.set(f'_loop_{loop_id}', 0)
            return ActionResult(
                success=True,
                message=f"循环结束",
                data={'current': current - 1, 'total': count},
                next_step_id=loop_end
            )
    
    def get_required_params(self) -> list:
        return ['loop_id', 'count']
    
    def get_optional_params(self) -> dict:
        return {
            'loop_start': None,
            'loop_end': None
        }


class SetVariableAction(BaseAction):
    action_type = "set_variable"
    display_name = "设置变量"
    description = "设置上下文变量值"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        name = params.get('name', '')
        value = params.get('value', '')
        value_type = params.get('value_type', 'string')
        
        if not name:
            return ActionResult(
                success=False,
                message="变量名为空"
            )
        
        try:
            if value_type == 'int':
                value = int(value)
            elif value_type == 'float':
                value = float(value)
            elif value_type == 'bool':
                value = value.lower() in ('true', '1', 'yes') if isinstance(value, str) else bool(value)
            elif value_type == 'expression':
                value = context._evaluate_expression(value)
            
            context.set(name, value)
            
            return ActionResult(
                success=True,
                message=f"设置变量 {name} = {value}",
                data={'name': name, 'value': value}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置变量失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['name', 'value']
    
    def get_optional_params(self) -> dict:
        return {
            'value_type': 'string'
        }
