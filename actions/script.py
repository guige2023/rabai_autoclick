"""Script action module for RabAI AutoClick.

Provides script execution and control flow actions:
- ScriptAction: Execute Python code snippets safely
- DelayAction: Wait/delay execution
- ConditionAction: Conditional branching
- LoopAction: Loop control
- SetVariableAction: Set context variables
"""

import ast
import json
import re
import time
import random
import sys
import os
from typing import Any, Dict, List, Optional, Tuple, Union

from rabai_autoclick.core.base_action import BaseAction, ActionResult


# Modules and names blocked from import/attribute access
DANGEROUS_MODULES: frozenset[str] = frozenset({
    'os', 'sys', 'subprocess', 'shutil', 'threading', 'multiprocessing',
    'ctypes', 'signal', 'socket', 'requests', 'urllib', 'http', 'ftplib',
    'telnetlib', 'poplib', 'imaplib', 'smtplib', 'pty', 'tty', 'termios',
    'importlib', 'pkgutil', 'runpy', 'code', 'codeop',
    'pickle', 'marshal', 'yaml', 'json',
})

# Attribute names that are always dangerous
DANGEROUS_ATTRS: frozenset[str] = frozenset({
    'system', 'popen', 'spawn', 'spawnl', 'spawnle', 'spawnv',
    'exec', 'execfile', 'compile', '__import__', '__builtins__',
    'getattr', 'setattr', 'delattr', 'write', 'read',
    'globals', 'locals', 'vars', 'open', 'file',
    'fork', 'execv', 'execl', 'execle', 'execlp', 'execvp',
})

# Builtin names that are dangerous when called
DANGEROUS_BUILTINS: frozenset[str] = frozenset({
    'exec', 'eval', 'compile', 'open', 'input', '__import__',
})

# Valid value types for SetVariableAction
VALID_VALUE_TYPES: List[str] = ['string', 'int', 'float', 'bool', 'expression', 'list', 'dict', 'tuple', 'none']

# Pattern for human-readable delay format (e.g., "2m", "30s", "1h30m")
DELAY_PATTERN = re.compile(r'^(\d+)([smh])$')


class ScriptAction(BaseAction):
    """Execute Python code snippets in a sandboxed environment."""
    action_type = "script"
    display_name = "执行脚本"
    description = "执行Python代码片段"
    
    def _check_safety(self, code: str) -> Tuple[bool, str]:
        """Check if code contains dangerous patterns using AST analysis.
        
        Args:
            code: Python code to check.
            
        Returns:
            Tuple of (is_safe, error_message).
        """
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return False, "安全限制: 代码语法错误"

        for node in ast.walk(tree):
            # Block import X / import X as Y
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split('.')[0]
                    if root in DANGEROUS_MODULES:
                        return False, f"安全限制: 不允许导入 '{alias.name}'"

            # Block from X import Y
            if isinstance(node, ast.ImportFrom):
                if node.module:
                    root = node.module.split('.')[0]
                    if root in DANGEROUS_MODULES:
                        return False, f"安全限制: 不允许从 '{node.module}' 导入"

            # Block attribute access: os.spawn, sys.exit, etc.
            if isinstance(node, ast.Attribute):
                if node.attr in DANGEROUS_ATTRS:
                    return False, f"安全限制: 不允许使用 '{node.attr}'"

            # Block dangerous calls: eval(), exec(), open(), compile()
            if isinstance(node, ast.Call):
                # Direct name call: eval(...), exec(...)
                if isinstance(node.func, ast.Name):
                    if node.func.id in DANGEROUS_BUILTINS:
                        return False, f"安全限制: 不允许调用 '{node.func.id}'"
                # Attribute call: os.system(...), obj.getattr(...)
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in DANGEROUS_ATTRS:
                        return False, f"安全限制: 不允许调用 '{node.func.attr}'"

            # Block named expressions (:=) which can shadow builtins
            if isinstance(node, ast.NamedExpr):
                return False, f"安全限制: 不允许使用命名表达式"

        return True, ""
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a Python code snippet.
        
        Args:
            context: Execution context.
            params: Dict with code, output_var.
            
        Returns:
            ActionResult indicating success or failure.
        """
        code = params.get('code', '')
        output_var = params.get('output_var', None)
        
        if not code:
            return ActionResult(
                success=False,
                message="脚本代码为空"
            )
        
        valid, msg = self.validate_type(code, str, 'code')
        if not valid:
            return ActionResult(success=False, message=msg)
        
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
    
    def get_required_params(self) -> List[str]:
        return ['code']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_var': None
        }


class DelayAction(BaseAction):
    """Wait/delay execution for specified duration."""
    action_type = "delay"
    display_name = "延时等待"
    description = "等待指定时间"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a delay.
        
        Args:
            context: Execution context.
            params: Dict with seconds, random_deviation.
            
        Returns:
            ActionResult indicating success or failure.
        """
        seconds = params.get('seconds', 1)
        random_deviation = params.get('random_deviation', 0)
        
        # Validate seconds
        valid, msg = self.validate_type(seconds, (int, float), 'seconds')
        if not valid:
            return ActionResult(success=False, message=msg)
        if seconds < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'seconds' must be >= 0, got {seconds}"
            )
        
        # Validate random_deviation
        valid, msg = self.validate_type(
            random_deviation, (int, float), 'random_deviation'
        )
        if not valid:
            return ActionResult(success=False, message=msg)
        if random_deviation < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'random_deviation' must be >= 0, got {random_deviation}"
            )
        
        try:
            if random_deviation > 0:
                actual_delay = seconds + random.uniform(
                    -random_deviation, random_deviation
                )
                actual_delay = max(0, actual_delay)
            else:
                actual_delay = max(0, float(seconds))
            
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
    
    def get_required_params(self) -> List[str]:
        return ['seconds']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'random_deviation': 0
        }


class ConditionAction(BaseAction):
    """Conditional branching based on expression evaluation."""
    action_type = "condition"
    display_name = "条件判断"
    description = "根据条件跳转到不同步骤"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a conditional branch.
        
        Args:
            context: Execution context.
            params: Dict with condition, true_next, false_next.
            
        Returns:
            ActionResult with next_step_id based on condition result.
        """
        condition = params.get('condition', '')
        true_next = params.get('true_next', None)
        false_next = params.get('false_next', None)
        
        if not condition:
            return ActionResult(
                success=False,
                message="条件表达式为空"
            )
        
        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)
        
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
    
    def get_required_params(self) -> List[str]:
        return ['condition']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'true_next': None,
            'false_next': None
        }


class LoopAction(BaseAction):
    """Control loop iterations and branching."""
    action_type = "loop"
    display_name = "循环控制"
    description = "控制循环次数和跳转"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a loop iteration.
        
        Args:
            context: Execution context.
            params: Dict with loop_id, count, loop_start, loop_end.
            
        Returns:
            ActionResult with next_step_id for loop control.
        """
        loop_id = params.get('loop_id', 'default')
        count = params.get('count', 1)
        loop_start = params.get('loop_start', None)
        loop_end = params.get('loop_end', None)
        
        # Validate count
        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'count' must be >= 1, got {count}"
            )
        
        try:
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
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"循环控制失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return ['loop_id', 'count']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'loop_start': None,
            'loop_end': None
        }


class SetVariableAction(BaseAction):
    """Set a variable in the execution context."""
    action_type = "set_variable"
    display_name = "设置变量"
    description = "设置上下文变量值"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a variable set operation.
        
        Args:
            context: Execution context.
            params: Dict with name, value, value_type.
            
        Returns:
            ActionResult indicating success or failure.
        """
        name = params.get('name', '')
        value = params.get('value', '')
        value_type = params.get('value_type', 'string')
        
        if not name:
            return ActionResult(
                success=False,
                message="变量名为空"
            )

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Block dangerous variable names
        if name.startswith('_'):
            return ActionResult(success=False, message=f"安全限制: 变量名不能以 '_' 开头")
        if re.search(r'\.|\[|\]|\(|\)', name):
            return ActionResult(success=False, message=f"安全限制: 变量名包含非法字符")
        blocked_names = {'context', 'self', 'import', 'global', 'nonlocal'}
        if name.lower() in blocked_names:
            return ActionResult(success=False, message=f"安全限制: '{name}' 是保留关键字")
        
        valid, msg = self.validate_in(value_type, VALID_VALUE_TYPES, 'value_type')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if value_type == 'int':
                value = int(value)
            elif value_type == 'float':
                value = float(value)
            elif value_type == 'bool':
                if isinstance(value, str):
                    value = value.lower() in ('true', '1', 'yes')
                else:
                    value = bool(value)
            elif value_type == 'expression':
                value = context._evaluate_expression(value)
            # else: keep as string
            
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
    
    def get_required_params(self) -> List[str]:
        return ['name', 'value']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value_type': 'string'
        }
