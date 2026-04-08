"""Condition action module for RabAI AutoClick.

Provides conditional logic actions including if/else, switch, and comparison operations.
"""

import operator
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CompareAction(BaseAction):
    """Compare two values using various comparison operators.
    
    Supports numeric, string, and general equality comparisons
    with configurable operators.
    """
    action_type = "compare"
    display_name = "比较运算"
    description = "比较两个值的大小关系"
    
    OPERATORS = {
        '==': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '>=': operator.ge,
        '<': operator.lt,
        '<=': operator.le,
        'is': lambda a, b: a is b,
        'is_not': lambda a, b: a is not b,
        'in': lambda a, b: a in b,
        'not_in': lambda a, b: a not in b,
        'contains': lambda a, b: b in a if hasattr(a, '__contains__') else False,
        'starts_with': lambda a, b: str(a).startswith(str(b)),
        'ends_with': lambda a, b: str(a).endswith(str(b)),
        'matches': lambda a, b: bool(__import__('re').match(str(b), str(a))),
    }
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute comparison.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: left, right, operator, case_sensitive.
        
        Returns:
            ActionResult with comparison result.
        """
        left = params.get('left')
        right = params.get('right')
        op = params.get('operator', '==')
        case_sensitive = params.get('case_sensitive', True)
        
        if op not in self.OPERATORS:
            return ActionResult(
                success=False,
                message=f"Unknown operator: {op}"
            )
        
        # String case handling
        if not case_sensitive and isinstance(left, str) and isinstance(right, str):
            left = left.lower()
            right = right.lower()
        
        try:
            result = self.OPERATORS[op](left, right)
            
            return ActionResult(
                success=True,
                message=f"{left} {op} {right} = {result}",
                data={
                    'result': result,
                    'left': left,
                    'right': right,
                    'operator': op
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Comparison error: {e}",
                data={'error': str(e), 'left': left, 'right': right, 'operator': op}
            )


class IfAction(BaseAction):
    """Evaluate conditions and branch execution.
    
    Implements if/elif/else logic with variable condition sources.
    """
    action_type = "if_condition"
    display_name = "条件分支"
    description = "条件判断与分支执行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate if condition.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: condition, then_value, else_value,
                   condition_var.
        
        Returns:
            ActionResult with selected branch value.
        """
        condition = params.get('condition')
        then_value = params.get('then_value')
        else_value = params.get('else_value', None)
        condition_var = params.get('condition_var', None)
        
        # If condition is a variable name, get from context
        if condition_var:
            condition = getattr(context, condition_var, None)
        
        # If condition is callable, evaluate it
        if callable(condition):
            try:
                condition = condition()
            except Exception:
                condition = False
        
        # If condition is a string, evaluate as expression
        if isinstance(condition, str):
            try:
                condition = bool(eval(condition, {'context': context}))
            except Exception:
                condition = False
        
        if condition:
            return ActionResult(
                success=True,
                message="Condition true, returning then_value",
                data={
                    'branch': 'then',
                    'value': then_value,
                    'condition': condition
                }
            )
        else:
            return ActionResult(
                success=True,
                message="Condition false, returning else_value",
                data={
                    'branch': 'else',
                    'value': else_value,
                    'condition': condition
                }
            )


class SwitchAction(BaseAction):
    """Match a value against multiple cases.
    
    Implements switch/case pattern with default fallback.
    """
    action_type = "switch"
    display_name = "多值匹配"
    description = "多值匹配分支"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute switch matching.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: value, cases (dict), default.
        
        Returns:
            ActionResult with matched case value.
        """
        value = params.get('value')
        cases = params.get('cases', {})
        default = params.get('default', None)
        
        if not isinstance(cases, dict):
            return ActionResult(
                success=False,
                message="cases must be a dictionary"
            )
        
        # Check for exact match
        if value in cases:
            return ActionResult(
                success=True,
                message=f"Matched case: {value}",
                data={
                    'matched': True,
                    'case': value,
                    'value': cases[value]
                }
            )
        
        # Check for pattern matches (regex)
        import re
        for pattern, result in cases.items():
            if isinstance(pattern, str) and '*' in pattern:
                regex_pattern = pattern.replace('*', '.*')
                if re.match(regex_pattern, str(value)):
                    return ActionResult(
                        success=True,
                        message=f"Matched pattern: {pattern}",
                        data={
                            'matched': True,
                            'pattern': pattern,
                            'value': result
                        }
                    )
        
        # Default case
        return ActionResult(
            success=True,
            message="No match, returning default",
            data={
                'matched': False,
                'case': None,
                'value': default
            }
        )


class LogicalOpAction(BaseAction):
    """Perform logical operations (AND, OR, NOT, XOR).
    
    Combines multiple conditions with logical operators.
    """
    action_type = "logical_op"
    display_name = "逻辑运算"
    description = "组合多个条件的逻辑运算"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical operation.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: operation, values, value.
        
        Returns:
            ActionResult with logical operation result.
        """
        operation = params.get('operation', 'and')
        values = params.get('values', [])
        value = params.get('value', None)
        
        if operation in ['and', 'or'] and not values:
            # Support single value with operation
            if value is None:
                return ActionResult(
                    success=False,
                    message="No values provided"
                )
            values = [value]
        
        # Convert all values to boolean
        try:
            bool_values = []
            for v in values:
                if callable(v):
                    v = v()
                elif isinstance(v, str):
                    v = bool(eval(v, {'context': context}))
                bool_values.append(bool(v))
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Logical operation error: {e}",
                data={'error': str(e)}
            )
        
        if operation == 'and':
            result = all(bool_values)
        elif operation == 'or':
            result = any(bool_values)
        elif operation == 'not':
            result = not bool(bool_values[0]) if bool_values else True
        elif operation == 'xor':
            result = sum(bool_values) % 2 == 1
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
        
        return ActionResult(
            success=True,
            message=f"{operation.upper()} of {len(bool_values)} values = {result}",
            data={
                'operation': operation,
                'values': bool_values,
                'result': result
            }
        )


class CoalesceAction(BaseAction):
    """Return first non-null/non-empty value from a list.
    
    Useful for providing fallback values.
    """
    action_type = "coalesce"
    display_name = "空值合并"
    description = "返回第一个非空值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute coalesce operation.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: values, default.
        
        Returns:
            ActionResult with first non-empty value or default.
        """
        values = params.get('values', [])
        default = params.get('default', None)
        
        if not values:
            return ActionResult(
                success=True,
                message="No values, returning default",
                data={'value': default, 'index': None}
            )
        
        for i, v in enumerate(values):
            # Check for None, empty string, empty list, etc.
            if v is not None and v != '' and v != [] and v != {}:
                return ActionResult(
                    success=True,
                    message=f"Found value at index {i}",
                    data={'value': v, 'index': i}
                )
        
        return ActionResult(
            success=True,
            message="No non-empty values, returning default",
            data={'value': default, 'index': None}
        )
