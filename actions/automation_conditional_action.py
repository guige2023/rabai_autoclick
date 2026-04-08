"""Automation conditional action module for RabAI AutoClick.

Provides conditional execution logic for workflows including
if/then/else branches, switch statements, matchers, and
dynamic routing based on conditions.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IfThenElseAction(BaseAction):
    """If/then/else conditional execution.
    
    Evaluates a condition and executes appropriate branch
    (then or else) based on the result.
    """
    action_type = "if_then_else"
    display_name = "条件分支"
    description = "If/then/else条件执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute conditional branch.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition, then_action,
                   else_action, then_params, else_params.
        
        Returns:
            ActionResult with executed branch result.
        """
        condition = params.get('condition', False)
        then_action = params.get('then_action')
        else_action = params.get('else_action')
        then_params = params.get('then_params', {})
        else_params = params.get('else_params', {})
        start_time = time.time()

        condition_result = self._resolve_condition(condition, context, params)

        if condition_result:
            branch = 'then'
            action_name = then_action
            action_params = then_params
        else:
            branch = 'else'
            action_name = else_action
            action_params = else_params

        if not action_name:
            return ActionResult(
                success=True,
                message=f"Condition evaluated to {condition_result}, no action to execute",
                data={
                    'condition_result': condition_result,
                    'branch': branch,
                    'executed': False
                },
                duration=time.time() - start_time
            )

        result = self._execute_action(context, action_name, action_params)
        result.data = result.data or {}
        result.data['branch'] = branch
        result.data['condition_result'] = condition_result

        return ActionResult(
            success=result.success,
            message=f"Executed '{branch}' branch (condition={condition_result})",
            data=result.data,
            duration=time.time() - start_time
        )

    def _resolve_condition(
        self,
        condition: Any,
        context: Any,
        params: Dict[str, Any]
    ) -> bool:
        """Resolve condition to boolean."""
        if isinstance(condition, bool):
            return condition
        if isinstance(condition, str):
            return self._evaluate_string_condition(condition, context, params)
        if isinstance(condition, dict):
            return self._evaluate_dict_condition(condition, context)
        if callable(condition):
            return condition(context, params)
        return bool(condition)

    def _evaluate_string_condition(
        self,
        condition: str,
        context: Any,
        params: Dict[str, Any]
    ) -> bool:
        """Evaluate string-based condition expressions."""
        condition = condition.strip()
        if condition.startswith('var '):
            var_name = condition[4:].strip()
            value = getattr(context, var_name, None) if hasattr(context, var_name) else None
            return bool(value)
        if '==' in condition:
            left, right = condition.split('==', 1)
            return self._get_value(left.strip(), context, params) == self._get_value(right.strip(), context, params)
        if '!=' in condition:
            left, right = condition.split('!=', 1)
            return self._get_value(left.strip(), context, params) != self._get_value(right.strip(), context, params)
        if '>' in condition:
            parts = re.split(r'(>=?|<=?)', condition)
            if len(parts) == 3:
                left = self._get_value(parts[0].strip(), context, params)
                right = self._get_value(parts[2].strip(), context, params)
                op = parts[1].strip()
                return eval(f"{left} {op} {right}") if isinstance(left, (int, float)) and isinstance(right, (int, float)) else False
        return bool(condition)

    def _evaluate_dict_condition(
        self,
        condition: Dict[str, Any],
        context: Any
    ) -> bool:
        """Evaluate dict-based conditions."""
        operator = condition.get('operator', 'eq')
        left = condition.get('left')
        right = condition.get('right')
        left_val = self._get_value(left, context, {}) if isinstance(left, str) else left
        right_val = self._get_value(right, context, {}) if isinstance(right, str) else right
        if operator == 'eq':
            return left_val == right_val
        elif operator == 'ne':
            return left_val != right_val
        elif operator == 'gt':
            return left_val > right_val
        elif operator == 'gte':
            return left_val >= right_val
        elif operator == 'lt':
            return left_val < right_val
        elif operator == 'lte':
            return left_val <= right_val
        elif operator == 'in':
            return left_val in right_val if right_val else False
        elif operator == 'not_in':
            return left_val not in right_val if right_val else True
        elif operator == 'contains':
            return right_val in left_val if left_val else False
        elif operator == 'matches':
            return bool(re.match(str(right_val or ''), str(left_val or '')))
        return False

    def _get_value(self, path: str, context: Any, params: Dict[str, Any]) -> Any:
        """Get value from context or params using dot notation."""
        if path.startswith('params.'):
            keys = path[7:].split('.')
            value = params
            for k in keys:
                value = value.get(k) if isinstance(value, dict) else getattr(value, k, None)
            return value
        if path.startswith('context.'):
            keys = path[8:].split('.')
            value = context
            for k in keys:
                value = getattr(value, k, None) if hasattr(context, k) else None
                if value is None:
                    value = context.variables.get(k) if hasattr(context, 'variables') else None
            return value
        if hasattr(context, path):
            return getattr(context, path)
        if hasattr(context, 'variables') and path in context.variables:
            return context.variables[path]
        return path

    def _execute_action(
        self,
        context: Any,
        action_name: str,
        action_params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a named action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                return action.execute(context, action_params)
        except ImportError:
            pass
        return ActionResult(success=False, message=f"Action '{action_name}' not found")


class SwitchCaseAction(BaseAction):
    """Switch/case multi-way conditional branching.
    
    Evaluates a value against multiple cases and executes
    the matching case's action. Supports default case.
    """
    action_type = "switch_case"
    display_name = "多路分支"
    description = "Switch/Case多条件分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute switch-case.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, cases (list of
                   {match, action, params}), default_action,
                   default_params.
        
        Returns:
            ActionResult with matched case result.
        """
        value = params.get('value')
        cases = params.get('cases', [])
        default_action = params.get('default_action')
        default_params = params.get('default_params', {})
        start_time = time.time()

        matched_case = None
        for case in cases:
            match = case.get('match')
            if self._matches(value, match):
                matched_case = case
                break

        if matched_case:
            action_name = matched_case.get('action')
            action_params = matched_case.get('params', {})
            branch = f"case:{matched_case.get('match')}"
        elif default_action:
            action_name = default_action
            action_params = default_params
            branch = 'default'
        else:
            return ActionResult(
                success=True,
                message=f"No case matched for value: {value}",
                data={
                    'value': value,
                    'branch': None,
                    'executed': False
                },
                duration=time.time() - start_time
            )

        result = self._execute_action(context, action_name, action_params)
        result.data = result.data or {}
        result.data['branch'] = branch
        result.data['matched_value'] = value

        return ActionResult(
            success=result.success,
            message=f"Executed '{branch}'",
            data=result.data,
            duration=time.time() - start_time
        )

    def _matches(self, value: Any, match: Any) -> bool:
        """Check if value matches case condition."""
        if match is None:
            return value is None
        if isinstance(match, str) and match.startswith('regex:'):
            pattern = match[6:]
            return bool(re.match(pattern, str(value)))
        if isinstance(match, list):
            return value in match
        if callable(match):
            return match(value)
        return value == match

    def _execute_action(
        self,
        context: Any,
        action_name: str,
        action_params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a named action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                return action.execute(context, action_params)
        except ImportError:
            pass
        return ActionResult(success=False, message=f"Action '{action_name}' not found")


class MatchExpressionAction(BaseAction):
    """Pattern matching with guard conditions.
    
    Matches input value against patterns with optional
    guard expressions and returns matched pattern result.
    """
    action_type = "match_expression"
    display_name = "模式匹配"
    description = "带守卫条件的结果模式匹配"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pattern matching.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, patterns (list of
                   {pattern, guard, result}), default_result.
        
        Returns:
            ActionResult with matched pattern result.
        """
        value = params.get('value')
        patterns = params.get('patterns', [])
        default_result = params.get('default_result')
        start_time = time.time()

        for p in patterns:
            pattern = p.get('pattern')
            guard = p.get('guard')
            result_data = p.get('result', {})
            guard_passed = True

            if guard:
                guard_passed = self._evaluate_guard(guard, value, context, params)

            if guard_passed and self._match_pattern(value, pattern):
                return ActionResult(
                    success=True,
                    message=f"Matched pattern: {pattern}",
                    data={
                        'matched': True,
                        'pattern': pattern,
                        'result': result_data,
                        'value': value
                    },
                    duration=time.time() - start_time
                )

        return ActionResult(
            success=True,
            message="No pattern matched, using default",
            data={
                'matched': False,
                'pattern': None,
                'result': default_result,
                'value': value
            },
            duration=time.time() - start_time
        )

    def _match_pattern(self, value: Any, pattern: Any) -> bool:
        """Match value against pattern."""
        if pattern is None:
            return value is None
        if isinstance(pattern, str):
            if pattern == '*':
                return True
            if pattern.startswith('type:'):
                return type(value).__name__ == pattern[5:]
            if pattern.startswith('regex:'):
                return bool(re.match(pattern[6:], str(value)))
            return str(value) == pattern
        if isinstance(pattern, type):
            return isinstance(value, pattern)
        if callable(pattern):
            return pattern(value)
        return value == pattern

    def _evaluate_guard(
        self,
        guard: Any,
        value: Any,
        context: Any,
        params: Dict[str, Any]
    ) -> bool:
        """Evaluate guard condition."""
        if isinstance(guard, bool):
            return guard
        if isinstance(guard, dict):
            operator = guard.get('operator', 'eq')
            left = guard.get('left')
            right = guard.get('right')
            if isinstance(left, str) and left == '$value':
                left = value
            if left == right:
                return operator == 'eq'
            return False
        if callable(guard):
            return guard(value, context)
        return bool(guard)


class ConditionalRouterAction(BaseAction):
    """Route data to different outputs based on conditions.
    
    Evaluates multiple routing rules and routes data
    to the first matching output destination.
    """
    action_type = "conditional_router"
    display_name = "条件路由"
    description = "根据条件将数据路由到不同输出"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route data based on conditions.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, routes (list of
                   {condition, destination, transform}),
                   default_destination.
        
        Returns:
            ActionResult with routing result.
        """
        data = params.get('data')
        routes = params.get('routes', [])
        default_destination = params.get('default_destination')
        start_time = time.time()

        for route in routes:
            condition = route.get('condition')
            destination = route.get('destination')
            transform = route.get('transform')
            condition_result = self._evaluate_condition(condition, data, context, params)

            if condition_result:
                output_data = data
                if transform:
                    output_data = self._apply_transform(data, transform)
                return ActionResult(
                    success=True,
                    message=f"Routed to '{destination}' (matched condition)",
                    data={
                        'destination': destination,
                        'data': output_data,
                        'matched': True,
                        'transform_applied': bool(transform)
                    },
                    duration=time.time() - start_time
                )

        if default_destination:
            return ActionResult(
                success=True,
                message=f"No route matched, using default: '{default_destination}'",
                data={
                    'destination': default_destination,
                    'data': data,
                    'matched': False
                },
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message="No route matched, data returned as-is",
            data={
                'destination': None,
                'data': data,
                'matched': False
            },
            duration=time.time() - start_time
        )

    def _evaluate_condition(
        self,
        condition: Any,
        data: Any,
        context: Any,
        params: Dict[str, Any]
    ) -> bool:
        """Evaluate routing condition."""
        if condition is None:
            return True
        if isinstance(condition, bool):
            return condition
        if isinstance(condition, dict):
            operator = condition.get('operator', 'eq')
            left_path = condition.get('field', '')
            expected = condition.get('value')
            actual = self._get_field(data, left_path)
            ops = {
                'eq': lambda a, e: a == e,
                'ne': lambda a, e: a != e,
                'gt': lambda a, e: a > e,
                'gte': lambda a, e: a >= e,
                'lt': lambda a, e: a < e,
                'lte': lambda a, e: a <= e,
                'in': lambda a, e: a in e,
                'not_in': lambda a, e: a not in e,
                'contains': lambda a, e: e in a,
            }
            return ops.get(operator, lambda a, e: a == e)(actual, expected)
        if callable(condition):
            return condition(data, context)
        return bool(condition)

    def _get_field(self, data: Any, path: str) -> Any:
        """Get field from data using dot notation."""
        if not path:
            return data
        keys = path.split('.')
        value = data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value

    def _apply_transform(self, data: Any, transform: Dict[str, Any]) -> Any:
        """Apply data transform."""
        transform_type = transform.get('type', 'passthrough')
        if transform_type == 'passthrough':
            return data
        if transform_type == 'pick':
            fields = transform.get('fields', [])
            if isinstance(data, dict):
                return {k: data.get(k) for k in fields if k in data}
        if transform_type == 'omit':
            fields = transform.get('fields', [])
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if k not in fields}
        if transform_type == 'map':
            mapping = transform.get('mapping', {})
            if isinstance(data, dict):
                return {mapping.get(k, k): v for k, v in data.items()}
        return data


class TernaryAction(BaseAction):
    """Ternary conditional expression.
    
    Returns one of two values based on a condition,
    similar to Python's ternary expression.
    """
    action_type = "ternary"
    display_name = "三元表达式"
    description = "简洁的三元条件表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate ternary expression.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition, true_value,
                   false_value.
        
        Returns:
            ActionResult with selected value.
        """
        condition = params.get('condition', False)
        true_value = params.get('true_value')
        false_value = params.get('false_value')
        start_time = time.time()

        condition_result = bool(condition)
        result_value = true_value if condition_result else false_value

        return ActionResult(
            success=True,
            message=f"Ternary: condition={condition_result} → selected {'true' if condition_result else 'false'}",
            data={
                'result': result_value,
                'selected_branch': 'true' if condition_result else 'false',
                'condition_result': condition_result
            },
            duration=time.time() - start_time
        )
