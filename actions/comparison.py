"""Comparison action module for RabAI AutoClick.

Provides value comparison and conditional logic actions
for workflow branching and validation.
"""

import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CompareAction(BaseAction):
    """Compare two values with various operators.
    
    Supports numeric, string, and equality comparisons.
    """
    action_type = "compare"
    display_name = "比较"
    description = "比较两个值"

    OPERATORS = ['==', '!=', '>', '<', '>=', '<=', 'is', 'is_not', 'in', 'not_in']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compare values.
        
        Args:
            context: Execution context.
            params: Dict with keys: left, operator, right,
                   case_sensitive, save_to_var.
        
        Returns:
            ActionResult with comparison result.
        """
        left = params.get('left', None)
        operator = params.get('operator', '==')
        right = params.get('right', None)
        case_sensitive = params.get('case_sensitive', True)
        save_to_var = params.get('save_to_var', None)

        if operator not in self.OPERATORS:
            return ActionResult(
                success=False,
                message=f"Invalid operator: {operator}"
            )

        # Prepare values for comparison
        left_val = left
        right_val = right

        if not case_sensitive and isinstance(left_val, str) and isinstance(right_val, str):
            left_val = left_val.lower()
            right_val = right_val.lower()

        # Perform comparison
        result = False
        try:
            if operator == '==':
                result = left_val == right_val
            elif operator == '!=':
                result = left_val != right_val
            elif operator == '>':
                result = float(left_val) > float(right_val)
            elif operator == '<':
                result = float(left_val) < float(right_val)
            elif operator == '>=':
                result = float(left_val) >= float(right_val)
            elif operator == '<=':
                result = float(left_val) <= float(right_val)
            elif operator == 'is':
                result = left_val is right_val
            elif operator == 'is_not':
                result = left_val is not right_val
            elif operator == 'in':
                result = right_val in left_val if left_val else False
            elif operator == 'not_in':
                result = right_val not in left_val if left_val else True
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"Comparison error: {e}"
            )

        result_data = {
            'result': result,
            'left': left,
            'right': right,
            'operator': operator
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"{left} {operator} {right} = {result}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['left', 'operator', 'right']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'case_sensitive': True,
            'save_to_var': None
        }


class SwitchCaseAction(BaseAction):
    """Switch-case style value matching.
    
    Returns value from first matching case.
    """
    action_type = "switch_case"
    display_name = "分支匹配"
    description = "根据值匹配返回对应结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Match value against cases.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, cases (dict),
                   default, save_to_var.
        
        Returns:
            ActionResult with matched case value.
        """
        value = params.get('value', None)
        cases = params.get('cases', {})
        default = params.get('default', None)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(cases, dict):
            return ActionResult(
                success=False,
                message=f"Cases must be dict, got {type(cases).__name__}"
            )

        # Find matching case
        result = default
        matched_key = None

        for key, val in cases.items():
            if key == value:
                result = val
                matched_key = key
                break

        result_data = {
            'matched': matched_key,
            'result': result,
            'value': value,
            'is_default': matched_key is None
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if matched_key:
            return ActionResult(
                success=True,
                message=f"匹配成功: {value} -> {result}",
                data=result_data
            )
        else:
            return ActionResult(
                success=True,
                message=f"使用默认值: {default}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'cases']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'default': None,
            'save_to_var': None
        }
