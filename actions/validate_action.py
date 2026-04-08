"""Validation action module for RabAI AutoClick.

Provides data validation utilities for common
data types including strings, numbers, and collections.
"""

import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ValidateRequiredAction(BaseAction):
    """Validate that a value is not empty/null.
    
    Checks for None, empty string, empty list, etc.
    """
    action_type = "validate_required"
    display_name = "验证必填"
    description = "验证值非空"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate required.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, field_name,
                   save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', None)
        field_name = params.get('field_name', 'field')
        save_to_var = params.get('save_to_var', None)

        is_valid = True
        error_msg = None

        if value is None:
            is_valid = False
            error_msg = f"{field_name} is required"
        elif isinstance(value, str) and not value.strip():
            is_valid = False
            error_msg = f"{field_name} cannot be empty"
        elif isinstance(value, (list, tuple, dict)) and len(value) == 0:
            is_valid = False
            error_msg = f"{field_name} cannot be empty"

        result_data = {
            'valid': is_valid,
            'field': field_name,
            'value': value,
            'error': error_msg
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"{field_name} validation passed",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=error_msg,
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field_name': 'field',
            'save_to_var': None
        }


class ValidateRangeAction(BaseAction):
    """Validate number is within range.
    
    Checks min and max bounds.
    """
    action_type = "validate_range"
    display_name = "验证范围"
    description = "验证数值在指定范围内"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate range.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, min, max, exclusive,
                   save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', 0)
        min_val = params.get('min', None)
        max_val = params.get('max', None)
        exclusive = params.get('exclusive', False)
        save_to_var = params.get('save_to_var', None)

        errors = []

        try:
            num = float(value)
        except (ValueError, TypeError):
            result_data = {'valid': False, 'error': f"Cannot convert {value} to number"}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"Invalid number: {value}",
                data=result_data
            )

        if min_val is not None:
            try:
                min_num = float(min_val)
                if exclusive:
                    if num <= min_num:
                        errors.append(f"{num} must be > {min_num}")
                else:
                    if num < min_num:
                        errors.append(f"{num} must be >= {min_num}")
            except (ValueError, TypeError):
                errors.append(f"Invalid min value: {min_val}")

        if max_val is not None:
            try:
                max_num = float(max_val)
                if exclusive:
                    if num >= max_num:
                        errors.append(f"{num} must be < {max_num}")
                else:
                    if num > max_num:
                        errors.append(f"{num} must be <= {max_num}")
            except (ValueError, TypeError):
                errors.append(f"Invalid max value: {max_val}")

        is_valid = len(errors) == 0

        result_data = {
            'valid': is_valid,
            'value': num,
            'errors': errors
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"Range validation passed: {num}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"Range validation failed: {'; '.join(errors)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min': None,
            'max': None,
            'exclusive': False,
            'save_to_var': None
        }


class ValidateLengthAction(BaseAction):
    """Validate string or collection length.
    
    Checks min and max length constraints.
    """
    action_type = "validate_length"
    display_name = "验证长度"
    description = "验证字符串或集合长度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate length.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, min_length, max_length,
                   save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        min_length = params.get('min_length', None)
        max_length = params.get('max_length', None)
        save_to_var = params.get('save_to_var', None)

        errors = []

        try:
            length = len(value)
        except TypeError:
            result_data = {'valid': False, 'error': f"Cannot get length of {type(value).__name__}"}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"Cannot get length of {type(value).__name__}",
                data=result_data
            )

        if min_length is not None:
            try:
                min_len = int(min_length)
                if length < min_len:
                    errors.append(f"Length {length} < minimum {min_len}")
            except (ValueError, TypeError):
                errors.append(f"Invalid min_length: {min_length}")

        if max_length is not None:
            try:
                max_len = int(max_length)
                if length > max_len:
                    errors.append(f"Length {length} > maximum {max_len}")
            except (ValueError, TypeError):
                errors.append(f"Invalid max_length: {max_length}")

        is_valid = len(errors) == 0

        result_data = {
            'valid': is_valid,
            'length': length,
            'errors': errors
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"Length validation passed: {length}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"Length validation failed: {'; '.join(errors)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min_length': None,
            'max_length': None,
            'save_to_var': None
        }
