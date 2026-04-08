"""Validator advanced action module for RabAI AutoClick.

Provides advanced data validation: cross-field validation,
conditional validation, and custom validator chains.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CrossFieldValidatorAction(BaseAction):
    """Validate relationships between fields.
    
    Check that field values satisfy cross-field constraints
    like start < end, sum equals total, etc.
    """
    action_type = "cross_field_validate"
    display_name = "跨字段验证"
    description = "验证多个字段之间的关系约束"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate cross-field constraints.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: dict (record to validate)
                - rules: list of {field1, field2, operator, message}
                - stop_on_first: bool
                - save_to_var: str
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data', {})
        rules = params.get('rules', [])
        stop_on_first = params.get('stop_on_first', False)
        save_to_var = params.get('save_to_var', 'cross_field_result')

        if not isinstance(data, dict):
            return ActionResult(success=False, message="Data must be a dict")

        violations = []

        for rule in rules:
            field1 = rule.get('field1', '')
            field2 = rule.get('field2', '')
            operator = rule.get('operator', '==')
            message = rule.get('message', f'{field1} {operator} {field2}')

            v1 = data.get(field1)
            v2 = data.get(field2)

            is_valid = self._check_relation(v1, operator, v2)

            if not is_valid:
                violations.append({
                    'field1': field1,
                    'field2': field2,
                    'operator': operator,
                    'value1': v1,
                    'value2': v2,
                    'message': message,
                })
                if stop_on_first:
                    break

        result = {
            'valid': len(violations) == 0,
            'violations': violations,
            'count': len(violations),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=len(violations) == 0,
            data=result,
            message=f"Cross-field validation: {'PASS' if not violations else f'{len(violations)} violations'}"
        )

    def _check_relation(self, v1: Any, op: str, v2: Any) -> bool:
        """Check relationship between two values."""
        try:
            n1 = float(v1) if v1 is not None else 0
            n2 = float(v2) if v2 is not None else 0
        except (ValueError, TypeError):
            n1, n2 = str(v1), str(v2)

        if op == '==':
            return v1 == v2
        elif op == '!=':
            return v1 != v2
        elif op == '>':
            return n1 > n2
        elif op == '>=':
            return n1 >= n2
        elif op == '<':
            return n1 < n2
        elif op == '<=':
            return n1 <= n2
        elif op == 'sum_equals':
            return abs(n1 - n2) < 1e-9
        elif op == 'contains':
            return str(v2) in str(v1)
        return False


class ConditionalValidatorAction(BaseAction):
    """Apply validation conditionally based on field values.
    
    Only validate certain fields when conditions are met.
    """
    action_type = "conditional_validate"
    display_name = "条件验证"
    description = "基于条件有选择地应用验证规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply conditional validation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: dict (record to validate)
                - conditions: list of {when, field, operator, value, validator}
                - save_to_var: str
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data', {})
        conditions = params.get('conditions', [])
        save_to_var = params.get('save_to_var', 'conditional_result')

        if not isinstance(data, dict):
            return ActionResult(success=False, message="Data must be a dict")

        violations = []
        applied = 0

        for cond in conditions:
            when_field = cond.get('when_field', '')
            when_op = cond.get('when_operator', '==')
            when_value = cond.get('when_value')

            # Check condition
            when_actual = data.get(when_field)
            if not self._eval_condition(when_actual, when_op, when_value):
                continue

            # Condition met, apply validator
            applied += 1
            field = cond.get('field', '')
            validator = cond.get('validator', 'required')
            validator_params = cond.get('params', {})

            field_value = data.get(field)
            is_valid = self._apply_validator(field_value, validator, validator_params)

            if not is_valid:
                violations.append({
                    'field': field,
                    'condition': f'{when_field} {when_op} {when_value}',
                    'validator': validator,
                    'message': f'{field} failed {validator} when {when_field} {when_op} {when_value}',
                })

        result = {
            'valid': len(violations) == 0,
            'applied': applied,
            'violations': violations,
            'count': len(violations),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=len(violations) == 0,
            data=result,
            message=f"Conditional validation: {applied} rules applied, {len(violations)} violations"
        )

    def _eval_condition(self, actual: Any, op: str, expected: Any) -> bool:
        """Evaluate a condition."""
        if op == '==':
            return actual == expected
        elif op == '!=':
            return actual != expected
        elif op == 'exists':
            return actual is not None
        elif op == 'not_exists':
            return actual is None
        elif op == 'truthy':
            return bool(actual)
        elif op == 'falsy':
            return not actual
        elif op == 'contains':
            return str(expected) in str(actual) if actual else False
        return False

    def _apply_validator(self, value: Any, validator: str, params: Dict) -> bool:
        """Apply a validator to a value."""
        if validator == 'required':
            return value is not None and value != ''
        elif validator == 'not_empty':
            if isinstance(value, (list, dict, str)):
                return len(value) > 0
            return value is not None
        elif validator == 'min_length':
            return len(value) >= params.get('min', 0)
        elif validator == 'max_length':
            return len(value) <= params.get('max', float('inf'))
        elif validator == 'min':
            try:
                return float(value) >= params.get('min', 0)
            except (ValueError, TypeError):
                return False
        elif validator == 'max':
            try:
                return float(value) <= params.get('max', float('inf'))
            except (ValueError, TypeError):
                return False
        elif validator == 'pattern':
            pattern = params.get('pattern', '')
            return bool(re.match(pattern, str(value))) if value else False
        elif validator == 'in':
            return value in params.get('values', [])
        elif validator == 'not_in':
            return value not in params.get('values', [])
        return True


class ValidatorChainAction(BaseAction):
    """Chain multiple validators on a single field.
    
    Apply a sequence of validators and collect
    all errors rather than failing on first.
    """
    action_type = "validator_chain"
    display_name = "验证链"
    description = "链式验证器：对单个字段应用多个验证规则"

    VALIDATORS = ['required', 'type', 'min', 'max', 'minLength', 'maxLength',
                  'pattern', 'enum', 'custom']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Chain validators on a field.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: dict (record)
                - field: str (field to validate)
                - validators: list of {name, params}
                - save_to_var: str
        
        Returns:
            ActionResult with chain validation result.
        """
        data = params.get('data', {})
        field = params.get('field', '')
        validators = params.get('validators', [])
        save_to_var = params.get('save_to_var', 'chain_result')

        if not field:
            return ActionResult(success=False, message="Field is required")

        value = data.get(field)
        errors = []

        for v in validators:
            name = v.get('name', '')
            v_params = v.get('params', {})

            is_valid = self._apply_validator(value, name, v_params)

            if not is_valid:
                errors.append({
                    'field': field,
                    'validator': name,
                    'params': v_params,
                    'value': value,
                })

        result = {
            'field': field,
            'valid': len(errors) == 0,
            'errors': errors,
            'count': len(errors),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=len(errors) == 0,
            data=result,
            message=f"Validator chain for {field}: {'PASS' if not errors else f'{len(errors)} failures'}"
        )

    def _apply_validator(self, value: Any, name: str, params: Dict) -> bool:
        """Apply a named validator."""
        if name == 'required':
            return value is not None and value != ''
        elif name == 'type':
            expected = params.get('type', 'string')
            if expected == 'string':
                return isinstance(value, str)
            elif expected == 'number':
                return isinstance(value, (int, float)) and not isinstance(value, bool)
            elif expected == 'integer':
                return isinstance(value, int) and not isinstance(value, bool)
            elif expected == 'boolean':
                return isinstance(value, bool)
            elif expected == 'array':
                return isinstance(value, list)
            elif expected == 'object':
                return isinstance(value, dict)
            return True
        elif name == 'min':
            try:
                return float(value) >= params.get('value', 0)
            except (ValueError, TypeError):
                return False
        elif name == 'max':
            try:
                return float(value) <= params.get('value', float('inf'))
            except (ValueError, TypeError):
                return False
        elif name == 'minLength':
            return len(value) >= params.get('value', 0) if value else False
        elif name == 'maxLength':
            return len(value) <= params.get('value', float('inf')) if value else True
        elif name == 'pattern':
            return bool(re.match(params.get('pattern', ''), str(value))) if value else False
        elif name == 'enum':
            return value in params.get('values', [])
        return True
