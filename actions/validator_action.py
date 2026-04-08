"""Validator action module for RabAI AutoClick.

Provides data validation against schemas (JSON Schema, custom rules).
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ValidatorAction(BaseAction):
    """Data validation against schemas and custom rules.
    
    Supports JSON Schema-like validation, type checking, range validation,
    pattern matching, and custom validator functions.
    """
    action_type = "validator"
    display_name = "数据验证器"
    description = "Schema验证与数据类型校验"
    
    TYPE_MAP = {
        'string': str, 'str': str,
        'integer': int, 'int': int,
        'float': float, 'double': float,
        'boolean': bool, 'bool': bool,
        'array': list, 'list': list,
        'object': dict,
        'null': type(None)
    }
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute validation operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'validate', 'validate_batch', 'validate_schema'
                - data: Data to validate
                - rules: Validation rules dict
                - rules_file: Path to rules JSON/YAML file
        
        Returns:
            ActionResult with validation result.
        """
        command = params.get('command', 'validate')
        data = params.get('data')
        rules = params.get('rules', {})
        rules_file = params.get('rules_file')
        
        if rules_file:
            rules = self._load_rules(rules_file)
            if rules is None:
                return ActionResult(success=False, message=f"Failed to load rules from {rules_file}")
        
        if command == 'validate':
            if data is None:
                return ActionResult(success=False, message="data is required for validate")
            return self._validate(data, rules)
        
        if command == 'validate_batch':
            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list for validate_batch")
            results = []
            all_valid = True
            for i, item in enumerate(data):
                result = self._validate(item, rules)
                if not result.success:
                    all_valid = False
                results.append({'index': i, 'valid': result.success, 'message': result.message})
            return ActionResult(
                success=all_valid,
                message=f"Batch: {sum(1 for r in results if r['valid'])}/{len(results)} valid",
                data={'results': results, 'total': len(results), 'valid_count': sum(1 for r in results if r['valid'])}
            )
        
        if command == 'validate_schema':
            return self._validate_schema(rules)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _load_rules(self, rules_file: str) -> Optional[Dict[str, Any]]:
        """Load rules from JSON or YAML file."""
        try:
            with open(rules_file, 'r', encoding='utf-8') as f:
                if rules_file.endswith('.yaml') or rules_file.endswith('.yml'):
                    import yaml
                    return yaml.safe_load(f)
                else:
                    import json
                    return json.load(f)
        except Exception:
            return None
    
    def _validate(self, data: Any, rules: Dict[str, Any]) -> ActionResult:
        """Validate single data item against rules."""
        errors: List[str] = []
        
        for field, rule in rules.items():
            if field == '_strict' and rule:
                data_keys = set(data.keys()) if isinstance(data, dict) else set()
                rule_keys = set(rules.keys()) - {'_strict'}
                extra = data_keys - rule_keys
                if extra:
                    errors.append(f"Unexpected fields: {extra}")
                continue
            
            value = data.get(field) if isinstance(data, dict) else getattr(data, field, None)
            
            required = rule.get('required', False)
            if required and value is None:
                errors.append(f"Field '{field}' is required")
                continue
            
            if value is None:
                continue
            
            expected_type = rule.get('type')
            if expected_type:
                py_type = self.TYPE_MAP.get(expected_type)
                if py_type and not isinstance(value, py_type):
                    errors.append(f"Field '{field}' must be {expected_type}, got {type(value).__name__}")
            
            min_val = rule.get('min')
            if min_val is not None and isinstance(value, (int, float)) and value < min_val:
                errors.append(f"Field '{field}' must be >= {min_val}, got {value}")
            
            max_val = rule.get('max')
            if max_val is not None and isinstance(value, (int, float)) and value > max_val:
                errors.append(f"Field '{field}' must be <= {max_val}, got {value}")
            
            min_len = rule.get('minLength')
            if min_len is not None and isinstance(value, (str, list, dict)) and len(value) < min_len:
                errors.append(f"Field '{field}' length must be >= {min_len}, got {len(value)}")
            
            max_len = rule.get('maxLength')
            if max_len is not None and isinstance(value, (str, list, dict)) and len(value) > max_len:
                errors.append(f"Field '{field}' length must be <= {max_len}, got {len(value)}")
            
            pattern = rule.get('pattern')
            if pattern and isinstance(value, str):
                if not re.match(pattern, value):
                    errors.append(f"Field '{field}' pattern mismatch: {pattern}")
            
            enum_vals = rule.get('enum')
            if enum_vals and value not in enum_vals:
                errors.append(f"Field '{field}' must be one of {enum_vals}, got {value}")
            
            custom = rule.get('validator')
            if custom and callable(custom):
                try:
                    result = custom(value)
                    if not result:
                        errors.append(f"Field '{field}' failed custom validation")
                except Exception as e:
                    errors.append(f"Field '{field}' validator error: {e}")
        
        if errors:
            return ActionResult(success=False, message='; '.join(errors), data={'errors': errors})
        return ActionResult(success=True, message="Validation passed", data={'data': data})
    
    def _validate_schema(self, rules: Dict[str, Any]) -> ActionResult:
        """Validate schema definition itself."""
        if not isinstance(rules, dict):
            return ActionResult(success=False, message="Schema rules must be a dict")
        return ActionResult(
            success=True,
            message=f"Schema valid with {len(rules)} fields",
            data={'fields': list(rules.keys())}
        )
