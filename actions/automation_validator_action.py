"""Automation validator action module for RabAI AutoClick.

Provides validation operations for automation workflows:
- SchemaValidatorAction: Validate data against schemas
- TypeValidatorAction: Validate data types
- RangeValidatorAction: Validate numeric ranges
- RequiredFieldsValidatorAction: Validate required fields
- CustomValidatorAction: Custom validation logic
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
import re

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidationError:
    """Represents a validation error."""
    
    def __init__(self, field: str, message: str, code: str = "validation_error"):
        self.field = field
        self.message = message
        self.code = code
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            "field": self.field,
            "message": self.message,
            "code": self.code,
            "timestamp": self.timestamp.isoformat()
        }


class SchemaValidatorAction(BaseAction):
    """Validate data against schemas."""
    action_type = "schema_validator"
    display_name = "模式验证"
    description = "根据模式验证数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            
            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")
            
            errors = self._validate_against_schema(data, schema)
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Validation failed with {len(errors)} errors",
                    data={
                        "valid": False,
                        "error_count": len(errors),
                        "errors": [e.to_dict() for e in errors]
                    }
                )
            
            return ActionResult(
                success=True,
                message="Schema validation passed",
                data={
                    "valid": True,
                    "error_count": 0
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _validate_against_schema(self, data: Dict, schema: Dict) -> List[ValidationError]:
        errors = []
        
        for field, rules in schema.items():
            value = data.get(field)
            
            required = rules.get("required", False)
            if required and value is None:
                errors.append(ValidationError(field, "Field is required", "required"))
                continue
            
            if value is None:
                continue
            
            expected_type = rules.get("type")
            if expected_type and not self._check_type(value, expected_type):
                errors.append(ValidationError(
                    field, 
                    f"Expected type {expected_type}, got {type(value).__name__}", 
                    "type_mismatch"
                ))
            
            min_val = rules.get("min")
            if min_val is not None and isinstance(value, (int, float)) and value < min_val:
                errors.append(ValidationError(field, f"Value must be >= {min_val}", "min_value"))
            
            max_val = rules.get("max")
            if max_val is not None and isinstance(value, (int, float)) and value > max_val:
                errors.append(ValidationError(field, f"Value must be <= {max_val}", "max_value"))
            
            min_length = rules.get("min_length")
            if min_length is not None and hasattr(value, "__len__") and len(value) < min_length:
                errors.append(ValidationError(field, f"Length must be >= {min_length}", "min_length"))
            
            max_length = rules.get("max_length")
            if max_length is not None and hasattr(value, "__len__") and len(value) > max_length:
                errors.append(ValidationError(field, f"Length must be <= {max_length}", "max_length"))
            
            pattern = rules.get("pattern")
            if pattern and isinstance(value, str) and not re.match(pattern, value):
                errors.append(ValidationError(field, f"Pattern mismatch: {pattern}", "pattern_mismatch"))
            
            allowed_values = rules.get("allowed_values")
            if allowed_values and value not in allowed_values:
                errors.append(ValidationError(
                    field, 
                    f"Value must be one of {allowed_values}", 
                    "allowed_values"
                ))
        
        return errors
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
            "string": str,
            "int": int,
            "float": (int, float),
            "bool": bool,
            "list": list,
            "dict": dict,
            "none": type(None)
        }
        
        expected = type_map.get(expected_type, expected_type)
        
        if expected == "none":
            return value is None
        
        return isinstance(value, expected)


class TypeValidatorAction(BaseAction):
    """Validate data types."""
    action_type = "type_validator"
    display_name = "类型验证"
    description = "验证数据类型"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            expected_type = params.get("expected_type")
            allow_none = params.get("allow_none", False)
            
            if expected_type is None:
                return ActionResult(success=False, message="expected_type is required")
            
            if data is None:
                if allow_none:
                    return ActionResult(
                        success=True,
                        message="Type validation passed (data is None, allowed)",
                        data={"valid": True, "actual_type": None, "expected_type": expected_type}
                    )
                else:
                    return ActionResult(
                        success=False,
                        message="Data is None but None is not allowed",
                        data={"valid": False, "actual_type": None, "expected_type": expected_type}
                    )
            
            actual_type = type(data).__name__
            valid = self._check_type(data, expected_type)
            
            if valid:
                return ActionResult(
                    success=True,
                    message="Type validation passed",
                    data={"valid": True, "actual_type": actual_type, "expected_type": expected_type}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Type mismatch: expected {expected_type}, got {actual_type}",
                    data={"valid": False, "actual_type": actual_type, "expected_type": expected_type}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
            "str": str,
            "string": str,
            "int": int,
            "integer": int,
            "float": (int, float),
            "number": (int, float),
            "bool": bool,
            "boolean": bool,
            "list": list,
            "array": list,
            "dict": dict,
            "object": dict,
            "none": type(None)
        }
        
        expected = type_map.get(expected_type.lower(), expected_type)
        
        if expected == "none":
            return value is None
        
        return isinstance(value, expected)


class RangeValidatorAction(BaseAction):
    """Validate numeric ranges."""
    action_type = "range_validator"
    display_name = "范围验证"
    description = "验证数值范围"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value")
            min_val = params.get("min")
            max_val = params.get("max")
            inclusive = params.get("inclusive", True)
            field_name = params.get("field_name", "value")
            
            if value is None:
                return ActionResult(
                    success=False,
                    message="value is required",
                    data={"valid": False, "error": "value is None"}
                )
            
            if not isinstance(value, (int, float)):
                return ActionResult(
                    success=False,
                    message="value must be numeric",
                    data={"valid": False, "actual_type": type(value).__name__}
                )
            
            errors = []
            
            if min_val is not None:
                if inclusive and value < min_val:
                    errors.append(f"Value must be >= {min_val}")
                elif not inclusive and value <= min_val:
                    errors.append(f"Value must be > {min_val}")
            
            if max_val is not None:
                if inclusive and value > max_val:
                    errors.append(f"Value must be <= {max_val}")
                elif not inclusive and value >= max_val:
                    errors.append(f"Value must be < {max_val}")
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Range validation failed: {errors[0]}",
                    data={
                        "valid": False,
                        "value": value,
                        "min": min_val,
                        "max": max_val,
                        "inclusive": inclusive,
                        "field": field_name,
                        "errors": errors
                    }
                )
            
            return ActionResult(
                success=True,
                message="Range validation passed",
                data={
                    "valid": True,
                    "value": value,
                    "min": min_val,
                    "max": max_val,
                    "inclusive": inclusive,
                    "field": field_name
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class RequiredFieldsValidatorAction(BaseAction):
    """Validate required fields."""
    action_type = "required_fields_validator"
    display_name = "必填字段验证"
    description = "验证必填字段是否存在"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            required_fields = params.get("required_fields", [])
            optional_fields = params.get("optional_fields", [])
            
            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")
            
            missing_fields = []
            empty_fields = []
            
            for field in required_fields:
                if field not in data:
                    missing_fields.append(field)
                elif data[field] is None or data[field] == "":
                    empty_fields.append(field)
            
            all_fields = list(data.keys())
            extra_fields = [f for f in all_fields if f not in required_fields and f not in optional_fields]
            
            if missing_fields or empty_fields:
                return ActionResult(
                    success=False,
                    message=f"Validation failed: missing {len(missing_fields)} fields, empty {len(empty_fields)} fields",
                    data={
                        "valid": False,
                        "missing_fields": missing_fields,
                        "empty_fields": empty_fields,
                        "extra_fields": extra_fields if extra_fields else None
                    }
                )
            
            return ActionResult(
                success=True,
                message="Required fields validation passed",
                data={
                    "valid": True,
                    "missing_fields": [],
                    "empty_fields": [],
                    "extra_fields": extra_fields if extra_fields else None
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CustomValidatorAction(BaseAction):
    """Custom validation logic."""
    action_type = "custom_validator"
    display_name = "自定义验证"
    description = "使用自定义验证逻辑"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            validator_fn = params.get("validator_fn")
            error_message = params.get("error_message", "Custom validation failed")
            
            if not callable(validator_fn):
                return ActionResult(success=False, message="validator_fn must be callable")
            
            try:
                is_valid = validator_fn(data)
            except Exception as e:
                is_valid = False
                error_message = f"Validation function error: {str(e)}"
            
            if is_valid:
                return ActionResult(
                    success=True,
                    message="Custom validation passed",
                    data={"valid": True, "data": data}
                )
            else:
                return ActionResult(
                    success=False,
                    message=error_message,
                    data={"valid": False, "error": error_message}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
