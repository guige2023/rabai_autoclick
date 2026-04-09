"""Data validation action module for RabAI AutoClick.

Provides data validation operations:
- SchemaValidationAction: Validate data against schema
- ConstraintValidationAction: Validate data constraints
- CrossFieldValidationAction: Validate across multiple fields
- CustomValidationAction: Custom validation rules
- DataIntegrityAction: Check data integrity
"""

from typing import Any, Dict, List, Optional, Callable
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
    
    def to_dict(self) -> Dict:
        return {"field": self.field, "message": self.message, "code": self.code}


class SchemaValidationAction(BaseAction):
    """Validate data against a schema."""
    action_type = "schema_validation"
    display_name = "模式验证"
    description = "根据模式验证数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            strict = params.get("strict", False)
            
            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")
            
            errors = self._validate(data, schema, strict)
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Schema validation failed with {len(errors)} errors",
                    data={
                        "valid": False,
                        "errors": [e.to_dict() for e in errors]
                    }
                )
            
            return ActionResult(
                success=True,
                message="Schema validation passed",
                data={"valid": True, "errors": []}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _validate(self, data: Dict, schema: Dict, strict: bool) -> List[ValidationError]:
        errors = []
        
        for field, rules in schema.items():
            value = data.get(field)
            
            if "required" in rules and rules["required"] and value is None:
                errors.append(ValidationError(field, "Field is required", "required"))
                continue
            
            if value is None:
                continue
            
            if "type" in rules:
                expected_type = rules["type"]
                if not self._check_type(value, expected_type):
                    errors.append(ValidationError(
                        field, 
                        f"Expected type {expected_type}, got {type(value).__name__}",
                        "type_mismatch"
                    ))
            
            if "min" in rules and isinstance(value, (int, float)):
                if value < rules["min"]:
                    errors.append(ValidationError(field, f"Value must be >= {rules['min']}", "min_value"))
            
            if "max" in rules and isinstance(value, (int, float)):
                if value > rules["max"]:
                    errors.append(ValidationError(field, f"Value must be <= {rules['max']}", "max_value"))
            
            if "min_length" in rules and hasattr(value, "__len__"):
                if len(value) < rules["min_length"]:
                    errors.append(ValidationError(field, f"Length must be >= {rules['min_length']}", "min_length"))
            
            if "max_length" in rules and hasattr(value, "__len__"):
                if len(value) > rules["max_length"]:
                    errors.append(ValidationError(field, f"Length must be <= {rules['max_length']}", "max_length"))
            
            if "pattern" in rules and isinstance(value, str):
                if not re.match(rules["pattern"], value):
                    errors.append(ValidationError(field, f"Pattern mismatch: {rules['pattern']}", "pattern"))
            
            if "enum" in rules:
                if value not in rules["enum"]:
                    errors.append(ValidationError(field, f"Value must be one of {rules['enum']}", "enum"))
        
        if strict:
            allowed_fields = set(schema.keys())
            extra_fields = set(data.keys()) - allowed_fields
            for field in extra_fields:
                errors.append(ValidationError(field, "Unknown field", "unknown_field"))
        
        return errors
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
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
            "null": type(None)
        }
        
        expected = type_map.get(expected_type.lower())
        if expected is None:
            return True
        
        if expected == type(None):
            return value is None
        
        return isinstance(value, expected)


class ConstraintValidationAction(BaseAction):
    """Validate data constraints."""
    action_type = "constraint_validation"
    display_name = "约束验证"
    description = "验证数据约束"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            constraints = params.get("constraints", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            errors = self._validate_constraints(data, constraints)
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Constraint validation failed: {errors[0].message}",
                    data={
                        "valid": False,
                        "errors": [e.to_dict() for e in errors]
                    }
                )
            
            return ActionResult(
                success=True,
                message="Constraint validation passed",
                data={"valid": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _validate_constraints(self, data: List[Dict], constraints: List[Dict]) -> List[ValidationError]:
        errors = []
        
        for i, item in enumerate(data):
            for constraint in constraints:
                constraint_type = constraint.get("type")
                
                if constraint_type == "unique":
                    field = constraint.get("field")
                    values = [str(d.get(field)) for d in data]
                    if len(values) != len(set(values)):
                        errors.append(ValidationError(
                            field, 
                            f"Values in '{field}' are not unique",
                            "unique"
                        ))
                
                elif constraint_type == "positive":
                    field = constraint.get("field")
                    value = item.get(field)
                    if isinstance(value, (int, float)) and value <= 0:
                        errors.append(ValidationError(
                            field, 
                            f"Value must be positive",
                            "positive"
                        ))
                
                elif constraint_type == "non_null":
                    field = constraint.get("field")
                    if item.get(field) is None:
                        errors.append(ValidationError(
                            field, 
                            f"Value cannot be null",
                            "non_null"
                        ))
        
        return errors


class CrossFieldValidationAction(BaseAction):
    """Validate across multiple fields."""
    action_type = "cross_field_validation"
    display_name = "跨字段验证"
    description = "跨多个字段验证数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", [])
            
            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")
            
            errors = self._validate_cross_fields(data, rules)
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Cross-field validation failed: {errors[0].message}",
                    data={
                        "valid": False,
                        "errors": [e.to_dict() for e in errors]
                    }
                )
            
            return ActionResult(
                success=True,
                message="Cross-field validation passed",
                data={"valid": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _validate_cross_fields(self, data: Dict, rules: List[Dict]) -> List[ValidationError]:
        errors = []
        
        for rule in rules:
            rule_name = rule.get("name", "rule")
            condition = rule.get("condition")
            error_msg = rule.get("message", "Cross-field validation failed")
            
            try:
                if callable(condition):
                    if not condition(data):
                        errors.append(ValidationError(rule_name, error_msg, "cross_field"))
                elif isinstance(condition, str):
                    if not eval(condition, {"data": data}):
                        errors.append(ValidationError(rule_name, error_msg, "cross_field"))
            except Exception as e:
                errors.append(ValidationError(rule_name, str(e), "cross_field_error"))
        
        return errors


class CustomValidationAction(BaseAction):
    """Custom validation rules."""
    action_type = "custom_validation"
    display_name = "自定义验证"
    description = "使用自定义规则验证数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            validator = params.get("validator")
            error_message = params.get("error_message", "Validation failed")
            
            if not callable(validator):
                return ActionResult(success=False, message="validator must be callable")
            
            try:
                is_valid = validator(data)
            except Exception as e:
                is_valid = False
                error_message = f"Validation error: {str(e)}"
            
            if is_valid:
                return ActionResult(
                    success=True,
                    message="Custom validation passed",
                    data={"valid": True}
                )
            else:
                return ActionResult(
                    success=False,
                    message=error_message,
                    data={"valid": False, "error": error_message}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DataIntegrityAction(BaseAction):
    """Check data integrity."""
    action_type = "data_integrity"
    display_name = "数据完整性检查"
    description = "检查数据完整性"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            checks = params.get("checks", ["duplicates", "nulls", "types"])
            
            results = {}
            issues = []
            
            if "duplicates" in checks and isinstance(data, list):
                if data != list(dict.fromkeys(data)):
                    issues.append("Duplicate items found")
                    results["duplicates"] = True
                else:
                    results["duplicates"] = False
            
            if "nulls" in checks and isinstance(data, list):
                null_count = sum(1 for item in data if item is None)
                if null_count > 0:
                    issues.append(f"Found {null_count} null items")
                    results["nulls"] = null_count
                else:
                    results["nulls"] = 0
            
            if "types" in checks and isinstance(data, list):
                types = set(type(item).__name__ for item in data)
                if len(types) > 1:
                    issues.append(f"Mixed types found: {types}")
                    results["types"] = list(types)
                else:
                    results["types"] = list(types)
            
            if issues:
                return ActionResult(
                    success=False,
                    message="Data integrity check found issues",
                    data={
                        "valid": False,
                        "issues": issues,
                        "checks": results
                    }
                )
            
            return ActionResult(
                success=True,
                message="Data integrity check passed",
                data={"valid": True, "checks": results}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
