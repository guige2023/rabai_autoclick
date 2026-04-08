"""Data Checker Action Module. Validates data against rules and schemas."""
import sys, os, re
from typing import Any, Callable
from dataclasses import dataclass
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ValidationRule:
    field_name: str; rule_type: str; rule_value: Any; error_message: str = ""

@dataclass
class ValidationError:
    field: str; rule_type: str; value: Any; message: str

class DataCheckerAction(BaseAction):
    action_type = "data_checker"; display_name = "数据校验"
    description = "校验数据"
    def __init__(self) -> None: super().__init__(); self._registry = {}
    def _validate_rule(self, value: Any, rule: ValidationRule) -> ValidationError:
        if rule.rule_type == "required":
            if value is None or value == "": return ValidationError(rule.field_name, rule.rule_type, value, rule.error_message or f"{rule.field_name} required")
        elif rule.rule_type == "type":
            expected = rule.rule_value
            if expected == "string" and not isinstance(value, str): return ValidationError(rule.field_name, rule.rule_type, type(value).__name__, f"{rule.field_name} must be string")
            elif expected == "int" and not isinstance(value, int): return ValidationError(rule.field_name, rule.rule_type, type(value).__name__, f"{rule.field_name} must be int")
            elif expected == "float" and not isinstance(value, (int, float)): return ValidationError(rule.field_name, rule.rule_type, type(value).__name__, f"{rule.field_name} must be numeric")
        elif rule.rule_type == "range":
            mn, mx = rule.rule_value.get("min"), rule.rule_value.get("max")
            if mn is not None and value < mn: return ValidationError(rule.field_name, rule.rule_type, value, f"{rule.field_name} < {mn}")
            if mx is not None and value > mx: return ValidationError(rule.field_name, rule.rule_type, value, f"{rule.field_name} > {mx}")
        elif rule.rule_type == "pattern":
            if not re.match(rule.rule_value, str(value)): return ValidationError(rule.field_name, rule.rule_type, value, f"{rule.field_name} pattern mismatch")
        elif rule.rule_type == "one_of":
            if value not in rule.rule_value: return ValidationError(rule.field_name, rule.rule_type, value, f"{rule.field_name} must be one of {rule.rule_value}")
        return None
    def execute(self, context: Any, params: dict) -> ActionResult:
        data = params.get("data",{}); rules_dicts = params.get("rules",[]); stop_on_first = params.get("stop_on_first", False)
        rules = [ValidationRule(field_name=rd.get("field_name",""), rule_type=rd.get("rule_type","required"),
                               rule_value=rd.get("rule_value"), error_message=rd.get("error_message",""))
                 for rd in rules_dicts]
        errors = []
        for rule in rules:
            error = self._validate_rule(data.get(rule.field_name), rule)
            if error:
                errors.append(error)
                if stop_on_first: break
        return ActionResult(success=len(errors)==0, message=f"Validation: {'PASSED' if not errors else f'FAILED ({len(errors)})'}",
                          data={"valid": len(errors)==0, "errors": [vars(e) for e in errors]})
