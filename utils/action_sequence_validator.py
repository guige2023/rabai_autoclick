"""Action sequence validator for validating action sequences before execution."""
from typing import List, Dict, Optional, Callable, Tuple
from dataclasses import dataclass
from enum import Enum, auto


class ValidationResult(Enum):
    """Result of validation."""
    VALID = auto()
    WARNING = auto()
    INVALID = auto()


@dataclass
class ValidationError:
    """A single validation error or warning."""
    result: ValidationResult
    code: str
    message: str
    action_index: Optional[int] = None
    field: Optional[str] = None


class ActionSequenceValidator:
    """Validates action sequences for correctness and safety.
    
    Checks action sequences for common errors, safety issues,
    and provides detailed validation reports.
    
    Example:
        validator = ActionSequenceValidator()
        result, errors = validator.validate(sequence)
        if result != ValidationResult.VALID:
            for error in errors:
                print(f"{error.code}: {error.message}")
    """

    def __init__(self) -> None:
        self._rules: List[Callable] = []

    def add_rule(self, rule: Callable) -> None:
        """Add a custom validation rule."""
        self._rules.append(rule)

    def validate(self, sequence: List[Dict]) -> Tuple[ValidationResult, List[ValidationError]]:
        """Validate an action sequence."""
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        
        for i, action in enumerate(sequence):
            action_type = action.get("type", "")
            if not action_type:
                errors.append(ValidationError(
                    ValidationResult.INVALID, "MISSING_TYPE",
                    f"Action at index {i} missing type", action_index=i, field="type"
                ))
            
            if action_type == "click" and self._has_invalid_coordinates(action):
                errors.append(ValidationError(
                    ValidationResult.INVALID, "INVALID_COORDS",
                    f"Click action at {i} has invalid coordinates", action_index=i, field="coordinates"
                ))
        
        for rule in self._rules:
            try:
                rule_errors = rule(sequence)
                errors.extend(rule_errors)
            except Exception:
                pass
        
        if any(e.result == ValidationResult.INVALID for e in errors):
            result = ValidationResult.INVALID
        elif warnings:
            result = ValidationResult.WARNING
        else:
            result = ValidationResult.VALID
        
        return result, errors + warnings

    def _has_invalid_coordinates(self, action: Dict) -> bool:
        """Check if action has invalid coordinates."""
        x = action.get("x", 0)
        y = action.get("y", 0)
        return not (isinstance(x, (int, float)) and isinstance(y, (int, float)))

    def validate_safety(self, sequence: List[Dict]) -> List[ValidationError]:
        """Perform safety checks on a sequence."""
        errors: List[ValidationError] = []
        dangerous_patterns = ["rm -rf", "delete *", "format"]
        
        for i, action in enumerate(sequence):
            cmd = action.get("command", "")
            for pattern in dangerous_patterns:
                if pattern.lower() in str(cmd).lower():
                    errors.append(ValidationError(
                        ValidationResult.INVALID, "DANGEROUS_COMMAND",
                        f"Dangerous command pattern at {i}", action_index=i, field="command"
                    ))
        
        return errors
