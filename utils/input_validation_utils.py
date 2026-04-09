"""Input validation utilities for UI automation.

Provides utilities for validating input data,
checking input constraints, and ensuring input correctness.
"""

from __future__ import annotations

import re
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    message: str
    error_code: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InputConstraints:
    """Constraints for input validation."""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None
    required: bool = True


class InputValidator:
    """Validates input data for UI operations.
    
    Provides comprehensive validation including
    type checking, range validation, and format checking.
    """
    
    def __init__(self) -> None:
        """Initialize the input validator."""
        self._validators: Dict[str, Callable[[Any], ValidationResult]] = {}
        self._register_default_validators()
    
    def _register_default_validators(self) -> None:
        """Register default validator functions."""
        self._validators["required"] = self._validate_required
        self._validators["number"] = self._validate_number
        self._validators["string"] = self._validate_string
        self._validators["coordinate"] = self._validate_coordinate
        self._validators["region"] = self._validate_region
        self._validators["selector"] = self._validate_selector
    
    def validate(
        self,
        value: Any,
        constraints: InputConstraints,
        field_name: str = "value"
    ) -> ValidationResult:
        """Validate a value against constraints.
        
        Args:
            value: Value to validate.
            constraints: Validation constraints.
            field_name: Name of field for error messages.
            
        Returns:
            ValidationResult.
        """
        if constraints.required and value is None:
            return ValidationResult(
                is_valid=False,
                message=f"{field_name} is required",
                error_code="required"
            )
        
        if value is None:
            return ValidationResult(is_valid=True, message="OK")
        
        if constraints.min_value is not None:
            if isinstance(value, (int, float)):
                if value < constraints.min_value:
                    return ValidationResult(
                        is_valid=False,
                        message=f"{field_name} must be >= {constraints.min_value}",
                        error_code="min_value"
                    )
        
        if constraints.max_value is not None:
            if isinstance(value, (int, float)):
                if value > constraints.max_value:
                    return ValidationResult(
                        is_valid=False,
                        message=f"{field_name} must be <= {constraints.max_value}",
                        error_code="max_value"
                    )
        
        if constraints.allowed_values is not None:
            if value not in constraints.allowed_values:
                return ValidationResult(
                    is_valid=False,
                    message=f"{field_name} must be one of {constraints.allowed_values}",
                    error_code="allowed_values"
                )
        
        if constraints.pattern is not None:
            if isinstance(value, str):
                if not re.match(constraints.pattern, value):
                    return ValidationResult(
                        is_valid=False,
                        message=f"{field_name} does not match required pattern",
                        error_code="pattern"
                    )
        
        return ValidationResult(is_valid=True, message="OK")
    
    def validate_type(
        self,
        value: Any,
        expected_type: str,
        field_name: str = "value"
    ) -> ValidationResult:
        """Validate value type.
        
        Args:
            value: Value to validate.
            expected_type: Expected type name.
            field_name: Field name for error messages.
            
        Returns:
            ValidationResult.
        """
        validator = self._validators.get(expected_type)
        if validator:
            return validator(value)
        
        return ValidationResult(
            is_valid=False,
            message=f"Unknown validator type: {expected_type}",
            error_code="unknown_type"
        )
    
    def _validate_required(self, value: Any) -> ValidationResult:
        """Validate required field.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if value is None:
            return ValidationResult(
                is_valid=False,
                message="Value is required",
                error_code="required"
            )
        return ValidationResult(is_valid=True, message="OK")
    
    def _validate_number(self, value: Any) -> ValidationResult:
        """Validate number type.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if not isinstance(value, (int, float)):
            return ValidationResult(
                is_valid=False,
                message="Value must be a number",
                error_code="type_number"
            )
        return ValidationResult(is_valid=True, message="OK")
    
    def _validate_string(self, value: Any) -> ValidationResult:
        """Validate string type.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False,
                message="Value must be a string",
                error_code="type_string"
            )
        return ValidationResult(is_valid=True, message="OK")
    
    def _validate_coordinate(self, value: Any) -> ValidationResult:
        """Validate coordinate.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if isinstance(value, tuple):
            if len(value) != 2:
                return ValidationResult(
                    is_valid=False,
                    message="Coordinate must have 2 values",
                    error_code="coordinate_length"
                )
            if not isinstance(value[0], (int, float)) or not isinstance(value[1], (int, float)):
                return ValidationResult(
                    is_valid=False,
                    message="Coordinate values must be numbers",
                    error_code="coordinate_type"
                )
            return ValidationResult(is_valid=True, message="OK")
        
        return ValidationResult(
            is_valid=False,
            message="Coordinate must be a tuple (x, y)",
            error_code="type_coordinate"
        )
    
    def _validate_region(self, value: Any) -> ValidationResult:
        """Validate region.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if isinstance(value, dict):
            required_keys = ["x", "y", "width", "height"]
            for key in required_keys:
                if key not in value:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Region missing required key: {key}",
                        error_code="region_missing_key"
                    )
            return ValidationResult(is_valid=True, message="OK")
        
        if isinstance(value, tuple) and len(value) == 4:
            return ValidationResult(is_valid=True, message="OK")
        
        return ValidationResult(
            is_valid=False,
            message="Region must be dict or tuple (x, y, width, height)",
            error_code="type_region"
        )
    
    def _validate_selector(self, value: Any) -> ValidationResult:
        """Validate selector.
        
        Args:
            value: Value to validate.
            
        Returns:
            ValidationResult.
        """
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False,
                message="Selector must be a string",
                error_code="type_selector"
            )
        
        if len(value) == 0:
            return ValidationResult(
                is_valid=False,
                message="Selector cannot be empty",
                error_code="selector_empty"
            )
        
        return ValidationResult(is_valid=True, message="OK")
    
    def register_validator(
        self,
        name: str,
        validator: Callable[[Any], ValidationResult]
    ) -> None:
        """Register a custom validator.
        
        Args:
            name: Validator name.
            validator: Validator function.
        """
        self._validators[name] = validator


class CoordinateValidator:
    """Validates coordinate values for input operations.
    
    Checks coordinates for validity including
    bounds checking and precision validation.
    """
    
    def __init__(
        self,
        min_x: float = 0.0,
        min_y: float = 0.0,
        max_x: float = 10000.0,
        max_y: float = 10000.0,
        precision: int = 2
    ) -> None:
        """Initialize the coordinate validator.
        
        Args:
            min_x: Minimum X value.
            min_y: Minimum Y value.
            max_x: Maximum X value.
            max_y: Maximum Y value.
            precision: Decimal precision.
        """
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self.precision = precision
    
    def validate_coordinate(
        self,
        x: float,
        y: float
    ) -> ValidationResult:
        """Validate a coordinate pair.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            ValidationResult.
        """
        if not isinstance(x, (int, float)):
            return ValidationResult(
                is_valid=False,
                message="X must be a number",
                error_code="x_not_number"
            )
        
        if not isinstance(y, (int, float)):
            return ValidationResult(
                is_valid=False,
                message="Y must be a number",
                error_code="y_not_number"
            )
        
        if math.isnan(x) or math.isinf(x):
            return ValidationResult(
                is_valid=False,
                message="X must be finite",
                error_code="x_not_finite"
            )
        
        if math.isnan(y) or math.isinf(y):
            return ValidationResult(
                is_valid=False,
                message="Y must be finite",
                error_code="y_not_finite"
            )
        
        if x < self.min_x or x > self.max_x:
            return ValidationResult(
                is_valid=False,
                message=f"X must be between {self.min_x} and {self.max_x}",
                error_code="x_out_of_bounds"
            )
        
        if y < self.min_y or y > self.max_y:
            return ValidationResult(
                is_valid=False,
                message=f"Y must be between {self.min_y} and {self.max_y}",
                error_code="y_out_of_bounds"
            )
        
        return ValidationResult(is_valid=True, message="OK")
    
    def validate_path(
        self,
        points: List[Tuple[float, float]]
    ) -> ValidationResult:
        """Validate a path of points.
        
        Args:
            points: List of (x, y) tuples.
            
        Returns:
            ValidationResult.
        """
        if not points:
            return ValidationResult(
                is_valid=False,
                message="Path must have at least one point",
                error_code="path_empty"
            )
        
        for i, (x, y) in enumerate(points):
            result = self.validate_coordinate(x, y)
            if not result.is_valid:
                result.message = f"Point {i}: {result.message}"
                return result
        
        return ValidationResult(is_valid=True, message="OK")
    
    def quantize_coordinate(self, value: float) -> float:
        """Quantize coordinate to precision.
        
        Args:
            value: Coordinate value.
            
        Returns:
            Quantized value.
        """
        factor = 10 ** self.precision
        return round(value * factor) / factor


class GestureValidator:
    """Validates gesture parameters.
    
    Checks gesture configurations for validity
    including duration, velocity, and gesture-specific checks.
    """
    
    def __init__(
        self,
        min_duration_ms: float = 50.0,
        max_duration_ms: float = 60000.0,
        min_velocity: float = 0.0,
        max_velocity: float = 10000.0
    ) -> None:
        """Initialize the gesture validator.
        
        Args:
            min_duration_ms: Minimum gesture duration.
            max_duration_ms: Maximum gesture duration.
            min_velocity: Minimum gesture velocity.
            max_velocity: Maximum gesture velocity.
        """
        self.min_duration_ms = min_duration_ms
        self.max_duration_ms = max_duration_ms
        self.min_velocity = min_velocity
        self.max_velocity = max_velocity
    
    def validate_tap(
        self,
        x: float,
        y: float,
        duration_ms: float
    ) -> ValidationResult:
        """Validate tap gesture.
        
        Args:
            x: Tap X coordinate.
            y: Tap Y coordinate.
            duration_ms: Tap duration.
            
        Returns:
            ValidationResult.
        """
        if not isinstance(x, (int, float)) or math.isnan(x) or math.isinf(x):
            return ValidationResult(
                is_valid=False,
                message="X must be a valid number",
                error_code="invalid_x"
            )
        
        if not isinstance(y, (int, float)) or math.isnan(y) or math.isinf(y):
            return ValidationResult(
                is_valid=False,
                message="Y must be a valid number",
                error_code="invalid_y"
            )
        
        if duration_ms < self.min_duration_ms:
            return ValidationResult(
                is_valid=False,
                message=f"Duration must be >= {self.min_duration_ms}ms",
                error_code="duration_too_short"
            )
        
        if duration_ms > self.max_duration_ms:
            return ValidationResult(
                is_valid=False,
                message=f"Duration must be <= {self.max_duration_ms}ms",
                error_code="duration_too_long"
            )
        
        return ValidationResult(is_valid=True, message="OK")
    
    def validate_swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: float
    ) -> ValidationResult:
        """Validate swipe gesture.
        
        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            duration_ms: Swipe duration.
            
        Returns:
            ValidationResult.
        """
        coord_validator = CoordinateValidator()
        result = coord_validator.validate_coordinate(start_x, start_y)
        if not result.is_valid:
            return result
        
        result = coord_validator.validate_coordinate(end_x, end_y)
        if not result.is_valid:
            return result
        
        if duration_ms < self.min_duration_ms:
            return ValidationResult(
                is_valid=False,
                message=f"Duration must be >= {self.min_duration_ms}ms",
                error_code="duration_too_short"
            )
        
        if duration_ms > self.max_duration_ms:
            return ValidationResult(
                is_valid=False,
                message=f"Duration must be <= {self.max_duration_ms}ms",
                error_code="duration_too_long"
            )
        
        dx = end_x - start_x
        dy = end_y - start_y
        distance = math.sqrt(dx * dx + dy * dy)
        
        if distance < 10:
            return ValidationResult(
                is_valid=False,
                message="Swipe distance must be at least 10 pixels",
                error_code="swipe_too_short"
            )
        
        return ValidationResult(is_valid=True, message="OK")


def validate_element_data(data: Dict[str, Any]) -> List[ValidationResult]:
    """Validate element data structure.
    
    Args:
        data: Element data dictionary.
        
    Returns:
        List of validation results.
    """
    results = []
    
    required_keys = ["id", "x", "y", "width", "height"]
    for key in required_keys:
        if key not in data:
            results.append(ValidationResult(
                is_valid=False,
                message=f"Missing required key: {key}",
                error_code=f"missing_{key}"
            ))
    
    validator = CoordinateValidator()
    if "x" in data and "y" in data:
        result = validator.validate_coordinate(data["x"], data["y"])
        if not result.is_valid:
            results.append(result)
    
    return results
