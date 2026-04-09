"""
Input Validation Utilities for UI Automation.

This module provides comprehensive input validation utilities for
validating coordinates, actions, and automation parameters.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional, Callable


class ValidationError(Exception):
    """Custom validation error exception."""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: list[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @classmethod
    def success(cls) -> 'ValidationResult':
        """Create a successful validation result."""
        return cls(valid=True)
    
    @classmethod
    def failure(cls, *errors: str) -> 'ValidationResult':
        """Create a failed validation result."""
        return cls(valid=False, errors=list(errors))
    
    def add_error(self, error: str) -> None:
        """Add an error message."""
        self.errors.append(error)
        self.valid = False
    
    def merge(self, other: 'ValidationResult') -> None:
        """Merge another validation result into this one."""
        if not other.valid:
            self.valid = False
            self.errors.extend(other.errors)


class Validator:
    """
    Base validator class with common validation methods.
    """
    
    @staticmethod
    def required(value: Any, field_name: str) -> ValidationResult:
        """Validate that a value is not None or empty."""
        if value is None:
            return ValidationResult.failure(f"{field_name} is required")
        if isinstance(value, str) and not value.strip():
            return ValidationResult.failure(f"{field_name} cannot be empty")
        return ValidationResult.success()
    
    @staticmethod
    def min_value(value: float, min_val: float, field_name: str) -> ValidationResult:
        """Validate minimum numeric value."""
        if value < min_val:
            return ValidationResult.failure(
                f"{field_name} must be at least {min_val}, got {value}"
            )
        return ValidationResult.success()
    
    @staticmethod
    def max_value(value: float, max_val: float, field_name: str) -> ValidationResult:
        """Validate maximum numeric value."""
        if value > max_val:
            return ValidationResult.failure(
                f"{field_name} must be at most {max_val}, got {value}"
            )
        return ValidationResult.success()
    
    @staticmethod
    def range(
        value: float, 
        min_val: float, 
        max_val: float, 
        field_name: str
    ) -> ValidationResult:
        """Validate value is within range."""
        result = ValidationResult.success()
        result.merge(Validator.min_value(value, min_val, field_name))
        result.merge(Validator.max_value(value, max_val, field_name))
        return result
    
    @staticmethod
    def length(
        value: str, 
        min_length: int, 
        max_length: int, 
        field_name: str
    ) -> ValidationResult:
        """Validate string length."""
        if len(value) < min_length:
            return ValidationResult.failure(
                f"{field_name} must be at least {min_length} characters"
            )
        if len(value) > max_length:
            return ValidationResult.failure(
                f"{field_name} must be at most {max_length} characters"
            )
        return ValidationResult.success()
    
    @staticmethod
    def pattern(value: str, pattern: str, field_name: str) -> ValidationResult:
        """Validate string matches regex pattern."""
        if not re.match(pattern, value):
            return ValidationResult.failure(
                f"{field_name} does not match required pattern"
            )
        return ValidationResult.success()
    
    @staticmethod
    def one_of(
        value: Any, 
        allowed: list[Any], 
        field_name: str
    ) -> ValidationResult:
        """Validate value is one of allowed values."""
        if value not in allowed:
            return ValidationResult.failure(
                f"{field_name} must be one of {allowed}, got {value}"
            )
        return ValidationResult.success()
    
    @staticmethod
    def type_check(
        value: Any, 
        expected_type: type, 
        field_name: str
    ) -> ValidationResult:
        """Validate value is of expected type."""
        if not isinstance(value, expected_type):
            return ValidationResult.failure(
                f"{field_name} must be of type {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )
        return ValidationResult.success()


class CoordinateValidator:
    """Validates screen/UI coordinates."""
    
    @staticmethod
    def screen_position(
        x: float, 
        y: float, 
        screen_width: float, 
        screen_height: float
    ) -> ValidationResult:
        """
        Validate screen coordinates are within bounds.
        
        Args:
            x: X coordinate
            y: Y coordinate
            screen_width: Screen width
            screen_height: Screen height
            
        Returns:
            ValidationResult
        """
        result = ValidationResult.success()
        result.merge(Validator.range(x, 0, screen_width, "x"))
        result.merge(Validator.range(y, 0, screen_height, "y"))
        return result
    
    @staticmethod
    def normalized_position(x: float, y: float) -> ValidationResult:
        """Validate normalized coordinates (0.0 - 1.0)."""
        result = ValidationResult.success()
        result.merge(Validator.range(x, 0.0, 1.0, "x"))
        result.merge(Validator.range(y, 0.0, 1.0, "y"))
        return result
    
    @staticmethod
    def point_tuple(point: tuple[float, float], field_name: str = "point") -> ValidationResult:
        """Validate a point tuple (x, y)."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(point, tuple, field_name))
        if isinstance(point, tuple) and len(point) != 2:
            result.add_error(f"{field_name} must have exactly 2 elements")
        elif isinstance(point, tuple):
            result.merge(Validator.type_check(point[0], (int, float), f"{field_name}[0]"))
            result.merge(Validator.type_check(point[1], (int, float), f"{field_name}[1]"))
        return result


class ActionValidator:
    """Validates automation action parameters."""
    
    VALID_ACTIONS = {
        "click", "double_click", "right_click", "middle_click",
        "hover", "move_to", "drag", "drop",
        "type", "press_key", "press_keys",
        "scroll", "scroll_to", "swipe", "fling",
        "screenshot", "wait", "execute_script"
    }
    
    VALID_KEYS = {
        "enter", "return", "tab", "escape", "esc", "space",
        "backspace", "delete", "del",
        "arrow_up", "arrow_down", "arrow_left", "arrow_right",
        "page_up", "page_down", "home", "end",
        "ctrl", "alt", "shift", "meta", "command",
        "f1", "f2", "f3", "f4", "f5", "f6",
        "f7", "f8", "f9", "f10", "f11", "f12"
    }
    
    @classmethod
    def action_type(cls, action: str) -> ValidationResult:
        """Validate action type."""
        return Validator.one_of(action, list(cls.VALID_ACTIONS), "action")
    
    @classmethod
    def key_name(cls, key: str) -> ValidationResult:
        """Validate key name."""
        key_lower = key.lower()
        return Validator.one_of(key_lower, list(cls.VALID_KEYS), "key")
    
    @classmethod
    def click_count(cls, count: int) -> ValidationResult:
        """Validate click count."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(count, int, "click_count"))
        result.merge(Validator.range(count, 1, 10, "click_count"))
        return result
    
    @classmethod
    def duration(cls, duration_ms: float) -> ValidationResult:
        """Validate duration in milliseconds."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(duration_ms, (int, float), "duration"))
        result.merge(Validator.min_value(duration_ms, 0, "duration"))
        result.merge(Validator.max_value(duration_ms, 60000, "duration"))
        return result
    
    @classmethod
    def text_input(cls, text: str, max_length: int = 10000) -> ValidationResult:
        """Validate text input."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(text, str, "text"))
        result.merge(Validator.length(text, 0, max_length, "text"))
        return result


class ConfigValidator:
    """Validates configuration parameters."""
    
    @staticmethod
    def timeout(timeout: float) -> ValidationResult:
        """Validate timeout value."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(timeout, (int, float), "timeout"))
        result.merge(Validator.min_value(timeout, 0, "timeout"))
        result.merge(Validator.max_value(timeout, 300, "timeout"))
        return result
    
    @staticmethod
    def retry_count(count: int) -> ValidationResult:
        """Validate retry count."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(count, int, "retry_count"))
        result.merge(Validator.range(count, 0, 10, "retry_count"))
        return result
    
    @staticmethod
    def polling_interval(interval: float) -> ValidationResult:
        """Validate polling interval."""
        result = ValidationResult.success()
        result.merge(Validator.type_check(interval, (int, float), "polling_interval"))
        result.merge(Validator.range(interval, 0.01, 60, "polling_interval"))
        return result


class ValidationChain:
    """
    Chain multiple validations together.
    
    Example:
        result = (ValidationChain()
            .add(Validator.required(value, "field"))
            .add(Validator.range(value, 0, 100, "field"))
            .validate())
    """
    
    def __init__(self):
        self._results: list[ValidationResult] = []
    
    def add(self, result: ValidationResult) -> 'ValidationChain':
        """Add a validation result to the chain."""
        self._results.append(result)
        return self
    
    def validate(self) -> ValidationResult:
        """Execute all validations and return combined result."""
        combined = ValidationResult.success()
        for result in self._results:
            combined.merge(result)
        return combined
