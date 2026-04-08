"""
Input Guard Utilities

Provides utilities for guarding input operations
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class GuardResult:
    """Result of a guard check."""
    allowed: bool
    message: str = ""


class InputGuard:
    """
    Guards input operations with validation.
    
    Prevents invalid inputs from reaching
    target elements or functions.
    """

    def __init__(self) -> None:
        self._validators: list[Callable[[Any], GuardResult]] = []

    def add_validator(
        self,
        validator: Callable[[Any], GuardResult],
    ) -> None:
        """Add a validation function."""
        self._validators.append(validator)

    def validate(self, value: Any) -> GuardResult:
        """
        Validate an input value.
        
        Args:
            value: Value to validate.
            
        Returns:
            GuardResult with validation outcome.
        """
        for validator in self._validators:
            result = validator(value)
            if not result.allowed:
                return result
        return GuardResult(allowed=True)

    def guard(
        self,
        value: Any,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any | None:
        """
        Guard a function call with validation.
        
        Args:
            value: Value to validate.
            func: Function to call if valid.
            *args, **kwargs: Arguments for function.
            
        Returns:
            Function result or None if blocked.
        """
        result = self.validate(value)
        if result.allowed:
            return func(*args, **kwargs)
        return None


def create_range_guard(
    min_val: float,
    max_val: float,
) -> Callable[[Any], GuardResult]:
    """Create a range validator."""
    def validator(value: Any) -> GuardResult:
        try:
            num = float(value)
            if min_val <= num <= max_val:
                return GuardResult(allowed=True)
            return GuardResult(allowed=False, message=f"Value {num} out of range")
        except (ValueError, TypeError):
            return GuardResult(allowed=False, message="Invalid numeric value")
    return validator
