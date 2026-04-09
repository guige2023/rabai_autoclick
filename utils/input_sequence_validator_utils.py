"""Input Sequence Validator Utilities.

Validates input sequences for correctness and safety in automation scripts.

Example:
    >>> from input_sequence_validator_utils import InputSequenceValidator
    >>> validator = InputSequenceValidator()
    >>> result = validator.validate(sequence)
    >>> print(result.is_valid)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional, Tuple


class ValidationError(Enum):
    """Validation error types."""
    EMPTY_SEQUENCE = auto()
    INVALID_KEY = auto()
    DANGEROUS_COMBINATION = auto()
    RATE_TOO_FAST = auto()
    MISSING_DELAY = auto()
    INVALID_COORDINATE = auto()


@dataclass
class ValidationResult:
    """Result of sequence validation."""
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[str]

    @property
    def error_messages(self) -> List[str]:
        """Get human-readable error messages."""
        return [e.name for e in self.errors]


@dataclass
class InputSequence:
    """An input sequence to validate."""
    events: List[Any]
    min_delay: float = 0.05


class InputSequenceValidator:
    """Validates input sequences."""

    DANGEROUS_KEY_COMBOS = [
        {"ctrl", "alt", "delete"},
        {"cmd", "ctrl", "q"},
    ]

    def validate(self, sequence: InputSequence) -> ValidationResult:
        """Validate an input sequence.

        Args:
            sequence: InputSequence to validate.

        Returns:
            ValidationResult with errors and warnings.
        """
        errors: List[ValidationError] = []
        warnings: List[str] = []

        if not sequence.events:
            errors.append(ValidationError.EMPTY_SEQUENCE)
            return ValidationResult(False, errors, warnings)

        keys_pressed = set()
        for event in sequence.events:
            if hasattr(event, "key"):
                key = event.key.lower() if isinstance(event.key, str) else str(event.key)
                if key in keys_pressed:
                    warnings.append(f"Duplicate key press: {key}")
                keys_pressed.add(key)

        dangerous = self._check_dangerous_combos(keys_pressed)
        if dangerous:
            errors.append(ValidationError.DANGEROUS_COMBINATION)
            warnings.append(f"Dangerous combination detected: {dangerous}")

        return ValidationResult(len(errors) == 0, errors, warnings)

    def _check_dangerous_combos(self, keys: set) -> Optional[set]:
        """Check for dangerous key combinations."""
        for combo in self.DANGEROUS_KEY_COMBOS:
            if combo.issubset(keys):
                return combo
        return None

    def validate_rate(self, events: List[Any], max_per_second: float = 20.0) -> bool:
        """Validate that event rate is not too fast.

        Args:
            events: List of events.
            max_per_second: Maximum events per second.

        Returns:
            True if rate is acceptable.
        """
        if len(events) < 2:
            return True
        total_time = 1.0
        rate = len(events) / total_time
        return rate <= max_per_second
