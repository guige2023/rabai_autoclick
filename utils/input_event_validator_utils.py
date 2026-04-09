"""
Input Event Validator Utilities

Validate input events for correctness, sanity, and
adherence to expected event schemas before processing.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Set, Any


@dataclass
class ValidationResult:
    """Result of input event validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


class InputEventValidator:
    """
    Validate input events against expected schemas and constraints.

    Rejects malformed events early in the pipeline to prevent
    downstream errors.
    """

    def __init__(
        self,
        allowed_event_types: Optional[Set[str]] = None,
        allowed_actions: Optional[Set[str]] = None,
        x_min: float = -10000.0,
        x_max: float = 10000.0,
        y_min: float = -10000.0,
        y_max: float = 10000.0,
        max_timestamp_age_ms: float = 10000.0,
    ):
        self.allowed_event_types = allowed_event_types or {
            "mouse", "keyboard", "touch", "trackpad"
        }
        self.allowed_actions = allowed_actions or {
            "down", "up", "move", "press", "release", "scroll", "swipe", "pinch"
        }
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.max_timestamp_age_ms = max_timestamp_age_ms

    def validate_event(
        self,
        event_type: str,
        action: str,
        x: float,
        y: float,
        timestamp_ms: float,
        current_time_ms: Optional[float] = None,
    ) -> ValidationResult:
        """
        Validate a single input event.

        Args:
            event_type: Type of event ('mouse', 'keyboard', 'touch', etc.).
            action: Action type ('down', 'up', 'move', etc.).
            x, y: Event coordinates.
            timestamp_ms: Event timestamp.
            current_time_ms: Current time (for age check). Defaults to now.

        Returns:
            ValidationResult with any errors or warnings.
        """
        errors: List[str] = []
        warnings: List[str] = []

        # Check event type
        if event_type not in self.allowed_event_types:
            errors.append(f"Unknown event type: {event_type}")

        # Check action
        if action not in self.allowed_actions:
            errors.append(f"Unknown action: {action}")

        # Check coordinates
        if not (self.x_min <= x <= self.x_max):
            errors.append(f"X coordinate out of range: {x}")
        if not (self.y_min <= y <= self.y_max):
            errors.append(f"Y coordinate out of range: {y}")

        # Check timestamp age
        if current_time_ms is not None:
            age_ms = current_time_ms - timestamp_ms
            if age_ms > self.max_timestamp_age_ms:
                warnings.append(f"Event timestamp is {age_ms:.0f}ms old")
            if age_ms < 0:
                warnings.append("Event timestamp is in the future")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def validate_batch(
        self,
        events: List[dict],
    ) -> List[ValidationResult]:
        """Validate a batch of events."""
        import time
        now = time.time() * 1000
        return [
            self.validate_event(
                event_type=e.get("event_type", ""),
                action=e.get("action", ""),
                x=e.get("x", 0),
                y=e.get("y", 0),
                timestamp_ms=e.get("timestamp_ms", now),
                current_time_ms=now,
            )
            for e in events
        ]
