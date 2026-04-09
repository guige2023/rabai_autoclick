"""
Input Validation Utilities for UI Automation

Provides validation for coordinates, regions, and input parameters
to ensure safe and correct automation actions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Sequence


@dataclass
class ValidationResult:
    """Result of a validation check."""
    valid: bool
    message: str = ""
    error_type: str = ""


@dataclass
class Coordinate:
    """Validated coordinate."""
    x: float
    y: float


@dataclass
class Region:
    """Validated rectangular region."""
    x: float
    y: float
    width: float
    height: float

    def contains(self, x: float, y: float) -> bool:
        """Check if point is within region."""
        return self.x <= x <= self.x + self.width and self.y <= y <= self.y + self.height

    def intersects(self, other: Region) -> bool:
        """Check if this region intersects with another."""
        return not (
            self.x + self.width < other.x or
            other.x + other.width < self.x or
            self.y + self.height < other.y or
            other.y + other.height < self.y
        )


class InputValidator:
    """
    Validates input parameters for automation actions.

    Ensures coordinates, regions, and other parameters are within
    safe bounds before executing actions.
    """

    def __init__(
        self,
        screen_width: int = 1920,
        screen_height: int = 1080,
        allow_negative: bool = False,
    ) -> None:
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.allow_negative = allow_negative

    def validate_coordinate(
        self,
        x: float,
        y: float,
    ) -> ValidationResult:
        """
        Validate a coordinate pair.

        Args:
            x: X coordinate
            y: Y coordinate

        Returns:
            ValidationResult indicating if coordinates are valid
        """
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            return ValidationResult(
                valid=False,
                message="Coordinates must be numeric",
                error_type="TypeError",
            )

        if not self.allow_negative and (x < 0 or y < 0):
            return ValidationResult(
                valid=False,
                message="Coordinates must be non-negative",
                error_type="ValueError",
            )

        return ValidationResult(valid=True)

    def validate_region(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
    ) -> ValidationResult:
        """
        Validate a rectangular region.

        Args:
            x: X coordinate of top-left corner
            y: Y coordinate of top-left corner
            width: Region width
            height: Region height

        Returns:
            ValidationResult indicating if region is valid
        """
        coord_result = self.validate_coordinate(x, y)
        if not coord_result.valid:
            return coord_result

        if width <= 0 or height <= 0:
            return ValidationResult(
                valid=False,
                message="Width and height must be positive",
                error_type="ValueError",
            )

        if x + width > self.screen_width or y + height > self.screen_height:
            return ValidationResult(
                valid=False,
                message=f"Region exceeds screen bounds ({self.screen_width}x{self.screen_height})",
                error_type="BoundsError",
            )

        return ValidationResult(valid=True)

    def validate_click_action(
        self,
        x: float,
        y: float,
        button: str = "left",
    ) -> ValidationResult:
        """Validate a click action's parameters."""
        coord_result = self.validate_coordinate(x, y)
        if not coord_result.valid:
            return coord_result

        valid_buttons = {"left", "right", "middle"}
        if button.lower() not in valid_buttons:
            return ValidationResult(
                valid=False,
                message=f"Invalid button '{button}'. Must be one of: {valid_buttons}",
                error_type="ValueError",
            )

        return ValidationResult(valid=True)

    def validate_key_combo(
        self,
        keys: Sequence[str],
    ) -> ValidationResult:
        """
        Validate a keyboard shortcut combination.

        Args:
            keys: Sequence of key names (e.g., ['ctrl', 'c'])

        Returns:
            ValidationResult
        """
        if not keys:
            return ValidationResult(
                valid=False,
                message="Key combination cannot be empty",
                error_type="ValueError",
            )

        valid_modifiers = {"ctrl", "alt", "shift", "meta", "cmd", "command"}
        valid_keys = {
            *valid_modifiers,
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
            "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
            "enter", "return", "tab", "escape", "esc", "space", "backspace",
            "delete", "up", "down", "left", "right",
            "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
        }

        for key in keys:
            key_lower = key.lower()
            if key_lower not in valid_keys:
                return ValidationResult(
                    valid=False,
                    message=f"Invalid key '{key}' in combination",
                    error_type="ValueError",
                )

        return ValidationResult(valid=True)

    def validate_scroll_amount(
        self,
        amount: int,
        direction: str,
    ) -> ValidationResult:
        """Validate scroll amount and direction."""
        if not isinstance(amount, int):
            return ValidationResult(
                valid=False,
                message="Scroll amount must be an integer",
                error_type="TypeError",
            )

        valid_directions = {"up", "down", "left", "right"}
        if direction.lower() not in valid_directions:
            return ValidationResult(
                valid=False,
                message=f"Invalid direction '{direction}'",
                error_type="ValueError",
            )

        return ValidationResult(valid=True)

    def validate_text_input(
        self,
        text: str,
        max_length: int = 10000,
    ) -> ValidationResult:
        """Validate text input for type and length."""
        if not isinstance(text, str):
            return ValidationResult(
                valid=False,
                message="Input text must be a string",
                error_type="TypeError",
            )

        if len(text) > max_length:
            return ValidationResult(
                valid=False,
                message=f"Text exceeds maximum length of {max_length}",
                error_type="LengthError",
            )

        return ValidationResult(valid=True)

    def validate_color(
        self,
        color: str | tuple[int, int, int] | tuple[int, int, int, int],
    ) -> ValidationResult:
        """Validate color format (hex or RGB/RGBA tuple)."""
        if isinstance(color, str):
            # Hex format
            hex_pattern = r"^#?([A-Fa-f0-9]{6}|[A-Fa-f0-9]{8})$"
            if not re.match(hex_pattern, color):
                return ValidationResult(
                    valid=False,
                    message="Color must be a valid hex string (e.g., #FF0000)",
                    error_type="ValueError",
                )
        elif isinstance(color, (tuple, list)):
            if len(color) not in {3, 4}:
                return ValidationResult(
                    valid=False,
                    message="Color tuple must have 3 (RGB) or 4 (RGBA) values",
                    error_type="ValueError",
                )
            for component in color:
                if not isinstance(component, (int, float)) or component < 0 or component > 255:
                    return ValidationResult(
                        valid=False,
                        message="Color components must be 0-255",
                        error_type="ValueError",
                    )
        else:
            return ValidationResult(
                valid=False,
                message="Color must be a hex string or RGB/RGBA tuple",
                error_type="TypeError",
            )

        return ValidationResult(valid=True)


def sanitize_text_input(text: str) -> str:
    """
    Sanitize text input by removing problematic characters.

    Args:
        text: Input text to sanitize

    Returns:
        Sanitized text string
    """
    # Remove null bytes
    text = text.replace("\x00", "")
    # Remove other control characters except newline and tab
    text = "".join(c for c in text if c == "\n" or c == "\t" or not (0 <= ord(c) < 32))
    return text.strip()


def normalize_coordinates(
    x: float,
    y: float,
    screen_width: int,
    screen_height: int,
) -> tuple[float, float]:
    """
    Normalize coordinates to screen bounds.

    Args:
        x: X coordinate
        y: Y coordinate
        screen_width: Screen width
        screen_height: Screen height

    Returns:
        Tuple of (normalized_x, normalized_y)
    """
    return (
        max(0, min(x, screen_width - 1)),
        max(0, min(y, screen_height - 1)),
    )
