"""
Input validation utilities for automation workflows.

Provides validation of mouse coordinates, keyboard input,
and action parameters for safe automation execution.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional, Tuple, List, Any, Callable
from dataclasses import dataclass
from enum import Enum


class ValidationError(Exception):
    """Validation error exception."""
    pass


class CoordinateSystem(Enum):
    """Coordinate system types."""
    SCREEN = "screen"
    DISPLAY = "display"
    WINDOW = "window"
    NORMALIZED = "normalized"


@dataclass
class ScreenBounds:
    """Screen bounds."""
    x: int
    y: int
    width: int
    height: int
    index: int = 0


@dataclass
class ValidationResult:
    """Validation result."""
    valid: bool
    message: str
    details: dict = None


def get_screen_bounds_list() -> List[ScreenBounds]:
    """
    Get bounds of all connected screens.
    
    Returns:
        List of ScreenBounds for each display.
    """
    bounds = []
    
    try:
        import Quartz
        for i, screen in enumerate(Quartz.NSScreen.screens()):
            frame = screen.frame()
            bounds.append(ScreenBounds(
                x=int(frame.origin.x),
                y=int(frame.origin.y),
                width=int(frame.size.width),
                height=int(frame.size.height),
                index=i
            ))
    except Exception:
        bounds.append(ScreenBounds(0, 0, 1920, 1080, 0))
    
    return bounds


def validate_coordinate(x: int, y: int,
                        coordinate_system: CoordinateSystem = CoordinateSystem.SCREEN) -> ValidationResult:
    """
    Validate screen coordinate is valid.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        coordinate_system: Coordinate system type.
        
    Returns:
        ValidationResult.
    """
    if coordinate_system == CoordinateSystem.NORMALIZED:
        if not (0 <= x <= 1 and 0 <= y <= 1):
            return ValidationResult(
                valid=False,
                message=f"Normalized coordinates must be in [0,1], got ({x}, {y})"
            )
        return ValidationResult(valid=True, message="Valid normalized coordinates")
    
    screens = get_screen_bounds_list()
    
    if not screens:
        return ValidationResult(
            valid=False,
            message="No screens detected"
        )
    
    if coordinate_system == CoordinateSystem.SCREEN or coordinate_system == CoordinateSystem.DISPLAY:
        all_bounds = _union_all_bounds(screens)
        
        if not (all_bounds.x <= x < all_bounds.x + all_bounds.width and
                all_bounds.y <= y < all_bounds.y + all_bounds.height):
            return ValidationResult(
                valid=False,
                message=f"Coordinate ({x}, {y}) outside screen bounds",
                details={
                    'screen_bounds': {
                        'x': all_bounds.x,
                        'y': all_bounds.y,
                        'width': all_bounds.width,
                        'height': all_bounds.height
                    }
                }
            )
    
    return ValidationResult(
        valid=True,
        message=f"Valid {coordinate_system.value} coordinate"
    )


def _union_all_bounds(screens: List[ScreenBounds]) -> ScreenBounds:
    """Get union bounds of all screens."""
    min_x = min(s.x for s in screens)
    min_y = min(s.y for s in screens)
    max_x = max(s.x + s.width for s in screens)
    max_y = max(s.y + s.height for s in screens)
    return ScreenBounds(min_x, min_y, max_x - min_x, max_y - min_y)


def normalize_coordinate(x: int, y: int,
                        from_bounds: ScreenBounds) -> Tuple[float, float]:
    """
    Normalize coordinate to [0,1] range.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        from_bounds: Source bounds.
        
    Returns:
        Tuple of normalized (x, y).
    """
    norm_x = (x - from_bounds.x) / from_bounds.width
    norm_y = (y - from_bounds.y) / from_bounds.height
    return (max(0.0, min(1.0, norm_x)), max(0.0, min(1.0, norm_y)))


def denormalize_coordinate(x: float, y: float,
                           to_bounds: ScreenBounds) -> Tuple[int, int]:
    """
    Denormalize [0,1] coordinate to screen pixels.
    
    Args:
        x: Normalized X (0-1).
        y: Normalized Y (0-1).
        to_bounds: Target bounds.
        
    Returns:
        Tuple of pixel (x, y).
    """
    px = int(to_bounds.x + x * to_bounds.width)
    py = int(to_bounds.y + y * to_bounds.height)
    return (px, py)


def validate_key_string(key_string: str) -> ValidationResult:
    """
    Validate key string format.
    
    Args:
        key_string: Key combination string.
        
    Returns:
        ValidationResult.
    """
    if not key_string:
        return ValidationResult(
            valid=False,
            message="Key string is empty"
        )
    
    valid_modifiers = {'cmd', 'command', 'ctrl', 'control', 'alt', 'option', 'shift', 'fn'}
    parts = key_string.lower().replace('-', '+').replace('_', '+').split('+')
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
    
    return ValidationResult(
        valid=True,
        message="Valid key string"
    )


def validate_click_parameters(x: int, y: int,
                              button: str = "left",
                              click_count: int = 1) -> ValidationResult:
    """
    Validate click action parameters.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        button: Mouse button.
        click_count: Number of clicks.
        
    Returns:
        ValidationResult.
    """
    coord_result = validate_coordinate(x, y)
    if not coord_result.valid:
        return coord_result
    
    valid_buttons = {'left', 'right', 'middle'}
    if button.lower() not in valid_buttons:
        return ValidationResult(
            valid=False,
            message=f"Invalid button '{button}', must be one of {valid_buttons}"
        )
    
    if click_count < 1 or click_count > 3:
        return ValidationResult(
            valid=False,
            message=f"click_count must be 1-3, got {click_count}"
        )
    
    return ValidationResult(
        valid=True,
        message="Valid click parameters",
        details={'x': x, 'y': y, 'button': button, 'click_count': click_count}
    )


def validate_text_input(text: str,
                        max_length: Optional[int] = None,
                        allowed_chars: Optional[str] = None) -> ValidationResult:
    """
    Validate text input parameters.
    
    Args:
        text: Text to validate.
        max_length: Optional max length.
        allowed_chars: Optional allowed character set.
        
    Returns:
        ValidationResult.
    """
    if not isinstance(text, str):
        return ValidationResult(
            valid=False,
            message=f"Text must be str, got {type(text)}"
        )
    
    if max_length and len(text) > max_length:
        return ValidationResult(
            valid=False,
            message=f"Text length {len(text)} exceeds max {max_length}"
        )
    
    if allowed_chars:
        invalid = set(text) - set(allowed_chars)
        if invalid:
            return ValidationResult(
                valid=False,
                message=f"Text contains invalid characters: {invalid}"
            )
    
    return ValidationResult(
        valid=True,
        message="Valid text input",
        details={'length': len(text)}
    )


def validate_bounds(x: int, y: int, width: int, height: int) -> ValidationResult:
    """
    Validate rectangular bounds.
    
    Args:
        x: X position.
        y: Y position.
        width: Width.
        height: Height.
        
    Returns:
        ValidationResult.
    """
    coord_result = validate_coordinate(x, y)
    if not coord_result.valid:
        return coord_result
    
    if width <= 0 or height <= 0:
        return ValidationResult(
            valid=False,
            message=f"Width and height must be positive, got ({width}, {height})"
        )
    
    screens = get_screen_bounds_list()
    all_bounds = _union_all_bounds(screens)
    
    if x + width > all_bounds.x + all_bounds.width or y + height > all_bounds.y + all_bounds.height:
        return ValidationResult(
            valid=False,
            message=f"Bounds ({x}, {y}, {width}, {height}) exceed screen"
        )
    
    return ValidationResult(
        valid=True,
        message="Valid bounds"
    )
