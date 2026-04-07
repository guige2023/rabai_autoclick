"""Input validation utilities for RabAI AutoClick.

Provides validation functions for:
- Workflow configurations
- Action parameters
- File paths
- Screen coordinates
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Tuple


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


class Severity(Enum):
    """Validation severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]

    def __bool__(self) -> bool:
        return self.is_valid


def validate_workflow_config(config: dict) -> ValidationResult:
    """Validate a workflow configuration.

    Args:
        config: Workflow configuration dictionary.

    Returns:
        ValidationResult with any errors or warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []

    # Required fields
    if "workflow_id" not in config:
        errors.append("Missing required field: workflow_id")
    elif not isinstance(config["workflow_id"], str):
        errors.append("workflow_id must be a string")
    elif not config["workflow_id"].strip():
        errors.append("workflow_id cannot be empty")

    if "name" not in config:
        errors.append("Missing required field: name")
    elif not isinstance(config["name"], str):
        errors.append("name must be a string")

    # Optional fields validation
    if "steps" in config:
        if not isinstance(config["steps"], list):
            errors.append("steps must be a list")
        else:
            for i, step in enumerate(config["steps"]):
                step_result = validate_step(step, i)
                errors.extend(step_result.errors)
                warnings.extend(step_result.warnings)

    if "timeout" in config:
        if not isinstance(config["timeout"], (int, float)):
            errors.append("timeout must be a number")
        elif config["timeout"] <= 0:
            errors.append("timeout must be positive")

    if "retry" in config:
        if not isinstance(config["retry"], int):
            errors.append("retry must be an integer")
        elif config["retry"] < 0:
            errors.append("retry cannot be negative")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_step(step: dict, index: int) -> ValidationResult:
    """Validate a workflow step.

    Args:
        step: Step configuration dictionary.
        index: Step index for error messages.

    Returns:
        ValidationResult with any errors or warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []
    prefix = f"steps[{index}]"

    if "action" not in step:
        errors.append(f"{prefix}: Missing required field: action")
    elif not isinstance(step["action"], str):
        errors.append(f"{prefix}: action must be a string")

    if "target" in step and not isinstance(step["target"], str):
        errors.append(f"{prefix}: target must be a string")

    if "delay" in step:
        if not isinstance(step["delay"], (int, float)):
            errors.append(f"{prefix}: delay must be a number")
        elif step["delay"] < 0:
            errors.append(f"{prefix}: delay cannot be negative")

    if "condition" in step:
        if not isinstance(step["condition"], dict):
            errors.append(f"{prefix}: condition must be a dict")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_coordinates(x: Any, y: Any) -> Tuple[bool, Optional[str]]:
    """Validate screen coordinates.

    Args:
        x: X coordinate.
        y: Y coordinate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        x_val = int(x)
        y_val = int(y)
    except (TypeError, ValueError):
        return False, f"Coordinates must be numbers, got x={x}, y={y}"

    if x_val < 0 or y_val < 0:
        return False, f"Coordinates cannot be negative, got x={x_val}, y={y_val}"

    # Practical limits for screen coordinates
    if x_val > 10000 or y_val > 10000:
        return False, f"Coordinates too large, got x={x_val}, y={y_val}"

    return True, None


def validate_file_path(
    path: str,
    must_exist: bool = False,
    extensions: Optional[List[str]] = None,
) -> ValidationResult:
    """Validate a file path.

    Args:
        path: File path to validate.
        must_exist: If True, file must exist.
        extensions: List of allowed extensions (e.g., ['.json', '.png']).

    Returns:
        ValidationResult with any errors or warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not isinstance(path, str):
        errors.append(f"Path must be a string, got {type(path)}")
        return ValidationResult(False, errors, warnings)

    if not path.strip():
        errors.append("Path cannot be empty")
        return ValidationResult(False, errors, warnings)

    p = Path(path)

    if must_exist and not p.exists():
        errors.append(f"File does not exist: {path}")
    elif must_exist and not p.is_file():
        errors.append(f"Path is not a file: {path}")

    if extensions:
        ext = p.suffix.lower()
        if ext not in extensions:
            errors.append(
                f"Invalid file extension '{ext}'. "
                f"Expected one of: {', '.join(extensions)}"
            )

    # Check for path traversal attempts
    if ".." in p.parts:
        errors.append("Path contains invalid traversal sequence '..'")

    # Check for dangerous characters
    if "\x00" in path:
        errors.append("Path contains null character")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_action_params(
    action_type: str,
    params: dict,
) -> ValidationResult:
    """Validate action parameters.

    Args:
        action_type: Type of action (click, type, press, etc.).
        params: Action parameters dictionary.

    Returns:
        ValidationResult with any errors or warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []

    # Common parameter validations
    if "x" in params or "y" in params:
        x = params.get("x", 0)
        y = params.get("y", 0)
        valid, msg = validate_coordinates(x, y)
        if not valid:
            errors.append(f"Invalid coordinates: {msg}")

    if "text" in params:
        if not isinstance(params["text"], str):
            errors.append("Parameter 'text' must be a string")
        elif len(params["text"]) > 10000:
            warnings.append("Parameter 'text' is very long")

    if "interval" in params:
        interval = params["interval"]
        if not isinstance(interval, (int, float)):
            errors.append("Parameter 'interval' must be a number")
        elif interval < 0:
            errors.append("Parameter 'interval' cannot be negative")

    # Action-specific validations
    if action_type == "click":
        if "button" in params:
            if params["button"] not in ("left", "right", "middle"):
                errors.append(
                    f"Invalid button value: {params['button']}. "
                    "Must be one of: left, right, middle"
                )
        if "clicks" in params:
            if not isinstance(params["clicks"], int):
                errors.append("Parameter 'clicks' must be an integer")
            elif params["clicks"] < 1 or params["clicks"] > 10:
                errors.append("Parameter 'clicks' must be between 1 and 10")

    elif action_type == "type":
        if "text" not in params:
            errors.append("Action 'type' requires 'text' parameter")

    elif action_type == "press":
        if "key" not in params:
            errors.append("Action 'press' requires 'key' parameter")

    elif action_type == "wait":
        if "duration" not in params:
            errors.append("Action 'wait' requires 'duration' parameter")
        elif params["duration"] < 0:
            errors.append("Wait duration cannot be negative")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_screen_region(
    region: Optional[List[int]],
) -> ValidationResult:
    """Validate a screen region [x, y, width, height].

    Args:
        region: Screen region as [x, y, width, height].

    Returns:
        ValidationResult with any errors or warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []

    if region is None:
        return ValidationResult(True, [], [])

    if not isinstance(region, (list, tuple)):
        errors.append(f"Region must be a list or tuple, got {type(region)}")
        return ValidationResult(False, errors, warnings)

    if len(region) != 4:
        errors.append(f"Region must have 4 elements, got {len(region)}")
        return ValidationResult(False, errors, warnings)

    x, y, w, h = region

    valid_xy, msg = validate_coordinates(x, y)
    if not valid_xy:
        errors.append(f"Invalid region position: {msg}")

    if not isinstance(w, (int, float)) or not isinstance(h, (int, float)):
        errors.append("Region width and height must be numbers")
    elif w <= 0 or h <= 0:
        errors.append("Region width and height must be positive")
    elif w > 10000 or h > 10000:
        errors.append("Region dimensions too large")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def sanitize_string(
    value: str,
    max_length: int = 1000,
    strip_whitespace: bool = True,
) -> str:
    """Sanitize a string value.

    Args:
        value: String to sanitize.
        max_length: Maximum allowed length.
        strip_whitespace: Whether to strip leading/trailing whitespace.

    Returns:
        Sanitized string.
    """
    if not isinstance(value, str):
        return str(value)

    if strip_whitespace:
        value = value.strip()

    if len(value) > max_length:
        value = value[:max_length]

    # Remove null characters
    value = value.replace("\x00", "")

    return value


def validate_json_serializable(
    obj: Any,
    path: str = "root",
) -> Tuple[bool, List[str]]:
    """Check if an object is JSON serializable.

    Args:
        obj: Object to check.
        path: Current path for error messages.

    Returns:
        Tuple of (is_serializable, list_of_error_messages).
    """
    errors: List[str] = []

    if obj is None or isinstance(obj, (bool, int, float, str)):
        return True, []

    if isinstance(obj, dict):
        for key, value in obj.items():
            if not isinstance(key, str):
                errors.append(f"{path}: dict key must be string, got {type(key)}")
            is_valid, errs = validate_json_serializable(value, f"{path}.{key}")
            errors.extend(errs)
    elif isinstance(obj, (list, tuple)):
        for i, item in enumerate(obj):
            is_valid, errs = validate_json_serializable(item, f"{path}[{i}]")
            errors.extend(errs)
    else:
        errors.append(f"{path}: type {type(obj)} is not JSON serializable")

    return len(errors) == 0, errors