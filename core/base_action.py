"""Base action classes for RabAI AutoClick.

Provides the abstract base class for all actions and the ActionResult
dataclass for standardized return values from action execution.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    Protocol,
    TypedDict,
    runtime_checkable,
)
from dataclasses import dataclass


# Type variable for generic BaseAction
T = TypeVar("T", bound="ActionResult")


# --- Protocol for Context ---

@runtime_checkable
class ContextProtocol(Protocol):
    """Protocol defining the required interface for action context.
    
    Actions receive a context object that must support these methods
    to interact with variables, state, and resolved values.
    """
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from context."""
        ...
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in context."""
        ...
    
    def delete(self, key: str) -> None:
        """Delete a value from context."""
        ...
    
    def set_all(self, data: Dict[str, Any]) -> None:
        """Set multiple values in context."""
        ...
    
    def resolve_value(self, value: Any) -> Any:
        """Resolve a value (e.g., variable references)."""
        ...
    
    def get_all(self) -> Dict[str, Any]:
        """Get all context data."""
        ...


# --- TypedDicts for Action Parameters ---

class ClickParams(TypedDict, total=False):
    """Parameters for ClickAction."""
    x: int
    y: int
    button: str
    clicks: int
    duration: float


class TypeParams(TypedDict, total=False):
    """Parameters for TypeAction."""
    text: str
    interval: float
    enter: bool


class DelayParams(TypedDict, total=False):
    """Parameters for DelayAction."""
    duration: float


class ScrollParams(TypedDict, total=False):
    """Parameters for ScrollAction."""
    x: int
    y: int
    direction: str
    amount: int


class MoveParams(TypedDict, total=False):
    """Parameters for MoveAction."""
    x: int
    y: int
    duration: float


class ScreenshotParams(TypedDict, total=False):
    """Parameters for ScreenshotAction."""
    path: str


class PressKeyParams(TypedDict, total=False):
    """Parameters for PressKeyAction."""
    key: str


# --- Validation Helpers ---

def validate_string(value: Any, param_name: str) -> str:
    """Validate that a value is a string.
    
    Args:
        value: The value to validate.
        param_name: Name of the parameter for error messages.
        
    Returns:
        The validated string value.
        
    Raises:
        ValueError: If value is not a string.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"Parameter '{param_name}' must be a string, got {type(value).__name__}"
        )
    return value


def validate_number(
    value: Any, 
    param_name: str,
    allowed_types: Union[type, Tuple[type, ...]] = (int, float)
) -> Union[int, float]:
    """Validate that a value is a number.
    
    Args:
        value: The value to validate.
        param_name: Name of the parameter for error messages.
        allowed_types: Acceptable numeric types (default: int, float).
        
    Returns:
        The validated numeric value.
        
    Raises:
        ValueError: If value is not a number.
    """
    if not isinstance(value, allowed_types):
        type_names = ' or '.join(t.__name__ for t in allowed_types)
        raise ValueError(
            f"Parameter '{param_name}' must be a number ({type_names}), "
            f"got {type(value).__name__}"
        )
    return value


def validate_bool(value: Any, param_name: str) -> bool:
    """Validate that a value is a boolean.
    
    Args:
        value: The value to validate.
        param_name: Name of the parameter for error messages.
        
    Returns:
        The validated boolean value.
        
    Raises:
        ValueError: If value is not a boolean.
    """
    if not isinstance(value, bool):
        raise ValueError(
            f"Parameter '{param_name}' must be a boolean, got {type(value).__name__}"
        )
    return value


def validate_list(
    value: Any, 
    param_name: str,
    allowed_types: Union[type, Tuple[type, ...]] = (str, int, float, bool)
) -> List[Any]:
    """Validate that a value is a list with allowed element types.
    
    Args:
        value: The value to validate.
        param_name: Name of the parameter for error messages.
        allowed_types: Acceptable element types (default: str, int, float, bool).
        
    Returns:
        The validated list value.
        
    Raises:
        ValueError: If value is not a list or contains invalid elements.
    """
    if not isinstance(value, list):
        raise ValueError(
            f"Parameter '{param_name}' must be a list, got {type(value).__name__}"
        )
    if allowed_types:
        type_names = ' or '.join(t.__name__ for t in allowed_types)
        for i, item in enumerate(value):
            if not isinstance(item, allowed_types):
                raise ValueError(
                    f"Parameter '{param_name}[{i}]' must be a valid type "
                    f"({type_names}), got {type(item).__name__}"
                )
    return value


# --- ActionResult Dataclass ---

@dataclass
class ActionResult:
    """Result returned by action execution.
    
    Attributes:
        success: Whether the action executed successfully.
        message: Human-readable status message.
        data: Optional output data from the action.
        next_step_id: Optional ID of the next step to execute.
        duration: Time taken to execute the action in seconds.
    """
    success: bool
    message: str = ""
    data: Any = None
    next_step_id: Optional[str] = None
    duration: float = 0.0


class BaseAction(ABC, Generic[T]):
    """Abstract base class for all automation actions.
    
    Subclasses must implement the execute() method and define
    action_type, display_name, and description class attributes.
    
    Type Parameters:
        T: The ActionResult subclass type returned by execute().
    """
    action_type: str = "base"
    display_name: str = "基础动作"
    description: str = "动作基类"
    
    # Supported button values for mouse actions
    VALID_BUTTONS: List[str] = ['left', 'right', 'middle']
    
    # Supported directions for scroll actions
    VALID_DIRECTIONS: List[str] = ['up', 'down']
    
    def __init__(self) -> None:
        self.params: Dict[str, Any] = {}
    
    def set_params(self, params: Dict[str, Any]) -> None:
        """Set the parameters for this action instance."""
        self.params = params
    
    @abstractmethod
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute the action with the given parameters.
        
        Args:
            context: The execution context (ContextManager instance).
            params: Dictionary of parameters for the action.
            
        Returns:
            ActionResult indicating success or failure.
        """
        pass
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate action parameters before execution.
        
        Default implementation checks required params.
        Override in subclasses for custom validation.
        
        Returns:
            Tuple of (is_valid, error_message).
        """
        required = self.get_required_params()
        missing = [p for p in required if p not in params or params[p] is None]
        if missing:
            return False, f"Missing required parameters: {', '.join(missing)}"
        return True, ""
    
    def validate_type(
        self, 
        value: Any, 
        expected_type: Union[type, Tuple[type, ...]], 
        param_name: str
    ) -> Tuple[bool, str]:
        """Validate a parameter's type.
        
        Args:
            value: The value to validate.
            expected_type: Expected type or tuple of types.
            param_name: Name of the parameter for error messages.
            
        Returns:
            Tuple of (is_valid, error_message).
        """
        if not isinstance(value, expected_type):
            type_names = (
                expected_type.__name__ 
                if isinstance(expected_type, type) 
                else ' or '.join(t.__name__ for t in expected_type)
            )
            return False, f"Parameter '{param_name}' must be of type {type_names}, got {type(value).__name__}"
        return True, ""
    
    def validate_range(
        self,
        value: Union[int, float],
        min_val: Union[int, float],
        max_val: Union[int, float],
        param_name: str
    ) -> Tuple[bool, str]:
        """Validate a numeric parameter is within range.
        
        Returns:
            Tuple of (is_valid, error_message).
        """
        if not (min_val <= value <= max_val):
            return False, f"Parameter '{param_name}' must be between {min_val} and {max_val}, got {value}"
        return True, ""
    
    def validate_in(
        self,
        value: Any,
        valid_values: List[Any],
        param_name: str
    ) -> Tuple[bool, str]:
        """Validate a parameter is in a list of valid values.
        
        Returns:
            Tuple of (is_valid, error_message).
        """
        if value not in valid_values:
            return False, f"Parameter '{param_name}' must be one of {valid_values}, got {value}"
        return True, ""
    
    def get_param(self, key: str, default: Any = None) -> Any:
        """Get a parameter value with optional default."""
        return self.params.get(key, default)
    
    def get_required_params(self) -> List[str]:
        """List of required parameter names."""
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        """Dict of optional parameters with their default values."""
        return {}
