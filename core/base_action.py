"""Base action classes for RabAI AutoClick.

Provides the abstract base class for all actions and the ActionResult
dataclass for standardized return values from action execution.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field


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


class BaseAction(ABC):
    """Abstract base class for all automation actions.
    
    Subclasses must implement the execute() method and define
    action_type, display_name, and description class attributes.
    """
    action_type: str = "base"
    display_name: str = "基础动作"
    description: str = "动作基类"
    
    # Supported button values for mouse actions
    VALID_BUTTONS: List[str] = field(default_factory=lambda: ['left', 'right', 'middle'])
    
    # Supported directions for scroll actions
    VALID_DIRECTIONS: List[str] = field(default_factory=lambda: ['up', 'down'])
    
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
