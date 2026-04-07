"""Base action classes for RabAI AutoClick.

Provides the abstract base class for all actions and the ActionResult
dataclass for standardized return values from action execution.
"""

from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union
from dataclasses import dataclass, field
import time
import logging

from .exceptions import RetryExhaustedError


logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])


def retry_on_failure(
    max_retries: int = 3,
    delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable[[F], F]:
    """Decorator to retry a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exception types to catch and retry.

    Returns:
        Decorated function with retry logic.
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for "
                        f"{func.__name__}: {e}. Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

            raise RetryExhaustedError(
                operation=func.__name__,
                max_retries=max_retries,
                last_cause=last_exception
            )
        return wrapper  # type: ignore
    return decorator


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
    next_step_id: Optional[int] = None
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

    def validate_coords(
        self,
        x: Any,
        y: Any,
        allow_none: bool = True
    ) -> Tuple[bool, str]:
        """Validate x and y coordinate parameters.

        Args:
            x: X coordinate value.
            y: Y coordinate value.
            allow_none: Whether None values are acceptable.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if x is None and y is None:
            if allow_none:
                return True, ""
            return False, "Coordinates cannot both be None"

        if x is not None:
            valid, msg = self.validate_type(x, (int, float), 'x')
            if not valid:
                return False, msg

        if y is not None:
            valid, msg = self.validate_type(y, (int, float), 'y')
            if not valid:
                return False, msg

        return True, ""

    def validate_positive(
        self,
        value: Union[int, float],
        param_name: str,
        allow_zero: bool = False
    ) -> Tuple[bool, str]:
        """Validate a numeric parameter is positive.

        Args:
            value: Value to validate.
            param_name: Name of the parameter.
            allow_zero: Whether zero is acceptable.

        Returns:
            Tuple of (is_valid, error_message).
        """
        valid, msg = self.validate_type(value, (int, float), param_name)
        if not valid:
            return False, msg

        if allow_zero:
            if value < 0:
                return False, f"Parameter '{param_name}' must be >= 0, got {value}"
        else:
            if value <= 0:
                return False, f"Parameter '{param_name}' must be > 0, got {value}"
        return True, ""

    def validate_string_not_empty(
        self,
        value: Any,
        param_name: str
    ) -> Tuple[bool, str]:
        """Validate a string parameter is not empty.

        Args:
            value: Value to validate.
            param_name: Name of the parameter.

        Returns:
            Tuple of (is_valid, error_message).
        """
        valid, msg = self.validate_type(value, str, param_name)
        if not valid:
            return False, msg
        if not value:
            return False, f"Parameter '{param_name}' cannot be empty"
        return True, ""

    def get_full_params(self) -> Dict[str, Any]:
        """Get all parameters including defaults.

        Returns:
            Dictionary with all required params filled with defaults.
        """
        full_params = self.get_optional_params().copy()
        full_params.update(self.params)
        return full_params

    def handle_error(
        self,
        error: Exception,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle an error that occurred during execution.

        This method can be overridden by subclasses to implement
        custom error recovery strategies.

        Args:
            error: The exception that occurred.
            context: Execution context.
            params: Parameters that were passed to execute.

        Returns:
            ActionResult representing the error handling outcome.
        """
        logger.error(
            f"Action '{self.action_type}' failed: {error}",
            exc_info=True
        )
        return ActionResult(
            success=False,
            message=f"执行失败: {str(error)}"
        )

    def on_success(
        self,
        context: Any,
        params: Dict[str, Any],
        result: ActionResult
    ) -> ActionResult:
        """Hook called after successful execution.

        This method can be overridden by subclasses to perform
        post-execution tasks like cleanup or notification.

        Args:
            context: Execution context.
            params: Parameters that were passed to execute.
            result: The successful ActionResult.

        Returns:
            The ActionResult (possibly modified).
        """
        return result

    def on_failure(
        self,
        context: Any,
        params: Dict[str, Any],
        result: ActionResult
    ) -> ActionResult:
        """Hook called after failed execution.

        This method can be overridden by subclasses to perform
        error recovery or cleanup tasks.

        Args:
            context: Execution context.
            params: Parameters that were passed to execute.
            result: The failed ActionResult.

        Returns:
            The ActionResult (possibly modified).
        """
        return result

    def before_execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> None:
        """Hook called before execution.

        This method can be overridden by subclasses to perform
        pre-execution setup tasks.

        Args:
            context: Execution context.
            params: Parameters that were passed to execute.
        """
        pass

    def safe_execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Safely execute the action with error handling hooks.

        This method wraps execute() with before/after hooks
        and error handling.

        Args:
            context: Execution context.
            params: Parameters for the action.

        Returns:
            ActionResult from execution or error handling.
        """
        try:
            self.before_execute(context, params)

            result = self.execute(context, params)

            if result.success:
                return self.on_success(context, params, result)
            else:
                return self.on_failure(context, params, result)

        except Exception as e:
            error_result = self.handle_error(e, context, params)
            return self.on_failure(context, params, error_result)
