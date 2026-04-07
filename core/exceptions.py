"""Custom exceptions for RabAI AutoClick.

Defines a hierarchy of exceptions for better error handling
and debugging across the application.
"""


class RabaiError(Exception):
    """Base exception for all RabAI AutoClick errors.

    All custom exceptions should inherit from this class
    to enable catch-all error handling.
    """

    def __init__(self, message: str = "", cause: Exception | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (caused by: {self.cause})"
        return self.message


class WorkflowError(RabaiError):
    """Exception raised for workflow-related errors.

    Includes loading, saving, parsing, and validation errors.
    """
    pass


class WorkflowLoadError(WorkflowError):
    """Raised when a workflow fails to load."""

    def __init__(self, path: str, cause: Exception | None = None) -> None:
        super().__init__(f"Failed to load workflow from: {path}", cause)
        self.path = path


class WorkflowSaveError(WorkflowError):
    """Raised when a workflow fails to save."""

    def __init__(self, path: str, cause: Exception | None = None) -> None:
        super().__init__(f"Failed to save workflow to: {path}", cause)
        self.path = path


class WorkflowValidationError(WorkflowError):
    """Raised when a workflow fails validation."""

    def __init__(self, message: str, step_id: str | None = None) -> None:
        super().__init__(message)
        self.step_id = step_id


class StepExecutionError(RabaiError):
    """Raised when a workflow step fails to execute."""

    def __init__(
        self,
        step_type: str,
        step_id: str | None = None,
        cause: Exception | None = None
    ) -> None:
        super().__init__(f"Step execution failed: {step_type}", cause)
        self.step_type = step_type
        self.step_id = step_id


class ActionNotFoundError(RabaiError):
    """Raised when a requested action type is not found."""

    def __init__(self, action_type: str) -> None:
        super().__init__(f"Action not found: {action_type}")
        self.action_type = action_type


class ActionValidationError(RabaiError):
    """Raised when action parameters fail validation."""

    def __init__(
        self,
        action_type: str,
        param_name: str,
        message: str
    ) -> None:
        super().__init__(f"Validation failed for '{param_name}' in {action_type}: {message}")
        self.action_type = action_type
        self.param_name = param_name


class ContextError(RabaiError):
    """Raised for context-related errors."""
    pass


class VariableNotFoundError(ContextError):
    """Raised when a requested variable is not found in context."""

    def __init__(self, var_name: str) -> None:
        super().__init__(f"Variable not found: {var_name}")
        self.var_name = var_name


class ExpressionEvaluationError(ContextError):
    """Raised when expression evaluation fails."""

    def __init__(self, expression: str, cause: Exception | None = None) -> None:
        super().__init__(f"Failed to evaluate expression: {expression}", cause)
        self.expression = expression


class RetryExhaustedError(RabaiError):
    """Raised when a retryable operation has exhausted all retry attempts."""

    def __init__(
        self,
        operation: str,
        max_retries: int,
        last_cause: Exception | None = None
    ) -> None:
        super().__init__(
            f"Operation '{operation}' failed after {max_retries} retries",
            last_cause
        )
        self.operation = operation
        self.max_retries = max_retries


class ConfigurationError(RabaiError):
    """Raised for configuration-related errors."""
    pass


class TimeoutError(RabaiError):
    """Raised when an operation times out."""

    def __init__(self, operation: str, timeout: float) -> None:
        super().__init__(f"Operation '{operation}' timed out after {timeout}s")
        self.operation = operation
        self.timeout = timeout