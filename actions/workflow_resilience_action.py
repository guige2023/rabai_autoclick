"""
Workflow Resilience Action Module.

Provides fault tolerance for workflows: automatic retries,
fallback handlers, bulkheads, and graceful degradation.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ResilienceConfig:
    """Resilience configuration."""
    max_retries: int = 3
    retry_delay_ms: int = 1000
    exponential_backoff: bool = True
    bulkhead_limit: int = 10
    timeout_ms: int = 30000
    fallback_enabled: bool = False


@dataclass
class ResilienceResult:
    """Result of resilient execution."""
    success: bool
    attempts: int
    final_result: Any
    fallback_used: bool
    error: Optional[str] = None


class WorkflowResilienceAction(BaseAction):
    """Fault tolerance for workflow execution."""

    def __init__(self) -> None:
        super().__init__("workflow_resilience")
        self._bulkhead_semaphore = None
        self._fallbacks: dict[str, Callable] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute with resilience patterns.

        Args:
            context: Execution context
            params: Parameters:
                - handler: Function to execute
                - fallback: Fallback function if all retries fail
                - config: ResilienceConfig
                - args: Handler arguments
                - kwargs: Handler keyword arguments

        Returns:
            ResilienceResult
        """
        handler = params.get("handler")
        fallback = params.get("fallback")
        config = params.get("config", {})
        args = params.get("args", [])
        kwargs = params.get("kwargs", {})

        max_retries = config.get("max_retries", 3)
        retry_delay = config.get("retry_delay_ms", 1000) / 1000
        exponential_backoff = config.get("exponential_backoff", True)
        timeout_ms = config.get("timeout_ms", 30000)

        attempts = 0
        last_error = None

        for attempt in range(max_retries):
            attempts += 1
            try:
                import signal

                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Execution timed out after {timeout_ms}ms")

                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(timeout_ms / 1000))

                try:
                    result = handler(*args, **kwargs)
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)

                return ResilienceResult(
                    success=True,
                    attempts=attempts,
                    final_result=result,
                    fallback_used=False
                ).__dict__

            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    delay = retry_delay * (2 ** attempt) if exponential_backoff else retry_delay
                    import time
                    time.sleep(delay)

        fallback_used = False
        final_result = None
        if fallback:
            try:
                final_result = fallback()
                fallback_used = True
            except Exception as e:
                last_error = f"Fallback also failed: {str(e)}"

        return ResilienceResult(
            success=fallback_used,
            attempts=attempts,
            final_result=final_result,
            fallback_used=fallback_used,
            error=last_error
        ).__dict__

    def register_fallback(self, name: str, fallback: Callable) -> None:
        """Register a fallback handler."""
        self._fallbacks[name] = fallback
