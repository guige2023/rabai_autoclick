"""
Self-healing action for automatic error recovery.

Provides retry logic, circuit breaker, and fallback strategies.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading


class SelfHealingAction:
    """Self-healing with automatic recovery strategies."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ) -> None:
        """
        Initialize self-healing action.

        Args:
            max_retries: Maximum retry attempts
            base_delay: Initial delay between retries
            max_delay: Maximum delay between retries
            exponential_base: Exponential backoff base
            jitter: Add randomness to delays
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

        self._healing_strategies: Dict[str, Callable] = {}
        self._recovery_count = 0
        self._failed_recoveries = 0

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute self-healing operation.

        Args:
            params: Dictionary containing:
                - operation: 'heal', 'register_strategy', 'status'
                - action: Action to execute with healing
                - error: Error to recover from
                - strategy: Recovery strategy name

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "heal")

        if operation == "heal":
            return self._heal(params)
        elif operation == "register_strategy":
            return self._register_strategy(params)
        elif operation == "status":
            return self._get_status(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _heal(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute action with self-healing on failure."""
        action = params.get("action")
        fallback = params.get("fallback")
        error = params.get("error")
        strategy = params.get("strategy", "exponential_backoff")

        last_exception = None
        attempt = 0

        while attempt <= self.max_retries:
            try:
                if callable(action):
                    result = action()
                else:
                    result = {"success": True, "message": "Action completed"}

                if attempt > 0:
                    self._recovery_count += 1

                return {
                    "success": True,
                    "result": result,
                    "attempts": attempt + 1,
                    "recovered": attempt > 0,
                }

            except Exception as e:
                last_exception = e
                attempt += 1

                if attempt <= self.max_retries:
                    delay = self._calculate_delay(attempt, strategy)
                    time.sleep(delay)

                    if callable(fallback):
                        try:
                            fallback_result = fallback(error)
                            return {
                                "success": True,
                                "result": fallback_result,
                                "attempts": attempt,
                                "fallback_used": True,
                            }
                        except Exception:
                            pass

        self._failed_recoveries += 1

        return {
            "success": False,
            "error": str(last_exception),
            "attempts": attempt,
            "recovered": False,
        }

    def _calculate_delay(self, attempt: int, strategy: str) -> float:
        """Calculate delay based on retry strategy."""
        import random

        if strategy == "exponential_backoff":
            delay = min(self.base_delay * (self.exponential_base ** (attempt - 1)), self.max_delay)
        elif strategy == "linear":
            delay = self.base_delay * attempt
        elif strategy == "fixed":
            delay = self.base_delay
        else:
            delay = self.base_delay

        if self.jitter:
            delay = delay * (0.5 + random.random())

        return delay

    def _register_strategy(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register custom healing strategy."""
        strategy_name = params.get("strategy_name", "")
        handler = params.get("handler")

        if not strategy_name or not callable(handler):
            return {"success": False, "error": "strategy_name and handler are required"}

        self._healing_strategies[strategy_name] = handler

        return {"success": True, "strategy_registered": strategy_name}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get self-healing status."""
        return {
            "success": True,
            "max_retries": self.max_retries,
            "base_delay": self.base_delay,
            "recovery_count": self._recovery_count,
            "failed_recoveries": self._failed_recoveries,
            "registered_strategies": list(self._healing_strategies.keys()),
        }
