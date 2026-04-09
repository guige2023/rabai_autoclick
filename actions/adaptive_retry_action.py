"""
Adaptive retry action with intelligent backoff strategies.

Provides error analysis, dynamic adjustment, and smart retry scheduling.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import random
import threading


class AdaptiveRetryAction:
    """Adaptive retry with intelligent backoff and error analysis."""

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        max_retries: int = 5,
        enable_adaptation: bool = True,
    ) -> None:
        """
        Initialize adaptive retry.

        Args:
            base_delay: Initial delay in seconds
            max_delay: Maximum delay cap
            max_retries: Maximum retry attempts
            enable_adaptation: Adjust delays based on error patterns
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.enable_adaptation = enable_adaptation

        self._error_patterns: Dict[str, int] = {}
        self._retry_stats: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute retry operation.

        Args:
            params: Dictionary containing:
                - operation: 'retry', 'status', 'analyze'
                - action: Action to execute with retry
                - error_class: Error classification

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "retry")

        if operation == "retry":
            return self._execute_with_retry(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "analyze":
            return self._analyze_error(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _execute_with_retry(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute action with adaptive retry."""
        action = params.get("action")
        error_class = params.get("error_class", "generic")
        fallback = params.get("fallback")

        last_error = None
        attempt = 0
        delays = []

        while attempt <= self.max_retries:
            try:
                if callable(action):
                    result = action()
                else:
                    result = {"success": True, "message": "Action completed"}

                if attempt > 0:
                    self._record_success(error_class, attempt, delays)

                return {
                    "success": True,
                    "result": result,
                    "attempts": attempt + 1,
                    "retries_performed": attempt,
                }

            except Exception as e:
                last_error = e
                attempt += 1

                if attempt <= self.max_retries:
                    delay = self._calculate_adaptive_delay(attempt, error_class)
                    delays.append(delay)
                    time.sleep(delay)

                    if callable(fallback):
                        try:
                            fallback_result = fallback(e)
                            self._record_fallback_usage(error_class)
                            return {
                                "success": True,
                                "result": fallback_result,
                                "attempts": attempt,
                                "fallback_used": True,
                            }
                        except Exception:
                            pass

        self._record_failure(error_class, attempt)

        return {
            "success": False,
            "error": str(last_error),
            "attempts": attempt,
            "retries_performed": attempt - 1,
        }

    def _calculate_adaptive_delay(self, attempt: int, error_class: str) -> float:
        """Calculate adaptive delay based on error class and history."""
        base = self.base_delay

        if self.enable_adaptation:
            with self._lock:
                if error_class in self._retry_stats:
                    stats = self._retry_stats[error_class]
                    success_rate = stats.get("success_rate", 0.5)
                    avg_attempts = stats.get("avg_attempts", 2)

                    if success_rate < 0.3:
                        base = base * 1.5
                    elif success_rate > 0.8:
                        base = base * 0.75

                    if avg_attempts > 3:
                        base = base * 1.25

        delay = min(base * (2 ** (attempt - 1)), self.max_delay)

        jitter = delay * 0.1 * (2 * random.random() - 1)
        delay = delay + jitter

        return delay

    def _record_success(self, error_class: str, attempts: int, delays: List[float]) -> None:
        """Record successful retry statistics."""
        with self._lock:
            if error_class not in self._retry_stats:
                self._retry_stats[error_class] = {
                    "total_attempts": 0,
                    "successful_retries": 0,
                    "total_retries": 0,
                }

            stats = self._retry_stats[error_class]
            stats["total_attempts"] += 1
            stats["successful_retries"] += 1
            stats["total_retries"] += attempts - 1

            stats["success_rate"] = stats["successful_retries"] / stats["total_attempts"]
            stats["avg_attempts"] = stats["total_retries"] / stats["successful_retries"] if stats["successful_retries"] > 0 else 1

    def _record_failure(self, error_class: str, attempts: int) -> None:
        """Record failed retry statistics."""
        with self._lock:
            if error_class not in self._retry_stats:
                self._retry_stats[error_class] = {
                    "total_attempts": 0,
                    "successful_retries": 0,
                    "total_retries": 0,
                    "failures": 0,
                }

            stats = self._retry_stats[error_class]
            stats["total_attempts"] += 1
            stats["total_retries"] += attempts - 1
            stats["failures"] = stats.get("failures", 0) + 1

            stats["success_rate"] = stats["successful_retries"] / stats["total_attempts"]
            stats["avg_attempts"] = stats["total_retries"] / stats["total_attempts"] if stats["total_attempts"] > 0 else 1

    def _record_fallback_usage(self, error_class: str) -> None:
        """Record fallback usage."""
        with self._lock:
            if error_class not in self._retry_stats:
                self._retry_stats[error_class] = {}

            self._retry_stats[error_class]["fallback_usage"] = self._retry_stats[error_class].get("fallback_usage", 0) + 1

    def _analyze_error(self, params: dict[str, Any]) -> dict[str, Any]:
        """Analyze error patterns and recommend strategies."""
        error_class = params.get("error_class", "generic")

        with self._lock:
            stats = self._retry_stats.get(error_class, {})

        analysis = {
            "error_class": error_class,
            "success_rate": stats.get("success_rate", 0),
            "avg_attempts": stats.get("avg_attempts", 1),
            "recommended_strategy": "exponential_backoff",
        }

        if stats.get("success_rate", 0) < 0.3:
            analysis["recommended_strategy"] = "heavy_backoff"
            analysis["recommendation"] = "Consider circuit breaker or alternative path"
        elif stats.get("avg_attempts", 1) > 4:
            analysis["recommended_strategy"] = "quick_fail"
            analysis["recommendation"] = "Too many retries, fail fast and use cache"

        return {"success": True, "analysis": analysis}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get retry action status."""
        with self._lock:
            return {
                "success": True,
                "max_retries": self.max_retries,
                "base_delay": self.base_delay,
                "max_delay": self.max_delay,
                "enable_adaptation": self.enable_adaptation,
                "error_classes_tracked": len(self._retry_stats),
                "retry_stats": dict(self._retry_stats),
            }
