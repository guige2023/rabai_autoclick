"""API Resilience Ticket Action Module.

Provides circuit breaker, bulkhead, and fault tolerance patterns for API clients.
Handles cascading failures, fallback mechanisms, and system recovery.

Example:
    context = {"circuit_breaker": {"failure_threshold": 5, "timeout": 60}}
    result = execute(context, {"action": "call_api", "fallback": "cache"})
"""
from typing import Any, Optional
from datetime import datetime, timedelta


class CircuitBreaker:
    """Circuit breaker pattern implementation.
    
    States:
        - CLOSED: Normal operation, requests pass through
        - OPEN: Circuit tripped, requests fail fast
        - HALF_OPEN: Testing if service recovered
    """
    
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: int = 60,
        half_open_max_calls: int = 3,
    ) -> None:
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            timeout: Seconds before attempting half-open state
            half_open_max_calls: Max calls in half-open state before deciding
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
    
    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == self.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_max_calls:
                self._close()
        elif self.state == self.CLOSED:
            self.failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == self.HALF_OPEN:
            self._open()
        elif self.failure_count >= self.failure_threshold:
            self._open()
    
    def can_execute(self) -> bool:
        """Check if a request can be executed."""
        if self.state == self.CLOSED:
            return True
        
        if self.state == self.OPEN:
            if self._should_attempt_reset():
                self._half_open()
                return True
            return False
        
        if self.state == self.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        
        return False
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.timeout
    
    def _open(self) -> None:
        """Transition to OPEN state."""
        self.state = self.OPEN
        self.half_open_calls = 0
        self.success_count = 0
    
    def _half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        self.state = self.HALF_OPEN
        self.half_open_calls = 0
        self.success_count = 0
    
    def _close(self) -> None:
        """Transition to CLOSED state."""
        self.state = self.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
    
    def get_state(self) -> dict[str, Any]:
        """Get current circuit breaker state."""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
        }


class Bulkhead:
    """Bulkhead pattern - isolate failures in parallel systems.
    
    Limits concurrent calls to specific resources to prevent cascade failures.
    """
    
    def __init__(self, max_concurrent: int = 10, max_queue_size: int = 20) -> None:
        """Initialize bulkhead.
        
        Args:
            max_concurrent: Maximum concurrent executions
            max_queue_size: Maximum queued executions
        """
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self._current = 0
        self._queue_size = 0
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire permission to execute.
        
        Args:
            timeout: Maximum seconds to wait for permit
            
        Returns:
            True if permit granted, False otherwise
        """
        if self._current < self.max_concurrent:
            self._current += 1
            return True
        
        if self._queue_size < self.max_queue_size:
            self._queue_size += 1
            # In real impl, would wait here
            self._queue_size -= 1
            if self._current < self.max_concurrent:
                self._current += 1
                return True
        
        return False
    
    def release(self) -> None:
        """Release execution permit."""
        if self._current > 0:
            self._current -= 1
    
    def get_state(self) -> dict[str, Any]:
        """Get current bulkhead state."""
        return {
            "current_executions": self._current,
            "max_concurrent": self.max_concurrent,
            "available": self.max_concurrent - self._current,
        }


class FallbackManager:
    """Manages fallback strategies for failed API calls."""
    
    def __init__(self) -> None:
        """Initialize fallback manager."""
        self._fallbacks: dict[str, Any] = {}
    
    def register(self, key: str, fallback: Any) -> None:
        """Register a fallback handler.
        
        Args:
            key: Identifier for this fallback
            fallback: Callable fallback function or static value
        """
        self._fallbacks[key] = fallback
    
    def execute(self, key: str, context: dict[str, Any], params: dict[str, Any]) -> Any:
        """Execute fallback for given key.
        
        Args:
            key: Fallback identifier
            context: Execution context
            params: Parameters for fallback
            
        Returns:
            Fallback result or None
        """
        fallback = self._fallbacks.get(key)
        if fallback is None:
            return None
        
        if callable(fallback):
            try:
                return fallback(context, params)
            except Exception:
                return None
        
        return fallback
    
    def clear(self) -> None:
        """Clear all registered fallbacks."""
        self._fallbacks.clear()


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute resilience action.
    
    Args:
        context: Execution context with optional circuit_breaker, bulkhead config
        params: Parameters including action and target
        
    Returns:
        Result dictionary with status, data, and metadata
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "circuit_status":
        cb_config = context.get("circuit_breaker", {})
        cb = CircuitBreaker(
            failure_threshold=cb_config.get("failure_threshold", 5),
            timeout=cb_config.get("timeout", 60),
        )
        result["data"] = cb.get_state()
    
    elif action == "bulkhead_status":
        bh_config = context.get("bulkhead", {})
        bh = Bulkhead(
            max_concurrent=bh_config.get("max_concurrent", 10),
            max_queue_size=bh_config.get("max_queue_size", 20),
        )
        result["data"] = bh.get_state()
    
    elif action == "record_success":
        # Would integrate with stored circuit breaker
        result["data"] = {"recorded": True}
    
    elif action == "record_failure":
        # Would integrate with stored circuit breaker
        result["data"] = {"recorded": True}
    
    elif action == "fallback_execute":
        fm = FallbackManager()
        key = params.get("fallback_key", "")
        fallback_result = fm.execute(key, context, params)
        result["data"] = {"fallback_result": fallback_result}
    
    elif action == "resilience_check":
        cb_config = context.get("circuit_breaker", {})
        bh_config = context.get("bulkhead", {})
        cb = CircuitBreaker(
            failure_threshold=cb_config.get("failure_threshold", 5),
            timeout=cb_config.get("timeout", 60),
        )
        bh = Bulkhead(
            max_concurrent=bh_config.get("max_concurrent", 10),
        )
        result["data"] = {
            "circuit_breaker": cb.get_state(),
            "bulkhead": bh.get_state(),
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
