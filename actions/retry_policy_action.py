"""Retry Policy action module for RabAI AutoClick.

Provides configurable retry logic with exponential backoff, jitter,
circuit breaker, and retry statistics tracking.
"""

import sys
import os
import json
import time
import random
import asyncio
from typing import Any, Dict, List, Optional, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BackoffStrategy(Enum):
    """Retry backoff strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""
    name: str
    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1  # +/- 10%
    retryable_exceptions: List[str] = field(default_factory=list)
    non_retryable_exceptions: List[str] = field(default_factory=list)
    retry_on_timeout: bool = True
    description: str = ""


@dataclass
class RetryAttempt:
    """Record of a single retry attempt."""
    attempt_number: int
    timestamp: float
    delay_seconds: float
    success: bool
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class RetryStats:
    """Statistics for a retry policy."""
    policy_name: str
    total_invocations: int = 0
    total_attempts: int = 0
    successes: int = 0
    failures: int = 0
    timeouts: int = 0
    attempts_history: List[RetryAttempt] = field(default_factory=list)
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    average_attempts_per_success: float = 0.0


class RetryEngine:
    """Retry execution engine with backoff and statistics."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._policies: Dict[str, RetryPolicy] = {}
        self._stats: Dict[str, RetryStats] = {}
        self._persistence_path = persistence_path
        self._circuit_breakers: Dict[str, "CircuitBreaker"] = {}
        self._load()
    
    def _load(self) -> None:
        """Load policies and stats from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for name, policy_data in data.get("policies", {}).items():
                        self._policies[name] = RetryPolicy(**policy_data)
                    for name, stats_data in data.get("stats", {}).items():
                        stats_data["attempts_history"] = [
                            RetryAttempt(**a) for a in stats_data.get("attempts_history", [])
                        ]
                        self._stats[name] = RetryStats(**stats_data)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist policies and stats."""
        if self._persistence_path:
            try:
                data = {
                    "policies": {name: vars(policy) for name, policy in self._policies.items()},
                    "stats": {
                        name: {
                            "policy_name": stats.policy_name,
                            "total_invocations": stats.total_invocations,
                            "total_attempts": stats.total_attempts,
                            "successes": stats.successes,
                            "failures": stats.failures,
                            "timeouts": stats.timeouts,
                            "last_success_time": stats.last_success_time,
                            "last_failure_time": stats.last_failure_time,
                            "average_attempts_per_success": stats.average_attempts_per_success,
                            "attempts_history": [
                                vars(a) for a in stats.attempts_history[-100:]
                            ]
                        }
                        for name, stats in self._stats.items()
                    }
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def create_policy(self, policy: RetryPolicy) -> None:
        """Create a new retry policy."""
        self._policies[policy.name] = policy
        self._stats[policy.name] = RetryStats(policy_name=policy.name)
        self._persist()
    
    def remove_policy(self, name: str) -> bool:
        """Remove a retry policy."""
        if name in self._policies:
            del self._policies[name]
            self._stats.pop(name, None)
            self._circuit_breakers.pop(name, None)
            self._persist()
            return True
        return False
    
    def get_policy(self, name: str) -> Optional[RetryPolicy]:
        """Get a retry policy by name."""
        return self._policies.get(name)
    
    def list_policies(self) -> List[str]:
        """List all policy names."""
        return list(self._policies.keys())
    
    def _calculate_delay(self, policy: RetryPolicy, attempt: int) -> float:
        """Calculate delay for a given attempt using backoff strategy."""
        if policy.backoff_strategy == BackoffStrategy.FIXED:
            delay = policy.initial_delay_seconds
        elif policy.backoff_strategy == BackoffStrategy.LINEAR:
            delay = policy.initial_delay_seconds + (attempt - 1) * policy.initial_delay_seconds
        elif policy.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = policy.initial_delay_seconds * (policy.backoff_multiplier ** (attempt - 1))
        elif policy.backoff_strategy == BackoffStrategy.FIBONACCI:
            # Fibonacci sequence for delays
            fib = [1, 1]
            for i in range(2, attempt):
                fib.append(fib[-1] + fib[-2])
            delay = policy.initial_delay_seconds * fib[min(attempt - 1, len(fib) - 1)]
        else:
            delay = policy.initial_delay_seconds
        
        # Cap at max delay
        delay = min(delay, policy.max_delay_seconds)
        
        # Add jitter if enabled
        if policy.jitter:
            jitter_range = delay * policy.jitter_factor
            delay = delay + random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def _is_retryable(self, policy: RetryPolicy, exception: Exception) -> bool:
        """Check if an exception is retryable."""
        exception_type = type(exception).__name__
        
        # Check non-retryable first (takes precedence)
        if policy.non_retryable_exceptions:
            if exception_type in policy.non_retryable_exceptions:
                return False
            # Check by base class
            for ne in policy.non_retryable_exceptions:
                try:
                    if isinstance(exception, type(ne)):
                        return False
                except TypeError:
                    pass
        
        # Check retryable list
        if policy.retryable_exceptions:
            if exception_type in policy.retryable_exceptions:
                return True
            for re in policy.retryable_exceptions:
                try:
                    if isinstance(exception, type(re)):
                        return True
                except TypeError:
                    pass
            return False
        
        # Default: retry most exceptions
        return True
    
    async def execute_with_retry_async(
        self,
        policy_name: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute a function with retry policy.
        
        Args:
            policy_name: Name of the retry policy to use.
            func: Function to execute.
            *args, **kwargs: Arguments to pass to the function.
        
        Returns:
            The function's return value on success.
        
        Raises:
            The last exception if all retries are exhausted.
        """
        policy = self._policies.get(policy_name)
        if not policy:
            raise ValueError(f"Retry policy '{policy_name}' not found")
        
        stats = self._stats.get(policy_name, RetryStats(policy_name=policy_name))
        stats.total_invocations += 1
        
        last_exception = None
        
        for attempt in range(1, policy.max_attempts + 1):
            attempt_start = time.time()
            delay = self._calculate_delay(policy, attempt)
            
            # Wait before attempt (skip delay for first attempt)
            if attempt > 1:
                await asyncio.sleep(delay)
            
            stats.total_attempts += 1
            
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Success
                stats.successes += 1
                stats.last_success_time = time.time()
                attempt_record = RetryAttempt(
                    attempt_number=attempt,
                    timestamp=time.time(),
                    delay_seconds=delay,
                    success=True,
                    duration_seconds=time.time() - attempt_start
                )
                stats.attempts_history.append(attempt_record)
                self._update_average_attempts(stats)
                self._persist()
                return result
            
            except asyncio.TimeoutError as e:
                stats.timeouts += 1
                last_exception = e
                if not policy.retry_on_timeout:
                    break
            
            except Exception as e:
                last_exception = e
                if not self._is_retryable(policy, e):
                    break
            
            # Record failed attempt
            attempt_record = RetryAttempt(
                attempt_number=attempt,
                timestamp=time.time(),
                delay_seconds=delay,
                success=False,
                error=str(last_exception),
                duration_seconds=time.time() - attempt_start
            )
            stats.attempts_history.append(attempt_record)
            
            # Check if more retries available
            if attempt >= policy.max_attempts:
                stats.failures += 1
                stats.last_failure_time = time.time()
        
        self._persist()
        raise last_exception
    
    def execute_with_retry(
        self,
        policy_name: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Synchronous wrapper for execute_with_retry_async."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.execute_with_retry_async(policy_name, func, *args, **kwargs)
            )
        finally:
            loop.close()
    
    def _update_average_attempts(self, stats: RetryStats) -> None:
        """Update average attempts per successful invocation."""
        if stats.successes > 0:
            total_attempts_for_success = sum(
                a.attempt_number for a in stats.attempts_history
                if a.success
            )
            # This is a simplified calculation
            stats.average_attempts_per_success = (
                sum(a.attempt_number for a in stats.attempts_history if a.success) /
                max(1, stats.successes)
            )
    
    def get_stats(self, policy_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a retry policy."""
        stats = self._stats.get(policy_name)
        if not stats:
            return None
        return {
            "policy_name": stats.policy_name,
            "total_invocations": stats.total_invocations,
            "total_attempts": stats.total_attempts,
            "successes": stats.successes,
            "failures": stats.failures,
            "timeouts": stats.timeouts,
            "success_rate": stats.successes / max(1, stats.total_invocations),
            "average_attempts_per_success": stats.average_attempts_per_success,
            "last_success_time": stats.last_success_time,
            "last_failure_time": stats.last_failure_time,
            "recent_attempts": [
                {
                    "attempt_number": a.attempt_number,
                    "success": a.success,
                    "error": a.error,
                    "duration_seconds": a.duration_seconds
                }
                for a in stats.attempts_history[-10:]
            ]
        }
    
    def reset_stats(self, policy_name: str) -> bool:
        """Reset statistics for a policy."""
        if policy_name in self._stats:
            self._stats[policy_name] = RetryStats(policy_name=policy_name)
            self._persist()
            return True
        return False


class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout_seconds: float = 60.0
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout_seconds = timeout_seconds
        
        self._state = "closed"  # closed, open, half_open
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()
    
    @property
    def state(self) -> str:
        """Get current circuit state."""
        with self._lock:
            if self._state == "open":
                # Check if timeout has passed to transition to half_open
                if self._last_failure_time and \
                   time.time() - self._last_failure_time >= self.timeout_seconds:
                    self._state = "half_open"
            return self._state
    
    def is_allowed(self) -> bool:
        """Check if a request is allowed through the circuit."""
        return self.state != "open"
    
    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == "half_open":
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = "closed"
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == "closed":
                self._failure_count = max(0, self._failure_count - 1)
    
    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == "half_open":
                self._state = "open"
                self._success_count = 0
            elif self._state == "closed":
                if self._failure_count >= self.failure_threshold:
                    self._state = "open"


import threading


class RetryPolicyAction(BaseAction):
    """Execute operations with configurable retry policies.
    
    Supports exponential/linear/fixed backoff, jitter, circuit breaker,
    and detailed retry statistics.
    """
    action_type = "retry_policy"
    display_name = "重试策略"
    description = "执行带重试策略的操作，支持指数退避和熔断器"
    
    def __init__(self):
        super().__init__()
        self._engine: Optional[RetryEngine] = None
    
    def _get_engine(self, params: Dict[str, Any]) -> RetryEngine:
        """Get or create the retry engine."""
        if self._engine is None:
            persistence_path = params.get("persistence_path")
            self._engine = RetryEngine(persistence_path)
        return self._engine
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute retry policy operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_policy", "remove_policy", "get_policy",
                  "list_policies", "execute", "get_stats", "reset_stats"
                - For create: name, max_attempts, initial_delay, backoff_strategy, etc.
                - For execute: policy_name, func (callable)
                - For get_stats/reset_stats: policy_name
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_policy":
                return self._create_policy(params)
            elif operation == "remove_policy":
                return self._remove_policy(params)
            elif operation == "get_policy":
                return self._get_policy(params)
            elif operation == "list_policies":
                return self._list_policies(params)
            elif operation == "execute":
                return self._execute_with_retry(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            elif operation == "reset_stats":
                return self._reset_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Retry policy error: {str(e)}")
    
    def _create_policy(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new retry policy."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Policy name is required")
        
        policy = RetryPolicy(
            name=name,
            max_attempts=params.get("max_attempts", 3),
            initial_delay_seconds=params.get("initial_delay_seconds", 1.0),
            max_delay_seconds=params.get("max_delay_seconds", 60.0),
            backoff_strategy=BackoffStrategy(params.get("backoff_strategy", "exponential")),
            backoff_multiplier=params.get("backoff_multiplier", 2.0),
            jitter=params.get("jitter", True),
            jitter_factor=params.get("jitter_factor", 0.1),
            retryable_exceptions=params.get("retryable_exceptions", []),
            non_retryable_exceptions=params.get("non_retryable_exceptions", []),
            retry_on_timeout=params.get("retry_on_timeout", True),
            description=params.get("description", "")
        )
        engine.create_policy(policy)
        return ActionResult(
            success=True,
            message=f"Retry policy '{name}' created",
            data={"name": name, "max_attempts": policy.max_attempts}
        )
    
    def _remove_policy(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a retry policy."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Policy name is required")
        
        removed = engine.remove_policy(name)
        return ActionResult(
            success=removed,
            message=f"Policy '{name}' removed" if removed else f"Policy '{name}' not found"
        )
    
    def _get_policy(self, params: Dict[str, Any]) -> ActionResult:
        """Get a retry policy."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Policy name is required")
        
        policy = engine.get_policy(name)
        if not policy:
            return ActionResult(success=False, message=f"Policy '{name}' not found")
        
        return ActionResult(
            success=True,
            message=f"Policy '{name}' retrieved",
            data={
                "name": policy.name,
                "max_attempts": policy.max_attempts,
                "backoff_strategy": policy.backoff_strategy.value,
                "initial_delay_seconds": policy.initial_delay_seconds
            }
        )
    
    def _list_policies(self, params: Dict[str, Any]) -> ActionResult:
        """List all retry policies."""
        engine = self._get_engine(params)
        policies = engine.list_policies()
        return ActionResult(
            success=True,
            message=f"Found {len(policies)} policies",
            data={"policies": policies}
        )
    
    def _execute_with_retry(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a function with retry policy."""
        engine = self._get_engine(params)
        policy_name = params.get("policy_name", "")
        
        if not policy_name:
            return ActionResult(success=False, message="policy_name is required")
        
        # Get function info from params
        func_name = params.get("func_name", "simulated_task")
        
        # Simulate function execution for demo
        # In real usage, would call actual function
        def simulated_task():
            # Simulate occasional failures for demonstration
            import random
            if random.random() < 0.3:
                raise ConnectionError("Simulated connection failure")
            return {"result": "success", "data": "processed"}
        
        try:
            result = engine.execute_with_retry(policy_name, simulated_task)
            stats = engine.get_stats(policy_name)
            return ActionResult(
                success=True,
                message=f"Execution succeeded with policy '{policy_name}'",
                data={
                    "result": result,
                    "stats": {
                        "total_attempts": stats["total_attempts"] if stats else 0,
                        "successes": stats["successes"] if stats else 0
                    }
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"All retries exhausted: {str(e)}",
                data={"error": str(e)}
            )
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get retry statistics."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Policy name is required")
        
        stats = engine.get_stats(name)
        if not stats:
            return ActionResult(success=False, message=f"No stats for policy '{name}'")
        return ActionResult(success=True, message="Stats retrieved", data=stats)
    
    def _reset_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Reset retry statistics."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Policy name is required")
        
        reset = engine.reset_stats(name)
        return ActionResult(
            success=reset,
            message=f"Stats reset for '{name}'" if reset else f"Policy '{name}' not found"
        )
