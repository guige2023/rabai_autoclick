"""Retry handler action module for RabAI AutoClick.

Provides retry operations:
- RetryExecuteAction: Execute with retry logic
- RetryConfigAction: Configure retry parameters
- CircuitBreakerAction: Circuit breaker pattern
- TimeoutAction: Execute with timeout
- FallbackAction: Fallback on failure
- BulkheadAction: Bulkhead isolation
- RateLimiterAction: Rate limiting
- BackoffAction: Exponential backoff
"""

import os
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryState:
    """Shared retry state."""
    
    _attempts: Dict[str, int] = {}
    _circuit_state: Dict[str, str] = {}
    
    @classmethod
    def get_attempts(cls, key: str) -> int:
        return cls._attempts.get(key, 0)
    
    @classmethod
    def increment_attempts(cls, key: str) -> int:
        cls._attempts[key] = cls._attempts.get(key, 0) + 1
        return cls._attempts[key]
    
    @classmethod
    def reset_attempts(cls, key: str) -> None:
        cls._attempts[key] = 0
    
    @classmethod
    def get_circuit_state(cls, key: str) -> str:
        return cls._circuit_state.get(key, "closed")
    
    @classmethod
    def set_circuit_state(cls, key: str, state: str) -> None:
        cls._circuit_state[key] = state


class RetryExecuteAction(BaseAction):
    """Execute with retry logic."""
    action_type = "retry_execute"
    display_name = "重试执行"
    description = "带重试逻辑的执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_key = params.get("task_key", "default")
            max_attempts = params.get("max_attempts", 3)
            delay = params.get("delay", 1)
            backoff = params.get("backoff", 2)
            retry_on = params.get("retry_on", [])
            
            attempt = RetryState.get_attempts(task_key)
            
            if attempt >= max_attempts:
                RetryState.reset_attempts(task_key)
                return ActionResult(
                    success=False,
                    message=f"Max attempts ({max_attempts}) reached",
                    data={"attempts": attempt, "task_key": task_key}
                )
            
            RetryState.increment_attempts(task_key)
            
            current_delay = delay * (backoff ** (attempt - 1))
            if attempt > 1:
                time.sleep(current_delay)
            
            success = params.get("simulate_success", True)
            
            if success:
                RetryState.reset_attempts(task_key)
                return ActionResult(
                    success=True,
                    message=f"Success on attempt {attempt}",
                    data={"attempt": attempt, "task_key": task_key}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Attempt {attempt} failed, will retry",
                    data={"attempt": attempt, "task_key": task_key, "will_retry": True}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry execute failed: {str(e)}")


class RetryConfigAction(BaseAction):
    """Configure retry parameters."""
    action_type = "retry_config"
    display_name = "重试配置"
    description = "配置重试参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            task_key = params.get("task_key", "default")
            
            if action == "reset":
                RetryState.reset_attempts(task_key)
                return ActionResult(
                    success=True,
                    message=f"Reset retry state for {task_key}",
                    data={"task_key": task_key}
                )
            elif action == "get":
                attempts = RetryState.get_attempts(task_key)
                return ActionResult(
                    success=True,
                    message=f"Attempts for {task_key}: {attempts}",
                    data={"task_key": task_key, "attempts": attempts}
                )
            elif action == "set":
                attempts = params.get("attempts", 0)
                RetryState._attempts[task_key] = attempts
                return ActionResult(
                    success=True,
                    message=f"Set attempts for {task_key} to {attempts}",
                    data={"task_key": task_key, "attempts": attempts}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry config failed: {str(e)}")


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern."""
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            circuit_key = params.get("circuit_key", "default")
            action = params.get("action", "call")
            failure_threshold = params.get("failure_threshold", 5)
            reset_timeout = params.get("reset_timeout", 60)
            
            state = RetryState.get_circuit_state(circuit_key)
            
            if action == "state":
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_key}: {state}",
                    data={"circuit_key": circuit_key, "state": state}
                )
            
            if state == "open":
                return ActionResult(
                    success=False,
                    message=f"Circuit {circuit_key} is OPEN",
                    data={"circuit_key": circuit_key, "state": "open"}
                )
            
            success = params.get("simulate_success", True)
            
            if success:
                if state == "half-open":
                    RetryState.set_circuit_state(circuit_key, "closed")
                
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_key}: call succeeded",
                    data={"circuit_key": circuit_key, "state": "closed"}
                )
            else:
                failures = RetryState.get_attempts(f"{circuit_key}_failures")
                RetryState.increment_attempts(f"{circuit_key}_failures")
                
                if failures + 1 >= failure_threshold:
                    RetryState.set_circuit_state(circuit_key, "open")
                
                return ActionResult(
                    success=False,
                    message=f"Circuit {circuit_key}: call failed",
                    data={"circuit_key": circuit_key, "failures": failures + 1, "state": "open" if failures + 1 >= failure_threshold else "closed"}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker failed: {str(e)}")


class TimeoutAction(BaseAction):
    """Execute with timeout."""
    action_type = "timeout_execute"
    display_name = "超时执行"
    description = "超时执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeout = params.get("timeout", 5)
            operation = params.get("operation", "sleep")
            
            start_time = time.time()
            
            if operation == "sleep":
                sleep_time = min(timeout + 1, timeout * 2)
                time.sleep(sleep_time)
            else:
                time.sleep(0.1)
            
            elapsed = time.time() - start_time
            
            if elapsed > timeout:
                return ActionResult(
                    success=False,
                    message=f"Operation timed out after {elapsed:.2f}s",
                    data={"timeout": timeout, "elapsed": elapsed}
                )
            
            return ActionResult(
                success=True,
                message=f"Operation completed in {elapsed:.2f}s",
                data={"elapsed": elapsed, "timeout": timeout}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Timeout execute failed: {str(e)}")


class FallbackAction(BaseAction):
    """Fallback on failure."""
    action_type = "fallback"
    display_name = "降级处理"
    description = "失败时降级处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            primary_success = params.get("primary_success", True)
            fallback_value = params.get("fallback_value", "default_fallback")
            
            if primary_success:
                return ActionResult(
                    success=True,
                    message="Primary operation succeeded",
                    data={"used_fallback": False, "result": "primary_result"}
                )
            else:
                return ActionResult(
                    success=True,
                    message="Using fallback value",
                    data={"used_fallback": True, "fallback_value": fallback_value}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Fallback failed: {str(e)}")


class BulkheadAction(BaseAction):
    """Bulkhead isolation."""
    action_type = "bulkhead"
    display_name = "隔板隔离"
    description = "隔板模式隔离"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bulkhead_key = params.get("bulkhead_key", "default")
            max_concurrent = params.get("max_concurrent", 10)
            operation = params.get("operation", "check")
            
            if operation == "check":
                active = RetryState.get_attempts(f"{bulkhead_key}_active")
                
                if active >= max_concurrent:
                    return ActionResult(
                        success=False,
                        message=f"Bulkhead {bulkhead_key}: at capacity ({active}/{max_concurrent})",
                        data={"bulkhead_key": bulkhead_key, "active": active, "capacity": max_concurrent}
                    )
                
                return ActionResult(
                    success=True,
                    message=f"Bulkhead {bulkhead_key}: available ({active}/{max_concurrent})",
                    data={"bulkhead_key": bulkhead_key, "active": active, "capacity": max_concurrent}
                )
            
            elif operation == "acquire":
                active = RetryState.get_attempts(f"{bulkhead_key}_active")
                if active < max_concurrent:
                    RetryState.increment_attempts(f"{bulkhead_key}_active")
                    return ActionResult(
                        success=True,
                        message=f"Acquired slot in bulkhead {bulkhead_key}",
                        data={"bulkhead_key": bulkhead_key, "active": active + 1}
                    )
                return ActionResult(
                    success=False,
                    message=f"Bulkhead {bulkhead_key}: no capacity",
                    data={"bulkhead_key": bulkhead_key, "active": active}
                )
            
            elif operation == "release":
                active = RetryState.get_attempts(f"{bulkhead_key}_active")
                if active > 0:
                    RetryState._attempts[f"{bulkhead_key}_active"] = max(0, active - 1)
                
                return ActionResult(
                    success=True,
                    message=f"Released slot in bulkhead {bulkhead_key}",
                    data={"bulkhead_key": bulkhead_key, "active": max(0, active - 1)}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Bulkhead failed: {str(e)}")


class RateLimiterAction(BaseAction):
    """Rate limiting."""
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "速率限制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limiter_key = params.get("key", "default")
            max_requests = params.get("max_requests", 100)
            window = params.get("window", 60)
            operation = params.get("operation", "check")
            
            now = time.time()
            
            if operation == "check":
                last_request = RetryState.get_attempts(f"{limiter_key}_last")
                count = RetryState.get_attempts(f"{limiter_key}_count")
                
                if now - last_request < window:
                    if count >= max_requests:
                        return ActionResult(
                            success=False,
                            message=f"Rate limit exceeded for {limiter_key}",
                            data={"key": limiter_key, "count": count, "limit": max_requests}
                        )
                    RetryState.increment_attempts(f"{limiter_key}_count")
                else:
                    RetryState._attempts[f"{limiter_key}_count"] = 1
                    RetryState._attempts[f"{limiter_key}_last"] = now
                
                return ActionResult(
                    success=True,
                    message=f"Rate limit check passed for {limiter_key}",
                    data={"key": limiter_key, "count": count, "limit": max_requests}
                )
            
            elif operation == "reset":
                RetryState._attempts.pop(f"{limiter_key}_count", None)
                RetryState._attempts.pop(f"{limiter_key}_last", None)
                return ActionResult(
                    success=True,
                    message=f"Reset rate limiter {limiter_key}",
                    data={"key": limiter_key}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limiter failed: {str(e)}")


class BackoffAction(BaseAction):
    """Exponential backoff calculator."""
    action_type = "backoff_calculate"
    display_name = "退避计算"
    description = "计算指数退避"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            attempt = params.get("attempt", 1)
            base_delay = params.get("base_delay", 1)
            max_delay = params.get("max_delay", 60)
            backoff_factor = params.get("backoff_factor", 2)
            jitter = params.get("jitter", True)
            
            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            
            if jitter:
                import random
                delay = delay * (0.5 + random.random() * 0.5)
            
            return ActionResult(
                success=True,
                message=f"Backoff for attempt {attempt}: {delay:.2f}s",
                data={
                    "attempt": attempt,
                    "delay": delay,
                    "base_delay": base_delay,
                    "max_delay": max_delay
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backoff calculation failed: {str(e)}")
