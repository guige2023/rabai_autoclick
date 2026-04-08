"""Data retry action module for RabAI AutoClick.

Provides retry mechanisms specifically for data operations:
- DataRetryPolicy: Configurable retry policies for data ops
- DataRetryExecutor: Execute data operations with retry logic
- DataRetryStrategy: Adaptive retry strategies for data workloads
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union
import time
import random
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryDecision(Enum):
    """Retry decision outcomes."""
    RETRY = "retry"
    GIVE_UP = "give_up"
    FALLBACK = "fallback"


@dataclass
class DataRetryConfig:
    """Configuration for data retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on: List[Type[Exception]] = field(default_factory=list)
    fallback_value: Any = None
    timeout: Optional[float] = None
    retryable_errors: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.retryable_errors:
            self.retryable_errors = [
                "ConnectionError",
                "TimeoutError",
                "TemporaryError",
                "TransientError",
                "RateLimitError",
                "ResourceBusy",
                "LockTimeout",
            ]


class DataRetryPolicy:
    """Policy-based retry logic for data operations."""
    
    def __init__(self, config: Optional[DataRetryConfig] = None):
        self.config = config or DataRetryConfig()
        self._lock = threading.Lock()
        self._stats = {"attempts": 0, "successes": 0, "failures": 0, "fallbacks": 0}
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        delay = self.config.base_delay * (self.config.exponential_base ** (attempt - 1))
        delay = min(delay, self.config.max_delay)
        
        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def should_retry(self, attempt: int, exception: Exception) -> RetryDecision:
        """Determine if operation should be retried."""
        if attempt >= self.config.max_attempts:
            if self.config.fallback_value is not None:
                return RetryDecision.FALLBACK
            return RetryDecision.GIVE_UP
        
        error_type = type(exception).__name__
        error_msg = str(exception)
        
        for retryable in self.config.retryable_errors:
            if retryable.lower() in error_type.lower() or retryable.lower() in error_msg.lower():
                return RetryDecision.RETRY
        
        for exc_type in self.config.retry_on:
            if isinstance(exception, exc_type):
                return RetryDecision.RETRY
        
        return RetryDecision.GIVE_UP
    
    def record_attempt(self, success: bool, used_fallback: bool = False):
        """Record attempt outcome."""
        with self._lock:
            self._stats["attempts"] += 1
            if success:
                self._stats["successes"] += 1
            elif used_fallback:
                self._stats["fallbacks"] += 1
            else:
                self._stats["failures"] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        with self._lock:
            return dict(self._stats)


class DataRetryExecutor(BaseAction):
    """Execute data operations with automatic retry."""
    action_type = "data_retry"
    display_name = "数据重试执行"
    description = "带重试逻辑的数据操作执行器"
    
    def __init__(self):
        super().__init__()
        self.policies: Dict[str, DataRetryPolicy] = {}
        self._lock = threading.Lock()
    
    def get_or_create_policy(self, name: str, config: Optional[DataRetryConfig] = None) -> DataRetryPolicy:
        """Get existing policy or create new one."""
        with self._lock:
            if name not in self.policies:
                self.policies[name] = DataRetryPolicy(config)
            return self.policies[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data operation with retry."""
        try:
            policy_name = params.get("policy", "default")
            operation = params.get("operation")
            operation_args = params.get("args", [])
            operation_kwargs = params.get("kwargs", {})
            
            config = DataRetryConfig(
                max_attempts=params.get("max_attempts", 3),
                base_delay=params.get("base_delay", 1.0),
                max_delay=params.get("max_delay", 60.0),
                exponential_base=params.get("exponential_base", 2.0),
                jitter=params.get("jitter", True),
                fallback_value=params.get("fallback_value"),
                timeout=params.get("timeout"),
            )
            
            policy = self.get_or_create_policy(policy_name, config)
            
            if operation is None:
                return ActionResult(success=False, message="operation is required")
            
            last_exception = None
            result = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    if config.timeout:
                        start_time = time.time()
                    
                    if callable(operation):
                        result = operation(*operation_args, **operation_kwargs)
                    else:
                        return ActionResult(success=False, message="operation must be callable")
                    
                    policy.record_attempt(success=True)
                    return ActionResult(success=True, data={"result": result, "attempts": attempt})
                
                except Exception as e:
                    last_exception = e
                    decision = policy.should_retry(attempt, e)
                    
                    if decision == RetryDecision.GIVE_UP:
                        policy.record_attempt(success=False)
                        return ActionResult(
                            success=False,
                            message=f"Operation failed after {attempt} attempts: {str(e)}",
                            data={"attempts": attempt, "error": str(e)}
                        )
                    
                    if decision == RetryDecision.FALLBACK:
                        policy.record_attempt(success=False, used_fallback=True)
                        return ActionResult(
                            success=True,
                            message=f"Operation fell back after {attempt} attempts",
                            data={"result": config.fallback_value, "attempts": attempt, "fallback": True}
                        )
                    
                    if attempt < config.max_attempts:
                        delay = policy.calculate_delay(attempt)
                        time.sleep(delay)
            
            policy.record_attempt(success=False)
            return ActionResult(
                success=False,
                message=f"Operation failed after {config.max_attempts} attempts",
                data={"error": str(last_exception) if last_exception else None}
            )
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataRetryExecutor error: {str(e)}")
    
    def get_policy_stats(self, policy_name: str) -> Dict[str, Any]:
        """Get statistics for a specific policy."""
        with self._lock:
            if policy_name in self.policies:
                return self.policies[policy_name].get_stats()
            return {}
