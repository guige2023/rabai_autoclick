"""Automation Backoff Action Module.

Provides backoff strategies for automation task retries including
exponential, linear, and adaptive backoff algorithms.

Example:
    >>> from actions.automation.automation_backoff_action import AutomationBackoffAction
    >>> action = AutomationBackoffAction()
    >>> wait_time = action.get_backoff(attempt=3)
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import threading
import time


class BackoffStrategy(Enum):
    """Backoff algorithm strategies."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"
    FIBONACCI = "fibonacci"
    ADAPTIVE = "adaptive"
    DECORRELATED = "decorrelated"


class BackoffState(Enum):
    """State of backoff controller."""
    READY = "ready"
    WAITING = "waiting"
    RETRYING = "retrying"
    EXHAUSTED = "exhausted"


@dataclass
class BackoffConfig:
    """Configuration for backoff behavior.
    
    Attributes:
        strategy: Backoff strategy
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        multiplier: Backoff multiplier
        jitter: Enable random jitter
        jitter_factor: Jitter factor (0-1)
        max_attempts: Maximum retry attempts
    """
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.3
    max_attempts: int = 5


@dataclass
class BackoffAttempt:
    """Record of a backoff attempt.
    
    Attributes:
        attempt: Attempt number
        delay: Calculated delay
        timestamp: Attempt timestamp
        success: Whether attempt succeeded
    """
    attempt: int
    delay: float
    timestamp: float
    success: bool = False


@dataclass
class BackoffResult:
    """Result of backoff calculation.
    
    Attributes:
        delay: Calculated delay in seconds
        next_attempt: Next attempt number
        should_retry: Whether should continue retrying
        strategy: Strategy used
    """
    delay: float
    next_attempt: int
    should_retry: bool
    strategy: BackoffStrategy


class AutomationBackoffAction:
    """Backoff controller for automation task retries.
    
    Provides configurable backoff strategies with jitter
    support for robust retry handling.
    
    Attributes:
        config: Backoff configuration
        _attempts: Attempt history per task
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[BackoffConfig] = None,
    ) -> None:
        """Initialize backoff action.
        
        Args:
            config: Backoff configuration
        """
        self.config = config or BackoffConfig()
        self._attempts: Dict[str, List[BackoffAttempt]] = {}
        self._lock = threading.RLock()
    
    def get_backoff(
        self,
        task_id: str,
        attempt: Optional[int] = None,
    ) -> BackoffResult:
        """Calculate backoff delay for task.
        
        Args:
            task_id: Unique task identifier
            attempt: Specific attempt number
        
        Returns:
            BackoffResult with delay and retry info
        """
        attempt = attempt or self._get_next_attempt(task_id)
        
        delay = self._calculate_delay(attempt)
        
        should_retry = attempt < self.config.max_attempts
        
        return BackoffResult(
            delay=delay,
            next_attempt=attempt + 1,
            should_retry=should_retry,
            strategy=self.config.strategy,
        )
    
    def record_attempt(
        self,
        task_id: str,
        success: bool = False,
    ) -> None:
        """Record attempt result.
        
        Args:
            task_id: Unique task identifier
            success: Whether attempt succeeded
        """
        with self._lock:
            if task_id not in self._attempts:
                self._attempts[task_id] = []
            
            attempt_num = len(self._attempts[task_id]) + 1
            
            last_delay = 0.0
            if self._attempts[task_id]:
                last_delay = self._attempts[task_id][-1].delay
            
            attempt = BackoffAttempt(
                attempt=attempt_num,
                delay=last_delay,
                timestamp=time.time(),
                success=success,
            )
            
            self._attempts[task_id].append(attempt)
    
    def reset(self, task_id: str) -> None:
        """Reset backoff state for task.
        
        Args:
            task_id: Unique task identifier
        """
        with self._lock:
            if task_id in self._attempts:
                self._attempts[task_id].clear()
    
    def get_delay(
        self,
        base_delay: float,
        attempt: int,
    ) -> float:
        """Calculate delay using current strategy.
        
        Args:
            base_delay: Base delay
            attempt: Attempt number
        
        Returns:
            Calculated delay
        """
        if self.config.strategy == BackoffStrategy.EXPONENTIAL:
            return self._exponential_delay(base_delay, attempt)
        elif self.config.strategy == BackoffStrategy.LINEAR:
            return self._linear_delay(base_delay, attempt)
        elif self.config.strategy == BackoffStrategy.CONSTANT:
            return self._constant_delay(base_delay)
        elif self.config.strategy == BackoffStrategy.FIBONACCI:
            return self._fibonacci_delay(base_delay, attempt)
        elif self.config.strategy == BackoffStrategy.ADAPTIVE:
            return self._adaptive_delay(base_delay, attempt)
        elif self.config.strategy == BackoffStrategy.DECORRELATED:
            return self._decorrelated_delay(base_delay, attempt)
        else:
            return self._exponential_delay(base_delay, attempt)
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate total delay with jitter.
        
        Args:
            attempt: Attempt number
        
        Returns:
            Final delay value
        """
        delay = self.get_delay(self.config.initial_delay, attempt)
        
        delay = min(delay, self.config.max_delay)
        
        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0.0, delay)
    
    def _exponential_delay(
        self,
        base: float,
        attempt: int,
    ) -> float:
        """Exponential backoff delay.
        
        Args:
            base: Base delay
            attempt: Attempt number
        
        Returns:
            Delay value
        """
        return min(base * (self.config.multiplier ** (attempt - 1)), self.config.max_delay)
    
    def _linear_delay(
        self,
        base: float,
        attempt: int,
    ) -> float:
        """Linear backoff delay.
        
        Args:
            base: Base delay
            attempt: Attempt number
        
        Returns:
            Delay value
        """
        return min(base * attempt, self.config.max_delay)
    
    def _constant_delay(self, base: float) -> float:
        """Constant delay.
        
        Args:
            base: Base delay
        
        Returns:
            Delay value
        """
        return base
    
    def _fibonacci_delay(
        self,
        base: float,
        attempt: int,
    ) -> float:
        """Fibonacci backoff delay.
        
        Args:
            base: Base delay
            attempt: Attempt number
        
        Returns:
            Delay value
        """
        a, b = 1, 1
        for _ in range(attempt - 1):
            a, b = b, a + b
        return min(base * a, self.config.max_delay)
    
    def _adaptive_delay(
        self,
        base: float,
        attempt: int,
    ) -> float:
        """Adaptive delay based on recent success rate.
        
        Args:
            base: Base delay
            attempt: Attempt number
        
        Returns:
            Delay value
        """
        success_rate = self._calculate_success_rate()
        
        adaptive_multiplier = 1.0 / (success_rate + 0.1)
        
        delay = self._exponential_delay(base, attempt) * adaptive_multiplier
        
        return min(delay, self.config.max_delay)
    
    def _decorrelated_delay(
        self,
        base: float,
        attempt: int,
    ) -> float:
        """Decorrelated jitter backoff delay.
        
        Args:
            base: Base delay
            attempt: Attempt number
        
        Returns:
            Delay value
        """
        import random
        prev_delay = base
        if attempt > 1:
            prev_delay = self._exponential_delay(base, attempt - 1)
        
        return min(prev_delay * 3 * random.random(), self.config.max_delay)
    
    def _calculate_success_rate(self) -> float:
        """Calculate recent success rate.
        
        Returns:
            Success rate (0-1)
        """
        with self._lock:
            all_attempts: List[BackoffAttempt] = []
            for attempts in self._attempts.values():
                all_attempts.extend(attempts[-10:])
            
            if not all_attempts:
                return 1.0
            
            successes = sum(1 for a in all_attempts if a.success)
            return successes / len(all_attempts)
    
    def _get_next_attempt(self, task_id: str) -> int:
        """Get next attempt number for task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            Next attempt number
        """
        with self._lock:
            if task_id not in self._attempts:
                return 1
            return len(self._attempts[task_id]) + 1
    
    def get_stats(self, task_id: str) -> Dict[str, Any]:
        """Get backoff statistics for task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            Statistics dictionary
        """
        with self._lock:
            attempts = self._attempts.get(task_id, [])
            
            return {
                "task_id": task_id,
                "total_attempts": len(attempts),
                "successful_attempts": sum(1 for a in attempts if a.success),
                "failed_attempts": sum(1 for a in attempts if not a.success),
                "current_delay": attempts[-1].delay if attempts else 0,
                "next_attempt": len(attempts) + 1,
                "should_retry": len(attempts) < self.config.max_attempts,
            }
    
    async def wait_for_backoff(
        self,
        task_id: str,
        attempt: Optional[int] = None,
    ) -> BackoffResult:
        """Wait for backoff period.
        
        Args:
            task_id: Task identifier
            attempt: Attempt number
        
        Returns:
            BackoffResult
        """
        result = self.get_backoff(task_id, attempt)
        
        if result.delay > 0:
            await asyncio.sleep(result.delay)
        
        return result
