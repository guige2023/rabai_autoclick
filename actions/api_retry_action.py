"""
API Retry Action - Retry logic for failed API requests.

This module provides retry capabilities with exponential backoff,
jitter, and configurable retry conditions.
"""

from __future__ import annotations

import asyncio
import time
import random
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum


class RetryStrategy(Enum):
    """Retry strategies."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.1
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retryable_status_codes: list[int] = field(default_factory=lambda: [408, 429, 500, 502, 503, 504])


@dataclass
class RetryResult:
    """Result of retry operation."""
    success: bool
    attempts: int
    final_error: str | None = None
    duration_ms: float = 0.0
    results: list[Any] = field(default_factory=list)


class RetryHandler:
    """Handles retry logic for operations."""
    
    def __init__(self, config: RetryConfig | None = None) -> None:
        self.config = config or RetryConfig()
    
    async def execute(self, operation: Callable[[], Any]) -> RetryResult:
        """Execute operation with retry logic."""
        start_time = time.time()
        results = []
        last_error = None
        
        for attempt in range(self.config.max_attempts):
            try:
                result = operation()
                if asyncio.iscoroutine(result):
                    result = await result
                results.append(result)
                
                if attempt == self.config.max_attempts - 1:
                    break
                    
            except Exception as e:
                last_error = str(e)
                results.append({"error": str(e)})
                
                if attempt == self.config.max_attempts - 1:
                    break
            
            delay = self._calculate_delay(attempt)
            await asyncio.sleep(delay)
        
        return RetryResult(
            success=last_error is None,
            attempts=self.config.max_attempts,
            final_error=last_error,
            duration_ms=(time.time() - start_time) * 1000,
            results=results,
        )
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay before next retry."""
        if self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.initial_delay * (self.config.multiplier ** attempt)
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.initial_delay * (attempt + 1)
        else:
            delay = self.config.initial_delay
        
        delay = min(delay, self.config.max_delay)
        
        if self.config.jitter > 0:
            jitter_amount = delay * self.config.jitter
            delay += random.uniform(-jitter_amount, jitter_amount)
        
        return max(0, delay)


class APIRetryAction:
    """API retry action for automation workflows."""
    
    def __init__(self, max_attempts: int = 3, strategy: str = "exponential") -> None:
        self.config = RetryConfig(
            max_attempts=max_attempts,
            strategy=RetryStrategy(strategy),
        )
        self.handler = RetryHandler(self.config)
    
    async def execute(self, operation: Callable[[], Any]) -> RetryResult:
        """Execute operation with retry."""
        return await self.handler.execute(operation)


__all__ = ["RetryStrategy", "RetryConfig", "RetryResult", "RetryHandler", "APIRetryAction"]
