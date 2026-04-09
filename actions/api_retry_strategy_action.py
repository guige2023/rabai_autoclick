"""API retry strategy action module for RabAI AutoClick.

Provides retry strategies for API operations:
- ExponentialBackoffRetryAction: Exponential backoff retry strategy
- LinearBackoffRetryAction: Linear backoff retry strategy
- FixedDelayRetryAction: Fixed delay retry strategy
- AdaptiveRetryAction: Adaptive retry based on error type
- JitterRetryAction: Retry with jitter to prevent thundering herd
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import random
import time

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryStrategy:
    """Base retry strategy."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
    
    def get_delay(self, attempt: int) -> float:
        raise NotImplementedError


class ExponentialBackoffRetry(RetryStrategy):
    """Exponential backoff retry strategy."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 multiplier: float = 2.0, max_delay: float = 60.0):
        super().__init__(max_retries, base_delay)
        self.multiplier = multiplier
        self.max_delay = max_delay
    
    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


class LinearBackoffRetry(RetryStrategy):
    """Linear backoff retry strategy."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0,
                 increment: float = 1.0, max_delay: float = 30.0):
        super().__init__(max_retries, base_delay)
        self.increment = increment
        self.max_delay = max_delay
    
    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay + (self.increment * attempt)
        return min(delay, self.max_delay)


class FixedDelayRetry(RetryStrategy):
    """Fixed delay retry strategy."""
    
    def __init__(self, max_retries: int = 3, delay: float = 1.0):
        super().__init__(max_retries, delay)
        self.delay = delay
    
    def get_delay(self, attempt: int) -> float:
        return self.delay


class AdaptiveRetry(RetryStrategy):
    """Adaptive retry based on error type."""
    
    TRANSIENT_ERRORS = {"timeout", "network", "rate_limit", "5xx"}
    PERMANENT_ERRORS = {"400", "401", "403", "404"}
    
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0):
        super().__init__(max_retries, base_delay)
        self.error_retry_counts: Dict[str, int] = {}
    
    def get_delay(self, attempt: int, error_type: Optional[str] = None) -> float:
        if error_type in self.PERMANENT_ERRORS:
            return float('inf')
        
        if error_type in self.TRANSIENT_ERRORS:
            delay = self.base_delay * (2 ** attempt)
        else:
            delay = self.base_delay * (1.5 ** attempt)
        
        return min(delay, 60.0)
    
    def should_retry(self, error_type: Optional[str]) -> bool:
        if error_type in self.PERMANENT_ERRORS:
            return False
        return True


class JitterRetry(RetryStrategy):
    """Retry with jitter to prevent thundering herd."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0,
                 jitter_type: str = "full"):
        super().__init__(max_retries, base_delay)
        self.jitter_type = jitter_type
    
    def get_delay(self, attempt: int) -> float:
        base = self.base_delay * (2 ** attempt)
        
        if self.jitter_type == "full":
            return random.uniform(0, base)
        elif self.jitter_type == "decorrelated":
            return random.uniform(self.base_delay, base * 3)
        elif self.jitter_type == "equal":
            return base / 2 + random.uniform(0, base / 2)
        else:
            return base


class ExponentialBackoffRetryAction(BaseAction):
    """Exponential backoff retry strategy."""
    action_type = "exponential_backoff_retry"
    display_name = "指数退避重试"
    description = "使用指数退避策略进行API重试"
    
    def __init__(self):
        super().__init__()
        self.strategy = ExponentialBackoffRetry()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            multiplier = params.get("multiplier", 2.0)
            max_delay = params.get("max_delay", 60.0)
            
            self.strategy = ExponentialBackoffRetry(
                max_retries=max_retries,
                base_delay=base_delay,
                multiplier=multiplier,
                max_delay=max_delay
            )
            
            delays = [self.strategy.get_delay(i) for i in range(max_retries)]
            
            return ActionResult(
                success=True,
                message="Exponential backoff strategy configured",
                data={
                    "strategy": "exponential_backoff",
                    "delays": delays,
                    "total_delay": sum(delays)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LinearBackoffRetryAction(BaseAction):
    """Linear backoff retry strategy."""
    action_type = "linear_backoff_retry"
    display_name = "线性退避重试"
    description = "使用线性退避策略进行API重试"
    
    def __init__(self):
        super().__init__()
        self.strategy = LinearBackoffRetry()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            increment = params.get("increment", 1.0)
            max_delay = params.get("max_delay", 30.0)
            
            self.strategy = LinearBackoffRetry(
                max_retries=max_retries,
                base_delay=base_delay,
                increment=increment,
                max_delay=max_delay
            )
            
            delays = [self.strategy.get_delay(i) for i in range(max_retries)]
            
            return ActionResult(
                success=True,
                message="Linear backoff strategy configured",
                data={
                    "strategy": "linear_backoff",
                    "delays": delays,
                    "total_delay": sum(delays)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class FixedDelayRetryAction(BaseAction):
    """Fixed delay retry strategy."""
    action_type = "fixed_delay_retry"
    display_name = "固定延迟重试"
    description = "使用固定延迟进行API重试"
    
    def __init__(self):
        super().__init__()
        self.strategy = FixedDelayRetry()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 3)
            delay = params.get("delay", 1.0)
            
            self.strategy = FixedDelayRetry(
                max_retries=max_retries,
                delay=delay
            )
            
            delays = [self.strategy.get_delay(i) for i in range(max_retries)]
            
            return ActionResult(
                success=True,
                message="Fixed delay strategy configured",
                data={
                    "strategy": "fixed_delay",
                    "delays": delays,
                    "total_delay": sum(delays)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class AdaptiveRetryAction(BaseAction):
    """Adaptive retry based on error type."""
    action_type = "adaptive_retry"
    display_name = "自适应重试"
    description = "根据错误类型自适应调整重试策略"
    
    def __init__(self):
        super().__init__()
        self.strategy = AdaptiveRetry()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 5)
            base_delay = params.get("base_delay", 1.0)
            error_type = params.get("error_type")
            
            self.strategy = AdaptiveRetry(
                max_retries=max_retries,
                base_delay=base_delay
            )
            
            if error_type:
                should_retry = self.strategy.should_retry(error_type)
                delay = self.strategy.get_delay(0, error_type)
                
                return ActionResult(
                    success=True,
                    message=f"Error type: {error_type}, should_retry: {should_retry}",
                    data={
                        "strategy": "adaptive",
                        "should_retry": should_retry,
                        "delay": delay if should_retry else None,
                        "error_type": error_type
                    }
                )
            else:
                return ActionResult(
                    success=True,
                    message="Adaptive retry strategy configured",
                    data={
                        "strategy": "adaptive",
                        "error_types": {
                            "transient": list(self.strategy.TRANSIENT_ERRORS),
                            "permanent": list(self.strategy.PERMANENT_ERRORS)
                        }
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class JitterRetryAction(BaseAction):
    """Retry with jitter to prevent thundering herd."""
    action_type = "jitter_retry"
    display_name = "抖动重试"
    description = "使用抖动防止雷鸣般的群体效应"
    
    def __init__(self):
        super().__init__()
        self.strategy = JitterRetry()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            jitter_type = params.get("jitter_type", "full")
            
            self.strategy = JitterRetry(
                max_retries=max_retries,
                base_delay=base_delay,
                jitter_type=jitter_type
            )
            
            delays = [self.strategy.get_delay(i) for i in range(max_retries)]
            
            return ActionResult(
                success=True,
                message=f"Jitter retry strategy configured ({jitter_type})",
                data={
                    "strategy": f"jitter_{jitter_type}",
                    "delays": delays,
                    "total_delay": sum(delays)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
