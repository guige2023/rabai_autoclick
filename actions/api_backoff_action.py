"""API backoff action module for RabAI AutoClick.

Provides backoff strategies specifically for API operations:
- ApiBackoffHandler: Handle backoff for rate-limited APIs
- ApiRetryWithBackoff: Execute API calls with automatic backoff
- RateLimitAwareBackoff: Backoff strategies that respect rate limits
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Set
import time
import random
import threading
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackoffTrigger(Enum):
    """What triggered the backoff."""
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    CUSTOM = "custom"


@dataclass
class ApiBackoffConfig:
    """Configuration for API backoff."""
    initial_delay: float = 1.0
    max_delay: float = 300.0
    multiplier: float = 2.0
    jitter: float = 0.1
    max_attempts: int = 5
    respect_retry_after: bool = True
    respect_x_rate_limit: bool = True
    exponential_base: float = 2.0
    fallback_delays: List[float] = field(default_factory=lambda: [1, 2, 5, 10, 30, 60])


class RateLimitInfo:
    """Parsed rate limit information."""
    def __init__(self):
        self.limit: Optional[int] = None
        self.remaining: Optional[int] = None
        self.reset_time: Optional[float] = None
        self.retry_after: Optional[int] = None
    
    @classmethod
    def from_headers(cls, headers: Dict[str, str]) -> "RateLimitInfo":
        """Parse rate limit info from HTTP headers."""
        info = cls()
        
        if "X-RateLimit-Limit" in headers:
            try:
                info.limit = int(headers["X-RateLimit-Limit"])
            except (ValueError, TypeError):
                pass
        
        if "X-RateLimit-Remaining" in headers:
            try:
                info.remaining = int(headers["X-RateLimit-Remaining"])
            except (ValueError, TypeError):
                pass
        
        if "X-RateLimit-Reset" in headers:
            try:
                info.reset_time = float(headers["X-RateLimit-Reset"])
            except (ValueError, TypeError):
                pass
        
        if "Retry-After" in headers:
            try:
                info.retry_after = int(headers["Retry-After"])
            except (ValueError, TypeError):
                pass
        
        return info
    
    def get_wait_time(self) -> float:
        """Get recommended wait time from rate limit info."""
        if self.retry_after:
            return float(self.retry_after)
        if self.reset_time:
            return max(0, self.reset_time - time.time())
        if self.remaining == 0 and self.limit:
            return 60.0
        return 0.0


class ApiBackoffHandler:
    """Handle backoff for API calls."""
    
    def __init__(self, config: Optional[ApiBackoffConfig] = None):
        self.config = config or ApiBackoffConfig()
        self._attempt = 0
        self._last_delay = 0.0
        self._lock = threading.Lock()
        self._rate_limit_history: List[RateLimitInfo] = []
        self._stats = {"total_retries": 0, "rate_limit_retries": 0, "server_error_retries": 0, "network_retries": 0}
    
    def _parse_retry_after(self, response_or_headers: Any) -> Optional[int]:
        """Parse Retry-After from response or headers."""
        try:
            if hasattr(response_or_headers, 'headers'):
                headers = response_or_headers.headers
                if isinstance(headers, dict) and "Retry-After" in headers:
                    return int(headers["Retry-After"])
            elif isinstance(response_or_headers, dict) and "Retry-After" in response_or_headers:
                return int(response_or_headers["Retry-After"])
        except (ValueError, TypeError):
            pass
        return None
    
    def _parse_rate_limit_from_response(self, response: Any) -> Optional[RateLimitInfo]:
        """Extract rate limit info from response."""
        try:
            if hasattr(response, 'headers'):
                headers = dict(response.headers)
                return RateLimitInfo.from_headers(headers)
        except Exception:
            pass
        return None
    
    def _determine_trigger(self, error: Exception, response: Any) -> BackoffTrigger:
        """Determine what triggered the need for backoff."""
        error_msg = str(error).lower()
        error_type = type(error).__name__
        
        if "429" in error_msg or "rate limit" in error_msg or "too many requests" in error_msg:
            return BackoffTrigger.RATE_LIMIT
        if "500" in error_msg or "502" in error_msg or "503" in error_msg or "server error" in error_msg:
            return BackoffTrigger.SERVER_ERROR
        if isinstance(error, (ConnectionError, TimeoutError)) or "timeout" in error_type.lower():
            return BackoffTrigger.NETWORK_ERROR
        if "timeout" in error_msg:
            return BackoffTrigger.TIMEOUT
        return BackoffTrigger.CUSTOM
    
    def compute_delay(self, attempt: int, trigger: BackoffTrigger = BackoffTrigger.CUSTOM, 
                      rate_info: Optional[RateLimitInfo] = None) -> float:
        """Compute backoff delay for given attempt."""
        with self._lock:
            self._attempt = attempt
            
            if rate_info and self.config.respect_retry_after:
                wait_time = rate_info.get_wait_time()
                if wait_time > 0:
                    self._last_delay = wait_time
                    return wait_time
            
            delay = self.config.initial_delay * (self.config.multiplier ** (attempt - 1))
            delay = min(delay, self.config.max_delay)
            
            if self.config.jitter > 0:
                delta = delay * self.config.jitter
                delay += random.uniform(-delta, delta)
            
            self._last_delay = max(0, delay)
            return self._last_delay
    
    def record_retry(self, trigger: BackoffTrigger):
        """Record retry attempt."""
        with self._lock:
            self._stats["total_retries"] += 1
            if trigger == BackoffTrigger.RATE_LIMIT:
                self._stats["rate_limit_retries"] += 1
            elif trigger == BackoffTrigger.SERVER_ERROR:
                self._stats["server_error_retries"] += 1
            elif trigger == BackoffTrigger.NETWORK_ERROR:
                self._stats["network_retries"] += 1
    
    def should_retry(self, attempt: int, error: Exception, response: Any = None) -> Tuple[bool, Optional[BackoffTrigger]]:
        """Determine if operation should be retried."""
        if attempt >= self.config.max_attempts:
            return False, None
        
        trigger = self._determine_trigger(error, response)
        
        if trigger == BackoffTrigger.RATE_LIMIT:
            rate_info = self._parse_rate_limit_from_response(response) if response else None
            if rate_info and rate_info.remaining == 0:
                return True, trigger
        
        if trigger in [BackoffTrigger.RATE_LIMIT, BackoffTrigger.SERVER_ERROR]:
            return True, trigger
        
        if trigger == BackoffTrigger.NETWORK_ERROR:
            return True, trigger
        
        return False, None
    
    def reset(self):
        """Reset backoff state."""
        with self._lock:
            self._attempt = 0
            self._last_delay = 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get backoff statistics."""
        with self._lock:
            return {
                "attempt": self._attempt,
                "last_delay": self._last_delay,
                **dict(self._stats)
            }


class ApiBackoffAction(BaseAction):
    """API backoff action."""
    action_type = "api_backoff"
    display_name = "API退避"
    description = "API请求退避策略"
    
    def __init__(self):
        super().__init__()
        self._handlers: Dict[str, ApiBackoffHandler] = {}
        self._lock = threading.Lock()
    
    def _get_handler(self, name: str, config: Optional[ApiBackoffConfig] = None) -> ApiBackoffHandler:
        """Get or create backoff handler."""
        with self._lock:
            if name not in self._handlers:
                self._handlers[name] = ApiBackoffHandler(config)
            return self._handlers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API operation with backoff."""
        try:
            name = params.get("name", "default")
            operation = params.get("operation")
            operation_args = params.get("args", [])
            operation_kwargs = params.get("kwargs", {})
            
            config = ApiBackoffConfig(
                initial_delay=params.get("initial_delay", 1.0),
                max_delay=params.get("max_delay", 300.0),
                multiplier=params.get("multiplier", 2.0),
                jitter=params.get("jitter", 0.1),
                max_attempts=params.get("max_attempts", 5),
                respect_retry_after=params.get("respect_retry_after", True),
            )
            
            handler = self._get_handler(name, config)
            
            if not operation:
                return ActionResult(success=True, data={"stats": handler.get_stats()})
            
            last_error = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    result = operation(*operation_args, **operation_kwargs)
                    return ActionResult(success=True, data={"result": result, "attempts": attempt})
                
                except Exception as e:
                    last_error = e
                    should_retry, trigger = handler.should_retry(attempt, e)
                    
                    if not should_retry:
                        return ActionResult(
                            success=False,
                            message=f"API call failed after {attempt} attempts: {str(e)}",
                            data={"attempts": attempt, "error": str(e)}
                        )
                    
                    handler.record_retry(trigger or BackoffTrigger.CUSTOM)
                    delay = handler.compute_delay(attempt, trigger or BackoffTrigger.CUSTOM)
                    
                    if attempt < config.max_attempts:
                        time.sleep(delay)
            
            return ActionResult(
                success=False,
                message=f"API call failed after {config.max_attempts} attempts",
                data={"error": str(last_error)}
            )
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiBackoffAction error: {str(e)}")
    
    def get_delay(self, name: str = "default") -> ActionResult:
        """Get next backoff delay without executing."""
        try:
            with self._lock:
                if name in self._handlers:
                    handler = self._handlers[name]
                    handler._lock.acquire()
                    try:
                        delay = handler.compute_delay(handler._attempt + 1)
                        return ActionResult(success=True, data={"delay": delay, "attempt": handler._attempt + 1})
                    finally:
                        handler._lock.release()
                return ActionResult(success=True, data={"delay": 0.0, "attempt": 0})
        except Exception as e:
            return ActionResult(success=False, message=str(e))
    
    def reset(self, name: Optional[str] = None) -> ActionResult:
        """Reset backoff state."""
        try:
            with self._lock:
                if name and name in self._handlers:
                    self._handlers[name].reset()
                else:
                    for h in self._handlers.values():
                        h.reset()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
