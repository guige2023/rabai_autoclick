"""Data throttle action module for RabAI AutoClick.

Provides throttling mechanisms for data operations:
- DataThrottler: Rate-limiting for data operations
- DataThrottlePolicy: Configurable throttle policies
- DataThroughputController: Throughput control for data pipelines
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ThrottleMode(Enum):
    """Throttling modes."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class DataThrottleConfig:
    """Configuration for data throttling."""
    mode: ThrottleMode = ThrottleMode.TOKEN_BUCKET
    rate: float = 100.0
    burst: float = 10.0
    window_size: float = 1.0
    max_queue_size: int = 1000
    block_on_full: bool = False
    priority_enabled: bool = False


class TokenBucketThrottler:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self._lock = threading.Lock()
    
    def consume(self, tokens: float = 1.0) -> Tuple[bool, float]:
        """Try to consume tokens. Returns (success, wait_time)."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True, 0.0
            else:
                wait_time = (tokens - self.tokens) / self.rate
                return False, wait_time


class SlidingWindowThrottler:
    """Sliding window rate limiter."""
    
    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.requests = deque()
        self._lock = threading.Lock()
    
    def acquire(self) -> Tuple[bool, float]:
        """Try to acquire. Returns (success, wait_time)."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_size
            
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            if len(self.requests) < self.rate * self.window_size:
                self.requests.append(now)
                return True, 0.0
            else:
                oldest = self.requests[0]
                wait_time = oldest + self.window_size - now
                return False, max(0, wait_time)


class DataThrottler:
    """Main data throttling coordinator."""
    
    def __init__(self, config: Optional[DataThrottleConfig] = None):
        self.config = config or DataThrottleConfig()
        self._throttlers: Dict[str, Any] = {}
        self._queues: Dict[str, deque] = {}
        self._lock = threading.Lock()
        self._stats = {"total_requests": 0, "total_accepted": 0, "total_rejected": 0, "total_waited": 0.0}
        self._init_throttlers()
    
    def _init_throttlers(self):
        """Initialize throttlers based on mode."""
        for name in ["default", "read", "write", "batch"]:
            if self.config.mode == ThrottleMode.TOKEN_BUCKET:
                self._throttlers[name] = TokenBucketThrottler(self.config.rate, self.config.burst)
            elif self.config.mode == ThrottleMode.SLIDING_WINDOW:
                self._throttlers[name] = SlidingWindowThrottler(self.config.rate, self.config.window_size)
            self._queues[name] = deque(maxlen=self.config.max_queue_size)
    
    def acquire(self, name: str = "default", tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Acquire throttle slot."""
        with self._lock:
            self._stats["total_requests"] += 1
            
            if name not in self._throttlers:
                if self.config.mode == ThrottleMode.TOKEN_BUCKET:
                    self._throttlers[name] = TokenBucketThrottler(self.config.rate, self.config.burst)
                else:
                    self._throttlers[name] = SlidingWindowThrottler(self.config.rate, self.config.window_size)
                self._queues[name] = deque(maxlen=self.config.max_queue_size)
            
            throttler = self._throttlers[name]
            
            if hasattr(throttler, 'consume'):
                success, wait_time = throttler.consume(tokens)
            else:
                success, wait_time = throttler.acquire()
            
            if success:
                self._stats["total_accepted"] += 1
                return True
            
            if timeout is not None and timeout <= 0:
                self._stats["total_rejected"] += 1
                return False
            
            start_wait = time.time()
            while True:
                if hasattr(throttler, 'consume'):
                    success, wait_time = throttler.consume(tokens)
                else:
                    success, wait_time = throttler.acquire()
                
                if success:
                    self._stats["total_accepted"] += 1
                    self._stats["total_waited"] += time.time() - start_wait
                    return True
                
                if timeout is not None and (time.time() - start_wait) >= timeout:
                    self._stats["total_rejected"] += 1
                    return False
                
                time.sleep(min(wait_time, 0.1))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get throttle statistics."""
        with self._lock:
            return dict(self._stats)


class DataThrottleAction(BaseAction):
    """Data throttling action."""
    action_type = "data_throttle"
    display_name = "数据节流"
    description = "数据操作速率限制"
    
    def __init__(self):
        super().__init__()
        self._throttler: Optional[DataThrottler] = None
        self._lock = threading.Lock()
    
    def _get_throttler(self, params: Dict[str, Any]) -> DataThrottler:
        """Get or create throttler."""
        with self._lock:
            if self._throttler is None:
                mode_str = params.get("mode", "token_bucket")
                mode = ThrottleMode[mode_str.upper().replace("-", "_")] if mode_str != "default" else ThrottleMode.TOKEN_BUCKET
                config = DataThrottleConfig(
                    mode=mode,
                    rate=params.get("rate", 100.0),
                    burst=params.get("burst", 10.0),
                    window_size=params.get("window_size", 1.0),
                    max_queue_size=params.get("max_queue_size", 1000),
                )
                self._throttler = DataThrottler(config)
            return self._throttler
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute throttled data operation."""
        try:
            operation = params.get("operation")
            operation_args = params.get("args", [])
            operation_kwargs = params.get("kwargs", {})
            bucket = params.get("bucket", "default")
            tokens = params.get("tokens", 1.0)
            timeout = params.get("timeout")
            
            throttler = self._get_throttler(params)
            
            if operation is None:
                acquired = throttler.acquire(bucket, tokens, timeout)
                if not acquired:
                    return ActionResult(success=False, message="Throttle timeout - could not acquire slot")
                return ActionResult(success=True, message="Throttle slot acquired")
            
            acquired = throttler.acquire(bucket, tokens, timeout)
            if not acquired:
                return ActionResult(success=False, message="Throttle timeout")
            
            try:
                result = operation(*operation_args, **operation_kwargs)
                return ActionResult(success=True, data={"result": result})
            except Exception as e:
                return ActionResult(success=False, message=f"Operation error: {str(e)}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataThrottleAction error: {str(e)}")
    
    def get_stats(self) -> ActionResult:
        """Get throttling statistics."""
        try:
            if self._throttler:
                return ActionResult(success=True, data=self._throttler.get_stats())
            return ActionResult(success=True, data={})
        except Exception as e:
            return ActionResult(success=False, message=str(e))
