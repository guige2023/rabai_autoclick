"""Token bucket rate limiter action module for RabAI AutoClick.

Provides advanced rate limiting using token bucket algorithm:
- TokenBucketRateLimiter: Token bucket rate limiter
- SlidingWindowRateLimiter: Sliding window rate limiter
- LeakyBucketRateLimiter: Leaky bucket for smooth outflow
- MultiTierRateLimiter: Rate limiter with multiple tiers
- AdaptiveRateLimiter: Auto-adjust rate based on success/failure
"""

from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from dataclasses import dataclass
import threading
import time
import logging

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class BucketState:
    """State for a rate limiter bucket."""
    tokens: float
    last_update: float
    request_count: int
    success_count: int
    failure_count: int


class TokenBucketRateLimiter(BaseAction):
    """Token bucket algorithm for rate limiting.
    
    Tokens are added to the bucket at a constant rate.
    Each request consumes tokens. If insufficient tokens,
    the request is rejected or queued.
    """
    
    action_type = "token_bucket_rate_limiter"
    display_name = "令牌桶限流器"
    description = "基于令牌桶算法的速率限制"
    
    def __init__(
        self,
        capacity: float = 100.0,
        refill_rate: float = 10.0,
        initial_tokens: Optional[float] = None
    ) -> None:
        super().__init__()
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._buckets: Dict[str, BucketState] = {}
        self._lock = threading.Lock()
        self._initial_tokens = initial_tokens if initial_tokens is not None else capacity
    
    def _get_bucket(self, key: str) -> BucketState:
        """Get or create bucket for key."""
        if key not in self._buckets:
            self._buckets[key] = BucketState(
                tokens=self._initial_tokens,
                last_update=time.time(),
                request_count=0,
                success_count=0,
                failure_count=0
            )
        return self._buckets[key]
    
    def _refill(self, bucket: BucketState) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket.last_update
        
        new_tokens = elapsed * self._refill_rate
        bucket.tokens = min(self._capacity, bucket.tokens + new_tokens)
        bucket.last_update = now
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Attempt to acquire tokens from the bucket.
        
        Args:
            params: {
                "key": Bucket key (str, required),
                "tokens": Number of tokens to consume (float, default 1.0),
                "blocking": Block until tokens available (bool, default False),
                "timeout": Max wait time in seconds (float, default 10.0),
                "capacity": Override bucket capacity (float),
                "refill_rate": Override refill rate (float)
            }
        """
        try:
            key = params.get("key", "default")
            tokens_to_consume = params.get("tokens", 1.0)
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 10.0)
            
            if params.get("capacity") is not None:
                self._capacity = params["capacity"]
            if params.get("refill_rate") is not None:
                self._refill_rate = params["refill_rate"]
            
            start_time = time.time()
            
            while True:
                with self._lock:
                    bucket = self._get_bucket(key)
                    self._refill(bucket)
                    bucket.request_count += 1
                    
                    if bucket.tokens >= tokens_to_consume:
                        bucket.tokens -= tokens_to_consume
                        bucket.success_count += 1
                        
                        return ActionResult(
                            success=True,
                            message=f"Acquired {tokens_to_consume} tokens",
                            data={
                                "key": key,
                                "tokens_consumed": tokens_to_consume,
                                "tokens_remaining": round(bucket.tokens, 4),
                                "capacity": self._capacity,
                                "refill_rate": self._refill_rate,
                                "request_count": bucket.request_count
                            }
                        )
                    elif blocking:
                        # Calculate wait time
                        tokens_needed = tokens_to_consume - bucket.tokens
                        wait_time = tokens_needed / self._refill_rate
                        
                        if time.time() - start_time + wait_time > timeout:
                            bucket.failure_count += 1
                            return ActionResult(
                                success=False,
                                message=f"Timeout waiting for tokens (needed {wait_time:.2f}s)",
                                data={
                                    "tokens_remaining": round(bucket.tokens, 4),
                                    "tokens_needed": tokens_needed,
                                    "request_count": bucket.request_count
                                }
                            )
                        # Release lock and wait
                    else:
                        bucket.failure_count += 1
                        tokens_needed = tokens_to_consume - bucket.tokens
                        return ActionResult(
                            success=False,
                            message=f"Insufficient tokens: have {bucket.tokens:.2f}, need {tokens_to_consume:.2f}",
                            data={
                                "key": key,
                                "tokens_available": round(bucket.tokens, 4),
                                "tokens_requested": tokens_to_consume,
                                "tokens_needed": round(tokens_needed, 4),
                                "retry_after": round(tokens_needed / self._refill_rate, 4)
                            }
                        )
                
                if blocking:
                    time.sleep(0.01)  # Small sleep to avoid busy-waiting
                    if time.time() - start_time > timeout:
                        return ActionResult(success=False, message="Timeout waiting for tokens")
                else:
                    break
            
            return ActionResult(success=False, message="Failed to acquire tokens")
        
        except Exception as e:
            logger.error(f"Token bucket error: {e}")
            return ActionResult(success=False, message=f"Rate limiter error: {str(e)}")
    
    def get_status(self, key: str = "default") -> Dict[str, Any]:
        """Get current status of a bucket."""
        with self._lock:
            if key not in self._buckets:
                return {"exists": False}
            
            bucket = self._buckets[key]
            self._refill(bucket)
            
            return {
                "exists": True,
                "tokens": round(bucket.tokens, 4),
                "capacity": self._capacity,
                "refill_rate": self._refill_rate,
                "utilization": round((self._capacity - bucket.tokens) / self._capacity * 100, 2),
                "request_count": bucket.request_count,
                "success_count": bucket.success_count,
                "failure_count": bucket.failure_count
            }
    
    def reset(self, key: Optional[str] = None) -> None:
        """Reset bucket(s) to initial state."""
        with self._lock:
            if key:
                if key in self._buckets:
                    del self._buckets[key]
            else:
                self._buckets.clear()


class SlidingWindowRateLimiter(BaseAction):
    """Sliding window rate limiter for more precise rate control.
    
    Tracks requests in a sliding time window, providing smoother
    rate limiting than fixed windows.
    """
    
    action_type = "sliding_window_rate_limiter"
    display_name = "滑动窗口限流器"
    description = "滑动窗口速率限制"
    
    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: float = 60.0
    ) -> None:
        super().__init__()
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._windows: Dict[str, deque] = {}
        self._lock = threading.Lock()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check if request is allowed under sliding window.
        
        Args:
            params: {
                "key": Rate limit key (str, required),
                "max_requests": Override max requests (int),
                "window_seconds": Override window size (float)
            }
        """
        try:
            key = params.get("key", "default")
            max_requests = params.get("max_requests", self._max_requests)
            window_seconds = params.get("window_seconds", self._window_seconds)
            
            now = time.time()
            cutoff = now - window_seconds
            
            with self._lock:
                if key not in self._windows:
                    self._windows[key] = deque()
                
                window = self._windows[key]
                
                # Remove expired entries
                while window and window[0] < cutoff:
                    window.popleft()
                
                if len(window) < max_requests:
                    window.append(now)
                    remaining = max_requests - len(window)
                    
                    return ActionResult(
                        success=True,
                        message=f"Request allowed ({len(window)}/{max_requests})",
                        data={
                            "key": key,
                            "current_count": len(window),
                            "max_requests": max_requests,
                            "remaining": remaining,
                            "reset_after": round(window_seconds, 2)
                        }
                    )
                else:
                    oldest = window[0]
                    retry_after = oldest + window_seconds - now
                    
                    return ActionResult(
                        success=False,
                        message=f"Rate limit exceeded ({max_requests}/{max_requests})",
                        data={
                            "key": key,
                            "current_count": len(window),
                            "max_requests": max_requests,
                            "retry_after": round(max(retry_after, 0), 4),
                            "window_seconds": window_seconds
                        }
                    )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Sliding window error: {str(e)}")
    
    def get_estimate(self, key: str = "default") -> Dict[str, Any]:
        """Get current estimate without modifying window."""
        with self._lock:
            if key not in self._windows:
                return {"current_count": 0, "remaining": self._max_requests}
            
            now = time.time()
            cutoff = now - self._window_seconds
            window = self._windows[key]
            
            # Count non-expired
            valid_count = sum(1 for t in window if t >= cutoff)
            
            return {
                "current_count": valid_count,
                "max_requests": self._max_requests,
                "remaining": self._max_requests - valid_count
            }
    
    def reset(self, key: Optional[str] = None) -> None:
        """Reset window(s)."""
        with self._lock:
            if key and key in self._windows:
                del self._windows[key]
            else:
                self._windows.clear()


class LeakyBucketRateLimiter(BaseAction):
    """Leaky bucket algorithm for smooth outflow rate limiting.
    
    Requests are processed at a constant rate. Excess requests
    are queued or rejected. Unlike token bucket, leaky bucket
    provides guaranteed smooth outflow.
    """
    
    action_type = "leaky_bucket_rate_limiter"
    display_name = "漏桶限流器"
    description = "漏桶算法速率限制"
    
    def __init__(
        self,
        capacity: int = 100,
        leak_rate: float = 10.0  # items per second
    ) -> None:
        super().__init__()
        self._capacity = capacity
        self._leak_rate = leak_rate
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    def _get_bucket(self, key: str) -> Dict[str, Any]:
        """Get or create bucket state."""
        if key not in self._buckets:
            self._buckets[key] = {
                "queue": [],
                "last_leak": time.time(),
                "total_processed": 0,
                "total_rejected": 0
            }
        return self._buckets[key]
    
    def _leak(self, bucket: Dict[str, Any]) -> None:
        """Leak items from bucket based on elapsed time."""
        now = time.time()
        elapsed = now - bucket["last_leak"]
        
        items_to_leak = int(elapsed * self._leak_rate)
        
        if items_to_leak > 0 and bucket["queue"]:
            leaked = min(items_to_leak, len(bucket["queue"]))
            bucket["queue"] = bucket["queue"][leaked:]
            bucket["total_processed"] += leaked
            bucket["last_leak"] = now
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Add item to leaky bucket.
        
        Args:
            params: {
                "key": Bucket key (str, required),
                "item": Item to add (any),
                "blocking": Block until processed (bool, default False),
                "timeout": Max wait time (float, default 10.0),
                "capacity": Override capacity (int),
                "leak_rate": Override leak rate (float)
            }
        """
        try:
            key = params.get("key", "default")
            item = params.get("item")
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 10.0)
            
            if params.get("capacity") is not None:
                self._capacity = params["capacity"]
            if params.get("leak_rate") is not None:
                self._leak_rate = params["leak_rate"]
            
            start_time = time.time()
            
            while True:
                with self._lock:
                    bucket = self._get_bucket(key)
                    self._leak(bucket)
                    
                    if len(bucket["queue"]) < self._capacity:
                        bucket["queue"].append(item)
                        
                        return ActionResult(
                            success=True,
                            message=f"Item added to bucket (queue size: {len(bucket['queue'])})",
                            data={
                                "key": key,
                                "queue_size": len(bucket["queue"]),
                                "capacity": self._capacity,
                                "leak_rate": self._leak_rate,
                                "total_processed": bucket["total_processed"]
                            }
                        )
                    elif blocking:
                        # Calculate wait time for one item to leak
                        wait_time = 1.0 / self._leak_rate
                        if time.time() - start_time + wait_time > timeout:
                            bucket["total_rejected"] += 1
                            return ActionResult(
                                success=False,
                                message="Timeout waiting for bucket space"
                            )
                    else:
                        bucket["total_rejected"] += 1
                        return ActionResult(
                            success=False,
                            message=f"Bucket full (capacity: {self._capacity})",
                            data={
                                "key": key,
                                "queue_size": len(bucket["queue"]),
                                "capacity": self._capacity,
                                "retry_after": round(1.0 / self._leak_rate, 4)
                            }
                        )
                
                if blocking:
                    time.sleep(0.01)
                    if time.time() - start_time > timeout:
                        return ActionResult(success=False, message="Timeout waiting for bucket space")
                else:
                    break
            
            return ActionResult(success=False, message="Failed to add to bucket")
        
        except Exception as e:
            return ActionResult(success=False, message=f"Leaky bucket error: {str(e)}")
    
    def get_status(self, key: str = "default") -> Dict[str, Any]:
        """Get bucket status."""
        with self._lock:
            if key not in self._buckets:
                return {"exists": False}
            
            bucket = self._get_bucket(key)
            self._leak(bucket)
            
            return {
                "queue_size": len(bucket["queue"]),
                "capacity": self._capacity,
                "utilization": round(len(bucket["queue"]) / self._capacity * 100, 2),
                "total_processed": bucket["total_processed"],
                "total_rejected": bucket["total_rejected"]
            }


class MultiTierRateLimiter(BaseAction):
    """Multi-tier rate limiter with different limits per tier.
    
    Supports different rate limits for different client tiers
    (e.g., free vs paid users).
    """
    
    action_type = "multi_tier_rate_limiter"
    display_name = "多层级限流器"
    description = "支持多层级不同限流策略"
    
    def __init__(self) -> None:
        super().__init__()
        self._tiers: Dict[str, Dict[str, float]] = {
            "free": {"requests": 60, "window": 60.0, "tokens": 100, "refill": 10},
            "basic": {"requests": 300, "window": 60.0, "tokens": 500, "refill": 50},
            "premium": {"requests": 1000, "window": 60.0, "tokens": 2000, "refill": 200}
        }
        self._sliding_windows: Dict[str, deque] = {}
        self._token_buckets: Dict[str, TokenBucketRateLimiter] = {}
        self._lock = threading.Lock()
    
    def configure_tier(self, tier: str, config: Dict[str, float]) -> None:
        """Configure a tier's rate limits."""
        self._tiers[tier] = config
        # Reset relevant limiters
        if tier in self._sliding_windows:
            del self._sliding_windows[tier]
        if tier in self._token_buckets:
            self._token_buckets[tier].reset()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check rate limit for tier.
        
        Args:
            params: {
                "tier": Client tier (str, required),
                "client_id": Client identifier (str, required),
                "check_requests": Check request count limit (bool, default True),
                "check_tokens": Check token bucket limit (bool, default True),
                "tokens_to_consume": Tokens for token bucket (float, default 1.0)
            }
        """
        try:
            tier = params.get("tier", "free")
            client_id = params.get("client_id", "unknown")
            check_requests = params.get("check_requests", True)
            check_tokens = params.get("check_tokens", True)
            tokens_to_consume = params.get("tokens_to_consume", 1.0)
            
            if tier not in self._tiers:
                return ActionResult(success=False, message=f"Unknown tier: {tier}")
            
            config = self._tiers[tier]
            key = f"{tier}:{client_id}"
            
            results = {}
            
            # Check sliding window
            if check_requests:
                sw_result = self._check_sliding_window(
                    key,
                    int(config["requests"]),
                    config["window"]
                )
                results["sliding_window"] = sw_result
                if not sw_result["allowed"]:
                    return ActionResult(
                        success=False,
                        message=f"Request limit exceeded for {tier} tier",
                        data={"tier": tier, "client_id": client_id, "limits": sw_result}
                    )
            
            # Check token bucket
            if check_tokens:
                tb_result = self._check_token_bucket(key, tokens_to_consume)
                results["token_bucket"] = tb_result
                if not tb_result["allowed"]:
                    return ActionResult(
                        success=False,
                        message=f"Token limit exceeded for {tier} tier",
                        data={"tier": tier, "client_id": client_id, "limits": tb_result}
                    )
            
            return ActionResult(
                success=True,
                message=f"Rate limit check passed for {tier} tier",
                data={"tier": tier, "client_id": client_id, "limits": results}
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-tier limiter error: {str(e)}")
    
    def _check_sliding_window(
        self,
        key: str,
        max_requests: int,
        window_seconds: float
    ) -> Dict[str, Any]:
        """Check sliding window limit."""
        now = time.time()
        cutoff = now - window_seconds
        
        with self._lock:
            if key not in self._sliding_windows:
                self._sliding_windows[key] = deque()
            
            window = self._sliding_windows[key]
            
            # Remove expired
            while window and window[0] < cutoff:
                window.popleft()
            
            if len(window) < max_requests:
                window.append(now)
                return {
                    "allowed": True,
                    "current": len(window),
                    "max": max_requests,
                    "remaining": max_requests - len(window)
                }
            else:
                oldest = window[0]
                retry_after = oldest + window_seconds - now
                return {
                    "allowed": False,
                    "current": len(window),
                    "max": max_requests,
                    "retry_after": round(max(retry_after, 0), 4)
                }
    
    def _check_token_bucket(self, key: str, tokens: float) -> Dict[str, Any]:
        """Check token bucket limit."""
        with self._lock:
            if key not in self._token_buckets:
                self._token_buckets[key] = TokenBucketRateLimiter(
                    capacity=1000,  # Large capacity for tier-based
                    refill_rate=100
                )
        
        result = self._token_buckets[key].execute(
            None,  # type: ignore
            {"key": key, "tokens": tokens}
        )
        
        return {
            "allowed": result.success,
            "tokens_remaining": result.data.get("tokens_remaining") if result.data else 0,
            "retry_after": result.data.get("retry_after") if not result.success else 0
        }


class AdaptiveRateLimiter(BaseAction):
    """Adaptive rate limiter that adjusts based on success/failure rates.
    
    Automatically reduces rate limits when errors increase
    and restores them when the system stabilizes.
    """
    
    action_type = "adaptive_rate_limiter"
    display_name = "自适应限流器"
    description = "根据成功率自动调整限流参数"
    
    def __init__(
        self,
        initial_rate: float = 100.0,
        min_rate: float = 10.0,
        max_rate: float = 1000.0,
        target_success_rate: float = 0.95
    ) -> None:
        super().__init__()
        self._current_rate = initial_rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._target_success_rate = target_success_rate
        self._success_window: deque = deque(maxlen=100)
        self._last_adjustment = time.time()
        self._adjustment_interval = 5.0  # seconds
        self._lock = threading.Lock()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Record request result and check next request.
        
        Args:
            params: {
                "key": Rate limit key (str, required),
                "success": Whether request succeeded (bool, required),
                "blocking": Block until allowed (bool, default False)
            }
        """
        try:
            key = params.get("key", "default")
            success = params.get("success", True)
            blocking = params.get("blocking", False)
            
            # Record result
            with self._lock:
                self._success_window.append(1.0 if success else 0.0)
            
            # Periodically adjust rate
            now = time.time()
            if now - self._last_adjustment >= self._adjustment_interval:
                self._adjust_rate()
                self._last_adjustment = now
            
            # Check if request is allowed
            with self._lock:
                if len(self._success_window) >= 10:
                    success_rate = sum(self._success_window) / len(self._success_window)
                    
                    if success_rate < self._target_success_rate:
                        # Too many failures, reduce rate
                        self._current_rate = max(self._min_rate, self._current_rate * 0.8)
                        
                        return ActionResult(
                            success=False,
                            message=f"Adaptive limit: rate reduced to {self._current_rate:.2f}/s",
                            data={
                                "allowed": False,
                                "current_rate": round(self._current_rate, 2),
                                "success_rate": round(success_rate, 4),
                                "reason": "success_rate_below_target"
                            }
                        )
            
            return ActionResult(
                success=True,
                message=f"Request allowed at rate {self._current_rate:.2f}/s",
                data={
                    "allowed": True,
                    "current_rate": round(self._current_rate, 2),
                    "min_rate": self._min_rate,
                    "max_rate": self._max_rate
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive limiter error: {str(e)}")
    
    def _adjust_rate(self) -> None:
        """Adjust rate based on recent success/failure history."""
        if len(self._success_window) < 10:
            return
        
        success_rate = sum(self._success_window) / len(self._success_window)
        
        if success_rate >= self._target_success_rate:
            # Good performance, gradually increase
            self._current_rate = min(self._max_rate, self._current_rate * 1.1)
        else:
            # Degraded performance, reduce
            self._current_rate = max(self._min_rate, self._current_rate * 0.8)
    
    def record_result(self, success: bool) -> None:
        """Manually record a request result."""
        with self._lock:
            self._success_window.append(1.0 if success else 0.0)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current limiter status."""
        with self._lock:
            success_rate = 0.0
            if len(self._success_window) >= 10:
                success_rate = sum(self._success_window) / len(self._success_window)
            
            return {
                "current_rate": round(self._current_rate, 2),
                "min_rate": self._min_rate,
                "max_rate": self._max_rate,
                "target_success_rate": self._target_success_rate,
                "actual_success_rate": round(success_rate, 4),
                "window_size": len(self._success_window)
            }
