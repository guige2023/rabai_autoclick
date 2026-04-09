"""Sliding window rate limiter action module for RabAI AutoClick.

Provides sliding window rate limiting:
- SlidingWindowLimiter: Token bucket with sliding window
- RateLimitMonitor: Monitor rate limit metrics
- DistributedRateLimiter: Redis-backed distributed rate limiter
"""

from __future__ import annotations

import time
import sys
import os
from typing import Any, Dict, List, Optional
from collections import deque
from dataclasses import dataclass

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class SlidingWindowLimiterAction(BaseAction):
    """Sliding window rate limiter."""
    action_type = "sliding_window_limiter"
    display_name = "滑动窗口限流"
    description = "滑动窗口算法限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            key = params.get("key", "default")
            limit = params.get("limit", 100)
            window_seconds = params.get("window_seconds", 60)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            limiter_path = os.path.join("/tmp/rate_limiter", key)
            os.makedirs(limiter_path, exist_ok=True)
            timestamps_file = os.path.join(limiter_path, "timestamps.json")

            if use_redis:
                client = redis.from_url(redis_url)
                now = time.time()
                window_start = now - window_seconds

                pipe = client.pipeline()
                pipe.zremrangebyscore(key, 0, window_start)
                pipe.zcard(key)
                pipe.zadd(key, {str(now): now})
                pipe.expire(key, window_seconds)
                results = pipe.execute()
                count = results[1]

                if count >= limit:
                    client.zremrangebyscore(key, 0, window_start)
                    return ActionResult(success=False, message=f"Rate limit exceeded: {count}/{limit}", data={"allowed": False, "count": count, "limit": limit})

                return ActionResult(success=True, message=f"Allowed: {count}/{limit}", data={"allowed": True, "count": count, "limit": limit, "remaining": limit - count})

            else:
                import json
                now = time.time()
                window_start = now - window_seconds

                timestamps = []
                if os.path.exists(timestamps_file):
                    with open(timestamps_file) as f:
                        timestamps = json.load(f)

                timestamps = [t for t in timestamps if t > window_start]

                if operation == "check":
                    if len(timestamps) >= limit:
                        return ActionResult(success=False, message=f"Rate limit exceeded: {len(timestamps)}/{limit}", data={"allowed": False, "count": len(timestamps), "limit": limit})

                    timestamps.append(now)
                    with open(timestamps_file, "w") as f:
                        json.dump(timestamps, f)

                    return ActionResult(success=True, message=f"Allowed: {len(timestamps)}/{limit}", data={"allowed": True, "count": len(timestamps), "limit": limit, "remaining": limit - len(timestamps)})

                elif operation == "get":
                    return ActionResult(success=True, message=f"Count: {len(timestamps)}/{limit}", data={"count": len(timestamps), "limit": limit, "remaining": limit - len(timestamps)})

                elif operation == "reset":
                    timestamps = []
                    with open(timestamps_file, "w") as f:
                        json.dump(timestamps, f)
                    return ActionResult(success=True, message="Rate limit reset")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class RateLimitMonitorAction(BaseAction):
    """Monitor rate limit metrics."""
    action_type = "rate_limit_monitor"
    display_name = "限流监控"
    description = "监控限流指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limiter_path = os.path.join("/tmp/rate_limiter")
            if not os.path.exists(limiter_path):
                return ActionResult(success=True, message="No rate limiters", data={"limiters": [], "total_keys": 0})

            import json
            limiters = []
            total_requests = 0
            total_rejected = 0

            for key_dir in os.listdir(limiter_path):
                key_path = os.path.join(limiter_path, key_dir)
                if os.path.isdir(key_path):
                    timestamps_file = os.path.join(key_path, "timestamps.json")
                    if os.path.exists(timestamps_file):
                        with open(timestamps_file) as f:
                            timestamps = json.load(f)
                        limiters.append({"key": key_dir, "current_count": len(timestamps)})
                        total_requests += len(timestamps)

            return ActionResult(
                success=True,
                message=f"Monitored {len(limiters)} limiters",
                data={"limiters": limiters, "total_requests": total_requests, "total_keys": len(limiters)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
