"""Rate14 action module for RabAI AutoClick.

Provides additional rate limiting operations:
- RateLimitAction: Check rate limit
- RateLimitTokenAction: Token bucket rate limit
- RateLimitSlidingAction: Sliding window rate limit
- RateLimitFixedAction: Fixed window rate limit
- RateLimitLeakyAction: Leaky bucket rate limit
- RateLimitStatusAction: Get rate limit status
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RateLimitAction(BaseAction):
    """Check rate limit."""
    action_type = "rate14_limit"
    display_name = "限流检查"
    description = "检查是否超过限制"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limit check.

        Args:
            context: Execution context.
            params: Dict with key, limit, window, output_var.

        Returns:
            ActionResult with rate limit status.
        """
        key = params.get('key', 'default')
        limit = params.get('limit', 10)
        window = params.get('window', 60)
        output_var = params.get('output_var', 'rate_limit_status')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else 'default'
            resolved_limit = int(context.resolve_value(limit)) if limit else 10
            resolved_window = int(context.resolve_value(window)) if window else 60

            if not hasattr(context, '_rate_limits'):
                context._rate_limits = {}

            if resolved_key not in context._rate_limits:
                context._rate_limits[resolved_key] = []

            current_time = time.time()
            window_start = current_time - resolved_window

            requests = [t for t in context._rate_limits[resolved_key] if t > window_start]
            context._rate_limits[resolved_key] = requests

            allowed = len(requests) < resolved_limit
            remaining = max(0, resolved_limit - len(requests))

            if allowed:
                context._rate_limits[resolved_key].append(current_time)

            context.set(output_var, {
                'allowed': allowed,
                'remaining': remaining,
                'limit': resolved_limit,
                'reset_at': current_time + resolved_window
            })

            return ActionResult(
                success=True,
                message=f"限流检查: {resolved_key} - {'允许' if allowed else '拒绝'} ({remaining}剩余)",
                data={
                    'key': resolved_key,
                    'allowed': allowed,
                    'remaining': remaining,
                    'limit': resolved_limit,
                    'reset_at': current_time + resolved_window,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"限流检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'limit', 'window']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rate_limit_status'}


class RateLimitTokenAction(BaseAction):
    """Token bucket rate limit."""
    action_type = "rate14_token"
    display_name = "令牌桶限流"
    description = "令牌桶算法限流"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute token bucket.

        Args:
            context: Execution context.
            params: Dict with key, capacity, refill_rate, output_var.

        Returns:
            ActionResult with token bucket status.
        """
        key = params.get('key', 'default')
        capacity = params.get('capacity', 10)
        refill_rate = params.get('refill_rate', 1)
        output_var = params.get('output_var', 'token_bucket_status')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else 'default'
            resolved_capacity = int(context.resolve_value(capacity)) if capacity else 10
            resolved_refill = float(context.resolve_value(refill_rate)) if refill_rate else 1

            if not hasattr(context, '_token_buckets'):
                context._token_buckets = {}

            if resolved_key not in context._token_buckets:
                context._token_buckets[resolved_key] = {
                    'tokens': resolved_capacity,
                    'last_refill': time.time()
                }

            bucket = context._token_buckets[resolved_key]
            current_time = time.time()
            elapsed = current_time - bucket['last_refill']
            bucket['tokens'] = min(resolved_capacity, bucket['tokens'] + elapsed * resolved_refill)
            bucket['last_refill'] = current_time

            allowed = bucket['tokens'] >= 1
            if allowed:
                bucket['tokens'] -= 1

            context.set(output_var, {
                'allowed': allowed,
                'tokens': bucket['tokens'],
                'capacity': resolved_capacity
            })

            return ActionResult(
                success=True,
                message=f"令牌桶: {resolved_key} - {'允许' if allowed else '拒绝'} ({bucket['tokens']:.2f}令牌)",
                data={
                    'key': resolved_key,
                    'allowed': allowed,
                    'tokens': bucket['tokens'],
                    'capacity': resolved_capacity,
                    'refill_rate': resolved_refill,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"令牌桶限流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'capacity', 'refill_rate']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'token_bucket_status'}


class RateLimitSlidingAction(BaseAction):
    """Sliding window rate limit."""
    action_type = "rate14_sliding"
    display_name = "滑动窗口限流"
    description = "滑动窗口算法限流"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sliding window.

        Args:
            context: Execution context.
            params: Dict with key, limit, window, output_var.

        Returns:
            ActionResult with sliding window status.
        """
        key = params.get('key', 'default')
        limit = params.get('limit', 10)
        window = params.get('window', 60)
        output_var = params.get('output_var', 'sliding_window_status')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else 'default'
            resolved_limit = int(context.resolve_value(limit)) if limit else 10
            resolved_window = int(context.resolve_value(window)) if window else 60

            if not hasattr(context, '_sliding_windows'):
                context._sliding_windows = {}

            if resolved_key not in context._sliding_windows:
                context._sliding_windows[resolved_key] = []

            current_time = time.time()
            window_start = current_time - resolved_window

            timestamps = context._sliding_windows[resolved_key]
            timestamps = [t for t in timestamps if t > window_start]
            context._sliding_windows[resolved_key] = timestamps

            allowed = len(timestamps) < resolved_limit
            remaining = max(0, resolved_limit - len(timestamps))

            if allowed:
                timestamps.append(current_time)

            context.set(output_var, {
                'allowed': allowed,
                'remaining': remaining,
                'limit': resolved_limit,
                'window': resolved_window
            })

            return ActionResult(
                success=True,
                message=f"滑动窗口: {resolved_key} - {'允许' if allowed else '拒绝'} ({remaining}剩余)",
                data={
                    'key': resolved_key,
                    'allowed': allowed,
                    'remaining': remaining,
                    'limit': resolved_limit,
                    'window': resolved_window,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"滑动窗口限流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'limit', 'window']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sliding_window_status'}


class RateLimitFixedAction(BaseAction):
    """Fixed window rate limit."""
    action_type = "rate14_fixed"
    display_name = "固定窗口限流"
    description = "固定窗口算法限流"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fixed window.

        Args:
            context: Execution context.
            params: Dict with key, limit, window, output_var.

        Returns:
            ActionResult with fixed window status.
        """
        key = params.get('key', 'default')
        limit = params.get('limit', 10)
        window = params.get('window', 60)
        output_var = params.get('output_var', 'fixed_window_status')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else 'default'
            resolved_limit = int(context.resolve_value(limit)) if limit else 10
            resolved_window = int(context.resolve_value(window)) if window else 60

            if not hasattr(context, '_fixed_windows'):
                context._fixed_windows = {}

            current_time = time.time()
            window_key = int(current_time / resolved_window)

            if resolved_key not in context._fixed_windows:
                context._fixed_windows[resolved_key] = {}

            old_windows = [k for k in context._fixed_windows[resolved_key] if k < window_key - 1]
            for k in old_windows:
                del context._fixed_windows[resolved_key][k]

            if window_key not in context._fixed_windows[resolved_key]:
                context._fixed_windows[resolved_key][window_key] = 0

            count = context._fixed_windows[resolved_key][window_key]
            allowed = count < resolved_limit
            remaining = max(0, resolved_limit - count)

            if allowed:
                context._fixed_windows[resolved_key][window_key] += 1

            reset_at = (window_key + 1) * resolved_window

            context.set(output_var, {
                'allowed': allowed,
                'remaining': remaining,
                'limit': resolved_limit,
                'reset_at': reset_at
            })

            return ActionResult(
                success=True,
                message=f"固定窗口: {resolved_key} - {'允许' if allowed else '拒绝'} ({remaining}剩余)",
                data={
                    'key': resolved_key,
                    'allowed': allowed,
                    'remaining': remaining,
                    'limit': resolved_limit,
                    'reset_at': reset_at,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"固定窗口限流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'limit', 'window']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'fixed_window_status'}


class RateLimitLeakyAction(BaseAction):
    """Leaky bucket rate limit."""
    action_type = "rate14_leaky"
    display_name = "漏桶限流"
    description = "漏桶算法限流"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute leaky bucket.

        Args:
            context: Execution context.
            params: Dict with key, capacity, leak_rate, output_var.

        Returns:
            ActionResult with leaky bucket status.
        """
        key = params.get('key', 'default')
        capacity = params.get('capacity', 10)
        leak_rate = params.get('leak_rate', 1)
        output_var = params.get('output_var', 'leaky_bucket_status')

        try:
            import time

            resolved_key = context.resolve_value(key) if key else 'default'
            resolved_capacity = int(context.resolve_value(capacity)) if capacity else 10
            resolved_leak = float(context.resolve_value(leak_rate)) if leak_rate else 1

            if not hasattr(context, '_leaky_buckets'):
                context._leaky_buckets = {}

            if resolved_key not in context._leaky_buckets:
                context._leaky_buckets[resolved_key] = {
                    'water': 0,
                    'last_leak': time.time()
                }

            bucket = context._leaky_buckets[resolved_key]
            current_time = time.time()
            elapsed = current_time - bucket['last_leak']
            bucket['water'] = max(0, bucket['water'] - elapsed * resolved_leak)
            bucket['last_leak'] = current_time

            allowed = bucket['water'] < resolved_capacity
            if allowed:
                bucket['water'] += 1

            context.set(output_var, {
                'allowed': allowed,
                'water': bucket['water'],
                'capacity': resolved_capacity
            })

            return ActionResult(
                success=True,
                message=f"漏桶: {resolved_key} - {'允许' if allowed else '拒绝'} ({bucket['water']:.2f}水量)",
                data={
                    'key': resolved_key,
                    'allowed': allowed,
                    'water': bucket['water'],
                    'capacity': resolved_capacity,
                    'leak_rate': resolved_leak,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"漏桶限流失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key', 'capacity', 'leak_rate']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'leaky_bucket_status'}


class RateLimitStatusAction(BaseAction):
    """Get rate limit status."""
    action_type = "rate14_status"
    display_name = "限流状态"
    description = "获取限流状态"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limit status.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with rate limit status.
        """
        output_var = params.get('output_var', 'rate_limit_status')

        try:
            result = {
                'rate_limits': hasattr(context, '_rate_limits') and len(context._rate_limits) or 0,
                'token_buckets': hasattr(context, '_token_buckets') and len(context._token_buckets) or 0,
                'sliding_windows': hasattr(context, '_sliding_windows') and len(context._sliding_windows) or 0,
                'fixed_windows': hasattr(context, '_fixed_windows') and len(context._fixed_windows) or 0,
                'leaky_buckets': hasattr(context, '_leaky_buckets') and len(context._leaky_buckets) or 0
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"限流状态: {result}",
                data={
                    'status': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"限流状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rate_limit_status'}