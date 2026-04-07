"""Rate limiting action module for RabAI AutoClick.

Provides rate limiting operations:
- RateLimitCheckAction: Check if action is rate limited
- RateLimitWaitAction: Wait until rate limit resets
- RateLimitResetAction: Reset rate limit
- RateLimitConfigAction: Configure rate limits
"""

from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RateLimitCheckAction(BaseAction):
    """Check if action is rate limited."""
    action_type = "rate_limit_check"
    display_name = "速率限制检查"
    description = "检查速率限制"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit check."""
        key = params.get('key', 'default')
        max_calls = params.get('max_calls', 10)
        window = params.get('window', 60)  # seconds
        output_var = params.get('output_var', 'rate_limit_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_max = context.resolve_value(max_calls) if context else max_calls
            resolved_window = context.resolve_value(window) if context else window

            if not hasattr(context, '_rate_limits'):
                context._rate_limits = {}
            if resolved_key not in context._rate_limits:
                context._rate_limits[resolved_key] = {'count': 0, 'window_start': time.time(), 'max': int(resolved_max), 'window': int(resolved_window)}

            limit = context._rate_limits[resolved_key]
            current_time = time.time()

            # Check if window has expired
            if current_time - limit['window_start'] >= limit['window']:
                limit['count'] = 0
                limit['window_start'] = current_time

            allowed = limit['count'] < limit['max']
            remaining = max(0, limit['max'] - limit['count'])
            reset_in = max(0, limit['window'] - (current_time - limit['window_start']))

            result = {
                'allowed': allowed,
                'remaining': remaining,
                'limit': limit['max'],
                'reset_in': reset_in,
                'key': resolved_key,
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=allowed, message=f"Rate limit: {'allowed' if allowed else 'blocked'} ({remaining} remaining)", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit check error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': 'default', 'max_calls': 10, 'window': 60, 'output_var': 'rate_limit_result'}


class RateLimitConsumeAction(BaseAction):
    """Consume a rate limit slot."""
    action_type = "rate_limit_consume"
    display_name = "速率限制消耗"
    description = "消耗速率限制配额"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit consume."""
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'rate_limit_consume_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            if not hasattr(context, '_rate_limits') or resolved_key not in context._rate_limits:
                return ActionResult(success=False, message=f"No rate limit configured for: {resolved_key}")

            limit = context._rate_limits[resolved_key]
            current_time = time.time()

            if current_time - limit['window_start'] >= limit['window']:
                limit['count'] = 0
                limit['window_start'] = current_time

            if limit['count'] < limit['max']:
                limit['count'] += 1
                remaining = limit['max'] - limit['count']
                result = {'consumed': True, 'remaining': remaining, 'key': resolved_key}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Rate limit slot consumed ({remaining} remaining)", data=result)
            else:
                result = {'consumed': False, 'remaining': 0, 'key': resolved_key}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=False, message="Rate limit exceeded", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit consume error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rate_limit_consume_result'}


class RateLimitResetAction(BaseAction):
    """Reset rate limit."""
    action_type = "rate_limit_reset"
    display_name = "速率限制重置"
    description = "重置速率限制"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit reset."""
        key = params.get('key', None)  # None = reset all
        output_var = params.get('output_var', 'rate_limit_reset_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            if resolved_key:
                if hasattr(context, '_rate_limits') and resolved_key in context._rate_limits:
                    del context._rate_limits[resolved_key]
                result = {'reset': True, 'key': resolved_key}
            else:
                if hasattr(context, '_rate_limits'):
                    count = len(context._rate_limits)
                    context._rate_limits.clear()
                else:
                    count = 0
                result = {'reset': True, 'keys_cleared': count}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Rate limit reset", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit reset error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': None, 'output_var': 'rate_limit_reset_result'}
