"""API Throttle action module for RabAI AutoClick.

Provides API throttling operations:
- ThrottleRateAction: Rate limiting
- ThrottleQuotaAction: Quota management
- ThrottleBurstAction: Burst limiting
- ThrottleAdaptiveAction: Adaptive throttling
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional
from collections import defaultdict, deque

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ThrottleRateAction(BaseAction):
    """Rate limiting."""
    action_type = "throttle_rate"
    display_name = "速率限制"
    description = "API速率限制"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._buckets = defaultdict(lambda: {'tokens': 100, 'last_refill': time.time()})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limiting."""
        key = params.get('key', 'default')
        rate = params.get('rate', 100)
        period = params.get('period', 60)
        cost = params.get('cost', 1)
        output_var = params.get('output_var', 'throttle_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_rate = context.resolve_value(rate) if context else rate
            resolved_period = context.resolve_value(period) if context else period

            bucket = self._buckets[resolved_key]
            now = time.time()

            elapsed = now - bucket['last_refill']
            tokens_to_add = (elapsed / resolved_period) * resolved_rate
            bucket['tokens'] = min(resolved_rate, bucket['tokens'] + tokens_to_add)
            bucket['last_refill'] = now

            allowed = bucket['tokens'] >= cost
            if allowed:
                bucket['tokens'] -= cost

            result = {
                'allowed': allowed,
                'key': resolved_key,
                'remaining': int(bucket['tokens']),
                'limit': resolved_rate,
                'reset_in': int(resolved_period * (1 - bucket['tokens'] / resolved_rate)) if bucket['tokens'] < resolved_rate else 0,
            }

            return ActionResult(
                success=allowed,
                data={output_var: result},
                message=f"Rate limit: {'allowed' if allowed else 'limited'}, {result['remaining']}/{resolved_rate} remaining"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle rate error: {e}")


class ThrottleQuotaAction(BaseAction):
    """Quota management."""
    action_type = "throttle_quota"
    display_name = "配额管理"
    description = "API配额管理"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._quotas = defaultdict(lambda: {'used': 0, 'limit': 1000, 'reset_at': time.time() + 86400})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute quota check."""
        key = params.get('key', 'default')
        cost = params.get('cost', 1)
        output_var = params.get('output_var', 'quota_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_cost = context.resolve_value(cost) if context else cost

            quota = self._quotas[resolved_key]
            now = time.time()

            if now >= quota['reset_at']:
                quota['used'] = 0
                quota['reset_at'] = now + 86400

            if quota['used'] + resolved_cost <= quota['limit']:
                quota['used'] += resolved_cost
                allowed = True
            else:
                allowed = False

            result = {
                'allowed': allowed,
                'key': resolved_key,
                'used': quota['used'],
                'limit': quota['limit'],
                'remaining': quota['limit'] - quota['used'],
                'reset_at': quota['reset_at'],
            }

            return ActionResult(
                success=allowed,
                data={output_var: result},
                message=f"Quota: {'allowed' if allowed else 'exceeded'}, {result['remaining']} remaining"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle quota error: {e}")


class ThrottleBurstAction(BaseAction):
    """Burst limiting."""
    action_type = "throttle_burst"
    display_name = "突发限制"
    description = "API突发限制"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._bursts = defaultdict(lambda: deque(maxlen=10))

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute burst limiting."""
        key = params.get('key', 'default')
        burst_limit = params.get('burst_limit', 10)
        window_seconds = params.get('window', 1)
        output_var = params.get('output_var', 'burst_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_limit = context.resolve_value(burst_limit) if context else burst_limit
            resolved_window = context.resolve_value(window_seconds) if context else window_seconds

            now = time.time()
            burst = self._bursts[resolved_key]

            while burst and burst[0] < now - resolved_window:
                burst.popleft()

            if len(burst) < resolved_limit:
                burst.append(now)
                allowed = True
            else:
                allowed = False

            result = {
                'allowed': allowed,
                'key': resolved_key,
                'current_burst': len(burst),
                'burst_limit': resolved_limit,
            }

            return ActionResult(
                success=allowed,
                data={output_var: result},
                message=f"Burst: {'allowed' if allowed else 'limited'} ({len(burst)}/{resolved_limit})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle burst error: {e}")


class ThrottleAdaptiveAction(BaseAction):
    """Adaptive throttling."""
    action_type = "throttle_adaptive"
    display_name = "自适应限制"
    description = "自适应API限制"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._state = defaultdict(lambda: {'load': 0, 'rate': 100, 'last_update': time.time()})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute adaptive throttling."""
        key = params.get('key', 'default')
        load = params.get('load', 0)
        base_rate = params.get('base_rate', 100)
        output_var = params.get('output_var', 'adaptive_result')

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_load = context.resolve_value(load) if context else load

            state = self._state[resolved_key]
            now = time.time()

            decay = min(1.0, (now - state['last_update']) / 60)
            state['load'] = state['load'] * (1 - decay) + resolved_load * decay
            state['last_update'] = now

            load_factor = 1.0 - (state['load'] / 100.0)
            current_rate = int(base_rate * max(0.1, load_factor))

            result = {
                'key': resolved_key,
                'current_rate': current_rate,
                'load': state['load'],
                'base_rate': base_rate,
                'load_factor': load_factor,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Adaptive: rate={current_rate}, load={state['load']:.1f}%"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle adaptive error: {e}")
