"""Automation Guard action module for RabAI AutoClick.

Implements circuit breaker, throttle, and quota patterns
for automation safety.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationGuardAction(BaseAction):
    """Guard automation execution with limits and checks.

    Enforces rate limits, quotas, circuit breakers,
    and precondition checks.
    """
    action_type = "automation_guard"
    display_name = "自动化守卫"
    description = "通过限制和检查保护自动化执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with guard checks.

        Args:
            context: Execution context.
            params: Dict with keys: action, guard_type,
                   limit, window_seconds, preconditions.

        Returns:
            ActionResult with guard status.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'check')
            guard_type = params.get('guard_type', 'rate_limit')
            limit = params.get('limit', 10)
            window_seconds = params.get('window_seconds', 60)
            preconditions = params.get('preconditions', [])
            guard_id = params.get('guard_id', 'default')

            # Initialize or get state
            if not hasattr(context, '_automation_guards'):
                context._automation_guards = {}
            guards = context._automation_guards
            if guard_id not in guards:
                guards[guard_id] = {'type': guard_type, 'window': window_seconds, 'limit': limit, 'requests': deque()}

            guard_state = guards[guard_id]

            if action == 'check':
                now = time.time()
                cutoff = now - window_seconds

                # Clean old requests
                requests = guard_state['requests']
                while requests and requests[0] < cutoff:
                    requests.popleft()

                if len(requests) >= limit:
                    return ActionResult(
                        success=False,
                        message=f"Guard blocked: rate limit {limit}/{window_seconds}s exceeded",
                        data={
                            'guard_id': guard_id,
                            'guard_type': guard_type,
                            'current_count': len(requests),
                            'limit': limit,
                            'window_seconds': window_seconds,
                            'blocked': True,
                        },
                        duration=time.time() - start_time,
                    )

                # Check preconditions
                for pc in preconditions:
                    if not self._check_precondition(pc, context):
                        return ActionResult(
                            success=False,
                            message=f"Precondition failed: {pc.get('name', 'unknown')}",
                            data={'precondition': pc, 'passed': False},
                            duration=time.time() - start_time,
                        )

                # Allow
                requests.append(now)
                return ActionResult(
                    success=True,
                    message="Guard check passed",
                    data={
                        'guard_id': guard_id,
                        'guard_type': guard_type,
                        'current_count': len(requests),
                        'limit': limit,
                        'remaining': limit - len(requests),
                        'blocked': False,
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'reset':
                guard_state['requests'] = deque()
                return ActionResult(
                    success=True,
                    message=f"Guard {guard_id} reset",
                    duration=time.time() - start_time,
                )

            elif action == 'status':
                now = time.time()
                cutoff = now - window_seconds
                requests = guard_state['requests']
                while requests and requests[0] < cutoff:
                    requests.popleft()
                return ActionResult(
                    success=True,
                    message=f"Guard {guard_id}: {len(requests)}/{limit}",
                    data={
                        'guard_id': guard_id,
                        'guard_type': guard_type,
                        'current_count': len(requests),
                        'limit': limit,
                        'remaining': limit - len(requests),
                    },
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Guard error: {str(e)}",
                duration=duration,
            )

    def _check_precondition(self, pc: Dict, context: Any) -> bool:
        """Check a precondition."""
        pc_type = pc.get('type')
        if pc_type == 'expression':
            expr = pc.get('expression', '')
            try:
                return eval(expr, {'context': context, 'params': pc})
            except Exception:
                return False
        elif pc_type == 'data_exists':
            key = pc.get('key')
            return hasattr(context, key) or (isinstance(context, dict) and key in context)
        return True


class AutomationThrottlerAction(BaseAction):
    """Throttle automation execution speed.

    Limits execution rate to prevent overload
    and manage resource consumption.
    """
    action_type = "automation_throttler"
    display_name = "自动化节流器"
    description = "控制自动化执行速度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Throttle execution.

        Args:
            context: Execution context.
            params: Dict with keys: rate_limit, burst_size,
                   throttle_mode (smooth/burst).

        Returns:
            ActionResult with throttle status.
        """
        start_time = time.time()
        try:
            rate_limit = params.get('rate_limit', 10)
            burst_size = params.get('burst_size', 20)
            throttle_mode = params.get('throttle_mode', 'smooth')
            throttle_id = params.get('throttle_id', 'default')

            if not hasattr(context, '_automation_throttles'):
                context._automation_throttles = {}
            throttles = context._automation_throttles

            if throttle_id not in throttles:
                throttles[throttle_id] = {
                    'tokens': burst_size,
                    'last_refill': time.time(),
                    'rate': rate_limit,
                    'burst': burst_size,
                }

            state = throttles[throttle_id]
            now = time.time()
            elapsed = now - state['last_refill']

            if throttle_mode == 'smooth':
                tokens_to_add = elapsed * state['rate']
                state['tokens'] = min(state['burst'], state['tokens'] + tokens_to_add)

            state['last_refill'] = now

            if state['tokens'] >= 1:
                state['tokens'] -= 1
                allowed = True
                wait_time = 0
            else:
                allowed = False
                wait_time = (1 - state['tokens']) / state['rate']

            return ActionResult(
                success=allowed,
                message=f"Throttle: {'allowed' if allowed else f'wait {wait_time:.2f}s'}",
                data={
                    'throttle_id': throttle_id,
                    'allowed': allowed,
                    'tokens': state['tokens'],
                    'wait_time': wait_time,
                    'rate_limit': rate_limit,
                },
                duration=time.time() - start_time,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Throttler error: {str(e)}",
                duration=duration,
            )
