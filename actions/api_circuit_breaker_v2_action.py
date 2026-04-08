"""API Circuit Breaker V2 action module for RabAI AutoClick.

Advanced circuit breaker with half-open state,
adaptive thresholds, and metrics.
"""

import time
import sys
import os
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiCircuitBreakerV2Action(BaseAction):
    """Advanced circuit breaker with half-open state.

    States: CLOSED (normal), OPEN (failing), HALF_OPEN (testing).
    Adaptive thresholds based on sliding window.
    """
    action_type = "api_circuit_breaker_v2"
    display_name = "高级熔断器V2"
    description = "带半开状态的先进熔断器"

    class State:
        CLOSED = "closed"
        OPEN = "open"
        HALF_OPEN = "half_open"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage circuit breaker.

        Args:
            context: Execution context.
            params: Dict with keys: action (call/record/status),
                   circuit_id, failure_threshold, success_threshold,
                   timeout_seconds, half_open_max_calls.

        Returns:
            ActionResult with circuit status.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'call')
            circuit_id = params.get('circuit_id', 'default')
            failure_threshold = params.get('failure_threshold', 5)
            success_threshold = params.get('success_threshold', 3)
            timeout_seconds = params.get('timeout_seconds', 60)
            half_open_max_calls = params.get('half_open_max_calls', 3)
            call_target = params.get('call_target')
            call_args = params.get('call_args', [])
            call_kwargs = params.get('call_kwargs', {})

            if not hasattr(context, '_circuit_breakers'):
                context._circuit_breakers = {}

            if circuit_id not in context._circuit_breakers:
                context._circuit_breakers[circuit_id] = {
                    'state': self.State.CLOSED,
                    'failures': 0,
                    'successes': 0,
                    'last_failure_time': None,
                    'window': deque(maxlen=100),
                    'failure_threshold': failure_threshold,
                    'success_threshold': success_threshold,
                    'timeout': timeout_seconds,
                    'half_open_max': half_open_max_calls,
                    'half_open_calls': 0,
                }

            cb = context._circuit_breakers[circuit_id]
            now = time.time()

            if action == 'status':
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_id}: {cb['state']}",
                    data={
                        'circuit_id': circuit_id,
                        'state': cb['state'],
                        'failures': cb['failures'],
                        'successes': cb['successes'],
                        'last_failure': cb['last_failure_time'],
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'record':
                is_success = params.get('is_success', True)
                return self._record_result(cb, is_success, circuit_id, start_time)

            elif action == 'call':
                return self._execute_with_circuit(cb, now, circuit_id, call_target, call_args, call_kwargs, start_time)

            elif action == 'reset':
                context._circuit_breakers[circuit_id] = {
                    'state': self.State.CLOSED,
                    'failures': 0,
                    'successes': 0,
                    'last_failure_time': None,
                    'window': deque(maxlen=100),
                    'failure_threshold': failure_threshold,
                    'success_threshold': success_threshold,
                    'timeout': timeout_seconds,
                    'half_open_max': half_open_max_calls,
                    'half_open_calls': 0,
                }
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_id} reset",
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
                message=f"Circuit breaker error: {str(e)}",
                duration=duration,
            )

    def _execute_with_circuit(
        self,
        cb: Dict,
        now: float,
        circuit_id: str,
        call_target: Any,
        call_args: list,
        call_kwargs: dict,
        start_time: float
    ) -> ActionResult:
        """Execute with circuit breaker."""
        # Check if should transition from OPEN to HALF_OPEN
        if cb['state'] == self.State.OPEN:
            if cb['last_failure_time'] and (now - cb['last_failure_time']) >= cb['timeout']:
                cb['state'] = self.State.HALF_OPEN
                cb['half_open_calls'] = 0

        if cb['state'] == self.State.OPEN:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Circuit {circuit_id} is OPEN",
                data={
                    'circuit_id': circuit_id,
                    'state': 'open',
                    'blocked': True,
                    'retry_after': cb['timeout'] - (now - (cb['last_failure_time'] or now)),
                },
                duration=duration,
            )

        # Execute call
        if cb['state'] == self.State.HALF_OPEN:
            if cb['half_open_calls'] >= cb['half_open_max']:
                duration = time.time() - start_time
                return ActionResult(
                    success=False,
                    message=f"Circuit {circuit_id} HALF_OPEN max calls reached",
                    data={'circuit_id': circuit_id, 'state': 'half_open', 'blocked': True},
                    duration=duration,
                )
            cb['half_open_calls'] += 1

        try:
            if callable(call_target):
                result = call_target(*call_args, **call_kwargs)
                self._record_result(cb, True, circuit_id, start_time)
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_id}: call succeeded",
                    data={'result': result, 'state': cb['state']},
                    duration=time.time() - start_time,
                )
            else:
                return ActionResult(
                    success=False,
                    message="call_target is not callable",
                    duration=time.time() - start_time,
                )
        except Exception as e:
            self._record_result(cb, False, circuit_id, start_time)
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Circuit {circuit_id}: call failed - {str(e)}",
                data={'error': str(e), 'state': cb['state']},
                duration=duration,
            )

    def _record_result(self, cb: Dict, is_success: bool, circuit_id: str, start_time: float) -> ActionResult:
        """Record call result and update state."""
        now = time.time()
        cb['window'].append({'time': now, 'success': is_success})

        if is_success:
            cb['successes'] += 1
            if cb['state'] == self.State.HALF_OPEN and cb['successes'] >= cb['success_threshold']:
                cb['state'] = self.State.CLOSED
                cb['failures'] = 0
                cb['successes'] = 0
        else:
            cb['failures'] += 1
            cb['last_failure_time'] = now
            if cb['state'] == self.State.HALF_OPEN:
                cb['state'] = self.State.OPEN
            elif cb['failures'] >= cb['failure_threshold']:
                cb['state'] = self.State.OPEN

        return ActionResult(
            success=is_success,
            message=f"Circuit {circuit_id}: {'success' if is_success else 'failure'} recorded",
            data={
                'circuit_id': circuit_id,
                'state': cb['state'],
                'failures': cb['failures'],
                'successes': cb['successes'],
            },
            duration=time.time() - start_time,
        )
