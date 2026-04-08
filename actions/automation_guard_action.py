"""Automation guard action module for RabAI AutoClick.

Provides guard conditions for workflow automation including
pre-condition checks, post-condition validation, resource checks,
and circuit breaker patterns.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GuardConditionAction(BaseAction):
    """Evaluate guard conditions before workflow execution.
    
    Checks pre-conditions such as file existence, variable values,
    time windows, and custom conditions before allowing execution.
    """
    action_type = "guard_condition"
    display_name = "条件守卫"
    description = "在工作流执行前检查前置条件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate guard conditions.
        
        Args:
            context: Execution context.
            params: Dict with keys: conditions (list of condition dicts),
                   mode (all|any), fail_fast.
        
        Returns:
            ActionResult with evaluation result.
        """
        conditions = params.get('conditions', [])
        mode = params.get('mode', 'all')
        fail_fast = params.get('fail_fast', True)
        start_time = time.time()

        if not conditions:
            return ActionResult(
                success=True,
                message="No conditions to evaluate",
                data={'passed': True, 'conditions': []}
            )

        results = []
        for cond in conditions:
            cond_name = cond.get('name', 'unnamed')
            cond_type = cond.get('type', 'always_pass')
            cond_params = cond.get('params', {})

            passed = self._evaluate_condition(context, cond_type, cond_params)
            results.append({
                'name': cond_name,
                'type': cond_type,
                'passed': passed,
                'params': cond_params
            })

            if not passed and fail_fast and mode == 'all':
                return ActionResult(
                    success=False,
                    message=f"Guard condition '{cond_name}' failed",
                    data={
                        'passed': False,
                        'conditions': results,
                        'failed_condition': cond_name
                    },
                    duration=time.time() - start_time
                )

        if mode == 'all':
            all_passed = all(r['passed'] for r in results)
        else:
            all_passed = any(r['passed'] for r in results)

        return ActionResult(
            success=all_passed,
            message=f"Guard conditions: {sum(r['passed'] for r in results)}/{len(results)} passed",
            data={
                'passed': all_passed,
                'conditions': results,
                'mode': mode
            },
            duration=time.time() - start_time
        )

    def _evaluate_condition(
        self,
        context: Any,
        cond_type: str,
        params: Dict[str, Any]
    ) -> bool:
        """Evaluate a single condition."""
        if cond_type == 'always_pass':
            return True
        elif cond_type == 'always_fail':
            return False
        elif cond_type == 'time_window':
            return self._check_time_window(params)
        elif cond_type == 'variable_equals':
            return self._check_variable_equals(context, params)
        elif cond_type == 'variable_exists':
            return self._check_variable_exists(context, params)
        elif cond_type == 'file_exists':
            return self._check_file_exists(params)
        elif cond_type == 'custom':
            return self._check_custom(params)
        return True

    def _check_time_window(self, params: Dict[str, Any]) -> bool:
        """Check if current time is within specified window."""
        import datetime
        now = datetime.datetime.now()
        start_hour = params.get('start_hour', 0)
        end_hour = params.get('end_hour', 24)
        days_of_week = params.get('days_of_week', list(range(7)))
        current_hour = now.hour
        current_day = now.weekday()
        if current_day not in days_of_week:
            return False
        if start_hour <= end_hour:
            return start_hour <= current_hour < end_hour
        else:
            return current_hour >= start_hour or current_hour < end_hour

    def _check_variable_equals(self, context: Any, params: Dict[str, Any]) -> bool:
        """Check if context variable equals expected value."""
        var_name = params.get('variable', '')
        expected = params.get('expected')
        actual = getattr(context, var_name, None) if hasattr(context, var_name) else None
        if actual is None and var_name in getattr(context, 'variables', {}):
            actual = context.variables.get(var_name)
        return actual == expected

    def _check_variable_exists(self, context: Any, params: Dict[str, Any]) -> bool:
        """Check if context variable exists."""
        var_name = params.get('variable', '')
        if hasattr(context, var_name):
            return True
        if hasattr(context, 'variables') and var_name in context.variables:
            return True
        return False

    def _check_file_exists(self, params: Dict[str, Any]) -> bool:
        """Check if file exists."""
        import os
        file_path = params.get('path', '')
        return os.path.exists(file_path) if file_path else False

    def _check_custom(self, params: Dict[str, Any]) -> bool:
        """Check custom condition."""
        return params.get('result', True)


class ResourceGuardAction(BaseAction):
    """Check resource availability before workflow execution.
    
    Validates system resources such as disk space, memory,
    network connectivity, and service availability.
    """
    action_type = "resource_guard"
    display_name = "资源守卫"
    description = "检查系统资源可用性"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check resource availability.
        
        Args:
            context: Execution context.
            params: Dict with keys: checks (list of {type, threshold}),
                   fail_if_unavailable.
        
        Returns:
            ActionResult with resource check results.
        """
        checks = params.get('checks', [])
        fail_if_unavailable = params.get('fail_if_unavailable', True)
        start_time = time.time()

        results = []
        for check in checks:
            check_type = check.get('type', 'disk_space')
            threshold = check.get('threshold', 0)
            check_result = self._check_resource(check_type, threshold)
            results.append({
                'type': check_type,
                'passed': check_result['passed'],
                'value': check_result['value'],
                'threshold': threshold
            })

        all_passed = all(r['passed'] for r in results)
        return ActionResult(
            success=all_passed if fail_if_unavailable else True,
            message=f"Resource check: {sum(r['passed'] for r in results)}/{len(results)} passed",
            data={
                'passed': all_passed,
                'checks': results
            },
            duration=time.time() - start_time
        )

    def _check_resource(self, check_type: str, threshold: float) -> Dict[str, Any]:
        """Check a single resource."""
        import os
        import shutil

        if check_type == 'disk_space':
            total, used, free = shutil.disk_usage('/')
            free_gb = free / (1024**3)
            return {'passed': free_gb >= threshold, 'value': free_gb, 'unit': 'GB'}
        elif check_type == 'memory':
            import psutil
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024**3)
            return {'passed': available_gb >= threshold, 'value': available_gb, 'unit': 'GB'}
        elif check_type == 'network':
            try:
                import urllib.request
                urllib.request.urlopen('https://www.google.com', timeout=5)
                return {'passed': True, 'value': True}
            except:
                return {'passed': False, 'value': False}
        return {'passed': True, 'value': None}


class CircuitBreakerGuardAction(BaseAction):
    """Circuit breaker pattern for protecting failing workflows.
    
    Monitors action failure rates and opens the circuit when
    failure threshold is exceeded, preventing cascade failures.
    """
    action_type = "circuit_breaker_guard"
    display_name = "断路器守卫"
    description = "断路器模式保护工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check or update circuit breaker state.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, threshold, timeout,
                   window_seconds, operation (check|report|reset).
        
        Returns:
            ActionResult with circuit state.
        """
        action_name = params.get('action', 'default')
        threshold = params.get('threshold', 5)
        timeout_seconds = params.get('timeout', 60)
        window_seconds = params.get('window_seconds', 300)
        operation = params.get('operation', 'check')
        start_time = time.time()

        if not hasattr(context, '_circuit_breaker_states'):
            context._circuit_breaker_states = {}

        state = context._circuit_breaker_states.get(action_name, {
            'failures': [],
            'state': 'closed',
            'opened_at': None
        })

        current_time = time.time()
        if operation == 'check':
            if state['state'] == 'open':
                if state['opened_at'] and (current_time - state['opened_at']) > timeout_seconds:
                    state['state'] = 'half_open'
                    context._circuit_breaker_states[action_name] = state
                    return ActionResult(
                        success=True,
                        message="Circuit moved to half-open (testing)",
                        data={'state': 'half_open', 'action': action_name}
                    )
                return ActionResult(
                    success=False,
                    message="Circuit is open - action blocked",
                    data={'state': 'open', 'action': action_name}
                )
            return ActionResult(
                success=True,
                message="Circuit closed - proceeding",
                data={'state': state['state'], 'action': action_name}
            )

        elif operation == 'report':
            success = params.get('success', True)
            if success:
                state['failures'] = []
                if state['state'] == 'half_open':
                    state['state'] = 'closed'
                    state['opened_at'] = None
            else:
                recent_failures = [f for f in state['failures'] if current_time - f < window_seconds]
                recent_failures.append(current_time)
                state['failures'] = recent_failures
                if len(recent_failures) >= threshold:
                    state['state'] = 'open'
                    state['opened_at'] = current_time
            context._circuit_breaker_states[action_name] = state
            return ActionResult(
                success=True,
                message=f"Failure reported, circuit state: {state['state']}",
                data={'state': state['state'], 'failures': len(state['failures'])}
            )

        elif operation == 'reset':
            state = {'failures': [], 'state': 'closed', 'opened_at': None}
            context._circuit_breaker_states[action_name] = state
            return ActionResult(
                success=True,
                message="Circuit reset to closed",
                data={'state': 'closed', 'action': action_name}
            )

        return ActionResult(
            success=False,
            message=f"Unknown operation: {operation}"
        )


class ConcurrencyGuardAction(BaseAction):
    """Limit concurrent executions of the same workflow.
    
    Uses distributed locking or in-memory counters to prevent
    concurrent runs of the same workflow or action.
    """
    action_type = "concurrency_guard"
    display_name = "并发守卫"
    description = "限制并发执行数量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check or update concurrency limit.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, max_concurrent, operation
                   (acquire|release|check), ttl_seconds.
        
        Returns:
            ActionResult with concurrency state.
        """
        key = params.get('key', 'default')
        max_concurrent = params.get('max_concurrent', 1)
        operation = params.get('operation', 'check')
        ttl_seconds = params.get('ttl_seconds', 300)
        start_time = time.time()

        if not hasattr(context, '_concurrency_locks'):
            context._concurrency_locks = {}

        lock_key = f"concurrency_{key}"
        if lock_key not in context._concurrency_locks:
            context._concurrency_locks[lock_key] = {
                'count': 0,
                'tokens': []
            }

        lock_state = context._concurrency_locks[lock_key]
        current_time = time.time()

        lock_state['tokens'] = [
            t for t in lock_state['tokens']
            if t['expires_at'] > current_time
        ]

        if operation == 'acquire':
            if len(lock_state['tokens']) >= max_concurrent:
                return ActionResult(
                    success=False,
                    message=f"Concurrency limit reached: {max_concurrent}",
                    data={
                        'acquired': False,
                        'current_count': len(lock_state['tokens']),
                        'max_concurrent': max_concurrent
                    }
                )
            token = {
                'id': f"{key}_{current_time}",
                'acquired_at': current_time,
                'expires_at': current_time + ttl_seconds
            }
            lock_state['tokens'].append(token)
            return ActionResult(
                success=True,
                message=f"Concurrency slot acquired (slot {len(lock_state['tokens'])}/{max_concurrent})",
                data={
                    'acquired': True,
                    'token': token['id'],
                    'current_count': len(lock_state['tokens']),
                    'max_concurrent': max_concurrent
                }
            )

        elif operation == 'release':
            token_id = params.get('token_id')
            if token_id:
                lock_state['tokens'] = [
                    t for t in lock_state['tokens']
                    if t['id'] != token_id
                ]
            else:
                if lock_state['tokens']:
                    lock_state['tokens'].pop()
            return ActionResult(
                success=True,
                message="Concurrency slot released",
                data={
                    'current_count': len(lock_state['tokens']),
                    'max_concurrent': max_concurrent
                }
            )

        elif operation == 'check':
            return ActionResult(
                success=True,
                message=f"Current concurrent: {len(lock_state['tokens'])}/{max_concurrent}",
                data={
                    'current_count': len(lock_state['tokens']),
                    'max_concurrent': max_concurrent,
                    'can_execute': len(lock_state['tokens']) < max_concurrent
                }
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class RateLimitGuardAction(BaseAction):
    """Rate limiting guard for workflow actions.
    
    Enforces rate limits per action or globally using
    token bucket or sliding window algorithms.
    """
    action_type = "rate_limit_guard"
    display_name = "限流守卫"
    description = "限流保护工作流执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check or update rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, max_calls, window_seconds,
                   algorithm (token_bucket|sliding_window),
                   operation (check|consume).
        
        Returns:
            ActionResult with rate limit state.
        """
        key = params.get('key', 'default')
        max_calls = params.get('max_calls', 100)
        window_seconds = params.get('window_seconds', 60)
        algorithm = params.get('algorithm', 'token_bucket')
        operation = params.get('operation', 'check')
        start_time = time.time()

        if not hasattr(context, '_rate_limit_state'):
            context._rate_limit_state = {}

        state_key = f"ratelimit_{algorithm}_{key}"
        if state_key not in context._rate_limit_state:
            context._rate_limit_state[state_key] = {
                'tokens': max_calls,
                'last_refill': start_time,
                'calls': []
            }

        state = context._rate_limit_state[state_key]
        current_time = time.time()

        if algorithm == 'token_bucket':
            self._refill_token_bucket(state, max_calls, window_seconds, current_time)
        elif algorithm == 'sliding_window':
            state['calls'] = [
                t for t in state['calls']
                if current_time - t < window_seconds
            ]

        if operation == 'check':
            if algorithm == 'token_bucket':
                allowed = state['tokens'] >= 1
            else:
                allowed = len(state['calls']) < max_calls
            return ActionResult(
                success=allowed,
                message="Rate limit check",
                data={
                    'allowed': allowed,
                    'current_tokens' if algorithm == 'token_bucket' else 'current_calls':
                        state['tokens'] if algorithm == 'token_bucket' else len(state['calls']),
                    'limit': max_calls
                }
            )

        elif operation == 'consume':
            if algorithm == 'token_bucket':
                if state['tokens'] < 1:
                    return ActionResult(
                        success=False,
                        message="Rate limit exceeded",
                        data={'allowed': False, 'tokens': state['tokens'], 'limit': max_calls}
                    )
                state['tokens'] -= 1
            else:
                state['calls'].append(current_time)
            return ActionResult(
                success=True,
                message="Call consumed",
                data={
                    'allowed': True,
                    'current_tokens' if algorithm == 'token_bucket' else 'current_calls':
                        state['tokens'] if algorithm == 'token_bucket' else len(state['calls']),
                    'limit': max_calls
                }
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _refill_token_bucket(
        self,
        state: Dict[str, Any],
        max_calls: int,
        window_seconds: int,
        current_time: float
    ) -> None:
        """Refill token bucket based on elapsed time."""
        elapsed = current_time - state['last_refill']
        refill_rate = max_calls / window_seconds
        tokens_to_add = elapsed * refill_rate
        state['tokens'] = min(max_calls, state['tokens'] + tokens_to_add)
        state['last_refill'] = current_time
