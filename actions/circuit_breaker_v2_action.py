"""Circuit breaker v2 action module for RabAI AutoClick.

Provides enhanced circuit breaker with configurable failure
thresholds, half-open state testing, and event hooks.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitBreakerV2Action(BaseAction):
    """Enhanced circuit breaker with state management.
    
    Implements circuit breaker pattern with closed, open,
    and half-open states with configurable transitions.
    """
    action_type = "circuit_breaker_v2"
    display_name = "断路器V2"
    description = "增强型断路器模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Operate circuit breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (call|get_state|reset),
                   name, failure_threshold, timeout_seconds,
                   half_open_max_calls.
        
        Returns:
            ActionResult with circuit state.
        """
        operation = params.get('operation', 'call')
        name = params.get('name', 'default')
        failure_threshold = params.get('failure_threshold', 5)
        timeout_seconds = params.get('timeout_seconds', 60)
        half_open_max_calls = params.get('half_open_max_calls', 3)
        start_time = time.time()

        if not hasattr(context, '_circuit_breakers'):
            context._circuit_breakers = {}
            context._circuit_breaker_locks = {}

        if name not in context._circuit_breakers:
            context._circuit_breakers[name] = {
                'state': 'closed',
                'failure_count': 0,
                'success_count': 0,
                'opened_at': None,
                'last_failure': None,
                'half_open_calls': 0,
                'total_calls': 0,
                'total_failures': 0,
            }
            context._circuit_breaker_locks[name] = threading.Lock()

        lock = context._circuit_breaker_locks[name]
        cb = context._circuit_breakers[name]
        current_time = time.time()

        with lock:
            if cb['state'] == 'open':
                if cb['opened_at'] and (current_time - cb['opened_at']) >= timeout_seconds:
                    cb['state'] = 'half_open'
                    cb['half_open_calls'] = 0
                    return ActionResult(
                        success=True,
                        message="Circuit moved to half-open",
                        data={'state': 'half_open', 'name': name}
                    )
                else:
                    remaining = timeout_seconds - (current_time - cb['opened_at']) if cb['opened_at'] else timeout_seconds
                    return ActionResult(
                        success=False,
                        message="Circuit is open",
                        data={
                            'state': 'open',
                            'name': name,
                            'remaining_seconds': round(remaining, 2),
                            'failure_count': cb['failure_count']
                        }
                    )

            elif cb['state'] == 'half_open':
                if cb['half_open_calls'] >= half_open_max_calls:
                    return ActionResult(
                        success=False,
                        message="Half-open calls exhausted",
                        data={
                            'state': 'half_open',
                            'name': name,
                            'half_open_calls': cb['half_open_calls']
                        }
                    )
                cb['half_open_calls'] += 1
                cb['total_calls'] += 1

        if operation == 'get_state':
            return ActionResult(
                success=True,
                message=f"Circuit '{name}': {cb['state']}",
                data={
                    'state': cb['state'],
                    'name': name,
                    'failure_count': cb['failure_count'],
                    'total_calls': cb['total_calls'],
                    'total_failures': cb['total_failures']
                }
            )

        elif operation == 'report_success':
            with lock:
                if cb['state'] == 'half_open':
                    cb['success_count'] += 1
                    if cb['success_count'] >= half_open_max_calls:
                        cb['state'] = 'closed'
                        cb['failure_count'] = 0
                        cb['success_count'] = 0
                        cb['opened_at'] = None
                return ActionResult(
                    success=True,
                    message="Success reported to circuit breaker",
                    data={'state': cb['state']}
                )

        elif operation == 'report_failure':
            with lock:
                cb['failure_count'] += 1
                cb['total_failures'] += 1
                cb['last_failure'] = current_time

                if cb['state'] == 'half_open':
                    cb['state'] = 'open'
                    cb['opened_at'] = current_time
                    cb['success_count'] = 0
                elif cb['failure_count'] >= failure_threshold:
                    cb['state'] = 'open'
                    cb['opened_at'] = current_time

                return ActionResult(
                    success=True,
                    message=f"Failure reported, circuit: {cb['state']}",
                    data={
                        'state': cb['state'],
                        'failure_count': cb['failure_count']
                    }
                )

        elif operation == 'reset':
            with lock:
                cb['state'] = 'closed'
                cb['failure_count'] = 0
                cb['success_count'] = 0
                cb['opened_at'] = None
                cb['half_open_calls'] = 0
            return ActionResult(
                success=True,
                message=f"Circuit '{name}' reset to closed",
                data={'state': 'closed'}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class BulkheadAction(BaseAction):
    """Bulkhead isolation pattern for resource partitioning.
    
    Isolates different types of operations into separate
    resource pools to prevent cascade failures.
    """
    action_type = "bulkhead"
    display_name = "隔板隔离"
    description = "隔板隔离模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage bulkhead isolation.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (acquire|release|status),
                   bulkhead_name, max_concurrent, timeout.
        
        Returns:
            ActionResult with bulkhead status.
        """
        bulkhead_name = params.get('bulkhead_name', 'default')
        max_concurrent = params.get('max_concurrent', 10)
        operation = params.get('operation', 'status')
        timeout = params.get('timeout', 30)
        start_time = time.time()

        if not hasattr(context, '_bulkheads'):
            context._bulkheads = {}
            context._bulkhead_locks = {}

        if bulkhead_name not in context._bulkheads:
            context._bulkheads[bulkhead_name] = {
                'current': 0,
                'max': max_concurrent,
                'total_acquired': 0,
                'total_rejected': 0,
                'queue': []
            }
            context._bulkhead_locks[bulkhead_name] = threading.Lock()

        lock = context._bulkhead_locks[bulkhead_name]
        bh = context._bulkheads[bulkhead_name]

        with lock:
            if operation == 'acquire':
                if bh['current'] < bh['max']:
                    bh['current'] += 1
                    bh['total_acquired'] += 1
                    return ActionResult(
                        success=True,
                        message=f"Bulkhead acquired: {bh['current']}/{bh['max']}",
                        data={
                            'acquired': True,
                            'current': bh['current'],
                            'max': bh['max']
                        }
                    )
                else:
                    bh['total_rejected'] += 1
                    return ActionResult(
                        success=False,
                        message="Bulkhead capacity exceeded",
                        data={
                            'acquired': False,
                            'current': bh['current'],
                            'max': bh['max'],
                            'rejected': True
                        }
                    )

            elif operation == 'release':
                bh['current'] = max(0, bh['current'] - 1)
                return ActionResult(
                    success=True,
                    message=f"Bulkhead released: {bh['current']}/{bh['max']}",
                    data={'current': bh['current'], 'max': bh['max']}
                )

            elif operation == 'status':
                return ActionResult(
                    success=True,
                    message=f"Bulkhead '{bulkhead_name}': {bh['current']}/{bh['max']}",
                    data={
                        'current': bh['current'],
                        'max': bh['max'],
                        'available': bh['max'] - bh['current'],
                        'total_acquired': bh['total_acquired'],
                        'total_rejected': bh['total_rejected']
                    }
                )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")
