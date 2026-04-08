"""API Circuit Breaker action module for RabAI AutoClick.

Provides circuit breaker pattern for API resilience:
- CircuitOpenAction: Open circuit
- CircuitCloseAction: Close circuit
- CircuitHalfOpenAction: Half-open circuit
- CircuitStateAction: Get circuit state
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional
from collections import defaultdict

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitOpenAction(BaseAction):
    """Open circuit breaker."""
    action_type = "circuit_open"
    display_name = "熔断开启"
    description = "开启熔断器"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._circuits = defaultdict(lambda: {'state': 'closed', 'failures': 0, 'last_failure': 0})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute circuit open."""
        key = params.get('key', 'default')
        reason = params.get('reason', 'manual')
        output_var = params.get('output_var', 'circuit_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            circuit = self._circuits[resolved_key]
            circuit['state'] = 'open'
            circuit['opened_at'] = time.time()
            circuit['open_reason'] = reason

            result = {
                'key': resolved_key,
                'state': 'open',
                'reason': reason,
                'opened_at': circuit['opened_at'],
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Circuit '{resolved_key}' opened: {reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit open error: {e}")


class CircuitCloseAction(BaseAction):
    """Close circuit breaker."""
    action_type = "circuit_close"
    display_name = "熔断关闭"
    description = "关闭熔断器"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute circuit close."""
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'circuit_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            circuit = self._circuits[resolved_key]
            circuit['state'] = 'closed'
            circuit['failures'] = 0
            circuit['closed_at'] = time.time()

            result = {
                'key': resolved_key,
                'state': 'closed',
                'closed_at': circuit['closed_at'],
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Circuit '{resolved_key}' closed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit close error: {e}")


class CircuitHalfOpenAction(BaseAction):
    """Half-open circuit breaker."""
    action_type = "circuit_half_open"
    display_name = "熔断半开"
    description = "半开熔断器"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute circuit half-open."""
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'circuit_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            circuit = self._circuits[resolved_key]
            circuit['state'] = 'half-open'
            circuit['half_open_at'] = time.time()

            result = {
                'key': resolved_key,
                'state': 'half-open',
                'half_open_at': circuit['half_open_at'],
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Circuit '{resolved_key}' half-open"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit half-open error: {e}")


class CircuitStateAction(BaseAction):
    """Get circuit breaker state."""
    action_type = "circuit_state"
    display_name = "熔断状态"
    description = "获取熔断状态"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute circuit state check."""
        key = params.get('key', 'default')
        failure_threshold = params.get('failure_threshold', 5)
        timeout = params.get('timeout', 60)
        output_var = params.get('output_var', 'circuit_state')

        try:
            resolved_key = context.resolve_value(key) if context else key

            circuit = self._circuits[resolved_key]
            state = circuit['state']

            if state == 'open' and circuit.get('opened_at', 0) + timeout < time.time():
                state = 'half-open'
                circuit['state'] = 'half-open'

            result = {
                'key': resolved_key,
                'state': state,
                'failures': circuit['failures'],
                'last_failure': circuit.get('last_failure', 0),
                'failure_threshold': failure_threshold,
                'timeout': timeout,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Circuit '{resolved_key}': {state} (failures: {circuit['failures']})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit state error: {e}")
