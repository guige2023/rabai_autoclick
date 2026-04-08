"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern for fault tolerance with
closed, open, and half-open states.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing, reject all
    HALF_OPEN = "half_open" # Testing recovery


class CircuitBreakerAction(BaseAction):
    """Circuit breaker for fault-tolerant action execution.
    
    Monitors failures and opens circuit when threshold is reached,
    providing fail-fast behavior and recovery testing.
    """
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式：故障检测和快速失败"

    _circuits: Dict[str, 'CircuitData'] = {}
    _locks: Dict[str, threading.Lock] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute action through circuit breaker or manage circuit.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (call/protect/reset/get_status/open)
                - circuit_name: str
                - action_name: str (for call/protect)
                - action_params: dict
                - failure_threshold: int, failures before opening
                - success_threshold: int, successes to close from half-open
                - timeout: float, seconds before half-open (open state)
                - half_open_max_calls: int, max calls in half-open
                - save_to_var: str
        
        Returns:
            ActionResult with execution result.
        """
        operation = params.get('operation', 'call')
        circuit_name = params.get('circuit_name', 'default')

        if operation in ('call', 'protect'):
            return self._call_through_circuit(context, params)
        elif operation == 'reset':
            return self._reset_circuit(circuit_name, params)
        elif operation == 'get_status':
            return self._get_status(circuit_name, params)
        elif operation == 'open':
            return self._force_open(circuit_name, params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _call_through_circuit(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute action through circuit breaker."""
        circuit_name = params.get('circuit_name', 'default')
        action_name = params.get('action_name', '')
        action_params = params.get('action_params', {})
        failure_threshold = params.get('failure_threshold', 5)
        success_threshold = params.get('success_threshold', 3)
        timeout = params.get('timeout', 60.0)
        half_open_max_calls = params.get('half_open_max_calls', 1)
        save_to_var = params.get('save_to_var', None)

        circuit = self._get_or_create_circuit(
            circuit_name, failure_threshold, success_threshold, timeout, half_open_max_calls
        )

        # Check circuit state
        state = circuit.state
        start_time = time.time()

        if state == CircuitState.OPEN:
            elapsed = time.time() - circuit.last_failure_time
            if elapsed >= circuit.timeout:
                self._transition_to_half_open(circuit)
            else:
                return ActionResult(
                    success=False,
                    message=f"Circuit '{circuit_name}' is OPEN (retry in {circuit.timeout - elapsed:.1f}s)",
                    data={'circuit': circuit_name, 'state': 'open'},
                    duration=time.time() - start_time
                )

        if state == CircuitState.HALF_OPEN:
            with circuit.lock:
                if circuit.half_open_calls >= circuit.half_open_max_calls:
                    return ActionResult(
                        success=False,
                        message=f"Circuit '{circuit_name}' is HALF-OPEN (max calls reached)",
                        data={'circuit': circuit_name, 'state': 'half_open'},
                        duration=time.time() - start_time
                    )
                circuit.half_open_calls += 1

        # Execute the action
        try:
            action = self._find_action(action_name)
            if action is None:
                return ActionResult(success=False, message=f"Action not found: {action_name}")

            result = action.execute(context, action_params)
            duration = time.time() - start_time

            if result.success:
                self._on_success(circuit)
                if save_to_var and hasattr(context, 'vars'):
                    context.vars[save_to_var] = result.data
                return ActionResult(
                    success=True,
                    message=f"Circuit '{circuit_name}': {result.message}",
                    data=result.data,
                    duration=duration
                )
            else:
                self._on_failure(circuit)
                return ActionResult(
                    success=False,
                    message=f"Circuit '{circuit_name}': action failed - {result.message}",
                    data=result.data,
                    duration=duration
                )

        except Exception as e:
            self._on_failure(circuit)
            return ActionResult(
                success=False,
                message=f"Circuit '{circuit_name}' exception: {e}",
                duration=time.time() - start_time
            )

    def _get_or_create_circuit(
        self, name: str, failure_threshold: int, success_threshold: int,
        timeout: float, half_open_max_calls: int
    ) -> 'CircuitData':
        """Get or create circuit data."""
        if name not in self._circuits:
            with threading.Lock():
                if name not in self._circuits:
                    self._circuits[name] = CircuitData(
                        name=name,
                        failure_threshold=failure_threshold,
                        success_threshold=success_threshold,
                        timeout=timeout,
                        half_open_max_calls=half_open_max_calls
                    )
                    self._locks[name] = threading.Lock()
        return self._circuits[name]

    def _on_success(self, circuit: 'CircuitData') -> None:
        """Handle successful call."""
        with circuit.lock:
            if circuit.state == CircuitState.HALF_OPEN:
                circuit.success_count += 1
                if circuit.success_count >= circuit.success_threshold:
                    self._transition_to_closed(circuit)
            else:
                circuit.failure_count = 0

    def _on_failure(self, circuit: 'CircuitData') -> None:
        """Handle failed call."""
        with circuit.lock:
            circuit.failure_count += 1
            circuit.last_failure_time = time.time()

            if circuit.state == CircuitState.HALF_OPEN:
                self._transition_to_open(circuit)
            elif circuit.failure_count >= circuit.failure_threshold:
                self._transition_to_open(circuit)

    def _transition_to_open(self, circuit: 'CircuitData') -> None:
        """Transition circuit to OPEN state."""
        circuit.state = CircuitState.OPEN
        circuit.success_count = 0
        circuit.last_failure_time = time.time()

    def _transition_to_half_open(self, circuit: 'CircuitData') -> None:
        """Transition circuit to HALF-OPEN state."""
        circuit.state = CircuitState.HALF_OPEN
        circuit.success_count = 0
        circuit.half_open_calls = 0

    def _transition_to_closed(self, circuit: 'CircuitData') -> None:
        """Transition circuit to CLOSED state."""
        circuit.state = CircuitState.CLOSED
        circuit.failure_count = 0
        circuit.success_count = 0

    def _reset_circuit(self, circuit_name: str, params: Dict[str, Any]) -> ActionResult:
        """Reset circuit to closed state."""
        save_to_var = params.get('save_to_var', None)
        if circuit_name in self._circuits:
            with self._circuits[circuit_name].lock:
                circuit = self._circuits[circuit_name]
                circuit.state = CircuitState.CLOSED
                circuit.failure_count = 0
                circuit.success_count = 0
                circuit.half_open_calls = 0

        return ActionResult(success=True, message=f"Circuit '{circuit_name}' reset")

    def _get_status(self, circuit_name: str, params: Dict[str, Any]) -> ActionResult:
        """Get circuit status."""
        save_to_var = params.get('save_to_var', None)
        if circuit_name not in self._circuits:
            return ActionResult(success=False, message=f"Circuit '{circuit_name}' not found")

        circuit = self._circuits[circuit_name]
        data = {
            'name': circuit.name,
            'state': circuit.state.value,
            'failure_count': circuit.failure_count,
            'success_count': circuit.success_count,
            'failure_threshold': circuit.failure_threshold,
            'success_threshold': circuit.success_threshold,
            'last_failure_time': circuit.last_failure_time,
            'half_open_calls': circuit.half_open_calls,
        }

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = data

        return ActionResult(
            success=True,
            message=f"Circuit '{circuit_name}' state: {circuit.state.value}",
            data=data
        )

    def _force_open(self, circuit_name: str, params: Dict[str, Any]) -> ActionResult:
        """Force circuit to open state."""
        if circuit_name in self._circuits:
            self._transition_to_open(self._circuits[circuit_name])
        return ActionResult(success=True, message=f"Circuit '{circuit_name}' forced OPEN")

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction, 'type': TypeAction,
                'key_press': KeyPressAction, 'image_match': ImageMatchAction,
                'find_image': FindImageAction, 'ocr': OCRAction,
                'scroll': ScrollAction, 'mouse_move': MouseMoveAction,
                'drag': DragAction, 'script': ScriptAction,
                'delay': DelayAction, 'condition': ConditionAction,
                'loop': LoopAction, 'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction, 'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            return action_cls() if action_cls else None
        except Exception:
            return None

    def get_required_params(self) -> List[str]:
        return ['operation', 'circuit_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'action_name': '',
            'action_params': {},
            'failure_threshold': 5,
            'success_threshold': 3,
            'timeout': 60.0,
            'half_open_max_calls': 1,
            'save_to_var': None,
        }


@dataclass
class CircuitData:
    """Circuit breaker state data."""
    name: str
    failure_threshold: int
    success_threshold: int
    timeout: float
    half_open_max_calls: int
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
