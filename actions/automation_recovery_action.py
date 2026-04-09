"""Automation recovery action module for RabAI AutoClick.

Provides recovery mechanisms for automation:
- AutomationRecoveryAction: Recover from automation failures
- AutomationRollbackAction: Rollback automation state
- AutomationHealthCheckAction: Health check automation
- AutomationCircuitBreakerAction: Circuit breaker for automation
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class AutomationRecoveryAction(BaseAction):
    """Recover from automation failures."""
    action_type = "automation_recovery"
    display_name = "自动化恢复"
    description = "从自动化失败中恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            failure_context = params.get("failure_context", {})
            recovery_strategy = params.get("recovery_strategy", "retry")
            max_recovery_attempts = params.get("max_recovery_attempts", 3)
            fallback_action = params.get("fallback_action")
            checkpoint_data = params.get("checkpoint_data")
            callback = params.get("callback")

            recovery_attempt = 0
            last_error = None

            while recovery_attempt <= max_recovery_attempts:
                try:
                    if recovery_strategy == "retry":
                        time.sleep(2.0 ** recovery_attempt)
                    elif recovery_strategy == "rollback" and checkpoint_data:
                        return ActionResult(success=True, message=f"Rolled back to checkpoint", data={"checkpoint_data": checkpoint_data})
                    elif recovery_strategy == "fallback" and callable(fallback_action):
                        result = fallback_action()
                        return ActionResult(success=True, message="Fallback action executed", data={"result": result})

                    raise Exception(f"Recovery attempt {recovery_attempt} simulated failure")

                except Exception as e:
                    last_error = e
                    recovery_attempt += 1

            if callable(fallback_action):
                try:
                    result = fallback_action()
                    return ActionResult(success=True, message="Fallback after recovery failure", data={"result": result})
                except Exception:
                    pass

            return ActionResult(
                success=False,
                message=f"Recovery failed after {recovery_attempt} attempts: {last_error}",
                data={"attempts": recovery_attempt, "last_error": str(last_error)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Recovery error: {e}")


class AutomationRollbackAction(BaseAction):
    """Rollback automation state."""
    action_type = "automation_rollback"
    display_name = "自动化回滚"
    description = "回滚自动化状态"

    def __init__(self):
        super().__init__()
        self._history: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "rollback")
            steps = params.get("steps", 1)
            state_data = params.get("state_data", {})
            save_state = params.get("save_state", True)

            if operation == "save":
                state_id = f"state_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
                self._history.append({
                    "id": state_id,
                    "data": state_data,
                    "timestamp": datetime.now().isoformat(),
                })
                return ActionResult(success=True, message=f"State saved as {state_id}", data={"state_id": state_id, "history_size": len(self._history)})

            elif operation == "rollback":
                if not self._history:
                    return ActionResult(success=False, message="No saved states to rollback to")

                target_steps = min(steps, len(self._history))
                restored_state = self._history[-target_steps]["data"]
                self._history = self._history[:-target_steps]

                return ActionResult(
                    success=True,
                    message=f"Rolled back {target_steps} state(s)",
                    data={"restored_state": restored_state, "states_remaining": len(self._history)}
                )

            elif operation == "history":
                return ActionResult(success=True, message=f"{len(self._history)} saved states", data={"history": self._history})

            elif operation == "clear":
                count = len(self._history)
                self._history.clear()
                return ActionResult(success=True, message=f"Cleared {count} history entries")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Rollback error: {e}")


class AutomationHealthCheckAction(BaseAction):
    """Health check for automation."""
    action_type = "automation_health_check"
    display_name = "自动化健康检查"
    description = "检查自动化组件健康状态"

    def __init__(self):
        super().__init__()
        self._component_status: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            components = params.get("components", [])
            threshold = params.get("failure_threshold", 3)

            if operation == "check":
                if not components:
                    return ActionResult(success=True, message="No components specified", data={"components": self._component_status})

                results = {}
                overall_healthy = True

                for component in components:
                    status = self._component_status.get(component, {"failures": 0, "state": "unknown"})
                    healthy = status.get("failures", 0) < threshold and status.get("state") != "unhealthy"
                    results[component] = {
                        "healthy": healthy,
                        "failures": status.get("failures", 0),
                        "last_check": status.get("last_check"),
                    }
                    if not healthy:
                        overall_healthy = False

                return ActionResult(
                    success=overall_healthy,
                    message=f"Health check: {'ALL HEALTHY' if overall_healthy else 'DEGRADED'}",
                    data={"results": results, "overall_healthy": overall_healthy}
                )

            elif operation == "report":
                component = params.get("component")
                state = params.get("state", "healthy")
                if component:
                    self._component_status[component] = {
                        "state": state,
                        "failures": self._component_status.get(component, {}).get("failures", 0),
                        "last_check": datetime.now().isoformat(),
                    }
                return ActionResult(success=True, message=f"Reported state for {component}")

            elif operation == "reset":
                component = params.get("component")
                if component and component in self._component_status:
                    self._component_status[component]["failures"] = 0
                    self._component_status[component]["state"] = "healthy"
                return ActionResult(success=True, message=f"Reset health for {component}")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Health check error: {e}")


class AutomationCircuitBreakerAction(BaseAction):
    """Circuit breaker for automation."""
    action_type = "automation_circuit_breaker"
    display_name = "自动化断路器"
    description = "自动化断路器保护"

    def __init__(self):
        super().__init__()
        self._circuit_state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_attempts = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")
            action = params.get("action")
            failure_threshold = params.get("failure_threshold", 5)
            success_threshold = params.get("success_threshold", 3)
            timeout = params.get("timeout", 60)
            reset_timeout = params.get("reset_timeout", 30)

            if operation == "state":
                return ActionResult(success=True, message=f"Circuit is {self._circuit_state.value}", data={"state": self._circuit_state.value, "failures": self._failure_count})

            if self._circuit_state == CircuitState.OPEN:
                if self._last_failure_time and time.time() - self._last_failure_time >= reset_timeout:
                    self._circuit_state = CircuitState.HALF_OPEN
                    self._half_open_attempts = 0
                    return ActionResult(success=True, message="Circuit transitioned to HALF_OPEN", data={"state": self._circuit_state.value})
                return ActionResult(success=False, message="Circuit is OPEN, request rejected", data={"state": self._circuit_state.value, "retry_after": reset_timeout})

            if operation == "call" and callable(action):
                try:
                    result = action()
                    self._on_success(success_threshold)
                    return ActionResult(success=True, message="Action succeeded", data={"result": result})
                except Exception as e:
                    self._on_failure(failure_threshold)
                    return ActionResult(success=False, message=f"Action failed: {e}", data={"state": self._circuit_state.value, "failures": self._failure_count})

            return ActionResult(success=False, message=f"Unknown operation or non-callable action: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker error: {e}")

    def _on_success(self, success_threshold: int) -> None:
        """Handle successful call."""
        self._success_count += 1
        self._failure_count = 0

        if self._circuit_state == CircuitState.HALF_OPEN:
            self._half_open_attempts += 1
            if self._half_open_attempts >= success_threshold:
                self._circuit_state = CircuitState.CLOSED
                self._half_open_attempts = 0
                self._success_count = 0

    def _on_failure(self, failure_threshold: int) -> None:
        """Handle failed call."""
        self._failure_count += 1
        self._success_count = 0
        self._last_failure_time = time.time()

        if self._failure_count >= failure_threshold:
            self._circuit_state = CircuitState.OPEN
