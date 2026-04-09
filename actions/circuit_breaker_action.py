"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern:
- CircuitBreaker: Circuit breaker state machine
- CircuitBreakerRegistry: Manage multiple breakers
- CircuitBreakerMetrics: Collect circuit breaker metrics
- StateTransitions: Track state transitions
"""

from __future__ import annotations

import time
import sys
import os
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern implementation."""
    action_type = "circuit_breaker"
    display_name = "断路器"
    description = "实现断路器模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")
            breaker_name = params.get("breaker_name", "default")
            config = params.get("config", {})

            config_obj = CircuitBreakerConfig(
                failure_threshold=config.get("failure_threshold", 5),
                success_threshold=config.get("success_threshold", 2),
                timeout_seconds=config.get("timeout_seconds", 60.0),
                half_open_max_calls=config.get("half_open_max_calls", 3),
            )

            state_file = os.path.join("/tmp/circuit_breakers", f"{breaker_name}.json")
            os.makedirs(os.path.dirname(state_file), exist_ok=True)

            state = {"state": CircuitState.CLOSED.value, "failure_count": 0, "success_count": 0, "last_failure_time": None, "opened_at": None}
            if os.path.exists(state_file):
                import json as json_module
                with open(state_file) as f:
                    state = json_module.load(f)

            if operation == "call":
                current_state = CircuitState(state["state"])

                if current_state == CircuitState.OPEN:
                    if state["opened_at"]:
                        elapsed = time.time() - state["opened_at"]
                        if elapsed >= config_obj.timeout_seconds:
                            state["state"] = CircuitState.HALF_OPEN.value
                            state["success_count"] = 0
                            current_state = CircuitState.HALF_OPEN
                        else:
                            remaining = config_obj.timeout_seconds - elapsed
                            return ActionResult(success=False, message=f"Circuit OPEN, retry in {remaining:.1f}s", data={"state": "open", "retry_after": remaining})

                call_result = params.get("call_result", None)
                call_success = params.get("call_success", call_result is not None)

                if call_success:
                    state["failure_count"] = 0
                    if current_state == CircuitState.HALF_OPEN:
                        state["success_count"] += 1
                        if state["success_count"] >= config_obj.success_threshold:
                            state["state"] = CircuitState.CLOSED.value
                            state["success_count"] = 0
                    return ActionResult(success=True, message="Call succeeded", data={"state": state["state"], "call_success": True})
                else:
                    state["failure_count"] += 1
                    if state["failure_count"] >= config_obj.failure_threshold:
                        state["state"] = CircuitState.OPEN.value
                        state["opened_at"] = time.time()
                    return ActionResult(success=False, message=f"Call failed (failures: {state['failure_count']})", data={"state": state["state"], "failure_count": state["failure_count"]})

            elif operation == "get_state":
                return ActionResult(success=True, message=f"Circuit: {state['state']}", data=state)

            elif operation == "reset":
                state = {"state": CircuitState.CLOSED.value, "failure_count": 0, "success_count": 0, "last_failure_time": None, "opened_at": None}
                import json as json_module
                with open(state_file, "w") as f:
                    json_module.dump(state, f)
                return ActionResult(success=True, message=f"Breaker reset: {breaker_name}")

            import json as json_module
            with open(state_file, "w") as f:
                json_module.dump(state, f)

            return ActionResult(success=True, message=f"Circuit: {state['state']}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
