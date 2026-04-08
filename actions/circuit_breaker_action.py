"""Circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern operations:
- CircuitBreakerAction: Circuit breaker for fault tolerance
- CircuitBreakerOpenAction: Force open circuit breaker
- CircuitBreakerCloseAction: Force close circuit breaker
- CircuitBreakerStatusAction: Get circuit breaker status
"""

import time
import threading
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


from enum import Enum


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation."""
    name: str
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    half_open_calls: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._to_half_open()
                else:
                    raise Exception(f"Circuit breaker '{self.name}' is OPEN")
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise e

    def _should_attempt_reset(self) -> bool:
        if self.last_failure_time:
            elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
            return elapsed >= self.timeout_seconds
        return True

    def _to_half_open(self):
        self.state = CircuitBreakerState.HALF_OPEN
        self.half_open_calls = 0
        self.success_count = 0

    def _on_success(self):
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            self.half_open_calls += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "failure_threshold": self.failure_threshold,
                "success_threshold": self.success_threshold,
                "timeout_seconds": self.timeout_seconds,
                "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
            }


_breakers: Dict[str, CircuitBreaker] = {}
_breakers_lock = threading.Lock()


class CircuitBreakerAction(BaseAction):
    """Execute with circuit breaker protection."""
    action_type = "circuit_breaker"
    display_name = "断路器"
    description = "使用断路器模式保护调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")
            func_ref = params.get("func_ref", None)
            args = params.get("args", [])
            kwargs = params.get("kwargs", {})
            failure_threshold = params.get("failure_threshold", 5)
            success_threshold = params.get("success_threshold", 2)
            timeout_seconds = params.get("timeout_seconds", 60.0)

            with _breakers_lock:
                if name not in _breakers:
                    _breakers[name] = CircuitBreaker(
                        name=name,
                        failure_threshold=failure_threshold,
                        success_threshold=success_threshold,
                        timeout_seconds=timeout_seconds
                    )
                breaker = _breakers[name]

            if not func_ref:
                return ActionResult(
                    success=True,
                    message=f"Circuit breaker '{name}' ready",
                    data=breaker.get_status()
                )

            try:
                result = breaker.call(func_ref, *args, **kwargs)
                return ActionResult(
                    success=True,
                    message=f"Circuit breaker '{name}' call succeeded",
                    data={"result": result, "status": breaker.get_status()}
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker '{name}' call failed: {str(e)}",
                    data={"error": str(e), "status": breaker.get_status()}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker failed: {str(e)}")


class CircuitBreakerOpenAction(BaseAction):
    """Force open a circuit breaker."""
    action_type = "circuit_breaker_open"
    display_name = "打开断路器"
    description = "强制打开断路器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")

            with _breakers_lock:
                if name not in _breakers:
                    _breakers[name] = CircuitBreaker(name=name)
                _breakers[name].state = CircuitBreakerState.OPEN

            return ActionResult(
                success=True,
                message=f"Circuit breaker '{name}' opened",
                data=_breakers[name].get_status()
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker open failed: {str(e)}")


class CircuitBreakerCloseAction(BaseAction):
    """Force close a circuit breaker."""
    action_type = "circuit_breaker_close"
    display_name = "关闭断路器"
    description = "强制关闭断路器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")

            with _breakers_lock:
                if name not in _breakers:
                    _breakers[name] = CircuitBreaker(name=name)
                _breakers[name].state = CircuitBreakerState.CLOSED
                _breakers[name].failure_count = 0
                _breakers[name].success_count = 0

            return ActionResult(
                success=True,
                message=f"Circuit breaker '{name}' closed",
                data=_breakers[name].get_status()
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker close failed: {str(e)}")


class CircuitBreakerStatusAction(BaseAction):
    """Get circuit breaker status."""
    action_type = "circuit_breaker_status"
    display_name = "断路器状态"
    description = "获取断路器状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", None)

            with _breakers_lock:
                if name:
                    if name not in _breakers:
                        return ActionResult(success=False, message=f"Circuit breaker '{name}' not found")
                    statuses = [breakers[name].get_status()]
                else:
                    statuses = [b.get_status() for b in _breakers.values()]

            return ActionResult(
                success=True,
                message=f"{len(statuses)} circuit breaker(s)",
                data={"breakers": statuses, "count": len(statuses)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker status failed: {str(e)}")
