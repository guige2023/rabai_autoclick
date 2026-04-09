"""API resilience action module for RabAI AutoClick.

Provides resilience pattern operations:
- CircuitBreakerAction: Circuit breaker pattern
- BulkheadAction: Bulkhead isolation pattern
- TimeoutAction: Timeout handling
- FallbackAction: Fallback strategies
"""

import sys
import os
import time
import logging
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance."""
    name: str
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    opened_at: Optional[datetime] = None

    def call(self, fn: Callable[[], Any], fallback: Optional[Callable[[], Any]] = None) -> Any:
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
            else:
                if fallback:
                    return fallback()
                raise Exception(f"Circuit {self.name} is OPEN")

        try:
            result = fn()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            if fallback:
                return fallback()
            raise e

    def _should_attempt_reset(self) -> bool:
        if self.last_failure_time:
            elapsed = (datetime.now() - self.last_failure_time).total_seconds()
            return elapsed >= self.timeout
        return True

    def _on_success(self) -> None:
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.success_count = 0

    def _on_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.opened_at = datetime.now()


class Bulkhead:
    """Bulkhead isolation for resource protection."""
    
    def __init__(self, name: str, max_concurrent: int = 10) -> None:
        self.name = name
        self.max_concurrent = max_concurrent
        self._semaphore = threading.Semaphore(max_concurrent)
        self._active_count = 0
        self._lock = threading.Lock()

    def execute(self, fn: Callable[[], Any]) -> Any:
        acquired = self._semaphore.acquire(timeout=5.0)
        if not acquired:
            raise Exception(f"Bulkhead {self.name}: max concurrent limit reached")
        try:
            with self._lock:
                self._active_count += 1
            return fn()
        finally:
            with self._lock:
                self._active_count -= 1
            self._semaphore.release()

    @property
    def active_count(self) -> int:
        return self._active_count


class TimeoutHandler:
    """Handles timeout for operations."""

    @staticmethod
    def with_timeout(fn: Callable[[], Any], timeout_seconds: float, default: Any = None) -> Any:
        result = [Exception("Timeout")]
        semaphore = threading.Semaphore(0)

        def worker():
            try:
                result[0] = fn()
            except Exception as e:
                result[0] = e
            finally:
                semaphore.release()

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

        acquired = semaphore.acquire(timeout=timeout_seconds)
        if not acquired:
            return default if default is not None else TimeoutError(f"Operation timed out after {timeout_seconds}s")

        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]


@dataclass
class FallbackStrategy:
    """Fallback execution strategy."""
    name: str
    fallback_fn: Callable[[], Any]
    trigger_on: str = "exception"
    max_attempts: int = 1


_breakers: Dict[str, CircuitBreaker] = {}
_bulkheads: Dict[str, Bulkhead] = {}


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern implementation."""
    action_type = "api_circuit_breaker"
    display_name = "断路器模式"
    description = "实现断路器容错模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "call")
        breaker_name = params.get("name", "default")
        failure_threshold = params.get("failure_threshold", 5)
        success_threshold = params.get("success_threshold", 2)
        timeout = params.get("timeout", 60.0)

        if operation == "create":
            if breaker_name in _breakers:
                return ActionResult(success=False, message=f"断路器 {breaker_name} 已存在")

            breaker = CircuitBreaker(
                name=breaker_name,
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                timeout=timeout
            )
            _breakers[breaker_name] = breaker
            return ActionResult(
                success=True,
                message=f"断路器 {breaker_name} 已创建",
                data={"name": breaker_name, "state": breaker.state.value}
            )

        if operation == "status":
            breaker = _breakers.get(breaker_name)
            if not breaker:
                return ActionResult(success=False, message=f"断路器 {breaker_name} 不存在")

            return ActionResult(
                success=True,
                message=f"断路器 {breaker_name}: {breaker.state.value}",
                data={
                    "name": breaker.name,
                    "state": breaker.state.value,
                    "failure_count": breaker.failure_count,
                    "success_count": breaker.success_count,
                    "opened_at": breaker.opened_at.isoformat() if breaker.opened_at else None
                }
            )

        if operation == "reset":
            breaker = _breakers.get(breaker_name)
            if not breaker:
                return ActionResult(success=False, message=f"断路器 {breaker_name} 不存在")
            breaker.state = CircuitState.CLOSED
            breaker.failure_count = 0
            breaker.success_count = 0
            return ActionResult(success=True, message=f"断路器 {breaker_name} 已重置")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class BulkheadAction(BaseAction):
    """Bulkhead isolation pattern implementation."""
    action_type = "api_bulkhead"
    display_name = "隔板模式"
    description = "实现资源隔离的隔板模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "create")
        name = params.get("name", "default")
        max_concurrent = params.get("max_concurrent", 10)

        if operation == "create":
            if name in _bulkheads:
                return ActionResult(success=False, message=f"隔板 {name} 已存在")

            bulkhead = Bulkhead(name=name, max_concurrent=max_concurrent)
            _bulkheads[name] = bulkhead
            return ActionResult(
                success=True,
                message=f"隔板 {name} 已创建，最大并发: {max_concurrent}",
                data={"name": name, "max_concurrent": max_concurrent}
            )

        if operation == "status":
            bulkhead = _bulkheads.get(name)
            if not bulkhead:
                return ActionResult(success=False, message=f"隔板 {name} 不存在")

            return ActionResult(
                success=True,
                message=f"隔板 {name}: {bulkhead.active_count}/{bulkhead.max_concurrent}",
                data={
                    "name": name,
                    "active": bulkhead.active_count,
                    "max": bulkhead.max_concurrent,
                    "available": bulkhead.max_concurrent - bulkhead.active_count
                }
            )

        if operation == "list":
            return ActionResult(
                success=True,
                message=f"共 {len(_bulkheads)} 个隔板",
                data={
                    "bulkheads": [
                        {"name": b.name, "active": b.active_count, "max": b.max_concurrent}
                        for b in _bulkheads.values()
                    ]
                }
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class TimeoutAction(BaseAction):
    """Timeout handling for operations."""
    action_type = "api_timeout"
    display_name = "超时处理"
    description = "为操作添加超时控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        timeout_seconds = params.get("timeout_seconds", 30.0)
        default_value = params.get("default_value")

        if timeout_seconds <= 0:
            return ActionResult(success=False, message="timeout_seconds必须大于0")

        def slow_operation():
            time.sleep(min(timeout_seconds * 0.5, 1.0))
            return {"result": "completed", "elapsed": 0.5}

        try:
            result = TimeoutHandler.with_timeout(
                slow_operation,
                timeout_seconds,
                default=default_value
            )
            if isinstance(result, Exception):
                return ActionResult(success=False, message=str(result))
            return ActionResult(success=True, message="操作完成", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"超时处理失败: {e}")


class FallbackAction(BaseAction):
    """Fallback strategy execution."""
    action_type = "api_fallback"
    display_name = "降级策略"
    description = "执行降级备用策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        primary_result = params.get("primary_result")
        fallback_result = params.get("fallback_result")
        use_fallback = params.get("use_fallback", False)

        if use_fallback:
            return ActionResult(
                success=True,
                message="使用降级结果",
                data={"result": fallback_result, "source": "fallback"}
            )

        return ActionResult(
            success=True,
            message="使用主结果",
            data={"result": primary_result, "source": "primary"}
        )
