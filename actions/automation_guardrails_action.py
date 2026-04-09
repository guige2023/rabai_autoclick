"""
Automation Guardrails Action Module.

Provides safety guardrails for automation workflows including
checks, limits, permissions, and audit logging.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class ViolationType(Enum):
    """Types of guardrail violations."""
    RATE_LIMIT = "rate_limit"
    RESOURCE_LIMIT = "resource_limit"
    PERMISSION_DENIED = "permission_denied"
    SAFETY_CHECK_FAILED = "safety_check_failed"
    TIMEOUT = "timeout"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class Violation:
    """A guardrail violation event."""
    type: ViolationType
    message: str
    timestamp: float = field(default_factory=time.time)
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceLimit:
    """Resource usage limit."""
    name: str
    max_usage: int
    window_seconds: float
    current_usage: int = 0
    window_start: float = field(default_factory=time.time)


@dataclass
class Permission:
    """Permission definition."""
    name: str
    resource: str
    actions: Set[str] = field(default_factory=set)


@dataclass
class GuardrailConfig:
    """Configuration for guardrails."""
    max_execution_time: float = 300.0
    max_retries: int = 3
    enable_audit_log: bool = True
    strict_mode: bool = False


class GuardrailViolation(Exception):
    """Exception raised when a guardrail is violated."""
    pass


class AuditLogger:
    """Audit logger for tracking actions."""

    def __init__(self, max_entries: int = 10000) -> None:
        self.entries: List[Dict[str, Any]] = []
        self.max_entries = max_entries

    def log(
        self,
        action: str,
        actor: str,
        resource: str,
        result: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log an audit entry."""
        entry = {
            "timestamp": time.time(),
            "action": action,
            "actor": actor,
            "resource": resource,
            "result": result,
            "metadata": metadata or {},
        }
        self.entries.append(entry)
        if len(self.entries) > self.max_entries:
            self.entries = self.entries[-self.max_entries:]

    def get_entries(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        actor: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query audit log entries."""
        results = self.entries
        if start_time is not None:
            results = [e for e in results if e["timestamp"] >= start_time]
        if end_time is not None:
            results = [e for e in results if e["timestamp"] <= end_time]
        if actor is not None:
            results = [e for e in results if e["actor"] == actor]
        return results


class SafetyChecker:
    """Safety check system for automation."""

    def __init__(self) -> None:
        self.checks: Dict[str, Callable[[], bool]] = {}

    def register_check(self, name: str, check_func: Callable[[], bool]) -> None:
        """Register a safety check."""
        self.checks[name] = check_func

    def run_checks(self) -> List[str]:
        """Run all safety checks. Returns list of failed check names."""
        failed = []
        for name, check in self.checks.items():
            try:
                if not check():
                    failed.append(name)
            except Exception:
                failed.append(name)
        return failed

    def assert_safe(self) -> None:
        """Assert all safety checks pass. Raises if any fail."""
        failed = self.run_checks()
        if failed:
            raise GuardrailViolation(
                f"Safety checks failed: {', '.join(failed)}"
            )


class AutomationGuardrails:
    """Main guardrails system for automation workflows."""

    def __init__(self, config: Optional[GuardrailConfig] = None) -> None:
        self.config = config or GuardrailConfig()
        self.resource_limits: Dict[str, ResourceLimit] = {}
        self.permissions: Dict[str, Permission] = {}
        self.violations: List[Violation] = []
        self.audit_logger = AuditLogger()
        self.safety_checker = SafetyChecker()
        self.active_workflows: Set[str] = set()
        self._execution_start: Optional[float] = None

    def add_resource_limit(
        self,
        name: str,
        max_usage: int,
        window_seconds: float,
    ) -> None:
        """Add a resource limit."""
        self.resource_limits[name] = ResourceLimit(
            name=name,
            max_usage=max_usage,
            window_seconds=window_seconds,
        )

    def check_resource(self, name: str) -> bool:
        """Check if resource usage is within limits."""
        if name not in self.resource_limits:
            return True

        limit = self.resource_limits[name]
        now = time.time()

        # Reset window if expired
        if now - limit.window_start >= limit.window_seconds:
            limit.current_usage = 0
            limit.window_start = now

        return limit.current_usage < limit.max_usage

    def consume_resource(self, name: str, amount: int = 1) -> bool:
        """Consume resource usage. Returns True if allowed."""
        if not self.check_resource(name):
            self._record_violation(
                ViolationType.RESOURCE_LIMIT,
                f"Resource limit exceeded: {name}",
                {"resource": name, "amount": amount},
            )
            return False

        if name in self.resource_limits:
            self.resource_limits[name].current_usage += amount
        return True

    def add_permission(
        self,
        name: str,
        resource: str,
        actions: List[str],
    ) -> None:
        """Add a permission."""
        self.permissions[name] = Permission(
            name=name,
            resource=resource,
            actions=set(actions),
        )

    def check_permission(
        self,
        permission_name: str,
        resource: str,
        action: str,
    ) -> bool:
        """Check if action is permitted."""
        if permission_name not in self.permissions:
            if self.config.strict_mode:
                self._record_violation(
                    ViolationType.PERMISSION_DENIED,
                    f"Unknown permission: {permission_name}",
                    {"resource": resource, "action": action},
                )
            return not self.config.strict_mode

        permission = self.permissions[permission_name]
        if resource != permission.resource or action not in permission.actions:
            self._record_violation(
                ViolationType.PERMISSION_DENIED,
                f"Permission denied: {permission_name}",
                {"resource": resource, "action": action},
            )
            return False

        return True

    def _record_violation(
        self,
        violation_type: ViolationType,
        message: str,
        context: Dict[str, Any],
    ) -> None:
        """Record a guardrail violation."""
        violation = Violation(
            type=violation_type,
            message=message,
            context=context,
        )
        self.violations.append(violation)
        if self.config.enable_audit_log:
            self.audit_logger.log(
                action="guardrail_violation",
                actor="system",
                resource=str(violation_type.value),
                result="blocked",
                metadata={"message": message, "context": context},
            )

    async def execute_with_guardrails(
        self,
        workflow_id: str,
        func: Callable,
        *args,
        **kwargs,
    ) -> Any:
        """Execute function with guardrail protection."""
        self._execution_start = time.time()
        self.active_workflows.add(workflow_id)

        try:
            # Run safety checks
            self.safety_checker.assert_safe()

            # Check timeout
            elapsed = time.time() - self._execution_start
            if elapsed > self.config.max_execution_time:
                raise GuardrailViolation(f"Execution timeout: {elapsed:.1f}s")

            # Execute
            result = await func(*args, **kwargs)

            return result

        except GuardrailViolation:
            raise
        except Exception as e:
            self._record_violation(
                ViolationType.SAFETY_CHECK_FAILED,
                f"Execution failed: {str(e)}",
                {"workflow_id": workflow_id},
            )
            raise
        finally:
            self.active_workflows.discard(workflow_id)
            self._execution_start = None

    def get_stats(self) -> Dict[str, Any]:
        """Get guardrail statistics."""
        return {
            "active_workflows": len(self.active_workflows),
            "total_violations": len(self.violations),
            "resource_limits": {
                name: {
                    "current_usage": limit.current_usage,
                    "max_usage": limit.max_usage,
                }
                for name, limit in self.resource_limits.items()
            },
            "violations_by_type": {
                vt.value: sum(1 for v in self.violations if v.type == vt)
                for vt in ViolationType
            },
        }
