"""
Automation Guardrails Action Module.

Provides safety guardrails, permission checking, resource limits,
and compliance enforcement for automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time
import uuid

logger = logging.getLogger(__name__)


class Permission(Enum):
    """Permission types."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    EXECUTE = "execute"
    ADMIN = "admin"


class ResourceType(Enum):
    """Resource types for limits."""
    CPU = "cpu"
    MEMORY = "memory"
    NETWORK = "network"
    STORAGE = "storage"
    API_CALLS = "api_calls"
    FILE_OPS = "file_operations"


@dataclass
class Principal:
    """Security principal (user or service)."""
    principal_id: str
    name: str
    roles: Set[str] = field(default_factory=set)
    permissions: Set[Permission] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Resource:
    """Resource with access controls."""
    resource_id: str
    name: str
    resource_type: ResourceType
    owner: Optional[str] = None
    acl: Dict[str, Set[Permission]] = field(default_factory=dict)


@dataclass
class ResourceLimit:
    """Resource usage limit."""
    resource_type: ResourceType
    limit: float
    window: timedelta
    current_usage: float = 0.0
    reset_at: datetime = field(default_factory=datetime.now)


@dataclass
class GuardrailResult:
    """Result of guardrail check."""
    allowed: bool
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)


class PermissionChecker:
    """Checks permissions against ACLs."""

    def __init__(self):
        self.principals: Dict[str, Principal] = {}
        self.resources: Dict[str, Resource] = {}

    def add_principal(self, principal: Principal):
        """Add a principal."""
        self.principals[principal.principal_id] = principal

    def add_resource(self, resource: Resource):
        """Add a resource."""
        self.resources[resource.resource_id] = resource

    def grant_permission(
        self,
        resource_id: str,
        principal_id: str,
        permission: Permission
    ):
        """Grant permission to principal for resource."""
        if resource_id in self.resources:
            if principal_id not in self.resources[resource_id].acl:
                self.resources[resource_id].acl[principal_id] = set()
            self.resources[resource_id].acl[principal_id].add(permission)

    def check_permission(
        self,
        principal_id: str,
        resource_id: str,
        permission: Permission
    ) -> bool:
        """Check if principal has permission for resource."""
        principal = self.principals.get(principal_id)
        resource = self.resources.get(resource_id)

        if not principal or not resource:
            return False

        if Permission.ADMIN in principal.permissions:
            return True

        if principal_id in resource.acl:
            if permission in resource.acl[principal_id]:
                return True

        if resource.owner == principal_id:
            return True

        return False


class RateLimiter:
    """Rate limiting for resources."""

    def __init__(self):
        self.limits: Dict[ResourceType, ResourceLimit] = {}

    def set_limit(
        self,
        resource_type: ResourceType,
        limit: float,
        window: timedelta
    ):
        """Set rate limit for resource type."""
        self.limits[resource_type] = ResourceLimit(
            resource_type=resource_type,
            limit=limit,
            window=window
        )

    def check_limit(self, resource_type: ResourceType, amount: float = 1.0) -> GuardrailResult:
        """Check if request is within rate limit."""
        if resource_type not in self.limits:
            return GuardrailResult(allowed=True, reason="No limit set")

        limit = self.limits[resource_type]

        if datetime.now() >= limit.reset_at:
            limit.current_usage = 0.0
            limit.reset_at = datetime.now() + limit.window

        if limit.current_usage + amount > limit.limit:
            return GuardrailResult(
                allowed=False,
                reason=f"Rate limit exceeded for {resource_type.value}",
                details={
                    "limit": limit.limit,
                    "current": limit.current_usage,
                    "requested": amount
                }
            )

        limit.current_usage += amount
        return GuardrailResult(
            allowed=True,
            reason="Within limits",
            details={"current_usage": limit.current_usage}
        )


class ResourceGuard:
    """Guards resource access."""

    def __init__(self):
        self.usage: Dict[str, float] = defaultdict(float)
        self.max_usage: Dict[str, float] = {}

    def set_max(self, resource_id: str, max_value: float):
        """Set maximum resource usage."""
        self.max_usage[resource_id] = max_value

    def check_usage(self, resource_id: str, amount: float = 1.0) -> GuardrailResult:
        """Check if usage is within limits."""
        current = self.usage.get(resource_id, 0.0)
        max_val = self.max_usage.get(resource_id, float("inf"))

        if current + amount > max_val:
            return GuardrailResult(
                allowed=False,
                reason=f"Resource limit exceeded: {resource_id}",
                details={"current": current, "max": max_val, "requested": amount}
            )

        self.usage[resource_id] = current + amount
        return GuardrailResult(
            allowed=True,
            reason="Resource available",
            details={"current_usage": self.usage[resource_id]}
        )

    def release(self, resource_id: str, amount: float = 1.0):
        """Release resource usage."""
        self.usage[resource_id] = max(0.0, self.usage.get(resource_id, 0.0) - amount)


class ComplianceChecker:
    """Checks compliance with rules."""

    def __init__(self):
        self.rules: List[Tuple[str, Callable]] = []

    def add_rule(self, name: str, checker: Callable[[Any], Tuple[bool, str]]):
        """Add compliance rule."""
        self.rules.append((name, checker))

    def check(self, context: Dict[str, Any]) -> List[Tuple[str, bool, str]]:
        """Check all compliance rules."""
        results = []
        for name, checker in self.rules:
            try:
                passed, reason = checker(context)
                results.append((name, passed, reason))
            except Exception as e:
                results.append((name, False, f"Error: {str(e)}"))
        return results


class GuardrailEngine:
    """Main guardrail enforcement engine."""

    def __init__(self):
        self.permission_checker = PermissionChecker()
        self.rate_limiter = RateLimiter()
        self.resource_guard = ResourceGuard()
        self.compliance_checker = ComplianceChecker()
        self._audit_log: List[Dict[str, Any]] = []

    def add_audit_log(self, entry: Dict[str, Any]):
        """Add to audit log."""
        entry["timestamp"] = datetime.now().isoformat()
        self._audit_log.append(entry)

    def get_audit_log(
        self,
        principal_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        log = self._audit_log
        if principal_id:
            log = [e for e in log if e.get("principal_id") == principal_id]
        return log[-limit:]

    async def check_guardrails(
        self,
        principal: Principal,
        action: str,
        resource_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> GuardrailResult:
        """Check all guardrails for action."""
        context = context or {}

        permission_map = {
            "read": Permission.READ,
            "write": Permission.WRITE,
            "delete": Permission.DELETE,
            "execute": Permission.EXECUTE
        }

        permission = permission_map.get(action)

        if resource_id and permission:
            if not self.permission_checker.check_permission(
                principal.principal_id,
                resource_id,
                permission
            ):
                self.add_audit_log({
                    "principal_id": principal.principal_id,
                    "action": action,
                    "resource_id": resource_id,
                    "result": "denied",
                    "reason": "Permission denied"
                })
                return GuardrailResult(
                    allowed=False,
                    reason=f"Permission denied for {action} on {resource_id}"
                )

        compliance_results = self.compliance_checker.check(context)
        failed = [r for r in compliance_results if not r[1]]
        if failed:
            self.add_audit_log({
                "principal_id": principal.principal_id,
                "action": action,
                "resource_id": resource_id,
                "result": "denied",
                "reason": f"Compliance failure: {failed[0][0]}"
            })
            return GuardrailResult(
                allowed=False,
                reason=f"Compliance violation: {failed[0][2]}"
            )

        self.add_audit_log({
            "principal_id": principal.principal_id,
            "action": action,
            "resource_id": resource_id,
            "result": "allowed"
        })

        return GuardrailResult(
            allowed=True,
            reason="All guardrails passed"
        )


async def main():
    """Demonstrate guardrails."""
    engine = GuardrailEngine()

    principal = Principal(
        principal_id="user1",
        name="Test User",
        roles={"user"}
    )
    engine.permission_checker.add_principal(principal)

    resource = Resource(
        resource_id="file1",
        name="Test File",
        resource_type=ResourceType.STORAGE,
        owner="user1"
    )
    engine.permission_checker.add_resource(resource)

    result = await engine.check_guardrails(
        principal,
        "read",
        "file1"
    )
    print(f"Guardrail result: {result.allowed}, {result.reason}")


if __name__ == "__main__":
    asyncio.run(main())
