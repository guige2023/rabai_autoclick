"""
Data Governance Action Module

Provides data governance and compliance capabilities including access control,
audit logging, policy enforcement, and data lineage tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class Permission(Enum):
    """Permission levels."""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    NONE = "none"


class DataClassification(Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class PolicyEffect(Enum):
    """IAM policy effect."""

    ALLOW = "allow"
    DENY = "deny"


@dataclass
class Principal:
    """A principal (user or service) in the governance system."""

    principal_id: str
    principal_type: str
    name: str
    roles: Set[str] = field(default_factory=set)
    groups: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataResource:
    """A data resource subject to governance."""

    resource_id: str
    resource_type: str
    name: str
    classification: DataClassification = DataClassification.INTERNAL
    owner: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Policy:
    """An IAM-style policy."""

    policy_id: str
    name: str
    effect: PolicyEffect
    principals: Set[str] = field(default_factory=set)
    resources: Set[str] = field(default_factory=set)
    permissions: Set[Permission] = field(default_factory=set)
    conditions: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    enabled: bool = True


@dataclass
class AuditEntry:
    """An audit log entry."""

    entry_id: str
    timestamp: float
    principal_id: str
    action: str
    resource_id: str
    permission: Permission
    effect: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageRecord:
    """A data lineage record."""

    record_id: str
    source_resource: str
    target_resource: str
    operation: str
    timestamp: float
    principal_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GovernanceConfig:
    """Configuration for data governance."""

    enable_audit_logging: bool = True
    enable_lineage_tracking: bool = True
    default_classification: DataClassification = DataClassification.INTERNAL
    enforce_classification: bool = True
    policy_evaluation_mode: str = "deny-overwrite"


class PolicyEngine:
    """Evaluates access policies."""

    def __init__(self, config: Optional[GovernanceConfig] = None):
        self.config = config or GovernanceConfig()
        self._policies: Dict[str, Policy] = {}

    def add_policy(self, policy: Policy) -> None:
        """Add a policy."""
        self._policies[policy.policy_id] = policy

    def evaluate(
        self,
        principal: Principal,
        resource: DataResource,
        permission: Permission,
    ) -> bool:
        """
        Evaluate if principal has permission on resource.

        Uses deny-overwrite: if any policy denies, the result is denied.
        """
        applicable_policies = []

        for policy in self._policies.values():
            if not policy.enabled:
                continue

            # Check if policy applies to this principal
            applies = False
            if policy.principals:
                if principal.principal_id in policy.principals:
                    applies = True
                if any(role in policy.principals for role in principal.roles):
                    applies = True
                if any(group in policy.principals for group in principal.groups):
                    applies = True
            else:
                applies = True  # Empty principals matches all

            if not applies:
                continue

            # Check if policy applies to this resource
            if policy.resources:
                if resource.resource_id not in policy.resources:
                    continue

            applicable_policies.append(policy)

        # Deny-overwrite evaluation
        for policy in applicable_policies:
            if policy.effect == PolicyEffect.DENY:
                if permission in policy.permissions or Permission.ADMIN in policy.permissions:
                    return False

        for policy in applicable_policies:
            if policy.effect == PolicyEffect.ALLOW:
                if permission in policy.permissions or Permission.ADMIN in policy.permissions:
                    return True

        return False


class AuditLogger:
    """Logs governance audit entries."""

    def __init__(self, config: Optional[GovernanceConfig] = None):
        self.config = config or GovernanceConfig()
        self._entries: List[AuditEntry] = []

    def log(
        self,
        principal_id: str,
        action: str,
        resource_id: str,
        permission: Permission,
        effect: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> AuditEntry:
        """Log an audit entry."""
        entry = AuditEntry(
            entry_id=f"audit_{uuid.uuid4().hex[:12]}",
            timestamp=time.time(),
            principal_id=principal_id,
            action=action,
            resource_id=resource_id,
            permission=permission,
            effect=effect,
            details=details or {},
        )
        self._entries.append(entry)
        return entry

    def query(
        self,
        principal_id: Optional[str] = None,
        resource_id: Optional[str] = None,
        action: Optional[str] = None,
        from_timestamp: Optional[float] = None,
        to_timestamp: Optional[float] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        """Query audit entries."""
        results = self._entries

        if principal_id:
            results = [e for e in results if e.principal_id == principal_id]
        if resource_id:
            results = [e for e in results if e.resource_id == resource_id]
        if action:
            results = [e for e in results if e.action == action]
        if from_timestamp:
            results = [e for e in results if e.timestamp >= from_timestamp]
        if to_timestamp:
            results = [e for e in results if e.timestamp <= to_timestamp]

        return sorted(results, key=lambda e: -e.timestamp)[:limit]


class LineageTracker:
    """Tracks data lineage."""

    def __init__(self, config: Optional[GovernanceConfig] = None):
        self.config = config or GovernanceConfig()
        self._records: List[LineageRecord] = []

    def record(
        self,
        source_resource: str,
        target_resource: str,
        operation: str,
        principal_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LineageRecord:
        """Record a lineage relationship."""
        record = LineageRecord(
            record_id=f"lineage_{uuid.uuid4().hex[:12]}",
            source_resource=source_resource,
            target_resource=target_resource,
            operation=operation,
            timestamp=time.time(),
            principal_id=principal_id,
            metadata=metadata or {},
        )
        self._records.append(record)
        return record

    def get_upstream(
        self,
        resource_id: str,
    ) -> List[LineageRecord]:
        """Get upstream lineage for a resource."""
        return [
            r for r in self._records
            if r.target_resource == resource_id
        ]

    def get_downstream(
        self,
        resource_id: str,
    ) -> List[LineageRecord]:
        """Get downstream lineage for a resource."""
        return [
            r for r in self._records
            if r.source_resource == resource_id
        ]

    def get_full_lineage(
        self,
        resource_id: str,
    ) -> Dict[str, List[LineageRecord]]:
        """Get full lineage (upstream and downstream)."""
        return {
            "upstream": self.get_upstream(resource_id),
            "downstream": self.get_downstream(resource_id),
        }


class DataGovernanceAction:
    """
    Data governance action for compliance and access control.

    Features:
    - IAM-style policy engine with deny-overwrite evaluation
    - Audit logging for all data access
    - Data lineage tracking
    - Multi-level data classification
    - Principal and resource management
    - Policy-based access control

    Usage:
        governance = DataGovernanceAction(config)
        
        # Add policies
        governance.add_policy(policy)
        
        # Check access
        allowed = governance.check_access(principal, resource, Permission.READ)
        
        # Access data with governance
        data = await governance.access_data(principal, resource, Permission.READ, data_func)
    """

    def __init__(self, config: Optional[GovernanceConfig] = None):
        self.config = config or GovernanceConfig()
        self._policy_engine = PolicyEngine(self.config)
        self._audit_logger = AuditLogger(self.config)
        self._lineage_tracker = LineageTracker(self.config)
        self._principals: Dict[str, Principal] = {}
        self._resources: Dict[str, DataResource] = {}
        self._stats = {
            "access_checks": 0,
            "access_allowed": 0,
            "access_denied": 0,
            "audit_entries": 0,
            "lineage_records": 0,
        }

    def add_principal(self, principal: Principal) -> None:
        """Add a principal to the system."""
        self._principals[principal.principal_id] = principal

    def add_resource(self, resource: DataResource) -> None:
        """Add a resource to the system."""
        self._resources[resource.resource_id] = resource

    def add_policy(self, policy: Policy) -> None:
        """Add an access policy."""
        self._policy_engine.add_policy(policy)

    def check_access(
        self,
        principal: Principal,
        resource: DataResource,
        permission: Permission,
    ) -> bool:
        """
        Check if a principal has permission on a resource.

        Args:
            principal: The principal requesting access
            resource: The resource being accessed
            permission: The requested permission

        Returns:
            True if access is allowed, False otherwise
        """
        self._stats["access_checks"] += 1

        allowed = self._policy_engine.evaluate(principal, resource, permission)

        if self.config.enable_audit_logging:
            self._audit_logger.log(
                principal_id=principal.principal_id,
                action=f"{permission.value}_access",
                resource_id=resource.resource_id,
                permission=permission,
                effect="allow" if allowed else "deny",
                details={"resource_type": resource.resource_type},
            )
            self._stats["audit_entries"] += 1

        if allowed:
            self._stats["access_allowed"] += 1
        else:
            self._stats["access_denied"] += 1

        return allowed

    async def access_data(
        self,
        principal: Principal,
        resource: DataResource,
        permission: Permission,
        data_func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Access data with governance enforcement.

        Args:
            principal: The principal requesting access
            resource: The resource being accessed
            permission: The requested permission
            data_func: Function to execute if access is allowed
            *args, **kwargs: Arguments for data_func

        Returns:
            Data from data_func if access is allowed

        Raises:
            AccessDeniedError if access is not allowed
        """
        if not self.check_access(principal, resource, permission):
            raise AccessDeniedError(
                f"Access denied: {principal.principal_id} cannot {permission.value} {resource.resource_id}"
            )

        try:
            if asyncio.iscoroutinefunction(data_func):
                result = await data_func(*args, **kwargs)
            else:
                result = data_func(*args, **kwargs)

            # Record lineage if this is a write operation
            if permission in {Permission.WRITE, Permission.DELETE} and self.config.enable_lineage_tracking:
                self.record_lineage(
                    source_resource=resource.resource_id,
                    target_resource=resource.resource_id,
                    operation=permission.value,
                    principal_id=principal.principal_id,
                )

            return result

        except Exception as e:
            raise

    def record_lineage(
        self,
        source_resource: str,
        target_resource: str,
        operation: str,
        principal_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LineageRecord:
        """Record a data lineage relationship."""
        record = self._lineage_tracker.record(
            source_resource=source_resource,
            target_resource=target_resource,
            operation=operation,
            principal_id=principal_id,
            metadata=metadata,
        )
        self._stats["lineage_records"] += 1
        return record

    def get_audit_log(
        self,
        principal_id: Optional[str] = None,
        resource_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        """Get audit log entries."""
        return self._audit_logger.query(
            principal_id=principal_id,
            resource_id=resource_id,
            limit=limit,
        )

    def get_lineage(
        self,
        resource_id: str,
    ) -> Dict[str, List[LineageRecord]]:
        """Get lineage for a resource."""
        return self._lineage_tracker.get_full_lineage(resource_id)

    def get_principal(self, principal_id: str) -> Optional[Principal]:
        """Get a principal by ID."""
        return self._principals.get(principal_id)

    def get_resource(self, resource_id: str) -> Optional[DataResource]:
        """Get a resource by ID."""
        return self._resources.get(resource_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get governance statistics."""
        return {
            **self._stats.copy(),
            "total_principals": len(self._principals),
            "total_resources": len(self._resources),
            "total_policies": len(self._policy_engine._policies),
            "total_audit_entries": len(self._audit_logger._entries),
            "total_lineage_records": len(self._lineage_tracker._records),
        }


class AccessDeniedError(Exception):
    """Raised when access is denied by governance policy."""

    pass


async def demo_governance():
    """Demonstrate data governance."""
    config = GovernanceConfig(
        enable_audit_logging=True,
        enable_lineage_tracking=True,
    )
    governance = DataGovernanceAction(config)

    # Create principal
    user = Principal(
        principal_id="user-001",
        principal_type="user",
        name="Alice",
        roles={"data-analyst"},
    )
    governance.add_principal(user)

    # Create resource
    dataset = DataResource(
        resource_id="dataset-sales",
        resource_type="dataset",
        name="Sales Dataset",
        classification=DataClassification.CONFIDENTIAL,
        owner="admin",
    )
    governance.add_resource(dataset)

    # Add policy
    policy = Policy(
        policy_id="policy-001",
        name="Analysts can read confidential data",
        effect=PolicyEffect.ALLOW,
        principals={"data-analyst"},
        resources={"dataset-sales"},
        permissions={Permission.READ},
    )
    governance.add_policy(policy)

    # Check access
    allowed = governance.check_access(user, dataset, Permission.READ)
    print(f"Access allowed: {allowed}")

    # Audit log
    audit = governance.get_audit_log()
    print(f"Audit entries: {len(audit)}")

    print(f"Stats: {governance.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_governance())
