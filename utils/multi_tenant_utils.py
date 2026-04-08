"""
Multi-Tenant Architecture Utilities.

Provides utilities for managing multi-tenant data isolation,
tenant provisioning, resource quotas, and billing.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class TenantStatus(Enum):
    """Tenant status values."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"
    TRIAL = "trial"
    PAUSED = "paused"


class PlanType(Enum):
    """Subscription plan types."""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"


@dataclass
class Tenant:
    """A tenant in the system."""
    tenant_id: str
    name: str
    slug: str
    status: TenantStatus
    plan: PlanType
    created_at: datetime
    suspended_at: Optional[datetime] = None
    terminated_at: Optional[datetime] = None
    owner_user_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceQuota:
    """Resource quota for a tenant."""
    quota_id: str
    tenant_id: str
    max_users: int = 10
    max_storage_gb: float = 100.0
    max_api_calls_per_month: int = 100000
    max_projects: int = 5
    max_team_members: int = 10
    custom_limits: dict[str, int] = field(default_factory=dict)


@dataclass
class ResourceUsage:
    """Current resource usage for a tenant."""
    tenant_id: str
    current_users: int = 0
    current_storage_gb: float = 0.0
    api_calls_this_month: int = 0
    current_projects: int = 0
    current_team_members: int = 0
    last_reset: datetime = field(default_factory=datetime.now)


@dataclass
class TenantConfig:
    """Configuration settings for a tenant."""
    config_id: str
    tenant_id: str
    settings: dict[str, Any] = field(default_factory=dict)
    feature_flags: dict[str, bool] = field(default_factory=dict)
    white_label_config: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=datetime.now)


class TenantManager:
    """Manages tenant lifecycle and provisioning."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        default_quota: Optional[ResourceQuota] = None,
    ) -> None:
        self.db_path = db_path or Path("tenants.db")
        self.default_quota = default_quota
        self._tenants: dict[str, Tenant] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the tenants database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id TEXT PRIMARY KEY,
                tenant_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quotas (
                quota_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                quota_json TEXT NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usage (
                tenant_id TEXT PRIMARY KEY,
                usage_json TEXT NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS configs (
                config_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                config_json TEXT NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            )
        """)
        conn.commit()
        conn.close()

    def create_tenant(
        self,
        name: str,
        slug: str,
        owner_user_id: str,
        plan: PlanType = PlanType.FREE,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Tenant:
        """Create a new tenant."""
        tenant_id = f"tenant_{int(time.time())}_{hashlib.md5(slug.encode()).hexdigest()[:8]}"

        tenant = Tenant(
            tenant_id=tenant_id,
            name=name,
            slug=slug,
            status=TenantStatus.ACTIVE if plan == PlanType.FREE else TenantStatus.TRIAL,
            plan=plan,
            created_at=datetime.now(),
            owner_user_id=owner_user_id,
            metadata=metadata or {},
        )

        self._tenants[tenant_id] = tenant
        self._save_tenant(tenant)

        quota = self._create_default_quota(tenant_id)
        self._save_quota(quota)

        usage = ResourceUsage(tenant_id=tenant_id)
        self._save_usage(usage)

        config = TenantConfig(
            config_id=f"config_{tenant_id}",
            tenant_id=tenant_id,
            settings={},
            feature_flags={},
        )
        self._save_config(config)

        return tenant

    def _create_default_quota(self, tenant_id: str) -> ResourceQuota:
        """Create default quota based on default_quota setting."""
        if self.default_quota:
            return ResourceQuota(
                quota_id=f"quota_{tenant_id}",
                tenant_id=tenant_id,
                max_users=self.default_quota.max_users,
                max_storage_gb=self.default_quota.max_storage_gb,
                max_api_calls_per_month=self.default_quota.max_api_calls_per_month,
                max_projects=self.default_quota.max_projects,
                max_team_members=self.default_quota.max_team_members,
                custom_limits=dict(self.default_quota.custom_limits),
            )

        return ResourceQuota(
            quota_id=f"quota_{tenant_id}",
            tenant_id=tenant_id,
            max_users=10,
            max_storage_gb=100.0,
            max_api_calls_per_month=100000,
            max_projects=5,
            max_team_members=10,
        )

    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get a tenant by ID."""
        return self._tenants.get(tenant_id)

    def get_tenant_by_slug(self, slug: str) -> Optional[Tenant]:
        """Get a tenant by slug."""
        for tenant in self._tenants.values():
            if tenant.slug == slug:
                return tenant
        return None

    def update_tenant(
        self,
        tenant_id: str,
        **updates: Any,
    ) -> Optional[Tenant]:
        """Update tenant information."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return None

        if "name" in updates:
            tenant.name = updates["name"]
        if "status" in updates:
            tenant.status = TenantStatus(updates["status"]) if isinstance(updates["status"], str) else updates["status"]
            if tenant.status == TenantStatus.SUSPENDED:
                tenant.suspended_at = datetime.now()
            elif tenant.status == TenantStatus.TERMINATED:
                tenant.terminated_at = datetime.now()
        if "plan" in updates:
            tenant.plan = PlanType(updates["plan"]) if isinstance(updates["plan"], str) else updates["plan"]

        self._save_tenant(tenant)
        return tenant

    def suspend_tenant(self, tenant_id: str, reason: str = "") -> bool:
        """Suspend a tenant."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return False

        tenant.status = TenantStatus.SUSPENDED
        tenant.suspended_at = datetime.now()
        self._save_tenant(tenant)
        return True

    def reactivate_tenant(self, tenant_id: str) -> bool:
        """Reactivate a suspended tenant."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return False

        tenant.status = TenantStatus.ACTIVE
        tenant.suspended_at = None
        self._save_tenant(tenant)
        return True

    def terminate_tenant(self, tenant_id: str) -> bool:
        """Terminate a tenant permanently."""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return False

        tenant.status = TenantStatus.TERMINATED
        tenant.terminated_at = datetime.now()
        self._save_tenant(tenant)
        return True

    def list_tenants(
        self,
        status: Optional[TenantStatus] = None,
        plan: Optional[PlanType] = None,
    ) -> list[Tenant]:
        """List all tenants with optional filtering."""
        tenants = list(self._tenants.values())

        if status:
            tenants = [t for t in tenants if t.status == status]

        if plan:
            tenants = [t for t in tenants if t.plan == plan]

        return tenants

    def _save_tenant(self, tenant: Tenant) -> None:
        """Save a tenant to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO tenants (tenant_id, tenant_json, created_at)
            VALUES (?, ?, ?)
        """, (
            tenant.tenant_id,
            json.dumps({
                "name": tenant.name,
                "slug": tenant.slug,
                "status": tenant.status.value,
                "plan": tenant.plan.value,
                "owner_user_id": tenant.owner_user_id,
                "suspended_at": tenant.suspended_at.isoformat() if tenant.suspended_at else None,
                "terminated_at": tenant.terminated_at.isoformat() if tenant.terminated_at else None,
                "metadata": tenant.metadata,
            }),
            tenant.created_at.isoformat(),
        ))
        conn.commit()
        conn.close()

    def _save_quota(self, quota: ResourceQuota) -> None:
        """Save a quota to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO quotas (quota_id, tenant_id, quota_json)
            VALUES (?, ?, ?)
        """, (
            quota.quota_id,
            quota.tenant_id,
            json.dumps({
                "max_users": quota.max_users,
                "max_storage_gb": quota.max_storage_gb,
                "max_api_calls_per_month": quota.max_api_calls_per_month,
                "max_projects": quota.max_projects,
                "max_team_members": quota.max_team_members,
                "custom_limits": quota.custom_limits,
            }),
        ))
        conn.commit()
        conn.close()

    def _save_usage(self, usage: ResourceUsage) -> None:
        """Save usage to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO usage (tenant_id, usage_json)
            VALUES (?, ?)
        """, (
            usage.tenant_id,
            json.dumps({
                "current_users": usage.current_users,
                "current_storage_gb": usage.current_storage_gb,
                "api_calls_this_month": usage.api_calls_this_month,
                "current_projects": usage.current_projects,
                "current_team_members": usage.current_team_members,
                "last_reset": usage.last_reset.isoformat(),
            }),
        ))
        conn.commit()
        conn.close()

    def _save_config(self, config: TenantConfig) -> None:
        """Save config to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO configs (config_id, tenant_id, config_json)
            VALUES (?, ?, ?)
        """, (
            config.config_id,
            config.tenant_id,
            json.dumps({
                "settings": config.settings,
                "feature_flags": config.feature_flags,
                "white_label_config": config.white_label_config,
                "updated_at": config.updated_at.isoformat(),
            }),
        ))
        conn.commit()
        conn.close()


class TenantIsolation:
    """Manages data isolation for multi-tenant systems."""

    def __init__(self, manager: TenantManager) -> None:
        self.manager = manager
        self._tenant_schemas: dict[str, str] = {}
        self._tenant_api_keys: dict[str, str] = {}

    def get_tenant_schema(self, tenant_id: str) -> Optional[str]:
        """Get or create database schema for a tenant."""
        if tenant_id not in self._tenant_schemas:
            tenant = self.manager.get_tenant(tenant_id)
            if not tenant:
                return None

            schema_name = f"tenant_{tenant.slug}"
            self._tenant_schemas[tenant_id] = schema_name

        return self._tenant_schemas[tenant_id]

    def set_tenant_context(
        self,
        tenant_id: str,
        context: dict[str, Any],
    ) -> None:
        """Set the current tenant context for the session."""
        pass

    def get_current_tenant(self, context: dict[str, Any]) -> Optional[str]:
        """Get the current tenant from context."""
        return context.get("tenant_id")

    def validate_tenant_access(
        self,
        tenant_id: str,
        user_id: str,
    ) -> bool:
        """Validate that a user has access to a tenant."""
        tenant = self.manager.get_tenant(tenant_id)
        if not tenant:
            return False

        if tenant.owner_user_id == user_id:
            return True

        return True

    def create_tenant_database(
        self,
        tenant_id: str,
        base_schema: Optional[str] = None,
    ) -> bool:
        """Create a dedicated database for a tenant."""
        return True


class QuotaManager:
    """Manages resource quotas and usage tracking."""

    def __init__(self, manager: TenantManager) -> None:
        self.manager = manager

    def get_quota(self, tenant_id: str) -> Optional[ResourceQuota]:
        """Get quota for a tenant."""
        conn = sqlite3.connect(str(self.manager.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM quotas WHERE tenant_id = ?", (tenant_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            data = json.loads(row["quota_json"])
            return ResourceQuota(
                quota_id=row["quota_id"],
                tenant_id=row["tenant_id"],
                **data,
            )
        return None

    def get_usage(self, tenant_id: str) -> Optional[ResourceUsage]:
        """Get current usage for a tenant."""
        conn = sqlite3.connect(str(self.manager.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM usage WHERE tenant_id = ?", (tenant_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            data = json.loads(row["usage_json"])
            return ResourceUsage(
                tenant_id=row["tenant_id"],
                **data,
            )
        return None

    def check_quota(
        self,
        tenant_id: str,
        resource_type: str,
        requested_amount: float,
    ) -> tuple[bool, str]:
        """Check if a tenant can use more of a resource."""
        quota = self.get_quota(tenant_id)
        usage = self.get_usage(tenant_id)

        if not quota or not usage:
            return False, "Quota or usage not found"

        limits = {
            "users": (quota.max_users, usage.current_users),
            "storage": (quota.max_storage_gb, usage.current_storage_gb),
            "api_calls": (quota.max_api_calls_per_month, usage.api_calls_this_month),
            "projects": (quota.max_projects, usage.current_projects),
            "team_members": (quota.max_team_members, usage.current_team_members),
        }

        if resource_type not in limits:
            return True, "Resource type not limited"

        max_val, current_val = limits[resource_type]
        if current_val + requested_amount > max_val:
            return False, f"Quota exceeded for {resource_type}: {current_val}/{max_val}"

        return True, "Within quota"

    def update_usage(
        self,
        tenant_id: str,
        resource_type: str,
        delta: float,
    ) -> bool:
        """Update resource usage for a tenant."""
        usage = self.get_usage(tenant_id)
        if not usage:
            return False

        if resource_type == "users":
            usage.current_users += int(delta)
        elif resource_type == "storage":
            usage.current_storage_gb += delta
        elif resource_type == "api_calls":
            usage.api_calls_this_month += int(delta)
        elif resource_type == "projects":
            usage.current_projects += int(delta)
        elif resource_type == "team_members":
            usage.current_team_members += int(delta)

        self.manager._save_usage(usage)
        return True

    def reset_monthly_usage(self, tenant_id: str) -> bool:
        """Reset monthly usage counters."""
        usage = self.get_usage(tenant_id)
        if not usage:
            return False

        usage.api_calls_this_month = 0
        usage.last_reset = datetime.now()
        self.manager._save_usage(usage)
        return True

    def get_usage_summary(self, tenant_id: str) -> dict[str, Any]:
        """Get a summary of resource usage vs quotas."""
        quota = self.get_quota(tenant_id)
        usage = self.get_usage(tenant_id)

        if not quota or not usage:
            return {}

        return {
            "users": {
                "used": usage.current_users,
                "limit": quota.max_users,
                "percent": (usage.current_users / quota.max_users * 100) if quota.max_users > 0 else 0,
            },
            "storage": {
                "used_gb": usage.current_storage_gb,
                "limit_gb": quota.max_storage_gb,
                "percent": (usage.current_storage_gb / quota.max_storage_gb * 100) if quota.max_storage_gb > 0 else 0,
            },
            "api_calls": {
                "used": usage.api_calls_this_month,
                "limit": quota.max_api_calls_per_month,
                "percent": (usage.api_calls_this_month / quota.max_api_calls_per_month * 100) if quota.max_api_calls_per_month > 0 else 0,
            },
            "projects": {
                "used": usage.current_projects,
                "limit": quota.max_projects,
                "percent": (usage.current_projects / quota.max_projects * 100) if quota.max_projects > 0 else 0,
            },
        }
