"""API Quota Action Module.

Provides API quota management with rate limiting, budget tracking,
resource allocation, and quota exhaustion handling.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QuotaType(Enum):
    """Quota types."""
    RATE_LIMIT = "rate_limit"
    DAILY = "daily"
    MONTHLY = "monthly"
    BUDGET = "budget"
    STORAGE = "storage"


@dataclass
class QuotaLimit:
    """Individual quota limit."""
    quota_type: QuotaType
    limit_value: float
    used_value: float
    window_seconds: float
    reset_at: float
    tier: str


@dataclass
class QuotaAllocation:
    """Quota allocation for a user or service."""
    owner_id: str
    quotas: Dict[str, QuotaLimit]
    created_at: float
    last_updated: float


class ApiQuotaAction(BaseAction):
    """API Quota Manager.

    Manages API quotas with multi-tier support,
    automatic reset, and quota exhaustion handling.
    """
    action_type = "api_quota"
    display_name = "API配额管理器"
    description = "API配额管理，限流和预算控制"

    _allocations: Dict[str, QuotaAllocation] = {}
    _lock = threading.RLock()
    _default_tiers = {
        'free': {'rate_limit': 100, 'daily': 1000, 'monthly': 10000},
        'basic': {'rate_limit': 1000, 'daily': 10000, 'monthly': 100000},
        'pro': {'rate_limit': 10000, 'daily': 100000, 'monthly': 1000000},
        'enterprise': {'rate_limit': 100000, 'daily': 1000000, 'monthly': 10000000},
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quota operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'check', 'consume', 'allocate', 'reset',
                               'get_status', 'set_limit', 'list_tiers'
                - owner_id: str - user or service identifier
                - quota_name: str - name of the quota
                - amount: float (optional) - amount to consume
                - tier: str (optional) - quota tier
                - limit_value: float (optional) - custom limit

        Returns:
            ActionResult with quota status.
        """
        start_time = time.time()
        operation = params.get('operation', 'check')

        try:
            with self._lock:
                if operation == 'check':
                    return self._check_quota(params, start_time)
                elif operation == 'consume':
                    return self._consume_quota(params, start_time)
                elif operation == 'allocate':
                    return self._allocate_quota(params, start_time)
                elif operation == 'reset':
                    return self._reset_quota(params, start_time)
                elif operation == 'get_status':
                    return self._get_status(params, start_time)
                elif operation == 'set_limit':
                    return self._set_limit(params, start_time)
                elif operation == 'list_tiers':
                    return self._list_tiers(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Quota error: {str(e)}",
                duration=time.time() - start_time
            )

    def _check_quota(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check if quota is available."""
        owner_id = params.get('owner_id', 'default')
        quota_name = params.get('quota_name', 'rate_limit')
        amount = params.get('amount', 1.0)

        self._ensure_allocation(owner_id)
        allocation = self._allocations[owner_id]

        if quota_name not in allocation.quotas:
            return ActionResult(
                success=True,
                message=f"Quota '{quota_name}' not allocated, assuming available",
                data={'available': True, 'owner_id': owner_id, 'quota_name': quota_name},
                duration=time.time() - start_time
            )

        quota = allocation.quotas[quota_name]
        available = quota.limit_value - quota.used_value
        can_consume = available >= amount

        return ActionResult(
            success=True,
            message=f"Quota check: {'allowed' if can_consume else 'denied'}",
            data={
                'owner_id': owner_id,
                'quota_name': quota_name,
                'available': available,
                'can_consume': can_consume,
                'limit': quota.limit_value,
                'used': quota.used_value,
                'reset_at': quota.reset_at,
            },
            duration=time.time() - start_time
        )

    def _consume_quota(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Consume quota units."""
        owner_id = params.get('owner_id', 'default')
        quota_name = params.get('quota_name', 'rate_limit')
        amount = params.get('amount', 1.0)

        self._ensure_allocation(owner_id)
        allocation = self._allocations[owner_id]

        if quota_name not in allocation.quotas:
            allocation.quotas[quota_name] = QuotaLimit(
                quota_type=QuotaType.RATE_LIMIT,
                limit_value=100,
                used_value=0,
                window_seconds=60,
                reset_at=time.time() + 60,
                tier='free'
            )

        quota = allocation.quotas[quota_name]

        if time.time() >= quota.reset_at:
            quota.used_value = 0.0
            quota.reset_at = time.time() + quota.window_seconds

        if quota.used_value + amount > quota.limit_value:
            return ActionResult(
                success=False,
                message=f"Quota exhausted for {quota_name}",
                data={
                    'consumed': 0,
                    'available': quota.limit_value - quota.used_value,
                    'reset_at': quota.reset_at,
                    'retry_after_sec': max(0, quota.reset_at - time.time()),
                },
                duration=time.time() - start_time
            )

        quota.used_value += amount
        allocation.last_updated = time.time()

        return ActionResult(
            success=True,
            message=f"Consumed {amount} units of {quota_name}",
            data={
                'consumed': amount,
                'remaining': quota.limit_value - quota.used_value,
                'reset_at': quota.reset_at,
            },
            duration=time.time() - start_time
        )

    def _allocate_quota(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Allocate quota for a user/service."""
        owner_id = params.get('owner_id', 'default')
        tier = params.get('tier', 'free')

        if tier not in self._default_tiers:
            return ActionResult(success=False, message=f"Unknown tier: {tier}", duration=time.time() - start_time)

        limits = self._default_tiers[tier]
        quotas: Dict[str, QuotaLimit] = {}

        now = time.time()
        quotas['rate_limit'] = QuotaLimit(QuotaType.RATE_LIMIT, limits['rate_limit'], 0, 60, now + 60, tier)
        quotas['daily'] = QuotaLimit(QuotaType.DAILY, limits['daily'], 0, 86400, now + 86400, tier)
        quotas['monthly'] = QuotaLimit(QuotaType.MONTHLY, limits['monthly'], 0, 2592000, now + 2592000, tier)

        self._allocations[owner_id] = QuotaAllocation(
            owner_id=owner_id,
            quotas=quotas,
            created_at=now,
            last_updated=now
        )

        return ActionResult(
            success=True,
            message=f"Allocated {tier} quota to {owner_id}",
            data={'owner_id': owner_id, 'tier': tier, 'quotas': {k: {'limit': v.limit_value, 'window_sec': v.window_seconds} for k, v in quotas.items()}},
            duration=time.time() - start_time
        )

    def _reset_quota(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset quota for a user/service."""
        owner_id = params.get('owner_id', 'default')
        quota_name = params.get('quota_name')

        if owner_id not in self._allocations:
            return ActionResult(success=False, message=f"No allocation found for {owner_id}", duration=time.time() - start_time)

        allocation = self._allocations[owner_id]

        if quota_name:
            if quota_name in allocation.quotas:
                allocation.quotas[quota_name].used_value = 0.0
                allocation.quotas[quota_name].reset_at = time.time() + allocation.quotas[quota_name].window_seconds
        else:
            for q in allocation.quotas.values():
                q.used_value = 0.0
                q.reset_at = time.time() + q.window_seconds

        allocation.last_updated = time.time()

        return ActionResult(
            success=True,
            message=f"Quota reset for {owner_id}" + (f" ({quota_name})" if quota_name else ""),
            data={'owner_id': owner_id, 'reset_at': time.time()},
            duration=time.time() - start_time
        )

    def _get_status(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get quota status for a user."""
        owner_id = params.get('owner_id', 'default')

        self._ensure_allocation(owner_id)
        allocation = self._allocations[owner_id]

        status = {}
        for name, quota in allocation.quotas.items():
            status[name] = {
                'limit': quota.limit_value,
                'used': quota.used_value,
                'available': quota.limit_value - quota.used_value,
                'usage_pct': (quota.used_value / quota.limit_value * 100) if quota.limit_value > 0 else 0,
                'reset_at': quota.reset_at,
                'tier': quota.tier,
            }

        return ActionResult(
            success=True,
            message=f"Quota status for {owner_id}",
            data={'owner_id': owner_id, 'quotas': status},
            duration=time.time() - start_time
        )

    def _set_limit(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set custom quota limit."""
        owner_id = params.get('owner_id', 'default')
        quota_name = params.get('quota_name', 'rate_limit')
        limit_value = params.get('limit_value', 100)
        window_seconds = params.get('window_seconds', 60)

        self._ensure_allocation(owner_id)
        allocation = self._allocations[owner_id]

        if quota_name in allocation.quotas:
            allocation.quotas[quota_name].limit_value = limit_value
            allocation.quotas[quota_name].window_seconds = window_seconds
        else:
            allocation.quotas[quota_name] = QuotaLimit(
                quota_type=QuotaType.RATE_LIMIT,
                limit_value=limit_value,
                used_value=0,
                window_seconds=window_seconds,
                reset_at=time.time() + window_seconds,
                tier='custom'
            )

        allocation.last_updated = time.time()

        return ActionResult(
            success=True,
            message=f"Set {quota_name} limit to {limit_value}",
            data={'owner_id': owner_id, 'quota_name': quota_name, 'limit': limit_value, 'window_sec': window_seconds},
            duration=time.time() - start_time
        )

    def _list_tiers(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List available quota tiers."""
        return ActionResult(
            success=True,
            message="Available quota tiers",
            data={'tiers': self._default_tiers},
            duration=time.time() - start_time
        )

    def _ensure_allocation(self, owner_id: str) -> None:
        """Ensure allocation exists for owner."""
        if owner_id not in self._allocations:
            self._allocate_quota({'owner_id': owner_id, 'tier': 'free'}, time.time())
