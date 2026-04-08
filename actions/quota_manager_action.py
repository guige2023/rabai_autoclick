"""Quota management action module for RabAI AutoClick.

Provides quota operations:
- QuotaCheckAction: Check quota
- QuotaConsumeAction: Consume quota
- QuotaResetAction: Reset quota
- QuotaSetAction: Set quota limit
- QuotaStatusAction: Get quota status
- QuotaHistoryAction: Quota usage history
- QuotaAlertAction: Quota threshold alerts
- QuotaAllocateAction: Allocate quota
"""

import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QuotaStore:
    """In-memory quota storage."""
    
    _quotas: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "limit": 1000,
        "used": 0,
        "window_start": time.time(),
        "history": []
    })
    
    @classmethod
    def check(cls, user: str, amount: int = 1) -> bool:
        quota = cls._quotas[user]
        window = 3600
        if time.time() - quota["window_start"] > window:
            quota["used"] = 0
            quota["window_start"] = time.time()
        return quota["used"] + amount <= quota["limit"]
    
    @classmethod
    def consume(cls, user: str, amount: int = 1) -> bool:
        if cls.check(user, amount):
            cls._quotas[user]["used"] += amount
            cls._quotas[user]["history"].append({
                "amount": amount,
                "timestamp": time.time()
            })
            return True
        return False
    
    @classmethod
    def reset(cls, user: str) -> None:
        cls._quotas[user]["used"] = 0
        cls._quotas[user]["window_start"] = time.time()
    
    @classmethod
    def set_limit(cls, user: str, limit: int) -> None:
        cls._quotas[user]["limit"] = limit
    
    @classmethod
    def get_status(cls, user: str) -> Dict[str, Any]:
        quota = cls._quotas[user]
        window = 3600
        remaining = max(0, quota["limit"] - quota["used"])
        reset_at = quota["window_start"] + window
        return {
            "user": user,
            "limit": quota["limit"],
            "used": quota["used"],
            "remaining": remaining,
            "reset_at": reset_at,
            "window_seconds": window
        }


class QuotaCheckAction(BaseAction):
    """Check quota availability."""
    action_type = "quota_check"
    display_name = "检查配额"
    description = "检查配额可用性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            amount = params.get("amount", 1)
            
            available = QuotaStore.check(user, amount)
            status = QuotaStore.get_status(user)
            
            return ActionResult(
                success=available,
                message=f"Quota {'available' if available else 'exceeded'} for user {user}",
                data={"available": available, "status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota check failed: {str(e)}")


class QuotaConsumeAction(BaseAction):
    """Consume quota."""
    action_type = "quota_consume"
    display_name = "消耗配额"
    description = "消耗配额"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            amount = params.get("amount", 1)
            
            consumed = QuotaStore.consume(user, amount)
            status = QuotaStore.get_status(user)
            
            return ActionResult(
                success=consumed,
                message=f"Quota consumed: {amount}" if consumed else f"Quota exceeded for user {user}",
                data={"consumed": amount, "status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota consume failed: {str(e)}")


class QuotaResetAction(BaseAction):
    """Reset quota."""
    action_type = "quota_reset"
    display_name = "重置配额"
    description = "重置配额"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            
            QuotaStore.reset(user)
            status = QuotaStore.get_status(user)
            
            return ActionResult(
                success=True,
                message=f"Reset quota for user {user}",
                data={"status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota reset failed: {str(e)}")


class QuotaSetAction(BaseAction):
    """Set quota limit."""
    action_type = "quota_set"
    display_name = "设置配额"
    description = "设置配额限制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            limit = params.get("limit", 1000)
            
            if limit < 0:
                return ActionResult(success=False, message="Limit must be non-negative")
            
            QuotaStore.set_limit(user, limit)
            status = QuotaStore.get_status(user)
            
            return ActionResult(
                success=True,
                message=f"Set quota limit to {limit} for user {user}",
                data={"status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota set failed: {str(e)}")


class QuotaStatusAction(BaseAction):
    """Get quota status."""
    action_type = "quota_status"
    display_name = "配额状态"
    description = "获取配额状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            
            status = QuotaStore.get_status(user)
            
            return ActionResult(
                success=True,
                message=f"Quota status for {user}: {status['remaining']}/{status['limit']}",
                data={"status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota status failed: {str(e)}")


class QuotaHistoryAction(BaseAction):
    """Get quota usage history."""
    action_type = "quota_history"
    display_name = "配额历史"
    description = "获取配额使用历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            limit = params.get("limit", 100)
            
            status = QuotaStore.get_status(user)
            history = QuotaStore._quotas[user]["history"][-limit:]
            
            return ActionResult(
                success=True,
                message=f"Quota history for {user}: {len(history)} entries",
                data={"history": history, "count": len(history)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota history failed: {str(e)}")


class QuotaAlertAction(BaseAction):
    """Quota threshold alerts."""
    action_type = "quota_alert"
    display_name = "配额告警"
    description = "配额阈值告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "default")
            threshold_percent = params.get("threshold_percent", 80)
            
            status = QuotaStore.get_status(user)
            usage_percent = (status["used"] / status["limit"] * 100) if status["limit"] > 0 else 0
            
            triggered = usage_percent >= threshold_percent
            
            return ActionResult(
                success=not triggered,
                message=f"Quota alert {'triggered' if triggered else 'not triggered'} for {user}: {usage_percent:.1f}%",
                data={
                    "triggered": triggered,
                    "usage_percent": usage_percent,
                    "threshold_percent": threshold_percent,
                    "status": status
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota alert failed: {str(e)}")


class QuotaAllocateAction(BaseAction):
    """Allocate quota to users."""
    action_type = "quota_allocate"
    display_name = "分配配额"
    description = "分配配额给用户"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            allocations = params.get("allocations", [])
            
            if not allocations:
                return ActionResult(success=False, message="allocations required")
            
            results = []
            for alloc in allocations:
                user = alloc.get("user", "")
                limit = alloc.get("limit", 1000)
                
                if not user:
                    continue
                
                QuotaStore.set_limit(user, limit)
                results.append({"user": user, "limit": limit})
            
            return ActionResult(
                success=True,
                message=f"Allocated quota to {len(results)} users",
                data={"allocations": results, "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quota allocate failed: {str(e)}")
