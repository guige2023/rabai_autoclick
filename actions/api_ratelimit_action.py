"""API rate limiting action module for RabAI AutoClick.

Provides rate limiting operations:
- RateLimitCreateAction: Create rate limit rule
- RateLimitCheckAction: Check rate limit
- RateLimitConsumeAction: Consume rate limit quota
- RateLimitResetAction: Reset rate limit
- RateLimitStatusAction: Get rate limit status
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RateLimitCreateAction(BaseAction):
    """Create a rate limit rule."""
    action_type = "ratelimit_create"
    display_name = "创建限速规则"
    description = "创建限速规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            limit = params.get("limit", 100)
            window = params.get("window", 60)
            scope = params.get("scope", "global")

            if not name:
                return ActionResult(success=False, message="name is required")

            rule_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "rate_limits"):
                context.rate_limits = {}
            context.rate_limits[rule_id] = {
                "rule_id": rule_id,
                "name": name,
                "limit": limit,
                "window": window,
                "scope": scope,
                "used": 0,
                "window_start": time.time(),
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"rule_id": rule_id, "name": name, "limit": limit, "window": window},
                message=f"Rate limit rule {rule_id}: {limit}/{window}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit create failed: {e}")


class RateLimitCheckAction(BaseAction):
    """Check if rate limit allows request."""
    action_type = "ratelimit_check"
    display_name = "检查限速"
    description = "检查限速是否允许请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            rule_id = params.get("rule_id", "")
            if not rule_id:
                return ActionResult(success=False, message="rule_id is required")

            rate_limits = getattr(context, "rate_limits", {})
            if rule_id not in rate_limits:
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            rule = rate_limits[rule_id]
            now = time.time()
            elapsed = now - rule["window_start"]

            if elapsed >= rule["window"]:
                rule["used"] = 0
                rule["window_start"] = now

            allowed = rule["used"] < rule["limit"]
            remaining = max(0, rule["limit"] - rule["used"])
            reset_in = max(0, rule["window"] - elapsed)

            return ActionResult(
                success=allowed,
                data={"rule_id": rule_id, "allowed": allowed, "remaining": remaining, "reset_in_s": reset_in},
                message=f"Rate limit {'allowed' if allowed else 'exceeded'}: {remaining} remaining",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit check failed: {e}")


class RateLimitConsumeAction(BaseAction):
    """Consume rate limit quota."""
    action_type = "ratelimit_consume"
    display_name = "消耗限速配额"
    description = "消耗限速配额"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            rule_id = params.get("rule_id", "")
            amount = params.get("amount", 1)

            if not rule_id:
                return ActionResult(success=False, message="rule_id is required")

            rate_limits = getattr(context, "rate_limits", {})
            if rule_id not in rate_limits:
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            rule = rate_limits[rule_id]
            now = time.time()
            if now - rule["window_start"] >= rule["window"]:
                rule["used"] = 0
                rule["window_start"] = now

            if rule["used"] + amount > rule["limit"]:
                return ActionResult(success=False, message="Rate limit exceeded")

            rule["used"] += amount

            return ActionResult(
                success=True,
                data={"rule_id": rule_id, "consumed": amount, "remaining": rule["limit"] - rule["used"]},
                message=f"Consumed {amount}, {rule['limit'] - rule['used']} remaining",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit consume failed: {e}")


class RateLimitResetAction(BaseAction):
    """Reset rate limit."""
    action_type = "ratelimit_reset"
    display_name = "重置限速"
    description = "重置限速计数器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            rule_id = params.get("rule_id", "")
            if not rule_id:
                return ActionResult(success=False, message="rule_id is required")

            rate_limits = getattr(context, "rate_limits", {})
            if rule_id not in rate_limits:
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            rule = rate_limits[rule_id]
            rule["used"] = 0
            rule["window_start"] = time.time()

            return ActionResult(success=True, data={"rule_id": rule_id}, message=f"Rate limit {rule_id} reset")
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit reset failed: {e}")


class RateLimitStatusAction(BaseAction):
    """Get rate limit status."""
    action_type = "ratelimit_status"
    display_name = "限速状态"
    description = "获取限速状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            rule_id = params.get("rule_id", "")
            if not rule_id:
                return ActionResult(success=False, message="rule_id is required")

            rate_limits = getattr(context, "rate_limits", {})
            if rule_id not in rate_limits:
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            rule = rate_limits[rule_id]
            return ActionResult(
                success=True,
                data={"rule_id": rule_id, "name": rule["name"], "limit": rule["limit"], "used": rule["used"], "window": rule["window"]},
                message=f"Rate limit {rule_id}: {rule['used']}/{rule['limit']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit status failed: {e}")
