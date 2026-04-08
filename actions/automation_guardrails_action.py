"""Automation guardrails action module for RabAI AutoClick.

Provides guardrails for automation:
- GuardrailCheckAction: Check guardrail constraints
- GuardrailLimitAction: Set execution limits
- GuardrailTimeoutAction: Enforce timeout
- GuardrailRateAction: Enforce rate limits
- GuardrailApproveAction: Manual approval gate
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GuardrailCheckAction(BaseAction):
    """Check guardrail constraints."""
    action_type = "guardrail_check"
    display_name = "护栏检查"
    description = "检查护栏约束条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            constraints = params.get("constraints", {})

            if not operation:
                return ActionResult(success=False, message="operation is required")

            violations = []
            max_cost = constraints.get("max_cost", float("inf"))
            max_duration = constraints.get("max_duration", float("inf"))
            requires_approval = constraints.get("requires_approval", False)

            if requires_approval:
                violations.append("Approval required")

            passed = len(violations) == 0

            return ActionResult(
                success=passed,
                data={"operation": operation, "passed": passed, "violations": violations},
                message=f"Guardrail check: {'PASSED' if passed else f'{len(violations)} violations'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Guardrail check failed: {e}")


class GuardrailLimitAction(BaseAction):
    """Set execution limits."""
    action_type = "guardrail_limit"
    display_name = "护栏限制"
    description = "设置执行限制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            resource = params.get("resource", "")
            limit = params.get("limit", 0)
            window = params.get("window", 60)

            if not resource:
                return ActionResult(success=False, message="resource is required")

            if not hasattr(context, "guardrails"):
                context.guardrails = {}
            context.guardrails[resource] = {"limit": limit, "window": window, "set_at": time.time()}

            return ActionResult(
                success=True,
                data={"resource": resource, "limit": limit, "window": window},
                message=f"Limit set: {resource} = {limit} per {window}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Guardrail limit failed: {e}")


class GuardrailTimeoutAction(BaseAction):
    """Enforce operation timeout."""
    action_type = "guardrail_timeout"
    display_name = "护栏超时"
    description = "强制操作超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            timeout = params.get("timeout", 30)

            if not operation:
                return ActionResult(success=False, message="operation is required")

            if not hasattr(context, "timeouts"):
                context.timeouts = {}
            context.timeouts[operation] = {"timeout": timeout, "set_at": time.time()}

            return ActionResult(
                success=True,
                data={"operation": operation, "timeout_s": timeout},
                message=f"Timeout set: {operation} = {timeout}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Guardrail timeout failed: {e}")


class GuardrailRateAction(BaseAction):
    """Enforce rate limits."""
    action_type = "guardrail_rate"
    display_name = "护栏速率"
    description = "强制速率限制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            rate = params.get("rate", 10)
            per = params.get("per", 60)

            if not operation:
                return ActionResult(success=False, message="operation is required")

            if not hasattr(context, "rate_limits"):
                context.rate_limits = {}
            context.rate_limits[operation] = {"rate": rate, "per": per}

            return ActionResult(
                success=True,
                data={"operation": operation, "rate": rate, "per": per},
                message=f"Rate limit set: {operation} = {rate}/{per}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Guardrail rate failed: {e}")


class GuardrailApproveAction(BaseAction):
    """Manual approval gate."""
    action_type = "guardrail_approve"
    display_name = "护栏审批"
    description = "手动审批门控"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            approver = params.get("approver", "")
            approved = params.get("approved", False)
            reason = params.get("reason", "")

            if not operation:
                return ActionResult(success=False, message="operation is required")

            approval_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "approvals"):
                context.approvals = {}
            context.approvals[approval_id] = {
                "approval_id": approval_id,
                "operation": operation,
                "approver": approver,
                "approved": approved,
                "reason": reason,
                "timestamp": time.time(),
            }

            return ActionResult(
                success=approved,
                data={"approval_id": approval_id, "operation": operation, "approved": approved},
                message=f"Approval: {operation} - {'APPROVED' if approved else 'PENDING'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Guardrail approve failed: {e}")
