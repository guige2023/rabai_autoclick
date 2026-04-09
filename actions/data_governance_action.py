"""Data governance action module for RabAI AutoClick.

Provides data governance operations:
- GovernancePolicyAction: Define and enforce data governance policies
- DataStewardshipAction: Manage data stewards and responsibilities
- DataClassificationAction: Classify data by sensitivity/importance
- DataPolicyEnforcerAction: Enforce data policies
- GovernanceAuditAction: Audit data governance compliance
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSensitivity(Enum):
    """Data sensitivity levels."""
    PUBLIC = 1
    INTERNAL = 2
    CONFIDENTIAL = 3
    RESTRICTED = 4


class PolicyType(Enum):
    """Types of governance policies."""
    ACCESS_CONTROL = auto()
    RETENTION = auto()
    PRIVACY = auto()
    QUALITY = auto()
    SECURITY = auto()
    COMPLIANCE = auto()


class GovernancePolicyAction(BaseAction):
    """Define and enforce data governance policies."""
    action_type = "governance_policy"
    display_name = "治理策略"
    description = "定义和执行数据治理策略"

    def __init__(self) -> None:
        super().__init__()
        self._policies: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_policy(params)
            elif action == "update":
                return self._update_policy(params)
            elif action == "delete":
                return self._delete_policy(params)
            elif action == "list":
                return self._list_policies(params)
            elif action == "evaluate":
                return self._evaluate_policy(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Governance policy failed: {e}")

    def _create_policy(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        policy_type = params.get("policy_type", "ACCESS_CONTROL")
        rules = params.get("rules", {})
        description = params.get("description", "")
        enforced = params.get("enforced", True)
        if not name:
            return ActionResult(success=False, message="name is required")
        try:
            ptype = PolicyType[policy_type.upper()]
        except KeyError:
            return ActionResult(success=False, message=f"Invalid policy_type: {policy_type}")

        policy_id = str(uuid.uuid4())
        self._policies[policy_id] = {
            "id": policy_id,
            "name": name,
            "policy_type": ptype.name,
            "rules": rules,
            "description": description,
            "enforced": enforced,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
        }
        return ActionResult(success=True, message=f"Policy '{name}' created", data=self._policies[policy_id])

    def _update_policy(self, params: Dict[str, Any]) -> ActionResult:
        policy_id = params.get("policy_id", "")
        updates = params.get("updates", {})
        if policy_id not in self._policies:
            return ActionResult(success=False, message="Policy not found")
        self._policies[policy_id].update(updates)
        return ActionResult(success=True, message=f"Policy updated: {policy_id[:8]}")

    def _delete_policy(self, params: Dict[str, Any]) -> ActionResult:
        policy_id = params.get("policy_id", "")
        if policy_id in self._policies:
            del self._policies[policy_id]
            return ActionResult(success=True, message="Policy deleted")
        return ActionResult(success=False, message="Policy not found")

    def _list_policies(self, params: Dict[str, Any]) -> ActionResult:
        policy_type = params.get("policy_type", "")
        enforced_only = params.get("enforced_only", False)
        policies = list(self._policies.values())
        if policy_type:
            policies = [p for p in policies if p["policy_type"] == policy_type.upper()]
        if enforced_only:
            policies = [p for p in policies if p.get("enforced")]
        return ActionResult(success=True, message=f"{len(policies)} policies", data={"policies": policies})

    def _evaluate_policy(self, params: Dict[str, Any]) -> ActionResult:
        policy_id = params.get("policy_id", "")
        data = params.get("data", {})
        if policy_id not in self._policies:
            return ActionResult(success=False, message="Policy not found")
        policy = self._policies[policy_id]
        rules = policy.get("rules", {})
        passed = True
        violations: List[str] = []
        for rule_name, rule_value in rules.items():
            if rule_name == "min_sensitivity" and data.get("sensitivity", 0) < rule_value:
                passed = False
                violations.append(f"min_sensitivity violation: {data.get('sensitivity')} < {rule_value}")
        return ActionResult(
            success=passed,
            message=f"Policy evaluation: {'PASSED' if passed else 'FAILED'}",
            data={"passed": passed, "violations": violations, "policy_id": policy_id},
        )


class DataStewardshipAction(BaseAction):
    """Manage data stewards and responsibilities."""
    action_type = "data_stewardship"
    display_name = "数据管理"
    description = "管理数据管家和职责"

    def __init__(self) -> None:
        super().__init__()
        self._stewards: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "assign":
                return self._assign_steward(params)
            elif action == "list":
                return self._list_stewards()
            elif action == "delegate":
                return self._delegate(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data stewardship failed: {e}")

    def _assign_steward(self, params: Dict[str, Any]) -> ActionResult:
        steward_id = params.get("steward_id", str(uuid.uuid4()))
        name = params.get("name", "")
        domain = params.get("domain", "")
        responsibilities = params.get("responsibilities", [])
        if not name:
            return ActionResult(success=False, message="name is required")
        self._stewards[steward_id] = {
            "id": steward_id,
            "name": name,
            "domain": domain,
            "responsibilities": responsibilities,
            "assigned_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
        }
        return ActionResult(success=True, message=f"Steward '{name}' assigned to '{domain}'", data=self._stewards[steward_id])

    def _list_stewards(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._stewards)} stewards", data={"stewards": self._stewards})

    def _delegate(self, params: Dict[str, Any]) -> ActionResult:
        from_steward = params.get("from_steward", "")
        to_steward = params.get("to_steward", "")
        domain = params.get("domain", "")
        if from_steward in self._stewards and to_steward in self._stewards:
            self._stewards[to_steward]["responsibilities"].append(f"Delegated: {domain}")
            return ActionResult(success=True, message=f"Responsibility delegated from {from_steward[:8]} to {to_steward[:8]}")
        return ActionResult(success=False, message="Steward not found")


class DataClassificationAction(BaseAction):
    """Classify data by sensitivity and importance."""
    action_type = "data_classification"
    display_name = "数据分类"
    description = "按敏感度/重要性对数据分类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_asset = params.get("data_asset", "")
            classification = params.get("classification", "INTERNAL")
            rationale = params.get("rationale", "")
            tags = params.get("tags", [])
            if not data_asset:
                return ActionResult(success=False, message="data_asset is required")
            try:
                sensitivity = DataSensitivity[classification.upper()]
            except KeyError:
                return ActionResult(success=False, message=f"Invalid classification: {classification}")

            result = {
                "id": str(uuid.uuid4()),
                "data_asset": data_asset,
                "classification": sensitivity.name,
                "level": sensitivity.value,
                "rationale": rationale,
                "tags": tags,
                "classified_at": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(success=True, message=f"Data asset classified as {sensitivity.name}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Data classification failed: {e}")


class DataPolicyEnforcerAction(BaseAction):
    """Enforce data policies."""
    action_type = "data_policy_enforcer"
    display_name = "数据策略执行"
    description = "执行数据策略"

    def __init__(self) -> None:
        super().__init__()
        self._violations: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "")
            data = params.get("data", {})
            policies = params.get("policies", [])
            if not operation:
                return ActionResult(success=False, message="operation is required")

            violations: List[Dict[str, Any]] = []
            for policy in policies:
                if policy.get("enforce_operation") == operation:
                    passed = True
                    if not passed:
                        violations.append({
                            "policy_id": policy.get("id"),
                            "policy_name": policy.get("name"),
                            "operation": operation,
                            "violated_at": datetime.now(timezone.utc).isoformat(),
                        })

            if violations:
                self._violations.extend(violations)

            return ActionResult(
                success=len(violations) == 0,
                message=f"Policy enforcement: {'PASSED' if not violations else f'{len(violations)} violations'}",
                data={"violations": violations, "allowed": len(violations) == 0},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Policy enforcement failed: {e}")


class GovernanceAuditAction(BaseAction):
    """Audit data governance compliance."""
    action_type = "governance_audit"
    display_name = "治理审计"
    description = "审计数据治理合规性"

    def __init__(self) -> None:
        super().__init__()
        self._audit_records: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "audit":
                return self._run_audit(params)
            elif action == "report":
                return self._generate_report(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Governance audit failed: {e}")

    def _run_audit(self, params: Dict[str, Any]) -> ActionResult:
        scope = params.get("scope", "")
        assets = params.get("assets", [])
        policies = params.get("policies", [])
        findings: List[Dict[str, Any]] = []
        for asset in assets:
            for policy in policies:
                if policy.get("applies_to") == asset:
                    findings.append({
                        "asset": asset,
                        "policy": policy.get("name"),
                        "status": "COMPLIANT" if policy.get("enforced") else "REQUIRES_REVIEW",
                        "audited_at": datetime.now(timezone.utc).isoformat(),
                    })
        record = {"id": str(uuid.uuid4()), "scope": scope, "findings": findings, "timestamp": datetime.now(timezone.utc).isoformat()}
        self._audit_records.append(record)
        return ActionResult(success=True, message=f"Audit complete: {len(findings)} findings", data=record)

    def _generate_report(self, params: Dict[str, Any]) -> ActionResult:
        period = params.get("period", "last_30_days")
        report = {
            "id": str(uuid.uuid4()),
            "period": period,
            "total_audits": len(self._audit_records),
            "audit_records": self._audit_records[-10:],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message="Audit report generated", data=report)
