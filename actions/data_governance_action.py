"""Data Governance Action Module.

Provides data governance framework with policies,
classification, and compliance tracking.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataClassification(Enum):
    """Data classification levels."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class GovernancePolicyType(Enum):
    """Type of governance policy."""
    RETENTION = "retention"
    ACCESS = "access"
    PRIVACY = "privacy"
    QUALITY = "quality"
    SECURITY = "security"


class ComplianceStatus(Enum):
    """Compliance status."""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    PENDING = "pending"
    NOT_APPLICABLE = "not_applicable"


@dataclass
class DataAsset:
    """A data asset."""
    asset_id: str
    name: str
    asset_type: str
    classification: DataClassification
    owner: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class GovernancePolicy:
    """A governance policy."""
    policy_id: str
    name: str
    policy_type: GovernancePolicyType
    description: Optional[str] = None
    rules: Dict[str, Any] = field(default_factory=dict)
    classification_levels: List[DataClassification] = field(default_factory=list)
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class ComplianceViolation:
    """A compliance violation."""
    violation_id: str
    asset_id: str
    policy_id: str
    severity: str
    description: str
    detected_at: float = field(default_factory=time.time)
    resolved: bool = False
    resolved_at: Optional[float] = None


class DataGovernanceManager:
    """Manages data governance and compliance."""

    def __init__(self):
        self._assets: Dict[str, DataAsset] = {}
        self._policies: Dict[str, GovernancePolicy] = {}
        self._violations: List[ComplianceViolation] = []
        self._compliance_history: List[Dict[str, Any]] = []

    def register_asset(
        self,
        name: str,
        asset_type: str,
        classification: DataClassification,
        owner: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> str:
        """Register a data asset."""
        asset_id = hashlib.md5(
            f"{name}{time.time()}".encode()
        ).hexdigest()[:8]

        asset = DataAsset(
            asset_id=asset_id,
            name=name,
            asset_type=asset_type,
            classification=classification,
            owner=owner,
            description=description,
            tags=tags or []
        )

        self._assets[asset_id] = asset
        return asset_id

    def update_asset(
        self,
        asset_id: str,
        classification: Optional[DataClassification] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Update a data asset."""
        if asset_id not in self._assets:
            return False

        asset = self._assets[asset_id]

        if classification is not None:
            asset.classification = classification
        if owner is not None:
            asset.owner = owner
        if tags is not None:
            asset.tags = tags

        asset.updated_at = time.time()
        return True

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get asset by ID."""
        return self._assets.get(asset_id)

    def get_assets_by_classification(
        self,
        classification: DataClassification
    ) -> List[DataAsset]:
        """Get assets by classification level."""
        return [
            a for a in self._assets.values()
            if a.classification == classification
        ]

    def create_policy(
        self,
        name: str,
        policy_type: GovernancePolicyType,
        rules: Dict[str, Any],
        description: Optional[str] = None,
        classification_levels: Optional[List[DataClassification]] = None
    ) -> str:
        """Create a governance policy."""
        policy_id = hashlib.md5(
            f"{name}{time.time()}".encode()
        ).hexdigest()[:8]

        policy = GovernancePolicy(
            policy_id=policy_id,
            name=name,
            policy_type=policy_type,
            description=description,
            rules=rules,
            classification_levels=classification_levels or []
        )

        self._policies[policy_id] = policy
        return policy_id

    def enable_policy(
        self,
        policy_id: str,
        enabled: bool = True
    ) -> bool:
        """Enable or disable a policy."""
        if policy_id not in self._policies:
            return False

        self._policies[policy_id].enabled = enabled
        return True

    def check_compliance(
        self,
        asset_id: str
    ) -> Dict[str, Any]:
        """Check compliance for an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return {"compliant": False, "violations": ["Asset not found"]}

        violations = []
        enabled_policies = [
            p for p in self._policies.values()
            if p.enabled
        ]

        for policy in enabled_policies:
            if policy.classification_levels and asset.classification not in policy.classification_levels:
                continue

            policy_violations = self._check_policy_compliance(
                asset, policy
            )
            violations.extend(policy_violations)

        status = ComplianceStatus.COMPLIANT if not violations else ComplianceStatus.NON_COMPLIANT

        self._compliance_history.append({
            "asset_id": asset_id,
            "timestamp": time.time(),
            "status": status.value,
            "violation_count": len(violations)
        })

        return {
            "asset_id": asset_id,
            "compliant": status == ComplianceStatus.COMPLIANT,
            "violations": violations,
            "checked_policies": len(enabled_policies)
        }

    def _check_policy_compliance(
        self,
        asset: DataAsset,
        policy: GovernancePolicy
    ) -> List[Dict[str, Any]]:
        """Check compliance against a specific policy."""
        violations = []

        if policy.policy_type == GovernancePolicyType.RETENTION:
            max_age_days = policy.rules.get("max_age_days")
            if max_age_days:
                age_days = (time.time() - asset.created_at) / 86400
                if age_days > max_age_days:
                    violations.append({
                        "policy_id": policy.policy_id,
                        "policy_name": policy.name,
                        "type": "retention",
                        "message": f"Asset exceeds max age: {age_days:.0f} > {max_age_days} days"
                    })

        elif policy.policy_type == GovernancePolicyType.ACCESS:
            required_tags = policy.rules.get("required_tags", [])
            for tag in required_tags:
                if tag not in asset.tags:
                    violations.append({
                        "policy_id": policy.policy_id,
                        "policy_name": policy.name,
                        "type": "access",
                        "message": f"Missing required tag: {tag}"
                    })

        elif policy.policy_type == GovernancePolicyType.PRIVACY:
            restricted_class = DataClassification.RESTRICTED
            if asset.classification == restricted_class:
                if not asset.owner:
                    violations.append({
                        "policy_id": policy.policy_id,
                        "policy_name": policy.name,
                        "type": "privacy",
                        "message": "Restricted data must have an owner"
                    })

        return violations

    def get_violations(
        self,
        asset_id: Optional[str] = None,
        unresolved_only: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get compliance violations."""
        violations = self._violations

        if asset_id:
            violations = [v for v in violations if v.asset_id == asset_id]

        if unresolved_only:
            violations = [v for v in violations if not v.resolved]

        violations = violations[-limit:]

        return [
            {
                "violation_id": v.violation_id,
                "asset_id": v.asset_id,
                "policy_id": v.policy_id,
                "severity": v.severity,
                "description": v.description,
                "detected_at": v.detected_at,
                "resolved": v.resolved
            }
            for v in violations
        ]

    def resolve_violation(
        self,
        violation_id: str
    ) -> bool:
        """Mark a violation as resolved."""
        for violation in self._violations:
            if violation.violation_id == violation_id:
                violation.resolved = True
                violation.resolved_at = time.time()
                return True
        return False

    def get_governance_report(
        self,
        classification: Optional[DataClassification] = None
    ) -> Dict[str, Any]:
        """Generate governance report."""
        assets = list(self._assets.values())

        if classification:
            assets = [a for a in assets if a.classification == classification]

        total = len(assets)
        by_classification = {}

        for c in DataClassification:
            by_classification[c.value] = sum(
                1 for a in assets if a.classification == c
            )

        enabled_policies = sum(1 for p in self._policies.values() if p.enabled)

        return {
            "total_assets": total,
            "by_classification": by_classification,
            "total_policies": len(self._policies),
            "enabled_policies": enabled_policies,
            "unresolved_violations": sum(
                1 for v in self._violations if not v.resolved
            )
        }


class DataGovernanceAction(BaseAction):
    """Action for data governance operations."""

    def __init__(self):
        super().__init__("data_governance")
        self._manager = DataGovernanceManager()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute data governance action."""
        try:
            operation = params.get("operation", "register_asset")

            if operation == "register_asset":
                return self._register_asset(params)
            elif operation == "update_asset":
                return self._update_asset(params)
            elif operation == "get_asset":
                return self._get_asset(params)
            elif operation == "create_policy":
                return self._create_policy(params)
            elif operation == "enable_policy":
                return self._enable_policy(params)
            elif operation == "check_compliance":
                return self._check_compliance(params)
            elif operation == "get_violations":
                return self._get_violations(params)
            elif operation == "resolve_violation":
                return self._resolve_violation(params)
            elif operation == "report":
                return self._get_report(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register_asset(self, params: Dict[str, Any]) -> ActionResult:
        """Register a data asset."""
        name = params.get("name")
        asset_type = params.get("asset_type", "dataset")
        classification = DataClassification(params.get("classification", "internal"))

        if not name:
            return ActionResult(success=False, message="name required")

        asset_id = self._manager.register_asset(
            name=name,
            asset_type=asset_type,
            classification=classification,
            owner=params.get("owner"),
            description=params.get("description"),
            tags=params.get("tags")
        )

        return ActionResult(
            success=True,
            data={"asset_id": asset_id}
        )

    def _update_asset(self, params: Dict[str, Any]) -> ActionResult:
        """Update a data asset."""
        asset_id = params.get("asset_id")

        if not asset_id:
            return ActionResult(success=False, message="asset_id required")

        classification = params.get("classification")
        if classification:
            classification = DataClassification(classification)

        success = self._manager.update_asset(
            asset_id=asset_id,
            classification=classification,
            owner=params.get("owner"),
            tags=params.get("tags")
        )

        return ActionResult(
            success=success,
            message="Asset updated" if success else "Asset not found"
        )

    def _get_asset(self, params: Dict[str, Any]) -> ActionResult:
        """Get asset details."""
        asset_id = params.get("asset_id")

        if not asset_id:
            assets = list(self._manager._assets.values())
            return ActionResult(
                success=True,
                data={
                    "assets": [
                        {
                            "asset_id": a.asset_id,
                            "name": a.name,
                            "classification": a.classification.value
                        }
                        for a in assets
                    ]
                }
            )

        asset = self._manager.get_asset(asset_id)
        if not asset:
            return ActionResult(success=False, message="Asset not found")

        return ActionResult(
            success=True,
            data={
                "asset_id": asset.asset_id,
                "name": asset.name,
                "asset_type": asset.asset_type,
                "classification": asset.classification.value,
                "owner": asset.owner,
                "tags": asset.tags
            }
        )

    def _create_policy(self, params: Dict[str, Any]) -> ActionResult:
        """Create a governance policy."""
        name = params.get("name")
        policy_type = GovernancePolicyType(params.get("policy_type", "retention"))

        if not name:
            return ActionResult(success=False, message="name required")

        policy_id = self._manager.create_policy(
            name=name,
            policy_type=policy_type,
            rules=params.get("rules", {}),
            description=params.get("description"),
            classification_levels=[
                DataClassification(c) for c in params.get("classification_levels", [])
            ]
        )

        return ActionResult(
            success=True,
            data={"policy_id": policy_id}
        )

    def _enable_policy(self, params: Dict[str, Any]) -> ActionResult:
        """Enable or disable a policy."""
        policy_id = params.get("policy_id")
        enabled = params.get("enabled", True)

        if not policy_id:
            return ActionResult(success=False, message="policy_id required")

        success = self._manager.enable_policy(policy_id, enabled)

        return ActionResult(
            success=success,
            message="Policy updated" if success else "Policy not found"
        )

    def _check_compliance(self, params: Dict[str, Any]) -> ActionResult:
        """Check compliance for an asset."""
        asset_id = params.get("asset_id")

        if not asset_id:
            return ActionResult(success=False, message="asset_id required")

        result = self._manager.check_compliance(asset_id)

        return ActionResult(
            success=result["compliant"],
            data=result
        )

    def _get_violations(self, params: Dict[str, Any]) -> ActionResult:
        """Get compliance violations."""
        violations = self._manager.get_violations(
            asset_id=params.get("asset_id"),
            unresolved_only=params.get("unresolved_only", False),
            limit=params.get("limit", 100)
        )

        return ActionResult(success=True, data={"violations": violations})

    def _resolve_violation(self, params: Dict[str, Any]) -> ActionResult:
        """Resolve a violation."""
        violation_id = params.get("violation_id")

        if not violation_id:
            return ActionResult(
                success=False,
                message="violation_id required"
            )

        success = self._manager.resolve_violation(violation_id)

        return ActionResult(
            success=success,
            message="Violation resolved" if success else "Violation not found"
        )

    def _get_report(self, params: Dict[str, Any]) -> ActionResult:
        """Get governance report."""
        classification = params.get("classification")

        if classification:
            classification = DataClassification(classification)

        report = self._manager.get_governance_report(classification)

        return ActionResult(success=True, data=report)
