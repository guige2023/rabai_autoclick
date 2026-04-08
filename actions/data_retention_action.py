"""
Data Retention Action Module.

Manages data retention policies including lifecycle management, archival,
deletion scheduling, and compliance-aware retention rules.

Author: RabAi Team
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class RetentionAction(Enum):
    """Actions to take on expired data."""
    DELETE = "delete"
    ARCHIVE = "archive"
    ANONYMIZE = "anonymize"
    WARN = "warn"
    TAG = "tag"


class DataCategory(Enum):
    """Data classification categories."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"
    SPI = "spi"
    PHI = "phi"


@dataclass
class RetentionRule:
    """A retention rule for specific data."""
    id: str
    name: str
    data_type: str
    category: DataCategory = DataCategory.INTERNAL
    retention_days: int = 365
    action: RetentionAction = RetentionAction.DELETE
    archive_destination: Optional[str] = None
    archive_format: str = "json"
    compression: bool = True
    encryption: bool = False
    tags: Set[str] = field(default_factory=set)
    conditions: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataAsset:
    """Represents a data asset with retention metadata."""
    id: str
    name: str
    data_type: str
    category: DataCategory
    created_at: datetime
    last_accessed: datetime = field(default_factory=datetime.now)
    size_bytes: int = 0
    retention_days: Optional[int] = None
    tags: Set[str] = field(default_factory=set)
    archived: bool = False
    deleted: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def age_days(self) -> int:
        return (datetime.now() - self.created_at).days

    @property
    def days_until_expiry(self) -> Optional[int]:
        if self.retention_days is None:
            return None
        return self.retention_days - self.age_days

    @property
    def is_expired(self) -> bool:
        if self.deleted or self.archived:
            return False
        if self.retention_days is None:
            return False
        return self.age_days >= self.retention_days

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "data_type": self.data_type,
            "category": self.category.value,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "size_bytes": self.size_bytes,
            "retention_days": self.retention_days,
            "age_days": self.age_days,
            "days_until_expiry": self.days_until_expiry,
            "is_expired": self.is_expired,
            "archived": self.archived,
            "deleted": self.deleted,
            "tags": list(self.tags),
            "metadata": self.metadata,
        }


@dataclass
class RetentionReport:
    """Report on retention policy execution."""
    executed_at: datetime
    total_assets: int
    expired_assets: int
    deleted_count: int
    archived_count: int
    anonymized_count: int
    warning_count: int
    errors: List[str] = field(default_factory=list)
    freed_bytes: int = 0
    archived_bytes: int = 0
    details: List[Dict[str, Any]] = field(default_factory=list)


class RetentionPolicy:
    """A named retention policy grouping multiple rules."""

    def __init__(self, name: str, description: str = ""):
        self.id = f"policy_{name}_{datetime.now().timestamp()}"
        self.name = name
        self.description = description
        self.rules: Dict[str, RetentionRule] = {}
        self.enabled: bool = True
        self.created_at = datetime.now()

    def add_rule(self, rule: RetentionRule) -> None:
        """Add a retention rule to this policy."""
        self.rules[rule.id] = rule

    def get_rule(self, data_type: str) -> Optional[RetentionRule]:
        """Get rule for a specific data type."""
        for rule in self.rules.values():
            if rule.data_type == data_type:
                return rule
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "enabled": self.enabled,
            "rules": [r.__dict__ for r in self.rules.values()],
            "created_at": self.created_at.isoformat(),
        }


class DataRetentionManager:
    """
    Data retention policy management and enforcement engine.

    Manages retention policies, evaluates data assets against rules,
    and executes retention actions (delete, archive, anonymize).

    Example:
        >>> manager = DataRetentionManager()
        >>> manager.create_rule("logs", retention_days=90, action=RetentionAction.ARCHIVE)
        >>> manager.create_rule("pii_backups", retention_days=30, action=RetentionAction.DELETE)
        >>> report = manager.evaluate_and_execute([asset1, asset2])
    """

    def __init__(
        self,
        archive_fn: Optional[Callable] = None,
        delete_fn: Optional[Callable] = None,
        notify_fn: Optional[Callable] = None,
    ):
        self.archive_fn = archive_fn
        self.delete_fn = delete_fn
        self.notify_fn = notify_fn
        self.policies: Dict[str, RetentionPolicy] = {}
        self.rules: Dict[str, RetentionRule] = {}
        self._default_retention_days = 365

    def create_rule(
        self,
        name: str,
        data_type: str,
        retention_days: int = 365,
        action: str = "delete",
        category: str = "internal",
        **kwargs,
    ) -> str:
        """Create a new retention rule."""
        import uuid
        rule_id = str(uuid.uuid4())
        rule = RetentionRule(
            id=rule_id,
            name=name,
            data_type=data_type,
            category=DataCategory(category),
            retention_days=retention_days,
            action=RetentionAction(action),
            **kwargs,
        )
        self.rules[rule_id] = rule
        return rule_id

    def create_policy(
        self,
        name: str,
        description: str = "",
        rules: Optional[List[str]] = None,
    ) -> str:
        """Create a retention policy with associated rules."""
        policy = RetentionPolicy(name=name, description=description)
        if rules:
            for rule_id in rules:
                if rule_id in self.rules:
                    policy.add_rule(self.rules[rule_id])
        self.policies[policy.id] = policy
        return policy.id

    def register_rule(self, rule: RetentionRule) -> None:
        """Register a retention rule directly."""
        self.rules[rule.id] = rule

    def evaluate_asset(self, asset: DataAsset) -> Tuple[Optional[RetentionRule], bool]:
        """
        Evaluate an asset against retention rules.

        Returns:
            Tuple of (matched_rule, should_expire)
        """
        for rule in self.rules.values():
            if rule.data_type == asset.data_type:
                if rule.category == asset.category or rule.category == DataCategory.INTERNAL:
                    should_expire = asset.age_days >= rule.retention_days
                    return rule, should_expire

        # Default rule based on category
        default_days = {
            DataCategory.PUBLIC: 180,
            DataCategory.INTERNAL: 365,
            DataCategory.CONFIDENTIAL: 730,
            DataCategory.RESTRICTED: 1825,
            DataCategory.PII: 365,
            DataCategory.SPI: 365,
            DataCategory.PHI: 2555,  # 7 years for healthcare
        }
        default_days_actual = default_days.get(asset.category, self._default_retention_days)
        should_expire = asset.age_days >= default_days_actual
        return None, should_expire

    def evaluate_and_execute(
        self,
        assets: List[DataAsset],
        dry_run: bool = False,
    ) -> RetentionReport:
        """Evaluate assets and execute retention actions."""
        now = datetime.now()
        report = RetentionReport(
            executed_at=now,
            total_assets=len(assets),
            expired_assets=0,
            deleted_count=0,
            archived_count=0,
            anonymized_count=0,
            warning_count=0,
        )

        for asset in assets:
            rule, should_expire = self.evaluate_asset(asset)
            action = rule.action if rule else RetentionAction.DELETE

            if should_expire:
                report.expired_assets += 1
                try:
                    if action == RetentionAction.DELETE:
                        if not dry_run:
                            self._execute_delete(asset)
                        report.deleted_count += 1
                        report.freed_bytes += asset.size_bytes

                    elif action == RetentionAction.ARCHIVE:
                        if not dry_run:
                            self._execute_archive(asset, rule)
                        report.archived_count += 1
                        report.archived_bytes += asset.size_bytes

                    elif action == RetentionAction.ANONYMIZE:
                        if not dry_run:
                            self._execute_anonymize(asset)
                        report.anonymized_count += 1

                    elif action == RetentionAction.WARN:
                        report.warning_count += 1
                        if not dry_run:
                            self._send_warning(asset)

                    report.details.append({
                        "asset_id": asset.id,
                        "action": action.value,
                        "status": "executed",
                        "rule_name": rule.name if rule else "default",
                    })

                except Exception as e:
                    report.errors.append(f"Asset {asset.id}: {str(e)}")
                    report.details.append({
                        "asset_id": asset.id,
                        "action": action.value,
                        "status": "error",
                        "error": str(e),
                    })

        return report

    def get_expiring_assets(
        self,
        assets: List[DataAsset],
        within_days: int = 30,
    ) -> List[DataAsset]:
        """Get assets expiring within specified days."""
        expiring = []
        for asset in assets:
            rule, should_expire = self.evaluate_asset(asset)
            days_left = asset.days_until_expiry
            if days_left is not None and 0 <= days_left <= within_days and not should_expire:
                expiring.append(asset)
        return expiring

    def get_expired_assets(self, assets: List[DataAsset]) -> List[DataAsset]:
        """Get all expired assets."""
        return [a for a in assets if a.is_expired]

    def get_retention_summary(self, assets: List[DataAsset]) -> Dict[str, Any]:
        """Get summary statistics of retention status."""
        expired = self.get_expired_assets(assets)
        expiring_30 = self.get_expiring_assets(assets, within_days=30)
        expiring_90 = self.get_expiring_assets(assets, within_days=90)

        by_category: Dict[str, int] = defaultdict(int)
        for asset in assets:
            by_category[asset.category.value] += 1

        return {
            "total_assets": len(assets),
            "expired": len(expired),
            "expiring_30_days": len(expiring_30),
            "expiring_90_days": len(expiring_90),
            "by_category": dict(by_category),
            "total_size_bytes": sum(a.size_bytes for a in assets),
            "expired_size_bytes": sum(a.size_bytes for a in expired),
        }

    def _execute_delete(self, asset: DataAsset) -> None:
        """Execute deletion of an asset."""
        if self.delete_fn:
            self.delete_fn(asset)
        asset.deleted = True

    def _execute_archive(
        self,
        asset: DataAsset,
        rule: Optional[RetentionRule],
    ) -> None:
        """Execute archival of an asset."""
        if self.archive_fn:
            dest = rule.archive_destination if rule else None
            self.archive_fn(asset, destination=dest, format=rule.archive_format if rule else "json")
        asset.archived = True

    def _execute_anonymize(self, asset: DataAsset) -> None:
        """Execute anonymization of an asset."""
        # Placeholder - implement based on data type
        asset.metadata["anonymized"] = True
        asset.metadata["anonymized_at"] = datetime.now().isoformat()

    def _send_warning(self, asset: DataAsset) -> None:
        """Send warning about impending expiration."""
        if self.notify_fn:
            self.notify_fn(asset)

    def import_rules_from_json(self, json_str: str) -> int:
        """Import retention rules from JSON."""
        data = json.loads(json_str)
        count = 0
        for rule_data in data.get("rules", []):
            category_str = rule_data.get("category", "internal")
            action_str = rule_data.get("action", "delete")
            rule_data["category"] = category_str
            rule_data["action"] = action_str
            import uuid
            rule = RetentionRule(
                id=str(uuid.uuid4()),
                name=rule_data["name"],
                data_type=rule_data["data_type"],
                retention_days=rule_data.get("retention_days", 365),
                action=RetentionAction(rule_data.get("action", "delete")),
                category=DataCategory(rule_data.get("category", "internal")),
            )
            self.rules[rule.id] = rule
            count += 1
        return count


def create_retention_manager(
    retention_rules: Optional[List[Dict[str, Any]]] = None,
) -> DataRetentionManager:
    """Factory to create a configured retention manager."""
    manager = DataRetentionManager()
    if retention_rules:
        for rule_def in retention_rules:
            manager.create_rule(**rule_def)
    return manager
