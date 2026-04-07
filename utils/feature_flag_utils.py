"""
Feature Flag Evaluation Engine.

Provides a flexible feature flag system with percentage rollouts,
user targeting, and real-time flag evaluation.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Union


class FlagType(Enum):
    """Types of feature flags."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"


class EvaluationReason(Enum):
    """Reason for flag evaluation result."""
    DEFAULT = "default"
    TARGETING_MATCH = "targeting_match"
    PERCENTAGE_ROLLOUT = "percentage_rollout"
    RULE_MATCH = "rule_match"
    DISABLED = "disabled"
    NOT_FOUND = "not_found"


@dataclass
class UserContext:
    """Context for user targeting evaluation."""
    user_id: str
    email: Optional[str] = None
    country: Optional[str] = None
    platform: Optional[str] = None
    app_version: Optional[str] = None
    created_at: Optional[datetime] = None
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class FlagRule:
    """Rule for targeting specific users."""
    rule_id: str
    attribute: str
    operator: str
    value: Any
    variation: Any


@dataclass
class FlagVariation:
    """A variation/value of a feature flag."""
    variation_id: str
    value: Any
    weight: int = 100


@dataclass
class FeatureFlag:
    """Definition of a feature flag."""
    flag_key: str
    flag_type: FlagType
    enabled: bool
    default_value: Any
    variations: list[FlagVariation] = field(default_factory=list)
    targeting_rules: list[FlagRule] = field(default_factory=list)
    percentage_rollout: float = 100.0
    rollout_attribute: str = "user_id"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    description: str = ""
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluationResult:
    """Result of flag evaluation."""
    flag_key: str
    value: Any
    reason: EvaluationReason
    variation_id: Optional[str] = None
    matched_rule: Optional[str] = None


class FeatureFlagEngine:
    """Engine for evaluating feature flags."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        cache_ttl_seconds: int = 60,
    ) -> None:
        self.db_path = db_path or Path("feature_flags.db")
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[str, tuple[Any, datetime]] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the feature flags database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flags (
                flag_key TEXT PRIMARY KEY,
                flag_type TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                default_value_json TEXT NOT NULL,
                variations_json TEXT NOT NULL DEFAULT '[]',
                targeting_rules_json TEXT NOT NULL DEFAULT '[]',
                percentage_rollout REAL NOT NULL DEFAULT 100.0,
                rollout_attribute TEXT NOT NULL DEFAULT 'user_id',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                description TEXT DEFAULT '',
                tags_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flag_evaluations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flag_key TEXT NOT NULL,
                user_id TEXT NOT NULL,
                evaluated_at TEXT NOT NULL,
                reason TEXT NOT NULL,
                variation_id TEXT,
                matched_rule_id TEXT,
                UNIQUE(flag_key, user_id)
            )
        """)
        conn.commit()
        conn.close()

    def create_flag(
        self,
        flag_key: str,
        flag_type: FlagType,
        default_value: Any,
        enabled: bool = True,
        description: str = "",
        tags: Optional[list[str]] = None,
    ) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            flag_key=flag_key,
            flag_type=flag_type,
            enabled=enabled,
            default_value=default_value,
            description=description,
            tags=tags or [],
        )
        self._save_flag(flag)
        return flag

    def update_flag(
        self,
        flag_key: str,
        **updates: Any,
    ) -> Optional[FeatureFlag]:
        """Update an existing feature flag."""
        flag = self.get_flag(flag_key)
        if not flag:
            return None

        if "enabled" in updates:
            flag.enabled = updates["enabled"]
        if "default_value" in updates:
            flag.default_value = updates["default_value"]
        if "variations" in updates:
            flag.variations = updates["variations"]
        if "targeting_rules" in updates:
            flag.targeting_rules = updates["targeting_rules"]
        if "percentage_rollout" in updates:
            flag.percentage_rollout = updates["percentage_rollout"]
        if "description" in updates:
            flag.description = updates["description"]
        if "tags" in updates:
            flag.tags = updates["tags"]

        flag.updated_at = datetime.now()
        self._save_flag(flag)
        return flag

    def delete_flag(self, flag_key: str) -> bool:
        """Delete a feature flag."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM flags WHERE flag_key = ?", (flag_key,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()

        if flag_key in self._cache:
            del self._cache[flag_key]

        return affected > 0

    def get_flag(self, flag_key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM flags WHERE flag_key = ?", (flag_key,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_flag(row)
        return None

    def list_flags(
        self,
        enabled_only: bool = False,
        tag: Optional[str] = None,
    ) -> list[FeatureFlag]:
        """List all feature flags."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = "SELECT * FROM flags WHERE 1=1"
        params: list[Any] = []

        if enabled_only:
            query += " AND enabled = 1"
        if tag:
            query += " AND tags_json LIKE ?"
            params.append(f"%{tag}%")

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        flags = [self._row_to_flag(row) for row in rows]

        if tag:
            flags = [f for f in flags if tag in f.tags]

        return flags

    def evaluate(
        self,
        flag_key: str,
        context: UserContext,
    ) -> EvaluationResult:
        """Evaluate a feature flag for a given user context."""
        cached = self._get_cached(flag_key)
        if cached is not None:
            return EvaluationResult(
                flag_key=flag_key,
                value=cached,
                reason=EvaluationReason.DEFAULT,
            )

        flag = self.get_flag(flag_key)

        if not flag:
            return EvaluationResult(
                flag_key=flag_key,
                value=None,
                reason=EvaluationReason.NOT_FOUND,
            )

        if not flag.enabled:
            self._cache_result(flag_key, flag.default_value)
            return EvaluationResult(
                flag_key=flag_key,
                value=flag.default_value,
                reason=EvaluationReason.DISABLED,
            )

        for rule in flag.targeting_rules:
            if self._evaluate_rule(rule, context):
                variation = self._get_variation_for_rule(flag, rule)
                self._cache_result(flag_key, variation)
                self._log_evaluation(flag_key, context.user_id, EvaluationReason.RULE_MATCH, matched_rule=rule.rule_id)
                return EvaluationResult(
                    flag_key=flag_key,
                    value=variation,
                    reason=EvaluationReason.RULE_MATCH,
                    matched_rule=rule.rule_id,
                )

        if flag.percentage_rollout < 100.0:
            inRollout = self._evaluate_percentage(flag, context)
            if not inRollout:
                self._cache_result(flag_key, flag.default_value)
                return EvaluationResult(
                    flag_key=flag_key,
                    value=flag.default_value,
                    reason=EvaluationReason.PERCENTAGE_ROLLOUT,
                )

        if flag.variations:
            variation = self._select_variation(flag, context)
            self._cache_result(flag_key, variation.value)
            self._log_evaluation(flag_key, context.user_id, EvaluationReason.TARGETING_MATCH, variation_id=variation.variation_id)
            return EvaluationResult(
                flag_key=flag_key,
                value=variation.value,
                reason=EvaluationReason.TARGETING_MATCH,
                variation_id=variation.variation_id,
            )

        self._cache_result(flag_key, flag.default_value)
        return EvaluationResult(
            flag_key=flag_key,
            value=flag.default_value,
            reason=EvaluationReason.DEFAULT,
        )

    def evaluate_boolean(
        self,
        flag_key: str,
        context: UserContext,
        default: bool = False,
    ) -> bool:
        """Evaluate a boolean feature flag."""
        result = self.evaluate(flag_key, context)
        if result.value is None:
            return default
        return bool(result.value)

    def _evaluate_rule(self, rule: FlagRule, context: UserContext) -> bool:
        """Evaluate if a rule matches the given context."""
        value = self._get_context_attribute(context, rule.attribute)
        if value is None:
            return False

        if rule.operator == "eq":
            return value == rule.value
        elif rule.operator == "neq":
            return value != rule.value
        elif rule.operator == "gt":
            return value > rule.value
        elif rule.operator == "gte":
            return value >= rule.value
        elif rule.operator == "lt":
            return value < rule.value
        elif rule.operator == "lte":
            return value <= rule.value
        elif rule.operator == "contains":
            return rule.value in value
        elif rule.operator == "starts_with":
            return str(value).startswith(str(rule.value))
        elif rule.operator == "ends_with":
            return str(value).endswith(str(rule.value))
        elif rule.operator == "in":
            return value in rule.value
        elif rule.operator == "regex":
            import re
            return bool(re.match(str(rule.value), str(value)))

        return False

    def _get_context_attribute(self, context: UserContext, attribute: str) -> Any:
        """Get an attribute from user context."""
        if hasattr(context, attribute):
            return getattr(context, attribute)
        return context.attributes.get(attribute)

    def _get_variation_for_rule(
        self,
        flag: FeatureFlag,
        rule: FlagRule,
    ) -> Any:
        """Get the variation value for a matching rule."""
        if not flag.variations:
            return flag.default_value
        return flag.variations[0].value

    def _evaluate_percentage(
        self,
        flag: FeatureFlag,
        context: UserContext,
    ) -> bool:
        """Evaluate if user is in the percentage rollout."""
        rollout_value = self._get_context_attribute(context, flag.rollout_attribute)
        if rollout_value is None:
            return False

        hash_input = f"{flag.flag_key}:{rollout_value}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        percentage = (hash_value % 10000) / 100.0

        return percentage < flag.percentage_rollout

    def _select_variation(
        self,
        flag: FeatureFlag,
        context: UserContext,
    ) -> FlagVariation:
        """Select a variation based on weighted distribution."""
        rollout_value = self._get_context_attribute(context, flag.rollout_attribute)
        if rollout_value is None:
            return flag.variations[0]

        hash_input = f"{flag.flag_key}:{flag.rollout_attribute}:{rollout_value}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16) % 10000

        cumulative = 0
        for variation in flag.variations:
            cumulative += variation.weight
            if hash_value < cumulative:
                return variation

        return flag.variations[-1]

    def _cache_result(self, flag_key: str, value: Any) -> None:
        """Cache a flag evaluation result."""
        self._cache[flag_key] = (value, datetime.now())

    def _get_cached(self, flag_key: str) -> Optional[Any]:
        """Get cached result if still valid."""
        if flag_key not in self._cache:
            return None

        value, cached_at = self._cache[flag_key]
        age = (datetime.now() - cached_at).total_seconds()

        if age > self.cache_ttl_seconds:
            del self._cache[flag_key]
            return None

        return value

    def _log_evaluation(
        self,
        flag_key: str,
        user_id: str,
        reason: EvaluationReason,
        variation_id: Optional[str] = None,
        matched_rule: Optional[str] = None,
    ) -> None:
        """Log a flag evaluation for analytics."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO flag_evaluations
            (flag_key, user_id, evaluated_at, reason, variation_id, matched_rule_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            flag_key,
            user_id,
            datetime.now().isoformat(),
            reason.value,
            variation_id,
            matched_rule,
        ))
        conn.commit()
        conn.close()

    def _save_flag(self, flag: FeatureFlag) -> None:
        """Save a flag to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO flags (
                flag_key, flag_type, enabled, default_value_json,
                variations_json, targeting_rules_json, percentage_rollout,
                rollout_attribute, created_at, updated_at, description,
                tags_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            flag.flag_key,
            flag.flag_type.value,
            int(flag.enabled),
            json.dumps(flag.default_value),
            json.dumps([v.__dict__ for v in flag.variations]),
            json.dumps([r.__dict__ for r in flag.targeting_rules]),
            flag.percentage_rollout,
            flag.rollout_attribute,
            flag.created_at.isoformat(),
            flag.updated_at.isoformat(),
            flag.description,
            json.dumps(flag.tags),
            json.dumps(flag.metadata),
        ))
        conn.commit()
        conn.close()

    def _row_to_flag(self, row: sqlite3.Row) -> FeatureFlag:
        """Convert a database row to a FeatureFlag."""
        variations = [
            FlagVariation(
                variation_id=v["variation_id"],
                value=v["value"],
                weight=v.get("weight", 100),
            )
            for v in json.loads(row["variations_json"])
        ]

        rules = [
            FlagRule(
                rule_id=r["rule_id"],
                attribute=r["attribute"],
                operator=r["operator"],
                value=r["value"],
                variation=r.get("variation"),
            )
            for r in json.loads(row["targeting_rules_json"])
        ]

        return FeatureFlag(
            flag_key=row["flag_key"],
            flag_type=FlagType(row["flag_type"]),
            enabled=bool(row["enabled"]),
            default_value=json.loads(row["default_value_json"]),
            variations=variations,
            targeting_rules=rules,
            percentage_rollout=row["percentage_rollout"],
            rollout_attribute=row["rollout_attribute"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            description=row["description"] or "",
            tags=json.loads(row["tags_json"]),
            metadata=json.loads(row["metadata_json"]),
        )
