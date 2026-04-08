"""Feature Flag V2 Action Module.

Provides feature flags with targeting
rules and percentage rollouts.
"""

import time
import random
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TargetingRule:
    """Targeting rule."""
    attribute: str
    operator: str
    value: Any


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    flag_id: str
    name: str
    enabled: bool
    rollout_percentage: float = 100.0
    targeting_rules: List[TargetingRule] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)


class FeatureFlagManager:
    """Manages feature flags."""

    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}

    def create_flag(
        self,
        name: str,
        enabled: bool = False,
        rollout_percentage: float = 100.0
    ) -> str:
        """Create feature flag."""
        flag_id = f"flag_{name.lower().replace(' ', '_')}"

        self._flags[flag_id] = FeatureFlag(
            flag_id=flag_id,
            name=name,
            enabled=enabled,
            rollout_percentage=rollout_percentage
        )

        return flag_id

    def is_enabled(
        self,
        flag_id: str,
        context: Optional[Dict] = None
    ) -> bool:
        """Check if flag is enabled."""
        flag = self._flags.get(flag_id)
        if not flag:
            return False

        if not flag.enabled:
            return False

        if flag.targeting_rules:
            if not context:
                return False

            for rule in flag.targeting_rules:
                if not self._evaluate_rule(rule, context):
                    return False

        if flag.rollout_percentage < 100.0:
            user_id = context.get("user_id", "") if context else ""
            if not self._is_in_rollout(flag_id, user_id, flag.rollout_percentage):
                return False

        return True

    def _evaluate_rule(self, rule: TargetingRule, context: Dict) -> bool:
        """Evaluate targeting rule."""
        value = context.get(rule.attribute)

        if rule.operator == "equals":
            return value == rule.value
        elif rule.operator == "not_equals":
            return value != rule.value
        elif rule.operator == "contains":
            return rule.value in str(value)
        elif rule.operator == "in":
            return value in rule.value

        return False

    def _is_in_rollout(
        self,
        flag_id: str,
        user_id: str,
        percentage: float
    ) -> bool:
        """Check if user is in rollout percentage."""
        hash_val = hash(f"{flag_id}:{user_id}")
        bucket = (hash_val % 100) + 1
        return bucket <= percentage

    def update_flag(
        self,
        flag_id: str,
        enabled: Optional[bool] = None,
        rollout_percentage: Optional[float] = None
    ) -> bool:
        """Update flag."""
        flag = self._flags.get(flag_id)
        if not flag:
            return False

        if enabled is not None:
            flag.enabled = enabled

        if rollout_percentage is not None:
            flag.rollout_percentage = rollout_percentage

        return True


class FeatureFlagV2Action(BaseAction):
    """Action for feature flag operations."""

    def __init__(self):
        super().__init__("feature_flag_v2")
        self._manager = FeatureFlagManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute feature flag action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "is_enabled":
                return self._is_enabled(params)
            elif operation == "update":
                return self._update(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create flag."""
        flag_id = self._manager.create_flag(
            name=params.get("name", ""),
            enabled=params.get("enabled", False),
            rollout_percentage=params.get("rollout_percentage", 100)
        )
        return ActionResult(success=True, data={"flag_id": flag_id})

    def _is_enabled(self, params: Dict) -> ActionResult:
        """Check if enabled."""
        enabled = self._manager.is_enabled(
            params.get("flag_id", ""),
            params.get("context")
        )
        return ActionResult(success=True, data={"enabled": enabled})

    def _update(self, params: Dict) -> ActionResult:
        """Update flag."""
        success = self._manager.update_flag(
            flag_id=params.get("flag_id", ""),
            enabled=params.get("enabled"),
            rollout_percentage=params.get("rollout_percentage")
        )
        return ActionResult(success=success)
