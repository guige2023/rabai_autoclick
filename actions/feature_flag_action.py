"""Feature Flag Action Module.

Provides feature flag management with targeting rules,
percentage rollouts, and flag toggling.
"""

import time
import random
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlagStatus(Enum):
    """Feature flag status."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    ROLLOUT = "rollout"


@dataclass
class TargetingRule:
    """Targeting rule for flag."""
    attribute: str
    operator: str
    value: Any


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    flag_id: str
    name: str
    description: Optional[str]
    status: FlagStatus
    rollout_percentage: float = 100.0
    targeting_rules: List[TargetingRule] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class FeatureFlagManager:
    """Manages feature flags."""

    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._history: List[Dict] = []

    def create_flag(
        self,
        name: str,
        description: Optional[str] = None,
        status: FlagStatus = FlagStatus.DISABLED,
        rollout_percentage: float = 100.0
    ) -> str:
        """Create a feature flag."""
        flag_id = hashlib.md5(
            f"{name}{time.time()}".encode()
        ).hexdigest()[:8]

        flag = FeatureFlag(
            flag_id=flag_id,
            name=name,
            description=description,
            status=status,
            rollout_percentage=rollout_percentage
        )

        self._flags[flag_id] = flag
        self._log_change(flag_id, "created")

        return flag_id

    def get_flag(self, flag_id: str) -> Optional[FeatureFlag]:
        """Get flag by ID."""
        return self._flags.get(flag_id)

    def is_enabled(
        self,
        flag_id: str,
        user_context: Optional[Dict] = None
    ) -> bool:
        """Check if flag is enabled for user."""
        flag = self._flags.get(flag_id)
        if not flag:
            return False

        if flag.status == FlagStatus.DISABLED:
            return False

        if flag.status == FlagStatus.ENABLED:
            return True

        if flag.status == FlagStatus.ROLLOUT:
            return self._check_rollout(flag, user_context)

        return False

    def _check_rollout(
        self,
        flag: FeatureFlag,
        user_context: Optional[Dict]
    ) -> bool:
        """Check rollout percentage."""
        if user_context:
            user_id = user_context.get("user_id", "")
            hash_input = f"{flag.flag_id}:{user_id}"
            hash_val = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16)
            percentage = (hash_val % 100) + 1
            return percentage <= flag.rollout_percentage
        else:
            return random.random() * 100 <= flag.rollout_percentage

    def update_flag(
        self,
        flag_id: str,
        status: Optional[FlagStatus] = None,
        rollout_percentage: Optional[float] = None
    ) -> bool:
        """Update flag."""
        flag = self._flags.get(flag_id)
        if not flag:
            return False

        if status is not None:
            flag.status = status
        if rollout_percentage is not None:
            flag.rollout_percentage = rollout_percentage

        flag.updated_at = time.time()
        self._log_change(flag_id, "updated")

        return True

    def delete_flag(self, flag_id: str) -> bool:
        """Delete flag."""
        if flag_id in self._flags:
            del self._flags[flag_id]
            self._log_change(flag_id, "deleted")
            return True
        return False

    def _log_change(self, flag_id: str, action: str) -> None:
        """Log flag change."""
        self._history.append({
            "flag_id": flag_id,
            "action": action,
            "timestamp": time.time()
        })

    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get flag change history."""
        return self._history[-limit:]


class FeatureFlagAction(BaseAction):
    """Action for feature flag operations."""

    def __init__(self):
        super().__init__("feature_flag")
        self._manager = FeatureFlagManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute feature flag action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "check":
                return self._check(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "history":
                return self._history(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create a flag."""
        flag_id = self._manager.create_flag(
            name=params.get("name", ""),
            description=params.get("description"),
            status=FlagStatus(params.get("status", "disabled")),
            rollout_percentage=params.get("rollout_percentage", 100)
        )
        return ActionResult(success=True, data={"flag_id": flag_id})

    def _get(self, params: Dict) -> ActionResult:
        """Get flag."""
        flag = self._manager.get_flag(params.get("flag_id", ""))
        if not flag:
            return ActionResult(success=False, message="Flag not found")
        return ActionResult(success=True, data={
            "flag_id": flag.flag_id,
            "name": flag.name,
            "status": flag.status.value,
            "rollout_percentage": flag.rollout_percentage
        })

    def _check(self, params: Dict) -> ActionResult:
        """Check if flag is enabled."""
        enabled = self._manager.is_enabled(
            params.get("flag_id", ""),
            params.get("user_context")
        )
        return ActionResult(success=True, data={"enabled": enabled})

    def _update(self, params: Dict) -> ActionResult:
        """Update flag."""
        status = params.get("status")
        if status:
            status = FlagStatus(status)

        success = self._manager.update_flag(
            params.get("flag_id", ""),
            status=status,
            rollout_percentage=params.get("rollout_percentage")
        )
        return ActionResult(success=success)

    def _delete(self, params: Dict) -> ActionResult:
        """Delete flag."""
        success = self._manager.delete_flag(params.get("flag_id", ""))
        return ActionResult(success=success)

    def _history(self, params: Dict) -> ActionResult:
        """Get change history."""
        history = self._manager.get_history(params.get("limit", 100))
        return ActionResult(success=True, data={"history": history})
