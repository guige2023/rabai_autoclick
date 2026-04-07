"""Feature flag (feature toggle) system with targeting rules and evaluation."""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = [
    "FlagStatus",
    "FeatureFlag",
    "TargetingRule",
    "FeatureFlagManager",
]


class FlagStatus(Enum):
    OFF = "off"
    ON = "on"
    CONDITIONAL = "conditional"


@dataclass
class TargetingRule:
    """A targeting rule for a feature flag."""
    name: str
    condition: str
    evaluate: Callable[[dict[str, Any]], bool] = field(default=lambda _: False)
    status: FlagStatus = FlagStatus.ON


@dataclass
class FeatureFlag:
    """A feature flag with status, targeting rules, and rollout config."""
    name: str
    status: FlagStatus = FlagStatus.OFF
    description: str = ""
    rollout_percentage: float = 0.0
    targeting_rules: list[TargetingRule] = field(default_factory=list)
    variants: tuple[str, ...] = ("on", "off")
    default_variant: str = "off"
    metadata: dict[str, Any] = field(default_factory=dict)
    enabled_for_users: set[str] = field(default_factory=set)
    disabled_for_users: set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class FeatureFlagManager:
    """Central feature flag management with evaluation."""

    def __init__(self) -> None:
        self._flags: dict[str, FeatureFlag] = {}
        self._lock = threading.RLock()
        self._evaluation_count = 0
        self._stats: dict[str, int] = {}

    def add_flag(self, flag: FeatureFlag) -> None:
        with self._lock:
            self._flags[flag.name] = flag

    def get_flag(self, name: str) -> FeatureFlag | None:
        return self._flags.get(name)

    def set_status(self, name: str, status: FlagStatus) -> None:
        with self._lock:
            flag = self._flags.get(name)
            if flag:
                flag.status = status
                flag.updated_at = time.time()

    def is_enabled(
        self,
        flag_name: str,
        context: dict[str, Any] | None = None,
    ) -> bool:
        """Evaluate a feature flag for a given context."""
        with self._lock:
            self._evaluation_count += 1
            self._stats[flag_name] = self._stats.get(flag_name, 0) + 1

            flag = self._flags.get(flag_name)
            if not flag:
                return False

            context = context or {}

            user_id = context.get("user_id") or context.get("userId") or ""

            if user_id in flag.disabled_for_users:
                return False
            if user_id in flag.enabled_for_users:
                return True

            if flag.status == FlagStatus.ON:
                if flag.targeting_rules:
                    for rule in flag.targeting_rules:
                        try:
                            if rule.evaluate(context):
                                return rule.status == FlagStatus.ON
                        except Exception:
                            pass

                if flag.rollout_percentage > 0:
                    return self._in_rollout(flag_name, user_id, flag.rollout_percentage)

                return True

            return False

    def get_variant(
        self,
        flag_name: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Return the variant value for a flag."""
        flag = self._flags.get(flag_name)
        if not flag:
            return flag.default_variant if flag else "off"

        if self.is_enabled(flag_name, context):
            if flag.variants:
                return flag.variants[-1]
            return "on"
        return flag.default_variant

    def _in_rollout(self, flag_name: str, user_id: str, percentage: float) -> bool:
        if not user_id or percentage >= 100.0:
            return True
        if percentage <= 0.0:
            return False
        hash_input = f"{flag_name}:{user_id}"
        hash_val = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        bucket = (hash_val % 10000) / 100.0
        return bucket < percentage

    def enable_for_user(self, flag_name: str, user_id: str) -> None:
        with self._lock:
            flag = self._flags.get(flag_name)
            if flag:
                flag.enabled_for_users.add(user_id)

    def disable_for_user(self, flag_name: str, user_id: str) -> None:
        with self._lock:
            flag = self._flags.get(flag_name)
            if flag:
                flag.disabled_for_users.add(user_id)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "total_flags": len(self._flags),
                "total_evaluations": self._evaluation_count,
                "per_flag": dict(self._stats),
            }
