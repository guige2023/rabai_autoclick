"""
Input Cooldown Manager Utilities

Manage cooldowns between repeated input actions to prevent
over-clicking, respect rate limits, and space out operations.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class CooldownInfo:
    """Current cooldown state for an action key."""
    action_key: str
    cooldown_end_ms: float
    cooldown_duration_ms: float
    last_executed_ms: float = 0.0
    execution_count: int = 0


class InputCooldownManager:
    """
    Manage cooldowns between repeated input actions.

    Prevents flooding the input pipeline with too many events
    in a short period.
    """

    def __init__(
        self,
        default_cooldown_ms: float = 100.0,
    ):
        self.default_cooldown_ms = default_cooldown_ms
        self._cooldowns: Dict[str, CooldownInfo] = {}
        self._custom_cooldowns: Dict[str, float] = {}

    def set_cooldown(self, action_key: str, duration_ms: float) -> None:
        """Set a custom cooldown duration for an action key."""
        self._custom_cooldowns[action_key] = duration_ms

    def can_execute(self, action_key: str, current_time_ms: Optional[float] = None) -> bool:
        """
        Check if an action can be executed now (cooldown has elapsed).

        Args:
            action_key: The action identifier.
            current_time_ms: Current time (defaults to now).

        Returns:
            True if the cooldown has elapsed and the action can proceed.
        """
        now = current_time_ms or time.time() * 1000
        info = self._cooldowns.get(action_key)

        if info is None:
            return True

        return now >= info.cooldown_end_ms

    def record_execution(
        self,
        action_key: str,
        current_time_ms: Optional[float] = None,
    ) -> CooldownInfo:
        """
        Record that an action was executed, starting its cooldown.

        Args:
            action_key: The action identifier.
            current_time_ms: Current time (defaults to now).

        Returns:
            CooldownInfo for the action.
        """
        now = current_time_ms or time.time() * 1000
        cooldown_duration = self._custom_cooldowns.get(
            action_key, self.default_cooldown_ms
        )

        info = self._cooldowns.get(action_key)
        if info:
            info.cooldown_end_ms = now + cooldown_duration
            info.last_executed_ms = now
            info.cooldown_duration_ms = cooldown_duration
            info.execution_count += 1
        else:
            info = CooldownInfo(
                action_key=action_key,
                cooldown_end_ms=now + cooldown_duration,
                cooldown_duration_ms=cooldown_duration,
                last_executed_ms=now,
                execution_count=1,
            )
            self._cooldowns[action_key] = info

        return info

    def get_remaining_cooldown_ms(
        self,
        action_key: str,
        current_time_ms: Optional[float] = None,
    ) -> float:
        """Get the remaining cooldown time for an action in milliseconds."""
        now = current_time_ms or time.time() * 1000
        info = self._cooldowns.get(action_key)
        if not info:
            return 0.0
        return max(0.0, info.cooldown_end_ms - now)

    def get_cooldown_info(self, action_key: str) -> Optional[CooldownInfo]:
        """Get full cooldown info for an action."""
        return self._cooldowns.get(action_key)

    def reset_cooldown(self, action_key: str) -> bool:
        """Reset (remove) the cooldown for an action."""
        return self._cooldowns.pop(action_key, None) is not None

    def reset_all(self) -> None:
        """Reset all cooldowns."""
        self._cooldowns.clear()

    def get_all_in_cooldown(
        self,
        current_time_ms: Optional[float] = None,
    ) -> Dict[str, float]:
        """Get all actions currently in cooldown with their remaining time."""
        now = current_time_ms or time.time() * 1000
        result = {}
        for key, info in self._cooldowns.items():
            remaining = info.cooldown_end_ms - now
            if remaining > 0:
                result[key] = remaining
        return result
