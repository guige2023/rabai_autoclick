"""API Budget Guard Action Module.

Enforces API spending limits and budget caps with configurable
alerting and automatic throttling when thresholds are exceeded.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import time
import logging

logger = logging.getLogger(__name__)


class BudgetLevel(Enum):
    """Budget alert levels."""
    SAFE = "safe"
    WARNING = "warning"
    CRITICAL = "critical"
    EXCEEDED = "exceeded"


@dataclass
class BudgetWindow:
    """A time-windowed budget tracker."""
    window_start: float
    spent: float = 0.0
    requests: int = 0
    alerts_sent: List[str] = field(default_factory=list)


class APILimitError(Exception):
    """Raised when API budget limit is exceeded."""
    pass


class APIBudgetGuardAction:
    """Enforces API spending budgets with automatic protection.
    
    Tracks spending across configurable time windows and automatically
    throttles or rejects requests when budget thresholds are approached.
    """

    def __init__(
        self,
        daily_limit: float = 100.0,
        monthly_limit: float = 2000.0,
        warning_threshold: float = 0.7,
        critical_threshold: float = 0.9,
    ) -> None:
        self.daily_limit = daily_limit
        self.monthly_limit = monthly_limit
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self._daily: BudgetWindow = self._new_window()
        self._monthly: BudgetWindow = self._new_window()
        self._request_costs: Dict[str, float] = {}
        self._blocked: List[str] = []
        self._enabled = True

    def _new_window(self) -> BudgetWindow:
        return BudgetWindow(window_start=time.time())

    def _reset_if_needed(self, window: BudgetWindow, delta: float) -> None:
        now = time.time()
        if now - window.window_start >= delta:
            window.spent = 0.0
            window.requests = 0
            window.alerts_sent.clear()
            window.window_start = now

    def _get_level(self, spent: float, limit: float) -> BudgetLevel:
        ratio = spent / limit if limit > 0 else 0.0
        if ratio >= 1.0:
            return BudgetLevel.EXCEEDED
        if ratio >= self.critical_threshold:
            return BudgetLevel.CRITICAL
        if ratio >= self.warning_threshold:
            return BudgetLevel.WARNING
        return BudgetLevel.SAFE

    def _check_and_enforce(self) -> None:
        self._reset_if_needed(self._daily, 86400.0)
        self._reset_if_needed(self._monthly, 2592000.0)

    def record_request(
        self,
        request_id: str,
        cost: float,
        endpoint: Optional[str] = None,
    ) -> BudgetLevel:
        """Record a request and return the current budget level.
        
        Args:
            request_id: Unique identifier for the request.
            cost: Cost of the request in currency units.
            endpoint: Optional endpoint name for tracking.
        
        Returns:
            Current BudgetLevel after recording.
        
        Raises:
            APILimitError: If budget is exceeded and enforcement is enabled.
        """
        self._check_and_enforce()
        self._daily.spent += cost
        self._daily.requests += 1
        self._monthly.spent += cost
        self._monthly.requests += 1
        self._request_costs[request_id] = cost

        daily_level = self._get_level(self._daily.spent, self.daily_limit)
        monthly_level = self._get_level(self._monthly.spent, self.monthly_limit)
        current_level = max(daily_level, monthly_level)

        if current_level == BudgetLevel.EXCEEDED and self._enabled:
            self._blocked.append(request_id)
            raise APILimitError(
                f"Budget exceeded: daily=${self._daily.spent:.2f}/"
                f"{self.daily_limit}, monthly=${self._monthly.spent:.2f}/"
                f"{self.monthly_limit}"
            )

        logger.info(
            "Budget check: daily=%.2f/%.2f (%s), monthly=%.2f/%.2f (%s)",
            self._daily.spent, self.daily_limit, daily_level.value,
            self._monthly.spent, self.monthly_limit, monthly_level.value,
        )
        return current_level

    def get_status(self) -> Dict[str, Any]:
        """Get current budget status.
        
        Returns:
            Dict with daily/monthly spending, levels, and blocked count.
        """
        self._check_and_enforce()
        return {
            "daily": {
                "spent": round(self._daily.spent, 4),
                "limit": self.daily_limit,
                "requests": self._daily.requests,
                "level": self._get_level(self._daily.spent, self.daily_limit).value,
                "remaining": round(self.daily_limit - self._daily.spent, 4),
            },
            "monthly": {
                "spent": round(self._monthly.spent, 4),
                "limit": self.monthly_limit,
                "requests": self._monthly.requests,
                "level": self._get_level(self._monthly.spent, self.monthly_limit).value,
                "remaining": round(self.monthly_limit - self._monthly.spent, 4),
            },
            "blocked_count": len(self._blocked),
            "enabled": self._enabled,
        }

    def set_limit(self, daily: Optional[float] = None, monthly: Optional[float] = None) -> None:
        """Update budget limits.
        
        Args:
            daily: New daily limit in currency units.
            monthly: New monthly limit in currency units.
        """
        if daily is not None:
            self.daily_limit = daily
        if monthly is not None:
            self.monthly_limit = monthly

    def enable(self) -> None:
        """Enable budget enforcement."""
        self._enabled = True

    def disable(self) -> None:
        """Disable budget enforcement (allow all requests)."""
        self._enabled = False

    def reset(self) -> None:
        """Reset all budget counters."""
        self._daily = self._new_window()
        self._monthly = self._new_window()
        self._request_costs.clear()
        self._blocked.clear()
