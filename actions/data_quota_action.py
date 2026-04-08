"""
Data Quota Action Module.

Quota management for rate limiting and resource allocation,
supports sliding windows, fixed windows, and token buckets.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class QuotaType(Enum):
    """Quota allocation types."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


@dataclass
class QuotaStatus:
    """Current quota status."""
    available: int
    used: int
    limit: int
    reset_at: float
    percent_used: float


class DataQuotaAction:
    """
    Quota management for rate limiting and resource control.

    Supports fixed window, sliding window, and token bucket
    quota strategies.

    Example:
        quota = DataQuotaAction(limit=100, window=60)
        if quota.allow():
            process_request()
    """

    def __init__(
        self,
        limit: int = 100,
        window: float = 60.0,
        quota_type: QuotaType = QuotaType.FIXED_WINDOW,
        initial_tokens: Optional[float] = None,
    ) -> None:
        self.limit = limit
        self.window = window
        self.quota_type = quota_type
        self._reset_at = time.time() + window
        self._used = 0
        self._tokens = initial_tokens if initial_tokens is not None else float(limit)
        self._refill_rate = limit / window if window > 0 else 0.0

    def allow(self, cost: int = 1) -> bool:
        """Check if operation is allowed under quota."""
        self._prune()

        if self.quota_type == QuotaType.TOKEN_BUCKET:
            return self._allow_token_bucket(cost)

        if self._used + cost > self.limit:
            return False

        self._used += cost
        return True

    def _allow_token_bucket(self, cost: float) -> bool:
        """Token bucket quota check."""
        now = time.time()
        elapsed = now - (self._reset_at - self.window)

        self._tokens = min(
            self.limit,
            self._tokens + elapsed * self._refill_rate
        )
        self._reset_at = now + self.window

        if self._tokens >= cost:
            self._tokens -= cost
            return True

        return False

    def _prune(self) -> None:
        """Reset quota if window has passed."""
        now = time.time()

        if now >= self._reset_at:
            self._used = 0
            self._reset_at = now + self.window

    def get_status(self) -> QuotaStatus:
        """Get current quota status."""
        self._prune()

        available = max(0, self.limit - self._used)

        if self.quota_type == QuotaType.TOKEN_BUCKET:
            available = max(0, int(self._tokens))

        return QuotaStatus(
            available=available,
            used=self._used,
            limit=self.limit,
            reset_at=self._reset_at,
            percent_used=(self._used / self.limit * 100) if self.limit > 0 else 0.0,
        )

    def reset(self) -> None:
        """Reset quota to initial state."""
        self._used = 0
        self._reset_at = time.time() + self.window
        self._tokens = float(self.limit)

    def set_limit(self, new_limit: int) -> None:
        """Update quota limit."""
        self.limit = new_limit
        self._refill_rate = new_limit / self.window if self.window > 0 else 0.0

    def consume(self, amount: int = 1) -> bool:
        """Force consume quota without checking."""
        self._prune()

        if self._used + amount > self.limit:
            return False

        self._used += amount
        return True

    def refund(self, amount: int = 1) -> None:
        """Refund previously consumed quota."""
        self._used = max(0, self._used - amount)

    @property
    def remaining(self) -> int:
        """Remaining quota in current window."""
        return self.get_status().available
