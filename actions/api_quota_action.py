"""API Quota Action Module. Manages API rate quotas."""
import sys, os, time, threading
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class QuotaLimit:
    requests_per_window: int; window_seconds: int; burst_allowance: int = 0

@dataclass
class QuotaStatus:
    consumer: str; endpoint: str; used: int; limit: int; remaining: int
    resets_at: float; window_seconds: int; over_limit: bool
    retry_after_seconds: Optional[float] = None

class APIQuotaAction(BaseAction):
    action_type = "api_quota"; display_name = "API配额管理"
    description = "管理API配额"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._quotas = {}; self._usage = defaultdict(list)
    def set_quota(self, key: str, limit: QuotaLimit) -> None:
        with self._lock: self._quotas[key] = limit
    def _make_key(self, consumer: str, endpoint: str) -> str: return f"{consumer}:{endpoint}"
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "check")
        consumer = params.get("consumer", "default")
        endpoint = params.get("endpoint", "*")
        key = self._make_key(consumer, endpoint)
        if mode == "set_limit":
            limit = QuotaLimit(requests_per_window=params.get("requests_per_window", 100),
                               window_seconds=params.get("window_seconds", 60),
                               burst_allowance=params.get("burst_allowance", 10))
            self.set_quota(key, limit)
            return ActionResult(success=True, message=f"Quota set for {key}")
        if mode == "reset":
            with self._lock:
                if key in self._usage: del self._usage[key]
            return ActionResult(success=True, message=f"Quota reset for {key}")
        if key not in self._quotas:
            default = QuotaLimit(requests_per_window=100, window_seconds=60)
            self.set_quota(key, default)
        quota = self._quotas[key]
        now = time.time(); cutoff = now - quota.window_seconds
        with self._lock:
            timestamps = [t for t in self._usage[key] if t > cutoff]
            self._usage[key] = timestamps; used = len(timestamps)
        remaining = max(0, quota.requests_per_window - used)
        effective_limit = quota.requests_per_window + quota.burst_allowance
        over_limit = used >= effective_limit
        resets_at = max(timestamps) + quota.window_seconds if timestamps else now + quota.window_seconds
        retry_after = max(0.0, resets_at - now) if over_limit else None
        status = QuotaStatus(consumer=consumer, endpoint=endpoint, used=used, limit=quota.requests_per_window,
                            remaining=remaining, resets_at=resets_at, window_seconds=quota.window_seconds,
                            over_limit=over_limit, retry_after_seconds=retry_after)
        if mode == "check":
            return ActionResult(success=not over_limit, message=f"Quota: {remaining} remaining", data=status)
        if over_limit:
            return ActionResult(success=False, message=f"Quota exceeded. Retry after {retry_after:.1f}s", data=status)
        with self._lock: self._usage[key].append(now)
        return ActionResult(success=True, message=f"Quota consumed: {used+1}/{quota.requests_per_window}", data=status)
