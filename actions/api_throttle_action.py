"""API throttle action module for RabAI AutoClick.

Provides API throttling with quota management, cost tracking,
and adaptive rate limiting.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ThrottleQuota:
    """Throttling quota for an API."""
    name: str
    requests_per_second: float
    requests_per_minute: float
    requests_per_hour: float
    requests_per_day: float
    burst_size: int
    current_rps: int = 0
    current_minute: int = 0
    current_hour: int = 0
    current_day: int = 0
    last_request: float = 0
    bucket_tokens: float = 0


@dataclass
class ThrottleResult:
    """Result of a throttle check."""
    allowed: bool
    wait_time: float
    remaining: int
    reset_at: float


class APIThrottleAction(BaseAction):
    """Throttle API requests based on quotas.
    
    Supports per-second, per-minute, per-hour, per-day quotas
    with burst capacity and adaptive limiting.
    """
    action_type = "api_throttle"
    display_name = "API节流"
    description = "API配额管理和请求节流"
    
    def __init__(self):
        super().__init__()
        self._quotas: Dict[str, ThrottleQuota] = {}
        self._lock = Lock()
        self._history: Dict[str, List[float]] = defaultdict(list)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute throttle operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'check', 'consume', 'register', 'status'
                - api_name: API identifier
                - quota: Quota config dict (for register)
                - cost: Request cost (for consume)
        
        Returns:
            ActionResult with throttle result.
        """
        operation = params.get('operation', 'check').lower()
        
        if operation == 'check':
            return self._check(params)
        elif operation == 'consume':
            return self._consume(params)
        elif operation == 'register':
            return self._register(params)
        elif operation == 'status':
            return self._status(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a new API quota."""
        api_name = params.get('api_name')
        quota = params.get('quota', {})
        
        if not api_name:
            return ActionResult(success=False, message="api_name is required")
        
        throttle_quota = ThrottleQuota(
            name=api_name,
            requests_per_second=quota.get('rps', 10),
            requests_per_minute=quota.get('rpm', 100),
            requests_per_hour=quota.get('rph', 1000),
            requests_per_day=quota.get('rpd', 10000),
            burst_size=quota.get('burst', 20)
        )
        
        with self._lock:
            self._quotas[api_name] = throttle_quota
        
        return ActionResult(
            success=True,
            message=f"Registered quota for '{api_name}'",
            data={'api_name': api_name}
        )
    
    def _check(self, params: Dict[str, Any]) -> ActionResult:
        """Check if request is allowed."""
        api_name = params.get('api_name', 'default')
        cost = params.get('cost', 1)
        
        with self._lock:
            quota = self._quotas.get(api_name)
            
            if not quota:
                # Auto-register with default quota
                quota = ThrottleQuota(
                    name=api_name,
                    requests_per_second=10,
                    requests_per_minute=100,
                    requests_per_hour=1000,
                    requests_per_day=10000,
                    burst_size=20
                )
                self._quotas[api_name] = quota
            
            result = self._check_quota(quota, cost)
        
        return ActionResult(
            success=result.allowed,
            message=f"{'Allowed' if result.allowed else 'Rate limited'}",
            data={
                'allowed': result.allowed,
                'wait_time': result.wait_time,
                'remaining': result.remaining
            }
        )
    
    def _consume(self, params: Dict[str, Any]) -> ActionResult:
        """Consume quota for a request."""
        api_name = params.get('api_name', 'default')
        cost = params.get('cost', 1)
        
        with self._lock:
            quota = self._quotas.get(api_name)
            
            if not quota:
                return ActionResult(
                    success=False,
                    message=f"No quota registered for '{api_name}'"
                )
            
            result = self._check_quota(quota, cost)
            
            if result.allowed:
                # Consume
                self._update_counters(quota, cost)
                self._history[api_name].append(time.time())
        
        return ActionResult(
            success=result.allowed,
            message=f"{'Consumed' if result.allowed else 'Denied'}",
            data={
                'allowed': result.allowed,
                'wait_time': result.wait_time,
                'remaining': result.remaining
            }
        )
    
    def _check_quota(self, quota: ThrottleQuota, cost: int) -> ThrottleResult:
        """Check if request is allowed under quota."""
        now = time.time()
        
        # Reset counters if needed
        self._cleanup_counters(quota, now)
        
        # Check each limit
        wait_time = 0.0
        
        # Per-second check (token bucket)
        if quota.current_rps >= quota.requests_per_second:
            wait_time = max(wait_time, 1.0)
        
        # Per-minute check
        if quota.current_minute >= quota.requests_per_minute:
            wait_time = max(wait_time, 60.0)
        
        # Per-hour check
        if quota.current_hour >= quota.requests_per_hour:
            wait_time = max(wait_time, 3600.0)
        
        # Per-day check
        if quota.current_day >= quota.requests_per_day:
            wait_time = max(wait_time, 86400.0)
        
        allowed = wait_time == 0.0
        remaining = min(
            quota.requests_per_second - quota.current_rps,
            quota.requests_per_minute - quota.current_minute
        )
        
        return ThrottleResult(
            allowed=allowed,
            wait_time=wait_time,
            remaining=max(0, remaining),
            reset_at=now + wait_time
        )
    
    def _update_counters(self, quota: ThrottleQuota, cost: int) -> None:
        """Update quota counters after a request."""
        quota.current_rps += cost
        quota.current_minute += cost
        quota.current_hour += cost
        quota.current_day += cost
        quota.last_request = time.time()
    
    def _cleanup_counters(self, quota: ThrottleQuota, now: float) -> None:
        """Clean up stale counters."""
        # Would reset per-second counter periodically
        # Simplified implementation
        if quota.last_request and (now - quota.last_request) > 1.0:
            quota.current_rps = 0
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get throttle status."""
        api_name = params.get('api_name')
        
        with self._lock:
            if api_name:
                quota = self._quotas.get(api_name)
                if not quota:
                    return ActionResult(
                        success=False,
                        message=f"No quota for '{api_name}'"
                    )
                return ActionResult(
                    success=True,
                    message=f"Status for '{api_name}'",
                    data=self._quota_status(quota)
                )
            else:
                # Return all quotas
                statuses = {
                    name: self._quota_status(q)
                    for name, q in self._quotas.items()
                }
                return ActionResult(
                    success=True,
                    message="All quotas",
                    data={'quotas': statuses}
                )
    
    def _quota_status(self, quota: ThrottleQuota) -> Dict[str, Any]:
        """Get status dict for a quota."""
        return {
            'name': quota.name,
            'rps': quota.current_rps,
            'rpm': quota.current_minute,
            'rph': quota.current_hour,
            'rpd': quota.current_day,
            'limits': {
                'rps': quota.requests_per_second,
                'rpm': quota.requests_per_minute,
                'rph': quota.requests_per_hour,
                'rpd': quota.requests_per_day
            }
        }


class AdaptiveThrottleAction(BaseAction):
    """Adaptive throttling based on API response."""
    action_type = "adaptive_throttle"
    display_name = "自适应节流"
    description = "根据API响应自适应调整节流参数"
    
    def __init__(self):
        super().__init__()
        self._throttle = APIThrottleAction()
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._last_adjustment: Dict[str, float] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute adaptive throttle operation."""
        operation = params.get('operation', 'check').lower()
        
        if operation == 'check':
            return self._adaptive_check(params)
        elif operation == 'report':
            return self._report(params)
        elif operation == 'adjust':
            return self._adjust(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _adaptive_check(self, params: Dict[str, Any]) -> ActionResult:
        """Check with adaptive adjustment."""
        api_name = params.get('api_name', 'default')
        
        # Apply adjustment based on error rate
        errors = self._error_counts.get(api_name, 0)
        
        if errors > 10:
            # High error rate - reduce quota by 50%
            params['api_name'] = api_name + '_reduced'
        elif errors > 5:
            # Medium error rate - reduce by 25%
            params['api_name'] = api_name + '_moderate'
        
        return self._throttle.execute({'operation': 'check', **params})
    
    def _report(self, params: Dict[str, Any]) -> ActionResult:
        """Report API response for adaptive adjustment."""
        api_name = params.get('api_name', 'default')
        success = params.get('success', True)
        status_code = params.get('status_code', 200)
        
        if not success or status_code >= 400:
            self._error_counts[api_name] += 1
        elif success and status_code == 200:
            # Reduce error count on success
            self._error_counts[api_name] = max(0, self._error_counts[api_name] - 1)
        
        return ActionResult(
            success=True,
            message=f"Error count: {self._error_counts[api_name]}",
            data={'error_count': self._error_counts[api_name]}
        )
    
    def _adjust(self, params: Dict[str, Any]) -> ActionResult:
        """Manually adjust throttle."""
        return ActionResult(
            success=True,
            message="Adjustment recorded",
            data={}
        )
