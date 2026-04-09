"""
Automation Failure Tracker Module.

Provides failure tracking, root cause analysis patterns,
and alerting for automation workflows.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
import logging
import hashlib

logger = logging.getLogger(__name__)


class FailureSeverity(Enum):
    """Failure severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class Failure:
    """Container for a failure event."""
    failure_id: str
    workflow_id: str
    step_id: Optional[str]
    error_type: str
    error_message: str
    severity: FailureSeverity
    timestamp: float
    context: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None
    retryable: bool = False
    resolved: bool = False
    resolved_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FailurePattern:
    """Pattern of recurring failures."""
    pattern_id: str
    error_signature: str
    count: int = 0
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    affected_workflows: Set[str] = field(default_factory=set)
    affected_steps: Set[str] = field(default_factory=set)
    resolution: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FailureStats:
    """Failure statistics."""
    total_failures: int = 0
    critical_failures: int = 0
    resolved_failures: int = 0
    unique_patterns: int = 0
    mean_time_to_recover: float = 0.0
    failure_rate: float = 0.0


class FailureTracker:
    """
    Tracks and analyzes failures in automation workflows.
    
    Example:
        tracker = FailureTracker()
        
        # Record a failure
        failure = await tracker.record_failure(
            workflow_id="wf_123",
            error=ValueError("Invalid input"),
            severity=FailureSeverity.HIGH
        )
        
        # Get failure patterns
        patterns = tracker.get_failure_patterns(threshold=5)
    """
    
    def __init__(
        self,
        max_failures: int = 10000,
        pattern_threshold: int = 3,
    ) -> None:
        """
        Initialize failure tracker.
        
        Args:
            max_failures: Maximum failures to keep in memory.
            pattern_threshold: Failures before a pattern is identified.
        """
        self.max_failures = max_failures
        self.pattern_threshold = pattern_threshold
        self._failures: deque = deque(maxlen=max_failures)
        self._patterns: Dict[str, FailurePattern] = {}
        self._workflow_failures: Dict[str, List[str]] = defaultdict(list)
        self._error_types: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._stats = FailureStats()
        
    async def record_failure(
        self,
        workflow_id: str,
        step_id: Optional[str],
        error: Exception,
        severity: FailureSeverity = FailureSeverity.MEDIUM,
        context: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None,
    ) -> Failure:
        """
        Record a failure event.
        
        Args:
            workflow_id: Workflow identifier.
            step_id: Step identifier where failure occurred.
            error: The exception that occurred.
            severity: Failure severity.
            context: Optional failure context.
            stack_trace: Optional stack trace.
            
        Returns:
            Recorded Failure object.
        """
        failure_id = self._generate_id(workflow_id, step_id, str(error))
        
        failure = Failure(
            failure_id=failure_id,
            workflow_id=workflow_id,
            step_id=step_id,
            error_type=type(error).__name__,
            error_message=str(error),
            severity=severity,
            timestamp=time.time(),
            context=context or {},
            stack_trace=stack_trace,
            retryable=self._is_retryable(error),
        )
        
        async with self._lock:
            self._failures.append(failure)
            self._workflow_failures[workflow_id].append(failure_id)
            self._error_types[failure.error_type] += 1
            
            # Update patterns
            await self._update_pattern(failure)
            
            # Update stats
            self._stats.total_failures += 1
            if severity == FailureSeverity.CRITICAL:
                self._stats.critical_failures += 1
                
        logger.warning(
            f"Failure recorded: {workflow_id}/{step_id} - "
            f"{failure.error_type}: {failure.error_message}"
        )
        
        return failure
        
    async def resolve_failure(
        self,
        failure_id: str,
        resolution: Optional[str] = None,
    ) -> bool:
        """
        Mark a failure as resolved.
        
        Args:
            failure_id: Failure to resolve.
            resolution: Optional resolution description.
            
        Returns:
            True if resolved.
        """
        async with self._lock:
            for failure in self._failures:
                if failure.failure_id == failure_id:
                    failure.resolved = True
                    failure.resolved_at = time.time()
                    if resolution:
                        failure.metadata["resolution"] = resolution
                    self._stats.resolved_failures += 1
                    return True
        return False
        
    async def get_failure(
        self,
        failure_id: str,
    ) -> Optional[Failure]:
        """Get failure by ID."""
        for failure in self._failures:
            if failure.failure_id == failure_id:
                return failure
        return None
        
    async def get_workflow_failures(
        self,
        workflow_id: str,
        limit: int = 100,
        unresolved_only: bool = False,
    ) -> List[Failure]:
        """
        Get failures for a workflow.
        
        Args:
            workflow_id: Workflow identifier.
            limit: Maximum failures to return.
            unresolved_only: Only return unresolved failures.
            
        Returns:
            List of failures.
        """
        failure_ids = self._workflow_failures.get(workflow_id, [])
        failures = []
        
        for fid in reversed(failure_ids):
            for failure in self._failures:
                if failure.failure_id == fid:
                    if unresolved_only and failure.resolved:
                        continue
                    failures.append(failure)
                    if len(failures) >= limit:
                        break
                    break
                    
        return failures
        
    def get_failure_patterns(
        self,
        threshold: Optional[int] = None,
        min_severity: FailureSeverity = FailureSeverity.LOW,
    ) -> List[FailurePattern]:
        """
        Get failure patterns.
        
        Args:
            threshold: Minimum count for a pattern.
            min_severity: Minimum severity level.
            
        Returns:
            List of FailurePattern objects.
        """
        threshold = threshold or self.pattern_threshold
        
        patterns = [
            p for p in self._patterns.values()
            if p.count >= threshold
        ]
        
        return sorted(patterns, key=lambda p: p.count, reverse=True)
        
    async def _update_pattern(self, failure: Failure) -> None:
        """Update failure pattern tracking."""
        signature = self._get_error_signature(failure)
        
        if signature not in self._patterns:
            self._patterns[signature] = FailurePattern(
                pattern_id=signature,
                error_signature=signature,
                count=0,
                first_seen=failure.timestamp,
            )
            
        pattern = self._patterns[signature]
        pattern.count += 1
        pattern.last_seen = failure.timestamp
        pattern.affected_workflows.add(failure.workflow_id)
        if failure.step_id:
            pattern.affected_steps.add(failure.step_id)
            
    def _get_error_signature(self, failure: Failure) -> str:
        """Generate error signature for pattern matching."""
        # Normalize error message
        msg = failure.error_message.lower()
        msg = "".join(c if c.isalnum() else "_" for c in msg)
        
        # Create signature
        sig_data = f"{failure.error_type}:{msg[:50]}"
        return hashlib.md5(sig_data.encode()).hexdigest()[:16]
        
    def _generate_id(
        self,
        workflow_id: str,
        step_id: Optional[str],
        error: str,
    ) -> str:
        """Generate unique failure ID."""
        data = f"{workflow_id}:{step_id}:{error}:{time.time()}"
        return f"fail_{hashlib.md5(data.encode()).hexdigest()[:12]}"
        
    def _is_retryable(self, error: Exception) -> bool:
        """Determine if error is retryable."""
        retryable_types = (
            TimeoutError,
            ConnectionError,
            asyncio.TimeoutError,
        )
        return isinstance(error, retryable_types)
        
    def get_stats(self) -> FailureStats:
        """Get failure statistics."""
        stats = FailureStats(
            total_failures=self._stats.total_failures,
            critical_failures=self._stats.critical_failures,
            resolved_failures=self._stats.resolved_failures,
            unique_patterns=len(self._patterns),
        )
        
        # Calculate MTTR
        resolved = [f for f in self._failures if f.resolved and f.resolved_at]
        if resolved:
            total_time = sum(f.resolved_at - f.timestamp for f in resolved)
            stats.mean_time_to_recover = total_time / len(resolved) if resolved else 0
            
        return stats
        
    def get_error_distribution(self) -> Dict[str, int]:
        """Get distribution of error types."""
        return dict(self._error_types)


class FailureAlertHandler:
    """
    Handles failure alerts and notifications.
    
    Example:
        handler = FailureAlertHandler()
        
        handler.add_rule(
            name="critical_alert",
            condition=lambda f: f.severity == FailureSeverity.CRITICAL,
            action=send_slack_message
        )
    """
    
    def __init__(self) -> None:
        """Initialize alert handler."""
        self._rules: List[Dict[str, Any]] = []
        self._alert_history: deque = deque(maxlen=1000)
        
    def add_rule(
        self,
        name: str,
        condition: Callable[[Failure], bool],
        action: Callable[[Failure], Any],
        cooldown: float = 300.0,
    ) -> None:
        """
        Add an alert rule.
        
        Args:
            name: Rule name.
            condition: Function that returns True when alert should fire.
            action: Function to execute when alert fires.
            cooldown: Cooldown period in seconds.
        """
        self._rules.append({
            "name": name,
            "condition": condition,
            "action": action,
            "cooldown": cooldown,
            "last_fired": 0,
        })
        
    async def handle_failure(self, failure: Failure) -> None:
        """Handle a failure and trigger any matching alerts."""
        for rule in self._rules:
            try:
                if rule["condition"](failure):
                    # Check cooldown
                    if time.time() - rule["last_fired"] < rule["cooldown"]:
                        continue
                        
                    # Fire alert
                    await rule["action"](failure)
                    rule["last_fired"] = time.time()
                    
                    self._alert_history.append({
                        "rule": rule["name"],
                        "failure_id": failure.failure_id,
                        "timestamp": time.time(),
                    })
                    
                    logger.info(f"Alert fired: {rule['name']} for {failure.failure_id}")
                    
            except Exception as e:
                logger.error(f"Alert rule {rule['name']} failed: {e}")
                
    def get_alert_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get alert history."""
        return list(self._alert_history)[-limit:]
