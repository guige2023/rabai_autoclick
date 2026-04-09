"""Automation Sentinel Action Module.

Monitors automation workflows for anomalies, health issues, and
performance degradation. Provides alerting and automatic remediation.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


@dataclass
class HealthCheck:
    """Health check definition."""
    name: str
    check_fn: Callable[[], Awaitable[bool]]
    interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    consecutive_failures: int = 0
    failure_threshold: int = 3


@dataclass
class Alert:
    """Represents an alert condition."""
    id: str
    name: str
    severity: AlertSeverity
    message: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False


@dataclass
class SentinelMetrics:
    """Sentinel performance metrics."""
    health_checks_run: int = 0
    health_checks_passed: int = 0
    health_checks_failed: int = 0
    alerts_triggered: int = 0
    alerts_resolved: int = 0
    alerts_acknowledged: int = 0


class AutomationSentinel:
    """Monitors automation workflows for health and performance issues.
    
    Features:
    - Configurable health checks
    - Anomaly detection
    - Alert management
    - Automatic remediation triggers
    """
    
    def __init__(
        self,
        name: str,
        check_interval: float = 30.0,
        retention_seconds: int = 3600
    ):
        self.name = name
        self.check_interval = check_interval
        self.retention_seconds = retention_seconds
        
        self._health_checks: Dict[str, HealthCheck] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: deque = deque(maxlen=1000)
        self._remediation_handlers: Dict[str, List[Callable]] = {}
        self._check_tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        self._metrics = SentinelMetrics()
        self._lock = asyncio.Lock()
    
    def register_health_check(
        self,
        name: str,
        check_fn: Callable[[], Awaitable[bool]],
        interval_seconds: float = 60.0,
        timeout_seconds: float = 10.0,
        failure_threshold: int = 3
    ) -> None:
        """Register a health check.
        
        Args:
            name: Check name
            check_fn: Async function returning True if healthy
            interval_seconds: Check interval
            timeout_seconds: Check timeout
            failure_threshold: Failures before alert
        """
        self._health_checks[name] = HealthCheck(
            name=name,
            check_fn=check_fn,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            failure_threshold=failure_threshold
        )
        logger.info(f"Registered health check: {name}")
    
    def register_remediation(
        self,
        alert_name: str,
        handler: Callable[[Alert], Awaitable[None]]
    ) -> None:
        """Register a remediation handler for an alert.
        
        Args:
            alert_name: Alert name to handle
            handler: Async function to remediate
        """
        if alert_name not in self._remediation_handlers:
            self._remediation_handlers[alert_name] = []
        self._remediation_handlers[alert_name].append(handler)
    
    async def start(self) -> None:
        """Start the sentinel monitor."""
        self._running = True
        
        for check in self._health_checks.values():
            task = asyncio.create_task(self._run_health_check(check))
            self._check_tasks[check.name] = task
        
        logger.info(f"Sentinel '{self.name}' started with {len(self._health_checks)} health checks")
    
    async def stop(self) -> None:
        """Stop the sentinel monitor."""
        self._running = False
        
        for task in self._check_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._check_tasks.clear()
        logger.info(f"Sentinel '{self.name}' stopped")
    
    async def _run_health_check(self, check: HealthCheck) -> None:
        """Run a health check on interval."""
        while self._running:
            try:
                await asyncio.sleep(check.interval_seconds)
                
                self._metrics.health_checks_run += 1
                
                try:
                    result = await asyncio.wait_for(
                        check.check_fn(),
                        timeout=check.timeout_seconds
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Health check '{check.name}' timed out")
                    result = False
                
                if result:
                    check.consecutive_failures = 0
                    self._metrics.health_checks_passed += 1
                    await self._maybe_resolve_alert(check.name)
                else:
                    check.consecutive_failures += 1
                    self._metrics.health_checks_failed += 1
                    
                    if check.consecutive_failures >= check.failure_threshold:
                        await self._trigger_alert(
                            name=check.name,
                            severity=AlertSeverity.ERROR,
                            message=f"Health check '{check.name}' failed {check.consecutive_failures} consecutive times"
                        )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check '{check.name}': {e}")
    
    async def _trigger_alert(
        self,
        name: str,
        severity: AlertSeverity,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Alert:
        """Trigger a new alert.
        
        Args:
            name: Alert name
            severity: Alert severity
            message: Alert message
            metadata: Optional metadata
            
        Returns:
            Created alert
        """
        async with self._lock:
            if name in self._active_alerts:
                return self._active_alerts[name]
            
            alert = Alert(
                id=f"{name}_{int(time.time())}",
                name=name,
                severity=severity,
                message=message,
                metadata=metadata or {}
            )
            
            self._active_alerts[name] = alert
            self._alert_history.append(alert)
            self._metrics.alerts_triggered += 1
            
            logger.log(
                logging.WARNING if severity == AlertSeverity.WARNING else logging.ERROR,
                f"Alert triggered: {name} - {message}"
            )
            
            await self._run_remediation(alert)
            
            return alert
    
    async def _maybe_resolve_alert(self, name: str) -> None:
        """Resolve alert if conditions are met."""
        if name in self._active_alerts:
            alert = self._active_alerts[name]
            alert.resolved = True
            self._metrics.alerts_resolved += 1
            logger.info(f"Alert resolved: {name}")
            
            del self._active_alerts[name]
    
    async def _run_remediation(self, alert: Alert) -> None:
        """Run remediation handlers for an alert."""
        handlers = self._remediation_handlers.get(alert.name, [])
        
        for handler in handlers:
            try:
                await handler(alert)
            except Exception as e:
                logger.error(f"Remediation handler error for '{alert.name}': {e}")
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert.
        
        Args:
            alert_id: Alert ID
            
        Returns:
            True if acknowledged
        """
        async with self._lock:
            for alert in self._active_alerts.values():
                if alert.id == alert_id:
                    alert.acknowledged = True
                    self._metrics.alerts_acknowledged += 1
                    return True
        return False
    
    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None
    ) -> List[Alert]:
        """Get active alerts, optionally filtered by severity.
        
        Args:
            severity: Optional severity filter
            
        Returns:
            List of active alerts
        """
        alerts = list(self._active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return alerts
    
    def get_alert_history(
        self,
        limit: int = 100,
        since: Optional[float] = None
    ) -> List[Alert]:
        """Get alert history.
        
        Args:
            limit: Maximum alerts to return
            since: Optional timestamp filter
            
        Returns:
            List of historical alerts
        """
        history = list(self._alert_history)
        
        if since:
            history = [a for a in history if a.timestamp >= datetime.fromtimestamp(since).isoformat()]
        
        return history[-limit:]
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        return {
            "sentinel_name": self.name,
            "running": self._running,
            "health_checks": {
                name: {
                    "consecutive_failures": check.consecutive_failures,
                    "failure_threshold": check.failure_threshold
                }
                for name, check in self._health_checks.items()
            },
            "active_alerts": {
                "total": len(self._active_alerts),
                "by_severity": {
                    s.name: sum(1 for a in self._active_alerts.values() if a.severity == s)
                    for s in AlertSeverity
                }
            },
            "metrics": {
                "health_checks_run": self._metrics.health_checks_run,
                "health_checks_passed": self._metrics.health_checks_passed,
                "health_checks_failed": self._metrics.health_checks_failed,
                "alerts_triggered": self._metrics.alerts_triggered,
                "alerts_resolved": self._metrics.alerts_resolved
            }
        }


class AnomalyDetector:
    """Detects anomalies in time-series metrics.
    
    Supports:
    - Z-score detection
    - Moving average deviation
    - IQR-based outliers
    """
    
    def __init__(
        self,
        window_size: int = 100,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5
    ):
        self.window_size = window_size
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self._values: deque = deque(maxlen=window_size)
    
    def add(self, value: float) -> None:
        """Add a value to the detector."""
        self._values.append(value)
    
    def detect_zscore(self, value: float) -> bool:
        """Detect anomaly using z-score method.
        
        Args:
            value: Value to check
            
        Returns:
            True if anomalous
        """
        if len(self._values) < 10:
            return False
        
        mean = sum(self._values) / len(self._values)
        variance = sum((v - mean) ** 2 for v in self._values) / len(self._values)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return False
        
        z_score = abs((value - mean) / std_dev)
        return z_score > self.zscore_threshold
    
    def detect_iqr(self, value: float) -> bool:
        """Detect anomaly using IQR method.
        
        Args:
            value: Value to check
            
        Returns:
            True if anomalous
        """
        if len(self._values) < 10:
            return False
        
        sorted_values = sorted(self._values)
        q1_idx = len(sorted_values) // 4
        q3_idx = 3 * len(sorted_values) // 4
        
        q1 = sorted_values[q1_idx]
        q3 = sorted_values[q3_idx]
        iqr = q3 - q1
        
        lower_bound = q1 - self.iqr_multiplier * iqr
        upper_bound = q3 + self.iqr_multiplier * iqr
        
        return value < lower_bound or value > upper_bound
    
    def detect_all(self, value: float) -> Dict[str, bool]:
        """Run all detection methods.
        
        Args:
            value: Value to check
            
        Returns:
            Dictionary of method -> is_anomaly
        """
        return {
            "zscore": self.detect_zscore(value),
            "iqr": self.detect_iqr(value)
        }
