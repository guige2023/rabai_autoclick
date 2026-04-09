"""
Watchdog Utilities for UI Automation.

This module provides watchdog utilities for monitoring system health,
detecting failures, and triggering recovery actions.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class WatchdogStatus(Enum):
    """Watchdog status values."""
    HEALTHY = auto()
    WARNING = auto()
    CRITICAL = auto()
    FAILED = auto()


@dataclass
class HealthCheck:
    """
    A health check definition.
    
    Attributes:
        name: Check name
        check_func: Function that returns True if healthy
        severity: Warning or critical threshold
        interval: Check interval in seconds
    """
    name: str
    check_func: Callable[[], bool]
    severity: str = "critical"  # "warning" or "critical"
    interval: float = 1.0
    timeout: float = 5.0
    
    # Runtime state
    is_healthy: bool = True
    last_check_time: float = 0
    consecutive_failures: int = 0


@dataclass
class WatchdogConfig:
    """
    Configuration for a watchdog.
    
    Attributes:
        name: Watchdog name
        check_interval: How often to run health checks
        warning_threshold: Failures before warning status
        critical_threshold: Failures before critical status
        recovery_threshold: Successful checks to recover
    """
    name: str = "watchdog"
    check_interval: float = 1.0
    warning_threshold: int = 3
    critical_threshold: int = 5
    recovery_threshold: int = 2


class Watchdog:
    """
    System watchdog for monitoring health and triggering recovery.
    
    Example:
        dog = Watchdog(config=WatchdogConfig(name="app_watchdog"))
        dog.add_check(HealthCheck(
            name="database",
            check_func=lambda: db.is_connected()
        ))
        dog.start()
    """
    
    def __init__(self, config: Optional[WatchdogConfig] = None):
        self.config = config or WatchdogConfig()
        self._checks: dict[str, HealthCheck] = {}
        self._status = WatchdogStatus.HEALTHY
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: dict[str, list[Callable[[], None]]] = {
            "warning": [],
            "critical": [],
            "recovery": [],
            "failure": []
        }
        self._recovery_count: int = 0
    
    def add_check(self, check: HealthCheck) -> None:
        """
        Add a health check.
        
        Args:
            check: HealthCheck to add
        """
        self._checks[check.name] = check
    
    def remove_check(self, name: str) -> bool:
        """Remove a health check by name."""
        if name in self._checks:
            del self._checks[name]
            return True
        return False
    
    def get_check(self, name: str) -> Optional[HealthCheck]:
        """Get a health check by name."""
        return self._checks.get(name)
    
    def on_warning(self, callback: Callable[[], None]) -> None:
        """Register a warning callback."""
        self._callbacks["warning"].append(callback)
    
    def on_critical(self, callback: Callable[[], None]) -> None:
        """Register a critical callback."""
        self._callbacks["critical"].append(callback)
    
    def on_recovery(self, callback: Callable[[], None]) -> None:
        """Register a recovery callback."""
        self._callbacks["recovery"].append(callback)
    
    def on_failure(self, callback: Callable[[], None]) -> None:
        """Register a failure callback."""
        self._callbacks["failure"].append(callback)
    
    def start(self) -> None:
        """Start the watchdog."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the watchdog."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
    
    def _run_loop(self) -> None:
        """Main watchdog loop."""
        while self._running:
            self._check_all()
            
            # Determine overall status
            new_status = self._calculate_status()
            
            if new_status != self._status:
                old_status = self._status
                self._status = new_status
                self._handle_status_change(old_status, new_status)
            
            time.sleep(self.config.check_interval)
    
    def _check_all(self) -> None:
        """Run all health checks."""
        for check in self._checks.values():
            try:
                # Run check with timeout
                start = time.time()
                result = self._run_check_with_timeout(check)
                elapsed = time.time() - start
                
                check.last_check_time = time.time()
                
                if result:
                    if not check.is_healthy:
                        check.consecutive_failures = 0
                    check.is_healthy = True
                else:
                    check.consecutive_failures += 1
                    check.is_healthy = False
                    
            except Exception:
                check.consecutive_failures += 1
                check.is_healthy = False
    
    def _run_check_with_timeout(self, check: HealthCheck) -> bool:
        """Run a check with timeout."""
        result = False
        start = time.time()
        
        while time.time() - start < check.timeout:
            try:
                if check.check_func():
                    result = True
                    break
            except Exception:
                pass
            time.sleep(0.1)
        
        return result
    
    def _calculate_status(self) -> WatchdogStatus:
        """Calculate overall watchdog status."""
        total_failures = sum(c.consecutive_failures for c in self._checks.values())
        critical_checks = [
            c for c in self._checks.values()
            if c.severity == "critical" and not c.is_healthy
        ]
        warning_checks = [
            c for c in self._checks.values()
            if c.severity == "warning" and not c.is_healthy
        ]
        
        if total_failures >= self.config.critical_threshold or critical_checks:
            return WatchdogStatus.CRITICAL
        elif total_failures >= self.config.warning_threshold or warning_checks:
            return WatchdogStatus.WARNING
        elif all(c.is_healthy for c in self._checks.values()):
            return WatchdogStatus.HEALTHY
        
        return WatchdogStatus.WARNING
    
    def _handle_status_change(
        self,
        old_status: WatchdogStatus,
        new_status: WatchdogStatus
    ) -> None:
        """Handle status changes and trigger callbacks."""
        if new_status == WatchdogStatus.CRITICAL:
            self._recovery_count = 0
            self._trigger_callbacks("critical")
        elif new_status == WatchdogStatus.WARNING:
            self._trigger_callbacks("warning")
        elif new_status == WatchdogStatus.HEALTHY:
            self._recovery_count += 1
            if old_status != WatchdogStatus.HEALTHY and self._recovery_count >= self.config.recovery_threshold:
                self._trigger_callbacks("recovery")
    
    def _trigger_callbacks(self, event: str) -> None:
        """Trigger callbacks for an event."""
        for callback in self._callbacks.get(event, []):
            try:
                callback()
            except Exception:
                pass
    
    @property
    def status(self) -> WatchdogStatus:
        """Get current watchdog status."""
        return self._status
    
    @property
    def is_healthy(self) -> bool:
        """Check if watchdog is in healthy state."""
        return self._status == WatchdogStatus.HEALTHY
    
    def get_unhealthy_checks(self) -> list[HealthCheck]:
        """Get list of unhealthy checks."""
        return [c for c in self._checks.values() if not c.is_healthy]
