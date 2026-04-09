"""
Automation Health Check Module.

Provides health monitoring, readiness probes, dependency checks,
and self-healing for distributed automation systems.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CheckType(Enum):
    """Type of health check."""
    HTTP = "http"
    TCP = "tcp"
    PROCESS = "process"
    MEMORY = "memory"
    DISK = "disk"
    CUSTOM = "custom"
    DEPENDENCY = "dependency"
    READINESS = "readiness"
    LIVENESS = "liveness"


@dataclass
class HealthCheck:
    """Container for a health check."""
    name: str
    check_type: CheckType
    func: Callable[..., Any]
    interval: float = 60.0  # seconds
    timeout: float = 5.0
    threshold: int = 3  # Consecutive failures before unhealthy
    enabled: bool = True
    critical: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    
@dataclass
class HealthCheckResult:
    """Result of a health check execution."""
    name: str
    status: HealthStatus
    latency: float
    timestamp: float
    message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    consecutive_failures: int = 0
    
    
@dataclass
class SystemHealth:
    """Overall system health status."""
    status: HealthStatus
    timestamp: float
    checks: List[HealthCheckResult]
    uptime: float
    version: str = "1.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


class HealthCheckManager:
    """
    Manages health checks for services and dependencies.
    
    Example:
        manager = HealthCheckManager()
        
        # Add HTTP health check
        manager.add_check(HealthCheck(
            name="api",
            check_type=CheckType.HTTP,
            func=lambda: check_http_endpoint("https://api.example.com/health"),
            interval=30
        ))
        
        # Add dependency check
        manager.add_check(HealthCheck(
            name="database",
            check_type=CheckType.TCP,
            func=lambda: check_tcp("localhost", 5432),
            critical=True
        ))
        
        # Get system health
        health = await manager.get_health()
        print(f"Status: {health.status.value}")
    """
    
    def __init__(
        self,
        enable_auto_recovery: bool = False,
        recovery_callback: Optional[Callable[[str, HealthCheckResult], Any]] = None,
    ) -> None:
        """
        Initialize health check manager.
        
        Args:
            enable_auto_recovery: Enable automatic recovery actions.
            recovery_callback: Callback function for recovery actions.
        """
        self._checks: Dict[str, HealthCheck] = {}
        self._results: Dict[str, deque] = {}
        self._consecutive_failures: Dict[str, int] = {}
        self._last_check: Dict[str, float] = {}
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._enable_auto_recovery = enable_auto_recovery
        self._recovery_callback = recovery_callback
        self._start_time = time.time()
        self._lock = asyncio.Lock()
        
    def add_check(self, check: HealthCheck) -> None:
        """
        Add a health check.
        
        Args:
            check: HealthCheck to add.
        """
        self._checks[check.name] = check
        self._results[check.name] = deque(maxlen=100)
        self._consecutive_failures[check.name] = 0
        
        logger.info(f"Added health check: {check.name} (type={check.check_type.value})")
        
    def remove_check(self, name: str) -> bool:
        """Remove a health check."""
        if name in self._checks:
            del self._checks[name]
            del self._results[name]
            del self._consecutive_failures[name]
            return True
        return False
        
    async def check_health(self, name: str) -> HealthCheckResult:
        """
        Execute a single health check.
        
        Args:
            name: Name of check to execute.
            
        Returns:
            HealthCheckResult.
        """
        if name not in self._checks:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNKNOWN,
                latency=0,
                timestamp=time.time(),
                message="Check not found"
            )
            
        check = self._checks[name]
        
        if not check.enabled:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNKNOWN,
                latency=0,
                timestamp=time.time(),
                message="Check disabled"
            )
            
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(check.func):
                result = await asyncio.wait_for(check.func(), timeout=check.timeout)
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(check.func),
                    timeout=check.timeout
                )
                
            latency = time.time() - start_time
            
            # Determine status from result
            if result is True:
                status = HealthStatus.HEALTHY
                message = "OK"
            elif isinstance(result, dict):
                status = HealthStatus(result.get("status", "healthy"))
                message = result.get("message", "OK")
            else:
                status = HealthStatus.HEALTHY
                message = str(result) if result else "OK"
                
            self._consecutive_failures[name] = 0
            
            check_result = HealthCheckResult(
                name=name,
                status=status,
                latency=latency,
                timestamp=time.time(),
                message=message,
                consecutive_failures=0,
            )
            
        except asyncio.TimeoutError:
            latency = time.time() - start_time
            self._consecutive_failures[name] = self._consecutive_failures.get(name, 0) + 1
            check_result = HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                latency=latency,
                timestamp=time.time(),
                message=f"Check timed out after {check.timeout}s",
                consecutive_failures=self._consecutive_failures[name],
            )
            
        except Exception as e:
            latency = time.time() - start_time
            self._consecutive_failures[name] = self._consecutive_failures.get(name, 0) + 1
            check_result = HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                latency=latency,
                timestamp=time.time(),
                message=str(e),
                consecutive_failures=self._consecutive_failures[name],
            )
            
        # Record result
        self._results[name].append(check_result)
        self._last_check[name] = time.time()
        
        # Trigger recovery if enabled
        if (self._enable_auto_recovery and 
            self._consecutive_failures[name] >= check.threshold):
            await self._trigger_recovery(name, check_result)
            
        return check_result
        
    async def get_health(self) -> SystemHealth:
        """
        Get overall system health.
        
        Returns:
            SystemHealth with aggregated status.
        """
        results = []
        overall_status = HealthStatus.HEALTHY
        has_critical_failure = False
        
        for name in self._checks:
            result = await self.check_health(name)
            results.append(result)
            
            # Determine overall status
            if result.status == HealthStatus.UNHEALTHY:
                if self._checks[name].critical:
                    has_critical_failure = True
                    
            if overall_status == HealthStatus.HEALTHY:
                if result.status == HealthStatus.UNHEALTHY:
                    overall_status = HealthStatus.DEGRADED
                elif result.status == HealthStatus.DEGRADED:
                    overall_status = HealthStatus.DEGRADED
            elif overall_status == HealthStatus.DEGRADED:
                if result.status == HealthStatus.UNHEALTHY and self._checks[name].critical:
                    overall_status = HealthStatus.UNHEALTHY
                    
        if has_critical_failure:
            overall_status = HealthStatus.UNHEALTHY
            
        return SystemHealth(
            status=overall_status,
            timestamp=time.time(),
            checks=results,
            uptime=time.time() - self._start_time,
        )
        
    async def get_readiness(self) -> Tuple[bool, List[str]]:
        """
        Check if system is ready to receive traffic.
        
        Returns:
            Tuple of (is_ready, list of reasons).
        """
        is_ready = True
        reasons = []
        
        for name, check in self._checks.items():
            if check.check_type == CheckType.READINESS:
                result = await self.check_health(name)
                if result.status != HealthStatus.HEALTHY:
                    is_ready = False
                    reasons.append(f"{name}: {result.message}")
                    
        return is_ready, reasons
        
    async def get_liveness(self) -> Tuple[bool, List[str]]:
        """
        Check if system is alive.
        
        Returns:
            Tuple of (is_alive, list of failed checks).
        """
        is_alive = True
        failed = []
        
        for name, check in self._checks.items():
            if check.check_type == CheckType.LIVENESS:
                result = await self.check_health(name)
                if result.status != HealthStatus.HEALTHY:
                    is_alive = False
                    failed.append(f"{name}: {result.message}")
                    
        return is_alive, failed
        
    async def start_monitoring(self) -> None:
        """Start background health monitoring."""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Health check monitoring started")
        
    async def stop_monitoring(self) -> None:
        """Stop background monitoring."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
        logger.info("Health check monitoring stopped")
        
    async def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                for name, check in self._checks.items():
                    if not check.enabled:
                        continue
                        
                    # Check if interval has passed
                    last = self._last_check.get(name, 0)
                    if time.time() - last >= check.interval:
                        await self.check_health(name)
                        
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(10)
                
    async def _trigger_recovery(
        self,
        name: str,
        result: HealthCheckResult,
    ) -> None:
        """Trigger recovery action for failed check."""
        logger.warning(f"Triggering recovery for {name} (failures={result.consecutive_failures})")
        
        if self._recovery_callback:
            try:
                await self._recovery_callback(name, result)
            except Exception as e:
                logger.error(f"Recovery callback failed: {e}")
                
    def get_stats(self) -> Dict[str, Any]:
        """Get health check statistics."""
        return {
            "total_checks": len(self._checks),
            "enabled_checks": sum(1 for c in self._checks.values() if c.enabled),
            "critical_checks": sum(1 for c in self._checks.values() if c.critical),
            "monitoring": self._running,
            "uptime": time.time() - self._start_time,
            "last_check": self._last_check,
        }
        
    def get_check_result_history(
        self,
        name: str,
        limit: int = 100,
    ) -> List[HealthCheckResult]:
        """Get history of check results."""
        if name in self._results:
            return list(self._results[name])[-limit:]
        return []


class ReadinessProbe:
    """
    Kubernetes-style readiness probe.
    
    Example:
        probe = ReadinessProbe(failure_threshold=3, success_threshold=1)
        
        # In request handler:
        if await probe.is_ready():
            handle_request()
        else:
            return 503 Service Unavailable
    """
    
    def __init__(
        self,
        failure_threshold: int = 3,
        success_threshold: int = 1,
    ) -> None:
        """
        Initialize readiness probe.
        
        Args:
            failure_threshold: Failures before not ready.
            success_threshold: Successes before ready again.
        """
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._ready = True
        
    async def is_ready(self) -> bool:
        """Check if ready to receive traffic."""
        return self._ready
        
    async def report_success(self) -> None:
        """Report successful health check."""
        self._consecutive_failures = 0
        self._consecutive_successes += 1
        
        if self._consecutive_successes >= self.success_threshold and not self._ready:
            self._ready = True
            logger.info("Readiness probe: became ready")
            
    async def report_failure(self) -> None:
        """Report failed health check."""
        self._consecutive_successes = 0
        self._consecutive_failures += 1
        
        if self._consecutive_failures >= self.failure_threshold and self._ready:
            self._ready = False
            logger.warning("Readiness probe: became not ready")


class LivenessProbe:
    """
    Kubernetes-style liveness probe.
    
    Example:
        probe = LivenessProbe()
        
        # Periodic check:
        alive, reasons = await probe.check()
        if not alive:
            restart_service()
    """
    
    def __init__(self, failure_threshold: int = 3) -> None:
        """
        Initialize liveness probe.
        
        Args:
            failure_threshold: Failures before considered dead.
        """
        self.failure_threshold = failure_threshold
        self._consecutive_failures = 0
        
    async def check(self) -> Tuple[bool, List[str]]:
        """Check if process is alive."""
        # Can add actual health checks here
        is_alive = self._consecutive_failures < self.failure_threshold
        return is_alive, []
        
    async def report_failure(self) -> None:
        """Report liveness check failure."""
        self._consecutive_failures += 1
        
    async def report_success(self) -> None:
        """Report liveness check success."""
        self._consecutive_failures = 0
