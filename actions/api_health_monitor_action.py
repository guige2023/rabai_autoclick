"""API health monitor action module for RabAI AutoClick.

Provides health monitoring for API endpoints:
- ApiHealthMonitor: Monitor API endpoint health
- HealthChecker: Periodic health checks
- EndpointHealthTracker: Track endpoint health history
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HealthStatus(Enum):
    """Health statuses."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthConfig:
    """Configuration for health monitoring."""
    check_interval: float = 60.0
    timeout: float = 5.0
    failure_threshold: int = 3
    success_threshold: int = 2
    track_history: int = 100


@dataclass
class EndpointHealth:
    """Endpoint health record."""
    endpoint: str
    status: HealthStatus
    latency: float
    status_code: int
    timestamp: float = field(default_factory=time.time)
    error: Optional[str] = None


class ApiHealthMonitor:
    """Monitor API endpoint health."""
    
    def __init__(self, config: HealthConfig):
        self.config = config
        self._endpoints: Dict[str, EndpointHealth] = {}
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=config.track_history))
        self._health_checks: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"total_checks": 0, "healthy_checks": 0, "unhealthy_checks": 0}
    
    def record_check(self, endpoint: str, status: HealthStatus, latency: float = 0.0, status_code: int = 200, error: Optional[str] = None):
        """Record health check result."""
        health = EndpointHealth(
            endpoint=endpoint,
            status=status,
            latency=latency,
            status_code=status_code,
            error=error
        )
        
        with self._lock:
            self._endpoints[endpoint] = health
            self._history[endpoint].append(health)
            self._stats["total_checks"] += 1
            
            if status == HealthStatus.HEALTHY:
                self._stats["healthy_checks"] += 1
            else:
                self._stats["unhealthy_checks"] += 1
    
    def get_health(self, endpoint: str) -> HealthStatus:
        """Get current health status for endpoint."""
        with self._lock:
            health = self._endpoints.get(endpoint)
            if not health:
                return HealthStatus.UNKNOWN
            return health.status
    
    def get_history(self, endpoint: str) -> List[EndpointHealth]:
        """Get health history for endpoint."""
        with self._lock:
            return list(self._history.get(endpoint, []))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get health monitoring statistics."""
        with self._lock:
            endpoint_stats = {}
            for ep, health in self._endpoints.items():
                history = list(self._history.get(ep, []))
                recent_statuses = [h.status for h in history[-10:]]
                healthy_count = sum(1 for s in recent_statuses if s == HealthStatus.HEALTHY)
                
                endpoint_stats[ep] = {
                    "current_status": health.status.value,
                    "latency": health.latency,
                    "recent_health_ratio": healthy_count / max(1, len(recent_statuses)),
                    "check_count": len(history),
                }
            
            return {
                "tracked_endpoints": len(self._endpoints),
                **{k: v for k, v in self._stats.items()},
                "endpoints": endpoint_stats,
            }


class ApiHealthMonitorAction(BaseAction):
    """API health monitor action."""
    action_type = "api_health_monitor"
    display_name = "API健康监控"
    description = "API端点健康状态监控"
    
    def __init__(self):
        super().__init__()
        self._monitor: Optional[ApiHealthMonitor] = None
        self._lock = threading.Lock()
    
    def _get_monitor(self, params: Dict[str, Any]) -> ApiHealthMonitor:
        """Get or create health monitor."""
        with self._lock:
            if self._monitor is None:
                config = HealthConfig(
                    check_interval=params.get("check_interval", 60.0),
                    timeout=params.get("timeout", 5.0),
                    failure_threshold=params.get("failure_threshold", 3),
                    track_history=params.get("track_history", 100),
                )
                self._monitor = ApiHealthMonitor(config)
            return self._monitor
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute health monitoring operation."""
        try:
            monitor = self._get_monitor(params)
            command = params.get("command", "record")
            
            if command == "record":
                endpoint = params.get("endpoint")
                status_str = params.get("status", "healthy").upper()
                
                try:
                    status = HealthStatus[status_str]
                except KeyError:
                    status = HealthStatus.UNKNOWN
                
                monitor.record_check(
                    endpoint=endpoint,
                    status=status,
                    latency=params.get("latency", 0.0),
                    status_code=params.get("status_code", 200),
                    error=params.get("error"),
                )
                return ActionResult(success=True)
            
            elif command == "health":
                endpoint = params.get("endpoint")
                health = monitor.get_health(endpoint)
                return ActionResult(success=True, data={"endpoint": endpoint, "status": health.value})
            
            elif command == "history":
                endpoint = params.get("endpoint")
                history = monitor.get_history(endpoint)
                return ActionResult(success=True, data={
                    "history": [{"status": h.status.value, "latency": h.latency, "timestamp": h.timestamp} for h in history]
                })
            
            elif command == "stats":
                stats = monitor.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiHealthMonitorAction error: {str(e)}")
