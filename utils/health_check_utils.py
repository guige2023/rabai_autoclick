"""Health check utilities for monitoring system and service health."""

from typing import Callable, Dict, List, Optional, Any, Tuple
from enum import Enum
import time
import threading


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class HealthCheck:
    """Individual health check definition."""

    def __init__(
        self,
        name: str,
        check_fn: Callable[[], Tuple[bool, str]],
        timeout: float = 5.0,
        critical: bool = True
    ):
        """Initialize health check.
        
        Args:
            name: Check name.
            check_fn: Function returning (is_healthy, message).
            timeout: Check timeout in seconds.
            critical: If True, affects overall system health.
        """
        self.name = name
        self.check_fn = check_fn
        self.timeout = timeout
        self.critical = critical
        self._last_result: Optional[Tuple[HealthStatus, str]] = None
        self._last_check_time: Optional[float] = None

    def run(self) -> Tuple[HealthStatus, str]:
        """Run the health check.
        
        Returns:
            Tuple of (status, message).
        """
        start = time.time()
        try:
            result = self.check_fn()
            if isinstance(result, (list, tuple)):
                is_healthy, message = result[0], result[1] if len(result) > 1 else ""
            else:
                is_healthy = bool(result)
                message = ""
            status = HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY
        except Exception as e:
            status = HealthStatus.UNHEALTHY
            message = str(e)
        finally:
            self._last_check_time = time.time() - start
        self._last_result = (status, message)
        return status, message


class HealthMonitor:
    """System health monitor managing multiple checks."""

    def __init__(self):
        """Initialize health monitor."""
        self._checks: Dict[str, HealthCheck] = {}
        self._lock = threading.RLock()
        self._history: List[Dict[str, Any]] = []
        self._max_history = 100

    def register(
        self,
        name: str,
        check_fn: Callable[[], Tuple[bool, str]],
        timeout: float = 5.0,
        critical: bool = True
    ) -> None:
        """Register a health check.
        
        Args:
            name: Check name.
            check_fn: Check function.
            timeout: Timeout in seconds.
            critical: Whether critical to overall health.
        """
        with self._lock:
            self._checks[name] = HealthCheck(name, check_fn, timeout, critical)

    def unregister(self, name: str) -> bool:
        """Unregister a health check."""
        with self._lock:
            if name in self._checks:
                del self._checks[name]
                return True
            return False

    def check_all(self) -> Dict[str, Any]:
        """Run all health checks.
        
        Returns:
            Dict with overall and individual check results.
        """
        results = {}
        overall_healthy = True
        overall_degraded = False
        for name, check in self._checks.items():
            status, message = check.run()
            results[name] = {
                "status": status.value,
                "message": message,
                "last_duration_ms": round(check._last_check_time * 1000, 2) if check._last_check_time else None,
                "critical": check.critical,
            }
            if status == HealthStatus.UNHEALTHY and check.critical:
                overall_healthy = False
            if status in (HealthStatus.DEGRADED, HealthStatus.UNHEALTHY):
                overall_degraded = True
        if overall_healthy:
            overall_status = HealthStatus.HEALTHY
        elif overall_degraded:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.UNKNOWN
        return {
            "status": overall_status.value,
            "timestamp": time.time(),
            "checks": results,
        }

    def get_check(self, name: str) -> Optional[HealthCheck]:
        """Get a specific health check."""
        return self._checks.get(name)


def check_display_available() -> Tuple[bool, str]:
    """Check if display is available."""
    try:
        import tkinter
        root = tkinter.Tk()
        root.destroy()
        return True, "Display available"
    except Exception as e:
        return False, f"Display unavailable: {e}"


def check_camera_available() -> Tuple[bool, str]:
    """Check if camera is available."""
    try:
        import cv2
        cap = cv2.VideoCapture(0)
        available = cap.isOpened()
        cap.release()
        return available, "Camera available" if available else "Camera not found"
    except Exception as e:
        return False, f"Camera check failed: {e}"


def check_input_system() -> Tuple[bool, str]:
    """Check if input system is responsive."""
    try:
        import pyautogui
        pyautogui.size()
        return True, "Input system responsive"
    except Exception as e:
        return False, f"Input system error: {e}"
