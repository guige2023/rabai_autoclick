"""Health check action module for RabAI AutoClick.

Provides health check operations:
- HealthCheckAction: Generic health check
- HealthStatusAction: Get health status
- HealthRegisterAction: Register service health
- HealthDeregisterAction: Deregister service
- HealthListAction: List all health statuses
- HealthAlertAction: Health-based alerting
- HealthReportAction: Generate health report
- HealthWatchdogAction: Watchdog monitoring
"""

import os
import psutil
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HealthRegistry:
    """Service health registry."""
    
    _services: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def register(cls, service: str, status: str, **metadata) -> None:
        cls._services[service] = {
            "status": status,
            "registered_at": time.time(),
            "last_check": time.time(),
            **metadata
        }
    
    @classmethod
    def get(cls, service: str) -> Optional[Dict[str, Any]]:
        return cls._services.get(service)
    
    @classmethod
    def list_all(cls) -> Dict[str, Dict[str, Any]]:
        return cls._services.copy()
    
    @classmethod
    def update(cls, service: str, status: str, **metadata) -> None:
        if service in cls._services:
            cls._services[service]["status"] = status
            cls._services[service]["last_check"] = time.time()
            for k, v in metadata.items():
                cls._services[service][k] = v
    
    @classmethod
    def deregister(cls, service: str) -> bool:
        if service in cls._services:
            del cls._services[service]
            return True
        return False
    
    @classmethod
    def is_healthy(cls, service: str) -> bool:
        if service not in cls._services:
            return False
        return cls._services[service].get("status") == "healthy"


class HealthCheckAction(BaseAction):
    """Perform health check."""
    action_type = "health_check"
    display_name = "健康检查"
    description = "执行健康检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            check_type = params.get("type", "system")
            service_name = params.get("service", "")
            
            if check_type == "system":
                cpu = psutil.cpu_percent(interval=0.1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage("/")
                
                status = "healthy"
                if cpu > 90 or memory.percent > 90 or disk.percent > 90:
                    status = "unhealthy"
                elif cpu > 70 or memory.percent > 70 or disk.percent > 70:
                    status = "degraded"
                
                return ActionResult(
                    success=True,
                    message=f"System health: {status}",
                    data={
                        "status": status,
                        "cpu_percent": cpu,
                        "memory_percent": memory.percent,
                        "disk_percent": disk.percent
                    }
                )
            
            elif check_type == "service":
                if not service_name:
                    return ActionResult(success=False, message="service name required")
                
                service = HealthRegistry.get(service_name)
                if not service:
                    return ActionResult(
                        success=False,
                        message=f"Service not registered: {service_name}",
                        data={"service": service_name, "status": "unknown"}
                    )
                
                return ActionResult(
                    success=True,
                    message=f"Service {service_name}: {service['status']}",
                    data={"service": service_name, **service}
                )
            
            return ActionResult(success=False, message=f"Unknown check type: {check_type}")
        except Exception as e:
            return ActionResult(success=False, message=f"Health check failed: {str(e)}")


class HealthStatusAction(BaseAction):
    """Get overall health status."""
    action_type = "health_status"
    display_name = "健康状态"
    description = "获取健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            services = HealthRegistry.list_all()
            
            healthy = sum(1 for s in services.values() if s.get("status") == "healthy")
            degraded = sum(1 for s in services.values() if s.get("status") == "degraded")
            unhealthy = sum(1 for s in services.values() if s.get("status") == "unhealthy")
            
            overall = "healthy"
            if unhealthy > 0:
                overall = "unhealthy"
            elif degraded > 0:
                overall = "degraded"
            
            return ActionResult(
                success=True,
                message=f"Overall health: {overall}",
                data={
                    "overall": overall,
                    "healthy": healthy,
                    "degraded": degraded,
                    "unhealthy": unhealthy,
                    "total": len(services)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health status failed: {str(e)}")


class HealthRegisterAction(BaseAction):
    """Register service health."""
    action_type = "health_register"
    display_name = "注册健康"
    description = "注册服务健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            service = params.get("service", "")
            status = params.get("status", "healthy")
            metadata = params.get("metadata", {})
            
            if not service:
                return ActionResult(success=False, message="service required")
            
            HealthRegistry.register(service, status, **metadata)
            
            return ActionResult(
                success=True,
                message=f"Registered service: {service}",
                data={"service": service, "status": status}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health register failed: {str(e)}")


class HealthDeregisterAction(BaseAction):
    """Deregister service."""
    action_type = "health_deregister"
    display_name = "注销健康"
    description = "注销服务健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            service = params.get("service", "")
            
            if not service:
                return ActionResult(success=False, message="service required")
            
            deregistered = HealthRegistry.deregister(service)
            
            return ActionResult(
                success=deregistered,
                message=f"Deregistered: {service}" if deregistered else f"Not found: {service}",
                data={"service": service, "deregistered": deregistered}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health deregister failed: {str(e)}")


class HealthListAction(BaseAction):
    """List all health statuses."""
    action_type = "health_list"
    display_name = "健康列表"
    description = "列出所有健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            services = HealthRegistry.list_all()
            
            return ActionResult(
                success=True,
                message=f"Found {len(services)} registered services",
                data={"services": services, "count": len(services)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health list failed: {str(e)}")


class HealthAlertAction(BaseAction):
    """Health-based alerting."""
    action_type = "health_alert"
    display_name = "健康告警"
    description = "健康状态告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            threshold_cpu = params.get("threshold_cpu", 90)
            threshold_memory = params.get("threshold_memory", 90)
            threshold_disk = params.get("threshold_disk", 90)
            
            alerts = []
            
            cpu = psutil.cpu_percent(interval=0.1)
            if cpu > threshold_cpu:
                alerts.append({"type": "cpu", "value": cpu, "threshold": threshold_cpu})
            
            memory = psutil.virtual_memory()
            if memory.percent > threshold_memory:
                alerts.append({"type": "memory", "value": memory.percent, "threshold": threshold_memory})
            
            disk = psutil.disk_usage("/")
            if disk.percent > threshold_disk:
                alerts.append({"type": "disk", "value": disk.percent, "threshold": threshold_disk})
            
            return ActionResult(
                success=len(alerts) == 0,
                message=f"Generated {len(alerts)} health alerts",
                data={"alerts": alerts, "alert_count": len(alerts)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health alert failed: {str(e)}")


class HealthReportAction(BaseAction):
    """Generate health report."""
    action_type = "health_report"
    display_name = "健康报告"
    description = "生成健康报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            services = HealthRegistry.list_all()
            cpu = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            
            report = {
                "generated_at": datetime.now().isoformat(),
                "system": {
                    "cpu_percent": cpu,
                    "memory_percent": memory.percent,
                    "disk_percent": disk.percent,
                    "uptime": time.time() - psutil.boot_time()
                },
                "services": services,
                "summary": {
                    "total_services": len(services),
                    "healthy": sum(1 for s in services.values() if s.get("status") == "healthy"),
                    "degraded": sum(1 for s in services.values() if s.get("status") == "degraded"),
                    "unhealthy": sum(1 for s in services.values() if s.get("status") == "unhealthy")
                }
            }
            
            return ActionResult(
                success=True,
                message="Health report generated",
                data={"report": report}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health report failed: {str(e)}")


class HealthWatchdogAction(BaseAction):
    """Watchdog monitoring."""
    action_type = "health_watchdog"
    display_name = "看门狗监控"
    description = "看门狗监控"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            service = params.get("service", "")
            timeout = params.get("timeout", 60)
            
            if not service:
                return ActionResult(success=False, message="service required")
            
            service_info = HealthRegistry.get(service)
            
            if not service_info:
                return ActionResult(
                    success=False,
                    message=f"Service not registered: {service}",
                    data={"service": service, "status": "unknown"}
                )
            
            last_check = service_info.get("last_check", 0)
            elapsed = time.time() - last_check
            
            if elapsed > timeout:
                HealthRegistry.update(service, "unhealthy", reason="watchdog_timeout")
                return ActionResult(
                    success=False,
                    message=f"Watchdog timeout for {service}",
                    data={"service": service, "elapsed": elapsed, "timeout": timeout}
                )
            
            return ActionResult(
                success=True,
                message=f"Watchdog OK for {service}",
                data={"service": service, "elapsed": elapsed, "timeout": timeout}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watchdog failed: {str(e)}")
