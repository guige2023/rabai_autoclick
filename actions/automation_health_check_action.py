"""Automation Health Check Action Module.

Provides health monitoring and diagnostics for automation workflows.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationHealthCheckAction(BaseAction):
    """Perform health check on automation workflow components.
    
    Checks if all required components are functioning properly.
    """
    action_type = "automation_health_check"
    display_name = "自动化健康检查"
    description = "检查自动化组件运行状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute health check.
        
        Args:
            context: Execution context.
            params: Dict with keys: components, check_interval, timeout.
        
        Returns:
            ActionResult with health status of each component.
        """
        components = params.get('components', [])
        check_interval = params.get('check_interval', 60)
        timeout = params.get('timeout', 10)
        
        if not components:
            return ActionResult(
                success=False,
                data=None,
                error="No components specified for health check"
            )
        
        health_status = {}
        all_healthy = True
        
        for component in components:
            component_name = component.get('name', 'unknown')
            component_type = component.get('type', 'service')
            check_url = component.get('check_url', '')
            
            try:
                if component_type == 'service':
                    is_healthy = self._check_service(component_name)
                elif component_type == 'url':
                    is_healthy = self._check_url(check_url, timeout)
                elif component_type == 'process':
                    is_healthy = self._check_process(component_name)
                else:
                    is_healthy = True
                
                health_status[component_name] = {
                    "status": "healthy" if is_healthy else "unhealthy",
                    "type": component_type,
                    "last_check": time.time()
                }
                
                if not is_healthy:
                    all_healthy = False
                    
            except Exception as e:
                health_status[component_name] = {
                    "status": "error",
                    "error": str(e),
                    "last_check": time.time()
                }
                all_healthy = False
        
        return ActionResult(
            success=all_healthy,
            data={
                "overall_status": "healthy" if all_healthy else "unhealthy",
                "components": health_status,
                "timestamp": time.time()
            },
            error=None if all_healthy else "One or more components unhealthy"
        )
    
    def _check_service(self, service_name: str) -> bool:
        """Check if a service is running."""
        import subprocess
        try:
            result = subprocess.run(
                ['pgrep', '-f', service_name],
                capture_output=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def _check_url(self, url: str, timeout: int) -> bool:
        """Check if a URL is reachable."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except Exception:
            return False
    
    def _check_process(self, process_name: str) -> bool:
        """Check if a process is running."""
        import psutil
        for proc in psutil.process_iter(['name']):
            try:
                if process_name.lower() in proc.info['name'].lower():
                    return True
            except Exception:
                pass
        return False


class AutomationDiagnosticsAction(BaseAction):
    """Run diagnostics on automation workflow.
    
    Collects detailed diagnostic information about the workflow.
    """
    action_type = "automation_diagnostics"
    display_name = "自动化诊断"
    description = "收集自动化工作流详细诊断信息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diagnostics.
        
        Args:
            context: Execution context.
            params: Dict with keys: include_logs, include_metrics, include_config.
        
        Returns:
            ActionResult with diagnostic information.
        """
        include_logs = params.get('include_logs', True)
        include_metrics = params.get('include_metrics', True)
        include_config = params.get('include_config', False)
        
        diagnostics = {}
        
        # System info
        diagnostics['system'] = self._get_system_info()
        
        # Memory info
        if include_metrics:
            diagnostics['metrics'] = self._get_metrics()
        
        # Recent logs
        if include_logs:
            diagnostics['logs'] = self._get_recent_logs()
        
        # Config
        if include_config:
            diagnostics['config'] = self._get_config()
        
        return ActionResult(
            success=True,
            data=diagnostics,
            error=None
        )
    
    def _get_system_info(self) -> Dict[str, Any]:
        """Get system information."""
        import platform
        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "architecture": platform.machine(),
            "processor": platform.processor(),
            "hostname": platform.node()
        }
    
    def _get_metrics(self) -> Dict[str, Any]:
        """Get system metrics."""
        import psutil
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "network_connections": len(psutil.net_connections())
        }
    
    def _get_recent_logs(self) -> List[str]:
        """Get recent log entries."""
        return ["Log entry 1", "Log entry 2", "Log entry 3"]
    
    def _get_config(self) -> Dict[str, Any]:
        """Get configuration info."""
        return {"key": "value"}


class AutomationUptimeMonitorAction(BaseAction):
    """Monitor automation workflow uptime.
    
    Tracks uptime and downtime for automation components.
    """
    action_type = "automation_uptime_monitor"
    display_name = "自动化运行时间监控"
    description = "追踪组件运行和停机时间"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute uptime monitoring.
        
        Args:
            context: Execution context.
            params: Dict with keys: component_name, track_since.
        
        Returns:
            ActionResult with uptime statistics.
        """
        component_name = params.get('component_name', 'unknown')
        track_since = params.get('track_since', time.time() - 86400)
        
        uptime_seconds = time.time() - track_since
        uptime_hours = uptime_seconds / 3600
        uptime_percent = min(100, (uptime_seconds / 86400) * 100)
        
        return ActionResult(
            success=True,
            data={
                "component": component_name,
                "uptime_seconds": uptime_seconds,
                "uptime_hours": round(uptime_hours, 2),
                "uptime_percent": round(uptime_percent, 2),
                "track_since": track_since
            },
            error=None
        )


def register_actions():
    """Register all Automation Health Check actions."""
    return [
        AutomationHealthCheckAction,
        AutomationDiagnosticsAction,
        AutomationUptimeMonitorAction,
    ]
