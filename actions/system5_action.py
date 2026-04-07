"""System5 action module for RabAI AutoClick.

Provides additional system operations:
- SystemUptimeAction: Get system uptime
- SystemLoadAverageAction: Get load average
- SystemDiskUsageAction: Get disk usage
- SystemMemoryInfoAction: Get memory info
- SystemCPUCountAction: Get CPU count
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SystemUptimeAction(BaseAction):
    """Get system uptime."""
    action_type = "system5_uptime"
    display_name = "获取运行时间"
    description = "获取系统运行时间"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute uptime.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with uptime info.
        """
        output_var = params.get('output_var', 'uptime')

        try:
            import time

            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.read().split()[0])

            uptime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(uptime_seconds))

            context.set(output_var, {
                'seconds': uptime_seconds,
                'formatted': uptime_str
            })

            return ActionResult(
                success=True,
                message=f"运行时间: {uptime_str}",
                data={
                    'uptime_seconds': uptime_seconds,
                    'uptime_formatted': uptime_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取运行时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uptime'}


class SystemLoadAverageAction(BaseAction):
    """Get load average."""
    action_type = "system5_load"
    display_name = "获取负载"
    description = "获取系统负载平均值"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute load average.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with load average.
        """
        output_var = params.get('output_var', 'load_average')

        try:
            import os

            load1, load5, load15 = os.getloadavg()

            context.set(output_var, {
                '1min': load1,
                '5min': load5,
                '15min': load15
            })

            return ActionResult(
                success=True,
                message=f"系统负载: {load1:.2f}",
                data={
                    'load_1min': load1,
                    'load_5min': load5,
                    'load_15min': load15,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取负载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'load_average'}


class SystemDiskUsageAction(BaseAction):
    """Get disk usage."""
    action_type = "system5_disk"
    display_name = "获取磁盘使用"
    description = "获取磁盘使用情况"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute disk usage.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with disk usage.
        """
        path = params.get('path', '/')
        output_var = params.get('output_var', 'disk_usage')

        try:
            import shutil

            resolved_path = context.resolve_value(path) if path else '/'

            usage = shutil.disk_usage(resolved_path)

            context.set(output_var, {
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': (usage.used / usage.total) * 100
            })

            return ActionResult(
                success=True,
                message=f"磁盘使用: {usage.used / (1024**3):.2f}GB / {usage.total / (1024**3):.2f}GB",
                data={
                    'total': usage.total,
                    'used': usage.used,
                    'free': usage.free,
                    'percent': (usage.used / usage.total) * 100,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘使用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '/', 'output_var': 'disk_usage'}


class SystemMemoryInfoAction(BaseAction):
    """Get memory info."""
    action_type = "system5_memory"
    display_name = "获取内存信息"
    description = "获取系统内存信息"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute memory info.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with memory info.
        """
        output_var = params.get('output_var', 'memory_info')

        try:
            import psutil

            memory = psutil.virtual_memory()

            context.set(output_var, {
                'total': memory.total,
                'available': memory.available,
                'used': memory.used,
                'percent': memory.percent
            })

            return ActionResult(
                success=True,
                message=f"内存使用: {memory.percent:.1f}%",
                data={
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'percent': memory.percent,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取内存信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'memory_info'}


class SystemCPUCountAction(BaseAction):
    """Get CPU count."""
    action_type = "system5_cpu_count"
    display_name = "获取CPU核心数"
    description = "获取CPU核心数"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CPU count.

        Args:
            context: Execution context.
            params: Dict with logical, output_var.

        Returns:
            ActionResult with CPU count.
        """
        logical = params.get('logical', True)
        output_var = params.get('output_var', 'cpu_count')

        try:
            import os

            resolved_logical = bool(context.resolve_value(logical)) if logical else True

            if resolved_logical:
                count = os.cpu_count()
            else:
                import subprocess
                result = subprocess.run(['sysctl', '-n', 'hw.physicalcpu'], capture_output=True, text=True)
                count = int(result.stdout.strip()) if result.returncode == 0 else os.cpu_count()

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"CPU核心数: {count}",
                data={
                    'cpu_count': count,
                    'logical': resolved_logical,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CPU核心数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'logical': True, 'output_var': 'cpu_count'}