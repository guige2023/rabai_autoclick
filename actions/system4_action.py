"""System4 action module for RabAI AutoClick.

Provides additional system operations:
- SystemUptimeAction: Get system uptime
- SystemBootTimeAction: Get system boot time
- SystemCpuCountAction: Get CPU count
- SystemLoadAverageAction: Get load average
- SystemMemoryInfoAction: Get memory info
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SystemUptimeAction(BaseAction):
    """Get system uptime."""
    action_type = "system4_uptime"
    display_name = "系统运行时间"
    description = "获取系统运行时间"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute system uptime.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with uptime.
        """
        output_var = params.get('output_var', 'uptime_result')

        try:
            import psutil
            uptime_seconds = psutil.boot_time()
            from datetime import datetime
            boot_time = datetime.fromtimestamp(uptime_seconds)
            now = datetime.now()
            uptime = now - boot_time

            result = {
                'seconds': uptime.total_seconds(),
                'days': uptime.days,
                'hours': uptime.seconds // 3600,
                'minutes': (uptime.seconds % 3600) // 60,
                'boot_time': boot_time.isoformat()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统运行时间: {result['days']} 天 {result['hours']} 小时",
                data={
                    'uptime': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取系统运行时间失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统运行时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uptime_result'}


class SystemBootTimeAction(BaseAction):
    """Get system boot time."""
    action_type = "system4_boottime"
    display_name = "系统启动时间"
    description = "获取系统启动时间"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute system boot time.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with boot time.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'boottime_result')

        try:
            import psutil
            from datetime import datetime

            boot_ts = psutil.boot_time()
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            boot_time = datetime.fromtimestamp(boot_ts).strftime(resolved_format)

            result = {
                'timestamp': boot_ts,
                'datetime': boot_time
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统启动时间: {boot_time}",
                data={
                    'boottime': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取系统启动时间失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统启动时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'boottime_result'}


class SystemCpuCountAction(BaseAction):
    """Get CPU count."""
    action_type = "system4_cpu_count"
    display_name = "CPU核心数"
    description = "获取CPU核心数"
    version = "4.0"

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
        output_var = params.get('output_var', 'cpu_count_result')

        try:
            import psutil

            resolved_logical = bool(context.resolve_value(logical)) if logical else True
            count = psutil.cpu_count(logical=resolved_logical)

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"CPU核心数: {count}",
                data={
                    'count': count,
                    'logical': resolved_logical,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取CPU核心数失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CPU核心数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'logical': True, 'output_var': 'cpu_count_result'}


class SystemLoadAverageAction(BaseAction):
    """Get load average."""
    action_type = "system4_loadavg"
    display_name = "系统负载"
    description = "获取系统负载平均值"
    version = "4.0"

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
        output_var = params.get('output_var', 'loadavg_result')

        try:
            import os
            load1, load5, load15 = os.getloadavg()

            result = {
                '1min': load1,
                '5min': load5,
                '15min': load15
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统负载: {load1:.2f}, {load5:.2f}, {load15:.2f}",
                data={
                    'load_average': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统负载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'loadavg_result'}


class SystemMemoryInfoAction(BaseAction):
    """Get memory info."""
    action_type = "system4_memory"
    display_name = "内存信息"
    description = "获取系统内存信息"
    version = "4.0"

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

            mem = psutil.virtual_memory()
            result = {
                'total': mem.total,
                'available': mem.available,
                'used': mem.used,
                'percent': mem.percent,
                'total_gb': mem.total / (1024**3),
                'available_gb': mem.available / (1024**3),
                'used_gb': mem.used / (1024**3)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"内存信息: {result['used_gb']:.2f}GB / {result['total_gb']:.2f}GB ({result['percent']:.1f}%)",
                data={
                    'memory': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取内存信息失败: 未安装psutil库"
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