"""System3 action module for RabAI AutoClick.

Provides additional system operations:
- SystemUptimeAction: Get system uptime
- SystemLoadAverageAction: Get load average
- SystemDiskUsageAction: Get disk usage
- SystemBootTimeAction: Get boot time
- SystemUserAction: Get current user
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SystemUptimeAction(BaseAction):
    """Get system uptime."""
    action_type = "system3_uptime"
    display_name = "系统运行时间"
    description = "获取系统运行时间"

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
            ActionResult with uptime.
        """
        output_var = params.get('output_var', 'uptime')

        try:
            import subprocess
            result = subprocess.check_output(['uptime'], text=True).strip()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统运行时间: {result}",
                data={
                    'uptime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统运行时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uptime'}


class SystemLoadAverageAction(BaseAction):
    """Get load average."""
    action_type = "system3_load_average"
    display_name = "系统负载"
    description = "获取系统负载平均值"

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
            load = os.getloadavg()
            result = {
                '1min': load[0],
                '5min': load[1],
                '15min': load[2]
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统负载: {load[0]:.2f}, {load[1]:.2f}, {load[2]:.2f}",
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
        return {'output_var': 'load_average'}


class SystemDiskUsageAction(BaseAction):
    """Get disk usage."""
    action_type = "system3_disk_usage"
    display_name = "磁盘使用"
    description = "获取磁盘使用情况"

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
            resolved = context.resolve_value(path) if path else '/'
            usage = shutil.disk_usage(resolved)
            result = {
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': (usage.used / usage.total) * 100
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"磁盘使用: {result['percent']:.1f}%",
                data={
                    'path': resolved,
                    'disk_usage': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘使用情况失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '/', 'output_var': 'disk_usage'}


class SystemBootTimeAction(BaseAction):
    """Get boot time."""
    action_type = "system3_boot_time"
    display_name = "启动时间"
    description = "获取系统启动时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute boot time.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with boot time.
        """
        output_var = params.get('output_var', 'boot_time')

        try:
            import subprocess
            result = subprocess.check_output(['sysctl', '-n', 'kern.boottime'], text=True).strip()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"启动时间: {result}",
                data={
                    'boot_time': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取启动时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'boot_time'}


class SystemUserAction(BaseAction):
    """Get current user."""
    action_type = "system3_user"
    display_name = "当前用户"
    description = "获取当前用户名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute user.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current user.
        """
        output_var = params.get('output_var', 'current_user')

        try:
            import getpass
            result = getpass.getuser()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前用户: {result}",
                data={
                    'user': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前用户失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'current_user'}
