"""System2 action module for RabAI AutoClick.

Provides additional system operations:
- SystemInfoAction: Get system information
- SystemPlatformAction: Get platform info
- SystemCpuCountAction: Get CPU count
- SystemMemoryAction: Get memory info
- SystemHostnameAction: Get hostname
"""

import platform
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SystemInfoAction(BaseAction):
    """Get system information."""
    action_type = "system_info"
    display_name = "获取系统信息"
    description = "获取系统信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute system info.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with system info.
        """
        output_var = params.get('output_var', 'system_info')

        try:
            result = {
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor(),
                'python_version': platform.python_version(),
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"系统信息: {platform.system()}",
                data={
                    'info': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取系统信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'system_info'}


class SystemPlatformAction(BaseAction):
    """Get platform info."""
    action_type = "system_platform"
    display_name = "获取平台信息"
    description = "获取平台信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute platform info.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with platform info.
        """
        output_var = params.get('output_var', 'platform_info')

        try:
            result = {
                'system': platform.system(),
                'release': platform.release(),
                'machine': platform.machine(),
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平台: {platform.system()}",
                data={
                    'info': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取平台信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'platform_info'}


class SystemCpuCountAction(BaseAction):
    """Get CPU count."""
    action_type = "system_cpu_count"
    display_name = "获取CPU数量"
    description = "获取CPU数量"

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
            resolved_logical = context.resolve_value(logical) if logical else True

            if resolved_logical:
                count = os.cpu_count()
            else:
                count = os.cpu_count()

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"CPU数量: {count}",
                data={
                    'count': count,
                    'logical': resolved_logical,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CPU数量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'logical': True, 'output_var': 'cpu_count'}


class SystemMemoryAction(BaseAction):
    """Get memory info."""
    action_type = "system_memory"
    display_name = "获取内存信息"
    description = "获取内存信息"

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
                'total_gb': round(mem.total / (1024**3), 2),
                'available_gb': round(mem.available / (1024**3), 2),
                'used_gb': round(mem.used / (1024**3), 2),
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"内存: {result['used_gb']}GB / {result['total_gb']}GB",
                data={
                    'info': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要安装 psutil 库"
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


class SystemHostnameAction(BaseAction):
    """Get hostname."""
    action_type = "system_hostname"
    display_name = "获取主机名"
    description = "获取主机名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hostname.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with hostname.
        """
        output_var = params.get('output_var', 'hostname')

        try:
            result = platform.node()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"主机名: {result}",
                data={
                    'hostname': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取主机名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hostname'}