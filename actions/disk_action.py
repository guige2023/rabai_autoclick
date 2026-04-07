"""Disk action module for RabAI AutoClick.

Provides disk operations:
- DiskUsageAction: Get disk usage
- DiskSpaceAction: Get disk space
- DiskPartitionsAction: Get disk partitions
- DiskIoCountersAction: Get disk I/O counters
- DiskPathExistsAction: Check if path exists
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DiskUsageAction(BaseAction):
    """Get disk usage."""
    action_type = "disk_usage"
    display_name = "磁盘使用情况"
    description = "获取磁盘使用情况"
    version = "1.0"

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

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import psutil
            resolved_path = context.resolve_value(path)
            usage = psutil.disk_usage(resolved_path)

            result = {
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': usage.percent,
                'total_gb': usage.total / (1024**3),
                'used_gb': usage.used / (1024**3),
                'free_gb': usage.free / (1024**3)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"磁盘使用情况: {result['used_gb']:.2f}GB / {result['total_gb']:.2f}GB ({result['percent']:.1f}%)",
                data={
                    'path': resolved_path,
                    'usage': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取磁盘使用情况失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘使用情况失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'disk_usage'}


class DiskSpaceAction(BaseAction):
    """Get disk space."""
    action_type = "disk_space"
    display_name = "磁盘空间"
    description = "获取磁盘空间信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute disk space.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with disk space.
        """
        output_var = params.get('output_var', 'disk_space')

        try:
            import psutil
            partitions = psutil.disk_partitions()

            result = []
            for partition in partitions:
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    result.append({
                        'device': partition.device,
                        'mountpoint': partition.mountpoint,
                        'fstype': partition.fstype,
                        'total': usage.total,
                        'used': usage.used,
                        'free': usage.free,
                        'percent': usage.percent
                    })
                except:
                    pass

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"磁盘空间: {len(result)} 个分区",
                data={
                    'partitions': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取磁盘空间失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘空间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'disk_space'}


class DiskPartitionsAction(BaseAction):
    """Get disk partitions."""
    action_type = "disk_partitions"
    display_name = "磁盘分区"
    description = "获取所有磁盘分区"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute disk partitions.

        Args:
            context: Execution context.
            params: Dict with all, output_var.

        Returns:
            ActionResult with partitions.
        """
        all_var = params.get('all', False)
        output_var = params.get('output_var', 'partitions_result')

        try:
            import psutil
            resolved_all = bool(context.resolve_value(all_var)) if all_var else False
            partitions = psutil.disk_partitions(all=resolved_all)

            result = [{
                'device': p.device,
                'mountpoint': p.mountpoint,
                'fstype': p.fstype,
                'opts': p.opts
            } for p in partitions]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"磁盘分区: {len(result)} 个",
                data={
                    'partitions': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取磁盘分区失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘分区失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'all': False, 'output_var': 'partitions_result'}


class DiskIoCountersAction(BaseAction):
    """Get disk I/O counters."""
    action_type = "disk_io_counters"
    display_name = "磁盘IO统计"
    description = "获取磁盘IO统计信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute disk I/O counters.

        Args:
            context: Execution context.
            params: Dict with perdisk, output_var.

        Returns:
            ActionResult with I/O counters.
        """
        perdisk = params.get('perdisk', False)
        output_var = params.get('output_var', 'io_counters')

        try:
            import psutil
            resolved_perdisk = bool(context.resolve_value(perdisk)) if perdisk else False
            io_counters = psutil.disk_io_counters(perdisk=resolved_perdisk)

            if resolved_perdisk:
                result = {}
                for disk, counters in io_counters.items():
                    result[disk] = {
                        'read_count': counters.read_count,
                        'write_count': counters.write_count,
                        'read_bytes': counters.read_bytes,
                        'write_bytes': counters.write_bytes,
                        'read_time': counters.read_time,
                        'write_time': counters.write_time
                    }
            else:
                result = {
                    'read_count': io_counters.read_count,
                    'write_count': io_counters.write_count,
                    'read_bytes': io_counters.read_bytes,
                    'write_bytes': io_counters.write_bytes,
                    'read_time': io_counters.read_time,
                    'write_time': io_counters.write_time
                }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"磁盘IO统计",
                data={
                    'io_counters': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取磁盘IO统计失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘IO统计失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'perdisk': False, 'output_var': 'io_counters'}


class DiskPathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "disk_path_exists"
    display_name = "路径存在检查"
    description = "检查路径是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path exists check.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            exists = os.path.exists(resolved_path)
            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"路径存在检查: {'存在' if exists else '不存在'}",
                data={
                    'path': resolved_path,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}