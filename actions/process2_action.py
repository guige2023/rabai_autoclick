"""Process2 action module for RabAI AutoClick.

Provides additional process operations:
- ProcessListAction: List running processes
- ProcessKillAction: Kill a process
- ProcessExistsAction: Check if process exists
- ProcessCpuUsageAction: Get CPU usage
- ProcessMemoryUsageAction: Get memory usage
"""

import psutil
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "process2_list"
    display_name = "列出进程"
    description = "列出所有运行中的进程"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list processes.

        Args:
            context: Execution context.
            params: Dict with filter, output_var.

        Returns:
            ActionResult with process list.
        """
        filter_str = params.get('filter', None)
        output_var = params.get('output_var', 'process_list')

        try:
            resolved_filter = context.resolve_value(filter_str) if filter_str else None

            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    info = proc.info
                    if resolved_filter:
                        if resolved_filter.lower() in info['name'].lower():
                            processes.append(info)
                    else:
                        processes.append(info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            context.set(output_var, processes)

            return ActionResult(
                success=True,
                message=f"进程列表: {len(processes)} 个",
                data={
                    'processes': processes,
                    'count': len(processes),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 psutil 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filter': None, 'output_var': 'process_list'}


class ProcessKillAction(BaseAction):
    """Kill a process."""
    action_type = "process2_kill"
    display_name = "终止进程"
    description = "终止指定进程"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute kill process.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with kill result.
        """
        pid = params.get('pid', 0)
        output_var = params.get('output_var', 'kill_result')

        try:
            resolved_pid = int(context.resolve_value(pid))

            process = psutil.Process(resolved_pid)
            process.terminate()
            process.wait(timeout=5)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"进程已终止: PID {resolved_pid}",
                data={
                    'pid': resolved_pid,
                    'output_var': output_var
                }
            )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"进程不存在: PID {resolved_pid}"
            )
        except psutil.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"进程终止超时: PID {resolved_pid}"
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 psutil 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"终止进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'kill_result'}


class ProcessExistsAction(BaseAction):
    """Check if process exists."""
    action_type = "process2_exists"
    display_name = "检查进程"
    description = "检查进程是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check process.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with check result.
        """
        pid = params.get('pid', 0)
        output_var = params.get('output_var', 'process_exists')

        try:
            resolved_pid = int(context.resolve_value(pid))

            exists = psutil.pid_exists(resolved_pid)
            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"进程{'存在' if exists else '不存在'}: PID {resolved_pid}",
                data={
                    'pid': resolved_pid,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 psutil 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查进程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_exists'}


class ProcessCpuUsageAction(BaseAction):
    """Get CPU usage."""
    action_type = "process2_cpu_usage"
    display_name = "进程CPU使用率"
    description = "获取进程CPU使用率"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CPU usage.

        Args:
            context: Execution context.
            params: Dict with pid, interval, output_var.

        Returns:
            ActionResult with CPU usage.
        """
        pid = params.get('pid', None)
        interval = params.get('interval', 1.0)
        output_var = params.get('output_var', 'cpu_usage')

        try:
            import psutil

            if pid is None:
                cpu_percent = psutil.cpu_percent(interval=float(context.resolve_value(interval)))
                context.set(output_var, cpu_percent)

                return ActionResult(
                    success=True,
                    message=f"系统CPU使用率: {cpu_percent}%",
                    data={
                        'cpu_percent': cpu_percent,
                        'output_var': output_var
                    }
                )
            else:
                resolved_pid = int(context.resolve_value(pid))
                resolved_interval = float(context.resolve_value(interval))

                process = psutil.Process(resolved_pid)
                cpu_percent = process.cpu_percent(interval=resolved_interval)

                context.set(output_var, cpu_percent)

                return ActionResult(
                    success=True,
                    message=f"进程CPU使用率: {cpu_percent}%",
                    data={
                        'pid': resolved_pid,
                        'cpu_percent': cpu_percent,
                        'output_var': output_var
                    }
                )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"进程不存在: PID {pid}"
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 psutil 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CPU使用率失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'interval': 1.0, 'output_var': 'cpu_usage'}


class ProcessMemoryUsageAction(BaseAction):
    """Get memory usage."""
    action_type = "process2_memory_usage"
    display_name = "进程内存使用"
    description = "获取进程内存使用情况"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute memory usage.

        Args:
            context: Execution context.
            params: Dict with pid, output_var.

        Returns:
            ActionResult with memory usage.
        """
        pid = params.get('pid', None)
        output_var = params.get('output_var', 'memory_usage')

        try:
            if pid is None:
                vm = psutil.virtual_memory()
                context.set(output_var, vm.percent)

                return ActionResult(
                    success=True,
                    message=f"系统内存使用率: {vm.percent}%",
                    data={
                        'memory_percent': vm.percent,
                        'total': vm.total,
                        'available': vm.available,
                        'used': vm.used,
                        'output_var': output_var
                    }
                )
            else:
                resolved_pid = int(context.resolve_value(pid))

                process = psutil.Process(resolved_pid)
                mem_info = process.memory_info()
                mem_percent = process.memory_percent()

                result = {
                    'rss': mem_info.rss,
                    'vms': mem_info.vms,
                    'percent': mem_percent,
                }

                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"进程内存使用: {mem_percent:.2f}%",
                    data={
                        'pid': resolved_pid,
                        'memory': result,
                        'output_var': output_var
                    }
                )
        except psutil.NoSuchProcess:
            return ActionResult(
                success=False,
                message=f"进程不存在: PID {pid}"
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 psutil 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取内存使用失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pid': None, 'output_var': 'memory_usage'}