"""System monitoring action module for RabAI AutoClick.

Provides system monitoring operations:
- CpuMonitorAction: Monitor CPU usage
- MemoryMonitorAction: Monitor memory usage
- DiskMonitorAction: Monitor disk usage
- NetworkMonitorAction: Monitor network stats
- ProcessListAction: List running processes
- SystemInfoAction: Get system information
- UptimeAction: Get system uptime
- LoadAverageAction: Get load average
"""

from __future__ import annotations

import os
import time
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CpuMonitorAction(BaseAction):
    """Monitor CPU usage."""
    action_type = "system_cpu_monitor"
    display_name = "CPU监控"
    description = "监控CPU使用率"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CPU monitoring."""
        interval = params.get('interval', 1.0)
        output_var = params.get('output_var', 'cpu_stats')

        try:
            resolved_interval = context.resolve_value(interval) if context else interval

            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['top', '-l', '1', '-n', '0'], capture_output=True, text=True, timeout=5)
                output = result.stdout
                cpu_pct = None
                for line in output.split('\n'):
                    if 'CPU usage' in line or 'CPU' in line:
                        parts = line.split()
                        for i, p in enumerate(parts):
                            if p.replace('.', '').replace('%', '').isdigit() and '%' in parts[i]:
                                cpu_pct = float(parts[i].replace('%', ''))
                                break
                        break

                if cpu_pct is None:
                    return ActionResult(success=False, message="Could not parse CPU usage")
                stats = {'cpu_percent': cpu_pct, 'timestamp': time.time()}
            else:
                # Linux /proc/stat approach
                with open('/proc/stat', 'r') as f:
                    line = f.readline()
                fields = line.split()
                idle1 = int(fields[4])
                total1 = sum(int(f) for f in fields[1:8])
                time.sleep(resolved_interval)
                with open('/proc/stat', 'r') as f:
                    line = f.readline()
                fields = line.split()
                idle2 = int(fields[4])
                total2 = sum(int(f) for f in fields[1:8])

                idle_delta = idle2 - idle1
                total_delta = total2 - total1
                cpu_pct = 100.0 * (total_delta - idle_delta) / total_delta if total_delta > 0 else 0
                stats = {'cpu_percent': round(cpu_pct, 2), 'timestamp': time.time()}

            if context:
                context.set(output_var, stats)
            return ActionResult(success=True, message=f"CPU: {stats['cpu_percent']}%", data=stats)
        except Exception as e:
            return ActionResult(success=False, message=f"CPU monitor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'interval': 1.0, 'output_var': 'cpu_stats'}


class MemoryMonitorAction(BaseAction):
    """Monitor memory usage."""
    action_type = "system_memory_monitor"
    display_name = "内存监控"
    description = "监控内存使用情况"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute memory monitoring."""
        output_var = params.get('output_var', 'memory_stats')

        try:
            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['vm_stat'], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().split('\n')
                stats = {'raw': result.stdout}
            else:
                with open('/proc/meminfo', 'r') as f:
                    lines = f.readlines()

            mem_info = {}
            for line in lines:
                if ':' in line:
                    key, val = line.split(':', 1)
                    parts = val.strip().split()
                    if len(parts) >= 1:
                        mem_info[key.strip()] = int(parts[0])  # kB

            if sys.platform == 'darwin':
                # Parse vm_stat output
                pagesize = 4096
                free = 0
                active = 0
                inactive = 0
                wired = 0
                for line in lines:
                    if 'Pages free' in line:
                        free = int(line.split()[-1].rstrip('.')) * pagesize / (1024 * 1024)
                    elif 'Pages active' in line:
                        active = int(line.split()[-1].rstrip('.')) * pagesize / (1024 * 1024)
                    elif 'Pages inactive' in line:
                        inactive = int(line.split()[-1].rstrip('.')) * pagesize / (1024 * 1024)
                    elif 'Pages wired' in line or 'Pages purgeable' in line:
                        wired = int(line.split()[-1].rstrip('.')) * pagesize / (1024 * 1024)

                total_est = free + active + inactive + wired
                stats = {
                    'total_mb': round(total_est, 2),
                    'free_mb': round(free, 2),
                    'active_mb': round(active, 2),
                    'inactive_mb': round(inactive, 2),
                    'wired_mb': round(wired, 2),
                    'used_percent': round((1 - free / total_est) * 100, 2) if total_est > 0 else 0,
                }
            else:
                total = mem_info.get('MemTotal', 0) / 1024  # MB
                free = mem_info.get('MemFree', 0) / 1024
                available = mem_info.get('MemAvailable', free) / 1024
                stats = {
                    'total_mb': round(total, 2),
                    'free_mb': round(free, 2),
                    'available_mb': round(available, 2),
                    'used_mb': round(total - available, 2),
                    'used_percent': round((1 - available / total) * 100, 2) if total > 0 else 0,
                }

            if context:
                context.set(output_var, stats)
            return ActionResult(success=True, message=f"Memory: {stats.get('used_percent', 'N/A')}% used", data=stats)
        except Exception as e:
            return ActionResult(success=False, message=f"Memory monitor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'memory_stats'}


class DiskMonitorAction(BaseAction):
    """Monitor disk usage."""
    action_type = "system_disk_monitor"
    display_name = "磁盘监控"
    description = "监控磁盘使用情况"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute disk monitoring."""
        path = params.get('path', '/')
        output_var = params.get('output_var', 'disk_stats')

        try:
            resolved_path = context.resolve_value(path) if context else path

            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['df', '-k', resolved_path], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().split('\n')
                if len(lines) < 2:
                    return ActionResult(success=False, message="Could not read disk info")
                parts = lines[1].split()
                total = int(parts[1]) / 1024  # MB
                used = int(parts[2]) / 1024
                available = int(parts[3]) / 1024
                percent = parts[4].replace('%', '')
            else:
                import subprocess
                result = subprocess.run(['df', '-k', resolved_path], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().split('\n')
                if len(lines) < 2:
                    return ActionResult(success=False, message="Could not read disk info")
                parts = lines[1].split()
                total = int(parts[1]) / 1024
                used = int(parts[2]) / 1024
                available = int(parts[3]) / 1024
                percent = parts[4].replace('%', '')

            stats = {
                'path': resolved_path,
                'total_mb': round(total, 2),
                'used_mb': round(used, 2),
                'available_mb': round(available, 2),
                'used_percent': float(percent),
            }

            if context:
                context.set(output_var, stats)
            return ActionResult(success=True, message=f"Disk {resolved_path}: {percent}% used", data=stats)
        except Exception as e:
            return ActionResult(success=False, message=f"Disk monitor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '/', 'output_var': 'disk_stats'}


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "system_process_list"
    display_name = "进程列表"
    description = "列出运行中的进程"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute process list."""
        filter_name = params.get('filter', '')
        limit = params.get('limit', 50)
        output_var = params.get('output_var', 'process_list')

        try:
            resolved_filter = context.resolve_value(filter_name) if context else filter_name
            resolved_limit = context.resolve_value(limit) if context else limit

            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=5)
                lines = result.stdout.strip().split('\n')
                header = lines[0]
                processes = []
                for line in lines[1:]:
                    parts = line.split(None, 10)
                    if len(parts) >= 11:
                        proc = {
                            'user': parts[0],
                            'pid': int(parts[1]),
                            'cpu': float(parts[2]),
                            'mem': float(parts[3]),
                            'command': parts[10] if len(parts) > 10 else '',
                        }
                        if not resolved_filter or resolved_filter.lower() in proc['command'].lower():
                            processes.append(proc)
            else:
                with open('/proc/processes', 'r') as f:
                    lines = f.readlines()

            processes = sorted(processes, key=lambda p: p['cpu'], reverse=True)[:resolved_limit]

            if context:
                context.set(output_var, processes)
            return ActionResult(success=True, message=f"Found {len(processes)} processes", data={'processes': processes[:resolved_limit]})
        except Exception as e:
            return ActionResult(success=False, message=f"Process list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filter': '', 'limit': 50, 'output_var': 'process_list'}


class SystemInfoAction(BaseAction):
    """Get system information."""
    action_type = "system_info"
    display_name = "系统信息"
    description = "获取系统信息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute system info."""
        output_var = params.get('output_var', 'system_info')

        try:
            import platform
            info = {
                'system': platform.system(),
                'release': platform.release(),
                'version': platform.version(),
                'machine': platform.machine(),
                'processor': platform.processor(),
                'python_version': platform.python_version(),
                'hostname': platform.node(),
            }

            if context:
                context.set(output_var, info)
            return ActionResult(success=True, message=f"System: {info['system']} {info['release']}", data=info)
        except Exception as e:
            return ActionResult(success=False, message=f"System info error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'system_info'}


class UptimeAction(BaseAction):
    """Get system uptime."""
    action_type = "system_uptime"
    display_name = "系统运行时间"
    description = "获取系统运行时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute uptime."""
        output_var = params.get('output_var', 'uptime')

        try:
            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['uptime'], capture_output=True, text=True, timeout=5)
                uptime_str = result.stdout.strip()
                return ActionResult(success=True, message=uptime_str, data={'uptime_string': uptime_str})
            else:
                with open('/proc/uptime', 'r') as f:
                    uptime_seconds = float(f.readline().split()[0])
                hours = int(uptime_seconds // 3600)
                minutes = int((uptime_seconds % 3600) // 60)
                uptime_str = f"{hours}h {minutes}m"
                result = {'uptime_seconds': uptime_seconds, 'uptime_string': uptime_str}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Uptime: {uptime_str}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Uptime error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uptime'}


class LoadAverageAction(BaseAction):
    """Get system load average."""
    action_type = "system_load_average"
    display_name = "系统负载"
    description = "获取系统负载平均值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute load average."""
        output_var = params.get('output_var', 'load_average')

        try:
            if sys.platform == 'darwin':
                import subprocess
                result = subprocess.run(['uptime'], capture_output=True, text=True, timeout=5)
                output = result.stdout
                # Parse "load averages: X.XX X.XX X.XX"
                for part in output.split(','):
                    if 'load' in part.lower() or 'averages' in part.lower():
                        pass
                loads = None
                for segment in output.split(':'):
                    if 'load' in segment.lower():
                        loads = segment.strip().split()[:3]
                        break
                if not loads:
                    import os
                    load1, load5, load15 = os.getloadavg()
                    loads = [load1, load5, load15]
            else:
                import os
                load1, load5, load15 = os.getloadavg()
                loads = [load1, load5, load15]

            result = {
                'load_1min': round(loads[0], 2),
                'load_5min': round(loads[1], 2),
                'load_15min': round(loads[2], 2),
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Load: {loads[0]:.2f} {loads[1]:.2f} {loads[2]:.2f}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Load average error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'load_average'}
