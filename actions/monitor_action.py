"""Monitor action module for RabAI AutoClick.

Provides system and application monitoring actions including
CPU, memory, disk usage, process monitoring, and custom metrics.
"""

import time
import psutil
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricSnapshot:
    """A snapshot of system metrics at a point in time.
    
    Attributes:
        timestamp: Unix timestamp of the snapshot.
        cpu_percent: CPU usage percentage.
        memory_percent: Memory usage percentage.
        disk_percent: Disk usage percentage.
        network_sent: Bytes sent.
        network_recv: Bytes received.
    """
    timestamp: float
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    network_sent: int
    network_recv: int


class MetricsBuffer:
    """Thread-safe circular buffer for storing metric snapshots.
    
    Automatically evicts oldest entries when max_size is reached.
    """
    
    def __init__(self, max_size: int = 1000):
        """Initialize metrics buffer.
        
        Args:
            max_size: Maximum number of snapshots to store.
        """
        self.max_size = max_size
        self._snapshots: List[MetricSnapshot] = []
        self._lock = threading.Lock()
        self._start_time = time.time()
    
    def add(self, snapshot: MetricSnapshot) -> None:
        """Add a snapshot to the buffer.
        
        Args:
            snapshot: MetricSnapshot to add.
        """
        with self._lock:
            if len(self._snapshots) >= self.max_size:
                self._snapshots.pop(0)
            self._snapshots.append(snapshot)
    
    def get_recent(self, count: int = 10) -> List[MetricSnapshot]:
        """Get the most recent snapshots.
        
        Args:
            count: Number of snapshots to retrieve.
        
        Returns:
            List of recent MetricSnapshot objects.
        """
        with self._lock:
            return self._snapshots[-count:] if self._snapshots else []
    
    def get_all(self) -> List[MetricSnapshot]:
        """Get all stored snapshots."""
        with self._lock:
            return list(self._snapshots)
    
    def get_range(self, start_time: float, end_time: float) -> List[MetricSnapshot]:
        """Get snapshots within a time range.
        
        Args:
            start_time: Start of time range (unix timestamp).
            end_time: End of time range (unix timestamp).
        
        Returns:
            List of snapshots in the range.
        """
        with self._lock:
            return [s for s in self._snapshots if start_time <= s.timestamp <= end_time]
    
    def clear(self) -> int:
        """Clear all snapshots.
        
        Returns:
            Number of snapshots cleared.
        """
        with self._lock:
            count = len(self._snapshots)
            self._snapshots.clear()
            return count
    
    def get_stats(self) -> Dict[str, float]:
        """Calculate statistics from stored snapshots.
        
        Returns:
            Dict with min, max, avg for each metric.
        """
        with self._lock:
            if not self._snapshots:
                return {}
            
            cpu_vals = [s.cpu_percent for s in self._snapshots]
            mem_vals = [s.memory_percent for s in self._snapshots]
            disk_vals = [s.disk_percent for s in self._snapshots]
            
            return {
                "cpu_min": min(cpu_vals),
                "cpu_max": max(cpu_vals),
                "cpu_avg": sum(cpu_vals) / len(cpu_vals),
                "memory_min": min(mem_vals),
                "memory_max": max(mem_vals),
                "memory_avg": sum(mem_vals) / len(mem_vals),
                "disk_min": min(disk_vals),
                "disk_max": max(disk_vals),
                "disk_avg": sum(disk_vals) / len(disk_vals),
                "count": len(self._snapshots),
                "duration": self._snapshots[-1].timestamp - self._snapshots[0].timestamp if len(self._snapshots) > 1 else 0
            }


# Global metrics storage
_metrics_buffers: Dict[str, MetricsBuffer] = {}
_metrics_lock = threading.Lock()


class SystemMetricsAction(BaseAction):
    """Capture current system resource metrics.
    
    Collects CPU, memory, disk, and network usage.
    """
    action_type = "system_metrics"
    display_name = "系统指标"
    description = "采集系统资源指标"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture system metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: buffer_name, store_snapshot.
        
        Returns:
            ActionResult with current system metrics.
        """
        buffer_name = params.get('buffer_name', 'default')
        store_snapshot = params.get('store_snapshot', False)
        
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net = psutil.net_io_counters()
            
            snapshot = MetricSnapshot(
                timestamp=time.time(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                disk_percent=disk.percent,
                network_sent=net.bytes_sent,
                network_recv=net.bytes_recv
            )
            
            if store_snapshot:
                with _metrics_lock:
                    if buffer_name not in _metrics_buffers:
                        _metrics_buffers[buffer_name] = MetricsBuffer()
                    _metrics_buffers[buffer_name].add(snapshot)
            
            return ActionResult(
                success=True,
                message="System metrics captured",
                data={
                    "cpu_percent": cpu_percent,
                    "memory_percent": round(memory.percent, 2),
                    "memory_available_gb": round(memory.available / (1024**3), 2),
                    "disk_percent": round(disk.percent, 2),
                    "disk_free_gb": round(disk.free / (1024**3), 2),
                    "network_sent_mb": round(net.bytes_sent / (1024**2), 2),
                    "network_recv_mb": round(net.bytes_recv / (1024**2), 2),
                    "timestamp": snapshot.timestamp
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to capture metrics: {str(e)}")


class ProcessMetricsAction(BaseAction):
    """Get metrics for a specific process or the current process."""
    action_type = "process_metrics"
    display_name = "进程指标"
    description = "获取进程资源使用"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get process metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: pid (optional, defaults to current).
        
        Returns:
            ActionResult with process metrics.
        """
        pid = params.get('pid', os.getpid())
        
        try:
            if isinstance(pid, str) and pid.lower() == 'current':
                pid = os.getpid()
            
            proc = psutil.Process(pid)
            
            with proc.oneshot():
                cpu_percent = proc.cpu_percent(interval=0.1)
                memory_info = proc.memory_info()
                num_threads = proc.num_threads()
                status = proc.status()
                create_time = proc.create_time()
                
                return ActionResult(
                    success=True,
                    message=f"Metrics for process {pid}",
                    data={
                        "pid": pid,
                        "name": proc.name(),
                        "cpu_percent": cpu_percent,
                        "memory_rss_mb": round(memory_info.rss / (1024**2), 2),
                        "memory_vms_mb": round(memory_info.vms / (1024**2), 2),
                        "num_threads": num_threads,
                        "status": status,
                        "create_time": create_time,
                        "uptime_seconds": time.time() - create_time
                    }
                )
        except psutil.NoSuchProcess:
            return ActionResult(success=False, message=f"Process {pid} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get process metrics: {str(e)}")


class MetricsHistoryAction(BaseAction):
    """Retrieve stored metric history from a buffer."""
    action_type = "metrics_history"
    display_name = "指标历史"
    description = "查询历史指标数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get historical metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: buffer_name, count, start_time, end_time.
        
        Returns:
            ActionResult with historical metrics and statistics.
        """
        buffer_name = params.get('buffer_name', 'default')
        count = params.get('count', 10)
        start_time = params.get('start_time', None)
        end_time = params.get('end_time', None)
        
        with _metrics_lock:
            if buffer_name not in _metrics_buffers:
                return ActionResult(
                    success=True,
                    message=f"No metrics buffer named {buffer_name}",
                    data={"snapshots": [], "stats": {}}
                )
            buffer = _metrics_buffers[buffer_name]
        
        if start_time is not None and end_time is not None:
            snapshots = buffer.get_range(start_time, end_time)
        else:
            snapshots = buffer.get_recent(count)
        
        stats = buffer.get_stats()
        
        snapshot_data = [
            {
                "timestamp": s.timestamp,
                "cpu_percent": s.cpu_percent,
                "memory_percent": s.memory_percent,
                "disk_percent": s.disk_percent
            }
            for s in snapshots
        ]
        
        return ActionResult(
            success=True,
            message=f"Retrieved {len(snapshots)} snapshots from {buffer_name}",
            data={"snapshots": snapshot_data, "stats": stats}
        )


class DiskUsageAction(BaseAction):
    """Get disk usage for a specific path or mount point."""
    action_type = "disk_usage"
    display_name = "磁盘使用"
    description = "查看磁盘空间使用情况"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get disk usage for a path.
        
        Args:
            context: Execution context.
            params: Dict with keys: path (default: '/').
        
        Returns:
            ActionResult with disk usage statistics.
        """
        path = params.get('path', '/')
        
        try:
            usage = psutil.disk_usage(path)
            
            return ActionResult(
                success=True,
                message=f"Disk usage for {path}",
                data={
                    "path": path,
                    "total_gb": round(usage.total / (1024**3), 2),
                    "used_gb": round(usage.used / (1024**3), 2),
                    "free_gb": round(usage.free / (1024**3), 2),
                    "percent": usage.percent
                }
            )
        except FileNotFoundError:
            return ActionResult(success=False, message=f"Path {path} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get disk usage: {str(e)}")


class MetricsBufferClearAction(BaseAction):
    """Clear a metrics buffer."""
    action_type = "metrics_buffer_clear"
    display_name = "清除指标缓冲"
    description = "清空指标历史数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear a metrics buffer.
        
        Args:
            context: Execution context.
            params: Dict with keys: buffer_name.
        
        Returns:
            ActionResult with number of entries cleared.
        """
        buffer_name = params.get('buffer_name', 'default')
        
        with _metrics_lock:
            if buffer_name not in _metrics_buffers:
                return ActionResult(success=True, message=f"Buffer {buffer_name} does not exist", data={"cleared": 0})
            buffer = _metrics_buffers[buffer_name]
        
        cleared = buffer.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared {cleared} entries from {buffer_name}",
            data={"buffer_name": buffer_name, "cleared": cleared}
        )
