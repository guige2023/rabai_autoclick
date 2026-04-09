"""
Resource Management Utilities for UI Automation.

This module provides utilities for managing system resources,
monitoring resource usage, and handling resource constraints.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import gc
import os
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable


class ResourceType(Enum):
    """Types of system resources."""
    CPU = auto()
    MEMORY = auto()
    DISK = auto()
    NETWORK = auto()


@dataclass
class ResourceUsage:
    """
    Resource usage snapshot.
    
    Attributes:
        timestamp: Snapshot timestamp
        cpu_percent: CPU usage percentage
        memory_used_bytes: Memory used in bytes
        memory_total_bytes: Total memory in bytes
        disk_used_bytes: Disk used in bytes
        disk_free_bytes: Free disk in bytes
    """
    timestamp: float
    cpu_percent: float = 0.0
    memory_used_bytes: int = 0
    memory_total_bytes: int = 0
    disk_used_bytes: int = 0
    disk_free_bytes: int = 0
    
    @property
    def memory_percent(self) -> float:
        """Get memory usage percentage."""
        if self.memory_total_bytes > 0:
            return (self.memory_used_bytes / self.memory_total_bytes) * 100
        return 0.0
    
    @property
    def memory_available_bytes(self) -> int:
        """Get available memory."""
        return self.memory_total_bytes - self.memory_used_bytes


class ResourceMonitor:
    """
    Monitors system resource usage.
    
    Example:
        monitor = ResourceMonitor()
        usage = monitor.get_usage()
        print(f"CPU: {usage.cpu_percent}%, Memory: {usage.memory_percent}%")
    """
    
    def __init__(self):
        self._usage_history: list[ResourceUsage] = []
        self._max_history = 1000
    
    def get_usage(self) -> ResourceUsage:
        """
        Get current resource usage.
        
        Returns:
            ResourceUsage snapshot
        """
        import psutil
        
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            vm = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            usage = ResourceUsage(
                timestamp=time.time(),
                cpu_percent=process.cpu_percent(interval=0.1),
                memory_used_bytes=memory_info.rss,
                memory_total_bytes=vm.total,
                disk_used_bytes=disk.used,
                disk_free_bytes=disk.free
            )
            
            self._add_to_history(usage)
            
            return usage
        except ImportError:
            # Fallback if psutil not available
            return ResourceUsage(timestamp=time.time())
    
    def _add_to_history(self, usage: ResourceUsage) -> None:
        """Add usage to history."""
        self._usage_history.append(usage)
        if len(self._usage_history) > self._max_history:
            self._usage_history.pop(0)
    
    def get_history(
        self,
        limit: Optional[int] = None
    ) -> list[ResourceUsage]:
        """Get resource usage history."""
        if limit:
            return self._usage_history[-limit:]
        return list(self._usage_history)
    
    @property
    def peak_memory_bytes(self) -> int:
        """Get peak memory usage."""
        if not self._usage_history:
            return 0
        return max(u.memory_used_bytes for u in self._usage_history)
    
    @property
    def peak_cpu_percent(self) -> float:
        """Get peak CPU usage."""
        if not self._usage_history:
            return 0.0
        return max(u.cpu_percent for u in self._usage_history)


class ResourceGuard:
    """
    Guard for resource constraints.
    
    Example:
        with ResourceGuard(max_memory_mb=512) as guard:
            do_work()
    """
    
    def __init__(
        self,
        max_memory_mb: Optional[float] = None,
        max_cpu_percent: Optional[float] = None,
        check_interval: float = 1.0
    ):
        self.max_memory_bytes = int(max_memory_mb * 1024 * 1024) if max_memory_mb else None
        self.max_cpu_percent = max_cpu_percent
        self.check_interval = check_interval
        self._monitor = ResourceMonitor()
        self._exceeded = False
        self._exceeded_reason: Optional[str] = None
    
    def check(self) -> bool:
        """
        Check if resource limits are exceeded.
        
        Returns:
            True if within limits, False if exceeded
        """
        usage = self._monitor.get_usage()
        
        if self.max_memory_bytes and usage.memory_used_bytes > self.max_memory_bytes:
            self._exceeded = True
            self._exceeded_reason = f"Memory: {usage.memory_used_bytes / (1024*1024):.1f}MB > {self.max_memory_bytes / (1024*1024):.1f}MB"
            return False
        
        if self.max_cpu_percent and usage.cpu_percent > self.max_cpu_percent:
            self._exceeded = True
            self._exceeded_reason = f"CPU: {usage.cpu_percent:.1f}% > {self.max_cpu_percent:.1f}%"
            return False
        
        return True
    
    def wait_until_available(self, timeout: Optional[float] = None) -> bool:
        """
        Wait until resources are available.
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            True if resources available, False on timeout
        """
        start_time = time.time()
        
        while True:
            if self.check():
                return True
            
            if timeout and (time.time() - start_time) >= timeout:
                return False
            
            time.sleep(self.check_interval)
    
    @property
    def is_exceeded(self) -> bool:
        """Check if limits were exceeded."""
        return self._exceeded
    
    @property
    def exceeded_reason(self) -> Optional[str]:
        """Get reason for exceeding limits."""
        return self._exceeded_reason
    
    def __enter__(self) -> 'ResourceGuard':
        if not self.check():
            raise ResourceExhaustedError(self.exceeded_reason or "Resource limit exceeded")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


class ResourceExhaustedError(Exception):
    """Raised when resources are exhausted."""
    pass


class MemoryPool:
    """
    Object pool for reusing memory allocations.
    
    Example:
        pool = MemoryPool(factory=lambda: MyObject())
        obj = pool.acquire()
        pool.release(obj)
    """
    
    def __init__(
        self,
        factory: Callable[[], Any],
        max_size: int = 100
    ):
        self.factory = factory
        self.max_size = max_size
        self._pool: list[Any] = []
        self._lock = __import__('threading').Lock()
    
    def acquire(self) -> Any:
        """Acquire an object from the pool."""
        with self._lock:
            if self._pool:
                return self._pool.pop()
        return self.factory()
    
    def release(self, obj: Any) -> None:
        """Release an object back to the pool."""
        with self._lock:
            if len(self._pool) < self.max_size:
                self._pool.append(obj)
    
    @property
    def size(self) -> int:
        """Get current pool size."""
        with self._lock:
            return len(self._pool)
    
    def clear(self) -> None:
        """Clear the pool."""
        with self._lock:
            self._pool.clear()


class TemporaryResource:
    """
    Context manager for temporary resources.
    
    Example:
        with TemporaryResource(delete_on_exit=True) as temp_file:
            write_data(temp_file.path)
    """
    
    def __init__(
        self,
        create_func: Callable[[], Any],
        cleanup_func: Optional[Callable[[Any], None]] = None,
        delete_on_exit: bool = True
    ):
        self.create_func = create_func
        self.cleanup_func = cleanup_func
        self.delete_on_exit = delete_on_exit
        self._resource: Optional[Any] = None
    
    def __enter__(self) -> Any:
        self._resource = self.create_func()
        return self._resource
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._resource and self.cleanup_func:
            try:
                self.cleanup_func(self._resource)
            except Exception:
                pass
    
    @property
    def resource(self) -> Optional[Any]:
        """Get the resource."""
        return self._resource
