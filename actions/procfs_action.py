"""procfs action module for rabai_autoclick.

Provides utilities for reading /proc filesystem: process info,
memory maps, CPU stats, load averages, and system information.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

__all__ = [
    "ProcReader",
    "ProcessInfo",
    "MemoryMap",
    "CpuStat",
    "LoadAvg",
    "MemInfo",
    "read_proc",
    "read_proc_file",
    "get_pid_list",
    "get_process_cmdline",
    "get_process_status",
    "get_process_stat",
    "get_process_memory_map",
    "get_memory_info",
    "get_cpu_stats",
    "get_load_average",
    "get_uptime",
    "get_version",
    "ProcParseError",
]


class ProcParseError(Exception):
    """Raised when /proc parsing fails."""
    pass


_proc_root = Path("/proc")


def read_proc(path: str) -> str:
    """Read a /proc file.

    Args:
        path: Path relative to /proc.

    Returns:
        File contents as string.
    """
    full_path = _proc_root / path
    try:
        return full_path.read_text()
    except FileNotFoundError:
        raise ProcParseError(f"/proc/{path} not found")
    except PermissionError:
        raise ProcParseError(f"Permission denied: /proc/{path}")


def read_proc_file(path: str, parser: Optional[Callable[[str], Any]] = None) -> Any:
    """Read and optionally parse a /proc file.

    Args:
        path: Path relative to /proc.
        parser: Optional function to parse content.

    Returns:
        Raw string or parsed result.
    """
    content = read_proc(path)
    if parser:
        return parser(content)
    return content


def get_pid_list() -> List[int]:
    """Get list of active process IDs."""
    pids = []
    for entry in _proc_root.iterdir():
        if entry.name.isdigit():
            pids.append(int(entry.name))
    return sorted(pids)


@dataclass
class ProcessInfo:
    """Information about a process."""
    pid: int
    name: str
    state: str
    ppid: int
    threads: int
    utime: int
    stime: int
    cutime: int
    cstime: int
    startcode: int
    endcode: int
    startstack: int
    kstkesp: int
    kstkeip: int
    signal: int
    blocked: int
    sigignore: int
    sigcatch: int
    wchan: str
    req: str
    starttime: int
    vsize: int
    rss: int
    rsslim: int
    cmdline: List[str] = None


def get_process_cmdline(pid: int) -> List[str]:
    """Get command line for a process."""
    try:
        cmdline = read_proc(f"{pid}/cmdline")
        return cmdline.split("\x00")[:-1]
    except ProcParseError:
        return []


def get_process_status(pid: int) -> Dict[str, str]:
    """Get process status info."""
    try:
        content = read_proc(f"{pid}/status")
        result = {}
        for line in content.split("\n"):
            if ":" in line:
                key, _, value = line.partition(":")
                result[key.strip()] = value.strip()
        return result
    except ProcParseError:
        return {}


def get_process_stat(pid: int) -> Dict[str, Any]:
    """Parse /proc/<pid>/stat."""
    try:
        content = read_proc(f"{pid}/stat")
        parts = content.split(" ", 3)
        if len(parts) < 3:
            raise ProcParseError(f"Invalid stat format for pid {pid}")
        name = parts[1].strip("()")
        rest = parts[2:]
        fields = rest[0].split() + (rest[1].split() if len(rest) > 1 else [])
        return {
            "pid": int(parts[0]),
            "name": name,
            "state": fields[0] if fields else "?",
            "ppid": int(fields[1]) if len(fields) > 1 else 0,
            "utime": int(fields[11]) if len(fields) > 11 else 0,
            "stime": int(fields[12]) if len(fields) > 12 else 0,
            "vsize": int(fields[20]) if len(fields) > 20 else 0,
            "rss": int(fields[21]) if len(fields) > 21 else 0,
        }
    except (ProcParseError, IndexError, ValueError):
        return {}


def get_process_memory_map(pid: int) -> List[MemoryMap]:
    """Get memory map for a process."""
    try:
        content = read_proc(f"{pid}/maps")
        maps = []
        for line in content.split("\n"):
            if line.strip():
                parts = line.split()
                if len(parts) >= 6:
                    addr_range = parts[0]
                    perms = parts[1]
                    offset = parts[2]
                    dev = parts[3]
                    inode = parts[4]
                    pathname = parts[5] if len(parts) > 5 else ""
                    start, end = addr_range.split("-")
                    maps.append(MemoryMap(
                        start=int(start, 16),
                        end=int(end, 16),
                        perms=perms,
                        offset=int(offset, 16),
                        dev=dev,
                        inode=inode,
                        pathname=pathname,
                    ))
        return maps
    except ProcParseError:
        return []


@dataclass
class MemoryMap:
    """Single memory mapping entry."""
    start: int
    end: int
    perms: str
    offset: int
    dev: str
    inode: int
    pathname: str


@dataclass
class CpuStat:
    """CPU statistics."""
    user: int
    nice: int
    system: int
    idle: int
    iowait: int
    irq: int
    softirq: int
    steal: int
    guest: int
    guest_nice: int

    @property
    def total(self) -> int:
        return sum([
            self.user, self.nice, self.system, self.idle,
            self.iowait, self.irq, self.softirq, self.steal,
            self.guest, self.guest_nice,
        ])

    @property
    def active(self) -> int:
        return self.total - self.idle - self.iowait

    @property
    def usage_percent(self) -> float:
        total = self.total
        if total == 0:
            return 0.0
        return (self.active / total) * 100


def get_cpu_stats(cpu: Optional[int] = None) -> Optional[CpuStat]:
    """Get CPU statistics from /proc/stat.

    Args:
        cpu: CPU number (None for aggregate).
    """
    try:
        if cpu is not None:
            content = read_proc(f"stat/cpu{cpu}")
            prefix = f"cpu{cpu}"
        else:
            content = read_proc("stat")
            prefix = "cpu "
    except ProcParseError:
        return None

    for line in content.split("\n"):
        if line.startswith(prefix):
            parts = line.split()
            if len(parts) >= 11:
                fields = [int(x) for x in parts[1:11]]
                while len(fields) < 10:
                    fields.append(0)
                return CpuStat(*fields)
    return None


@dataclass
class LoadAvg:
    """System load average."""
    avg_1: float
    avg_5: float
    avg_15: float
    running: int
    total: int
    latest_pid: int


def get_load_average() -> LoadAvg:
    """Get system load average from /proc/loadavg."""
    content = read_proc("loadavg")
    parts = content.split()
    avg_parts = parts[0].split("/")
    run_parts = parts[1].split("/")
    return LoadAvg(
        avg_1=float(avg_parts[0]),
        avg_5=float(avg_parts[1]) if len(avg_parts) > 1 else 0.0,
        avg_15=float(avg_parts[2]) if len(avg_parts) > 2 else 0.0,
        running=int(run_parts[0]),
        total=int(run_parts[1]),
        latest_pid=int(parts[2]),
    )


@dataclass
class MemInfo:
    """Memory information."""
    mem_total: int
    mem_free: int
    mem_available: int
    buffers: int
    cached: int
    swap_cached: int
    active: int
    inactive: int
    swap_total: int
    swap_free: int
    dirty: int
    mapped: int
    slab: int


def get_memory_info() -> Optional[MemInfo]:
    """Parse /proc/meminfo."""
    try:
        content = read_proc("meminfo")
        values: Dict[str, int] = {}
        for line in content.split("\n"):
            if ":" in line:
                key, _, value = line.partition(":")
                val_str = value.strip().split()[0]
                values[key.strip()] = int(val_str) * 1024  # Convert kB to bytes
        if "MemTotal" in values:
            return MemInfo(
                mem_total=values.get("MemTotal", 0),
                mem_free=values.get("MemFree", 0),
                mem_available=values.get("MemAvailable", 0),
                buffers=values.get("Buffers", 0),
                cached=values.get("Cached", 0),
                swap_cached=values.get("SwapCached", 0),
                active=values.get("Active", 0),
                inactive=values.get("Inactive", 0),
                swap_total=values.get("SwapTotal", 0),
                swap_free=values.get("SwapFree", 0),
                dirty=values.get("Dirty", 0),
                mapped=values.get("Mapped", 0),
                slab=values.get("Slab", 0),
            )
    except Exception:
        pass
    return None


def get_uptime() -> float:
    """Get system uptime in seconds."""
    try:
        content = read_proc("uptime")
        return float(content.split()[0])
    except (ProcParseError, IndexError, ValueError):
        return 0.0


def get_version() -> str:
    """Get kernel version from /proc/version."""
    try:
        content = read_proc("version")
        return content.strip()
    except ProcParseError:
        return ""


class ProcReader:
    """Convenience class for reading /proc data."""

    def __init__(self) -> None:
        self._cache: Dict[str, tuple[float, Any]] = {}
        self._cache_ttl = 0.1

    def get_pid_list(self) -> List[int]:
        """Get list of process IDs."""
        return get_pid_list()

    def get_all_process_info(self) -> List[Dict[str, Any]]:
        """Get info for all processes."""
        results = []
        for pid in self.get_pid_list():
            try:
                stat = get_process_stat(pid)
                cmdline = get_process_cmdline(pid)
                results.append({
                    "pid": pid,
                    "name": stat.get("name", ""),
                    "state": stat.get("state", ""),
                    "vsize": stat.get("vsize", 0),
                    "rss": stat.get("rss", 0),
                    "cmdline": cmdline,
                })
            except Exception:
                pass
        return results

    def get_system_info(self) -> Dict[str, Any]:
        """Get comprehensive system information."""
        mem = get_memory_info()
        cpu = get_cpu_stats()
        load = get_load_average()
        uptime = get_uptime()
        version = get_version()
        return {
            "memory": {
                "total_gb": mem.mem_total / (1024**3) if mem else 0,
                "free_gb": mem.mem_free / (1024**3) if mem else 0,
                "available_gb": mem.mem_available / (1024**3) if mem else 0,
            },
            "cpu": {
                "usage_percent": cpu.usage_percent if cpu else 0,
                "active": cpu.active if cpu else 0,
            },
            "load": {
                "1m": load.avg_1,
                "5m": load.avg_5,
                "15m": load.avg_15,
            },
            "uptime_seconds": uptime,
            "kernel_version": version,
        }
