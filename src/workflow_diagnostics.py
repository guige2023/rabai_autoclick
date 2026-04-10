"""
增强版智能工作流健康诊断 v23
Production-ready diagnostics with performance profiler, resource monitoring,
dependency health check, configuration validator, action graph, regression detection
"""
import json
import time
import os
import sys
import traceback
import subprocess
import platform
import psutil
import importlib.util
import socket
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from collections.abc import Mapping
import statistics
import heapq
import re
import hashlib
import threading
import io
import math

# Optional imports for charting
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    from graphviz import Digraph
    HAS_GRAPHVIZ = True
except ImportError:
    HAS_GRAPHVIZ = False


class HealthLevel(Enum):
    """健康等级"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class IssueSeverity(Enum):
    """问题严重程度"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class RootCause(Enum):
    """根本原因"""
    NETWORK = "network"
    PERMISSION = "permission"
    TIMEOUT = "timeout"
    RESOURCE = "resource"
    CONFIG = "config"
    CODE = "code"
    DEPENDENCY = "dependency"
    ENVIRONMENT = "environment"
    USER_INPUT = "user_input"
    PERFORMANCE = "performance"
    UNKNOWN = "unknown"


class DiagnosticStatus(Enum):
    """诊断状态"""
    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"


@dataclass
class StepMetrics:
    """步骤指标"""
    step_name: str
    step_index: int
    avg_duration: float = 0.0
    min_duration: float = 0.0
    max_duration: float = 0.0
    std_duration: float = 0.0
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    error_messages: List[str] = field(default_factory=list)
    p50_duration: float = 0.0
    p95_duration: float = 0.0


@dataclass
class HealthIssue:
    """健康问题"""
    issue_id: str
    issue_type: str
    severity: IssueSeverity
    root_cause: RootCause
    title: str
    description: str
    location: str
    suggestion: str
    auto_fixable: bool = False
    fix_command: Optional[str] = None
    impact: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthTrend:
    """健康趋势"""
    period: str
    success_rate_change: float
    duration_change: float
    trend_direction: str
    confidence: float


@dataclass
class AnomalyDetection:
    """异常检测"""
    detected_at: float
    anomaly_type: str
    metric: str
    expected_value: float
    actual_value: float
    deviation: float


@dataclass
class HealthReport:
    """增强健康报告"""
    workflow_id: str
    workflow_name: str
    overall_health: HealthLevel
    health_score: float
    execution_count: int
    success_rate: float
    avg_duration: float
    median_duration: float
    trends: List[HealthTrend]
    anomalies: List[AnomalyDetection]
    issues: List[HealthIssue]
    step_metrics: List[StepMetrics]
    root_causes: Dict[str, int]
    recommendations: List[Dict]
    predicted_next_failure: Optional[str]
    predicted_duration: Optional[float]
    generated_at: float
    first_execution: Optional[float]
    last_execution: Optional[float]
    comparison_to_average: Optional[float] = None
    # New fields
    performance_profile: Optional[Dict] = None
    resource_usage: Optional[Dict] = None
    coverage_analysis: Optional[Dict] = None
    regression_detected: Optional[Dict] = None


# ============================================================================
# NEW DATACLASSES FOR ADDED FEATURES
# ============================================================================

@dataclass
class PerformanceProfile:
    """性能剖析数据"""
    action_name: str
    total_time: float
    call_count: int
    avg_time: float
    min_time: float
    max_time: float
    std_time: float
    p50_time: float
    p95_time: float
    p99_time: float
    time_percentage: float  # 占总时间的百分比


@dataclass
class ResourceSnapshot:
    """资源使用快照"""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_read_mb: float
    disk_write_mb: float
    network_sent_mb: float
    network_recv_mb: float
    thread_count: int
    open_files: int


@dataclass
class DependencyInfo:
    """依赖信息"""
    name: str
    version: Optional[str]
    status: DiagnosticStatus
    is_optional: bool
    import_error: Optional[str] = None
    latest_version: Optional[str] = None
    is_outdated: bool = False


@dataclass
class ConfigValidationResult:
    """配置验证结果"""
    key: str
    value: Any
    status: DiagnosticStatus
    message: str
    expected_type: Optional[str] = None
    valid_range: Optional[Dict] = None


@dataclass
class ActionNode:
    """动作图节点"""
    action_id: str
    action_name: str
    action_type: str
    dependencies: List[str]
    dependents: List[str]
    avg_duration: float
    call_count: int
    success_rate: float


@dataclass
class CoverageItem:
    """覆盖率项目"""
    name: str
    category: str
    is_tested: bool
    last_tested: Optional[float]
    test_count: int
    failure_rate: float


@dataclass
class RegressionResult:
    """回归检测结果"""
    detected: bool
    metric: str
    current_value: float
    historical_avg: float
    historical_std: float
    deviation: float
    severity: IssueSeverity
    description: str
    timestamp: float


@dataclass
class DiagnosticResult:
    """单项诊断结果"""
    name: str
    status: DiagnosticStatus
    message: str
    details: Optional[Dict] = None
    duration_ms: float = 0.0
    recommendations: List[str] = field(default_factory=list)


@dataclass
class FullDiagnosticReport:
    """完整诊断报告"""
    generated_at: float
    hostname: str
    platform: str
    python_version: str
    uptime_seconds: float
    results: Dict[str, DiagnosticResult]
    overall_status: DiagnosticStatus
    total_issues: int
    critical_issues: int
    html_report_path: Optional[str] = None


# ============================================================================
# PERFORMANCE PROFILER
# ============================================================================

class PerformanceProfiler:
    """性能剖析器 - 追踪每个动作的时间消耗"""

    def __init__(self):
        self.records: Dict[str, List[float]] = defaultdict(list)
        self._active_timers: Dict[str, float] = {}
        self._lock = threading.Lock()
        self.total_start = time.time()

    def start(self, action_name: str) -> None:
        """开始计时"""
        with self._lock:
            self._active_timers[action_name] = time.time()

    def stop(self, action_name: str) -> Optional[float]:
        """停止计时，返回耗时"""
        start_time = None
        with self._lock:
            if action_name in self._active_timers:
                start_time = self._active_timers.pop(action_name)

        if start_time is not None:
            duration = time.time() - start_time
            with self._lock:
                self.records[action_name].append(duration)
            return duration
        return None

    def get_profile(self) -> List[PerformanceProfile]:
        """获取性能剖析报告"""
        profiles = []
        total_time = time.time() - self.total_start

        for action_name, durations in self.records.items():
            if not durations:
                continue

            sorted_durations = sorted(durations)
            count = len(durations)
            avg = statistics.mean(durations)
            std = statistics.stdev(durations) if count > 1 else 0

            p50_idx = int(count * 0.50)
            p95_idx = int(count * 0.95)
            p99_idx = int(count * 0.99)

            profile = PerformanceProfile(
                action_name=action_name,
                total_time=sum(durations),
                call_count=count,
                avg_time=avg,
                min_time=min(durations),
                max_time=max(durations),
                std_time=std,
                p50_time=sorted_durations[p50_idx] if sorted_durations else 0,
                p95_time=sorted_durations[p95_idx] if sorted_durations else 0,
                p99_time=sorted_durations[p99_idx] if sorted_durations else 0,
                time_percentage=(sum(durations) / total_time * 100) if total_time > 0 else 0
            )
            profiles.append(profile)

        return sorted(profiles, key=lambda p: p.total_time, reverse=True)

    def get_bottlenecks(self, top_n: int = 5) -> List[PerformanceProfile]:
        """识别性能瓶颈"""
        profiles = self.get_profile()
        # 瓶颈：平均时间长或p95高的动作
        for p in profiles:
            p.bottleneck_score = (p.p95_time * 0.5 + p.avg_time * 0.3 +
                                   p.time_percentage * 0.2)
        return sorted(profiles, key=lambda p: p.bottleneck_score, reverse=True)[:top_n]

    def reset(self) -> None:
        """重置数据"""
        with self._lock:
            self.records.clear()
            self._active_timers.clear()
            self.total_start = time.time()

    def to_dict(self) -> Dict:
        """导出为字典"""
        return {
            "profiles": [
                {
                    "action_name": p.action_name,
                    "total_time": round(p.total_time, 3),
                    "call_count": p.call_count,
                    "avg_time": round(p.avg_time, 3),
                    "min_time": round(p.min_time, 3),
                    "max_time": round(p.max_time, 3),
                    "p50_time": round(p.p50_time, 3),
                    "p95_time": round(p.p95_time, 3),
                    "p99_time": round(p.p99_time, 3),
                    "time_percentage": round(p.time_percentage, 2)
                }
                for p in self.get_profile()
            ],
            "bottlenecks": [
                {
                    "action_name": p.action_name,
                    "avg_time": round(p.avg_time, 3),
                    "p95_time": round(p.p95_time, 3),
                    "time_percentage": round(p.time_percentage, 2)
                }
                for p in self.get_bottlenecks()
            ]
        }


# Context manager for profiling
class ProfileContext:
    """性能剖析上下文管理器"""

    def __init__(self, profiler: PerformanceProfiler, action_name: str):
        self.profiler = profiler
        self.action_name = action_name

    def __enter__(self):
        self.profiler.start(self.action_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.profiler.stop(self.action_name)
        return False


# ============================================================================
# RESOURCE MONITOR
# ============================================================================

class ResourceMonitor:
    """资源使用监控器 - CPU、内存、磁盘IO、网络"""

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.snapshots: List[ResourceSnapshot] = []
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._process = psutil.Process()
        self._last_disk_io = self._get_disk_io_counters()
        self._last_net_io = self._get_net_io_counters()
        self._start_time = time.time()

    def _get_disk_io_counters(self) -> Tuple[float, float]:
        try:
            io = psutil.disk_io_counters()
            return (io.read_bytes / (1024 * 1024), io.write_bytes / (1024 * 1024))
        except Exception:
            return (0.0, 0.0)

    def _get_net_io_counters(self) -> Tuple[float, float]:
        try:
            io = psutil.net_io_counters()
            return (io.bytes_sent / (1024 * 1024), io.bytes_recv / (1024 * 1024))
        except Exception:
            return (0.0, 0.0)

    def _take_snapshot(self) -> ResourceSnapshot:
        """获取当前资源快照"""
        try:
            cpu = self._process.cpu_percent(interval=0.1)
            mem_info = self._process.memory_info()
            mem_mb = mem_info.rss / (1024 * 1024)
            mem_percent = self._process.memory_percent()

            # Disk I/O
            disk_io = self._get_disk_io_counters()
            disk_read = disk_io[0] - self._last_disk_io[0]
            disk_write = disk_io[1] - self._last_disk_io[1]
            self._last_disk_io = disk_io

            # Network I/O
            net_io = self._get_net_io_counters()
            net_sent = net_io[0] - self._last_net_io[0]
            net_recv = net_io[1] - self._last_net_io[1]
            self._last_net_io = net_io

            # System-wide resource usage
            sys_cpu = psutil.cpu_percent(interval=0.1)
            sys_mem = psutil.virtual_memory()

            return ResourceSnapshot(
                timestamp=time.time(),
                cpu_percent=cpu,
                memory_mb=mem_mb,
                memory_percent=mem_percent,
                disk_read_mb=max(0, disk_read),
                disk_write_mb=max(0, disk_write),
                network_sent_mb=max(0, net_sent),
                network_recv_mb=max(0, net_recv),
                thread_count=self._process.num_threads(),
                open_files=len(self._process.open_files())
            )
        except Exception as e:
            return ResourceSnapshot(
                timestamp=time.time(),
                cpu_percent=0, memory_mb=0, memory_percent=0,
                disk_read_mb=0, disk_write_mb=0,
                network_sent_mb=0, network_recv_mb=0,
                thread_count=0, open_files=0
            )

    def start_monitoring(self) -> None:
        """开始监控"""
        if self._monitoring:
            return
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _monitor_loop(self) -> None:
        """监控循环"""
        while self._monitoring:
            snapshot = self._take_snapshot()
            with threading.Lock():
                self.snapshots.append(snapshot)
                # 保留最近1000个快照
                if len(self.snapshots) > 1000:
                    self.snapshots = self.snapshots[-1000:]
            time.sleep(self.interval)

    def stop_monitoring(self) -> None:
        """停止监控"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)
            self._monitor_thread = None

    def get_summary(self) -> Dict[str, Any]:
        """获取资源使用摘要"""
        if not self.snapshots:
            return {"status": "no_data"}

        cpu_values = [s.cpu_percent for s in self.snapshots]
        mem_values = [s.memory_mb for s in self.snapshots]
        disk_read = sum(s.disk_read_mb for s in self.snapshots)
        disk_write = sum(s.disk_write_mb for s in self.snapshots)
        net_sent = sum(s.network_sent_mb for s in self.snapshots)
        net_recv = sum(s.network_recv_mb for s in self.snapshots)

        return {
            "cpu_percent_avg": round(statistics.mean(cpu_values), 2),
            "cpu_percent_max": round(max(cpu_values), 2),
            "memory_mb_avg": round(statistics.mean(mem_values), 2),
            "memory_mb_max": round(max(mem_values), 2),
            "memory_percent_avg": round(statistics.mean([s.memory_percent for s in self.snapshots]), 2),
            "disk_read_mb_total": round(disk_read, 2),
            "disk_write_mb_total": round(disk_write, 2),
            "network_sent_mb_total": round(net_sent, 2),
            "network_recv_mb_total": round(net_recv, 2),
            "snapshot_count": len(self.snapshots),
            "duration_seconds": round(time.time() - self._start_time, 2)
        }

    def get_timeline(self) -> List[Dict]:
        """获取资源使用时间线"""
        return [
            {
                "timestamp": s.timestamp,
                "cpu_percent": round(s.cpu_percent, 2),
                "memory_mb": round(s.memory_mb, 2),
                "disk_read_mb": round(s.disk_read_mb, 2),
                "disk_write_mb": round(s.disk_write_mb, 2)
            }
            for s in self.snapshots
        ]

    def detect_resource_anomalies(self) -> List[Dict]:
        """检测资源异常"""
        anomalies = []
        if len(self.snapshots) < 5:
            return anomalies

        # CPU异常检测
        cpu_values = [s.cpu_percent for s in self.snapshots]
        cpu_mean = statistics.mean(cpu_values)
        cpu_std = statistics.stdev(cpu_values) if len(cpu_values) > 1 else 0
        if cpu_std > 0:
            for i, s in enumerate(self.snapshots[-10:]):
                if (s.cpu_percent - cpu_mean) / cpu_std > 2:
                    anomalies.append({
                        "type": "cpu_spike",
                        "timestamp": s.timestamp,
                        "value": s.cpu_percent,
                        "expected": round(cpu_mean, 2),
                        "deviation": round((s.cpu_percent - cpu_mean) / cpu_std, 2)
                    })

        # 内存泄漏检测
        if len(self.snapshots) >= 20:
            first_half = statistics.mean([s.memory_mb for s in self.snapshots[:len(self.snapshots)//2]])
            second_half = statistics.mean([s.memory_mb for s in self.snapshots[len(self.snapshots)//2:]])
            mem_growth = second_half - first_half
            if mem_growth > 50:  # 增长超过50MB
                anomalies.append({
                    "type": "memory_growth",
                    "value": round(mem_growth, 2),
                    "message": f"内存增长 {mem_growth:.1f}MB，可能存在内存泄漏"
                })

        return anomalies

    def reset(self) -> None:
        """重置数据"""
        with threading.Lock():
            self.snapshots.clear()
            self._last_disk_io = self._get_disk_io_counters()
            self._last_net_io = self._get_net_io_counters()
            self._start_time = time.time()


# ============================================================================
# NETWORK DIAGNOSTICS
# ============================================================================

class NetworkDiagnostics:
    """网络诊断 - 检查远程服务连接"""

    def __init__(self):
        self.results: Dict[str, Dict] = {}

    def check_connectivity(self, host: str, port: int = 80,
                          timeout: float = 5.0) -> Dict[str, Any]:
        """检查到主机的连接"""
        start_time = time.time()
        result = {
            "host": host,
            "port": port,
            "reachable": False,
            "latency_ms": None,
            "error": None,
            "timestamp": time.time()
        }

        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            result["reachable"] = True
            result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
        except socket.timeout:
            result["error"] = "Connection timeout"
        except socket.gaierror as e:
            result["error"] = f"DNS resolution failed: {e}"
        except ConnectionRefusedError:
            result["error"] = "Connection refused"
        except Exception as e:
            result["error"] = str(e)

        self.results[f"{host}:{port}"] = result
        return result

    def check_http_endpoint(self, url: str, timeout: float = 10.0) -> Dict[str, Any]:
        """检查HTTP端点"""
        result = {
            "url": url,
            "status_code": None,
            "reachable": False,
            "latency_ms": None,
            "error": None,
            "timestamp": time.time()
        }

        start_time = time.time()
        try:
            req = urllib.request.Request(url, method='HEAD')
            response = urllib.request.urlopen(req, timeout=timeout)
            result["status_code"] = response.getcode()
            result["reachable"] = True
            result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
        except urllib.error.HTTPError as e:
            result["status_code"] = e.code
            result["reachable"] = e.code < 500
            result["error"] = f"HTTP {e.code}"
            result["latency_ms"] = round((time.time() - start_time) * 1000, 2)
        except urllib.error.URLError as e:
            result["error"] = str(e.reason)
        except Exception as e:
            result["error"] = str(e)

        return result

    def diagnose_common_services(self) -> Dict[str, Any]:
        """诊断常见服务连接"""
        services = [
            {"name": "Google DNS", "host": "8.8.8.8", "port": 53},
            {"name": "Cloudflare DNS", "host": "1.1.1.1", "port": 53},
            {"name": "HTTP Check", "host": "www.google.com", "port": 80},
            {"name": "HTTPS Check", "host": "www.google.com", "port": 443},
        ]

        results = {}
        for svc in services:
            results[svc["name"]] = self.check_connectivity(
                svc["host"], svc["port"], timeout=5.0
            )

        return {
            "services": results,
            "all_reachable": all(r.get("reachable", False) for r in results.values()),
            "failed_services": [
                name for name, r in results.items() if not r.get("reachable", False)
            ]
        }

    def get_results_summary(self) -> Dict:
        """获取结果摘要"""
        if not self.results:
            return {"status": "no_checks_performed"}

        total = len(self.results)
        reachable = sum(1 for r in self.results.values() if r.get("reachable", False))
        latencies = [r["latency_ms"] for r in self.results.values()
                    if r.get("latency_ms") is not None]

        return {
            "total_checks": total,
            "reachable_count": reachable,
            "unreachable_count": total - reachable,
            "avg_latency_ms": round(statistics.mean(latencies), 2) if latencies else None,
            "max_latency_ms": round(max(latencies), 2) if latencies else None,
            "results": self.results
        }


# ============================================================================
# DEPENDENCY HEALTH CHECK
# ============================================================================

class DependencyHealthCheck:
    """依赖健康检查 - 验证所有必需模块是否可用"""

    def __init__(self, known_dependencies: Optional[List[str]] = None):
        self.known_dependencies = known_dependencies or []
        self.results: Dict[str, DependencyInfo] = {}

    def check_module(self, module_name: str,
                    is_optional: bool = False) -> DependencyInfo:
        """检查单个模块"""
        result = DependencyInfo(
            name=module_name,
            version=None,
            status=DiagnosticStatus.UNKNOWN,
            is_optional=is_optional,
            import_error=None
        )

        try:
            spec = importlib.util.find_spec(module_name)
            if spec is None:
                result.status = DiagnosticStatus.FAIL
                result.import_error = "Module not found"
                return result

            module = importlib.import_module(module_name)
            result.version = getattr(module, '__version__', None)

            # 检查子模块
            if hasattr(module, '__all__'):
                result.status = DiagnosticStatus.PASS
            elif spec.submodule_search_locations:
                result.status = DiagnosticStatus.PASS
            else:
                result.status = DiagnosticStatus.WARNING
                result.import_error = "Module exists but may be incomplete"

        except ImportError as e:
            result.status = DiagnosticStatus.FAIL
            result.import_error = str(e)
        except Exception as e:
            result.status = DiagnosticStatus.WARNING
            result.import_error = str(e)

        self.results[module_name] = result
        return result

    def check_dependencies(self, dependencies: List[str],
                         optional: List[str] = None) -> Dict[str, DependencyInfo]:
        """检查依赖列表"""
        optional = optional or []
        results = {}

        for dep in dependencies:
            results[dep] = self.check_module(dep, is_optional=False)

        for dep in optional:
            results[dep] = self.check_module(dep, is_optional=True)

        return results

    def check_common_dependencies(self) -> Dict[str, DependencyInfo]:
        """检查常见依赖"""
        common_deps = [
            "json", "time", "os", "sys", "datetime", "threading",
            "statistics", "collections", "dataclasses", "enum",
            "subprocess", "socket", "urllib", "psutil", "platform"
        ]

        optional_deps = [
            "matplotlib", "graphviz", "numpy", "pandas"
        ]

        return self.check_dependencies(common_deps, optional_deps)

    def get_health_summary(self) -> Dict[str, Any]:
        """获取健康摘要"""
        if not self.results:
            return {"status": "no_checks_performed"}

        total = len(self.results)
        passed = sum(1 for r in self.results.values()
                     if r.status == DiagnosticStatus.PASS)
        failed = sum(1 for r in self.results.values()
                     if r.status == DiagnosticStatus.FAIL)
        warnings = sum(1 for r in self.results.values()
                       if r.status == DiagnosticStatus.WARNING)

        required_failed = [r.name for r in self.results.values()
                          if r.status == DiagnosticStatus.FAIL and not r.is_optional]

        return {
            "total_checked": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "health_percentage": round(passed / total * 100, 1) if total > 0 else 0,
            "required_failed": required_failed,
            "is_healthy": len(required_failed) == 0,
            "results": {
                name: {
                    "version": r.version,
                    "status": r.status.value,
                    "is_optional": r.is_optional,
                    "error": r.import_error
                }
                for name, r in self.results.items()
            }
        }


# ============================================================================
# CONFIGURATION VALIDATOR
# ============================================================================

class ConfigurationValidator:
    """配置验证器 - 验证所有设置"""

    def __init__(self, config_schema: Optional[Dict] = None):
        self.config_schema = config_schema or {}
        self.results: List[ConfigValidationResult] = []

    def validate_value(self, key: str, value: Any,
                       expected_type: type = None,
                       valid_range: Dict = None) -> ConfigValidationResult:
        """验证单个配置值"""
        result = ConfigValidationResult(
            key=key,
            value=value,
            status=DiagnosticStatus.UNKNOWN,
            message="",
            expected_type=expected_type.__name__ if expected_type else None,
            valid_range=valid_range
        )

        # 类型检查
        if expected_type and not isinstance(value, expected_type):
            result.status = DiagnosticStatus.FAIL
            result.message = f"Expected {expected_type.__name__}, got {type(value).__name__}"
            return result

        # 范围检查
        if valid_range:
            if "min" in valid_range and value < valid_range["min"]:
                result.status = DiagnosticStatus.FAIL
                result.message = f"Value {value} is below minimum {valid_range['min']}"
                return result
            if "max" in valid_range and value > valid_range["max"]:
                result.status = DiagnosticStatus.FAIL
                result.message = f"Value {value} is above maximum {valid_range['max']}"
                return result
            if "choices" in valid_range and value not in valid_range["choices"]:
                result.status = DiagnosticStatus.FAIL
                result.message = f"Value must be one of {valid_range['choices']}"
                return result

        result.status = DiagnosticStatus.PASS
        result.message = "Valid"
        return result

    def validate_dict(self, config: Dict, schema: Dict) -> List[ConfigValidationResult]:
        """验证配置字典"""
        results = []

        for key, rules in schema.items():
            if key not in config:
                if rules.get("required", False):
                    results.append(ConfigValidationResult(
                        key=key,
                        value=None,
                        status=DiagnosticStatus.FAIL,
                        message="Required key is missing"
                    ))
                continue

            expected_type = rules.get("type")
            valid_range = rules.get("range") or rules.get("choices")
            result = self.validate_value(
                key, config[key], expected_type, valid_range
            )
            results.append(result)

        # 检查未知键
        known_keys = set(schema.keys())
        for key in config.keys():
            if key not in known_keys:
                results.append(ConfigValidationResult(
                    key=key,
                    value=config[key],
                    status=DiagnosticStatus.WARNING,
                    message="Unknown configuration key"
                ))

        self.results.extend(results)
        return results

    def validate_workflow_config(self, config: Dict) -> List[ConfigValidationResult]:
        """验证工作流配置"""
        schema = {
            "workflow_id": {"type": str, "required": True},
            "workflow_name": {"type": str, "required": True},
            "timeout": {"type": (int, float), "range": {"min": 1, "max": 86400}},
            "retry_count": {"type": int, "range": {"min": 0, "max": 10}},
            "log_level": {"type": str, "choices": ["DEBUG", "INFO", "WARNING", "ERROR"]},
            "max_concurrent_steps": {"type": int, "range": {"min": 1, "max": 100}},
            "enable_monitoring": {"type": bool},
            "resource_limits": {
                "type": dict,
                "schema": {
                    "cpu_percent": {"type": (int, float), "range": {"min": 0.1, "max": 100}},
                    "memory_mb": {"type": (int, float), "range": {"min": 64, "max": 65536}}
                }
            }
        }

        return self.validate_dict(config, schema)

    def get_validation_summary(self) -> Dict[str, Any]:
        """获取验证摘要"""
        if not self.results:
            return {"status": "no_validations_performed"}

        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == DiagnosticStatus.PASS)
        failed = sum(1 for r in self.results if r.status == DiagnosticStatus.FAIL)
        warnings = sum(1 for r in self.results if r.status == DiagnosticStatus.WARNING)

        return {
            "total_validated": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "is_valid": failed == 0,
            "failed_keys": [r.key for r in self.results if r.status == DiagnosticStatus.FAIL],
            "warning_keys": [r.key for r in self.results if r.status == DiagnosticStatus.WARNING]
        }


# ============================================================================
# ACTION DEPENDENCY GRAPH
# ============================================================================

class ActionDependencyGraph:
    """动作依赖图 - 可视化动作间的调用关系"""

    def __init__(self):
        self.nodes: Dict[str, ActionNode] = {}
        self.edges: List[Tuple[str, str]] = []

    def add_action(self, action_id: str, action_name: str,
                  action_type: str = "action",
                  dependencies: List[str] = None,
                  avg_duration: float = 0.0,
                  call_count: int = 0,
                  success_rate: float = 1.0) -> None:
        """添加动作节点"""
        self.nodes[action_id] = ActionNode(
            action_id=action_id,
            action_name=action_name,
            action_type=action_type,
            dependencies=dependencies or [],
            dependents=[],
            avg_duration=avg_duration,
            call_count=call_count,
            success_rate=success_rate
        )

        # 更新依赖关系
        for dep_id in (dependencies or []):
            self.edges.append((dep_id, action_id))
            if dep_id in self.nodes:
                self.nodes[dep_id].dependents.append(action_id)

    def add_execution_flow(self, step_results: List[Dict]) -> None:
        """从执行结果添加流程"""
        for i, step in enumerate(step_results):
            step_id = step.get("id", f"step_{i}")
            step_name = step.get("name", f"Step {i+1}")
            step_type = step.get("type", "action")
            deps = step.get("depends_on", [])
            duration = step.get("duration", 0.0)
            success = step.get("success", True)

            self.add_action(
                action_id=step_id,
                action_name=step_name,
                action_type=step_type,
                dependencies=deps if isinstance(deps, list) else [deps],
                avg_duration=duration,
                call_count=1,
                success_rate=1.0 if success else 0.0
            )

    def get_execution_order(self) -> List[List[str]]:
        """获取拓扑排序的执行顺序"""
        in_degree = defaultdict(int)
        for node in self.nodes.values():
            for dep in node.dependencies:
                in_degree[node.action_id] += 1

        queue = [n for n in self.nodes if in_degree[n] == 0]
        result = []

        while queue:
            level = queue.copy()
            result.append(level)
            queue.clear()

            for node_id in level:
                for dependent in self.nodes[node_id].dependents:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        return result

    def find_circular_dependencies(self) -> List[List[str]]:
        """检测循环依赖"""
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node_id: str, path: List[str]) -> None:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)

            for dependent in self.nodes[node_id].dependents:
                if dependent not in visited:
                    dfs(dependent, path.copy())
                elif dependent in rec_stack:
                    cycle_start = path.index(dependent)
                    cycles.append(path[cycle_start:] + [dependent])

            rec_stack.remove(node_id)

        for node_id in self.nodes:
            if node_id not in visited:
                dfs(node_id, [])

        return cycles

    def render_text(self) -> str:
        """文本渲染"""
        lines = ["Action Dependency Graph", "=" * 40]

        # 按层级显示
        execution_order = self.get_execution_order()
        for i, level in enumerate(execution_order):
            lines.append(f"\nLevel {i} (parallel: {len(level)}):")
            for action_id in level:
                node = self.nodes[action_id]
                status = "✓" if node.success_rate > 0.5 else "✗"
                lines.append(
                    f"  [{status}] {node.action_name} ({node.action_type}) "
                    f"- avg: {node.avg_duration:.2f}s, calls: {node.call_count}"
                )
                if node.dependencies:
                    lines.append(f"       depends on: {node.dependencies}")

        # 循环依赖警告
        cycles = self.find_circular_dependencies()
        if cycles:
            lines.append("\n⚠️ Circular Dependencies Detected:")
            for cycle in cycles:
                lines.append(f"  {' -> '.join(cycle)}")

        return "\n".join(lines)

    def render_graphviz(self, filename: str = None) -> Optional[str]:
        """渲染为Graphviz图"""
        if not HAS_GRAPHVIZ:
            return None

        dot = Digraph(comment='Action Dependency Graph')
        dot.attr(rankdir='TB')

        for node_id, node in self.nodes.items():
            color = 'green' if node.success_rate > 0.5 else 'red'
            shape = 'box' if node.action_type == 'action' else 'ellipse'

            label = f"{node.action_name}\n({node.avg_duration:.2f}s avg)"
            dot.node(node_id, label, shape=shape, color=color, style='filled')

        for src, dst in self.edges:
            dot.edge(src, dst)

        if filename:
            dot.render(filename, format='png', cleanup=True)

        return dot.source


# ============================================================================
# COVERAGE ANALYSIS
# ============================================================================

class CoverageAnalyzer:
    """覆盖率分析 - 分析哪些部分被测试过"""

    def __init__(self):
        self.items: Dict[str, CoverageItem] = {}
        self.test_executions: Dict[str, List[float]] = defaultdict(list)

    def register_item(self, name: str, category: str) -> None:
        """注册需要覆盖的项目"""
        if name not in self.items:
            self.items[name] = CoverageItem(
                name=name,
                category=category,
                is_tested=False,
                last_tested=None,
                test_count=0,
                failure_rate=0.0
            )

    def record_test_execution(self, item_name: str, success: bool,
                             timestamp: float = None) -> None:
        """记录测试执行"""
        timestamp = timestamp or time.time()

        if item_name not in self.items:
            self.register_item(item_name, "unknown")

        item = self.items[item_name]
        item.is_tested = True
        item.last_tested = timestamp
        item.test_count += 1

        self.test_executions[item_name].append(1.0 if success else 0.0)

        # 更新失败率
        executions = self.test_executions[item_name]
        failures = sum(1 for e in executions if e == 0.0)
        item.failure_rate = failures / len(executions)

    def get_coverage_summary(self) -> Dict[str, Any]:
        """获取覆盖率摘要"""
        if not self.items:
            return {"status": "no_items_registered"}

        categories = defaultdict(lambda: {"total": 0, "tested": 0, "failed": 0})
        for item in self.items.values():
            categories[item.category]["total"] += 1
            if item.is_tested:
                categories[item.category]["tested"] += 1
            if item.failure_rate > 0:
                categories[item.category]["failed"] += 1

        total = len(self.items)
        tested = sum(1 for i in self.items.values() if i.is_tested)
        untested = [i.name for i in self.items.values() if not i.is_tested]

        return {
            "total_items": total,
            "tested_items": tested,
            "untested_items": tested,
            "coverage_percentage": round(tested / total * 100, 1) if total > 0 else 0,
            "untested_list": untested[:10],  # 最多10个
            "by_category": {
                cat: {
                    "total": data["total"],
                    "tested": data["tested"],
                    "coverage": round(data["tested"] / data["total"] * 100, 1)
                           if data["total"] > 0 else 0
                }
                for cat, data in categories.items()
            },
            "high_failure_rate_items": [
                {"name": i.name, "failure_rate": round(i.failure_rate * 100, 1)}
                for i in self.items.values()
                if i.failure_rate > 0.3
            ]
        }

    def generate_coverage_report(self) -> str:
        """生成覆盖率报告"""
        summary = self.get_coverage_summary()

        lines = ["Coverage Analysis Report", "=" * 40]
        lines.append(f"\nOverall Coverage: {summary['coverage_percentage']}%")
        lines.append(f"Tested: {summary['tested_items']}/{summary['total_items']} items")

        if summary.get("untested_list"):
            lines.append(f"\n⚠️ Untested Items ({len(summary['untested_list'])}):")
            for item in summary["untested_list"][:5]:
                lines.append(f"  - {item}")

        if summary.get("high_failure_rate_items"):
            lines.append(f"\n🔴 High Failure Rate Items:")
            for item in summary["high_failure_rate_items"]:
                lines.append(f"  - {item['name']}: {item['failure_rate']}%")

        return "\n".join(lines)


# ============================================================================
# REGRESSION DETECTION
# ============================================================================

class RegressionDetector:
    """回归检测 - 比较当前运行与历史运行"""

    def __init__(self, history_file: str = None):
        self.history_file = history_file or "./data/regression_history.json"
        self.history: Dict[str, List[Dict]] = {}
        self._load_history()

    def _load_history(self) -> None:
        """加载历史数据"""
        try:
            with open(self.history_file, "r", encoding="utf-8") as f:
                self.history = json.load(f)
        except FileNotFoundError:
            self.history = {}

    def _save_history(self) -> None:
        """保存历史数据"""
        os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(self.history, f, ensure_ascii=False, indent=2)

    def record_run(self, workflow_id: str, metrics: Dict) -> None:
        """记录一次运行"""
        if workflow_id not in self.history:
            self.history[workflow_id] = []

        self.history[workflow_id].append({
            "timestamp": time.time(),
            "duration": metrics.get("duration"),
            "success_rate": metrics.get("success_rate"),
            "cpu_percent_avg": metrics.get("cpu_percent_avg"),
            "memory_mb_avg": metrics.get("memory_mb_avg"),
            "step_count": metrics.get("step_count"),
            "error_count": metrics.get("error_count", 0)
        })

        # 保留最近100条
        if len(self.history[workflow_id]) > 100:
            self.history[workflow_id] = self.history[workflow_id][-100:]

        self._save_history()

    def detect_regressions(self, workflow_id: str,
                          current_metrics: Dict) -> List[RegressionResult]:
        """检测回归"""
        results = []

        if workflow_id not in self.history or len(self.history[workflow_id]) < 5:
            return results

        historical = self.history[workflow_id]
        metrics_to_check = ["duration", "success_rate", "cpu_percent_avg", "memory_mb_avg"]

        for metric in metrics_to_check:
            if metric not in current_metrics:
                continue

            current_val = current_metrics[metric]
            if current_val is None:
                continue

            historical_vals = [h.get(metric) for h in historical if h.get(metric) is not None]
            if not historical_vals:
                continue

            avg = statistics.mean(historical_vals)
            std = statistics.stdev(historical_vals) if len(historical_vals) > 1 else 0

            # 计算偏差
            if std > 0:
                deviation = (current_val - avg) / std
            else:
                deviation = 0 if current_val == avg else float('inf')

            # 判断是否回归
            is_regression = False
            severity = IssueSeverity.INFO
            description = ""

            # 对于成功率，越低越差
            if metric == "success_rate":
                if deviation < -1.5:
                    is_regression = True
                    severity = IssueSeverity.HIGH if deviation < -2 else IssueSeverity.MEDIUM
                    description = f"Success rate dropped from {avg*100:.1f}% to {current_val*100:.1f}%"

            # 对于duration，越高越差
            elif metric == "duration":
                if deviation > 1.5:
                    is_regression = True
                    severity = IssueSeverity.HIGH if deviation > 2 else IssueSeverity.MEDIUM
                    description = f"Duration increased from {avg:.2f}s to {current_val:.2f}s"

            # 对于资源使用，越高越差
            elif metric in ["cpu_percent_avg", "memory_mb_avg"]:
                if deviation > 2:
                    is_regression = True
                    severity = IssueSeverity.MEDIUM
                    description = f"{metric} increased significantly (z-score: {deviation:.2f})"

            if is_regression:
                results.append(RegressionResult(
                    detected=True,
                    metric=metric,
                    current_value=current_val,
                    historical_avg=round(avg, 3),
                    historical_std=round(std, 3),
                    deviation=round(deviation, 2),
                    severity=severity,
                    description=description,
                    timestamp=time.time()
                ))

        return results

    def get_historical_summary(self, workflow_id: str) -> Dict[str, Any]:
        """获取历史摘要"""
        if workflow_id not in self.history:
            return {"status": "no_history"}

        historical = self.history[workflow_id]

        durations = [h.get("duration") for h in historical if h.get("duration")]
        success_rates = [h.get("success_rate") for h in historical if h.get("success_rate") is not None]

        return {
            "workflow_id": workflow_id,
            "total_runs": len(historical),
            "first_run": historical[0].get("timestamp") if historical else None,
            "last_run": historical[-1].get("timestamp") if historical else None,
            "duration_avg": round(statistics.mean(durations), 2) if durations else None,
            "duration_std": round(statistics.stdev(durations), 2) if len(durations) > 1 else 0,
            "success_rate_avg": round(statistics.mean(success_rates) * 100, 1) if success_rates else None,
            "recent_trend": "improving" if len(success_rates) >= 10 and
                           sum(success_rates[-5:]) > sum(success_rates[-10:-5]) else "stable"
        }


# ============================================================================
# HTML REPORT GENERATOR
# ============================================================================

class HTMLReportGenerator:
    """HTML报告生成器 - 生成带图表的诊断报告"""

    def __init__(self, output_dir: str = "./data/reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate_diagnostic_report(self, report: FullDiagnosticReport,
                                   performance_profile: Dict = None,
                                   resource_usage: Dict = None,
                                   coverage: Dict = None) -> str:
        """生成完整HTML诊断报告"""
        timestamp = datetime.fromtimestamp(report.generated_at).strftime("%Y%m%d_%H%M%S")
        filename = f"diagnostic_report_{timestamp}.html"
        filepath = os.path.join(self.output_dir, filename)

        html_content = self._build_html_report(
            report, performance_profile, resource_usage, coverage
        )

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)

        return filepath

    def _build_html_report(self, report: FullDiagnosticReport,
                          performance_profile: Dict,
                          resource_usage: Dict,
                          coverage: Dict) -> str:
        """构建HTML内容"""
        status_color = {
            "pass": "#28a745",
            "warning": "#ffc107",
            "fail": "#dc3545",
            "skipped": "#6c757d"
        }.get(report.overall_status.value, "#6c757d")

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Workflow Diagnostics Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                   color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; }}
        .header h1 {{ margin: 0 0 10px 0; }}
        .status-badge {{ display: inline-block; padding: 5px 15px; border-radius: 20px;
                        background: {status_color}; font-weight: bold; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px; margin-bottom: 20px; }}
        .card {{ background: white; border-radius: 10px; padding: 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .card h3 {{ margin-top: 0; color: #333; border-bottom: 2px solid #667eea;
                   padding-bottom: 10px; }}
        .metric {{ display: flex; justify-content: space-between; padding: 10px 0;
                  border-bottom: 1px solid #eee; }}
        .metric:last-child {{ border-bottom: none; }}
        .metric-label {{ color: #666; }}
        .metric-value {{ font-weight: bold; color: #333; }}
        .issue-list {{ list-style: none; padding: 0; }}
        .issue-item {{ padding: 10px; margin: 5px 0; border-radius: 5px;
                      background: #f8f9fa; border-left: 4px solid #dc3545; }}
        .issue-item.warning {{ border-left-color: #ffc107; }}
        .issue-item.pass {{ border-left-color: #28a745; }}
        .chart-container {{ margin: 20px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f9fa; font-weight: 600; }}
        .footer {{ text-align: center; color: #666; margin-top: 30px; padding: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Workflow Diagnostics Report</h1>
            <p>Generated: {datetime.fromtimestamp(report.generated_at).strftime('%Y-%m-%d %H:%M:%S')}</p>
            <span class="status-badge">{report.overall_status.value.upper()}</span>
        </div>

        <div class="grid">
            <div class="card">
                <h3>📊 System Info</h3>
                <div class="metric">
                    <span class="metric-label">Hostname</span>
                    <span class="metric-value">{report.hostname}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Platform</span>
                    <span class="metric-value">{report.platform}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Python Version</span>
                    <span class="metric-value">{report.python_version}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Uptime</span>
                    <span class="metric-value">{report.uptime_seconds:.0f}s</span>
                </div>
            </div>

            <div class="card">
                <h3>📈 Diagnostics Summary</h3>
                <div class="metric">
                    <span class="metric-label">Total Checks</span>
                    <span class="metric-value">{len(report.results)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Total Issues</span>
                    <span class="metric-value">{report.total_issues}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Critical Issues</span>
                    <span class="metric-value" style="color: {'#dc3545' if report.critical_issues > 0 else '#28a745'}">{report.critical_issues}</span>
                </div>
            </div>
        </div>
"""

        # Diagnostics Results
        html += '<div class="card"><h3>🔍 Diagnostic Results</h3><ul class="issue-list">'
        for name, result in report.results.items():
            icon = "✅" if result.status == DiagnosticStatus.PASS else "⚠️" if result.status == DiagnosticStatus.WARNING else "❌"
            css_class = "pass" if result.status == DiagnosticStatus.PASS else "warning" if result.status == DiagnosticStatus.WARNING else ""
            html += f'<li class="issue-item {css_class}">{icon} <strong>{name}</strong>: {result.message}</li>'
        html += '</ul></div>'

        # Performance Profile
        if performance_profile and performance_profile.get("profiles"):
            html += '<div class="card"><h3>⚡ Performance Profile</h3><table>'
            html += '<tr><th>Action</th><th>Calls</th><th>Avg Time</th><th>P95</th><th>% Time</th></tr>'
            for p in performance_profile["profiles"][:10]:
                html += f'<tr><td>{p["action_name"]}</td><td>{p["call_count"]}</td>'
                html += f'<td>{p["avg_time"]:.3f}s</td><td>{p["p95_time"]:.3f}s</td>'
                html += f'<td>{p["time_percentage"]:.1f}%</td></tr>'
            html += '</table></div>'

        # Resource Usage
        if resource_usage:
            html += '<div class="card"><h3>💾 Resource Usage</h3>'
            html += f'<div class="metric"><span class="metric-label">CPU (avg/max)</span>'
            html += f'<span class="metric-value">{resource_usage.get("cpu_percent_avg", 0)}% / {resource_usage.get("cpu_percent_max", 0)}%</span></div>'
            html += f'<div class="metric"><span class="metric-label">Memory (avg/max)</span>'
            html += f'<span class="metric-value">{resource_usage.get("memory_mb_avg", 0):.1f}MB / {resource_usage.get("memory_mb_max", 0):.1f}MB</span></div>'
            html += f'<div class="metric"><span class="metric-label">Disk I/O</span>'
            html += f'<span class="metric-value">R: {resource_usage.get("disk_read_mb_total", 0):.1f}MB / W: {resource_usage.get("disk_write_mb_total", 0):.1f}MB</span></div>'
            html += '</div>'

        # Coverage
        if coverage:
            html += '<div class="card"><h3>📊 Coverage Analysis</h3>'
            html += f'<p>Overall Coverage: <strong>{coverage.get("coverage_percentage", 0)}%</strong></p>'
            html += f'<p>Tested: {coverage.get("tested_items", 0)}/{coverage.get("total_items", 0)} items</p>'
            if coverage.get("untested_list"):
                html += '<h4>Untested Items:</h4><ul>'
                for item in coverage["untested_list"][:5]:
                    html += f'<li>{item}</li>'
                html += '</ul>'
            html += '</div>'

        html += f'''
        <div class="footer">
            <p>Report generated by Workflow Diagnostics v23</p>
        </div>
    </div>
</body>
</html>'''

        return html

    def generate_chart(self, data: List[float], labels: List[str],
                      title: str, ylabel: str, filename: str) -> Optional[str]:
        """生成图表"""
        if not HAS_MATPLOTLIB:
            return None

        filepath = os.path.join(self.output_dir, filename)

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(labels, data, marker='o', linewidth=2, markersize=6)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(filepath, dpi=100)
        plt.close()

        return filepath


# ============================================================================
# MAIN DIAGNOSTICS CLASS
# ============================================================================

class WorkflowDiagnosticsV2:
    """增强版智能工作流诊断室"""

    def __init__(self, data_dir: str = "./data", flow_engine_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.flow_engine_callback = flow_engine_callback
        self.execution_history: Dict[str, List[Dict]] = defaultdict(list)
        self.workflow_definitions: Dict[str, Dict] = {}
        self.health_score_history: Dict[str, List[Dict]] = defaultdict(list)

        # Initialize new components
        self.profiler = PerformanceProfiler()
        self.resource_monitor = ResourceMonitor()
        self.network_diagnostics = NetworkDiagnostics()
        self.dependency_checker = DependencyHealthCheck()
        self.config_validator = ConfigurationValidator()
        self.coverage_analyzer = CoverageAnalyzer()
        self.regression_detector = RegressionDetector(data_dir + "/regression_history.json")
        self.html_generator = HTMLReportGenerator(data_dir + "/reports")

        self._ensure_data_dir()
        self._load_data()
        self._load_health_score_history()

    def _load_data(self) -> None:
        """加载数据"""
        try:
            with open(f"{self.data_dir}/execution_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for wf_id, executions in data.items():
                    self.execution_history[wf_id] = executions
        except FileNotFoundError:
            pass

        try:
            with open(f"{self.data_dir}/workflow_registry.json", "r", encoding="utf-8") as f:
                self.workflow_definitions = json.load(f)
        except FileNotFoundError:
            pass

    def _save_history(self) -> None:
        """保存历史"""
        data = {}
        for wf_id, executions in self.execution_history.items():
            data[wf_id] = executions[-100:]

        with open(f"{self.data_dir}/execution_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _ensure_data_dir(self) -> None:
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(f"{self.data_dir}/reports", exist_ok=True)

    def _load_health_score_history(self) -> None:
        """加载健康分数历史"""
        try:
            with open(f"{self.data_dir}/health_score_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for wf_id, scores in data.items():
                    self.health_score_history[wf_id] = scores
        except FileNotFoundError:
            pass

    def _save_health_score_history(self) -> None:
        """保存健康分数历史"""
        data = {}
        for wf_id, scores in self.health_score_history.items():
            data[wf_id] = scores[-100:]
        with open(f"{self.data_dir}/health_score_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # ========================================================================
    # NEW DIAGNOSTIC METHODS
    # ========================================================================

    def run_full_diagnostics(self, workflow_id: str = None) -> FullDiagnosticReport:
        """运行所有诊断检查"""
        start_time = time.time()
        results = {}

        # 1. Performance Profiler
        perf_result = self._diagnose_performance()
        results["performance"] = perf_result

        # 2. Resource Usage
        resource_result = self._diagnose_resource_usage()
        results["resource_usage"] = resource_result

        # 3. Network Diagnostics
        network_result = self._diagnose_network()
        results["network"] = network_result

        # 4. Dependency Health Check
        dep_result = self._diagnose_dependencies()
        results["dependencies"] = dep_result

        # 5. Configuration Validator
        config_result = self._diagnose_configuration()
        results["configuration"] = config_result

        # 6. Action Dependency Graph
        graph_result = self._diagnose_action_graph(workflow_id)
        results["action_graph"] = graph_result

        # 7. Coverage Analysis
        coverage_result = self._diagnose_coverage(workflow_id)
        results["coverage"] = coverage_result

        # 8. Regression Detection
        regression_result = self._diagnose_regression(workflow_id)
        results["regression"] = regression_result

        # Calculate overall status
        failed = sum(1 for r in results.values() if r.status == DiagnosticStatus.FAIL)
        warnings = sum(1 for r in results.values() if r.status == DiagnosticStatus.WARNING)
        critical = failed
        overall = DiagnosticStatus.PASS
        if critical > 0:
            overall = DiagnosticStatus.FAIL
        elif warnings > 2:
            overall = DiagnosticStatus.WARNING

        duration_ms = (time.time() - start_time) * 1000

        return FullDiagnosticReport(
            generated_at=time.time(),
            hostname=socket.gethostname(),
            platform=platform.system(),
            python_version=platform.python_version(),
            uptime_seconds=time.time() - psutil.Process().create_time(),
            results=results,
            overall_status=overall,
            total_issues=len([r for r in results.values() if r.status != DiagnosticStatus.PASS]),
            critical_issues=critical
        )

    def _diagnose_performance(self) -> DiagnosticResult:
        """诊断性能"""
        start = time.time()
        profile = self.profiler.get_profile()
        bottlenecks = self.profiler.get_bottlenecks()

        if not profile:
            return DiagnosticResult(
                name="performance",
                status=DiagnosticStatus.WARNING,
                message="No performance data available",
                duration_ms=(time.time() - start) * 1000
            )

        total_time = sum(p.total_time for p in profile)
        msg = f"{len(profile)} actions tracked, total time: {total_time:.2f}s"

        recommendations = []
        if bottlenecks:
            recommendations.append(f"Top bottleneck: {bottlenecks[0].action_name} (P95: {bottlenecks[0].p95_time:.3f}s)")

        return DiagnosticResult(
            name="performance",
            status=DiagnosticStatus.PASS,
            message=msg,
            details=self.profiler.to_dict(),
            duration_ms=(time.time() - start) * 1000,
            recommendations=recommendations
        )

    def _diagnose_resource_usage(self) -> DiagnosticResult:
        """诊断资源使用"""
        start = time.time()
        summary = self.resource_monitor.get_summary()
        anomalies = self.resource_monitor.detect_resource_anomalies()

        if summary.get("status") == "no_data":
            return DiagnosticResult(
                name="resource_usage",
                status=DiagnosticStatus.WARNING,
                message="No resource usage data available",
                duration_ms=(time.time() - start) * 1000
            )

        issues = []
        if anomalies:
            issues.extend([a.get("message", str(a)) for a in anomalies])

        status = DiagnosticStatus.PASS
        if any(a.get("type") == "memory_growth" for a in anomalies):
            status = DiagnosticStatus.WARNING
        if summary.get("cpu_percent_max", 0) > 90:
            status = DiagnosticStatus.WARNING

        return DiagnosticResult(
            name="resource_usage",
            status=status,
            message=f"CPU: {summary.get('cpu_percent_avg', 0)}% avg, Memory: {summary.get('memory_mb_avg', 0):.0f}MB avg",
            details=summary,
            duration_ms=(time.time() - start) * 1000,
            recommendations=issues
        )

    def _diagnose_network(self) -> DiagnosticResult:
        """诊断网络连接"""
        start = time.time()
        diag = self.network_diagnostics.diagnose_common_services()

        status = DiagnosticStatus.PASS
        if not diag.get("all_reachable", True):
            status = DiagnosticStatus.WARNING

        failed = diag.get("failed_services", [])

        return DiagnosticResult(
            name="network",
            status=status,
            message=f"{diag.get('reachable_count', 0)}/{diag.get('total_checks', 0)} services reachable",
            details=diag,
            duration_ms=(time.time() - start) * 1000,
            recommendations=[f"Failed: {', '.join(failed)}"] if failed else []
        )

    def _diagnose_dependencies(self) -> DiagnosticResult:
        """诊断依赖健康"""
        start = time.time()
        self.dependency_checker.check_common_dependencies()
        summary = self.dependency_checker.get_health_summary()

        status = DiagnosticStatus.PASS
        if not summary.get("is_healthy", True):
            status = DiagnosticStatus.FAIL

        return DiagnosticResult(
            name="dependencies",
            status=status,
            message=f"{summary.get('passed', 0)}/{summary.get('total_checked', 0)} dependencies healthy",
            details=summary,
            duration_ms=(time.time() - start) * 1000,
            recommendations=[f"Failed: {', '.join(summary.get('required_failed', []))}"] if summary.get('required_failed') else []
        )

    def _diagnose_configuration(self) -> DiagnosticResult:
        """诊断配置"""
        start = time.time()

        # 获取配置示例
        sample_config = self._get_sample_config()
        self.config_validator.validate_workflow_config(sample_config)
        summary = self.config_validator.get_validation_summary()

        status = DiagnosticStatus.PASS
        if not summary.get("is_valid", True):
            status = DiagnosticStatus.FAIL

        return DiagnosticResult(
            name="configuration",
            status=status,
            message=f"{summary.get('passed', 0)}/{summary.get('total_validated', 0)} config values valid",
            details=summary,
            duration_ms=(time.time() - start) * 1000,
            recommendations=[f"Failed: {', '.join(summary.get('failed_keys', []))}"] if summary.get('failed_keys') else []
        )

    def _diagnose_action_graph(self, workflow_id: str = None) -> DiagnosticResult:
        """诊断动作依赖图"""
        start = time.time()
        graph = ActionDependencyGraph()

        # 从执行历史构建图
        if workflow_id:
            executions = self.execution_history.get(workflow_id, [])
            if executions:
                latest = executions[-1]
                steps = latest.get("step_results", [])
                graph.add_execution_flow(steps)

        cycles = graph.find_circular_dependencies()
        execution_order = graph.get_execution_order()

        status = DiagnosticStatus.PASS
        msg = f"{len(graph.nodes)} actions, {len(execution_order)} levels"

        if cycles:
            status = DiagnosticStatus.FAIL
            msg = f"Circular dependency detected!"

        recommendations = []
        if cycles:
            recommendations.append(f"Cycle: {' -> '.join(cycles[0])}")

        return DiagnosticResult(
            name="action_graph",
            status=status,
            message=msg,
            details={"node_count": len(graph.nodes), "edge_count": len(graph.edges),
                    "circular_dependencies": cycles},
            duration_ms=(time.time() - start) * 1000,
            recommendations=recommendations
        )

    def _diagnose_coverage(self, workflow_id: str = None) -> DiagnosticResult:
        """诊断覆盖率"""
        start = time.time()

        # 模拟覆盖率数据（实际应从测试框架获取）
        if workflow_id:
            executions = self.execution_history.get(workflow_id, [])
            for e in executions:
                for step in e.get("step_results", []):
                    step_name = step.get("name", "unknown")
                    success = step.get("success", True)
                    self.coverage_analyzer.register_item(step_name, "workflow_step")
                    self.coverage_analyzer.record_test_execution(step_name, success)

        summary = self.coverage_analyzer.get_coverage_summary()

        status = DiagnosticStatus.WARNING
        if summary.get("coverage_percentage", 0) >= 80:
            status = DiagnosticStatus.PASS

        return DiagnosticResult(
            name="coverage",
            status=status,
            message=f"Coverage: {summary.get('coverage_percentage', 0)}%",
            details=summary,
            duration_ms=(time.time() - start) * 1000,
            recommendations=[f"Untested: {', '.join(summary.get('untested_list', [])[:3])}"] if summary.get('untested_list') else []
        )

    def _diagnose_regression(self, workflow_id: str = None) -> DiagnosticResult:
        """诊断回归"""
        start = time.time()

        if not workflow_id:
            return DiagnosticResult(
                name="regression",
                status=DiagnosticStatus.SKIPPED,
                message="No workflow_id provided",
                duration_ms=(time.time() - start) * 1000
            )

        executions = self.execution_history.get(workflow_id, [])
        if len(executions) < 5:
            return DiagnosticResult(
                name="regression",
                status=DiagnosticStatus.SKIPPED,
                message="Insufficient history for regression detection",
                duration_ms=(time.time() - start) * 1000
            )

        # 计算当前指标
        recent = executions[-5:]
        current_metrics = {
            "duration": statistics.mean([e.get("duration", 0) for e in recent]),
            "success_rate": sum(1 for e in recent if e.get("success")) / len(recent)
        }

        regressions = self.regression_detector.detect_regressions(workflow_id, current_metrics)

        status = DiagnosticStatus.PASS
        if any(r.severity == IssueSeverity.HIGH for r in regressions):
            status = DiagnosticStatus.FAIL
        elif regressions:
            status = DiagnosticStatus.WARNING

        return DiagnosticResult(
            name="regression",
            status=status,
            message=f"{len(regressions)} regression(s) detected",
            details={"regressions": [vars(r) for r in regressions]},
            duration_ms=(time.time() - start) * 1000,
            recommendations=[r.description for r in regressions[:3]]
        )

    def _get_sample_config(self) -> Dict:
        """获取示例配置"""
        return {
            "workflow_id": "sample_wf",
            "workflow_name": "Sample Workflow",
            "timeout": 300,
            "retry_count": 3,
            "log_level": "INFO",
            "enable_monitoring": True
        }

    def generate_html_report(self, workflow_id: str = None) -> str:
        """生成HTML诊断报告"""
        report = self.run_full_diagnostics(workflow_id)

        perf_data = None
        if hasattr(self.profiler, 'records') and self.profiler.records:
            perf_data = self.profiler.to_dict()

        resource_data = self.resource_monitor.get_summary()
        coverage_data = self.coverage_analyzer.get_coverage_summary()

        filepath = self.html_generator.generate_diagnostic_report(
            report, perf_data, resource_data, coverage_data
        )

        return filepath

    def profile_action(self, action_name: str) -> ProfileContext:
        """创建性能剖析上下文"""
        return ProfileContext(self.profiler, action_name)

    def start_resource_monitoring(self) -> None:
        """开始资源监控"""
        self.resource_monitor.start_monitoring()

    def stop_resource_monitoring(self) -> None:
        """停止资源监控"""
        self.resource_monitor.stop_monitoring()

    # ========================================================================
    # LEGACY METHODS (kept for backward compatibility)
    # ========================================================================

    def get_health_score_trend(self, workflow_id: str, period_hours: int = 168) -> Dict[str, Any]:
        """获取健康分数趋势"""
        scores = self.health_score_history.get(workflow_id, [])
        if not scores:
            return {"status": "no_data", "trend": "unknown"}

        now = time.time()
        cutoff = now - (period_hours * 3600)
        recent_scores = [s for s in scores if s.get("timestamp", 0) > cutoff]

        if len(recent_scores) < 2:
            return {"status": "insufficient_data", "trend": "unknown"}

        first_half = recent_scores[:len(recent_scores)//2]
        second_half = recent_scores[len(recent_scores)//2:]

        first_avg = sum(s.get("score", 0) for s in first_half) / len(first_half)
        second_avg = sum(s.get("score", 0) for s in second_half) / len(second_half)

        change = second_avg - first_avg

        if change > 5:
            trend = "improving"
        elif change < -5:
            trend = "declining"
        else:
            trend = "stable"

        return {
            "status": "ok",
            "trend": trend,
            "change": change,
            "current_score": recent_scores[-1].get("score", 0),
            "avg_score": (first_avg + second_avg) / 2,
            "data_points": len(recent_scores),
            "period_hours": period_hours
        }

    def perform_root_cause_analysis(self, workflow_id: str) -> Dict[str, Any]:
        """执行根因分析"""
        executions = self.execution_history.get(workflow_id, [])
        if not executions:
            return {"status": "no_data", "causes": []}

        failures = [e for e in executions if not e.get("success")]
        if not failures:
            return {"status": "healthy", "causes": []}

        cause_counts = defaultdict(int)
        cause_examples = defaultdict(list)

        for failure in failures:
            error = failure.get("error", "Unknown error")
            cause = self._infer_root_cause(error)
            cause_counts[cause.value] += 1
            if len(cause_examples[cause.value]) < 3:
                cause_examples[cause.value].append(error[:100])

        total_failures = len(failures)
        causes = []
        for cause_value, count in sorted(cause_counts.items(), key=lambda x: -x[1]):
            percentage = (count / total_failures) * 100
            causes.append({
                "cause": cause_value,
                "count": count,
                "percentage": round(percentage, 1),
                "examples": cause_examples[cause_value],
                "recommendation": self._get_suggestion_for_error(RootCause(cause_value), "")
            })

        time_patterns = self._analyze_failure_time_pattern(failures)

        return {
            "status": "analyzed",
            "total_failures": total_failures,
            "causes": causes,
            "time_patterns": time_patterns
        }

    def _analyze_failure_time_pattern(self, failures: List[Dict]) -> Dict:
        """分析失败时间模式"""
        if not failures:
            return {"pattern": "none"}

        hours = defaultdict(int)
        weekdays = defaultdict(int)

        for f in failures:
            ts = f.get("timestamp", 0)
            if ts:
                dt = datetime.fromtimestamp(ts)
                hours[dt.hour] += 1
                weekdays[dt.weekday()] += 1

        peak_hour = max(hours.items(), key=lambda x: x[1]) if hours else (0, 0)
        peak_weekday = max(weekdays.items(), key=lambda x: x[1]) if weekdays else (0, 0)

        weekdays_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

        return {
            "peak_hour": peak_hour[0],
            "peak_hour_count": peak_hour[1],
            "peak_weekday": weekdays_names[peak_weekday[0]] if peak_weekday[0] < 7 else "未知",
            "peak_weekday_count": peak_weekday[1]
        }

    def set_flow_engine_callback(self, callback: Callable) -> None:
        """设置 FlowEngine 回调函数"""
        self.flow_engine_callback = callback

    def _notify_flow_engine(self, event_type: str, data: Dict) -> None:
        """通知 FlowEngine 事件"""
        if self.flow_engine_callback:
            try:
                self.flow_engine_callback(event_type, data)
            except Exception:
                pass

    def record_execution(self, workflow_id: str, workflow_name: str,
                        step_results: List[Dict[str, Any]],
                        duration: float, success: bool,
                        error: str = None,
                        context: Dict = None) -> None:
        """记录执行结果"""
        record = {
            "timestamp": time.time(),
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
            "step_results": step_results,
            "duration": duration,
            "success": success,
            "error": error,
            "context": context or {}
        }

        self.execution_history[workflow_id].append(record)

        if len(self.execution_history[workflow_id]) > 1000:
            self.execution_history[workflow_id] = \
                self.execution_history[workflow_id][-1000:]

        self._save_history()

        # Record health score
        report = self.diagnose(workflow_id)
        self.health_score_history[workflow_id].append({
            "timestamp": time.time(),
            "score": report.health_score,
            "success": success
        })
        self._save_health_score_history()

        # Record for regression detection
        self.regression_detector.record_run(workflow_id, {
            "duration": duration,
            "success_rate": 1.0 if success else 0.0,
            "step_count": len(step_results)
        })

        self._notify_flow_engine("execution_recorded", {
            "workflow_id": workflow_id,
            "success": success
        })

    def diagnose(self, workflow_id: str) -> HealthReport:
        """诊断工作流"""
        executions = self.execution_history.get(workflow_id, [])

        if not executions:
            return self._empty_report(workflow_id)

        execution_count = len(executions)
        success_count = sum(1 for e in executions if e.get("success"))
        success_rate = success_count / execution_count if execution_count else 0
        durations = [e["duration"] for e in executions]
        avg_duration = statistics.mean(durations) if durations else 0
        median_duration = statistics.median(durations) if durations else 0

        trends = self._analyze_trends(executions)
        anomalies = self._detect_anomalies(executions)
        issues = self._analyze_issues(executions)
        root_causes = self._analyze_root_causes(executions)
        step_metrics = self._calculate_step_metrics(executions)

        recommendations = self._generate_recommendations(
            issues, trends, root_causes, step_metrics
        )

        predicted_failure = self._predict_next_failure(executions)
        predicted_duration = self._predict_duration(executions)

        health_score = self._calculate_health_score_v2(
            success_rate, len(issues), avg_duration, trends, anomalies, issues
        )

        overall_health = self._get_health_level(health_score)
        comparison = self._compare_to_average(workflow_id, health_score)

        # Add performance profile and resource usage to report
        perf_profile = self.profiler.to_dict() if self.profiler.records else None
        resource_usage = self.resource_monitor.get_summary()

        report = HealthReport(
            workflow_id=workflow_id,
            workflow_name=executions[0].get("workflow_name", workflow_id),
            overall_health=overall_health,
            health_score=health_score,
            execution_count=execution_count,
            success_rate=success_rate,
            avg_duration=avg_duration,
            median_duration=median_duration,
            trends=trends,
            anomalies=anomalies,
            issues=issues,
            step_metrics=step_metrics,
            root_causes=root_causes,
            recommendations=recommendations,
            predicted_next_failure=predicted_failure,
            predicted_duration=predicted_duration,
            generated_at=time.time(),
            first_execution=executions[0].get("timestamp"),
            last_execution=executions[-1].get("timestamp"),
            comparison_to_average=comparison,
            performance_profile=perf_profile,
            resource_usage=resource_usage
        )

        self._notify_flow_engine("diagnosis_complete", {
            "workflow_id": workflow_id,
            "health_score": health_score,
            "health_level": overall_health.value
        })

        return report

    def _empty_report(self, workflow_id: str) -> HealthReport:
        """空报告"""
        return HealthReport(
            workflow_id=workflow_id,
            workflow_name="Unknown",
            overall_health=HealthLevel.FAIR,
            health_score=50,
            execution_count=0,
            success_rate=0,
            avg_duration=0,
            median_duration=0,
            trends=[],
            anomalies=[],
            issues=[],
            step_metrics=[],
            root_causes={},
            recommendations=[{"priority": "high", "suggestion": "暂无执行数据，请先运行工作流收集数据"}],
            predicted_next_failure=None,
            predicted_duration=None,
            generated_at=time.time(),
            first_execution=None,
            last_execution=None,
            comparison_to_average=None
        )

    def _analyze_trends(self, executions: List[Dict]) -> List[HealthTrend]:
        """分析趋势"""
        if len(executions) < 5:
            return []

        trends = []
        sorted_executions = sorted(executions, key=lambda x: x["timestamp"])

        periods = [
            ("24h", 24 * 3600),
            ("7d", 7 * 24 * 3600),
            ("30d", 30 * 24 * 3600)
        ]

        now = time.time()

        for period_name, period_seconds in periods:
            period_executions = [
                e for e in sorted_executions
                if now - e["timestamp"] <= period_seconds
            ]

            if len(period_executions) < 4:
                continue

            mid = len(period_executions) // 2
            first_half = period_executions[:mid]
            second_half = period_executions[mid:]

            first_success = sum(1 for e in first_half if e.get("success")) / len(first_half)
            second_success = sum(1 for e in second_half if e.get("success")) / len(second_half)
            success_change = second_success - first_success

            first_duration = statistics.mean([e["duration"] for e in first_half])
            second_duration = statistics.mean([e["duration"] for e in second_half])
            duration_change = second_duration - first_duration

            if abs(success_change) < 0.05:
                success_direction = "stable"
            elif success_change > 0:
                success_direction = "improving"
            else:
                success_direction = "declining"

            if abs(duration_change) < 1:
                duration_direction = "stable"
            elif duration_change < 0:
                duration_direction = "improving"
            else:
                duration_direction = "declining"

            confidence = min(1.0, len(period_executions) / 20)

            trends.append(HealthTrend(
                period=period_name,
                success_rate_change=success_change,
                duration_change=duration_change,
                trend_direction=success_direction,
                confidence=confidence
            ))

        return trends

    def _detect_anomalies(self, executions: List[Dict]) -> List[AnomalyDetection]:
        """异常检测"""
        if len(executions) < 10:
            return []

        anomalies = []

        success_values = [1 if e.get("success") else 0 for e in executions]
        recent_success = success_values[-5:]
        older_success = success_values[:-5]

        if older_success:
            older_rate = sum(older_success) / len(older_success)
            recent_rate = sum(recent_success) / len(recent_success)

            if recent_rate < older_rate - 0.3:
                anomalies.append(AnomalyDetection(
                    detected_at=time.time(),
                    anomaly_type="drop",
                    metric="success_rate",
                    expected_value=older_rate,
                    actual_value=recent_rate,
                    deviation=older_rate - recent_rate
                ))

        durations = [e["duration"] for e in executions]
        if len(durations) >= 10:
            mean = statistics.mean(durations)
            std = statistics.stdev(durations) if len(durations) > 1 else 0

            recent_durations = [e["duration"] for e in executions[-3:]]
            recent_mean = statistics.mean(recent_durations)

            if std > 0 and (recent_mean - mean) / std > 2:
                anomalies.append(AnomalyDetection(
                    detected_at=time.time(),
                    anomaly_type="spike",
                    metric="duration",
                    expected_value=mean,
                    actual_value=recent_mean,
                    deviation=recent_mean - mean
                ))

        return anomalies

    def _analyze_issues(self, executions: List[Dict]) -> List[HealthIssue]:
        """分析问题"""
        issues = []

        errors = defaultdict(list)
        for e in executions:
            if not e.get("success") and e.get("error"):
                errors[e["error"]].append(e["timestamp"])

        for error_msg, timestamps in errors.items():
            count = len(timestamps)
            if count < 2:
                continue

            severity = IssueSeverity.LOW
            if count > 5:
                severity = IssueSeverity.CRITICAL
            elif count > 3:
                severity = IssueSeverity.HIGH
            elif count > 1:
                severity = IssueSeverity.MEDIUM

            root_cause = self._infer_root_cause(error_msg)

            issues.append(HealthIssue(
                issue_id=f"issue_{len(issues) + 1}",
                issue_type="recurring_error",
                severity=severity,
                root_cause=root_cause,
                title=f"重复错误: {error_msg[:50]}...",
                description=f"该错误已出现 {count} 次",
                location="工作流执行",
                suggestion=self._get_suggestion_for_error(root_cause, error_msg),
                auto_fixable=root_cause in [RootCause.CONFIG, RootCause.TIMEOUT],
                impact={"failure_count": count, "last_occurrence": max(timestamps)}
            ))

        step_durations = defaultdict(list)
        for e in executions:
            for i, step in enumerate(e.get("step_results", [])):
                step_name = step.get("name", f"Step {i+1}")
                if "duration" in step:
                    step_durations[step_name].append(step["duration"])

        for step_name, durations in step_durations.items():
            if len(durations) >= 3:
                avg = statistics.mean(durations)
                max_d = max(durations)

                if avg > 10:
                    severity = IssueSeverity.MEDIUM
                    if avg > 30:
                        severity = IssueSeverity.HIGH

                    issues.append(HealthIssue(
                        issue_id=f"issue_slow_{len(issues) + 1}",
                        issue_type="slow_step",
                        severity=severity,
                        root_cause=RootCause.PERFORMANCE,
                        title=f"慢步骤: {step_name}",
                        description=f"平均耗时 {avg:.1f}秒，最长 {max_d:.1f}秒",
                        location=step_name,
                        suggestion="考虑优化步骤或增加并行执行",
                        auto_fixable=False
                    ))

        severity_order = {
            IssueSeverity.CRITICAL: 0,
            IssueSeverity.HIGH: 1,
            IssueSeverity.MEDIUM: 2,
            IssueSeverity.LOW: 3,
            IssueSeverity.INFO: 4
        }
        issues.sort(key=lambda i: severity_order[i.severity])

        return issues

    def _infer_root_cause(self, error_msg: str) -> RootCause:
        """推断根本原因"""
        error_lower = error_msg.lower()

        if any(kw in error_lower for kw in ["timeout", "超时", "timed out"]):
            return RootCause.TIMEOUT
        elif any(kw in error_lower for kw in ["network", "网络", "connection", "连接"]):
            return RootCause.NETWORK
        elif any(kw in error_lower for kw in ["permission", "权限", "denied", "拒绝"]):
            return RootCause.PERMISSION
        elif any(kw in error_lower for kw in ["not found", "不存在", "404"]):
            return RootCause.CONFIG
        elif any(kw in error_lower for kw in ["memory", "内存", "cpu", "资源"]):
            return RootCause.RESOURCE
        elif any(kw in error_lower for kw in ["import", "module", "dependency"]):
            return RootCause.DEPENDENCY
        elif any(kw in error_lower for kw in ["config", "配置", "setting"]):
            return RootCause.CONFIG
        else:
            return RootCause.UNKNOWN

    def _get_suggestion_for_error(self, root_cause: RootCause,
                                  error_msg: str) -> str:
        """获取错误建议"""
        suggestions = {
            RootCause.TIMEOUT: "增加超时时间或优化网络连接",
            RootCause.NETWORK: "检查网络稳定性，考虑添加重试机制",
            RootCause.PERMISSION: "检查权限设置，确保有足够的访问权限",
            RootCause.CONFIG: "检查配置文件，确保路径和参数正确",
            RootCause.RESOURCE: "优化资源使用，考虑增加系统资源",
            RootCause.DEPENDENCY: "检查依赖包版本，确保兼容性",
            RootCause.ENVIRONMENT: "检查运行环境，确保环境配置正确",
            RootCause.USER_INPUT: "检查用户输入，确保输入数据有效",
            RootCause.UNKNOWN: "查看详细错误信息，联系技术支持"
        }
        return suggestions.get(root_cause, suggestions[RootCause.UNKNOWN])

    def _analyze_root_causes(self, executions: List[Dict]) -> Dict[str, int]:
        """分析根本原因"""
        root_causes = defaultdict(int)

        for e in executions:
            if not e.get("success") and e.get("error"):
                cause = self._infer_root_cause(e["error"])
                root_causes[cause.value] += 1

        return dict(root_causes)

    def _calculate_step_metrics(self, executions: List[Dict]) -> List[StepMetrics]:
        """计算步骤指标"""
        step_data = defaultdict(lambda: {"durations": [], "success": 0, "failure": 0, "errors": []})

        for e in executions:
            for i, step in enumerate(e.get("step_results", [])):
                step_name = step.get("name", f"Step {i+1}")

                if "duration" in step:
                    step_data[step_name]["durations"].append(step["duration"])

                if step.get("success", e.get("success", True)):
                    step_data[step_name]["success"] += 1
                else:
                    step_data[step_name]["failure"] += 1
                    if step.get("error"):
                        step_data[step_name]["errors"].append(step["error"])

        metrics = []
        for name, data in step_data.items():
            durations = data["durations"]
            count = len(durations)

            if count == 0:
                continue

            p50 = statistics.median(durations) if durations else 0
            sorted_durations = sorted(durations)
            p95_idx = int(count * 0.95)
            p95 = sorted_durations[p95_idx] if sorted_durations else 0

            metrics.append(StepMetrics(
                step_name=name,
                step_index=len(metrics),
                avg_duration=statistics.mean(durations) if durations else 0,
                min_duration=min(durations) if durations else 0,
                max_duration=max(durations) if durations else 0,
                std_duration=statistics.stdev(durations) if count > 1 else 0,
                execution_count=data["success"] + data["failure"],
                success_count=data["success"],
                failure_count=data["failure"],
                error_messages=list(set(data["errors"]))[:3],
                p50_duration=p50,
                p95_duration=p95
            ))

        return sorted(metrics, key=lambda m: m.step_index)

    def _generate_recommendations(self, issues: List[HealthIssue],
                                  trends: List[HealthTrend],
                                  root_causes: Dict[str, int],
                                  step_metrics: List[StepMetrics]) -> List[Dict]:
        """生成建议"""
        recommendations = []

        for issue in issues[:5]:
            priority = "high" if issue.severity in [IssueSeverity.CRITICAL, IssueSeverity.HIGH] else "medium"
            recommendations.append({
                "priority": priority,
                "issue": issue.title,
                "suggestion": issue.suggestion,
                "auto_fixable": issue.auto_fixable
            })

        for trend in trends:
            if trend.trend_direction == "declining" and trend.confidence > 0.5:
                recommendations.append({
                    "priority": "high",
                    "suggestion": f"检测到{trend.period}内成功率下降趋势，建议检查最近是否有变化",
                    "trend": f"成功率变化: {trend.success_rate_change:.1%}"
                })

        if root_causes:
            top_cause = max(root_causes.items(), key=lambda x: x[1])
            if top_cause[1] >= 3:
                recommendations.append({
                    "priority": "medium",
                    "suggestion": f"主要问题根因: {top_cause[0]}，建议重点排查"
                })

        slow_steps = [m for m in step_metrics if m.avg_duration > 10]
        if slow_steps:
            recommendations.append({
                "priority": "low",
                "suggestion": f"发现 {len(slow_steps)} 个慢步骤，建议优化"
            })

        return recommendations

    def _predict_next_failure(self, executions: List[Dict]) -> Optional[str]:
        """预测下一次失败"""
        if len(executions) < 10:
            return None

        recent = executions[-5:]
        failures = [e for e in recent if not e.get("success")]

        if len(failures) >= 2:
            if failures[-1].get("error"):
                return failures[-1]["error"][:100]

        now = datetime.now()
        hour = now.hour

        failure_hours = []
        for e in executions:
            if not e.get("success"):
                hour = datetime.fromtimestamp(e["timestamp"]).hour
                failure_hours.append(hour)

        if failure_hours and hour in failure_hours:
            return f"当前小时({hour}时)历史失败率较高"

        return None

    def _predict_duration(self, executions: List[Dict]) -> Optional[float]:
        """预测执行时长"""
        if len(executions) < 3:
            return None

        recent = executions[-5:]
        durations = [e["duration"] for e in recent]

        weights = [1, 2, 3, 4, 5]
        weighted_sum = sum(d * w for d, w in zip(durations, weights[:len(durations)]))
        weight_sum = sum(weights[:len(durations)])

        return weighted_sum / weight_sum

    def _calculate_health_score_v2(self, success_rate: float, issue_count: int,
                                    avg_duration: float, trends: List[HealthTrend],
                                    anomalies: List[AnomalyDetection],
                                    issues: List[HealthIssue] = None) -> float:
        """计算健康分数 v2"""
        score = success_rate * 50

        issues = issues or []
        critical_issues = sum(1 for i in issues if i.severity in [IssueSeverity.CRITICAL, IssueSeverity.HIGH])
        score -= critical_issues * 10
        score -= max(0, (issue_count - critical_issues) * 2)

        for trend in trends:
            if trend.trend_direction == "improving":
                score += 10 * trend.confidence
            elif trend.trend_direction == "declining":
                score -= 10 * trend.confidence

        score -= len(anomalies) * 5

        if avg_duration < 5:
            score += 20
        elif avg_duration < 10:
            score += 15
        elif avg_duration < 30:
            score += 10
        elif avg_duration < 60:
            score += 5

        return min(100, max(0, score))

    def _get_health_level(self, score: float) -> HealthLevel:
        """获取健康等级"""
        if score >= 90:
            return HealthLevel.EXCELLENT
        elif score >= 75:
            return HealthLevel.GOOD
        elif score >= 50:
            return HealthLevel.FAIR
        elif score >= 25:
            return HealthLevel.POOR
        else:
            return HealthLevel.CRITICAL

    def _compare_to_average(self, workflow_id: str, health_score: float = None) -> Optional[float]:
        """与平均水平对比"""
        all_workflow_ids = list(self.execution_history.keys())
        if len(all_workflow_ids) <= 1 or health_score is None:
            return None

        other_ids = [wid for wid in all_workflow_ids if wid != workflow_id]
        if not other_ids:
            return None

        scores = []
        for wid in other_ids:
            executions = self.execution_history.get(wid, [])
            if executions:
                success = sum(1 for e in executions if e.get("success"))
                score = (success / len(executions)) * 50 + 30
                scores.append(score)

        if not scores:
            return None

        avg_score = sum(scores) / len(scores)
        return health_score - avg_score

    def generate_report_text(self, report: HealthReport) -> str:
        """生成报告文本"""
        lines = []

        lines.append("=" * 60)
        lines.append(f"📊 工作流健康诊断报告 (v23 增强版)")
        lines.append("=" * 60)

        lines.append(f"\n工作流: {report.workflow_name}")
        lines.append(f"执行次数: {report.execution_count}")
        lines.append(f"成功率: {report.success_rate*100:.1f}%")
        lines.append(f"平均耗时: {report.avg_duration:.1f}秒")
        lines.append(f"中位耗时: {report.median_duration:.1f}秒")

        emoji = {
            HealthLevel.EXCELLENT: "🟢",
            HealthLevel.GOOD: "🟡",
            HealthLevel.FAIR: "🟠",
            HealthLevel.POOR: "🔴",
            HealthLevel.CRITICAL: "⛔"
        }
        lines.append(f"\n🩺 健康等级: {emoji.get(report.overall_health)} {report.overall_health.value}")
        lines.append(f"   健康分数: {report.health_score:.1f}/100")

        if report.comparison_to_average is not None:
            comparison = "↑" if report.comparison_to_average > 0 else "↓"
            lines.append(f"   与平均对比: {comparison}{abs(report.comparison_to_average):.1f}分")

        if report.trends:
            lines.append(f"\n📈 趋势分析:")
            for trend in report.trends:
                icon = "↗️" if trend.trend_direction == "improving" else "↘️" if trend.trend_direction == "declining" else "➡️"
                lines.append(f"   {trend.period}: {icon} 成功率 {trend.success_rate_change:+.1%}")

        if report.anomalies:
            lines.append(f"\n⚠️ 异常检测:")
            for anomaly in report.anomalies:
                lines.append(f"   - {anomaly.anomaly_type}: {anomaly.metric} 偏离 {anomaly.deviation:.2f}")

        if report.issues:
            lines.append(f"\n❌ 发现问题 ({len(report.issues)}个):")
            for issue in report.issues[:5]:
                icon = "🔴" if issue.severity == IssueSeverity.CRITICAL else "🟠" if issue.severity == IssueSeverity.HIGH else "🟡"
                lines.append(f"   {icon} [{issue.severity.value}] {issue.title}")
                lines.append(f"      💡 {issue.suggestion}")

        if report.root_causes:
            lines.append(f"\n🔍 根因分析:")
            for cause, count in sorted(report.root_causes.items(), key=lambda x: -x[1]):
                lines.append(f"   - {cause}: {count}次")

        if report.step_metrics:
            lines.append(f"\n📊 步骤性能 TOP5:")
            for m in report.step_metrics[:5]:
                success_rate = m.success_count / m.execution_count if m.execution_count else 0
                lines.append(f"   • {m.step_name}:")
                lines.append(f"     执行 {m.execution_count}次, 成功率 {success_rate:.0%}")
                lines.append(f"     耗时: {m.avg_duration:.1f}秒 (P50: {m.p50_duration:.1f}s, P95: {m.p95_duration:.1f}s)")

        if report.recommendations:
            lines.append(f"\n💡 优化建议:")
            for rec in report.recommendations[:5]:
                priority_icon = "🔴" if rec.get("priority") == "high" else "🟡"
                lines.append(f"   {priority_icon} {rec['suggestion']}")

        if report.predicted_next_failure:
            lines.append(f"\n🔮 故障预测:")
            lines.append(f"   可能的失败原因: {report.predicted_next_failure}")

        if report.predicted_duration:
            lines.append(f"   预测下次耗时: {report.predicted_duration:.1f}秒")

        # New: Performance profile
        if report.performance_profile:
            lines.append(f"\n⚡ 性能剖析:")
            for p in report.performance_profile.get("profiles", [])[:5]:
                lines.append(f"   • {p['action_name']}: {p['avg_time']:.3f}s avg, {p['p95_time']:.3f}s P95")

        # New: Resource usage
        if report.resource_usage and report.resource_usage.get("status") != "no_data":
            ru = report.resource_usage
            lines.append(f"\n💾 资源使用:")
            lines.append(f"   CPU: {ru.get('cpu_percent_avg', 0):.1f}% avg / {ru.get('cpu_percent_max', 0):.1f}% max")
            lines.append(f"   Memory: {ru.get('memory_mb_avg', 0):.0f}MB avg / {ru.get('memory_mb_max', 0):.0f}MB max")

        lines.append(f"\n{'=' * 60}")
        lines.append(f"生成时间: {datetime.fromtimestamp(report.generated_at).strftime('%Y-%m-%d %H:%M:%S')}")

        return "\n".join(lines)

    def get_all_workflows_health(self) -> List[HealthReport]:
        """获取所有工作流健康状态"""
        reports = []
        for workflow_id in self.execution_history.keys():
            report = self.diagnose(workflow_id)
            reports.append(report)
        return sorted(reports, key=lambda r: r.health_score)

    def get_health_summary(self) -> Dict[str, Any]:
        """获取健康概览"""
        reports = self.get_all_workflows_health()

        if not reports:
            return {"total_workflows": 0}

        distribution = defaultdict(int)
        for r in reports:
            distribution[r.overall_health.value] += 1

        return {
            "total_workflows": len(reports),
            "avg_health_score": sum(r.health_score for r in reports) / len(reports),
            "health_distribution": dict(distribution),
            "avg_success_rate": sum(r.success_rate for r in reports) / len(reports),
            "avg_duration": sum(r.avg_duration for r in reports) / len(reports),
            "needs_attention": [r.workflow_name for r in reports if r.health_score < 50]
        }

    @property
    def health_level(self) -> str:
        """获取所有工作流的总体健康等级"""
        reports = self.get_all_workflows_health()
        if not reports:
            return HealthLevel.FAIR.value

        avg_score = sum(r.health_score for r in reports) / len(reports)
        return self._get_health_level(avg_score).value


# Compatibility
WorkflowDiagnostics = WorkflowDiagnosticsV2


def create_diagnostics(data_dir: str = "./data") -> WorkflowDiagnosticsV2:
    """创建诊断系统实例"""
    return WorkflowDiagnosticsV2(data_dir)


# Test
if __name__ == "__main__":
    diag = create_diagnostics("./data")

    # Simulate execution records
    for i in range(20):
        success = i < 16
        error = None if success else "Connection timeout after 30s"

        diag.record_execution(
            "wf_test",
            "测试工作流",
            [
                {"name": "打开应用", "duration": 2.5, "success": True},
                {"name": "点击按钮", "duration": 1.2 + (i % 3), "success": success},
                {"name": "保存结果", "duration": 3.0, "success": True}
            ],
            6.7 + (i % 5),
            success,
            error
        )

    # Run diagnostics
    report = diag.diagnose("wf_test")

    # Output report
    print(diag.generate_report_text(report))

    # Health overview
    summary = diag.get_health_summary()
    print(f"\n=== 健康概览 ===")
    print(f"总工作流数: {summary['total_workflows']}")
    print(f"平均健康分: {summary['avg_health_score']:.1f}")
    print(f"健康分布: {summary['health_distribution']}")

    # Run full diagnostics
    print("\n=== 运行完整诊断 ===")
    full_report = diag.run_full_diagnostics("wf_test")
    print(f"Overall status: {full_report.overall_status.value}")
    print(f"Total issues: {full_report.total_issues}")

    # Generate HTML report
    html_path = diag.generate_html_report("wf_test")
    print(f"\nHTML报告已生成: {html_path}")
