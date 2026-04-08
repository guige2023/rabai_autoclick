"""Automation analytics and metrics action module for RabAI AutoClick.

Provides:
- AutomationAnalyticsAction: Analytics and metrics for automation
- AutomationBenchmarkAction: Benchmark automation performance
- AutomationProfilerAction: Profile automation execution
- AutomationHealthCheckAction: Health checks for automation systems
"""

import time
import json
import hashlib
import math
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HealthStatus(str, Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class AutomationAnalyticsAction(BaseAction):
    """Analytics and metrics for automation workflows."""
    action_type = "automation_analytics"
    display_name = "自动化分析"
    description = "工作流分析与指标"

    def __init__(self):
        super().__init__()
        self._analytics: Dict[str, List[Dict]] = {}
        self._workflow_metrics: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "track")
            workflow_id = params.get("workflow_id", "")

            if operation == "track":
                if not workflow_id:
                    return ActionResult(success=False, message="workflow_id required")

                if workflow_id not in self._analytics:
                    self._analytics[workflow_id] = []
                if workflow_id not in self._workflow_metrics:
                    self._workflow_metrics[workflow_id] = {"runs": 0, "successes": 0, "failures": 0, "total_duration": 0}

                run_data = {
                    "timestamp": time.time(),
                    "duration": params.get("duration", 0),
                    "success": params.get("success", True),
                    "steps_completed": params.get("steps_completed", 0),
                    "steps_total": params.get("steps_total", 0),
                    "records_processed": params.get("records_processed", 0),
                    "error_count": params.get("error_count", 0),
                    "resource_usage": params.get("resource_usage", {})
                }

                self._analytics[workflow_id].append(run_data)
                if len(self._analytics[workflow_id]) > 10000:
                    self._analytics[workflow_id] = self._analytics[workflow_id][-10000:]

                metrics = self._workflow_metrics[workflow_id]
                metrics["runs"] += 1
                metrics["total_duration"] += run_data["duration"]
                if run_data["success"]:
                    metrics["successes"] += 1
                else:
                    metrics["failures"] += 1

                return ActionResult(success=True, data={"tracked": run_data})

            elif operation == "dashboard":
                if workflow_id:
                    if workflow_id not in self._workflow_metrics:
                        return ActionResult(success=False, message=f"No data for '{workflow_id}'")

                    metrics = self._workflow_metrics[workflow_id]
                    history = self._analytics.get(workflow_id, [])
                    recent = history[-100:] if len(history) > 100 else history

                    if recent:
                        avg_duration = sum(r["duration"] for r in recent) / len(recent)
                        avg_records = sum(r.get("records_processed", 0) for r in recent) / len(recent)
                        total_errors = sum(r.get("error_count", 0) for r in recent)
                        success_rate = metrics["successes"] / metrics["runs"] if metrics["runs"] > 0 else 0
                    else:
                        avg_duration = avg_records = total_errors = success_rate = 0

                    p50 = self._percentile([r["duration"] for r in recent], 50) if recent else 0
                    p95 = self._percentile([r["duration"] for r in recent], 95) if recent else 0
                    p99 = self._percentile([r["duration"] for r in recent], 99) if recent else 0

                    return ActionResult(
                        success=True,
                        data={
                            "workflow_id": workflow_id,
                            "total_runs": metrics["runs"],
                            "success_rate": round(success_rate, 4),
                            "avg_duration_ms": round(avg_duration, 2),
                            "avg_records": round(avg_records, 1),
                            "total_errors": total_errors,
                            "p50_ms": round(p50, 2),
                            "p95_ms": round(p95, 2),
                            "p99_ms": round(p99, 2)
                        }
                    )

                all_dashboards = {}
                for wid, mm in self._workflow_metrics.items():
                    all_dashboards[wid] = {
                        "runs": mm["runs"],
                        "success_rate": round(mm["successes"] / mm["runs"], 4) if mm["runs"] > 0 else 0
                    }
                return ActionResult(success=True, data={"workflows": all_dashboards})

            elif operation == "trends":
                if workflow_id not in self._analytics:
                    return ActionResult(success=False, message=f"No data for '{workflow_id}'")

                history = self._analytics[workflow_id]
                now = time.time()
                buckets = params.get("buckets", 24)
                interval = 3600 / buckets if buckets > 0 else 3600

                trends = []
                for i in range(buckets):
                    start = now - (buckets - i) * interval
                    end = start + interval
                    bucket_runs = [r for r in history if start <= r["timestamp"] < end]
                    if bucket_runs:
                        trends.append({
                            "bucket": i,
                            "start_time": start,
                            "runs": len(bucket_runs),
                            "avg_duration": sum(r["duration"] for r in bucket_runs) / len(bucket_runs),
                            "success_count": sum(1 for r in bucket_runs if r["success"])
                        })
                    else:
                        trends.append({"bucket": i, "start_time": start, "runs": 0})

                return ActionResult(success=True, data={"workflow_id": workflow_id, "trends": trends})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Analytics error: {str(e)}")

    def _percentile(self, values: List[float], percentile: int) -> float:
        if not values:
            return 0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]


class AutomationBenchmarkAction(BaseAction):
    """Benchmark automation performance."""
    action_type = "automation_benchmark"
    display_name = "自动化基准测试"
    description = "性能基准测试"

    def __init__(self):
        super().__init__()
        self._benchmarks: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "run")
            benchmark_name = params.get("benchmark_name", "")

            if operation == "define":
                if not benchmark_name:
                    return ActionResult(success=False, message="benchmark_name required")

                self._benchmarks[benchmark_name] = {
                    "name": benchmark_name,
                    "tests": params.get("tests", []),
                    "created_at": time.time(),
                    "last_run": None,
                    "results": []
                }
                return ActionResult(success=True, data={"benchmark": benchmark_name}, message=f"Benchmark '{benchmark_name}' defined")

            elif operation == "run":
                if not benchmark_name:
                    return ActionResult(success=False, message="benchmark_name required")

                if benchmark_name not in self._benchmarks:
                    return ActionResult(success=False, message=f"Benchmark '{benchmark_name}' not found")

                bench = self._benchmarks[benchmark_name]
                iterations = params.get("iterations", 10)
                warmup = params.get("warmup", 2)

                test_results = []
                for test in bench["tests"]:
                    test_name = test.get("name", "unnamed")
                    test_func = test.get("func", "sleep")
                    test_config = test.get("config", {})

                    for _ in range(warmup):
                        self._run_test(test_func, test_config)

                    durations = []
                    for _ in range(iterations):
                        start = time.time()
                        self._run_test(test_func, test_config)
                        durations.append(time.time() - start)

                    test_results.append({
                        "name": test_name,
                        "iterations": iterations,
                        "min_ms": round(min(durations) * 1000, 3),
                        "max_ms": round(max(durations) * 1000, 3),
                        "avg_ms": round(sum(durations) / len(durations) * 1000, 3),
                        "p50_ms": round(self._percentile(durations, 50) * 1000, 3),
                        "p95_ms": round(self._percentile(durations, 95) * 1000, 3),
                        "p99_ms": round(self._percentile(durations, 99) * 1000, 3)
                    })

                bench["last_run"] = time.time()
                bench["results"] = test_results

                return ActionResult(
                    success=True,
                    data={"benchmark": benchmark_name, "results": test_results},
                    message=f"Benchmark '{benchmark_name}' completed: {len(test_results)} tests"
                )

            elif operation == "compare":
                benchmarks = params.get("benchmarks", [])
                comparison = {}
                for bname in benchmarks:
                    if bname in self._benchmarks and self._benchmarks[bname]["results"]:
                        comparison[bname] = self._benchmarks[bname]["results"]
                return ActionResult(success=True, data={"comparison": comparison})

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={"benchmarks": [{"name": k, "last_run": v["last_run"]} for k, v in self._benchmarks.items()]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Benchmark error: {str(e)}")

    def _run_test(self, test_func: str, config: Dict) -> None:
        if test_func == "sleep":
            time.sleep(config.get("duration", 0.001))
        elif test_func == "compute":
            _ = sum(i * i for i in range(config.get("iterations", 1000)))
        elif test_func == "json_serialize":
            data = config.get("data", {})
            _ = json.dumps(data)

    def _percentile(self, values: List[float], percentile: int) -> float:
        if not values:
            return 0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]


class AutomationProfilerAction(BaseAction):
    """Profile automation execution."""
    action_type = "automation_profiler"
    display_name = "自动化性能分析"
    description = "执行性能分析"

    def __init__(self):
        super().__init__()
        self._profiles: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "profile")
            profile_name = params.get("profile_name", "")

            if operation == "start":
                if not profile_name:
                    return ActionResult(success=False, message="profile_name required")

                self._profiles[profile_name] = {
                    "name": profile_name,
                    "started_at": time.time(),
                    "events": [],
                    "memory_snapshots": [],
                    "cpu_snapshots": [],
                    "status": "running"
                }
                return ActionResult(success=True, message=f"Profiler '{profile_name}' started")

            elif operation == "record_event":
                if profile_name not in self._profiles:
                    return ActionResult(success=False, message=f"Profiler '{profile_name}' not found")

                self._profiles[profile_name]["events"].append({
                    "timestamp": time.time(),
                    "event_type": params.get("event_type", "marker"),
                    "name": params.get("event_name", ""),
                    "metadata": params.get("metadata", {})
                })
                return ActionResult(success=True, message=f"Event recorded: {params.get('event_name', '')}")

            elif operation == "stop":
                if profile_name not in self._profiles:
                    return ActionResult(success=False, message=f"Profiler '{profile_name}' not found")

                profile = self._profiles[profile_name]
                profile["status"] = "completed"
                profile["ended_at"] = time.time()
                profile["total_duration"] = profile["ended_at"] - profile["started_at"]

                events_by_type = {}
                for event in profile["events"]:
                    etype = event["event_type"]
                    if etype not in events_by_type:
                        events_by_type[etype] = []
                    events_by_type[etype].append(event)

                return ActionResult(
                    success=True,
                    data={
                        "profile": profile_name,
                        "duration_ms": round(profile["total_duration"] * 1000, 2),
                        "total_events": len(profile["events"]),
                        "events_by_type": {k: len(v) for k, v in events_by_type.items()}
                    }
                )

            elif operation == "analyze":
                if profile_name not in self._profiles:
                    return ActionResult(success=False, message=f"Profiler '{profile_name}' not found")

                profile = self._profiles[profile_name]
                events = profile["events"]

                if not events:
                    return ActionResult(success=True, data={"profile": profile_name, "events": []})

                marker_events = [e for e in events if e["event_type"] == "marker"]
                gaps = []
                if marker_events:
                    for i in range(1, len(marker_events)):
                        gap = marker_events[i]["timestamp"] - marker_events[i-1]["timestamp"]
                        gaps.append({"from": marker_events[i-1]["name"], "to": marker_events[i]["name"], "gap_ms": round(gap * 1000, 2)})

                return ActionResult(
                    success=True,
                    data={
                        "profile": profile_name,
                        "event_count": len(events),
                        "largest_gaps": sorted(gaps, key=lambda g: g["gap_ms"], reverse=True)[:5]
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Profiler error: {str(e)}")


class AutomationHealthCheckAction(BaseAction):
    """Health checks for automation systems."""
    action_type = "automation_health_check"
    display_name = "自动化健康检查"
    description = "系统健康检查"

    def __init__(self):
        super().__init__()
        self._health_records: Dict[str, Dict] = {}
        self._check_configs: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            component = params.get("component", "")

            if operation == "register":
                if not component:
                    return ActionResult(success=False, message="component required")

                self._check_configs[component] = {
                    "component": component,
                    "check_interval": params.get("check_interval", 60),
                    "timeout": params.get("timeout", 5),
                    "threshold": params.get("threshold", 0.8),
                    "enabled": params.get("enabled", True),
                    "created_at": time.time()
                }
                return ActionResult(success=True, message=f"Health check registered for '{component}'")

            elif operation == "check":
                if not component:
                    return ActionResult(success=False, message="component required")

                if component not in self._check_configs:
                    return ActionResult(success=False, message=f"No health check for '{component}'")

                check_config = self._check_configs[component]
                start = time.time()
                passed = params.get("passed", True)
                details = params.get("details", {})
                response_time = time.time() - start

                health_status = HealthStatus.HEALTHY if passed else HealthStatus.UNHEALTHY

                record = {
                    "component": component,
                    "timestamp": time.time(),
                    "status": health_status.value,
                    "response_time_ms": round(response_time * 1000, 2),
                    "details": details,
                    "passed": passed
                }
                self._health_records[component] = record

                return ActionResult(
                    success=True,
                    data={"component": component, "status": health_status.value, "response_time_ms": record["response_time_ms"]},
                    message=f"Health check for '{component}': {health_status.value}"
                )

            elif operation == "status":
                overall_status = HealthStatus.HEALTHY
                component_statuses = {}

                for comp, config in self._check_configs.items():
                    if comp in self._health_records:
                        record = self._health_records[comp]
                        component_statuses[comp] = record["status"]
                        if record["status"] == HealthStatus.UNHEALTHY.value:
                            overall_status = HealthStatus.DEGRADED
                    else:
                        component_statuses[comp] = HealthStatus.UNKNOWN.value

                healthy_count = sum(1 for s in component_statuses.values() if s == HealthStatus.HEALTHY.value)
                total_count = len(component_statuses)
                health_ratio = healthy_count / total_count if total_count > 0 else 0

                if health_ratio < 0.5:
                    overall_status = HealthStatus.UNHEALTHY
                elif health_ratio < 1.0:
                    overall_status = HealthStatus.DEGRADED

                return ActionResult(
                    success=True,
                    data={
                        "overall_status": overall_status.value,
                        "components": component_statuses,
                        "healthy_count": healthy_count,
                        "total_count": total_count,
                        "uptime_ratio": round(health_ratio, 4)
                    }
                )

            elif operation == "history":
                return ActionResult(
                    success=True,
                    data={"records": list(self._health_records.values())}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Health check error: {str(e)}")
