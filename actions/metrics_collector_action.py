"""Metrics collector action module for RabAI AutoClick.

Provides metrics operations:
- MetricsCollectAction: Collect metrics
- MetricsAggregateAction: Aggregate metrics
- MetricsStoreAction: Store metrics
- MetricsQueryAction: Query metrics
- MetricsAlertAction: Alert on metrics thresholds
- MetricsExportAction: Export metrics
- MetricsDashboardAction: Generate dashboard data
- MetricsAnomalyAction: Detect anomalies
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricsStore:
    """In-memory metrics storage."""
    
    _metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    @classmethod
    def add(cls, metric: str, value: float, tags: Dict[str, str] = None) -> None:
        """Add a metric value."""
        cls._metrics[metric].append({
            "value": value,
            "timestamp": time.time(),
            "tags": tags or {}
        })
    
    @classmethod
    def query(cls, metric: str, start_time: float = None, end_time: float = None) -> List[Dict[str, Any]]:
        """Query metric values."""
        if metric not in cls._metrics:
            return []
        
        results = cls._metrics[metric]
        if start_time:
            results = [r for r in results if r["timestamp"] >= start_time]
        if end_time:
            results = [r for r in results if r["timestamp"] <= end_time]
        
        return results
    
    @classmethod
    def list_metrics(cls) -> List[str]:
        """List all metric names."""
        return list(cls._metrics.keys())
    
    @classmethod
    def clear(cls) -> None:
        """Clear all metrics."""
        cls._metrics.clear()


class MetricsCollectAction(BaseAction):
    """Collect metrics."""
    action_type = "metrics_collect"
    display_name = "指标采集"
    description = "采集系统指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "system")
            metrics = params.get("metrics", [])
            tags = params.get("tags", {})
            
            collected = {}
            
            if source == "system" or not source:
                import psutil
                
                for metric in metrics:
                    if metric == "cpu":
                        collected["cpu_percent"] = psutil.cpu_percent(interval=0.1)
                    elif metric == "memory":
                        mem = psutil.virtual_memory()
                        collected["memory_percent"] = mem.percent
                        collected["memory_available_mb"] = mem.available / (1024 * 1024)
                    elif metric == "disk":
                        disk = psutil.disk_usage("/")
                        collected["disk_percent"] = disk.percent
                    elif metric == "network":
                        net = psutil.net_io_counters()
                        collected["bytes_sent"] = net.bytes_sent
                        collected["bytes_recv"] = net.bytes_recv
            
            for metric_name, value in collected.items():
                MetricsStore.add(metric_name, value, tags)
            
            return ActionResult(
                success=True,
                message=f"Collected {len(collected)} metrics",
                data={"collected": collected, "source": source}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics collect failed: {str(e)}")


class MetricsAggregateAction(BaseAction):
    """Aggregate metrics."""
    action_type = "metrics_aggregate"
    display_name = "指标聚合"
    description = "聚合指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric = params.get("metric", "")
            aggregation = params.get("aggregation", "avg")
            window = params.get("window", 60)
            
            if not metric:
                return ActionResult(success=False, message="metric required")
            
            end_time = time.time()
            start_time = end_time - window
            
            values = MetricsStore.query(metric, start_time, end_time)
            
            if not values:
                return ActionResult(success=False, message=f"No data for metric: {metric}")
            
            data_points = [v["value"] for v in values]
            
            if aggregation == "avg":
                result = sum(data_points) / len(data_points)
            elif aggregation == "sum":
                result = sum(data_points)
            elif aggregation == "min":
                result = min(data_points)
            elif aggregation == "max":
                result = max(data_points)
            elif aggregation == "count":
                result = len(data_points)
            elif aggregation == "last":
                result = data_points[-1]
            else:
                return ActionResult(success=False, message=f"Unknown aggregation: {aggregation}")
            
            return ActionResult(
                success=True,
                message=f"{aggregation} of {metric}: {result:.2f}",
                data={"metric": metric, "aggregation": aggregation, "value": result, "data_points": len(data_points)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics aggregate failed: {str(e)}")


class MetricsStoreAction(BaseAction):
    """Store metrics."""
    action_type = "metrics_store"
    display_name = "指标存储"
    description = "存储指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric = params.get("metric", "")
            value = params.get("value")
            tags = params.get("tags", {})
            
            if not metric or value is None:
                return ActionResult(success=False, message="metric and value required")
            
            MetricsStore.add(metric, float(value), tags)
            
            return ActionResult(
                success=True,
                message=f"Stored metric: {metric}={value}",
                data={"metric": metric, "value": value, "tags": tags}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics store failed: {str(e)}")


class MetricsQueryAction(BaseAction):
    """Query metrics."""
    action_type = "metrics_query"
    display_name = "指标查询"
    description = "查询指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric = params.get("metric", "")
            start_time = params.get("start_time")
            end_time = params.get("end_time")
            
            if not metric:
                return ActionResult(success=False, message="metric required")
            
            start_ts = datetime.fromisoformat(start_time).timestamp() if start_time else None
            end_ts = datetime.fromisoformat(end_time).timestamp() if end_time else None
            
            results = MetricsStore.query(metric, start_ts, end_ts)
            
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} points",
                data={"metric": metric, "data_points": results[:100], "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics query failed: {str(e)}")


class MetricsAlertAction(BaseAction):
    """Alert on metrics thresholds."""
    action_type = "metrics_alert"
    display_name = "指标告警"
    description = "指标阈值告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric = params.get("metric", "")
            threshold = params.get("threshold", 0)
            condition = params.get("condition", "above")
            duration = params.get("duration", 60)
            
            if not metric:
                return ActionResult(success=False, message="metric required")
            
            end_time = time.time()
            start_time = end_time - duration
            
            values = MetricsStore.query(metric, start_time, end_time)
            
            if not values:
                return ActionResult(success=False, message=f"No data for metric: {metric}")
            
            triggered = False
            for v in values:
                if condition == "above" and v["value"] > threshold:
                    triggered = True
                    break
                elif condition == "below" and v["value"] < threshold:
                    triggered = True
                    break
            
            return ActionResult(
                success=True,
                message=f"Alert {'triggered' if triggered else 'not triggered'} for {metric}",
                data={
                    "metric": metric,
                    "threshold": threshold,
                    "condition": condition,
                    "triggered": triggered,
                    "data_points": len(values)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics alert failed: {str(e)}")


class MetricsExportAction(BaseAction):
    """Export metrics."""
    action_type = "metrics_export"
    display_name = "指标导出"
    description = "导出指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metrics = params.get("metrics", [])
            format = params.get("format", "json")
            output_path = params.get("output_path", "/tmp/metrics_export.json")
            
            all_metrics = MetricsStore.list_metrics() if not metrics else metrics
            
            export_data = {}
            for metric in all_metrics:
                export_data[metric] = MetricsStore.query(metric)
            
            if format == "json":
                with open(output_path, "w") as f:
                    json.dump(export_data, f, indent=2)
            elif format == "csv":
                import csv
                with open(output_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["metric", "timestamp", "value"])
                    for metric, values in export_data.items():
                        for v in values:
                            writer.writerow([metric, v["timestamp"], v["value"]])
            
            return ActionResult(
                success=True,
                message=f"Exported {len(all_metrics)} metrics to {output_path}",
                data={"output_path": output_path, "metrics": all_metrics, "format": format}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics export failed: {str(e)}")


class MetricsDashboardAction(BaseAction):
    """Generate dashboard data."""
    action_type = "metrics_dashboard"
    display_name = "指标面板"
    description = "生成仪表盘数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metrics = params.get("metrics", [])
            window = params.get("window", 3600)
            
            if not metrics:
                metrics = MetricsStore.list_metrics()
            
            dashboard = {}
            for metric in metrics:
                values = MetricsStore.query(metric, time.time() - window)
                if values:
                    data_points = [v["value"] for v in values]
                    dashboard[metric] = {
                        "current": data_points[-1] if data_points else None,
                        "avg": sum(data_points) / len(data_points),
                        "min": min(data_points),
                        "max": max(data_points),
                        "count": len(data_points)
                    }
            
            return ActionResult(
                success=True,
                message=f"Dashboard: {len(dashboard)} metrics",
                data={"dashboard": dashboard, "window": window}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dashboard generation failed: {str(e)}")


class MetricsAnomalyAction(BaseAction):
    """Detect anomalies in metrics."""
    action_type = "metrics_anomaly"
    display_name = "异常检测"
    description = "检测指标异常"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric = params.get("metric", "")
            sensitivity = params.get("sensitivity", 2.0)
            window = params.get("window", 300)
            
            if not metric:
                return ActionResult(success=False, message="metric required")
            
            values = MetricsStore.query(metric, time.time() - window)
            
            if len(values) < 3:
                return ActionResult(success=False, message="Not enough data points for anomaly detection")
            
            data_points = [v["value"] for v in values]
            mean = sum(data_points) / len(data_points)
            variance = sum((x - mean) ** 2 for x in data_points) / len(data_points)
            std_dev = variance ** 0.5
            
            threshold = sensitivity * std_dev
            anomalies = []
            
            for i, v in enumerate(data_points):
                if abs(v["value"] - mean) > threshold:
                    anomalies.append({
                        "index": i,
                        "value": v["value"],
                        "timestamp": v["timestamp"],
                        "deviation": abs(v["value"] - mean) / std_dev if std_dev > 0 else 0
                    })
            
            return ActionResult(
                success=True,
                message=f"Found {len(anomalies)} anomalies",
                data={"metric": metric, "anomalies": anomalies, "mean": mean, "std_dev": std_dev}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Anomaly detection failed: {str(e)}")
