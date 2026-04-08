"""Data observability action module for RabAI AutoClick.

Provides data observability and monitoring:
- DataObservabilityAction: Track data quality and health
- DataLineageTrackerAction: Track data provenance
- DataQualityMonitorAction: Monitor data quality metrics
- DataFingerprintAction: Generate and track data fingerprints
- DataAuditLogAction: Comprehensive data audit logging
"""

import time
import hashlib
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataQualityLevel(str, Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class DataObservabilityAction(BaseAction):
    """Track data quality, freshness, and health metrics."""
    action_type = "data_observability"
    display_name = "数据可观测性"
    description = "数据质量与健康追踪"

    def __init__(self):
        super().__init__()
        self._datasets: Dict[str, Dict] = {}
        self._metrics_history: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "track")
            dataset_name = params.get("dataset_name", "")

            if operation == "register":
                if not dataset_name:
                    return ActionResult(success=False, message="dataset_name required")

                self._datasets[dataset_name] = {
                    "name": dataset_name,
                    "registered_at": time.time(),
                    "last_observed": None,
                    "schema": params.get("schema", {}),
                    "expected_rows": params.get("expected_rows"),
                    "expected_columns": params.get("expected_columns", []),
                    "quality_threshold": params.get("quality_threshold", 0.8),
                    "freshness_ttl": params.get("freshness_ttl", 3600),
                    "alerts_enabled": params.get("alerts_enabled", True),
                    "observations": 0
                }

                return ActionResult(
                    success=True,
                    data={"dataset": dataset_name},
                    message=f"Dataset '{dataset_name}' registered"
                )

            elif operation == "observe":
                if dataset_name not in self._datasets:
                    return ActionResult(success=False, message=f"Dataset '{dataset_name}' not found")

                dataset = self._datasets[dataset_name]
                dataset["last_observed"] = time.time()
                dataset["observations"] += 1

                current_rows = params.get("row_count", 0)
                current_columns = params.get("columns", [])
                null_counts = params.get("null_counts", {})
                duplicate_count = params.get("duplicate_count", 0)
                data freshness = params.get("freshness", None)

                completeness = self._calculate_completeness(null_counts, len(current_columns))
                uniqueness = self._calculate_uniqueness(duplicate_count, current_rows)
                timeliness = self._calculate_timeliness(dataset, freshness)

                overall_quality = (completeness * 0.4 + uniqueness * 0.3 + timeliness * 0.3)

                quality_level = DataQualityLevel.EXCELLENT
                if overall_quality < 0.5:
                    quality_level = DataQualityLevel.CRITICAL
                elif overall_quality < 0.6:
                    quality_level = DataQualityLevel.POOR
                elif overall_quality < 0.75:
                    quality_level = DataQualityLevel.FAIR
                elif overall_quality < 0.9:
                    quality_level = DataQualityLevel.GOOD

                observation = {
                    "timestamp": time.time(),
                    "quality_score": overall_quality,
                    "quality_level": quality_level.value,
                    "completeness": completeness,
                    "uniqueness": uniqueness,
                    "timeliness": timeliness,
                    "row_count": current_rows,
                    "column_count": len(current_columns)
                }

                if dataset_name not in self._metrics_history:
                    self._metrics_history[dataset_name] = []
                self._metrics_history[dataset_name].append(observation)
                if len(self._metrics_history[dataset_name]) > 1000:
                    self._metrics_history[dataset_name] = self._metrics_history[dataset_name][-1000:]

                alert_triggered = False
                if dataset["alerts_enabled"] and overall_quality < dataset["quality_threshold"]:
                    alert_triggered = True

                return ActionResult(
                    success=True,
                    data={
                        "dataset": dataset_name,
                        "quality_score": round(overall_quality, 4),
                        "quality_level": quality_level.value,
                        "completeness": round(completeness, 4),
                        "uniqueness": round(uniqueness, 4),
                        "timeliness": round(timeliness, 4),
                        "alert_triggered": alert_triggered,
                        "observation_count": dataset["observations"]
                    },
                    message=f"Observation: quality={quality_level.value} ({overall_quality:.2%})"
                )

            elif operation == "status":
                if dataset_name:
                    if dataset_name not in self._datasets:
                        return ActionResult(success=False, message=f"Dataset '{dataset_name}' not found")

                    dataset = self._datasets[dataset_name]
                    history = self._metrics_history.get(dataset_name, [])

                    recent_avg = None
                    if history:
                        recent_avg = sum(h["quality_score"] for h in history[-10:]) / min(len(history), 10)

                    return ActionResult(
                        success=True,
                        data={
                            "dataset": dataset_name,
                            "last_observed": dataset["last_observed"],
                            "observations": dataset["observations"],
                            "recent_avg_quality": round(recent_avg, 4) if recent_avg else None,
                            "quality_trend": self._calculate_trend(history)
                        }
                    )

                else:
                    all_status = {}
                    for name, ds in self._datasets.items():
                        history = self._metrics_history.get(name, [])
                        recent_avg = sum(h["quality_score"] for h in history[-10:]) / min(len(history), 10) if history else None
                        all_status[name] = {
                            "last_observed": ds["last_observed"],
                            "recent_quality": round(recent_avg, 4) if recent_avg else None
                        }
                    return ActionResult(success=True, data={"datasets": all_status})

            elif operation == "anomaly":
                if dataset_name not in self._datasets:
                    return ActionResult(success=False, message=f"Dataset '{dataset_name}' not found")

                history = self._metrics_history.get(dataset_name, [])
                if len(history) < 3:
                    return ActionResult(success=False, message="Not enough history for anomaly detection")

                recent = history[-1]["quality_score"]
                prev_avg = sum(h["quality_score"] for h in history[-5:-1]) / 4

                drop = prev_avg - recent
                is_anomaly = abs(drop) > 0.15

                return ActionResult(
                    success=True,
                    data={
                        "dataset": dataset_name,
                        "is_anomaly": is_anomaly,
                        "current": recent,
                        "baseline": prev_avg,
                        "drop": round(drop, 4)
                    },
                    message=f"Anomaly {'detected' if is_anomaly else 'not detected'}: drop={drop:.2%}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data observability error: {str(e)}")

    def _calculate_completeness(self, null_counts: Dict, total_columns: int) -> float:
        if total_columns == 0:
            return 1.0
        total_nulls = sum(null_counts.values())
        total_cells = total_columns * 100
        if total_cells == 0:
            return 1.0
        return max(0.0, 1.0 - (total_nulls / total_cells))

    def _calculate_uniqueness(self, duplicate_count: int, total_rows: int) -> float:
        if total_rows == 0:
            return 1.0
        return max(0.0, 1.0 - (duplicate_count / total_rows))

    def _calculate_timeliness(self, dataset: Dict, freshness: Optional[float]) -> float:
        if freshness is not None:
            return max(0.0, 1.0 - (freshness / dataset.get("freshness_ttl", 3600)))

        if dataset["last_observed"] is None:
            return 0.0

        age = time.time() - dataset["last_observed"]
        ttl = dataset.get("freshness_ttl", 3600)
        return max(0.0, 1.0 - (age / ttl))

    def _calculate_trend(self, history: List[Dict]) -> str:
        if len(history) < 3:
            return "insufficient_data"
        recent_scores = [h["quality_score"] for h in history[-5:]]
        first = sum(recent_scores[:2]) / 2
        last = sum(recent_scores[-2:]) / 2
        diff = last - first
        if diff > 0.05:
            return "improving"
        elif diff < -0.05:
            return "declining"
        return "stable"


class DataLineageTrackerAction(BaseAction):
    """Track data provenance and lineage."""
    action_type = "data_lineage"
    display_name = "数据血缘追踪"
    description = "数据溯源与血缘追踪"

    def __init__(self):
        super().__init__()
        self._lineage_graph: Dict[str, Dict] = {}
        self._records: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            dataset_id = params.get("dataset_id", "")
            record_id = params.get("record_id", "")

            if operation == "register":
                if not dataset_id:
                    return ActionResult(success=False, message="dataset_id required")

                self._lineage_graph[dataset_id] = {
                    "id": dataset_id,
                    "name": params.get("name", dataset_id),
                    "source_type": params.get("source_type", "unknown"),
                    "parent_ids": params.get("parent_ids", []),
                    "child_ids": [],
                    "created_at": time.time(),
                    "schema": params.get("schema", {}),
                    "owner": params.get("owner", ""),
                    "tags": params.get("tags", [])
                }

                for parent_id in params.get("parent_ids", []):
                    if parent_id in self._lineage_graph:
                        if dataset_id not in self._lineage_graph[parent_id]["child_ids"]:
                            self._lineage_graph[parent_id]["child_ids"].append(dataset_id)

                return ActionResult(
                    success=True,
                    data={"dataset_id": dataset_id, "parents": len(params.get("parent_ids", []))},
                    message=f"Dataset '{dataset_id}' registered in lineage"
                )

            elif operation == "track":
                if not record_id:
                    return ActionResult(success=False, message="record_id required")

                parent_record_ids = params.get("parent_record_ids", [])
                self._records[record_id] = {
                    "record_id": record_id,
                    "dataset_id": dataset_id,
                    "parent_record_ids": parent_record_ids,
                    "created_at": time.time(),
                    "operation": params.get("operation", "created"),
                    "version": params.get("version", "1.0"),
                    "metadata": params.get("metadata", {})
                }

                return ActionResult(
                    success=True,
                    data={"record_id": record_id, "parents": len(parent_record_ids)},
                    message=f"Record '{record_id}' tracked"
                )

            elif operation == "ancestors":
                if dataset_id not in self._lineage_graph:
                    return ActionResult(success=False, message=f"Dataset '{dataset_id}' not found")

                ancestors = self._get_ancestors(dataset_id)
                return ActionResult(
                    success=True,
                    data={"dataset_id": dataset_id, "ancestors": ancestors},
                    message=f"Found {len(ancestors)} ancestors for '{dataset_id}'"
                )

            elif operation == "descendants":
                if dataset_id not in self._lineage_graph:
                    return ActionResult(success=False, message=f"Dataset '{dataset_id}' not found")

                descendants = self._get_descendants(dataset_id)
                return ActionResult(
                    success=True,
                    data={"dataset_id": dataset_id, "descendants": descendants},
                    message=f"Found {len(descendants)} descendants for '{dataset_id}'"
                )

            elif operation == "path":
                if not dataset_id:
                    return ActionResult(success=False, message="dataset_id required")
                target_id = params.get("target_id", "")

                path = self._find_lineage_path(dataset_id, target_id)
                return ActionResult(
                    success=True,
                    data={"from": dataset_id, "to": target_id, "path": path},
                    message=f"Lineage path: {' -> '.join(path) if path else 'no path found'}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Lineage tracker error: {str(e)}")

    def _get_ancestors(self, dataset_id: str, visited: Optional[set] = None) -> List[str]:
        if visited is None:
            visited = set()
        if dataset_id in visited or dataset_id not in self._lineage_graph:
            return []
        visited.add(dataset_id)

        ancestors = list(self._lineage_graph[dataset_id].get("parent_ids", []))
        for parent_id in self._lineage_graph[dataset_id].get("parent_ids", []):
            ancestors.extend(self._get_ancestors(parent_id, visited))
        return list(set(ancestors))

    def _get_descendants(self, dataset_id: str, visited: Optional[set] = None) -> List[str]:
        if visited is None:
            visited = set()
        if dataset_id in visited or dataset_id not in self._lineage_graph:
            return []
        visited.add(dataset_id)

        descendants = list(self._lineage_graph[dataset_id].get("child_ids", []))
        for child_id in self._lineage_graph[dataset_id].get("child_ids", []):
            descendants.extend(self._get_descendants(child_id, visited))
        return list(set(descendants))

    def _find_lineage_path(self, start_id: str, end_id: str) -> List[str]:
        if start_id == end_id:
            return [start_id]

        from collections import deque
        queue = deque([(start_id, [start_id])])
        visited = {start_id}

        while queue:
            current, path = queue.popleft()
            if current not in self._lineage_graph:
                continue

            for child_id in self._lineage_graph[current].get("child_ids", []):
                if child_id == end_id:
                    return path + [child_id]
                if child_id not in visited:
                    visited.add(child_id)
                    queue.append((child_id, path + [child_id]))

            for parent_id in self._lineage_graph[current].get("parent_ids", []):
                if parent_id == end_id:
                    return path + [parent_id]
                if parent_id not in visited:
                    visited.add(parent_id)
                    queue.append((parent_id, path + [parent_id]))

        return []


class DataQualityMonitorAction(BaseAction):
    """Monitor data quality metrics over time."""
    action_type = "data_quality_monitor"
    display_name = "数据质量监控"
    description = "数据质量指标监控"

    def __init__(self):
        super().__init__()
        self._monitors: Dict[str, Dict] = {}
        self._violations: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            monitor_name = params.get("monitor_name", "")

            if operation == "create":
                if not monitor_name:
                    return ActionResult(success=False, message="monitor_name required")

                rules = params.get("rules", [])
                self._monitors[monitor_name] = {
                    "name": monitor_name,
                    "dataset": params.get("dataset", ""),
                    "rules": rules,
                    "created_at": time.time(),
                    "last_checked": None,
                    "violation_count": 0,
                    "status": "active"
                }

                return ActionResult(
                    success=True,
                    data={"monitor": monitor_name, "rules": len(rules)},
                    message=f"Monitor '{monitor_name}' created with {len(rules)} rules"
                )

            elif operation == "check":
                if monitor_name not in self._monitors:
                    return ActionResult(success=False, message=f"Monitor '{monitor_name}' not found")

                monitor = self._monitors[monitor_name]
                monitor["last_checked"] = time.time()
                data_sample = params.get("data_sample", [])

                violations_found = []
                for rule in monitor["rules"]:
                    rule_violations = self._check_rule(rule, data_sample)
                    violations_found.extend(rule_violations)

                monitor["violation_count"] += len(violations_found)
                self._violations.extend([{
                    "monitor": monitor_name,
                    "rule": v["rule"],
                    "severity": v["severity"],
                    "timestamp": time.time()
                } for v in violations_found])

                return ActionResult(
                    success=True,
                    data={
                        "monitor": monitor_name,
                        "violations": len(violations_found),
                        "total_violations": monitor["violation_count"],
                        "violation_details": violations_found
                    },
                    message=f"Check: {len(violations_found)} violations"
                )

            elif operation == "status":
                return ActionResult(
                    success=True,
                    data={
                        "monitors": {k: {"violations": v["violation_count"], "last_checked": v["last_checked"]} for k, v in self._monitors.items()},
                        "recent_violations": self._violations[-20:]
                    }
                )

            elif operation == "acknowledge":
                if not self._violations:
                    return ActionResult(success=True, message="No violations to acknowledge")

                acknowledged = self._violations[-params.get("count", 1):]
                self._violations = self._violations[:-params.get("count", 1)]
                return ActionResult(success=True, data={"acknowledged": len(acknowledged)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Quality monitor error: {str(e)}")

    def _check_rule(self, rule: Dict, data: List[Dict]) -> List[Dict]:
        violations = []
        rule_type = rule.get("type", "not_null")
        field = rule.get("field", "")
        threshold = rule.get("threshold", 0)

        if rule_type == "not_null":
            for i, row in enumerate(data):
                if field not in row or row[field] is None:
                    violations.append({"rule": f"not_null:{field}", "row": i, "severity": "high"})

        elif rule_type == "uniqueness":
            seen = set()
            for i, row in enumerate(data):
                val = row.get(field)
                if val in seen:
                    violations.append({"rule": f"uniqueness:{field}", "row": i, "severity": "medium"})
                seen.add(val)

        elif rule_type == "range":
            min_val = rule.get("min")
            max_val = rule.get("max")
            for i, row in enumerate(data):
                val = row.get(field)
                if val is not None and ((min_val is not None and val < min_val) or (max_val is not None and val > max_val)):
                    violations.append({"rule": f"range:{field}", "row": i, "severity": "high"})

        elif rule_type == "threshold":
            count = sum(1 for row in data if row.get(field))
            if count < threshold:
                violations.append({"rule": f"threshold:{field}", "actual": count, "threshold": threshold, "severity": "medium"})

        return violations


class DataFingerprintAction(BaseAction):
    """Generate and track data fingerprints for change detection."""
    action_type = "data_fingerprint"
    display_name = "数据指纹"
    description = "数据指纹生成与变更检测"

    def __init__(self):
        super().__init__()
        self._fingerprints: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "generate")
            dataset_name = params.get("dataset_name", "")
            data = params.get("data", [])

            if operation == "generate":
                if not data:
                    return ActionResult(success=False, message="data required")

                algorithm = params.get("algorithm", "xxh64")
                include_schema = params.get("include_schema", False)

                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict):
                        normalized = json.dumps(data, sort_keys=True, default=str).encode("utf-8")
                    else:
                        normalized = str(data).encode("utf-8")
                else:
                    normalized = str(data).encode("utf-8")

                if algorithm == "xxh64":
                    import xxhash
                    fp = xxhash.xxh64(normalized).hexdigest()
                elif algorithm == "sha256":
                    fp = hashlib.sha256(normalized).hexdigest()
                elif algorithm == "md5":
                    fp = hashlib.md5(normalized).hexdigest()
                else:
                    fp = hashlib.blake2b(normalized).hexdigest()

                result = {
                    "fingerprint": fp,
                    "algorithm": algorithm,
                    "row_count": len(data) if isinstance(data, list) else 0,
                    "generated_at": time.time()
                }

                if dataset_name:
                    self._fingerprints[dataset_name] = {
                        "current": fp,
                        "previous": self._fingerprints.get(dataset_name, {}).get("current"),
                        "history": self._fingerprints.get(dataset_name, {}).get("history", [])
                    }
                    self._fingerprints[dataset_name]["history"].append({
                        "fingerprint": fp,
                        "timestamp": time.time()
                    })
                    if len(self._fingerprints[dataset_name]["history"]) > 100:
                        self._fingerprints[dataset_name]["history"] = self._fingerprints[dataset_name]["history"][-100:]
                    result["changed"] = self._fingerprints[dataset_name]["previous"] != fp if self._fingerprints[dataset_name]["previous"] else False

                return ActionResult(
                    success=True,
                    data=result,
                    message=f"Fingerprint: {fp[:16]}... changed={result.get('changed', 'N/A')}"
                )

            elif operation == "compare":
                fp1 = params.get("fingerprint1", "")
                fp2 = params.get("fingerprint2", "")

                if not fp1 or not fp2:
                    return ActionResult(success=False, message="Both fingerprints required")

                return ActionResult(
                    success=True,
                    data={"match": fp1 == fp2, "fingerprint1": fp1[:16], "fingerprint2": fp2[:16]},
                    message="Fingerprints match" if fp1 == fp2 else "Fingerprints differ"
                )

            elif operation == "history":
                if dataset_name not in self._fingerprints:
                    return ActionResult(success=False, message=f"No history for '{dataset_name}'")

                history = self._fingerprints[dataset_name]["history"]
                return ActionResult(
                    success=True,
                    data={"dataset": dataset_name, "current": self._fingerprints[dataset_name]["current"], "history": history}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint error: {str(e)}")


class DataAuditLogAction(BaseAction):
    """Comprehensive data audit logging."""
    action_type = "data_audit"
    display_name = "数据审计日志"
    description = "数据操作审计日志"

    def __init__(self):
        super().__init__()
        self._audit_log: List[Dict] = []
        self._log_id_counter = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "log")
            action = params.get("action", "")
            resource_type = params.get("resource_type", "")
            resource_id = params.get("resource_id", "")

            if operation == "log":
                if not action:
                    return ActionResult(success=False, message="action required")

                self._log_id_counter += 1
                log_entry = {
                    "id": self._log_id_counter,
                    "timestamp": time.time(),
                    "action": action,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "actor": params.get("actor", "system"),
                    "details": params.get("details", {}),
                    "ip_address": params.get("ip_address", ""),
                    "session_id": params.get("session_id", ""),
                    "result": params.get("result", "success"),
                    "tags": params.get("tags", [])
                }

                self._audit_log.append(log_entry)
                if len(self._audit_log) > 10000:
                    self._audit_log = self._audit_log[-10000:]

                return ActionResult(
                    success=True,
                    data={"log_id": self._log_id_counter},
                    message=f"Audit log: {action} on {resource_type}/{resource_id}"
                )

            elif operation == "query":
                filters = params.get("filters", {})
                start_time = filters.get("start_time", 0)
                end_time = filters.get("end_time", float("inf"))
                action_filter = filters.get("action")
                resource_filter = filters.get("resource_type")
                actor_filter = filters.get("actor")

                results = []
                for entry in self._audit_log:
                    if entry["timestamp"] < start_time or entry["timestamp"] > end_time:
                        continue
                    if action_filter and entry["action"] != action_filter:
                        continue
                    if resource_filter and entry["resource_type"] != resource_filter:
                        continue
                    if actor_filter and entry["actor"] != actor_filter:
                        continue
                    results.append(entry)

                limit = params.get("limit", 100)
                offset = params.get("offset", 0)

                return ActionResult(
                    success=True,
                    data={
                        "total": len(results),
                        "returned": min(len(results) - offset, limit),
                        "entries": results[offset:offset + limit]
                    }
                )

            elif operation == "summary":
                if not self._audit_log:
                    return ActionResult(success=True, data={"total_entries": 0})

                recent = [e for e in self._audit_log if e["timestamp"] > time.time() - 86400]

                action_counts = {}
                for entry in self._audit_log:
                    action_counts[entry["action"]] = action_counts.get(entry["action"], 0) + 1

                return ActionResult(
                    success=True,
                    data={
                        "total_entries": len(self._audit_log),
                        "last_24h": len(recent),
                        "action_breakdown": action_counts,
                        "oldest": self._audit_log[0]["timestamp"] if self._audit_log else None,
                        "newest": self._audit_log[-1]["timestamp] if self._audit_log else None
                    }
                )

            elif operation == "export":
                export_format = params.get("format", "json")
                start_time = params.get("start_time", 0)
                end_time = params.get("end_time", float("inf"))

                filtered = [e for e in self._audit_log if start_time <= e["timestamp"] <= end_time]

                return ActionResult(
                    success=True,
                    data={"entries": filtered, "count": len(filtered), "format": export_format},
                    message=f"Exported {len(filtered)} audit entries"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Audit log error: {str(e)}")
