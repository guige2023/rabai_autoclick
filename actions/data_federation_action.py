"""Data federation and aggregation action module for RabAI AutoClick.

Provides:
- DataFederationAction: Query and aggregate from multiple data sources
- DataAggregatorAction: Aggregate data from multiple sources
- DataFanoutAction: Fan out data to multiple destinations
- DataFaninAction: Fan in data from multiple sources
"""

import time
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataFederationAction(BaseAction):
    """Query and aggregate from multiple data sources."""
    action_type = "data_federation"
    display_name = "数据联邦"
    description = "多数据源联合查询"

    def __init__(self):
        super().__init__()
        self._sources: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                source_name = params.get("source_name", "")
                if not source_name:
                    return ActionResult(success=False, message="source_name required")

                self._sources[source_name] = {
                    "name": source_name,
                    "type": params.get("source_type", "unknown"),
                    "connection": params.get("connection", {}),
                    "tables": params.get("tables", []),
                    "enabled": params.get("enabled", True),
                    "created_at": time.time()
                }
                return ActionResult(success=True, data={"source": source_name}, message=f"Source '{source_name}' registered")

            elif operation == "query":
                sources = params.get("sources", [])
                query = params.get("query", "")
                timeout = params.get("timeout", 30)

                results = {}
                for src in sources:
                    if src in self._sources:
                        results[src] = self._execute_query_on_source(src, query)
                    else:
                        results[src] = {"error": f"Source '{src}' not found"}

                return ActionResult(
                    success=True,
                    data={"results": results, "sources_queried": len(results)}
                )

            elif operation == "federate":
                sources = params.get("sources", [])
                merge_key = params.get("merge_key", "")

                all_data = []
                for src in sources:
                    data = self._get_source_data(src)
                    all_data.extend(data)

                if merge_key:
                    merged = self._merge_by_key(all_data, merge_key)
                else:
                    merged = all_data

                return ActionResult(
                    success=True,
                    data={"federated_data": merged, "total_records": len(merged), "sources": len(sources)}
                )

            elif operation == "list":
                return ActionResult(success=True, data={"sources": list(self._sources.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Federation error: {str(e)}")

    def _execute_query_on_source(self, source_name: str, query: str) -> Dict:
        return {"source": source_name, "query": query, "status": "executed", "rows": []}

    def _get_source_data(self, source_name: str) -> List[Dict]:
        return [{"source": source_name, "data": f"sample_data_{source_name}", "timestamp": time.time()}]

    def _merge_by_key(self, data: List[Dict], key: str) -> List[Dict]:
        merged_map = {}
        for item in data:
            if isinstance(item, dict) and key in item:
                k = item[key]
                if k not in merged_map:
                    merged_map[k] = item
        return list(merged_map.values())


class DataAggregatorAction(BaseAction):
    """Aggregate data from multiple sources."""
    action_type = "data_aggregator"
    display_name = "数据聚合"
    description = "多源数据聚合"

    def __init__(self):
        super().__init__()
        self._aggregations: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "aggregate")
            agg_name = params.get("aggregation_name", "")

            if operation == "create":
                if not agg_name:
                    return ActionResult(success=False, message="aggregation_name required")

                self._aggregations[agg_name] = {
                    "name": agg_name,
                    "sources": params.get("sources", []),
                    "group_by": params.get("group_by", []),
                    "metrics": params.get("metrics", []),
                    "created_at": time.time()
                }
                return ActionResult(success=True, data={"aggregation": agg_name}, message=f"Aggregation '{agg_name}' created")

            elif operation == "aggregate":
                if not agg_name:
                    return ActionResult(success=False, message="aggregation_name required")

                if agg_name not in self._aggregations:
                    return ActionResult(success=False, message=f"Aggregation '{agg_name}' not found")

                agg = self._aggregations[agg_name]
                data = params.get("data", [])

                result = self._do_aggregate(data, agg["group_by"], agg["metrics"])

                return ActionResult(
                    success=True,
                    data={
                        "aggregation": agg_name,
                        "result": result,
                        "total_groups": len(result)
                    }
                )

            elif operation == "list":
                return ActionResult(success=True, data={"aggregations": list(self._aggregations.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregator error: {str(e)}")

    def _do_aggregate(self, data: List[Dict], group_by: List[str], metrics: List[Dict]) -> List[Dict]:
        if not group_by:
            result = {"_group": "all", "_count": len(data)}
            for metric in metrics:
                metric_name = metric.get("name", "unknown")
                metric_type = metric.get("type", "count")
                field = metric.get("field", "")

                if metric_type == "count":
                    result[metric_name] = len(data)
                elif metric_type == "sum":
                    result[metric_name] = sum(row.get(field, 0) for row in data)
                elif metric_type == "avg":
                    values = [row.get(field, 0) for row in data]
                    result[metric_name] = sum(values) / len(values) if values else 0
                elif metric_type == "min":
                    result[metric_name] = min(row.get(field, 0) for row in data) if data else 0
                elif metric_type == "max":
                    result[metric_name] = max(row.get(field, 0) for row in data) if data else 0
            return [result]

        groups = {}
        for row in data:
            key = tuple(row.get(g) for g in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        results = []
        for key, group_data in groups.items():
            group_key = dict(zip(group_by, key))
            for metric in metrics:
                metric_name = metric.get("name", "unknown")
                metric_type = metric.get("type", "count")
                field = metric.get("field", "")

                if metric_type == "count":
                    group_key[metric_name] = len(group_data)
                elif metric_type == "sum":
                    group_key[metric_name] = sum(row.get(field, 0) for row in group_data)
                elif metric_type == "avg":
                    values = [row.get(field, 0) for row in group_data]
                    group_key[metric_name] = sum(values) / len(values) if values else 0
                elif metric_type == "min":
                    group_key[metric_name] = min(row.get(field, 0) for row in group_data) if group_data else 0
                elif metric_type == "max":
                    group_key[metric_name] = max(row.get(field, 0) for row in group_data) if group_data else 0
            results.append(group_key)

        return results


class DataFanoutAction(BaseAction):
    """Fan out data to multiple destinations."""
    action_type = "data_fanout"
    display_name = "数据扇出"
    description = "数据向多目标分发"

    def __init__(self):
        super().__init__()
        self._destinations: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "dispatch")
            dest_name = params.get("dest_name", "")

            if operation == "register":
                if not dest_name:
                    return ActionResult(success=False, message="dest_name required")

                self._destinations[dest_name] = {
                    "name": dest_name,
                    "type": params.get("dest_type", "unknown"),
                    "config": params.get("config", {}),
                    "enabled": params.get("enabled", True)
                }
                return ActionResult(success=True, data={"destination": dest_name}, message=f"Destination '{dest_name}' registered")

            elif operation == "dispatch":
                destinations = params.get("destinations", [])
                data = params.get("data", [])
                parallelism = params.get("parallelism", 1)

                results = {}
                for dest in destinations:
                    if dest in self._destinations:
                        results[dest] = {"status": "dispatched", "records": len(data)}
                    else:
                        results[dest] = {"status": "error", "message": f"Destination '{dest}' not found"}

                return ActionResult(
                    success=True,
                    data={
                        "dispatched": len([r for r in results.values() if r["status"] == "dispatched"]),
                        "failed": len([r for r in results.values() if r.get("status") == "error"]),
                        "total_records": len(data),
                        "results": results
                    }
                )

            elif operation == "list":
                return ActionResult(success=True, data={"destinations": list(self._destinations.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Fanout error: {str(e)}")


class DataFaninAction(BaseAction):
    """Fan in data from multiple sources."""
    action_type = "data_fanin"
    display_name = "数据扇入"
    description = "多源数据汇聚"

    def __init__(self):
        super().__init__()
        self._sources: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "collect")
            source_name = params.get("source_name", "")

            if operation == "register":
                if not source_name:
                    return ActionResult(success=False, message="source_name required")

                self._sources[source_name] = {
                    "name": source_name,
                    "type": params.get("source_type", "unknown"),
                    "config": params.get("config", {}),
                    "enabled": params.get("enabled", True)
                }
                return ActionResult(success=True, data={"source": source_name}, message=f"Source '{source_name}' registered")

            elif operation == "collect":
                sources = params.get("sources", [])
                merge_strategy = params.get("merge_strategy", "union")
                deduplicate = params.get("deduplicate", False)

                collected = []
                for src in sources:
                    if src in self._sources:
                        data = self._fetch_from_source(src)
                        collected.extend(data)

                if deduplicate:
                    seen = set()
                    unique = []
                    for item in collected:
                        key = json.dumps(item, sort_keys=True, default=str)
                        if key not in seen:
                            seen.add(key)
                            unique.append(item)
                    collected = unique

                return ActionResult(
                    success=True,
                    data={
                        "collected": len(collected),
                        "sources_used": len(sources),
                        "merge_strategy": merge_strategy,
                        "deduplicated": deduplicate
                    }
                )

            elif operation == "list":
                return ActionResult(success=True, data={"sources": list(self._sources.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Fanin error: {str(e)}")

    def _fetch_from_source(self, source_name: str) -> List[Dict]:
        return [{"source": source_name, "data": f"from_{source_name}", "timestamp": time.time()}]
