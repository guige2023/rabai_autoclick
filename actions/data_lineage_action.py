"""Data lineage action module for RabAI AutoClick.

Provides data lineage tracking:
- LineageTracker: Track data flow between datasets
- LineageGraph: Build and query lineage graphs
- ColumnLineage: Track column-level lineage
- ImpactAnalyzer: Analyze downstream impact
- LineageRecorder: Record lineage events
"""

from __future__ import annotations

import json
import sys
import os
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LineageTrackerAction(BaseAction):
    """Track data flow between datasets."""
    action_type = "lineage_tracker"
    display_name = "数据血缘追踪"
    description = "追踪数据集之间的数据流向"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "track")
            lineage_path = params.get("lineage_path", "/tmp/data_lineage")
            dataset = params.get("dataset", "")
            source_dataset = params.get("source_dataset", "")
            operation_type = params.get("operation_type", "transform")
            metadata = params.get("metadata", {})

            os.makedirs(lineage_path, exist_ok=True)
            graph_file = os.path.join(lineage_path, "lineage_graph.json")

            graph = {}
            if os.path.exists(graph_file):
                with open(graph_file) as f:
                    graph = json.load(f)

            if operation == "track":
                if not dataset:
                    return ActionResult(success=False, message="dataset required")

                event_id = str(uuid.uuid4())[:12]
                event = {
                    "event_id": event_id,
                    "dataset": dataset,
                    "source_dataset": source_dataset,
                    "operation": operation_type,
                    "metadata": metadata,
                    "timestamp": datetime.now().isoformat(),
                }

                if dataset not in graph:
                    graph[dataset] = {"upstream": [], "downstream": [], "events": []}

                if source_dataset:
                    if source_dataset not in graph:
                        graph[source_dataset] = {"upstream": [], "downstream": [], "events": []}
                    if dataset not in graph[source_dataset]["downstream"]:
                        graph[source_dataset]["downstream"].append(dataset)
                    if source_dataset not in graph[dataset]["upstream"]:
                        graph[dataset]["upstream"].append(source_dataset)

                graph[dataset]["events"].append(event)

                with open(graph_file, "w") as f:
                    json.dump(graph, f, indent=2)

                return ActionResult(success=True, message=f"Tracked: {source_dataset} -> {dataset}", data={"event_id": event_id})

            elif operation == "get":
                if not dataset:
                    return ActionResult(success=False, message="dataset required")

                if dataset not in graph:
                    return ActionResult(success=False, message=f"Dataset not found: {dataset}")

                return ActionResult(success=True, message=f"Lineage: {dataset}", data=graph[dataset])

            elif operation == "get_upstream":
                if not dataset:
                    return ActionResult(success=False, message="dataset required")

                visited = set()
                queue = deque([dataset])
                while queue:
                    current = queue.popleft()
                    if current in graph:
                        for upstream in graph[current].get("upstream", []):
                            if upstream not in visited:
                                visited.add(upstream)
                                queue.append(upstream)

                return ActionResult(success=True, message=f"{len(visited)} upstream datasets", data={"upstream": list(visited)})

            elif operation == "get_downstream":
                if not dataset:
                    return ActionResult(success=False, message="dataset required")

                visited = set()
                queue = deque([dataset])
                while queue:
                    current = queue.popleft()
                    if current in graph:
                        for downstream in graph[current].get("downstream", []):
                            if downstream not in visited:
                                visited.add(downstream)
                                queue.append(downstream)

                return ActionResult(success=True, message=f"{len(visited)} downstream datasets", data={"downstream": list(visited)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ColumnLineageAction(BaseAction):
    """Track column-level lineage."""
    action_type = "column_lineage"
    display_name = "列血缘追踪"
    description = "追踪列级别的数据血缘"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "track")
            lineage_path = params.get("lineage_path", "/tmp/column_lineage")
            source_table = params.get("source_table", "")
            target_table = params.get("target_table", "")
            column_mapping = params.get("column_mapping", {})

            os.makedirs(lineage_path, exist_ok=True)
            col_graph_file = os.path.join(lineage_path, "column_graph.json")

            col_graph = {}
            if os.path.exists(col_graph_file):
                with open(col_graph_file) as f:
                    col_graph = json.load(f)

            if operation == "track":
                if not source_table or not target_table:
                    return ActionResult(success=False, message="source_table and target_table required")

                if target_table not in col_graph:
                    col_graph[target_table] = {"columns": {}, "upstream": {}}

                for target_col, source_info in column_mapping.items():
                    if isinstance(source_info, str):
                        source_col = source_info
                        source_table_ref = source_table
                    elif isinstance(source_info, dict):
                        source_col = source_info.get("column", "")
                        source_table_ref = source_info.get("table", source_table)
                    else:
                        continue

                    col_graph[target_table]["columns"][target_col] = {
                        "source_column": source_col,
                        "source_table": source_table_ref,
                        "transformed": isinstance(source_info, dict) and source_info.get("transformed", False),
                        "timestamp": datetime.now().isoformat(),
                    }

                    if source_table_ref not in col_graph[target_table]["upstream"]:
                        col_graph[target_table]["upstream"][source_table_ref] = []
                    if target_col not in col_graph[target_table]["upstream"][source_table_ref]:
                        col_graph[target_table]["upstream"][source_table_ref].append(target_col)

                with open(col_graph_file, "w") as f:
                    json.dump(col_graph, f, indent=2)

                return ActionResult(success=True, message=f"Column lineage: {source_table} -> {target_table}")

            elif operation == "get":
                if not target_table:
                    return ActionResult(success=False, message="target_table required")

                if target_table not in col_graph:
                    return ActionResult(success=False, message=f"Table not found: {target_table}")

                return ActionResult(success=True, message=f"Column lineage: {target_table}", data=col_graph[target_table])

            elif operation == "trace":
                if not target_table:
                    return ActionResult(success=False, message="target_table required")

                column = params.get("column", "")
                trace_path = []

                current_table = target_table
                current_col = column

                while current_table in col_graph and current_col in col_graph[current_table].get("columns", {}):
                    col_info = col_graph[current_table]["columns"][current_col]
                    trace_path.append({
                        "table": current_table,
                        "column": current_col,
                        "source_column": col_info.get("source_column"),
                        "source_table": col_info.get("source_table"),
                    })
                    current_table = col_info.get("source_table", "")
                    current_col = col_info.get("source_column", "")

                return ActionResult(success=True, message=f"Traced {len(trace_path)} hops", data={"trace": trace_path})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImpactAnalyzerAction(BaseAction):
    """Analyze downstream impact of data changes."""
    action_type = "impact_analyzer"
    display_name = "影响分析"
    description = "分析数据变更的下游影响"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            lineage_path = params.get("lineage_path", "/tmp/data_lineage")
            target_dataset = params.get("target_dataset", "")
            impact_depth = params.get("impact_depth", 10)

            if not target_dataset:
                return ActionResult(success=False, message="target_dataset required")

            graph_file = os.path.join(lineage_path, "lineage_graph.json")
            if not os.path.exists(graph_file):
                return ActionResult(success=False, message="No lineage data found")

            with open(graph_file) as f:
                graph = json.load(f)

            impacted = []
            critical_paths = []
            queue = deque([(target_dataset, [target_dataset])])

            while queue:
                current, path = queue.popleft()
                if len(path) > impact_depth:
                    continue

                if current in graph:
                    for downstream in graph[current].get("downstream", []):
                        if downstream not in path:
                            new_path = path + [downstream]
                            impacted.append({
                                "dataset": downstream,
                                "path": " -> ".join(new_path),
                                "depth": len(new_path) - 1,
                            })
                            if len(new_path) >= 3:
                                critical_paths.append(new_path)
                            queue.append((downstream, new_path))

            impact_summary = {
                "total_impacted": len(impacted),
                "impacted_datasets": [i["dataset"] for i in impacted],
                "critical_paths": critical_paths[:5],
                "max_depth": max((i["depth"] for i in impacted), default=0),
            }

            risk_level = "LOW"
            if impact_summary["total_impacted"] > 10 or impact_summary["max_depth"] > 5:
                risk_level = "HIGH"
            elif impact_summary["total_impacted"] > 5:
                risk_level = "MEDIUM"

            impact_summary["risk_level"] = risk_level

            return ActionResult(
                success=True,
                message=f"Impact analysis: {risk_level} risk",
                data=impact_summary
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LineageGraphAction(BaseAction):
    """Build and query lineage graphs."""
    action_type = "lineage_graph"
    display_name = "血缘图查询"
    description = "构建和查询血缘图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "export")
            lineage_path = params.get("lineage_path", "/tmp/data_lineage")
            dataset = params.get("dataset", "")
            direction = params.get("direction", "both")
            max_depth = params.get("max_depth", 5)

            graph_file = os.path.join(lineage_path, "lineage_graph.json")
            if not os.path.exists(graph_file):
                return ActionResult(success=False, message="No lineage graph found")

            with open(graph_file) as f:
                graph = json.load(f)

            if operation == "export":
                return ActionResult(success=True, message=f"Graph: {len(graph)} nodes", data={"nodes": list(graph.keys()), "edges": {k: {"upstream": v["upstream"], "downstream": v["downstream"]} for k, v in graph.items()}})

            elif operation == "subgraph":
                if not dataset:
                    return ActionResult(success=False, message="dataset required")

                if dataset not in graph:
                    return ActionResult(success=False, message=f"Dataset not found: {dataset}")

                visited = set([dataset])
                to_visit = [dataset]
                depth = 0

                while to_visit and depth < max_depth:
                    next_level = []
                    for node in to_visit:
                        if node in graph:
                            if direction in ("both", "upstream"):
                                for u in graph[node].get("upstream", []):
                                    if u not in visited:
                                        visited.add(u)
                                        next_level.append(u)
                            if direction in ("both", "downstream"):
                                for d in graph[node].get("downstream", []):
                                    if d not in visited:
                                        visited.add(d)
                                        next_level.append(d)
                    to_visit = next_level
                    depth += 1

                subgraph = {k: v for k, v in graph.items() if k in visited}
                return ActionResult(success=True, message=f"Subgraph: {len(visited)} nodes", data={"nodes": list(visited), "subgraph": subgraph})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
