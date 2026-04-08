"""Data lineage action module for RabAI AutoClick.

Provides data lineage operations:
- LineageTrackAction: Track data lineage
- LineageQueryAction: Query lineage graph
- LineageRootAction: Find root sources
- LineageLeafAction: Find leaf destinations
- LineagePathAction: Find path between nodes
- LineageImpactAction: Impact analysis
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LineageTrackAction(BaseAction):
    """Track data lineage between nodes."""
    action_type = "lineage_track"
    display_name = "追踪血缘"
    description = "追踪数据血缘关系"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            transform = params.get("transform", "")
            metadata = params.get("metadata", {})

            if not source or not target:
                return ActionResult(success=False, message="source and target are required")

            edge_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "lineage_graph"):
                context.lineage_graph = {"nodes": {}, "edges": []}

            if source not in context.lineage_graph["nodes"]:
                context.lineage_graph["nodes"][source] = {"node_id": source, "type": "dataset"}
            if target not in context.lineage_graph["nodes"]:
                context.lineage_graph["nodes"][target] = {"node_id": target, "type": "dataset"}

            context.lineage_graph["edges"].append({
                "edge_id": edge_id,
                "source": source,
                "target": target,
                "transform": transform,
                "metadata": metadata,
                "tracked_at": time.time(),
            })

            return ActionResult(
                success=True,
                data={"edge_id": edge_id, "source": source, "target": target},
                message=f"Lineage tracked: {source} -> {target}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage track failed: {e}")


class LineageQueryAction(BaseAction):
    """Query lineage graph."""
    action_type = "lineage_query"
    display_name = "查询血缘"
    description = "查询数据血缘图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            node_id = params.get("node_id", "")
            direction = params.get("direction", "both")

            if not node_id:
                return ActionResult(success=False, message="node_id is required")

            graph = getattr(context, "lineage_graph", {"nodes": {}, "edges": []})

            upstream = [e["source"] for e in graph["edges"] if e["target"] == node_id]
            downstream = [e["target"] for e in graph["edges"] if e["source"] == node_id]

            if direction == "upstream":
                related = upstream
            elif direction == "downstream":
                related = downstream
            else:
                related = upstream + downstream

            return ActionResult(
                success=True,
                data={"node_id": node_id, "upstream": upstream, "downstream": downstream},
                message=f"Node {node_id}: {len(upstream)} upstream, {len(downstream)} downstream",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage query failed: {e}")


class LineageRootAction(BaseAction):
    """Find root source nodes."""
    action_type = "lineage_root"
    display_name = "血缘根节点"
    description = "查找血缘根节点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            graph = getattr(context, "lineage_graph", {"nodes": {}, "edges": []})
            all_targets = {e["target"] for e in graph["edges"]}
            roots = [n for n in graph["nodes"] if n not in all_targets]

            return ActionResult(
                success=True,
                data={"root_nodes": roots, "count": len(roots)},
                message=f"Found {len(roots)} root nodes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage root failed: {e}")


class LineageLeafAction(BaseAction):
    """Find leaf destination nodes."""
    action_type = "lineage_leaf"
    display_name = "血缘叶节点"
    description = "查找血缘叶节点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            graph = getattr(context, "lineage_graph", {"nodes": {}, "edges": []})
            all_sources = {e["source"] for e in graph["edges"]}
            leaves = [n for n in graph["nodes"] if n not in all_sources]

            return ActionResult(
                success=True,
                data={"leaf_nodes": leaves, "count": len(leaves)},
                message=f"Found {len(leaves)} leaf nodes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage leaf failed: {e}")


class LineagePathAction(BaseAction):
    """Find path between two nodes."""
    action_type = "lineage_path"
    display_name = "血缘路径"
    description = "查找两个节点间的血缘路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            max_depth = params.get("max_depth", 10)

            if not source or not target:
                return ActionResult(success=False, message="source and target are required")

            graph = getattr(context, "lineage_graph", {"nodes": {}, "edges": []})
            adj = {}
            for e in graph["edges"]:
                if e["source"] not in adj:
                    adj[e["source"]] = []
                adj[e["source"]].append(e["target"])

            visited = set()
            queue = [(source, [source])]
            found_path = None

            while queue and not found_path:
                node, path = queue.pop(0)
                if len(path) > max_depth:
                    continue
                if node == target:
                    found_path = path
                    break
                visited.add(node)
                for neighbor in adj.get(node, []):
                    if neighbor not in visited:
                        queue.append((neighbor, path + [neighbor]))

            return ActionResult(
                success=True,
                data={"source": source, "target": target, "path": found_path or [], "found": found_path is not None},
                message=f"Path found: {' -> '.join(found_path)}" if found_path else "No path found",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage path failed: {e}")


class LineageImpactAction(BaseAction):
    """Impact analysis for data changes."""
    action_type = "lineage_impact"
    display_name = "血缘影响分析"
    description = "分析数据变更影响"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source_node = params.get("source_node", "")
            depth = params.get("depth", 3)

            if not source_node:
                return ActionResult(success=False, message="source_node is required")

            graph = getattr(context, "lineage_graph", {"nodes": {}, "edges": []})
            adj = {}
            for e in graph["edges"]:
                if e["source"] not in adj:
                    adj[e["source"]] = []
                adj[e["source"]].append(e["target"])

            impacted = []
            current_level = [source_node]
            for d in range(depth):
                next_level = []
                for node in current_level:
                    for neighbor in adj.get(node, []):
                        if neighbor not in impacted:
                            impacted.append(neighbor)
                        next_level.append(neighbor)
                current_level = next_level

            return ActionResult(
                success=True,
                data={"source": source_node, "impacted_nodes": impacted, "depth": depth, "count": len(impacted)},
                message=f"{len(impacted)} nodes impacted by changes to {source_node}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lineage impact failed: {e}")
