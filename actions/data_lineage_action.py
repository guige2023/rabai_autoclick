"""Data lineage tracking action module for RabAI AutoClick.

Tracks data provenance through pipelines: source->transform->output
with DAG representation and impact analysis.
"""

from __future__ import annotations

import sys
import os
import uuid
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DataNode:
    """A node in the data lineage graph."""
    node_id: str
    name: str
    node_type: str  # source, transform, output, dataset
    owner: Optional[str] = None
    schema: Optional[Dict[str, str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    tags: List[str] = field(default_factory=list)


@dataclass
class DataEdge:
    """A directed edge in the data lineage graph."""
    edge_id: str
    source_id: str
    target_id: str
    transform_type: str = "direct"  # direct, join, aggregate, filter, enrich
    columns_passed: Optional[List[str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class LineageGraph:
    """In-memory directed graph for data lineage."""

    def __init__(self):
        self.nodes: Dict[str, DataNode] = {}
        self.edges: Dict[str, DataEdge] = {}
        self._adj: Dict[str, Set[str]] = defaultdict(set)  # outgoing
        self._rev_adj: Dict[str, Set[str]] = defaultdict(set)  # incoming

    def add_node(self, node: DataNode) -> None:
        self.nodes[node.node_id] = node

    def add_edge(self, edge: DataEdge) -> None:
        self.edges[edge.edge_id] = edge
        self._adj[edge.source_id].add(edge.target_id)
        self._rev_adj[edge.target_id].add(edge.source_id)

    def upstream(self, node_id: str, max_depth: int = 10) -> List[str]:
        """Get all upstream (ancestor) nodes."""
        visited: Set[str] = set()
        queue = [(node_id, 0)]
        result = []
        while queue:
            curr, depth = queue.pop(0)
            if curr in visited or depth > max_depth:
                continue
            visited.add(curr)
            for parent in self._rev_adj.get(curr, []):
                if parent not in visited:
                    result.append(parent)
                    queue.append((parent, depth + 1))
        return result

    def downstream(self, node_id: str, max_depth: int = 10) -> List[str]:
        """Get all downstream (descendant) nodes."""
        visited: Set[str] = set()
        queue = [(node_id, 0)]
        result = []
        while queue:
            curr, depth = queue.pop(0)
            if curr in visited or depth > max_depth:
                continue
            visited.add(curr)
            for child in self._adj.get(curr, []):
                if child not in visited:
                    result.append(child)
                    queue.append((child, depth + 1))
        return result


class DataLineageAction(BaseAction):
    """Track data lineage through transformations.
    
    Maintains a DAG of data assets and their transformations.
    Supports source registration, transform tracking, and
    upstream/downstream impact analysis.
    
    Args:
        graph: Optional LineageGraph instance for sharing across calls
    """

    def __init__(self, graph: Optional[LineageGraph] = None):
        super().__init__()
        self._graph = graph or LineageGraph()

    def execute(
        self,
        action: str,
        node_id: Optional[str] = None,
        node_name: Optional[str] = None,
        node_type: Optional[str] = None,
        source_id: Optional[str] = None,
        target_id: Optional[str] = None,
        transform_type: str = "direct",
        columns: Optional[List[str]] = None,
        owner: Optional[str] = None,
        schema: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        max_depth: int = 10
    ) -> ActionResult:
        try:
            if action == "register_source":
                if not node_name or not node_type:
                    return ActionResult(success=False, error="node_name and node_type required")
                nid = node_id or str(uuid.uuid4())[:8]
                node = DataNode(
                    node_id=nid, name=node_name, node_type=node_type,
                    owner=owner, schema=schema,
                    metadata=metadata or {},
                    created_at=datetime.now(timezone.utc).isoformat(),
                    tags=tags or []
                )
                self._graph.add_node(node)
                return ActionResult(success=True, data={
                    "node_id": nid, "name": node_name, "type": node_type
                })

            elif action == "register_transform":
                if not source_id or not target_id:
                    return Result(success=False, error="source_id and target_id required")
                if source_id not in self._graph.nodes:
                    return ActionResult(success=False, error=f"Source node {source_id} not found")
                if target_id not in self._graph.nodes:
                    return ActionResult(success=False, error=f"Target node {target_id} not found")
                eid = str(uuid.uuid4())[:8]
                edge = DataEdge(
                    edge_id=eid, source_id=source_id, target_id=target_id,
                    transform_type=transform_type, columns_passed=columns,
                    metadata=metadata or {}
                )
                self._graph.add_edge(edge)
                return ActionResult(success=True, data={
                    "edge_id": eid, "source": source_id, "target": target_id,
                    "transform": transform_type
                })

            elif action == "upstream":
                if not node_id:
                    return ActionResult(success=False, error="node_id required")
                ancestors = self._graph.upstream(node_id, max_depth)
                node_info = {nid: self._graph.nodes[nid].name for nid in ancestors if nid in self._graph.nodes}
                return ActionResult(success=True, data={
                    "node": node_id,
                    "upstream_nodes": ancestors,
                    "upstream_names": node_info,
                    "count": len(ancestors)
                })

            elif action == "downstream":
                if not node_id:
                    return ActionResult(success=False, error="node_id required")
                descendants = self._graph.downstream(node_id, max_depth)
                node_info = {nid: self._graph.nodes[nid].name for nid in descendants if nid in self._graph.nodes}
                return ActionResult(success=True, data={
                    "node": node_id,
                    "downstream_nodes": descendants,
                    "downstream_names": node_info,
                    "count": len(descendants)
                })

            elif action == "impact_analysis":
                if not node_id:
                    return ActionResult(success=False, error="node_id required")
                upstream = self._graph.upstream(node_id, max_depth)
                downstream = self._graph.downstream(node_id, max_depth)
                return ActionResult(success=True, data={
                    "node": node_id,
                    "node_name": self._graph.nodes.get(node_id, DataNode(node_id="","name="","node_type="")).name,
                    "upstream_count": len(upstream),
                    "downstream_count": len(downstream),
                    "total_impact": len(upstream) + len(downstream) + 1
                })

            elif action == "list_nodes":
                return ActionResult(success=True, data={
                    "nodes": [
                        {"node_id": n.node_id, "name": n.name, "type": n.node_type,
                         "owner": n.owner, "tags": n.tags}
                        for n in self._graph.nodes.values()
                    ],
                    "edge_count": len(self._graph.edges)
                })

            elif action == "get_node":
                if not node_id or node_id not in self._graph.nodes:
                    return ActionResult(success=False, error="node_id not found")
                node = self._graph.nodes[node_id]
                return ActionResult(success=True, data={
                    "node_id": node.node_id, "name": node.name,
                    "type": node.node_type, "owner": node.owner,
                    "schema": node.schema, "metadata": node.metadata,
                    "tags": node.tags, "created_at": node.created_at
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class SchemaEvolutionAction(BaseAction):
    """Track schema evolution across data versions.
    
    Detects added, removed, and type-changed columns.
    Useful for understanding how data contracts change over time.
    """

    def execute(
        self,
        action: str,
        old_schema: Optional[Dict[str, str]] = None,
        new_schema: Optional[Dict[str, str]] = None,
        schemas: Optional[List[Dict[str, Any]]] = None
    ) -> ActionResult:
        try:
            if action == "diff":
                if not old_schema or not new_schema:
                    return ActionResult(success=False, error="old_schema and new_schema required")
                
                added = [k for k in new_schema if k not in old_schema]
                removed = [k for k in old_schema if k not in new_schema]
                unchanged = [k for k in new_schema if k in old_schema and old_schema[k] == new_schema[k]]
                type_changed = [k for k in unchanged if old_schema[k] != new_schema[k]]

                return ActionResult(success=True, data={
                    "added_columns": added,
                    "removed_columns": removed,
                    "unchanged_columns": unchanged,
                    "type_changed_columns": type_changed,
                    "backward_compatible": len(removed) == 0 and len(type_changed) == 0
                })

            elif action == "timeline":
                if not schemas:
                    return ActionResult(success=False, error="schemas required")
                
                timeline = []
                for i in range(1, len(schemas)):
                    old = schemas[i - 1].get("fields", {})
                    new = schemas[i].get("fields", {})
                    added = [k for k in new if k not in old]
                    removed = [k for k in old if k not in new]
                    timeline.append({
                        "version": schemas[i].get("version", f"v{i}"),
                        "added": added,
                        "removed": removed,
                        "backward_compatible": len(removed) == 0
                    })
                return ActionResult(success=True, data={"timeline": timeline})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
