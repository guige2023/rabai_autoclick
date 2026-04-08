"""
Data Lineage Action Module.

Tracks and manages data lineage across pipelines, recording the provenance,
transformation history, and dependency graph of data assets.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class NodeType(Enum):
    """Types of nodes in data lineage graph."""
    SOURCE = "source"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    JOIN = "join"
    OUTPUT = "output"
    DATASET = "dataset"
    TABLE = "table"
    FILE = "file"
    API = "api"
    STREAM = "stream"


class EdgeType(Enum):
    """Types of relationships between nodes."""
    DERIVES_FROM = "derives_from"
    DEPENDS_ON = "depends_on"
    TRANSFORMS = "transforms"
    WRITES_TO = "writes_to"
    READS_FROM = "reads_from"
    TRIGGERS = "triggers"


@dataclass
class DataNode:
    """Represents a node in the data lineage graph."""
    id: str
    name: str
    node_type: NodeType
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    tags: Set[str] = field(default_factory=set)
    schema: Optional[Dict[str, str]] = None
    owner: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type.value,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "tags": list(self.tags),
            "schema": self.schema,
            "owner": self.owner,
            "description": self.description,
        }


@dataclass
class DataEdge:
    """Represents a relationship between two nodes."""
    id: str
    source_id: str
    target_id: str
    edge_type: EdgeType
    transform_info: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type.value,
            "transform_info": self.transform_info,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Transformation:
    """Records a data transformation operation."""
    id: str
    name: str
    operation: str
    input_fields: List[str] = field(default_factory=list)
    output_fields: List[str] = field(default_factory=list)
    logic: Optional[str] = None
    duration_ms: Optional[float] = None
    record_count: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "operation": self.operation,
            "input_fields": self.input_fields,
            "output_fields": self.output_fields,
            "logic": self.logic,
            "duration_ms": self.duration_ms,
            "record_count": self.record_count,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class LineageSnapshot:
    """Point-in-time snapshot of lineage state."""
    id: str
    nodes: List[DataNode]
    edges: List[DataEdge]
    transformations: List[Transformation]
    timestamp: datetime = field(default_factory=datetime.now)
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "transformations": [t.to_dict() for t in self.transformations],
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


class DataLineageGraph:
    """
    In-memory data lineage graph with traversal capabilities.

    Tracks data flow from sources through transformations to outputs,
    providing full provenance tracking and impact analysis.

    Example:
        >>> graph = DataLineageGraph()
        >>> graph.add_source("raw_events", {"fields": ["id", "timestamp", "value"]})
        >>> graph.add_transform("enriched_events", "enrich", ["raw_events"])
        >>> ancestors = graph.get_ancestors("enriched_events")
    """

    def __init__(self):
        self.nodes: Dict[str, DataNode] = {}
        self.edges: Dict[str, DataEdge] = {}
        self.transformations: Dict[str, Transformation] = {}
        self._adjacency: Dict[str, Set[str]] = defaultdict(set)
        self._reverse_adjacency: Dict[str, Set[str]] = defaultdict(set)
        self._snapshots: List[LineageSnapshot] = []

    def add_source(
        self,
        name: str,
        schema: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a data source node."""
        node_id = self._generate_id(name)
        node = DataNode(
            id=node_id,
            name=name,
            node_type=NodeType.SOURCE,
            schema=schema,
            owner=owner,
            metadata=metadata or {},
        )
        self.nodes[node_id] = node
        return node_id

    def add_transform(
        self,
        name: str,
        operation: str,
        input_names: List[str],
        output_schema: Optional[Dict[str, str]] = None,
        logic: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a transformation node and its dependencies."""
        transform_id = self._generate_id(name)
        input_ids = []
        for input_name in input_names:
            input_id = self._find_node_by_name(input_name)
            if input_id:
                input_ids.append(input_id)

        node = DataNode(
            id=transform_id,
            name=name,
            node_type=NodeType.TRANSFORM,
            schema=output_schema,
            metadata=metadata or {},
        )
        self.nodes[transform_id] = node

        transform = Transformation(
            id=transform_id,
            name=name,
            operation=operation,
            input_fields=input_names,
            output_fields=list(output_schema.keys()) if output_schema else [],
            logic=logic,
        )
        self.transformations[transform_id] = transform

        for input_id in input_ids:
            edge_id = str(uuid.uuid4())
            edge = DataEdge(
                id=edge_id,
                source_id=input_id,
                target_id=transform_id,
                edge_type=EdgeType.DERIVES_FROM,
                transform_info={"operation": operation},
            )
            self.edges[edge_id] = edge
            self._adjacency[input_id].add(transform_id)
            self._reverse_adjacency[transform_id].add(input_id)

        return transform_id

    def add_output(
        self,
        name: str,
        input_name: str,
        output_type: NodeType = NodeType.OUTPUT,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register an output node."""
        node_id = self._generate_id(name)
        input_id = self._find_node_by_name(input_name)

        node = DataNode(
            id=node_id,
            name=name,
            node_type=output_type,
            metadata=metadata or {},
        )
        self.nodes[node_id] = node

        if input_id:
            edge_id = str(uuid.uuid4())
            edge = DataEdge(
                id=edge_id,
                source_id=input_id,
                target_id=node_id,
                edge_type=EdgeType.WRITES_TO,
            )
            self.edges[edge_id] = edge
            self._adjacency[input_id].add(node_id)
            self._reverse_adjacency[node_id].add(input_id)

        return node_id

    def add_edge(
        self,
        source_name: str,
        target_name: str,
        edge_type: EdgeType = EdgeType.DEPENDS_ON,
        transform_info: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Manually add an edge between two named nodes."""
        source_id = self._find_node_by_name(source_name)
        target_id = self._find_node_by_name(target_name)
        if not source_id or not target_id:
            return None

        edge_id = str(uuid.uuid4())
        edge = DataEdge(
            id=edge_id,
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            transform_info=transform_info,
        )
        self.edges[edge_id] = edge
        self._adjacency[source_id].add(target_id)
        self._reverse_adjacency[target_id].add(source_id)
        return edge_id

    def get_ancestors(self, node_name: str, max_depth: int = 10) -> List[DataNode]:
        """Get all upstream ancestors of a node (BFS)."""
        node_id = self._find_node_by_name(node_name)
        if not node_id:
            return []

        visited: Set[str] = set()
        queue = deque([(node_id, 0)])
        ancestors = []

        while queue:
            current_id, depth = queue.popleft()
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)

            for parent_id in self._reverse_adjacency[current_id]:
                if parent_id not in visited:
                    ancestors.append(self.nodes[parent_id])
                    queue.append((parent_id, depth + 1))

        return ancestors

    def get_descendants(self, node_name: str, max_depth: int = 10) -> List[DataNode]:
        """Get all downstream descendants of a node (BFS)."""
        node_id = self._find_node_by_name(node_name)
        if not node_id:
            return []

        visited: Set[str] = set()
        queue = deque([(node_id, 0)])
        descendants = []

        while queue:
            current_id, depth = queue.popleft()
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)

            for child_id in self._adjacency[current_id]:
                if child_id not in visited:
                    descendants.append(self.nodes[child_id])
                    queue.append((child_id, depth + 1))

        return descendants

    def get_path(self, source_name: str, target_name: str) -> List[str]:
        """Find the path between two nodes (BFS)."""
        source_id = self._find_node_by_name(source_name)
        target_id = self._find_node_by_name(target_name)
        if not source_id or not target_id:
            return []

        if source_id == target_id:
            return [source_id]

        visited: Set[str] = set()
        parent: Dict[str, str] = {}
        queue = deque([source_id])
        visited.add(source_id)

        while queue:
            current = queue.popleft()
            if current == target_id:
                path = []
                node = target_id
                while node in parent:
                    path.append(node)
                    node = parent[node]
                path.append(source_id)
                return path[::-1]

            for neighbor in self._adjacency[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    parent[neighbor] = current
                    queue.append(neighbor)

        return []

    def impact_analysis(self, node_name: str) -> Dict[str, Any]:
        """Perform impact analysis on a node."""
        node_id = self._find_node_by_name(node_name)
        if not node_id:
            return {"error": "Node not found"}

        descendants = self.get_descendants(node_name)
        affected_outputs = [n for n in descendants if n.node_type == NodeType.OUTPUT]
        affected_transforms = [n for n in descendants if n.node_type == NodeType.TRANSFORM]

        return {
            "node": self.nodes[node_id].to_dict(),
            "total_affected": len(descendants),
            "affected_outputs": [n.to_dict() for n in affected_outputs],
            "affected_transforms": [n.to_dict() for n in affected_transforms],
            "impact_depth": self._calculate_max_depth(node_id, direction="down"),
        }

    def provenance_analysis(self, node_name: str) -> Dict[str, Any]:
        """Perform provenance analysis on a node."""
        node_id = self._find_node_by_name(node_name)
        if not node_id:
            return {"error": "Node not found"}

        ancestors = self.get_ancestors(node_name)
        source_nodes = [n for n in ancestors if n.node_type == NodeType.SOURCE]

        return {
            "node": self.nodes[node_id].to_dict(),
            "total_sources": len(source_nodes),
            "source_nodes": [n.to_dict() for n in source_nodes],
            "provenance_depth": self._calculate_max_depth(node_id, direction="up"),
            "transformations": [
                self.transformations[t.id].to_dict()
                for t in ancestors
                if t.id in self.transformations
            ],
        }

    def take_snapshot(self) -> str:
        """Take a point-in-time snapshot of the lineage graph."""
        snapshot_id = str(uuid.uuid4())
        snapshot = LineageSnapshot(
            id=snapshot_id,
            nodes=list(self.nodes.values()),
            edges=list(self.edges.values()),
            transformations=list(self.transformations.values()),
        )
        self._snapshots.append(snapshot)
        return snapshot_id

    def get_snapshot(self, snapshot_id: str) -> Optional[LineageSnapshot]:
        """Retrieve a previously taken snapshot."""
        for snap in self._snapshots:
            if snap.id == snapshot_id:
                return snap
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Export lineage graph to dictionary."""
        return {
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [e.to_dict() for e in self.edges.values()],
            "transformations": [t.to_dict() for t in self.transformations.values()],
            "snapshot_count": len(self._snapshots),
        }

    def to_json(self) -> str:
        """Export lineage graph to JSON."""
        return json.dumps(self.to_dict(), indent=2, default=str)

    def _generate_id(self, name: str) -> str:
        """Generate a unique ID for a node based on its name."""
        raw = f"{name}:{time.time()}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    def _find_node_by_name(self, name: str) -> Optional[str]:
        """Find node ID by name."""
        for node_id, node in self.nodes.items():
            if node.name == name:
                return node_id
        return None

    def _calculate_max_depth(self, node_id: str, direction: str) -> int:
        """Calculate maximum depth from a node."""
        adj = self._adjacency if direction == "down" else self._reverse_adjacency
        visited: Set[str] = set()
        max_depth = 0
        queue = deque([(node_id, 0)])
        visited.add(node_id)

        while queue:
            current, depth = queue.popleft()
            max_depth = max(max_depth, depth)
            for neighbor in adj[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth + 1))

        return max_depth


def create_lineage_tracker(config: Optional[Dict[str, Any]] = None) -> DataLineageGraph:
    """Factory function to create a configured lineage tracker."""
    return DataLineageGraph()
