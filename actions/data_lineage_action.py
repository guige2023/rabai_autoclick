"""
Data Lineage Tracking Module.

Tracks the origin, transformation, and movement of data
throughout its lifecycle. Supports DAG-based lineage graphs,
provenance queries, and impact analysis.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class LineageType(Enum):
    """Types of data lineage."""
    TABLE = "table"
    COLUMN = "column"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    MODEL = "model"


class OperationType(Enum):
    """Types of data operations."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    JOIN = "join"
    UNION = "union"
    COPY = "copy"
    EXPORT = "export"
    IMPORT = "import"


@dataclass
class LineageNode:
    """Represents a node in the lineage graph."""
    node_id: str
    name: str
    lineage_type: LineageType
    version: str = "1.0"
    schema: Optional[dict[str, Any]] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class LineageEdge:
    """Represents an edge (relationship) in the lineage graph."""
    edge_id: str
    source_id: str
    target_id: str
    operation: OperationType
    transformation: Optional[str] = None
    columns: Optional[list[str]] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class DataProvenance:
    """Provenance record for a data asset."""
    asset_id: str
    asset_name: str
    asset_type: LineageType
    version: str
    created_by: str
    created_at: float
    sources: list[str]
    operations: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class DataLineageTracker:
    """
    Data lineage and provenance tracker.

    Builds and queries data lineage graphs showing how data
    flows and transforms through systems.

    Example:
        tracker = DataLineageTracker()
        tracker.register_node("orders", LineageType.TABLE, schema={...})
        tracker.register_node("order_reports", LineageType.TABLE)
        tracker.link("orders", "order_reports", OperationType.READ)
        lineage = tracker.get_lineage("order_reports", direction="upstream")
    """

    def __init__(self) -> None:
        self._nodes: dict[str, LineageNode] = {}
        self._edges: dict[str, LineageEdge] = {}
        self._outgoing: dict[str, list[str]] = {}
        self._incoming: dict[str, list[str]] = {}
        self._provenance: dict[str, DataProvenance] = {}

    def register_node(
        self,
        name: str,
        lineage_type: LineageType,
        version: str = "1.0",
        schema: Optional[dict[str, Any]] = None,
        metadata: Optional[dict[str, Any]] = None
    ) -> LineageNode:
        """
        Register a data asset node.

        Args:
            name: Node name
            lineage_type: Type of data asset
            version: Asset version
            schema: Optional schema definition
            metadata: Additional metadata

        Returns:
            Created LineageNode
        """
        node_id = str(uuid.uuid4())
        node = LineageNode(
            node_id=node_id,
            name=name,
            lineage_type=lineage_type,
            version=version,
            schema=schema,
            metadata=metadata or {}
        )
        self._nodes[name] = node
        self._outgoing[name] = []
        self._incoming[name] = []
        return node

    def get_node(self, name: str) -> Optional[LineageNode]:
        """Get a node by name."""
        return self._nodes.get(name)

    def link(
        self,
        source: str,
        target: str,
        operation: OperationType,
        transformation: Optional[str] = None,
        columns: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None
    ) -> LineageEdge:
        """
        Create a lineage edge between two nodes.

        Args:
            source: Source node name
            target: Target node name
            operation: Operation type
            transformation: SQL or expression describing transformation
            columns: Columns involved in the lineage
            metadata: Additional metadata

        Returns:
            Created LineageEdge
        """
        if source not in self._nodes:
            self.register_node(source, LineageType.TABLE)
        if target not in self._nodes:
            self.register_node(target, LineageType.TABLE)

        edge_id = str(uuid.uuid4())
        edge = LineageEdge(
            edge_id=edge_id,
            source_id=source,
            target_id=target,
            operation=operation,
            transformation=transformation,
            columns=columns,
            metadata=metadata or {}
        )

        self._edges[f"{source}:{target}"] = edge
        self._outgoing[source].append(target)
        self._incoming[target].append(source)

        return edge

    def get_lineage(
        self,
        node_name: str,
        direction: str = "both",
        depth: int = -1
    ) -> dict[str, Any]:
        """
        Get lineage for a node.

        Args:
            node_name: Starting node
            direction: "upstream", "downstream", or "both"
            depth: Maximum depth (-1 for unlimited)

        Returns:
            Dictionary with upstream/downstream lineage
        """
        result: dict[str, Any] = {
            "node": self._nodes.get(node_name),
            "upstream": [],
            "downstream": []
        }

        if direction in ("upstream", "both"):
            result["upstream"] = self._get_upstream(node_name, depth)

        if direction in ("downstream", "both"):
            result["downstream"] = self._get_downstream(node_name, depth)

        return result

    def _get_upstream(
        self,
        node: str,
        max_depth: int,
        current_depth: int = 0,
        visited: Optional[set[str]] = None
    ) -> list[dict[str, Any]]:
        """Get upstream lineage recursively."""
        if visited is None:
            visited = set()

        if node in visited or (max_depth >= 0 and current_depth >= max_depth):
            return []

        visited.add(node)
        results = []

        for parent in self._incoming.get(node, []):
            edge = self._edges.get(f"{parent}:{node}")
            node_data = {
                "name": parent,
                "node": self._nodes.get(parent),
                "edge": edge,
                "depth": current_depth + 1
            }
            results.append(node_data)
            results.extend(self._get_upstream(parent, max_depth, current_depth + 1, visited))

        return results

    def _get_downstream(
        self,
        node: str,
        max_depth: int,
        current_depth: int = 0,
        visited: Optional[set[str]] = None
    ) -> list[dict[str, Any]]:
        """Get downstream lineage recursively."""
        if visited is None:
            visited = set()

        if node in visited or (max_depth >= 0 and current_depth >= max_depth):
            return []

        visited.add(node)
        results = []

        for child in self._outgoing.get(node, []):
            edge = self._edges.get(f"{node}:{child}")
            node_data = {
                "name": child,
                "node": self._nodes.get(child),
                "edge": edge,
                "depth": current_depth + 1
            }
            results.append(node_data)
            results.extend(self._get_downstream(child, max_depth, current_depth + 1, visited))

        return results

    def impact_analysis(
        self,
        node_name: str,
        column: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Perform impact analysis on a node.

        Shows all downstream consumers and dependencies.

        Args:
            node_name: Node to analyze
            column: Optional column to focus on

        Returns:
            Impact analysis results
        """
        downstream = self._get_downstream(node_name, depth=-1)

        impacted_nodes = []
        impacted_apis = []

        for item in downstream:
            node = item["node"]
            if node:
                if node.lineage_type == LineageType.API:
                    impacted_apis.append(node.name)
                else:
                    impacted_nodes.append(node.name)

        return {
            "source": node_name,
            "column": column,
            "total_impacted": len(impacted_nodes) + len(impacted_apis),
            "impacted_tables": impacted_nodes,
            "impacted_apis": impacted_apis,
            "lineage": downstream
        }

    def record_provenance(
        self,
        asset_name: str,
        created_by: str,
        sources: list[str],
        operations: list[str],
        metadata: Optional[dict[str, Any]] = None
    ) -> DataProvenance:
        """Record provenance for a data asset."""
        node = self._nodes.get(asset_name)
        if not node:
            self.register_node(asset_name, LineageType.TABLE)

        provenance = DataProvenance(
            asset_id=str(uuid.uuid4()),
            asset_name=asset_name,
            asset_type=node.lineage_type if node else LineageType.TABLE,
            version=node.version if node else "1.0",
            created_by=created_by,
            created_at=time.time(),
            sources=sources,
            operations=operations,
            metadata=metadata or {}
        )

        self._provenance[asset_name] = provenance
        return provenance

    def get_provenance(self, asset_name: str) -> Optional[DataProvenance]:
        """Get provenance record for an asset."""
        return self._provenance.get(asset_name)

    def get_full_graph(self) -> dict[str, Any]:
        """Get the full lineage graph."""
        return {
            "nodes": list(self._nodes.values()),
            "edges": list(self._edges.values()),
            "statistics": {
                "total_nodes": len(self._nodes),
                "total_edges": len(self._edges),
                "by_type": self._count_by_type()
            }
        }

    def _count_by_type(self) -> dict[str, int]:
        """Count nodes by lineage type."""
        counts: dict[str, int] = {}
        for node in self._nodes.values():
            type_name = node.lineage_type.value
            counts[type_name] = counts.get(type_name, 0) + 1
        return counts

    def find_path(
        self,
        source: str,
        target: str
    ) -> Optional[list[str]]:
        """Find a lineage path between two nodes."""
        if source not in self._nodes or target not in self._nodes:
            return None

        visited: set[str] = set()
        queue: list[tuple[str, list[str]]] = [(source, [source])]

        while queue:
            current, path = queue.pop(0)

            if current == target:
                return path

            if current in visited:
                continue
            visited.add(current)

            for child in self._outgoing.get(current, []):
                if child not in visited:
                    queue.append((child, path + [child]))

        return None
