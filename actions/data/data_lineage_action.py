"""Data lineage tracking for automation workflows.

Records the provenance and transformation history of data
as it flows through automation pipelines.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class LineageEventType(Enum):
    """Types of lineage events."""
    CREATED = "created"
    TRANSFORMED = "transformed"
    FILTERED = "filtered"
    AGGREGATED = "aggregated"
    JOINED = "joined"
    SPLIT = "split"
    COPIED = "copied"
    ARCHIVED = "archived"
    DELETED = "deleted"
    VALIDATED = "validated"
    IMPORTED = "imported"
    EXPORTED = "exported"


@dataclass
class LineageNode:
    """A single node in the lineage graph."""
    node_id: str
    dataset_name: str
    event_type: LineageEventType
    timestamp: float = field(default_factory=time.time)
    upstream_ids: List[str] = field(default_factory=list)
    downstream_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    schema: Optional[Dict[str, str]] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    tags: Set[str] = field(default_factory=set)
    owner: Optional[str] = None
    source_location: Optional[str] = None
    quality_score: Optional[float] = None


@dataclass
class LineageEdge:
    """An edge connecting two lineage nodes."""
    edge_id: str
    source_id: str
    target_id: str
    transformation: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetVersion:
    """Version information for a dataset."""
    version_id: str
    node_id: str
    content_hash: str
    created_at: float = field(default_factory=time.time)
    changes: List[str] = field(default_factory=list)
    parent_version_id: Optional[str] = None


class LineageGraph:
    """In-memory lineage graph with thread safety."""

    def __init__(self, max_versions: int = 100):
        self._nodes: Dict[str, LineageNode] = {}
        self._edges: Dict[str, LineageEdge] = {}
        self._versions: Dict[str, List[DatasetVersion]] = {}
        self._lock = threading.RLock()
        self._max_versions = max_versions
        self._name_to_ids: Dict[str, List[str]] = {}

    def add_node(
        self,
        dataset_name: str,
        event_type: LineageEventType,
        upstream_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        schema: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        tags: Optional[Set[str]] = None,
        owner: Optional[str] = None,
        source_location: Optional[str] = None,
    ) -> str:
        """Add a new lineage node."""
        node_id = str(uuid.uuid4())[:16]

        node = LineageNode(
            node_id=node_id,
            dataset_name=dataset_name,
            event_type=event_type,
            upstream_ids=upstream_ids or [],
            metadata=metadata or {},
            schema=schema,
            row_count=row_count,
            size_bytes=size_bytes,
            tags=tags or set(),
            owner=owner,
            source_location=source_location,
        )

        with self._lock:
            self._nodes[node_id] = node
            self._name_to_ids.setdefault(dataset_name, []).append(node_id)

            for upstream_id in (upstream_ids or []):
                edge_id = f"{upstream_id}->{node_id}"
                edge = LineageEdge(
                    edge_id=edge_id,
                    source_id=upstream_id,
                    target_id=node_id,
                )
                self._edges[edge_id] = edge

                upstream_node = self._nodes.get(upstream_id)
                if upstream_node:
                    upstream_node.downstream_ids.append(node_id)

        return node_id

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        """Get a lineage node by ID."""
        with self._lock:
            return copy.deepcopy(self._nodes.get(node_id))

    def get_ancestors(
        self,
        node_id: str,
        max_depth: int = 10,
    ) -> List[LineageNode]:
        """Get all ancestor nodes (upstream lineage)."""
        with self._lock:
            visited = set()
            result = []
            queue = [(node_id, 0)]

            while queue:
                current_id, depth = queue.pop(0)
                if current_id in visited or depth > max_depth:
                    continue
                visited.add(current_id)

                node = self._nodes.get(current_id)
                if node:
                    result.append(copy.deepcopy(node))
                    for upstream_id in node.upstream_ids:
                        if upstream_id not in visited:
                            queue.append((upstream_id, depth + 1))

            return result

    def get_descendants(
        self,
        node_id: str,
        max_depth: int = 10,
    ) -> List[LineageNode]:
        """Get all descendant nodes (downstream lineage)."""
        with self._lock:
            visited = set()
            result = []
            queue = [(node_id, 0)]

            while queue:
                current_id, depth = queue.pop(0)
                if current_id in visited or depth > max_depth:
                    continue
                visited.add(current_id)

                node = self._nodes.get(current_id)
                if node:
                    result.append(copy.deepcopy(node))
                    for downstream_id in node.downstream_ids:
                        if downstream_id not in visited:
                            queue.append((downstream_id, depth + 1))

            return result

    def get_full_lineage(
        self,
        node_id: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """Get complete lineage tree for a node."""
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return {}

            ancestors = self.get_ancestors(node_id, max_depth)
            descendants = self.get_descendants(node_id, max_depth)

            return {
                "node": copy.deepcopy(node),
                "ancestors": ancestors,
                "descendants": descendants,
                "total_upstream": len(ancestors),
                "total_downstream": len(descendants),
            }

    def add_version(
        self,
        node_id: str,
        content_hash: str,
        changes: Optional[List[str]] = None,
        parent_version_id: Optional[str] = None,
    ) -> str:
        """Add a version record for a dataset node."""
        version_id = str(uuid.uuid4())[:16]
        version = DatasetVersion(
            version_id=version_id,
            node_id=node_id,
            content_hash=content_hash,
            changes=changes or [],
            parent_version_id=parent_version_id,
        )

        with self._lock:
            self._versions.setdefault(node_id, []).append(version)
            if len(self._versions[node_id]) > self._max_versions:
                self._versions[node_id] = self._versions[node_id][-self._max_versions:]

            if node_id in self._nodes:
                self._nodes[node_id].metadata["latest_version"] = version_id

        return version_id

    def get_versions(self, node_id: str) -> List[DatasetVersion]:
        """Get version history for a node."""
        with self._lock:
            return list(self._versions.get(node_id, []))

    def get_graph(self) -> Dict[str, Any]:
        """Get the full lineage graph as a dictionary."""
        with self._lock:
            nodes = {
                nid: {
                    "node_id": node.node_id,
                    "dataset_name": node.dataset_name,
                    "event_type": node.event_type.value,
                    "timestamp": datetime.fromtimestamp(node.timestamp).isoformat(),
                    "upstream_ids": node.upstream_ids,
                    "downstream_ids": node.downstream_ids,
                    "tags": list(node.tags),
                    "owner": node.owner,
                    "row_count": node.row_count,
                    "size_bytes": node.size_bytes,
                }
                for nid, node in self._nodes.items()
            }

            edges = {
                eid: {
                    "edge_id": e.edge_id,
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "transformation": e.transformation,
                    "timestamp": datetime.fromtimestamp(e.timestamp).isoformat(),
                }
                for eid, e in self._edges.items()
            }

            return {
                "nodes": nodes,
                "edges": edges,
                "total_nodes": len(nodes),
                "total_edges": len(edges),
            }


class AutomationLineageAction:
    """Action providing data lineage tracking for automation workflows."""

    def __init__(self, graph: Optional[LineageGraph] = None):
        self._graph = graph or LineageGraph()

    def track(
        self,
        dataset_name: str,
        event_type: str,
        upstream_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        schema: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        source_location: Optional[str] = None,
    ) -> str:
        """Track a new data event in the lineage."""
        try:
            event_type_enum = LineageEventType(event_type)
        except ValueError:
            event_type_enum = LineageEventType.TRANSFORMED

        return self._graph.add_node(
            dataset_name=dataset_name,
            event_type=event_type_enum,
            upstream_ids=upstream_ids,
            metadata=metadata,
            schema=schema,
            row_count=row_count,
            size_bytes=size_bytes,
            tags=set(tags) if tags else None,
            owner=owner,
            source_location=source_location,
        )

    def track_version(
        self,
        node_id: str,
        data: Any,
        changes: Optional[List[str]] = None,
    ) -> str:
        """Track a new version of a dataset."""
        content_str = json.dumps(data, sort_keys=True, default=str)
        content_hash = hashlib.sha256(content_str.encode()).hexdigest()

        versions = self._graph.get_versions(node_id)
        parent_version_id = versions[-1].version_id if versions else None

        return self._graph.add_version(
            node_id=node_id,
            content_hash=content_hash,
            changes=changes,
            parent_version_id=parent_version_id,
        )

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an action with lineage tracking.

        Required params:
            dataset_name: str - Name of the dataset
            event_type: str - Type of lineage event
            operation: callable - The operation to track

        Optional params:
            upstream_ids: list - IDs of upstream nodes
            metadata: dict - Additional metadata
            tags: list - Tags for the node
            track_version: bool - Whether to track data version
        """
        dataset_name = params.get("dataset_name")
        event_type = params.get("event_type", "transformed")
        operation = params.get("operation")
        upstream_ids = params.get("upstream_ids", [])
        metadata = params.get("metadata", {})
        tags = params.get("tags", [])
        track_version = params.get("track_version", False)
        schema = params.get("schema")
        row_count = params.get("row_count")
        size_bytes = params.get("size_bytes")
        owner = params.get("owner")
        source_location = params.get("source_location")

        if not dataset_name:
            raise ValueError("dataset_name is required")
        if not callable(operation):
            raise ValueError("operation must be a callable")

        node_id = self.track(
            dataset_name=dataset_name,
            event_type=event_type,
            upstream_ids=upstream_ids,
            metadata=metadata,
            schema=schema,
            row_count=row_count,
            size_bytes=size_bytes,
            tags=tags,
            owner=owner,
            source_location=source_location,
        )

        result = operation(context=context, params=params)

        if track_version and result is not None:
            self.track_version(node_id, result)

        return {
            "node_id": node_id,
            "dataset_name": dataset_name,
            "event_type": event_type,
            "upstream_count": len(upstream_ids),
            "result": result,
        }

    def get_lineage(
        self,
        node_id: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """Get complete lineage for a node."""
        return self._graph.get_full_lineage(node_id, max_depth)

    def get_ancestors(self, node_id: str, max_depth: int = 10) -> List[Dict[str, Any]]:
        """Get upstream lineage."""
        nodes = self._graph.get_ancestors(node_id, max_depth)
        return [self._node_to_dict(n) for n in nodes]

    def get_descendants(self, node_id: str, max_depth: int = 10) -> List[Dict[str, Any]]:
        """Get downstream lineage."""
        nodes = self._graph.get_descendants(node_id, max_depth)
        return [self._node_to_dict(n) for n in nodes]

    def get_versions(self, node_id: str) -> List[Dict[str, Any]]:
        """Get version history for a node."""
        versions = self._graph.get_versions(node_id)
        return [
            {
                "version_id": v.version_id,
                "content_hash": v.content_hash,
                "created_at": datetime.fromtimestamp(v.created_at).isoformat(),
                "changes": v.changes,
                "parent_version_id": v.parent_version_id,
            }
            for v in versions
        ]

    def get_graph(self) -> Dict[str, Any]:
        """Get the full lineage graph."""
        return self._graph.get_graph()

    def _node_to_dict(self, node: LineageNode) -> Dict[str, Any]:
        """Convert a LineageNode to dictionary."""
        return {
            "node_id": node.node_id,
            "dataset_name": node.dataset_name,
            "event_type": node.event_type.value,
            "timestamp": datetime.fromtimestamp(node.timestamp).isoformat(),
            "upstream_ids": node.upstream_ids,
            "downstream_ids": node.downstream_ids,
            "metadata": node.metadata,
            "schema": node.schema,
            "row_count": node.row_count,
            "size_bytes": node.size_bytes,
            "tags": list(node.tags),
            "owner": node.owner,
            "source_location": node.source_location,
            "quality_score": node.quality_score,
        }
