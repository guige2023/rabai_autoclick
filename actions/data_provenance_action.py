"""
Data Provenance Action Module

Tracks data lineage and provenance through transformations,
recording source, processing steps, and ownership.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import logging

logger = logging.getLogger(__name__)


class ProvenanceEventType(Enum):
    """Types of provenance events."""

    CREATED = auto()
    READ = auto()
    TRANSFORMED = auto()
    AGGREGATED = auto()
    FILTERED = auto()
    JOINED = auto()
    EXPORTED = auto()
    DELETED = auto()
    VALIDATED = auto()
    ENRICHED = auto()


@dataclass
class ProvenanceNode:
    """A node in the data provenance graph."""

    node_id: str
    data_id: str
    event_type: ProvenanceEventType
    timestamp: float
    actor: str
    source_uri: Optional[str] = None
    schema: Optional[Dict[str, Any]] = None
    row_count: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "data_id": self.data_id,
            "event_type": self.event_type.name,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "source_uri": self.source_uri,
            "schema": self.schema,
            "row_count": self.row_count,
            "metadata": self.metadata,
            "parent_ids": self.parent_ids,
        }


@dataclass
class ProvenanceEdge:
    """An edge connecting provenance nodes."""

    edge_id: str
    from_node_id: str
    to_node_id: str
    relationship: str
    transformation: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "from_node_id": self.from_node_id,
            "to_node_id": self.to_node_id,
            "relationship": self.relationship,
            "transformation": self.transformation,
        }


@dataclass
class DataFingerprint:
    """Fingerprint of data for integrity verification."""

    data_id: str
    algorithm: str
    hash_value: str
    row_count: int
    column_count: int
    timestamp: float

    def verify(self, data: Any) -> bool:
        """Verify data matches this fingerprint."""
        new_hash = self._compute_hash(data)
        return new_hash == self.hash_value

    def _compute_hash(self, data: Any) -> str:
        """Compute hash of data."""
        serialized = json.dumps(data, sort_keys=True, default=str)
        if self.algorithm == "sha256":
            return hashlib.sha256(serialized.encode()).hexdigest()
        elif self.algorithm == "md5":
            return hashlib.md5(serialized.encode()).hexdigest()
        return serialized[:16]


class ProvenanceGraph:
    """Directed acyclic graph of data provenance."""

    def __init__(self) -> None:
        self._nodes: Dict[str, ProvenanceNode] = {}
        self._edges: Dict[str, ProvenanceEdge] = {}
        self._data_index: Dict[str, List[str]] = {}

    def add_node(self, node: ProvenanceNode) -> None:
        """Add a provenance node."""
        self._nodes[node.node_id] = node
        if node.data_id not in self._data_index:
            self._data_index[node.data_id] = []
        self._data_index[node.data_id].append(node.node_id)

    def add_edge(self, edge: ProvenanceEdge) -> None:
        """Add a provenance edge."""
        self._edges[edge.edge_id] = edge

    def get_lineage(
        self,
        data_id: str,
        direction: str = "upstream",
    ) -> List[ProvenanceNode]:
        """Get lineage chain for a data entity."""
        if data_id not in self._data_index:
            return []

        nodes = []
        visited: set = set()
        queue = self._data_index[data_id].copy()

        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)

            if node_id in self._nodes:
                node = self._nodes[node_id]
                nodes.append(node)

                if direction == "upstream":
                    queue.extend(node.parent_ids)
                else:
                    for edge in self._edges.values():
                        if edge.from_node_id == node_id:
                            queue.append(edge.to_node_id)

        return nodes

    def to_dict(self) -> Dict[str, Any]:
        """Export graph as dictionary."""
        return {
            "nodes": [n.to_dict() for n in self._nodes.values()],
            "edges": [e.to_dict() for e in self._edges.values()],
        }


class ProvenanceAction:
    """Action class for data provenance tracking."""

    def __init__(self, actor: str = "system") -> None:
        self.actor = actor
        self.graph = ProvenanceGraph()
        self._current_data: Dict[str, Any] = {}

    def track_create(
        self,
        data: Any,
        source_uri: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        row_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Track creation of new data."""
        data_id = str(uuid.uuid4())
        node_id = str(uuid.uuid4())

        node = ProvenanceNode(
            node_id=node_id,
            data_id=data_id,
            event_type=ProvenanceEventType.CREATED,
            timestamp=time.time(),
            actor=self.actor,
            source_uri=source_uri,
            schema=schema,
            row_count=row_count,
            metadata=metadata or {},
        )

        self.graph.add_node(node)
        self._current_data[data_id] = data

        return data_id

    def track_transform(
        self,
        input_data_ids: List[str],
        output_data: Any,
        transformation: str,
        schema: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Track data transformation."""
        output_data_id = str(uuid.uuid4())
        output_node_id = str(uuid.uuid4())

        node = ProvenanceNode(
            node_id=output_node_id,
            data_id=output_data_id,
            event_type=ProvenanceEventType.TRANSFORMED,
            timestamp=time.time(),
            actor=self.actor,
            schema=schema,
            metadata=metadata or {},
            parent_ids=input_data_ids,
        )

        self.graph.add_node(node)

        for parent_id in input_data_ids:
            edge = ProvenanceEdge(
                edge_id=str(uuid.uuid4()),
                from_node_id=self.graph._data_index.get(parent_id, [""])[0],
                to_node_id=output_node_id,
                relationship="derived_from",
                transformation=transformation,
            )
            self.graph.add_edge(edge)

        self._current_data[output_data_id] = output_data
        return output_data_id

    def track_read(
        self,
        data_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Track data read access."""
        node = ProvenanceNode(
            node_id=str(uuid.uuid4()),
            data_id=data_id,
            event_type=ProvenanceEventType.READ,
            timestamp=time.time(),
            actor=self.actor,
            metadata=metadata or {},
        )
        self.graph.add_node(node)

    def get_full_lineage(self, data_id: str) -> Dict[str, Any]:
        """Get complete lineage for a data entity."""
        upstream = self.graph.get_lineage(data_id, direction="upstream")
        downstream = self.graph.get_lineage(data_id, direction="downstream")
        return {
            "data_id": data_id,
            "upstream": [n.to_dict() for n in upstream],
            "downstream": [n.to_dict() for n in downstream],
        }

    def export_graph(self) -> Dict[str, Any]:
        """Export the entire provenance graph."""
        return self.graph.to_dict()
