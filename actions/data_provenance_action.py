"""
Data Provenance Tracking Module.

Tracks the complete lineage and provenance of data assets
throughout their lifecycle. Supports reproducibility,
audit trails, and trust verification.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class ProvenanceType(Enum):
    """Types of provenance records."""
    DATA_ENTITY = "data_entity"
    PROCESS = "process"
    AGENT = "agent"
    DERIVATION = "derivation"
    USAGE = "usage"


@dataclass
class ProvenanceNode:
    """Represents a node in the provenance graph."""
    node_id: str
    node_type: ProvenanceType
    name: str
    version: str = "1.0"
    metadata: dict[str, Any] = field(default_factory=dict)
    content_hash: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    created_by: Optional[str] = None


@dataclass
class ProvenanceEdge:
    """Represents a relationship between provenance nodes."""
    edge_id: str
    source_id: str
    target_id: str
    relation_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProvenanceRecord:
    """Complete provenance record for a data asset."""
    entity_id: str
    nodes: list[ProvenanceNode] = field(default_factory=list)
    edges: list[ProvenanceEdge] = field(default_factory=list)
    annotations: dict[str, Any] = field(default_factory=dict)


class DataProvenanceTracker:
    """
    Data provenance tracking and verification.

    Maintains complete audit trails of data assets including
    their origin, transformations, and usage.

    Example:
        tracker = DataProvenanceTracker()
        entity = tracker.register_entity("report.csv", content_hash="...")
        tracker.add_derivation(entity.id, source_ids=[input.id])
        lineage = tracker.get_lineage(entity.id)
    """

    def __init__(self) -> None:
        self._nodes: dict[str, ProvenanceNode] = {}
        self._edges: list[ProvenanceEdge] = []
        self._outgoing: dict[str, list[str]] = {}
        self._incoming: dict[str, list[str]] = {}
        self._entity_nodes: dict[str, str] = {}

    def register_entity(
        self,
        name: str,
        version: str = "1.0",
        content_hash: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        created_by: Optional[str] = None
    ) -> ProvenanceNode:
        """
        Register a data entity.

        Args:
            name: Entity name
            version: Version string
            content_hash: Hash of the entity content
            metadata: Additional metadata
            created_by: Creator identifier

        Returns:
            Created ProvenanceNode
        """
        node_id = str(uuid.uuid4())
        node = ProvenanceNode(
            node_id=node_id,
            node_type=ProvenanceType.DATA_ENTITY,
            name=name,
            version=version,
            content_hash=content_hash,
            metadata=metadata or {},
            created_by=created_by
        )
        self._nodes[node_id] = node
        self._entity_nodes[name] = node_id
        self._outgoing[node_id] = []
        self._incoming[node_id] = []
        return node

    def register_process(
        self,
        name: str,
        version: str = "1.0",
        metadata: Optional[dict[str, Any]] = None,
        executed_by: Optional[str] = None
    ) -> ProvenanceNode:
        """Register a process that transforms data."""
        node_id = str(uuid.uuid4())
        node = ProvenanceNode(
            node_id=node_id,
            node_type=ProvenanceType.PROCESS,
            name=name,
            version=version,
            metadata={**(metadata or {}), "executed_by": executed_by}
        )
        self._nodes[node_id] = node
        self._outgoing[node_id] = []
        self._incoming[node_id] = []
        return node

    def register_agent(
        self,
        name: str,
        agent_type: str,
        metadata: Optional[dict[str, Any]] = None
    ) -> ProvenanceNode:
        """Register an agent (person, system, or service)."""
        node_id = str(uuid.uuid4())
        node = ProvenanceNode(
            node_id=node_id,
            node_type=ProvenanceType.AGENT,
            name=name,
            metadata={**(metadata or {}), "agent_type": agent_type}
        )
        self._nodes[node_id] = node
        self._outgoing[node_id] = []
        self._incoming[node_id] = []
        return node

    def add_derivation(
        self,
        target_id: str,
        source_ids: list[str],
        metadata: Optional[dict[str, Any]] = None
    ) -> ProvenanceEdge:
        """
        Record that target was derived from sources.

        Args:
            target_id: Derived entity/process
            source_ids: Source entity/processes
            metadata: Additional metadata

        Returns:
            Created ProvenanceEdge
        """
        edge_id = str(uuid.uuid4())
        for source_id in source_ids:
            edge = ProvenanceEdge(
                edge_id=edge_id,
                source_id=source_id,
                target_id=target_id,
                relation_type="wasDerivedFrom",
                metadata=metadata or {}
            )
            self._edges.append(edge)
            self._outgoing[source_id].append(target_id)
            self._incoming[target_id].append(source_id)
            edge_id = str(uuid.uuid4())
        return self._edges[-1]

    def add_usage(
        self,
        entity_id: str,
        used_by_id: str,
        metadata: Optional[dict[str, Any]] = None
    ) -> ProvenanceEdge:
        """Record that an entity was used by a process."""
        edge_id = str(uuid.uuid4())
        edge = ProvenanceEdge(
            edge_id=edge_id,
            source_id=entity_id,
            target_id=used_by_id,
            relation_type="wasUsedBy",
            metadata=metadata or {}
        )
        self._edges.append(edge)
        self._outgoing[entity_id].append(used_by_id)
        self._incoming[used_by_id].append(entity_id)
        return edge

    def add_generation(
        self,
        entity_id: str,
        generated_by_id: str,
        metadata: Optional[dict[str, Any]] = None
    ) -> ProvenanceEdge:
        """Record that an entity was generated by a process."""
        edge_id = str(uuid.uuid4())
        edge = ProvenanceEdge(
            edge_id=edge_id,
            source_id=generated_by_id,
            target_id=entity_id,
            relation_type="wasGeneratedBy",
            metadata=metadata or {}
        )
        self._edges.append(edge)
        self._outgoing[generated_by_id].append(entity_id)
        self._incoming[entity_id].append(generated_by_id)
        return edge

    def add_attribution(
        self,
        entity_id: str,
        agent_id: str,
        role: str = "creator"
    ) -> ProvenanceEdge:
        """Attribute an entity to an agent."""
        edge_id = str(uuid.uuid4())
        edge = ProvenanceEdge(
            edge_id=edge_id,
            source_id=agent_id,
            target_id=entity_id,
            relation_type="wasAttributedTo",
            metadata={"role": role}
        )
        self._edges.append(edge)
        self._outgoing[agent_id].append(entity_id)
        self._incoming[entity_id].append(agent_id)
        return edge

    def get_lineage(
        self,
        node_id: str,
        direction: str = "both",
        depth: int = -1
    ) -> ProvenanceRecord:
        """
        Get complete lineage for a node.

        Args:
            node_id: Starting node
            direction: "upstream", "downstream", or "both"
            depth: Maximum depth (-1 for unlimited)

        Returns:
            ProvenanceRecord with lineage
        """
        nodes = [self._nodes[node_id]] if node_id in self._nodes else []
        edges: list[ProvenanceEdge] = []

        if direction in ("upstream", "both"):
            upstream = self._get_upstream_nodes(node_id, depth)
            nodes.extend(upstream)

        if direction in ("downstream", "both"):
            downstream = self._get_downstream_nodes(node_id, depth)
            nodes.extend(downstream)

        for edge in self._edges:
            if edge.source_id in [n.node_id for n in nodes] or edge.target_id in [n.node_id for n in nodes]:
                edges.append(edge)

        unique_nodes = {n.node_id: n for n in nodes}.values()

        return ProvenanceRecord(
            entity_id=node_id,
            nodes=list(unique_nodes),
            edges=edges
        )

    def _get_upstream_nodes(
        self,
        node_id: str,
        max_depth: int,
        current_depth: int = 0,
        visited: Optional[set[str]] = None
    ) -> list[ProvenanceNode]:
        """Get all upstream ancestors."""
        if visited is None:
            visited = set()

        if node_id in visited or (max_depth >= 0 and current_depth >= max_depth):
            return []

        visited.add(node_id)
        nodes = []

        for parent_id in self._incoming.get(node_id, []):
            if parent_id in self._nodes:
                nodes.append(self._nodes[parent_id])
                nodes.extend(self._get_upstream_nodes(parent_id, max_depth, current_depth + 1, visited))

        return nodes

    def _get_downstream_nodes(
        self,
        node_id: str,
        max_depth: int,
        current_depth: int = 0,
        visited: Optional[set[str]] = None
    ) -> list[ProvenanceNode]:
        """Get all downstream descendants."""
        if visited is None:
            visited = set()

        if node_id in visited or (max_depth >= 0 and current_depth >= max_depth):
            return []

        visited.add(node_id)
        nodes = []

        for child_id in self._outgoing.get(node_id, []):
            if child_id in self._nodes:
                nodes.append(self._nodes[child_id])
                nodes.extend(self._get_downstream_nodes(child_id, max_depth, current_depth + 1, visited))

        return nodes

    def verify_integrity(self, node_id: str) -> dict[str, Any]:
        """
        Verify the integrity of a provenance chain.

        Args:
            node_id: Node to verify

        Returns:
            Verification result with any issues found
        """
        issues: list[str] = []
        warnings: list[str] = []

        node = self._nodes.get(node_id)
        if not node:
            return {"valid": False, "issues": [f"Node {node_id} not found"]}

        if node.content_hash:
            upstream = self._get_upstream_nodes(node_id, depth=-1)
            if not upstream:
                warnings.append("No upstream lineage - cannot verify derivation chain")

        cycles = self._detect_cycles(node_id)
        if cycles:
            issues.append(f"Cycle detected: {' -> '.join(cycles)}")

        return {
            "valid": len(issues) == 0,
            "node_id": node_id,
            "issues": issues,
            "warnings": warnings
        }

    def _detect_cycles(self, start_id: str) -> Optional[list[str]]:
        """Detect cycles in provenance graph."""
        visited: set[str] = set()
        path: list[str] = []

        def dfs(node_id: str) -> Optional[list[str]]:
            if node_id in path:
                cycle_start = path.index(node_id)
                return path[cycle_start:] + [node_id]

            if node_id in visited:
                return None

            visited.add(node_id)
            path.append(node_id)

            for child_id in self._outgoing.get(node_id, []):
                result = dfs(child_id)
                if result:
                    return result

            path.pop()
            return None

        return dfs(start_id)

    def get_record(self, entity_name: str) -> Optional[ProvenanceRecord]:
        """Get provenance record by entity name."""
        node_id = self._entity_nodes.get(entity_name)
        if node_id:
            return self.get_lineage(node_id)
        return None

    def export_provenance(self, node_id: str) -> str:
        """Export provenance as JSON."""
        record = self.get_lineage(node_id)
        return json.dumps({
            "entity_id": record.entity_id,
            "nodes": [n.__dict__ for n in record.nodes],
            "edges": [e.__dict__ for e in record.edges],
            "annotations": record.annotations
        }, indent=2, default=str)

    def get_statistics(self) -> dict[str, Any]:
        """Get provenance statistics."""
        by_type: dict[str, int] = {}
        for node in self._nodes.values():
            type_name = node.node_type.value
            by_type[type_name] = by_type.get(type_name, 0) + 1

        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "by_type": by_type
        }
