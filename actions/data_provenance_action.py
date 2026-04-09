"""
Data Provenance Action.

Tracks the complete lineage and origin of data assets,
recording transformations, sources, and custody chain.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class ProvenanceEventType(Enum):
    """Types of provenance events."""
    CREATED = auto()
    TRANSFORMED = auto()
    COPIED = auto()
    MOVED = auto()
    DELETED = auto()
    VALIDATED = auto()
    CLASSIFIED = auto()
    SHARED = auto()
    ARCHIVED = auto()
    RESTORED = auto()


@dataclass
class ProvenanceNode:
    """A single node in the provenance graph."""
    node_id: str
    event_type: ProvenanceEventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    actor: str = "unknown"
    source_urn: Optional[str] = None
    destination_urn: Optional[str] = None
    transformation_type: Optional[str] = None
    transformation_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_ids: List[str] = field(default_factory=list)  # For derived data
    hash: Optional[str] = None


@dataclass
class DataAsset:
    """A data asset with its complete provenance chain."""
    asset_id: str
    asset_type: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    owner: str = "unknown"
    classification: str = "unknown"
    provenance_chain: List[str] = field(default_factory=list)  # node IDs
    metadata: Dict[str, Any] = field(default_factory=dict)
    current_hash: Optional[str] = None
    version: int = 1


@dataclass
class ProvenanceQuery:
    """Query parameters for provenance lookups."""
    asset_id: Optional[str] = None
    node_id: Optional[str] = None
    event_types: Optional[List[ProvenanceEventType]] = None
    actor: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    limit: int = 100


class DataProvenanceTracker:
    """
    Track data provenance and lineage.

    Example:
        tracker = DataProvenanceTracker()
        tracker.register_asset("dataset-001", "dataset", owner="alice")
        tracker.record_event("dataset-001", ProvenanceEventType.TRANSFORMED,
                           actor="pipeline-1", transformation_type="filter",
                           transformation_params={"column": "status", "value": "active"})
        chain = tracker.get_provenance_chain("dataset-001")
    """

    def __init__(self) -> None:
        self._assets: Dict[str, DataAsset] = {}
        self._nodes: Dict[str, ProvenanceNode] = {}
        self._edges: Dict[str, List[str]] = {}  # node_id -> child node_ids

    def register_asset(
        self,
        asset_id: str,
        asset_type: str,
        owner: str = "unknown",
        classification: str = "unknown",
        source_urn: Optional[str] = None,
    ) -> Self:
        """Register a new data asset."""
        node = ProvenanceNode(
            node_id=f"{asset_id}-origin",
            event_type=ProvenanceEventType.CREATED,
            source_urn=source_urn,
            actor=owner,
        )
        self._nodes[node.node_id] = node

        asset = DataAsset(
            asset_id=asset_id,
            asset_type=asset_type,
            owner=owner,
            classification=classification,
            provenance_chain=[node.node_id],
        )
        self._assets[asset_id] = asset
        self._edges[node.node_id] = []
        return self

    def record_event(
        self,
        asset_id: str,
        event_type: ProvenanceEventType,
        actor: str = "unknown",
        transformation_type: Optional[str] = None,
        transformation_params: Optional[Dict[str, Any]] = None,
        parent_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        hash_value: Optional[str] = None,
    ) -> str:
        """Record a provenance event for an asset. Returns the new node ID."""
        asset = self._assets.get(asset_id)
        if not asset:
            raise KeyError(f"Asset {asset_id} not found")

        node_id = f"{asset_id}-{event_type.name.lower()}-{datetime.now(timezone.utc).timestamp()}"
        node = ProvenanceNode(
            node_id=node_id,
            event_type=event_type,
            actor=actor,
            transformation_type=transformation_type,
            transformation_params=transformation_params or {},
            metadata=metadata or {},
            parent_ids=parent_ids or [],
            hash=hash_value,
        )
        self._nodes[node_id] = node
        self._edges[node_id] = []

        # Update parent edges
        if parent_ids:
            for parent_id in parent_ids:
                if parent_id in self._edges:
                    self._edges[parent_id].append(node_id)

        # Update asset
        asset.provenance_chain.append(node_id)
        asset.updated_at = datetime.now(timezone.utc)
        asset.version += 1
        if hash_value:
            asset.current_hash = hash_value

        return node_id

    def get_provenance_chain(self, asset_id: str) -> List[ProvenanceNode]:
        """Get the full provenance chain for an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return []
        return [self._nodes[nid] for nid in asset.provenance_chain if nid in self._nodes]

    def get_upstream(self, node_id: str, max_depth: int = 10) -> List[ProvenanceNode]:
        """Get all upstream (ancestor) nodes."""
        visited: List[ProvenanceNode] = []
        queue: List[Tuple[str, int]] = [(node_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            if current_id not in self._nodes:
                continue
            node = self._nodes[current_id]
            visited.append(node)
            for parent_id in node.parent_ids:
                if parent_id in self._nodes:
                    queue.append((parent_id, depth + 1))

        return visited

    def get_downstream(self, node_id: str, max_depth: int = 10) -> List[ProvenanceNode]:
        """Get all downstream (descendant) nodes."""
        visited: List[ProvenanceNode] = []
        queue: List[Tuple[str, int]] = [(node_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            if current_id not in self._edges:
                continue
            for child_id in self._edges[current_id]:
                if child_id in self._nodes:
                    visited.append(self._nodes[child_id])
                    queue.append((child_id, depth + 1))

        return visited

    def query(
        self,
        query: ProvenanceQuery,
    ) -> List[ProvenanceNode]:
        """Query provenance nodes with filters."""
        results: List[ProvenanceNode] = []

        for node in self._nodes.values():
            if query.asset_id:
                if not any(query.asset_id in nid for nid in [node.node_id]):
                    continue

            if query.node_id and node.node_id != query.node_id:
                continue

            if query.event_types and node.event_type not in query.event_types:
                continue

            if query.actor and node.actor != query.actor:
                continue

            if query.since and node.timestamp < query.since:
                continue

            if query.until and node.timestamp > query.until:
                continue

            results.append(node)

        return sorted(results, key=lambda n: n.timestamp, reverse=True)[:query.limit]

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get an asset by ID."""
        return self._assets.get(asset_id)

    def list_assets(self, classification: Optional[str] = None) -> List[DataAsset]:
        """List all registered assets."""
        if classification:
            return [a for a in self._assets.values() if a.classification == classification]
        return list(self._assets.values())
