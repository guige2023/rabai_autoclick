"""Consistent Hashing action module for RabAI AutoClick.

Provides consistent hash ring for distributed caching
and sharding with virtual nodes and replication.
"""

import sys
import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import bisect

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Node:
    """Represents a node in the hash ring."""
    node_id: str
    name: str
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HashRingConfig:
    """Configuration for consistent hash ring."""
    num_virtual_nodes: int = 150
    replication_factor: int = 1
    hash_algorithm: str = "md5"  # md5, sha256, murmur3


class ConsistentHashRing:
    """Consistent hash ring implementation with virtual nodes."""
    
    def __init__(self, config: Optional[HashRingConfig] = None):
        self._config = config or HashRingConfig()
        self._physical_nodes: Dict[str, Node] = {}
        self._virtual_nodes: Dict[int, str] = {}  # hash -> node_id
        self._sorted_keys: List[int] = []  # Sorted virtual node hashes
    
    def add_node(self, node: Node) -> None:
        """Add a node to the ring."""
        self._physical_nodes[node.node_id] = node
        
        # Add virtual nodes
        for i in range(self._config.num_virtual_nodes * node.weight):
            key = self._generate_key(node.node_id, i)
            self._virtual_nodes[key] = node.node_id
        
        self._rebuild_sorted_keys()
    
    def remove_node(self, node_id: str) -> None:
        """Remove a node from the ring."""
        if node_id not in self._physical_nodes:
            return
        
        node = self._physical_nodes[node_id]
        # Remove virtual nodes
        for i in range(self._config.num_virtual_nodes * node.weight):
            key = self._generate_key(node_id, i)
            self._virtual_nodes.pop(key, None)
        
        del self._physical_nodes[node_id]
        self._rebuild_sorted_keys()
    
    def _generate_key(self, node_id: str, virtual_index: int) -> int:
        """Generate hash key for a virtual node."""
        key_str = f"{node_id}:{virtual_index}"
        
        if self._config.hash_algorithm == "md5":
            hash_bytes = hashlib.md5(key_str.encode()).digest()
        elif self._config.hash_algorithm == "sha256":
            hash_bytes = hashlib.sha256(key_str.encode()).digest()
        else:
            hash_bytes = hashlib.md5(key_str.encode()).digest()
        
        return int.from_bytes(hash_bytes[:4], byteorder='big')
    
    def _rebuild_sorted_keys(self) -> None:
        """Rebuild sorted list of virtual node keys."""
        self._sorted_keys = sorted(self._virtual_nodes.keys())
    
    def _hash_key(self, key: str) -> int:
        """Hash a data key."""
        if self._config.hash_algorithm == "md5":
            hash_bytes = hashlib.md5(key.encode()).digest()
        elif self._config.hash_algorithm == "sha256":
            hash_bytes = hashlib.sha256(key.encode()).digest()
        else:
            hash_bytes = hashlib.md5(key.encode()).digest()
        
        return int.from_bytes(hash_bytes[:4], byteorder='big')
    
    def get_node(self, key: str) -> Optional[Node]:
        """Get the primary node for a key."""
        node_id = self.get_node_id(key)
        return self._physical_nodes.get(node_id)
    
    def get_node_id(self, key: str) -> Optional[str]:
        """Get the node ID for a key."""
        if not self._virtual_nodes:
            return None
        
        hash_key = self._hash_key(key)
        
        # Binary search for the first virtual node >= hash_key
        idx = bisect.bisect_right(self._sorted_keys, hash_key)
        
        # Wrap around to beginning if necessary
        if idx >= len(self._sorted_keys):
            idx = 0
        
        return self._virtual_nodes.get(self._sorted_keys[idx])
    
    def get_nodes_for_key(self, key: str, count: int = 1) -> List[Node]:
        """Get multiple nodes for a key (for replication)."""
        if not self._virtual_nodes:
            return []
        
        nodes = []
        hash_key = self._hash_key(key)
        
        idx = bisect.bisect_right(self._sorted_keys, hash_key)
        if idx >= len(self._sorted_keys):
            idx = 0
        
        seen_nodes = set()
        while len(nodes) < count and len(seen_nodes) < len(self._physical_nodes):
            node_id = self._virtual_nodes.get(self._sorted_keys[idx])
            if node_id and node_id not in seen_nodes:
                seen_nodes.add(node_id)
                nodes.append(self._physical_nodes[node_id])
            
            idx = (idx + 1) % len(self._sorted_keys)
        
        return nodes
    
    def list_nodes(self) -> List[Node]:
        """List all physical nodes."""
        return list(self._physical_nodes.values())
    
    def get_ring_info(self) -> Dict[str, Any]:
        """Get ring information."""
        return {
            "total_virtual_nodes": len(self._virtual_nodes),
            "total_physical_nodes": len(self._physical_nodes),
            "replication_factor": self._config.replication_factor,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "name": n.name,
                    "weight": n.weight,
                    "virtual_nodes": self._config.num_virtual_nodes * n.weight
                }
                for n in self._physical_nodes.values()
            ]
        }


class ConsistentHashAction(BaseAction):
    """Consistent hash ring for distributed caching and sharding.
    
    Supports virtual nodes, weighted nodes, and replication.
    """
    action_type = "consistent_hash"
    display_name = "一致性哈希"
    description = "一致性哈希环，用于分布式缓存和分片"
    
    def __init__(self):
        super().__init__()
        self._ring = ConsistentHashRing()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute consistent hash operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "add_node":
                return self._add_node(params)
            elif operation == "remove_node":
                return self._remove_node(params)
            elif operation == "get_node":
                return self._get_node(params)
            elif operation == "get_nodes":
                return self._get_nodes(params)
            elif operation == "list_nodes":
                return self._list_nodes(params)
            elif operation == "get_info":
                return self._get_info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _add_node(self, params: Dict[str, Any]) -> ActionResult:
        """Add a node to the ring."""
        node_id = params.get("node_id", "")
        name = params.get("name", "")
        weight = params.get("weight", 1)
        
        if not node_id:
            return ActionResult(success=False, message="node_id is required")
        
        node = Node(node_id=node_id, name=name or node_id, weight=weight)
        self._ring.add_node(node)
        return ActionResult(success=True, message=f"Node '{node_id}' added")
    
    def _remove_node(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a node from the ring."""
        node_id = params.get("node_id", "")
        self._ring.remove_node(node_id)
        return ActionResult(success=True, message=f"Node '{node_id}' removed")
    
    def _get_node(self, params: Dict[str, Any]) -> ActionResult:
        """Get primary node for a key."""
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="key is required")
        
        node = self._ring.get_node(key)
        if not node:
            return ActionResult(success=False, message="No nodes in ring")
        
        return ActionResult(success=True, message=f"Node: {node.node_id}",
                         data={"node_id": node.node_id, "name": node.name})
    
    def _get_nodes(self, params: Dict[str, Any]) -> ActionResult:
        """Get multiple nodes for a key (replication)."""
        key = params.get("key", "")
        count = params.get("count", 1)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        nodes = self._ring.get_nodes_for_key(key, count)
        return ActionResult(success=True, message=f"Found {len(nodes)} nodes",
                         data={"nodes": [{"node_id": n.node_id, "name": n.name} for n in nodes]})
    
    def _list_nodes(self, params: Dict[str, Any]) -> ActionResult:
        """List all nodes."""
        nodes = self._ring.list_nodes()
        return ActionResult(success=True, message=f"{len(nodes)} nodes",
                         data={"nodes": [{"node_id": n.node_id, "name": n.name, "weight": n.weight} for n in nodes]})
    
    def _get_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get ring info."""
        info = self._ring.get_ring_info()
        return ActionResult(success=True, message="Ring info retrieved", data=info)
