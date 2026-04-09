"""
Hierarchy Serialization Utilities

Serialize and deserialize accessibility hierarchy trees for caching,
snapshot comparison, and state persistence.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional, List


@dataclass
class SerializedNode:
    """A serialized accessibility tree node."""
    role: str
    label: str
    value: str
    children: List["SerializedNode"] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)
    bounds: Optional[tuple] = None

    def to_dict(self) -> dict:
        """Convert to a dictionary representation."""
        return {
            "role": self.role,
            "label": self.label,
            "value": self.value,
            "attributes": self.attributes,
            "bounds": self.bounds,
            "children": [c.to_dict() for c in self.children],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SerializedNode":
        """Reconstruct from a dictionary."""
        return cls(
            role=data.get("role", ""),
            label=data.get("label", ""),
            value=data.get("value", ""),
            attributes=data.get("attributes", {}),
            bounds=data.get("bounds"),
            children=[cls.from_dict(c) for c in data.get("children", [])],
        )


def serialize_tree(root: SerializedNode) -> str:
    """Serialize an accessibility tree to JSON string."""
    return json.dumps(root.to_dict(), ensure_ascii=False)


def deserialize_tree(json_str: str) -> SerializedNode:
    """Deserialize a JSON string back to a SerializedNode tree."""
    data = json.loads(json_str)
    return SerializedNode.from_dict(data)


def compute_tree_hash(root: SerializedNode) -> str:
    """Compute a deterministic hash of a serialized tree for caching."""
    serialized = json.dumps(root.to_dict(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def filter_tree_by_role(root: SerializedNode, allowed_roles: set[str]) -> SerializedNode:
    """Create a filtered copy of the tree containing only specific roles."""
    if root.role in allowed_roles:
        return SerializedNode(
            role=root.role,
            label=root.label,
            value=root.value,
            attributes=root.attributes,
            bounds=root.bounds,
            children=[filter_tree_by_role(c, allowed_roles) for c in root.children],
        )
    # Role not in allowed set, return only matching children
    matching_children = [filter_tree_by_role(c, allowed_roles) for c in root.children]
    matching_children = [c for c in matching_children if c]
    if matching_children:
        # Return first matching child's subtree (merged)
        return matching_children[0]
    return SerializedNode(role="", label="", value="")


def get_tree_depth(root: SerializedNode) -> int:
    """Compute the maximum depth of a serialized tree."""
    if not root.children or not root.role:
        return 0
    return 1 + max(get_tree_depth(c) for c in root.children)
