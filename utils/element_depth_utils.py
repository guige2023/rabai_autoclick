"""
Element depth utilities for z-order and layering management.

Provides depth ordering, layering, and stacking context for UI elements.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LayerInfo:
    """Information about a UI layer."""
    layer_id: str
    name: str
    depth: int
    is_visible: bool = True
    opacity: float = 1.0
    parent_layer: Optional[str] = None


@dataclass
class DepthNode:
    """Node in a depth hierarchy."""
    element_id: str
    depth: int
    layer_id: str = "default"
    parent_id: Optional[str] = None
    children: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class DepthManager:
    """Manages element depth ordering and layering."""

    def __init__(self):
        self._layers: dict[str, LayerInfo] = {
            "default": LayerInfo(layer_id="default", name="Default", depth=0)
        }
        self._elements: dict[str, DepthNode] = {}
        self._max_depth: int = 0

    def add_layer(self, layer_id: str, name: str, depth: int, parent: Optional[str] = None) -> None:
        """Add a new layer."""
        self._layers[layer_id] = LayerInfo(
            layer_id=layer_id,
            name=name,
            depth=depth,
            parent_layer=parent,
        )

    def register_element(
        self,
        element_id: str,
        layer_id: str = "default",
        parent_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Register an element with depth information."""
        layer = self._layers.get(layer_id)
        layer_depth = layer.depth if layer else 0

        # Compute cumulative depth
        depth = layer_depth
        if parent_id and parent_id in self._elements:
            depth += self._elements[parent_id].depth + 1

        node = DepthNode(
            element_id=element_id,
            depth=depth,
            layer_id=layer_id,
            parent_id=parent_id,
            metadata=metadata or {},
        )
        self._elements[element_id] = node
        self._max_depth = max(self._max_depth, depth)

        # Update parent's children
        if parent_id and parent_id in self._elements:
            self._elements[parent_id].children.append(element_id)

    def get_depth(self, element_id: str) -> int:
        """Get the depth of an element."""
        node = self._elements.get(element_id)
        return node.depth if node else 0

    def is_above(self, element_a: str, element_b: str) -> bool:
        """Check if element_a is visually above element_b."""
        depth_a = self.get_depth(element_a)
        depth_b = self.get_depth(element_b)
        return depth_a > depth_b

    def is_below(self, element_a: str, element_b: str) -> bool:
        """Check if element_a is visually below element_b."""
        return not self.is_above(element_a, element_b)

    def get_elements_in_layer(self, layer_id: str) -> list[str]:
        """Get all elements in a layer."""
        return [
            e.element_id for e in self._elements.values()
            if e.layer_id == layer_id
        ]

    def sort_by_depth(self, element_ids: list[str]) -> list[str]:
        """Sort elements by depth (back to front)."""
        return sorted(
            element_ids,
            key=lambda e: self.get_depth(e),
            reverse=True,
        )

    def get_ancestors(self, element_id: str) -> list[str]:
        """Get ancestor elements from parent to root."""
        ancestors = []
        current = element_id
        visited = set()
        while current:
            if current in visited:
                break
            visited.add(current)
            node = self._elements.get(current)
            if not node or not node.parent_id:
                break
            ancestors.append(node.parent_id)
            current = node.parent_id
        return ancestors

    def get_descendants(self, element_id: str) -> list[str]:
        """Get all descendant elements."""
        descendants = []
        stack = [element_id]
        visited = set()
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            node = self._elements.get(current)
            if not node:
                continue
            for child in node.children:
                descendants.append(child)
                stack.append(child)
        return descendants

    def get_layer_at_depth(self, depth: int) -> Optional[str]:
        """Get the layer that contains elements at a given depth range."""
        for layer in sorted(self._layers.values(), key=lambda l: l.depth, reverse=True):
            if depth >= layer.depth:
                return layer.layer_id
        return "default"


__all__ = ["DepthManager", "DepthNode", "LayerInfo"]
