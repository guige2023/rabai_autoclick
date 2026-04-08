"""Element Z-Index and Layer Management Utilities.

Utilities for managing UI element stacking order and z-index relationships.
Helps determine which elements are above/below others and resolve z-order conflicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class LayerType(Enum):
    """Types of UI layers."""

    BACKGROUND = auto()
    CONTENT = auto()
    POPUP = auto()
    MODAL = auto()
    TOOLTIP = auto()
    NOTIFICATION = auto()
    OVERLAY = auto()


@dataclass
class ZIndexInfo:
    """Z-index information for a UI element.

    Attributes:
        element_id: Unique identifier of the element.
        z_index: Numeric z-index value.
        layer_type: Categorization of the layer.
        parent_z_index: Parent element's z-index (for inherited ordering).
        is_fixed: Whether z-index is explicitly set.
        explicit_order: Relative order among siblings at same z-index.
    """

    element_id: str
    z_index: int
    layer_type: LayerType = LayerType.CONTENT
    parent_z_index: Optional[int] = None
    is_fixed: bool = False
    explicit_order: int = 0


@dataclass
class ZOrderRelation:
    """Describes the z-order relationship between two elements."""

    higher: ZIndexInfo
    lower: ZIndexInfo
    overlap_area: Optional[tuple[int, int, int, int]] = None


class ZIndexResolver:
    """Resolves z-index values and determines element ordering.

    Handles both explicit z-index values and implicit layer-based ordering.

    Example:
        resolver = ZIndexResolver()
        is_above = resolver.is_above(element_a, element_b)
        ordered = resolver.sort_by_zorder([elem1, elem2, elem3])
    """

    # Default z-index ranges for different layer types
    LAYER_RANGES: dict[LayerType, tuple[int, int]] = {
        LayerType.BACKGROUND: (-100, -1),
        LayerType.CONTENT: (0, 99),
        LayerType.POPUP: (100, 199),
        LayerType.MODAL: (200, 299),
        LayerType.TOOLTIP: (300, 399),
        LayerType.NOTIFICATION: (400, 499),
        LayerType.OVERLAY: (500, 999),
    }

    def __init__(self):
        """Initialize the z-index resolver."""
        self._cache: dict[str, ZIndexInfo] = {}

    def register(self, info: ZIndexInfo) -> None:
        """Register an element's z-index information.

        Args:
            info: ZIndexInfo for the element.
        """
        self._cache[info.element_id] = info

    def get_info(self, element_id: str) -> Optional[ZIndexInfo]:
        """Get registered z-index info for an element.

        Args:
            element_id: Element identifier.

        Returns:
            ZIndexInfo or None if not registered.
        """
        return self._cache.get(element_id)

    def is_above(self, higher_id: str, lower_id: str) -> bool:
        """Check if one element is above another in z-order.

        Args:
            higher_id: Element that should be higher.
            lower_id: Element that should be lower.

        Returns:
            True if higher_id is above lower_id.
        """
        higher = self._cache.get(higher_id)
        lower = self._cache.get(lower_id)
        if not higher or not lower:
            return False

        effective_higher = self._get_effective_zindex(higher)
        effective_lower = self._get_effective_zindex(lower)

        if effective_higher != effective_lower:
            return effective_higher > effective_lower

        return higher.explicit_order > lower.explicit_order

    def is_below(self, lower_id: str, higher_id: str) -> bool:
        """Check if one element is below another in z-order.

        Args:
            lower_id: Element that should be lower.
            higher_id: Element that should be higher.

        Returns:
            True if lower_id is below higher_id.
        """
        return self.is_above(higher_id, lower_id)

    def _get_effective_zindex(self, info: ZIndexInfo) -> int:
        """Calculate effective z-index including inheritance.

        Args:
            info: ZIndexInfo for the element.

        Returns:
            Effective z-index value.
        """
        if info.parent_z_index is not None:
            return info.parent_z_index + info.z_index
        return info.z_index

    def sort_by_zorder(self, element_ids: list[str]) -> list[str]:
        """Sort elements by their z-order from bottom to top.

        Args:
            element_ids: List of element identifiers.

        Returns:
            Sorted list from lowest to highest z-order.
        """
        elements = [(eid, self._cache.get(eid)) for eid in element_ids]
        valid = [(eid, info) for eid, info in elements if info is not None]

        def sort_key(item: tuple[str, ZIndexInfo]) -> tuple[int, int, int]:
            _, info = item
            effective = self._get_effective_zindex(info)
            return (effective, info.explicit_order, 0)

        valid.sort(key=sort_key)
        return [eid for eid, _ in valid]

    def get_layer_bounds(self, layer_type: LayerType) -> tuple[int, int]:
        """Get the z-index range for a layer type.

        Args:
            layer_type: The layer type.

        Returns:
            Tuple of (min_z, max_z) for the layer.
        """
        return self.LAYER_RANGES.get(layer_type, (0, 99))

    def assign_to_layer(
        self,
        element_id: str,
        layer_type: LayerType,
        offset: int = 0,
    ) -> int:
        """Assign an element to a layer with automatic z-index.

        Args:
            element_id: Element to assign.
            layer_type: Target layer type.
            offset: Offset within the layer.

        Returns:
            Assigned z-index value.
        """
        min_z, max_z = self.get_layer_bounds(layer_type)
        assigned = min_z + offset

        info = self._cache.get(element_id)
        if info:
            info.z_index = assigned
            info.layer_type = layer_type
            info.is_fixed = False

        return assigned


class ZOrderConflictResolver:
    """Resolves z-order conflicts between overlapping elements."""

    def __init__(self, resolver: Optional[ZIndexResolver] = None):
        """Initialize the conflict resolver.

        Args:
            resolver: ZIndexResolver to use for ordering.
        """
        self.resolver = resolver or ZIndexResolver()

    def resolve_overlap(
        self,
        element1_id: str,
        element2_id: str,
        prefer_higher: bool = True,
    ) -> str:
        """Determine which element should be on top in an overlap.

        Args:
            element1_id: First element in overlap.
            element2_id: Second element in overlap.
            prefer_higher: Whether to prefer the element that's already higher.

        Returns:
            Element ID that should be on top.
        """
        if self.resolver.is_above(element1_id, element2_id):
            return element1_id if prefer_higher else element2_id
        elif self.resolver.is_below(element1_id, element2_id):
            return element2_id if prefer_higher else element1_id
        else:
            return element1_id

    def bring_to_front(self, element_id: str, siblings: list[str]) -> int:
        """Move element to the front among its siblings.

        Args:
            element_id: Element to bring to front.
            siblings: All sibling element IDs.

        Returns:
            New explicit order value.
        """
        siblings_without = [s for s in siblings if s != element_id]
        sorted_siblings = self.resolver.sort_by_zorder(siblings_without)
        max_order = 0
        for sid in sorted_siblings:
            info = self.resolver.get_info(sid)
            if info:
                max_order = max(max_order, info.explicit_order)

        info = self.resolver.get_info(element_id)
        if info:
            info.explicit_order = max_order + 1
        return max_order + 1

    def send_to_back(self, element_id: str, siblings: list[str]) -> int:
        """Move element to the back among its siblings.

        Args:
            element_id: Element to send to back.
            siblings: All sibling element IDs.

        Returns:
            New explicit order value.
        """
        siblings_without = [s for s in siblings if s != element_id]
        sorted_siblings = self.resolver.sort_by_zorder(siblings_without)
        min_order = 0
        for sid in sorted_siblings:
            info = self.resolver.get_info(sid)
            if info:
                min_order = min(min_order, info.explicit_order)

        info = self.resolver.get_info(element_id)
        if info:
            info.explicit_order = min_order - 1
        return min_order - 1


@dataclass
class LayerStack:
    """Represents a stack of UI layers.

    Attributes:
        layers: Ordered list of layer types from bottom to top.
        elements: Mapping of layer type to element IDs in that layer.
    """

    layers: list[LayerType] = field(default_factory=list)
    elements: dict[LayerType, list[str]] = field(default_factory=dict)

    def add_layer(self, layer_type: LayerType) -> None:
        """Add a layer to the stack if not present.

        Args:
            layer_type: Layer type to add.
        """
        if layer_type not in self.layers:
            self.layers.append(layer_type)
            self.elements[layer_type] = []

    def add_to_layer(self, layer_type: LayerType, element_id: str) -> None:
        """Add an element to a specific layer.

        Args:
            layer_type: Target layer type.
            element_id: Element to add.
        """
        self.add_layer(layer_type)
        if element_id not in self.elements[layer_type]:
            self.elements[layer_type].append(element_id)

    def get_top_element(self, layer_type: LayerType) -> Optional[str]:
        """Get the topmost element in a layer.

        Args:
            layer_type: Layer to query.

        Returns:
            Top element ID or None.
        """
        elements = self.elements.get(layer_type, [])
        return elements[-1] if elements else None

    def get_bottom_element(self, layer_type: LayerType) -> Optional[str]:
        """Get the bottommost element in a layer.

        Args:
            layer_type: Layer to query.

        Returns:
            Bottom element ID or None.
        """
        elements = self.elements.get(layer_type, [])
        return elements[0] if elements else None
