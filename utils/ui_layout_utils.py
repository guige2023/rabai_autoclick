"""UI Layout utilities for layout analysis and optimization.

This module provides utilities for analyzing and working with UI layouts,
including alignment detection, spacing analysis, and layout hierarchy.
"""

from typing import List, Optional, Tuple, Dict, Any, Set, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict


class Alignment(Enum):
    """Horizontal and vertical alignment options."""
    START = auto()
    CENTER = auto()
    END = auto()
    STRETCH = auto()
    BASELINE = auto()


@dataclass
class LayoutConstraint:
    """Represents a layout constraint."""
    min_width: Optional[float] = None
    max_width: Optional[float] = None
    min_height: Optional[float] = None
    max_height: Optional[float] = None
    preferred_width: Optional[float] = None
    preferred_height: Optional[float] = None
    aspect_ratio: Optional[float] = None
    weight: float = 1.0


@dataclass
class LayoutBox:
    """Represents a rectangular layout box with position and size."""
    x: float
    y: float
    width: float
    height: float
    margin_left: float = 0.0
    margin_right: float = 0.0
    margin_top: float = 0.0
    margin_bottom: float = 0.0
    padding_left: float = 0.0
    padding_right: float = 0.0
    padding_top: float = 0.0
    padding_bottom: float = 0.0

    @property
    def left(self) -> float:
        return self.x - self.margin_left

    @property
    def right(self) -> float:
        return self.x + self.width + self.margin_right

    @property
    def top(self) -> float:
        return self.y - self.margin_top

    @property
    def bottom(self) -> float:
        return self.y + self.height + self.margin_bottom

    @property
    def content_left(self) -> float:
        return self.x + self.padding_left

    @property
    def content_right(self) -> float:
        return self.x + self.width - self.padding_right

    @property
    def content_top(self) -> float:
        return self.y + self.padding_top

    @property
    def content_bottom(self) -> float:
        return self.y + self.height - self.padding_bottom

    @property
    def content_width(self) -> float:
        return self.width - self.padding_left - self.padding_right

    @property
    def content_height(self) -> float:
        return self.height - self.padding_top - self.padding_bottom

    @property
    def center_x(self) -> float:
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        return self.y + self.height / 2

    @property
    def bounds(self) -> Tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)

    @property
    def content_bounds(self) -> Tuple[float, float, float, float]:
        return (self.content_left, self.content_top,
                self.content_width, self.content_height)

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is within this box."""
        return (self.left <= px < self.right and
                self.top <= py < self.bottom)

    def contains_box(self, other: 'LayoutBox') -> bool:
        """Check if another box is fully contained."""
        return (self.left <= other.left and
                self.right >= other.right and
                self.top <= other.top and
                self.bottom >= other.bottom)

    def intersects(self, other: 'LayoutBox') -> bool:
        """Check if this box intersects with another."""
        return not (self.right <= other.left or
                    self.left >= other.right or
                    self.bottom <= other.top or
                    self.top >= other.bottom)

    def distance_to(self, other: 'LayoutBox') -> float:
        """Calculate minimum distance to another box."""
        dx = max(self.left - other.right, 0, other.left - self.right)
        dy = max(self.top - other.bottom, 0, other.top - self.bottom)
        return (dx ** 2 + dy ** 2) ** 0.5

    def expand(self, dx: float, dy: float) -> 'LayoutBox':
        """Expand box by margins."""
        return LayoutBox(
            x=self.x, y=self.y, width=self.width, height=self.height,
            margin_left=self.margin_left + dx,
            margin_right=self.margin_right + dx,
            margin_top=self.margin_top + dy,
            margin_bottom=self.margin_bottom + dy
        )

    def shrink(self, dx: float, dy: float) -> 'LayoutBox':
        """Shrink box by margins."""
        return LayoutBox(
            x=self.x, y=self.y, width=self.width, height=self.height,
            margin_left=max(0, self.margin_left - dx),
            margin_right=max(0, self.margin_right - dx),
            margin_top=max(0, self.margin_top - dy),
            margin_bottom=max(0, self.margin_bottom - dy)
        )


@dataclass
class LayoutNode:
    """Represents a node in a layout hierarchy."""
    box: LayoutBox
    name: str = ""
    layout_type: str = "unknown"
    children: List['LayoutNode'] = field(default_factory=list)
    parent: Optional['LayoutNode'] = None
    constraints: LayoutConstraint = field(default_factory=LayoutConstraint)
    visible: bool = True
    enabled: bool = True

    def add_child(self, child: 'LayoutNode') -> None:
        """Add a child node."""
        child.parent = self
        self.children.append(child)

    def get_depth(self) -> int:
        """Get depth in layout tree."""
        depth = 0
        node = self.parent
        while node is not None:
            depth += 1
            node = node.parent
        return depth

    def get_all_descendants(self) -> List['LayoutNode']:
        """Get all descendant nodes."""
        descendants = []
        stack = list(self.children)
        while stack:
            node = stack.pop()
            descendants.append(node)
            stack.extend(node.children)
        return descendants

    def get_visible_descendants(self) -> List['LayoutNode']:
        """Get all visible descendant nodes."""
        return [n for n in self.get_all_descendants() if n.visible]


class LayoutAnalyzer:
    """Analyzes layout structures and relationships."""

    def __init__(self, root: Optional[LayoutNode] = None):
        self.root = root

    @classmethod
    def from_bounds_list(cls, bounds_list: List[Tuple[float, float, float, float]],
                         names: Optional[List[str]] = None) -> 'LayoutAnalyzer':
        """Build layout from list of bounds (x, y, width, height)."""
        root = LayoutNode(
            box=LayoutBox(x=0, y=0, width=1920, height=1080),
            name="root",
            layout_type="root"
        )
        names = names or [f"element_{i}" for i in range(len(bounds_list))]

        for i, (x, y, w, h) in enumerate(bounds_list):
            node = LayoutNode(
                box=LayoutBox(x=x, y=y, width=w, height=h),
                name=names[i] if i < len(names) else f"element_{i}",
                layout_type="element"
            )
            root.add_child(node)

        return cls(root)

    def find_aligned_elements(self, axis: str = "horizontal") -> List[Tuple[LayoutNode, LayoutNode]]:
        """Find pairs of elements that are aligned."""
        if self.root is None:
            return []

        aligned = []
        nodes = self.root.get_all_descendants()

        for i, node1 in enumerate(nodes):
            for node2 in nodes[i + 1:]:
                if axis == "horizontal":
                    if abs(node1.box.y - node2.box.y) < 5:
                        aligned.append((node1, node2))
                else:
                    if abs(node1.box.x - node2.box.x) < 5:
                        aligned.append((node1, node2))

        return aligned

    def find_distributed_elements(self, axis: str = "horizontal",
                                  tolerance: float = 2.0) -> List[List[LayoutNode]]:
        """Find groups of evenly distributed elements."""
        if self.root is None:
            return []

        nodes = self.root.get_all_descendants()
        if axis == "horizontal":
            nodes = sorted(nodes, key=lambda n: n.box.y)
        else:
            nodes = sorted(nodes, key=lambda n: n.box.x)

        groups: List[List[LayoutNode]] = []
        current_group = [nodes[0]] if nodes else []

        for i in range(1, len(nodes)):
            if axis == "horizontal":
                if abs(nodes[i].box.y - nodes[i - 1].box.y) < tolerance:
                    current_group.append(nodes[i])
                else:
                    if len(current_group) > 1:
                        groups.append(current_group)
                    current_group = [nodes[i]]
            else:
                if abs(nodes[i].box.x - nodes[i - 1].box.x) < tolerance:
                    current_group.append(nodes[i])
                else:
                    if len(current_group) > 1:
                        groups.append(current_group)
                    current_group = [nodes[i]]

        if len(current_group) > 1:
            groups.append(current_group)

        return groups

    def detect_gaps(self, axis: str = "horizontal",
                   min_gap: float = 5.0) -> List[Tuple[float, float]]:
        """Detect gaps between elements along an axis."""
        if self.root is None:
            return []

        gaps = []
        nodes = self.root.get_all_descendants()

        if axis == "horizontal":
            nodes = sorted(nodes, key=lambda n: n.box.y)
            for i in range(len(nodes) - 1):
                current = nodes[i]
                next_node = nodes[i + 1]
                if abs(current.box.y - next_node.box.y) < 20:
                    gap_start = current.box.right
                    gap_end = next_node.box.left
                    if gap_end - gap_start >= min_gap:
                        gaps.append((gap_start, gap_end))
        else:
            nodes = sorted(nodes, key=lambda n: n.box.x)
            for i in range(len(nodes) - 1):
                current = nodes[i]
                next_node = nodes[i + 1]
                if abs(current.box.x - next_node.box.x) < 20:
                    gap_start = current.box.bottom
                    gap_end = next_node.box.top
                    if gap_end - gap_start >= min_gap:
                        gaps.append((gap_start, gap_end))

        return gaps

    def find_spacing_violations(self, expected_spacing: float,
                                tolerance: float = 2.0) -> List[Tuple[LayoutNode, LayoutNode, float]]:
        """Find pairs with unexpected spacing."""
        if self.root is None:
            return []

        violations = []
        nodes = self.root.get_all_descendants()

        for i, node1 in enumerate(nodes):
            for node2 in nodes[i + 1:]:
                if node1.box.y == node2.box.y:
                    spacing = abs(node2.box.left - node1.box.right)
                    if abs(spacing - expected_spacing) > tolerance:
                        violations.append((node1, node2, spacing))
                elif node1.box.x == node2.box.x:
                    spacing = abs(node2.box.top - node1.box.bottom)
                    if abs(spacing - expected_spacing) > tolerance:
                        violations.append((node1, node2, spacing))

        return violations

    def get_layout_summary(self) -> Dict[str, Any]:
        """Get summary statistics of the layout."""
        if self.root is None:
            return {}

        nodes = self.root.get_all_descendants()
        visible_nodes = [n for n in nodes if n.visible]

        x_positions = [n.box.x for n in visible_nodes]
        y_positions = [n.box.y for n in visible_nodes]
        widths = [n.box.width for n in visible_nodes]
        heights = [n.box.height for n in visible_nodes]

        return {
            "total_nodes": len(nodes),
            "visible_nodes": len(visible_nodes),
            "depth": max((n.get_depth() for n in nodes), default=0),
            "bounds": {
                "min_x": min(x_positions) if x_positions else 0,
                "min_y": min(y_positions) if y_positions else 0,
                "max_x": max(x_positions) if x_positions else 0,
                "max_y": max(y_positions) if y_positions else 0,
            },
            "size_stats": {
                "avg_width": sum(widths) / len(widths) if widths else 0,
                "avg_height": sum(heights) / len(heights) if heights else 0,
                "min_width": min(widths) if widths else 0,
                "min_height": min(heights) if heights else 0,
            }
        }


def calculate_layout_score(boxes: List[LayoutBox],
                           grid_cols: int = 1,
                           grid_rows: int = 1) -> float:
    """Calculate a layout quality score based on alignment and spacing."""
    if not boxes:
        return 0.0

    score = 100.0

    sorted_by_y = sorted(boxes, key=lambda b: b.y)
    sorted_by_x = sorted(boxes, key=lambda b: b.x)

    y_groups = defaultdict(list)
    for box in sorted_by_y:
        y_groups[round(box.y / 20) * 20].append(box)

    for group in y_groups.values():
        if len(group) > 1:
            x_positions = [b.x for b in group]
            if max(x_positions) - min(x_positions) > 50:
                score -= 5

    x_groups = defaultdict(list)
    for box in sorted_by_x:
        x_groups[round(box.x / 20) * 20].append(box)

    for group in x_groups.values():
        if len(group) > 1:
            y_positions = [b.y for b in group]
            if max(y_positions) - min(y_positions) > 50:
                score -= 5

    widths = [b.width for b in boxes]
    if widths:
        width_variance = sum((w - sum(widths) / len(widths)) ** 2 for w in widths) / len(widths)
        if width_variance > 1000:
            score -= 10

    heights = [b.height for b in boxes]
    if heights:
        height_variance = sum((h - sum(heights) / len(heights)) ** 2 for h in heights) / len(heights)
        if height_variance > 1000:
            score -= 10

    return max(0.0, score)


def suggest_layout_improvements(boxes: List[LayoutBox]) -> List[str]:
    """Suggest improvements for a layout."""
    suggestions = []

    if not boxes:
        return suggestions

    sorted_by_x = sorted(boxes, key=lambda b: b.x)
    x_diffs = []
    for i in range(len(sorted_by_x) - 1):
        diff = sorted_by_x[i + 1].box.x - sorted_by_x[i].box.x
        if diff > 0:
            x_diffs.append(diff)

    if x_diffs:
        x_variance = sum((d - sum(x_diffs) / len(x_diffs)) ** 2 for d in x_diffs) / len(x_diffs)
        if x_variance > 100:
            suggestions.append("Consider using consistent horizontal spacing")

    sorted_by_y = sorted(boxes, key=lambda b: b.y)
    y_diffs = []
    for i in range(len(sorted_by_y) - 1):
        diff = sorted_by_y[i + 1].box.y - sorted_by_y[i].box.y
        if diff > 0:
            y_diffs.append(diff)

    if y_diffs:
        y_variance = sum((d - sum(y_diffs) / len(y_diffs)) ** 2 for d in y_diffs) / len(y_diffs)
        if y_variance > 100:
            suggestions.append("Consider using consistent vertical spacing")

    widths = [b.width for b in boxes]
    if widths and max(widths) - min(widths) > 100:
        suggestions.append("Consider standardizing element widths")

    heights = [b.height for b in boxes]
    if heights and max(heights) - min(heights) > 100:
        suggestions.append("Consider standardizing element heights")

    return suggestions
