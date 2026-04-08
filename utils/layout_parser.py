"""
Layout Parser Utility

Parses UI layout structures from accessibility trees or screenshots.
Generates hierarchical layout descriptions for automation targeting.

Example:
    >>> parser = LayoutParser()
    >>> tree = parser.parse_accessibility_tree(raw_xml)
    >>> print(parser.to_json(tree))
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum


class LayoutDirection(Enum):
    """Layout orientation."""
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"
    UNKNOWN = "unknown"


class LayoutAlignment(Enum):
    """Element alignment within a container."""
    START = "start"
    CENTER = "center"
    END = "end"
    STRETCH = "stretch"


@dataclass
class LayoutElement:
    """
    Represents a single UI element in a layout tree.

    Attributes:
        role: Element role (button, textfield, etc.).
        name: Accessible name of the element.
        value: Current value if applicable.
        bounds: (x, y, width, height) tuple.
        children: Child elements.
        parent: Parent element reference.
        index: Index among siblings.
    """
    role: str
    name: Optional[str] = None
    value: Optional[str] = None
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)
    children: list[LayoutElement] = field(default_factory=list)
    parent: Optional[LayoutElement] = None
    index: int = 0
    enabled: bool = True
    focused: bool = False
    selected: bool = False
    expanded: bool = True
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def x(self) -> int:
        return self.bounds[0]

    @property
    def y(self) -> int:
        return self.bounds[1]

    @property
    def width(self) -> int:
        return self.bounds[2]

    @property
    def height(self) -> int:
        return self.bounds[3]

    @property
    def center_x(self) -> int:
        return self.x + self.width // 2

    @property
    def center_y(self) -> int:
        return self.y + self.height // 2

    @property
    def is_visible(self) -> bool:
        return self.width > 0 and self.height > 0


@dataclass
class LayoutContainer:
    """
    A container element with explicit layout direction and alignment.
    """
    element: LayoutElement
    direction: LayoutDirection = LayoutDirection.UNKNOWN
    alignment: LayoutAlignment = LayoutAlignment.START
    spacing: int = 0
    padding: tuple[int, int, int, int] = (0, 0, 0, 0)
    line_wrap: bool = False


class LayoutParser:
    """
    Parses UI layouts from various sources.

    Supports:
        - Accessibility tree XML
        - Dictionary representations
        - Custom element trees
    """

    def __init__(self) -> None:
        self._element_cache: dict[str, LayoutElement] = {}

    def parse_accessibility_tree(self, xml_string: str) -> LayoutElement:
        """
        Parse an accessibility tree from XML string.

        Args:
            xml_string: Raw accessibility tree XML.

        Returns:
            Root LayoutElement of the parsed tree.
        """
        import xml.etree.ElementTree as ET

        root = LayoutElement(role="root")
        try:
            tree = ET.fromstring(xml_string)
            root = self._parse_node(tree)
        except ET.ParseError:
            pass
        return root

    def _parse_node(self, node: Any) -> LayoutElement:
        """Recursively parse an XML node into LayoutElement."""
        role = node.attrib.get("role", "unknown")
        name = node.attrib.get("name")
        value = node.attrib.get("value")

        bounds_str = node.attrib.get("bounds", "0,0,0,0")
        try:
            parts = [int(x) for x in bounds_str.split(",")]
            bounds = tuple(parts[:4]) if len(parts) >= 4 else (0, 0, 0, 0)
        except ValueError:
            bounds = (0, 0, 0, 0)

        element = LayoutElement(
            role=role,
            name=name,
            value=value,
            bounds=bounds,
            enabled=node.attrib.get("enabled", "true").lower() == "true",
            focused=node.attrib.get("focused", "false").lower() == "true",
        )

        for attr, val in node.attrib.items():
            if attr not in ("role", "name", "value", "bounds", "enabled", "focused"):
                element.attributes[attr] = val

        for i, child_node in enumerate(node):
            child = self._parse_node(child_node)
            child.parent = element
            child.index = i
            element.children.append(child)

        return element

    def parse_dict(self, data: dict[str, Any]) -> LayoutElement:
        """
        Parse layout from dictionary representation.

        Args:
            data: Dict with keys: role, name, value, bounds, children.

        Returns:
            Root LayoutElement.
        """
        element = LayoutElement(
            role=data.get("role", "unknown"),
            name=data.get("name"),
            value=data.get("value"),
            bounds=tuple(data.get("bounds", [0, 0, 0, 0])),
            enabled=data.get("enabled", True),
        )
        for i, child_data in enumerate(data.get("children", [])):
            child = self.parse_dict(child_data)
            child.parent = element
            child.index = i
            element.children.append(child)
        return element

    def find_by_role(self, root: LayoutElement, role: str) -> list[LayoutElement]:
        """Find all elements with a given role."""
        results: list[LayoutElement] = []
        if root.role == role:
            results.append(root)
        for child in root.children:
            results.extend(self.find_by_role(child, role))
        return results

    def find_by_name(self, root: LayoutElement, name: str) -> list[LayoutElement]:
        """Find all elements whose name contains the given string."""
        results: list[LayoutElement] = []
        if root.name and name.lower() in root.name.lower():
            results.append(root)
        for child in root.children:
            results.extend(self.find_by_name(child, name))
        return results

    def to_json(self, root: LayoutElement) -> dict[str, Any]:
        """Convert layout tree to JSON-serializable dict."""
        return {
            "role": root.role,
            "name": root.name,
            "value": root.value,
            "bounds": list(root.bounds),
            "enabled": root.enabled,
            "focused": root.focused,
            "children": [self.to_json(c) for c in root.children],
        }

    def to_simple_list(self, root: LayoutElement) -> list[dict[str, Any]]:
        """Flatten layout tree to a list of element descriptors."""
        result: list[dict[str, Any]] = []
        result.append({
            "role": root.role,
            "name": root.name,
            "bounds": list(root.bounds),
            "depth": self._get_depth(root),
        })
        for child in root.children:
            result.extend(self.to_simple_list(child))
        return result

    def _get_depth(self, element: LayoutElement) -> int:
        """Get depth of element in tree."""
        depth = 0
        current = element.parent
        while current:
            depth += 1
            current = current.parent
        return depth

    def infer_direction(self, container: LayoutElement) -> LayoutDirection:
        """
        Infer layout direction from children arrangement.

        Args:
            container: Container element with children.

        Returns:
            HORIZONTAL if children are side-by-side,
            VERTICAL if stacked, UNKNOWN otherwise.
        """
        if len(container.children) < 2:
            return LayoutDirection.UNKNOWN

        children = container.children[:4]
        h_count = sum(
            1 for i in range(len(children) - 1)
            if abs(children[i].center_y - children[i + 1].center_y) < 10
        )
        v_count = sum(
            1 for i in range(len(children) - 1)
            if abs(children[i].center_x - children[i + 1].center_x) < 10
        )

        if h_count > v_count:
            return LayoutDirection.HORIZONTAL
        elif v_count > h_count:
            return LayoutDirection.VERTICAL
        return LayoutDirection.UNKNOWN
