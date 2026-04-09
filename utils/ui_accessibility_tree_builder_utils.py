"""UI Accessibility Tree Builder Utilities.

Builds accessibility trees from UI element hierarchies.

Example:
    >>> from ui_accessibility_tree_builder_utils import A11yTreeBuilder
    >>> builder = A11yTreeBuilder()
    >>> tree = builder.build(element)
    >>> print(tree.to_string())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class A11yNode:
    """An accessibility tree node."""
    role: str
    name: str
    value: str = ""
    enabled: bool = True
    focused: bool = False
    children: List[A11yNode] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_string(self, indent: int = 0) -> str:
        """Convert tree to string representation."""
        prefix = "  " * indent
        lines = [f"{prefix}[{self.role}] {self.name!r}"]
        if self.value:
            lines.append(f"{prefix}  value={self.value!r}")
        if not self.enabled:
            lines.append(f"{prefix}  disabled")
        if self.focused:
            lines.append(f"{prefix}  focused")
        for child in self.children:
            lines.append(child.to_string(indent + 1))
        return "\n".join(lines)

    def find_by_role(self, role: str) -> List[A11yNode]:
        """Find all nodes with given role."""
        results = []
        if self.role == role:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_role(role))
        return results

    def find_by_name(self, name: str, exact: bool = False) -> List[A11yNode]:
        """Find nodes by name."""
        results = []
        match = (self.name == name) if exact else (name.lower() in self.name.lower())
        if match:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_name(name, exact))
        return results


class A11yTreeBuilder:
    """Builds accessibility trees from UI elements."""

    def __init__(self):
        """Initialize tree builder."""
        self._element_extractor: Optional[Callable[..., Dict[str, Any]]] = None

    def set_extractor(self, extractor: Callable[..., Dict[str, Any]]) -> None:
        """Set custom element property extractor.

        Args:
            extractor: Function that extracts properties from an element.
        """
        self._element_extractor = extractor

    def build(self, root: Any) -> A11yNode:
        """Build accessibility tree from root element.

        Args:
            root: Root UI element.

        Returns:
            A11yNode tree root.
        """
        if self._element_extractor:
            props = self._element_extractor(root)
            return self._node_from_props(props)
        return self._default_build(root)

    def _default_build(self, element: Any) -> A11yNode:
        """Default tree building logic."""
        role = getattr(element, "role", "unknown")
        name = getattr(element, "name", "")
        value = getattr(element, "value", "")
        enabled = getattr(element, "enabled", True)
        focused = getattr(element, "focused", False)
        children_elems = getattr(element, "children", [])

        children = [self._default_build(c) for c in children_elems]
        return A11yNode(
            role=role,
            name=name,
            value=value,
            enabled=enabled,
            focused=focused,
            children=children,
        )

    def _node_from_props(self, props: Dict[str, Any]) -> A11yNode:
        """Build node from properties dictionary."""
        children_data = props.pop("children", [])
        node = A11yNode(
            role=props.get("role", "unknown"),
            name=props.get("name", ""),
            value=props.get("value", ""),
            enabled=props.get("enabled", True),
            focused=props.get("focused", False),
            properties={k: v for k, v in props.items() if k not in ("role", "name", "value", "enabled", "focused")},
        )
        node.children = [self._node_from_props(c) if isinstance(c, dict) else self._default_build(c) for c in children_data]
        return node
