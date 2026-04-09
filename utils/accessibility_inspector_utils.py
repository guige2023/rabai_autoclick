"""
Accessibility inspector utilities for UI debugging.

This module provides utilities for inspecting and debugging
accessibility elements during automation development.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum, auto


class InspectionLevel(Enum):
    """Level of detail for inspections."""
    BASIC = auto()
    STANDARD = auto()
    VERBOSE = auto()


@dataclass
class InspectionResult:
    """
    Result of an element inspection.

    Attributes:
        element_id: Element identifier.
        path: Path to element in tree.
        role: Element role.
        label: Accessibility label.
        title: Element title.
        value: Element value.
        enabled: Whether element is enabled.
        focused: Whether element has focus.
        bounds: Element bounds.
        children: Number of children.
        attributes: Additional attributes.
        issues: List of accessibility issues found.
    """
    element_id: str
    path: str
    role: str = ""
    label: str = ""
    title: str = ""
    value: str = ""
    enabled: bool = True
    focused: bool = False
    bounds: Optional[tuple] = None
    children: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)

    def add_issue(self, issue: str) -> None:
        """Add an accessibility issue."""
        self.issues.append(issue)

    @property
    def has_issues(self) -> bool:
        """Check if any issues were found."""
        return len(self.issues) > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "elementId": self.element_id,
            "path": self.path,
            "role": self.role,
            "label": self.label,
            "title": self.title,
            "value": self.value,
            "enabled": self.enabled,
            "focused": self.focused,
            "bounds": self.bounds,
            "children": self.children,
            "attributes": self.attributes,
            "issues": self.issues,
        }


class AccessibilityInspector:
    """
    Inspects accessibility elements and reports issues.

    Provides debugging utilities for identifying accessibility
    problems and element property inspection.
    """

    def __init__(self) -> None:
        self._rules: List[Callable[[Dict[str, Any]], Optional[str]]] = []
        self._inspection_level = InspectionLevel.STANDARD
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Set up default accessibility inspection rules."""

        def check_missing_label(element: Dict[str, Any]) -> Optional[str]:
            """Check for missing accessibility label on interactive elements."""
            role = element.get("role", "")
            label = element.get("label", "")
            title = element.get("title", "")

            interactive_roles = {"AXButton", "AXTextField", "AXCheckBox",
                                  "AXRadioButton", "AXLink"}

            if role in interactive_roles and not label and not title:
                return f"Interactive element '{role}' has no label"

            return None

        def check_disabled_focusable(element: Dict[str, Any]) -> Optional[str]:
            """Check for disabled but focusable elements."""
            enabled = element.get("enabled", True)
            focused = element.get("focused", False)
            role = element.get("role", "")

            if not enabled and focused:
                return f"Disabled element '{role}' has focus"

            return None

        def check_small_target(element: Dict[str, Any]) -> Optional[str]:
            """Check for potentially too-small touch targets."""
            bounds = element.get("bounds")
            if bounds and len(bounds) >= 4:
                width, height = bounds[2], bounds[3]
                min_size = 44  # Apple HIG minimum

                if width < min_size or height < min_size:
                    return f"Small target: {width}x{height} (minimum: {min_size})"

            return None

        def check_missing_role(element: Dict[str, Any]) -> Optional[str]:
            """Check for elements without a role."""
            role = element.get("role", "")
            if not role:
                return "Element has no accessibility role"

            return None

        self._rules.extend([
            check_missing_label,
            check_disabled_focusable,
            check_small_target,
            check_missing_role,
        ])

    def set_inspection_level(self, level: InspectionLevel) -> AccessibilityInspector:
        """Set inspection detail level."""
        self._inspection_level = level
        return self

    def add_rule(self, rule: Callable[[Dict[str, Any]], Optional[str]]) -> AccessibilityInspector:
        """Add a custom inspection rule."""
        self._rules.append(rule)
        return self

    def inspect(self, element: Dict[str, Any]) -> InspectionResult:
        """
        Inspect an element and return detailed results.

        Runs all inspection rules against the element.
        """
        role = element.get("role", "") or element.get("AXRole", "")
        label = element.get("label", "") or element.get("AXLabel", "")
        title = element.get("title", "") or element.get("AXTitle", "")
        value = element.get("value", "") or element.get("AXValue", "")

        result = InspectionResult(
            element_id=element.get("elementId", element.get("identifier", "")),
            path=element.get("path", ""),
            role=role,
            label=label,
            title=title,
            value=value,
            enabled=element.get("enabled", True),
            focused=element.get("focused", False),
            bounds=element.get("bounds"),
            children=len(element.get("children", [])),
        )

        # Run inspection rules
        for rule in self._rules:
            issue = rule(element)
            if issue:
                result.add_issue(issue)

        # Add verbose attributes
        if self._inspection_level == InspectionLevel.VERBOSE:
            result.attributes = {k: v for k, v in element.items()
                                if k not in ("children", "bounds", "role")}

        return result

    def inspect_tree(
        self,
        elements: List[Dict[str, Any]],
        max_depth: int = 10,
    ) -> List[InspectionResult]:
        """Inspect all elements in a tree."""
        results: List[InspectionResult] = []
        self._inspect_recursive(elements, "", results, max_depth)
        return results

    def _inspect_recursive(
        self,
        elements: List[Dict[str, Any]],
        parent_path: str,
        results: List[InspectionResult],
        depth: int,
    ) -> None:
        """Recursively inspect elements."""
        if depth <= 0:
            return

        for element in elements:
            element_id = element.get("elementId", element.get("identifier", ""))
            current_path = f"{parent_path}/{element_id}" if parent_path else element_id

            result = self.inspect(element)
            result.path = current_path
            results.append(result)

            children = element.get("children", [])
            if children:
                self._inspect_recursive(children, current_path, results, depth - 1)


class AccessibilityTreeDumper:
    """
    Dumps accessibility tree in various formats.

    Useful for debugging and logging accessibility structures.
    """

    def __init__(self) -> None:
        self._max_value_length = 50

    def dump_tree(
        self,
        elements: List[Dict[str, Any]],
        level: int = 0,
        max_depth: int = 20,
    ) -> str:
        """Dump accessibility tree as formatted string."""
        lines: List[str] = []
        self._dump_recursive(elements, lines, level, max_depth)
        return "\n".join(lines)

    def _dump_recursive(
        self,
        elements: List[Dict[str, Any]],
        lines: List[str],
        level: int,
        max_depth: int,
    ) -> None:
        """Recursively dump elements."""
        if level > max_depth:
            return

        indent = "  " * level

        for element in elements:
            role = element.get("role", "Unknown")
            label = element.get("label", "")
            title = element.get("title", "")

            display = f"{role}"
            if label:
                display += f' label="{self._truncate(label)}"'
            elif title:
                display += f' title="{self._truncate(title)}"'

            lines.append(f"{indent}{display}")

            children = element.get("children", [])
            if children:
                self._dump_recursive(children, lines, level + 1, max_depth)

    def _truncate(self, text: str, max_len: Optional[int] = None) -> str:
        """Truncate text for display."""
        if max_len is None:
            max_len = self._max_value_length
        if len(text) <= max_len:
            return text
        return text[:max_len - 3] + "..."


def format_inspection_result(result: InspectionResult) -> str:
    """Format inspection result as readable string."""
    lines = [
        f"Element: {result.element_id}",
        f"Path: {result.path}",
        f"Role: {result.role}",
    ]

    if result.label:
        lines.append(f"Label: {result.label}")
    if result.title:
        lines.append(f"Title: {result.title}")
    if result.value:
        lines.append(f"Value: {result.value}")

    lines.append(f"Enabled: {result.enabled}")
    lines.append(f"Focused: {result.focused}")

    if result.bounds:
        lines.append(f"Bounds: {result.bounds}")

    if result.has_issues:
        lines.append("Issues:")
        for issue in result.issues:
            lines.append(f"  - {issue}")

    return "\n".join(lines)
