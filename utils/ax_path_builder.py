"""
AX Path Builder.

Build macOS Accessibility API (AXAPI) paths for element identification
and targeting using the same path format used by AX inspector tools.

Usage:
    from utils.ax_path_builder import AXPathBuilder, build_ax_path

    builder = AXPathBuilder()
    path = builder.build_path(element)
    print(path)  # e.g., "/Window[0]/Group[0]/Button[0]"
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class AXPath:
    """Represents a built AX path."""
    path_string: str
    segments: List["AXPathSegment"]
    element: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        return self.path_string


@dataclass
class AXPathSegment:
    """A single segment in an AX path."""
    role: str
    index: int
    title: Optional[str] = None
    identifier: Optional[str] = None
    role_description: Optional[str] = None

    def __str__(self) -> str:
        parts = [self.role, f"[{self.index}]"]
        if self.title:
            parts.append(f"(@title={self.title!r})")
        if self.identifier:
            parts.append(f"(@id={self.identifier!r})")
        return "".join(parts)


class AXPathBuilder:
    """
    Build AX (Accessibility) paths for UI elements.

    AX paths follow the format:
        /Window[0]/Group[0]/Button[0](@title="OK")

    Example:
        builder = AXPathBuilder()
        path = builder.build_path(element)
        print(path.path_string)
    """

    def __init__(
        self,
        include_title: bool = True,
        include_id: bool = True,
    ) -> None:
        """
        Initialize the path builder.

        Args:
            include_title: Include title/description in path segments.
            include_id: Include identifier in path segments.
        """
        self._include_title = include_title
        self._include_id = include_id

    def build_path(
        self,
        element: Dict[str, Any],
        root: Optional[Dict[str, Any]] = None,
    ) -> AXPath:
        """
        Build an AX path for an element.

        Args:
            element: Element dictionary to build path for.
            root: Optional root element to build relative path from.

        Returns:
            AXPath object.
        """
        segments: List[AXPathSegment] = []

        if root:
            segments = self._build_relative_path(element, root)
        else:
            segments = self._build_absolute_path(element)

        path_str = "".join(str(seg) for seg in segments)
        return AXPath(path_string=path_str, segments=segments, element=element)

    def _build_absolute_path(
        self,
        element: Dict[str, Any],
    ) -> List[AXPathSegment]:
        """Build path from root to element."""
        segments: List[AXPathSegment] = []
        current: Optional[Dict[str, Any]] = element

        while current:
            seg = self._element_to_segment(current, segments)
            if seg:
                segments.insert(0, seg)
            parent = current.get("parent") or current.get("_parent")
            current = parent

        return segments

    def _build_relative_path(
        self,
        element: Dict[str, Any],
        root: Dict[str, Any],
    ) -> List[AXPathSegment]:
        """Build path from root to element (relative)."""
        ancestors = self._get_ancestors(element)
        root_ancestors = self._get_ancestors(root)

        common_count = 0
        for i, ancestor in enumerate(ancestors):
            if i < len(root_ancestors) and ancestors[i] == root_ancestors[i]:
                common_count += 1
            else:
                break

        segments: List[AXPathSegment] = []
        for a in ancestors[common_count:]:
            seg = self._element_to_segment(a, segments)
            if seg:
                segments.append(seg)

        return segments

    def _get_ancestors(
        self,
        element: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Get list of ancestors from root to element."""
        ancestors: List[Dict[str, Any]] = []
        current: Optional[Dict[str, Any]] = element

        while current:
            ancestors.insert(0, current)
            parent = current.get("parent") or current.get("_parent")
            current = parent

        return ancestors

    def _element_to_segment(
        self,
        element: Dict[str, Any],
        previous_segments: List[AXPathSegment],
    ) -> Optional[AXPathSegment]:
        """Convert an element to a path segment."""
        role = element.get("role", "unknown")
        if not role:
            return None

        siblings = self._get_siblings(element)
        index = 0
        for i, sib in enumerate(siblings):
            if sib == element:
                index = i
                break
        elif element in siblings:
            index = siblings.index(element)

        title = None
        if self._include_title:
            title = element.get("title") or element.get("description")

        identifier = None
        if self._include_id:
            identifier = element.get("identifier") or element.get("id")

        return AXPathSegment(
            role=role,
            index=index,
            title=title,
            identifier=identifier,
            role_description=element.get("role_description"),
        )

    def _get_siblings(
        self,
        element: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Get siblings of an element."""
        parent = element.get("parent") or element.get("_parent")
        if parent is None:
            return [element]
        return parent.get("children", [element])

    def parse_path(
        self,
        path_string: str,
    ) -> List[AXPathSegment]:
        """
        Parse an AX path string back into segments.

        Args:
            path_string: AX path string like "/Window[0]/Button[0]".

        Returns:
            List of AXPathSegment objects.
        """
        import re

        segments = []
        parts = path_string.strip("/").split("/")

        for part in parts:
            match = re.match(r"([A-Za-z_]+)\[(\d+)\](?:@(\w+)=(.+))?", part)
            if match:
                role, idx_str, attr_name, attr_val = match.groups()
                segments.append(AXPathSegment(
                    role=role,
                    index=int(idx_str),
                    identifier=attr_val if attr_name == "id" else None,
                    title=attr_val if attr_name == "title" else None,
                ))

        return segments

    def match_path(
        self,
        element: Dict[str, Any],
        path: AXPath,
    ) -> bool:
        """
        Check if an element matches a path.

        Args:
            element: Element to check.
            path: AXPath to match against.

        Returns:
            True if the element matches the path.
        """
        built = self.build_path(element)
        return built.path_string == path.path_string


def build_ax_path(
    element: Dict[str, Any],
    include_title: bool = True,
) -> str:
    """
    Convenience function to build an AX path string.

    Args:
        element: Element dictionary.
        include_title: Include title in path.

    Returns:
        AX path string.
    """
    builder = AXPathBuilder(include_title=include_title)
    return builder.build_path(element).path_string


def parse_ax_path(path_string: str) -> List[AXPathSegment]:
    """
    Parse an AX path string into segments.

    Args:
        path_string: AX path string.

    Returns:
        List of AXPathSegment objects.
    """
    builder = AXPathBuilder()
    return builder.parse_path(path_string)
