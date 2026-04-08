"""
Element Path Builder.

Build reliable, stable paths to UI elements using various strategies
(AX path, XPath, role-title index, breadcrumbs) for element targeting
across tree changes.

Usage:
    from utils.element_path_builder import ElementPathBuilder, build_path

    builder = ElementPathBuilder(bridge)
    path = builder.build_path(target_element, strategy="ax_path")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass
from enum import Enum, auto

if TYPE_CHECKING:
    from utils.accessibility_bridge import AccessibilityBridge


class PathStrategy(Enum):
    """Strategy for building element paths."""
    AX_PATH = auto()
    ROLE_INDEX = auto()
    TITLE_BREADCRUMB = auto()
    STABLE_ID = auto()
    XPATH_LIKE = auto()
    BREADCRUMB = auto()


@dataclass
class ElementPath:
    """A path representation for targeting a UI element."""
    strategy: PathStrategy
    path_string: str
    confidence: float  # 0.0 to 1.0
    segments: List[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.segments is None:
            self.segments = []
        if self.metadata is None:
            self.metadata = {}

    def __repr__(self) -> str:
        return f"ElementPath({self.strategy.name}, confidence={self.confidence:.2f})"


class ElementPathBuilder:
    """
    Build reliable paths to UI elements.

    Different strategies provide different trade-offs between
    stability (surviving tree changes) and readability.

    Example:
        builder = ElementPathBuilder(bridge)
        path = builder.build_path(
            element,
            strategy=PathStrategy.ROLE_INDEX,
        )
        print(path.path_string)
    """

    def __init__(self, bridge: Optional["AccessibilityBridge"] = None) -> None:
        """
        Initialize the path builder.

        Args:
            bridge: Optional AccessibilityBridge for tree operations.
        """
        self._bridge = bridge

    def build_path(
        self,
        element: Dict[str, Any],
        strategy: PathStrategy = PathStrategy.ROLE_INDEX,
    ) -> ElementPath:
        """
        Build a path to the given element using the specified strategy.

        Args:
            element: Element dictionary to build path for.
            strategy: Path building strategy to use.

        Returns:
            ElementPath object with the path string and metadata.
        """
        if strategy == PathStrategy.AX_PATH:
            return self._build_ax_path(element)
        elif strategy == PathStrategy.ROLE_INDEX:
            return self._build_role_index_path(element)
        elif strategy == PathStrategy.TITLE_BREADCRUMB:
            return self._build_title_breadcrumb(element)
        elif strategy == PathStrategy.STABLE_ID:
            return self._build_stable_id(element)
        elif strategy == PathStrategy.XPATH_LIKE:
            return self._build_xpath_like(element)
        elif strategy == PathStrategy.BREADCRUMB:
            return self._build_breadcrumb(element)
        else:
            return self._build_role_index_path(element)

    def _build_ax_path(self, element: Dict[str, Any]) -> ElementPath:
        """Build AX-style path (e.g., /Window[0]/Group[0]/Button[0])."""
        segments = []
        current = element
        confidence = 1.0

        while current:
            role = current.get("role", "unknown")
            title = current.get("title", "")
            siblings = current.get("_siblings", [])
            index = siblings.index(current) + 1 if siblings and current in siblings else 1

            seg = f"/{role}[{index - 1}]"
            if title:
                seg += f"(@title={title!r})"
            segments.insert(0, seg)

            parent = current.get("_parent")
            if not parent:
                parent = current.get("parent")
            current = parent

        path_str = "".join(segments)
        return ElementPath(
            strategy=PathStrategy.AX_PATH,
            path_string=path_str,
            confidence=confidence,
            segments=segments,
        )

    def _build_role_index_path(self, element: Dict[str, Any]) -> ElementPath:
        """Build role+index path (e.g., Window[0] > Group[0] > Button[0])."""
        segments = []
        confidence = 1.0
        current = element

        while current:
            role = current.get("role", "unknown")
            title = current.get("title", "")
            siblings = current.get("_siblings", [])
            index = siblings.index(current) + 1 if siblings and current in siblings else 0

            seg = f"{role}[{index}]"
            if title:
                seg = f"{title} ({seg})"
            segments.insert(0, seg)

            parent = current.get("_parent") or current.get("parent")
            current = parent

        path_str = " > ".join(segments)
        return ElementPath(
            strategy=PathStrategy.ROLE_INDEX,
            path_string=path_str,
            confidence=confidence,
            segments=segments,
        )

    def _build_title_breadcrumb(self, element: Dict[str, Any]) -> ElementPath:
        """Build breadcrumb using titles."""
        segments = []
        confidence = 0.8
        current = element

        while current:
            title = current.get("title") or current.get("role", "")
            if title:
                segments.insert(0, str(title))
            parent = current.get("_parent") or current.get("parent")
            current = parent

        path_str = " > ".join(segments)
        return ElementPath(
            strategy=PathStrategy.TITLE_BREADCRUMB,
            path_string=path_str,
            confidence=confidence,
            segments=segments,
        )

    def _build_stable_id(self, element: Dict[str, Any]) -> ElementPath:
        """Build a stable ID using role+title hash."""
        role = element.get("role", "")
        title = element.get("title", "")
        identifier = element.get("identifier") or element.get("id")

        if identifier:
            stable_id = f"#{identifier}"
            confidence = 1.0
        elif role and title:
            stable_id = f"{role}:{title}"
            confidence = 0.9
        elif role:
            stable_id = f"{role}:{id(element)}"
            confidence = 0.5
        else:
            stable_id = f"element:{id(element)}"
            confidence = 0.3

        return ElementPath(
            strategy=PathStrategy.STABLE_ID,
            path_string=stable_id,
            confidence=confidence,
        )

    def _build_xpath_like(self, element: Dict[str, Any]) -> ElementPath:
        """Build XPath-like path using role and attributes."""
        segments = []
        confidence = 0.9
        current = element

        while current:
            role = current.get("role", "unknown")
            title = current.get("title")
            value = current.get("value")

            if title:
                seg = f'{role}[@title="{title}"]'
            elif value:
                seg = f'{role}[@value="{value}"]'
            else:
                seg = role

            segments.insert(0, seg)
            parent = current.get("_parent") or current.get("parent")
            current = parent

        path_str = "/".join(segments)
        return ElementPath(
            strategy=PathStrategy.XPATH_LIKE,
            path_string=path_str,
            confidence=confidence,
            segments=segments,
        )

    def _build_breadcrumb(self, element: Dict[str, Any]) -> ElementPath:
        """Build a simple breadcrumb of roles."""
        segments = []
        current = element

        while current:
            role = current.get("role", "?")
            segments.insert(0, role)
            parent = current.get("_parent") or current.get("parent")
            current = parent

        path_str = " > ".join(segments)
        return ElementPath(
            strategy=PathStrategy.BREADCRUMB,
            path_string=path_str,
            confidence=0.7,
            segments=segments,
        )

    def build_all(
        self,
        element: Dict[str, Any],
    ) -> Dict[PathStrategy, ElementPath]:
        """
        Build paths using all strategies.

        Args:
            element: Element to build paths for.

        Returns:
            Dictionary mapping strategy to ElementPath.
        """
        return {s: self.build_path(element, s) for s in PathStrategy}


def build_path(
    element: Dict[str, Any],
    strategy: PathStrategy = PathStrategy.ROLE_INDEX,
) -> ElementPath:
    """
    Convenience function to build an element path.

    Args:
        element: Element dictionary.
        strategy: Path building strategy.

    Returns:
        ElementPath object.
    """
    builder = ElementPathBuilder()
    return builder.build_path(element, strategy)
