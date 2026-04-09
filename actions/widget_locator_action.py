"""
Widget Locator Action Module

Locates UI widgets using multiple strategies including
accessibility tree, visual recognition, and spatial queries.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LocatorStrategy(Enum):
    """Widget locator strategies."""

    ID = "id"
    ACCESSIBILITY = "accessibility"
    TEXT = "text"
    IMAGE = "image"
    POSITION = "position"
    HIERARCHY = "hierarchy"


@dataclass
class Widget:
    """Represents a UI widget."""

    id: str
    type: str
    name: str
    bounds: Tuple[int, int, int, int]
    visible: bool = True
    enabled: bool = True
    parent: Optional["Widget"] = None
    children: List["Widget"] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


class WidgetLocator:
    """
    Locates UI widgets using various strategies.

    Supports ID-based, accessibility-based, text-based,
    and position-based widget location.
    """

    def __init__(
        self,
        widget_tree: Optional[Any] = None,
    ):
        self._widget_tree: Optional[Widget] = None
        self._locators: Dict[LocatorStrategy, Callable] = {}
        self._cache: Dict[str, Widget] = {}

    def set_widget_tree(self, root: Widget) -> None:
        """Set the widget tree root."""
        self._widget_tree = root
        self._cache.clear()

    def find_by_id(self, widget_id: str) -> Optional[Widget]:
        """Find widget by ID."""
        if widget_id in self._cache:
            return self._cache[widget_id]

        if self._widget_tree:
            result = self._search_tree(self._widget_tree, lambda w: w.id == widget_id)
            if result:
                self._cache[widget_id] = result
            return result
        return None

    def find_by_type(self, widget_type: str) -> List[Widget]:
        """Find all widgets of a type."""
        if self._widget_tree:
            results: List[Widget] = []
            self._collect_matching(self._widget_tree, lambda w: w.type == widget_type, results)
            return results
        return []

    def find_by_text(self, text: str, exact: bool = False) -> List[Widget]:
        """Find widgets by text content."""
        if self._widget_tree:
            results: List[Widget] = []
            matcher = lambda w: (w.name == text) if exact else (text.lower() in w.name.lower())
            self._collect_matching(self._widget_tree, matcher, results)
            return results
        return []

    def find_at_position(self, x: int, y: int) -> Optional[Widget]:
        """Find widget at screen position."""
        if self._widget_tree:
            return self._find_at_point(self._widget_tree, x, y)
        return None

    def _search_tree(
        self,
        widget: Widget,
        matcher: Callable[[Widget], bool],
    ) -> Optional[Widget]:
        """Search tree for matching widget."""
        if matcher(widget):
            return widget

        for child in widget.children:
            result = self._search_tree(child, matcher)
            if result:
                return result

        return None

    def _collect_matching(
        self,
        widget: Widget,
        matcher: Callable[[Widget], bool],
        results: List[Widget],
    ) -> None:
        """Collect all matching widgets."""
        if matcher(widget):
            results.append(widget)

        for child in widget.children:
            self._collect_matching(child, matcher, results)

    def _find_at_point(
        self,
        widget: Widget,
        x: int,
        y: int,
    ) -> Optional[Widget]:
        """Find deepest widget at point."""
        bx, by, bw, bh = widget.bounds

        if not (bx <= x < bx + bw and by <= y < by + bh):
            return None

        for child in widget.children:
            result = self._find_at_point(child, x, y)
            if result:
                return result

        return widget

    def get_ancestors(self, widget: Widget) -> List[Widget]:
        """Get ancestor chain for widget."""
        ancestors: List[Widget] = []
        current = widget.parent

        while current:
            ancestors.append(current)
            current = current.parent

        return ancestors

    def get_siblings(self, widget: Widget) -> List[Widget]:
        """Get sibling widgets."""
        if widget.parent:
            return [w for w in widget.parent.children if w != widget]
        return []

    def clear_cache(self) -> None:
        """Clear the widget cache."""
        self._cache.clear()


def create_widget_locator() -> WidgetLocator:
    """Factory function."""
    return WidgetLocator()
