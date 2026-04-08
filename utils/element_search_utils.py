"""
Element Search Utilities.

Advanced element search utilities including fuzzy matching,
role-path queries, and batch element finding.

Usage:
    from utils.element_search_utils import ElementSearcher

    searcher = ElementSearcher(tree)
    results = searcher.fuzzy_search("submit")
    results = searcher.search_by_role_path("window/group/button")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, Tuple, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class SearchResult:
    """Result of an element search."""
    element: Dict[str, Any]
    score: float
    match_type: str
    matched_text: Optional[str] = None

    def __repr__(self) -> str:
        return f"SearchResult({self.element.get('role')!r}, score={self.score:.2f})"


class ElementSearcher:
    """
    Advanced element search utilities.

    Provides fuzzy search, role-path queries, and batch
    element finding for accessibility trees.

    Example:
        searcher = ElementSearcher(tree)
        results = searcher.fuzzy_search("cancel")
        for r in results:
            print(f"Found: {r.element.get('title')}")
    """

    def __init__(self, tree: Dict[str, Any]) -> None:
        """
        Initialize the searcher.

        Args:
            tree: Accessibility tree dictionary.
        """
        self._tree = tree

    def fuzzy_search(
        self,
        query: str,
        threshold: float = 0.5,
        fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        """
        Perform fuzzy search across elements.

        Args:
            query: Search query string.
            threshold: Minimum match score (0.0-1.0).
            fields: Fields to search (default: title, value, description).

        Returns:
            List of SearchResult objects sorted by score.
        """
        if fields is None:
            fields = ["title", "value", "description"]

        query_lower = query.lower()
        results: List[SearchResult] = []

        for element in self._flatten():
            best_score = 0.0
            best_match = None

            for field in fields:
                value = element.get(field, "")
                if not value or not isinstance(value, str):
                    continue

                score = self._fuzzy_score(query_lower, value.lower())
                if score > best_score:
                    best_score = score
                    best_match = value

            if best_score >= threshold:
                results.append(SearchResult(
                    element=element,
                    score=best_score,
                    match_type="fuzzy",
                    matched_text=best_match,
                ))

        results.sort(key=lambda r: -r.score)
        return results

    def _fuzzy_score(
        self,
        query: str,
        text: str,
    ) -> float:
        """Calculate a fuzzy match score."""
        if query in text:
            return 1.0

        if not query or not text:
            return 0.0

        query_chars = list(query)
        text_chars = list(text)

        matches = 0
        qi = 0

        for tc in text_chars:
            if qi < len(query_chars) and tc == query_chars[qi]:
                matches += 1
                qi += 1

        if qi < len(query_chars):
            return 0.0

        return matches / len(text_chars)

    def search_by_role_path(
        self,
        path: str,
    ) -> List[Dict[str, Any]]:
        """
        Search by role path (e.g., "window/button[0]/text_field").

        Args:
            path: Role path string with optional indices.

        Returns:
            List of matching elements.
        """
        import re

        parts = path.split("/")
        results: List[Dict[str, Any]] = []

        def matches_part(element: Dict[str, Any], part: str) -> bool:
            role_match = False
            index = -1

            match = re.match(r"(\w+)\[(\d+)\]", part)
            if match:
                role_name = match.group(1)
                index = int(match.group(2))
                role_match = element.get("role") == role_name
            else:
                role_match = element.get("role") == part

            return role_match

        def traverse(node: Dict[str, Any], depth: int) -> None:
            if depth >= len(parts):
                results.append(node)
                return

            part = parts[depth]
            role = part.split("[")[0]

            if node.get("role") == role:
                if depth == len(parts) - 1:
                    results.append(node)
                else:
                    for child in node.get("children", []):
                        if isinstance(child, dict):
                            traverse(child, depth + 1)

        traverse(self._tree, 0)
        return results

    def search_by_attributes(
        self,
        **attrs,
    ) -> List[Dict[str, Any]]:
        """
        Search by element attributes.

        Args:
            **attrs: Attribute name=value pairs.

        Returns:
            List of matching elements.
        """
        results: List[Dict[str, Any]] = []

        for element in self._flatten():
            match = True
            for key, value in attrs.items():
                if element.get(key) != value:
                    match = False
                    break
            if match:
                results.append(element)

        return results

    def search_interactive(
        self,
        include_disabled: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Search for all interactive elements.

        Args:
            include_disabled: Include disabled elements.

        Returns:
            List of interactive elements.
        """
        interactive_roles = {
            "button", "push_button", "radio_button", "check_box",
            "text_field", "text_area", "combo_box", "pop_up_button",
            "menu_item", "link", "tab", "slider", "incrementor",
            "color_well", "search_field", "toggle",
        }

        results: List[Dict[str, Any]] = []

        for element in self._flatten():
            role = element.get("role", "")
            if role not in interactive_roles:
                continue

            if not include_disabled and not element.get("enabled", True):
                continue

            results.append(element)

        return results

    def search_within(
        self,
        element: Dict[str, Any],
        **criteria,
    ) -> List[Dict[str, Any]]:
        """
        Search for elements within a specific element subtree.

        Args:
            element: Root element to search within.
            **criteria: Search criteria (role, title, etc.).

        Returns:
            List of matching descendant elements.
        """
        results: List[Dict[str, Any]] = []

        def traverse(node: Dict[str, Any]) -> None:
            match = True
            for key, value in criteria.items():
                if node.get(key) != value:
                    match = False
                    break

            if match:
                results.append(node)

            for child in node.get("children", []):
                if isinstance(child, dict):
                    traverse(child)

        traverse(element)
        return results

    def _flatten(self) -> List[Dict[str, Any]]:
        """Flatten the tree into a list."""
        results: List[Dict[str, Any]] = []

        def traverse(node: Dict[str, Any]) -> None:
            results.append(node)
            for child in node.get("children", []):
                if isinstance(child, dict):
                    traverse(child)

        traverse(self._tree)
        return results


def fuzzy_search(
    tree: Dict[str, Any],
    query: str,
    threshold: float = 0.5,
) -> List[SearchResult]:
    """
    Quick fuzzy search in a tree.

    Args:
        tree: Accessibility tree.
        query: Search query.
        threshold: Minimum score.

    Returns:
        List of SearchResults.
    """
    searcher = ElementSearcher(tree)
    return searcher.fuzzy_search(query, threshold)
