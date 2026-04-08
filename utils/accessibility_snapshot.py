"""
Accessibility Snapshot Utilities.

Take and analyze full accessibility tree snapshots with
filtering, element search, and difference comparison.

Usage:
    from utils.accessibility_snapshot import AccessibilitySnapshot

    snapshot = AccessibilitySnapshot.take(bridge)
    buttons = snapshot.filter(role="button")
    print(f"Found {len(buttons)} buttons")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
import time

if TYPE_CHECKING:
    pass


@dataclass
class SnapshotOptions:
    """Options for taking a snapshot."""
    include_values: bool = True
    include_bounds: bool = True
    max_depth: Optional[int] = None
    role_filter: Optional[List[str]] = None


class AccessibilitySnapshot:
    """
    Take and analyze accessibility tree snapshots.

    Provides utilities for capturing a point-in-time snapshot
    of the accessibility tree and performing queries on it.

    Example:
        snapshot = AccessibilitySnapshot.take(bridge)
        elements = snapshot.find_all(title="Submit", role="button")
        print(f"Found: {len(elements)}")
    """

    def __init__(
        self,
        tree: Dict[str, Any],
        timestamp: float,
        app_name: Optional[str] = None,
    ) -> None:
        """
        Initialize a snapshot.

        Args:
            tree: The accessibility tree dictionary.
            timestamp: Unix timestamp when snapshot was taken.
            app_name: Name of the application.
        """
        self._tree = tree
        self._timestamp = timestamp
        self._app_name = app_name
        self._flat_cache: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def take(
        cls,
        bridge: Any,
        app: Optional[Any] = None,
        options: Optional[SnapshotOptions] = None,
    ) -> "AccessibilitySnapshot":
        """
        Take a snapshot of the current accessibility state.

        Args:
            bridge: AccessibilityBridge instance.
            app: Application element (defaults to frontmost).
            options: Optional snapshot options.

        Returns:
            AccessibilitySnapshot object.
        """
        opts = options or SnapshotOptions()

        if app is None:
            app = bridge.get_frontmost_app()
            app_name = None
        else:
            app_name = app.get("name") if isinstance(app, dict) else None

        if app is None:
            tree = {}
        else:
            tree = bridge.build_accessibility_tree(app)

        timestamp = time.time()

        return cls(tree=tree, timestamp=timestamp, app_name=app_name)

    def filter(
        self,
        role: Optional[str] = None,
        title: Optional[str] = None,
        enabled: Optional[bool] = None,
        interactive: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Filter elements in the snapshot.

        Args:
            role: Filter by role.
            title: Filter by title (substring match).
            enabled: Filter by enabled state.
            interactive: Only return interactive elements.

        Returns:
            List of matching element dictionaries.
        """
        results = self._flatten()
        filtered = []

        interactive_roles = {
            "button", "push_button", "radio_button", "check_box",
            "text_field", "text_area", "combo_box", "pop_up_button",
            "menu_item", "link", "tab", "slider",
        }

        for elem in results:
            if role and elem.get("role") != role:
                continue
            if title:
                elem_title = elem.get("title", "")
                if title.lower() not in elem_title.lower():
                    continue
            if enabled is not None and elem.get("enabled") != enabled:
                continue
            if interactive and elem.get("role") not in interactive_roles:
                continue

            filtered.append(elem)

        return filtered

    def find_all(
        self,
        **criteria,
    ) -> List[Dict[str, Any]]:
        """
        Find all elements matching criteria.

        Supported criteria: role, title, enabled, interactive.

        Returns:
            List of matching elements.
        """
        return self.filter(**criteria)

    def find_first(
        self,
        **criteria,
    ) -> Optional[Dict[str, Any]]:
        """
        Find the first element matching criteria.

        Args:
            **criteria: Filter criteria.

        Returns:
            First matching element or None.
        """
        results = self.filter(**criteria)
        return results[0] if results else None

    def count(
        self,
        role: Optional[str] = None,
    ) -> int:
        """
        Count elements, optionally filtered by role.

        Args:
            role: Optional role filter.

        Returns:
            Count of matching elements.
        """
        if role is None:
            return len(self._flatten())
        return len(self.filter(role=role))

    def get_all_roles(self) -> List[str]:
        """
        Get all unique roles in the snapshot.

        Returns:
            List of role names.
        """
        roles = set()
        for elem in self._flatten():
            role = elem.get("role")
            if role:
                roles.add(role)
        return sorted(roles)

    def get_interactive_elements(self) -> List[Dict[str, Any]]:
        """
        Get all interactive elements.

        Returns:
            List of interactive element dictionaries.
        """
        return self.filter(interactive=True)

    def _flatten(self) -> List[Dict[str, Any]]:
        """Flatten the tree into a list."""
        if self._flat_cache is not None:
            return self._flat_cache

        results: List[Dict[str, Any]] = []

        def traverse(node: Dict[str, Any]) -> None:
            results.append(node)
            for child in node.get("children", []):
                if isinstance(child, dict):
                    traverse(child)

        traverse(self._tree)
        self._flat_cache = results
        return results

    @property
    def timestamp(self) -> float:
        """Get the snapshot timestamp."""
        return self._timestamp

    @property
    def tree(self) -> Dict[str, Any]:
        """Get the raw tree dictionary."""
        return self._tree

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert snapshot to a dictionary.

        Returns:
            Dictionary representation.
        """
        return {
            "timestamp": self._timestamp,
            "app_name": self._app_name,
            "tree": self._tree,
        }
