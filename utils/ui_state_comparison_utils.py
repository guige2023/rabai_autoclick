"""UI State Comparison Utilities.

Compares UI states for change detection, regression testing, and validation.
Computes diffs between element trees and identifies meaningful state changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional


class ChangeType(Enum):
    """Types of changes detected between states."""

    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    MOVED = auto()
    REORDERED = auto()
    UNCHANGED = auto()


@dataclass
class PropertyChange:
    """Represents a change to a single property.

    Attributes:
        property_name: Name of the changed property.
        old_value: Previous value.
        new_value: New value.
    """

    property_name: str
    old_value: Any
    new_value: Any


@dataclass
class ElementChange:
    """Represents a change to a UI element.

    Attributes:
        element_id: Unique identifier of the element.
        element_role: Role of the element.
        change_type: Type of change detected.
        path: Hierarchical path to the element.
        property_changes: List of property modifications.
        old_position: Previous position (x, y) if changed.
        new_position: New position (x, y) if changed.
    """

    element_id: str
    element_role: str
    change_type: ChangeType
    path: str = ""
    property_changes: list[PropertyChange] = field(default_factory=list)
    old_position: Optional[tuple[int, int]] = None
    new_position: Optional[tuple[int, int]] = None


@dataclass
class StateComparisonResult:
    """Result of comparing two UI states.

    Attributes:
        added: Elements that were added.
        removed: Elements that were removed.
        modified: Elements that were modified.
        moved: Elements that changed position.
        reordered: Elements that changed order among siblings.
        unchanged: Elements that remain the same.
        total_changes: Total number of changes detected.
    """

    added: list[ElementChange] = field(default_factory=list)
    removed: list[ElementChange] = field(default_factory=list)
    modified: list[ElementChange] = field(default_factory=list)
    moved: list[ElementChange] = field(default_factory=list)
    reordered: list[ElementChange] = field(default_factory=list)
    unchanged: list[ElementChange] = field(default_factory=list)

    @property
    def total_changes(self) -> int:
        """Total number of changes detected."""
        return (
            len(self.added)
            + len(self.removed)
            + len(self.modified)
            + len(self.moved)
            + len(self.reordered)
        )

    @property
    def has_changes(self) -> bool:
        """Whether any changes were detected."""
        return self.total_changes > 0

    @property
    def summary(self) -> str:
        """Human-readable summary of changes."""
        parts = []
        if self.added:
            parts.append(f"+{len(self.added)} added")
        if self.removed:
            parts.append(f"-{len(self.removed)} removed")
        if self.modified:
            parts.append(f"~{len(self.modified)} modified")
        if self.moved:
            parts.append(f">{len(self.moved)} moved")
        if self.reordered:
            parts.append(f"#{len(self.reordered)} reordered")
        if not parts:
            return "No changes"
        return ", ".join(parts)


@dataclass
class UIElementSnapshot:
    """Snapshot of a UI element at a point in time.

    Attributes:
        element_id: Unique identifier.
        role: Accessibility role.
        name: Accessible name.
        value: Current value (for inputs, etc.).
        bounds: Position and size (x, y, width, height).
        visible: Whether element is visible.
        enabled: Whether element is enabled.
        focused: Whether element has focus.
        children: List of child element IDs.
        properties: Additional properties.
    """

    element_id: str
    role: str = ""
    name: str = ""
    value: str = ""
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)
    visible: bool = True
    enabled: bool = True
    focused: bool = False
    children: list[str] = field(default_factory=list)
    properties: dict = field(default_factory=dict)

    def __hash__(self) -> int:
        """Hash based on element_id."""
        return hash(self.element_id)

    def __eq__(self, other: object) -> bool:
        """Equality based on element_id."""
        if not isinstance(other, UIElementSnapshot):
            return False
        return self.element_id == other.element_id

    def compute_diff(self, other: "UIElementSnapshot") -> list[ElementChange]:
        """Compute differences between this and another snapshot.

        Args:
            other: Snapshot to compare against.

        Returns:
            List of ElementChanges.
        """
        changes = []

        # Check position/size change
        if self.bounds != other.bounds:
            changes.append(
                ElementChange(
                    element_id=self.element_id,
                    element_role=self.role,
                    change_type=ChangeType.MOVED,
                    old_position=(self.bounds[0], self.bounds[1]),
                    new_position=(other.bounds[0], other.bounds[1]),
                )
            )

        # Check property changes
        for prop in ["name", "value", "visible", "enabled", "focused"]:
            self_val = getattr(self, prop)
            other_val = getattr(other, prop)
            if self_val != other_val:
                changes.append(
                    ElementChange(
                        element_id=self.element_id,
                        element_role=self.role,
                        change_type=ChangeType.MODIFIED,
                        property_changes=[
                            PropertyChange(
                                property_name=prop,
                                old_value=self_val,
                                new_value=other_val,
                            )
                        ],
                    )
                )

        # Check custom properties
        all_keys = set(self.properties.keys()) | set(other.properties.keys())
        for key in all_keys:
            self_val = self.properties.get(key)
            other_val = other.properties.get(key)
            if self_val != other_val:
                changes.append(
                    ElementChange(
                        element_id=self.element_id,
                        element_role=self.role,
                        change_type=ChangeType.MODIFIED,
                        property_changes=[
                            PropertyChange(
                                property_name=key,
                                old_value=self_val,
                                new_value=other_val,
                            )
                        ],
                    )
                )

        return changes


class StateComparator:
    """Compares UI states and computes differences.

    Example:
        comparator = StateComparator()
        before = capture_state()
        perform_action()
        after = capture_state()
        result = comparator.compare(before, after)
    """

    def __init__(self, ignore_properties: Optional[list[str]] = None):
        """Initialize the comparator.

        Args:
            ignore_properties: Properties to ignore in comparisons.
        """
        self.ignore_properties = ignore_properties or ["focused", "timestamp"]

    def compare(
        self,
        before: dict[str, UIElementSnapshot],
        after: dict[str, UIElementSnapshot],
    ) -> StateComparisonResult:
        """Compare two UI states.

        Args:
            before: Map of element_id to snapshot before.
            after: Map of element_id to snapshot after.

        Returns:
            StateComparisonResult with all detected changes.
        """
        result = StateComparisonResult()

        before_ids = set(before.keys())
        after_ids = set(after.keys())

        # Find added elements
        for element_id in after_ids - before_ids:
            result.added.append(
                ElementChange(
                    element_id=element_id,
                    element_role=after[element_id].role,
                    change_type=ChangeType.ADDED,
                )
            )

        # Find removed elements
        for element_id in before_ids - after_ids:
            result.removed.append(
                ElementChange(
                    element_id=element_id,
                    element_role=before[element_id].role,
                    change_type=ChangeType.REMOVED,
                )
            )

        # Find modified elements
        for element_id in before_ids & after_ids:
            snapshot_before = before[element_id]
            snapshot_after = after[element_id]

            changes = snapshot_before.compute_diff(snapshot_after)
            if changes:
                for change in changes:
                    if change.change_type == ChangeType.MOVED:
                        result.moved.append(change)
                    elif change.change_type == ChangeType.MODIFIED:
                        result.modified.append(change)
            else:
                result.unchanged.append(
                    ElementChange(
                        element_id=element_id,
                        element_role=snapshot_before.role,
                        change_type=ChangeType.UNCHANGED,
                    )
                )

        return result

    def has_semantic_changes(
        self,
        result: StateComparisonResult,
        ignored_roles: Optional[list[str]] = None,
    ) -> bool:
        """Check if result contains semantically significant changes.

        Args:
            result: StateComparisonResult to check.
            ignored_roles: Element roles to ignore.

        Returns:
            True if there are significant changes.
        """
        ignored_roles = set(ignored_roles or [])

        # Check for added/removed elements (except decorative)
        decorative_roles = {"separator", "spacer", "presentation"}
        significant_added = [
            e for e in result.added
            if e.element_role not in decorative_roles
        ]
        significant_removed = [
            e for e in result.removed
            if e.element_role not in decorative_roles
        ]

        if significant_added or significant_removed:
            return True

        # Check for property changes on interactive elements
        interactive_roles = {"button", "link", "checkbox", "radio", "textbox", "slider"}
        for change in result.modified:
            if change.element_role in interactive_roles:
                return True

        return False


class ChangeDetector:
    """Detects changes in UI state over time.

    Maintains history and can detect specific change patterns.

    Example:
        detector = ChangeDetector(threshold=5)
        detector.record_state(state)
        if detector.has_significant_change():
            notify_user()
    """

    def __init__(self, threshold: int = 3):
        """Initialize the change detector.

        Args:
            threshold: Number of changes considered significant.
        """
        self.threshold = threshold
        self._history: list[StateComparisonResult] = []

    def record_comparison(self, result: StateComparisonResult) -> None:
        """Record a comparison result in history.

        Args:
            result: StateComparisonResult to record.
        """
        self._history.append(result)
        if len(self._history) > 100:
            self._history = self._history[-100:]

    def has_significant_change(self) -> bool:
        """Check if the latest comparison had significant changes.

        Returns:
            True if changes exceed threshold.
        """
        if not self._history:
            return False
        return self._history[-1].total_changes >= self.threshold

    def get_change_frequency(self, window: int = 10) -> float:
        """Get frequency of changes over recent comparisons.

        Args:
            window: Number of recent comparisons to analyze.

        Returns:
            Average number of changes per comparison.
        """
        if not self._history:
            return 0.0
        recent = self._history[-window:]
        total = sum(r.total_changes for r in recent)
        return total / len(recent)

    def detect_stable_state(self, required_stable: int = 3) -> bool:
        """Detect if UI has been stable for N consecutive comparisons.

        Args:
            required_stable: Number of comparisons with no changes required.

        Returns:
            True if UI has been stable.
        """
        if len(self._history) < required_stable:
            return False
        recent = self._history[-required_stable:]
        return all(not r.has_changes for r in recent)
