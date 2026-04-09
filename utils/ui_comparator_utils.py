"""
UI comparison and diff utilities for automation testing.

This module provides utilities for comparing UI states,
detecting visual differences, and generating diff reports.
"""

from __future__ import annotations

import time
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple, Any
from enum import Enum, auto


class DiffType(Enum):
    """Types of differences detected between UI states."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    MOVED = auto()
    STYLE_CHANGED = auto()
    TEXT_CHANGED = auto()
    VISIBILITY_CHANGED = auto()


@dataclass
class Diff:
    """
    Represents a single difference between UI elements.

    Attributes:
        diff_type: Type of difference.
        element_id: Identifier of the element.
        path: Path to the element in the UI tree.
        old_value: Previous value (for MODIFIED).
        new_value: Current value (for MODIFIED).
        description: Human-readable description.
    """
    diff_type: DiffType
    element_id: str
    path: str
    old_value: Any = None
    new_value: Any = None
    description: str = ""


@dataclass
class DiffReport:
    """
    Complete diff report between two UI snapshots.

    Attributes:
        timestamp: When the comparison was made.
        total_changes: Total number of changes.
        diffs: List of individual differences.
        summary: Short summary of changes.
    """
    timestamp: float = field(default_factory=time.time)
    total_changes: int = 0
    diffs: List[Diff] = field(default_factory=list)
    summary: str = ""

    def has_changes(self) -> bool:
        """Check if any changes were detected."""
        return self.total_changes > 0

    def get_diffs_by_type(self, diff_type: DiffType) -> List[Diff]:
        """Get all diffs of a specific type."""
        return [d for d in self.diffs if d.diff_type == diff_type]

    def added_count(self) -> int:
        """Count of added elements."""
        return len(self.get_diffs_by_type(DiffType.ADDED))

    def removed_count(self) -> int:
        """Count of removed elements."""
        return len(self.get_diffs_by_type(DiffType.REMOVED))

    def modified_count(self) -> int:
        """Count of modified elements."""
        return len(self.get_diffs_by_type(DiffType.MODIFIED))


@dataclass
class UIElementSnapshot:
    """
    Snapshot of a UI element at a point in time.

    Attributes:
        element_id: Unique identifier.
        path: Path in the UI tree.
        role: Accessibility role.
        title: Element title.
        value: Current value.
        bounds: Bounding rectangle.
        children: IDs of child elements.
        attributes: Additional attributes.
        checksum: Hash of element state.
    """
    element_id: str
    path: str
    role: str = ""
    title: str = ""
    value: str = ""
    bounds: Optional[Tuple[float, float, float, float]] = None
    children: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    checksum: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> UIElementSnapshot:
        """Create snapshot from dictionary."""
        return cls(
            element_id=data.get("elementId", ""),
            path=data.get("path", ""),
            role=data.get("role", ""),
            title=data.get("title", ""),
            value=data.get("value", ""),
            bounds=data.get("bounds"),
            children=data.get("children", []),
            attributes=data.get("attributes", {}),
            checksum=data.get("checksum", ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "elementId": self.element_id,
            "path": self.path,
            "role": self.role,
            "title": self.title,
            "value": self.value,
            "bounds": self.bounds,
            "children": self.children,
            "attributes": self.attributes,
            "checksum": self.checksum,
        }

    def compute_checksum(self) -> str:
        """Compute hash of element state."""
        content = f"{self.role}:{self.title}:{self.value}:{self.bounds}"
        return hashlib.md5(content.encode()).hexdigest()


class UIDiffEngine:
    """
    Engine for comparing UI snapshots and generating diff reports.

    Performs deep comparison of UI trees and detects various
    types of changes between states.
    """

    def __init__(self) -> None:
        self._element_matchers: Dict[str, Callable[[str, str], bool]] = {}

    def compare(
        self,
        old_snapshot: Dict[str, UIElementSnapshot],
        new_snapshot: Dict[str, UIElementSnapshot],
    ) -> DiffReport:
        """
        Compare two UI snapshots and generate a diff report.

        Args:
            old_snapshot: Map of element_id to snapshot.
            new_snapshot: Map of element_id to snapshot.

        Returns:
            DiffReport with all detected differences.
        """
        report = DiffReport()
        old_ids = set(old_snapshot.keys())
        new_ids = set(new_snapshot.keys())

        # Detect added elements
        for element_id in new_ids - old_ids:
            new_elem = new_snapshot[element_id]
            report.diffs.append(Diff(
                diff_type=DiffType.ADDED,
                element_id=element_id,
                path=new_elem.path,
                new_value=new_elem.to_dict(),
                description=f"Element added: {new_elem.role} '{new_elem.title}'",
            ))

        # Detect removed elements
        for element_id in old_ids - new_ids:
            old_elem = old_snapshot[element_id]
            report.diffs.append(Diff(
                diff_type=DiffType.REMOVED,
                element_id=element_id,
                path=old_elem.path,
                old_value=old_elem.to_dict(),
                description=f"Element removed: {old_elem.role} '{old_elem.title}'",
            ))

        # Detect modified elements
        for element_id in old_ids & new_ids:
            old_elem = old_snapshot[element_id]
            new_elem = new_snapshot[element_id]

            # Compute checksums
            old_checksum = old_elem.compute_checksum()
            new_checksum = new_elem.compute_checksum()

            if old_checksum != new_checksum:
                diffs = self._compare_elements(old_elem, new_elem)
                report.diffs.extend(diffs)

        # Detect moved elements (position change)
        for diff in self._detect_moves(old_snapshot, new_snapshot):
            report.diffs.append(diff)

        report.total_changes = len(report.diffs)
        report.summary = self._generate_summary(report)
        return report

    def _compare_elements(self, old_elem: UIElementSnapshot, new_elem: UIElementSnapshot) -> List[Diff]:
        """Compare two elements and return list of differences."""
        diffs: List[Diff] = []

        if old_elem.role != new_elem.role:
            diffs.append(Diff(
                diff_type=DiffType.MODIFIED,
                element_id=old_elem.element_id,
                path=old_elem.path,
                old_value=old_elem.role,
                new_value=new_elem.role,
                description="Role changed",
            ))

        if old_elem.title != new_elem.title:
            diffs.append(Diff(
                diff_type=DiffType.TEXT_CHANGED,
                element_id=old_elem.element_id,
                path=old_elem.path,
                old_value=old_elem.title,
                new_value=new_elem.title,
                description="Title changed",
            ))

        if old_elem.value != new_elem.value:
            diffs.append(Diff(
                diff_type=DiffType.MODIFIED,
                element_id=old_elem.element_id,
                path=old_elem.path,
                old_value=old_elem.value,
                new_value=new_elem.value,
                description="Value changed",
            ))

        if old_elem.bounds != new_elem.bounds:
            diffs.append(Diff(
                diff_type=DiffType.MOVED,
                element_id=old_elem.element_id,
                path=old_elem.path,
                old_value=old_elem.bounds,
                new_value=new_elem.bounds,
                description="Position changed",
            ))

        # Check for style/attribute changes
        for attr in set(old_elem.attributes.keys()) | set(new_elem.attributes.keys()):
            old_val = old_elem.attributes.get(attr)
            new_val = new_elem.attributes.get(attr)
            if old_val != new_val:
                diffs.append(Diff(
                    diff_type=DiffType.STYLE_CHANGED,
                    element_id=old_elem.element_id,
                    path=f"{old_elem.path}.{attr}",
                    old_value=old_val,
                    new_value=new_val,
                    description=f"Attribute '{attr}' changed",
                ))

        return diffs

    def _detect_moves(
        self,
        old_snapshot: Dict[str, UIElementSnapshot],
        new_snapshot: Dict[str, UIElementSnapshot],
    ) -> List[Diff]:
        """Detect elements that moved to different positions."""
        diffs: List[Diff] = []

        # Find elements that exist in both with same ID but different parent
        for element_id in old_snapshot.keys() & new_snapshot.keys():
            old_elem = old_snapshot[element_id]
            new_elem = new_snapshot[element_id]

            # Check if path changed (indicating move)
            old_parent = "/".join(old_elem.path.split("/")[:-1])
            new_parent = "/".join(new_elem.path.split("/")[:-1])

            if old_parent != new_parent:
                diffs.append(Diff(
                    diff_type=DiffType.MOVED,
                    element_id=element_id,
                    path=new_elem.path,
                    old_value=old_elem.path,
                    new_value=new_elem.path,
                    description=f"Moved from {old_parent} to {new_parent}",
                ))

        return diffs

    def _generate_summary(self, report: DiffReport) -> str:
        """Generate a short summary string."""
        added = report.added_count()
        removed = report.removed_count()
        modified = report.modified_count()
        moved = len(report.get_diffs_by_type(DiffType.MOVED))

        parts = []
        if added:
            parts.append(f"+{added}")
        if removed:
            parts.append(f"-{removed}")
        if modified:
            parts.append(f"~{modified}")
        if moved:
            parts.append(f">{moved}")

        return " ".join(parts) if parts else "No changes"


def generate_screenshot_diff(
    old_image: bytes,
    new_image: bytes,
    threshold: float = 0.1,
) -> Tuple[bool, float, List[Tuple[int, int]]]:
    """
    Compare two screenshots and find differing regions.

    Returns:
        Tuple of (has_diff, diff_percentage, diff_regions)
    """
    # Simple pixel-by-pixel comparison
    if len(old_image) != len(new_image):
        return True, 100.0, []

    diff_count = 0
    total_pixels = len(old_image) // 4  # Assuming RGBA
    diff_regions: List[Tuple[int, int]] = []

    # For simplicity, just return a basic diff indicator
    # In production, would use image processing libraries
    has_diff = old_image != new_image

    return has_diff, diff_count / total_pixels if total_pixels else 0, diff_regions
