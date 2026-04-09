"""Element stability tracker for monitoring element stability over time."""
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import time


@dataclass
class StabilitySnapshot:
    """A snapshot of element state for stability tracking."""
    timestamp: float
    bounds: Tuple[int, int, int, int]
    visible: bool
    text: Optional[str] = None


@dataclass
class StabilityReport:
    """Report on element stability."""
    element_id: str
    is_stable: bool
    stability_score: float
    position_variance: float
    size_variance: float
    visibility_changes: int
    sample_count: int


class ElementStabilityTracker:
    """Tracks element stability by monitoring bounds and visibility changes.
    
    Records element snapshots over time to determine if an element
    is stable enough for interaction.
    
    Example:
        tracker = ElementStabilityTracker()
        tracker.record("btn1", bounds=(100,200,50,20), visible=True)
        tracker.record("btn1", bounds=(100,200,50,20), visible=True)
        report = tracker.get_report("btn1")
        if report.is_stable:
            print("Element is stable, safe to click")
    """

    def __init__(self, stability_threshold: float = 0.8, sample_window: float = 2.0) -> None:
        self._snapshots: Dict[str, List[StabilitySnapshot]] = {}
        self._threshold = stability_threshold
        self._window = sample_window

    def record(
        self,
        element_id: str,
        bounds: Tuple[int, int, int, int],
        visible: bool,
        text: Optional[str] = None,
    ) -> None:
        """Record a snapshot of element state."""
        if element_id not in self._snapshots:
            self._snapshots[element_id] = []
        snapshot = StabilitySnapshot(
            timestamp=time.time(),
            bounds=bounds,
            visible=visible,
            text=text,
        )
        self._snapshots[element_id].append(snapshot)
        cutoff = time.time() - self._window
        self._snapshots[element_id] = [
            s for s in self._snapshots[element_id] if s.timestamp >= cutoff
        ]

    def get_report(self, element_id: str) -> Optional[StabilityReport]:
        """Get stability report for an element."""
        snapshots = self._snapshots.get(element_id, [])
        if len(snapshots) < 2:
            return None
        bounds_list = [s.bounds for s in snapshots]
        x_coords = [b[0] for b in bounds_list]
        y_coords = [b[1] for b in bounds_list]
        widths = [b[2] for b in bounds_list]
        heights = [b[3] for b in bounds_list]
        pos_var = (self._variance(x_coords) + self._variance(y_coords)) / 2
        size_var = (self._variance(widths) + self._variance(heights)) / 2
        visibility_changes = sum(
            1 for i in range(1, len(snapshots))
            if snapshots[i].visible != snapshots[i-1].visible
        )
        pos_stable = max(0, 1 - pos_var / 100)
        size_stable = max(0, 1 - size_var / 50)
        vis_stable = max(0, 1 - visibility_changes / len(snapshots))
        stability_score = pos_stable * 0.5 + size_stable * 0.3 + vis_stable * 0.2
        return StabilityReport(
            element_id=element_id,
            is_stable=stability_score >= self._threshold,
            stability_score=round(stability_score, 3),
            position_variance=round(pos_var, 2),
            size_variance=round(size_var, 2),
            visibility_changes=visibility_changes,
            sample_count=len(snapshots),
        )

    def is_stable(self, element_id: str) -> bool:
        """Quick check if element is currently stable."""
        report = self.get_report(element_id)
        return report.is_stable if report else False

    def wait_for_stability(
        self,
        element_id: str,
        timeout: float = 5.0,
    ) -> bool:
        """Wait for an element to become stable."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_stable(element_id):
                return True
            time.sleep(0.1)
        return False

    def clear(self, element_id: Optional[str] = None) -> None:
        """Clear snapshots for an element or all elements."""
        if element_id:
            self._snapshots.pop(element_id, None)
        else:
            self._snapshots.clear()

    def _variance(self, values: List[float]) -> float:
        """Calculate variance of values."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((v - mean) ** 2 for v in values) / len(values)
