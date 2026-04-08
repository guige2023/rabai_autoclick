"""UI Transition Detection Utilities.

Detects and analyzes UI state transitions and animations.
Supports change detection, transition classification, and timing analysis.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class TransitionState(Enum):
    """State of a UI transition."""

    IDLE = auto()
    STARTING = auto()
    IN_PROGRESS = auto()
    COMPLETING = auto()
    COMPLETED = auto()


class TransitionType(Enum):
    """Types of UI transitions."""

    NONE = auto()
    APPEAR = auto()
    DISAPPEAR = auto()
    MOVE = auto()
    RESIZE = auto()
    FADE_IN = auto()
    FADE_OUT = auto()
    COLOR_CHANGE = auto()
    CONTENT_UPDATE = auto()
    LAYOUT_CHANGE = auto()


@dataclass
class TransitionEvent:
    """Represents a detected transition.

    Attributes:
        transition_type: Type of transition detected.
        element_id: Element involved in the transition.
        start_time: When transition started.
        end_time: When transition ended (if known).
        start_bounds: Element bounds at start.
        end_bounds: Element bounds at end (if known).
        metadata: Additional transition metadata.
    """

    transition_type: TransitionType
    element_id: str
    start_time: float
    end_time: Optional[float] = None
    start_bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    end_bounds: Optional[tuple[float, float, float, float]] = None
    metadata: dict = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[float]:
        """Get transition duration in milliseconds."""
        if self.end_time:
            return (self.end_time - self.start_time) * 1000
        return None


@dataclass
class TransitionMetrics:
    """Metrics for a transition analysis.

    Attributes:
        total_transitions: Number of transitions detected.
        average_duration_ms: Average transition duration.
        longest_transition: Longest transition in ms.
        shortest_transition: Shortest transition in ms.
        transition_types: Count by transition type.
    """

    total_transitions: int = 0
    average_duration_ms: float = 0.0
    longest_transition_ms: float = 0.0
    shortest_transition_ms: float = float("inf")
    transition_types: dict[TransitionType, int] = field(default_factory=dict)


class TransitionDetector:
    """Detects UI transitions by comparing states.

    Example:
        detector = TransitionDetector()
        detector.record_state({"button1": (100, 100, 50, 20)})
        time.sleep(0.1)
        transitions = detector.detect({"button1": (150, 100, 50, 20)})
    """

    def __init__(
        self,
        position_threshold: float = 5.0,
        size_threshold: float = 2.0,
        time_threshold_ms: int = 50,
    ):
        """Initialize the transition detector.

        Args:
            position_threshold: Pixels of movement to consider a transition.
            size_threshold: Pixels of resize to consider a transition.
            time_threshold_ms: Minimum time between states to detect transition.
        """
        self.position_threshold = position_threshold
        self.size_threshold = size_threshold
        self.time_threshold_ms = time_threshold_ms
        self._last_state: Optional[dict] = None
        self._last_state_time: float = 0
        self._active_transitions: dict[str, TransitionEvent] = {}

    def record_state(
        self,
        state: dict[str, tuple[float, float, float, float]],
    ) -> None:
        """Record a state snapshot.

        Args:
            state: Map of element_id to bounds (x, y, w, h).
        """
        self._last_state = state.copy()
        self._last_state_time = time.time()

    def detect(
        self,
        new_state: dict[str, tuple[float, float, float, float]],
    ) -> list[TransitionEvent]:
        """Detect transitions between last recorded state and new state.

        Args:
            new_state: New state snapshot.

        Returns:
            List of detected TransitionEvents.
        """
        if self._last_state is None:
            self._last_state = new_state
            return []

        transitions = []
        current_time = time.time()
        all_elements = set(self._last_state.keys()) | set(new_state.keys())

        for element_id in all_elements:
            old_bounds = self._last_state.get(element_id)
            new_bounds = new_state.get(element_id)

            if old_bounds is None and new_bounds is not None:
                # Element appeared
                transitions.append(
                    TransitionEvent(
                        transition_type=TransitionType.APPEAR,
                        element_id=element_id,
                        start_time=self._last_state_time,
                        end_time=current_time,
                        start_bounds=(0, 0, 0, 0),
                        end_bounds=new_bounds,
                    )
                )
            elif old_bounds is not None and new_bounds is None:
                # Element disappeared
                transitions.append(
                    TransitionEvent(
                        transition_type=TransitionType.DISAPPEAR,
                        element_id=element_id,
                        start_time=self._last_state_time,
                        end_time=current_time,
                        start_bounds=old_bounds,
                        end_bounds=(0, 0, 0, 0),
                    )
                )
            elif old_bounds != new_bounds:
                # Element changed
                transition_type = self._classify_bounds_change(
                    element_id, old_bounds, new_bounds
                )
                transitions.append(
                    TransitionEvent(
                        transition_type=transition_type,
                        element_id=element_id,
                        start_time=self._last_state_time,
                        end_time=current_time,
                        start_bounds=old_bounds,
                        end_bounds=new_bounds,
                    )
                )

        self._last_state = new_state
        self._last_state_time = current_time
        return transitions

    def _classify_bounds_change(
        self,
        element_id: str,
        old_bounds: tuple[float, float, float, float],
        new_bounds: tuple[float, float, float, float],
    ) -> TransitionType:
        """Classify the type of bounds change.

        Args:
            element_id: Element identifier.
            old_bounds: Previous bounds.
            new_bounds: New bounds.

        Returns:
            TransitionType classification.
        """
        ox, oy, ow, oh = old_bounds
        nx, ny, nw, nh = new_bounds

        pos_changed = abs(ox - nx) > self.position_threshold or abs(oy - ny) > self.position_threshold
        size_changed = abs(ow - nw) > self.size_threshold or abs(oh - nh) > self.size_threshold

        if pos_changed and size_changed:
            return TransitionType.LAYOUT_CHANGE
        elif pos_changed:
            return TransitionType.MOVE
        elif size_changed:
            return TransitionType.RESIZE
        else:
            return TransitionType.CONTENT_UPDATE


class TransitionClassifier:
    """Classifies transitions by their characteristics."""

    # Duration thresholds in milliseconds
    INSTANT_THRESHOLD_MS = 50
    QUICK_THRESHOLD_MS = 200
    NORMAL_THRESHOLD_MS = 500
    SLOW_THRESHOLD_MS = 1000

    @classmethod
    def classify_speed(cls, duration_ms: float) -> str:
        """Classify transition speed.

        Args:
            duration_ms: Transition duration.

        Returns:
            Speed classification string.
        """
        if duration_ms < cls.INSTANT_THRESHOLD_MS:
            return "instant"
        elif duration_ms < cls.QUICK_THRESHOLD_MS:
            return "quick"
        elif duration_ms < cls.NORMAL_THRESHOLD_MS:
            return "normal"
        elif duration_ms < cls.SLOW_THRESHOLD_MS:
            return "slow"
        else:
            return "very_slow"

    @classmethod
    def is_animation(cls, duration_ms: float) -> bool:
        """Check if a transition appears to be an animation.

        Args:
            duration_ms: Transition duration.

        Returns:
            True if transition duration suggests animation.
        """
        return cls.QUICK_THRESHOLD_MS <= duration_ms <= cls.SLOW_THRESHOLD_MS


class TransitionMonitor:
    """Monitors UI transitions over time.

    Tracks transition patterns and can detect when UI has stabilized.

    Example:
        monitor = TransitionMonitor()
        monitor.start()
        # ... perform actions ...
        monitor.stop()
        metrics = monitor.get_metrics()
    """

    def __init__(self, stability_threshold_ms: float = 500):
        """Initialize the monitor.

        Args:
            stability_threshold_ms: Time without transitions to consider UI stable.
        """
        self.stability_threshold_ms = stability_threshold_ms
        self._detector = TransitionDetector()
        self._transition_history: list[TransitionEvent] = []
        self._is_monitoring = False
        self._monitor_start_time: Optional[float] = None
        self._last_transition_time: float = 0

    def start(self) -> None:
        """Start monitoring transitions."""
        self._is_monitoring = True
        self._monitor_start_time = time.time()
        self._last_transition_time = self._monitor_start_time

    def stop(self) -> None:
        """Stop monitoring transitions."""
        self._is_monitoring = False

    def record_state(
        self,
        state: dict[str, tuple[float, float, float, float]],
    ) -> list[TransitionEvent]:
        """Record a state and detect any transitions.

        Args:
            state: Current UI state snapshot.

        Returns:
            List of detected transitions.
        """
        if not self._is_monitoring:
            return []

        self._detector.record_state(state)
        return []

    def detect_and_record(
        self,
        new_state: dict[str, tuple[float, float, float, float]],
    ) -> list[TransitionEvent]:
        """Detect transitions and add to history.

        Args:
            new_state: New state snapshot.

        Returns:
            List of detected transitions.
        """
        if not self._is_monitoring:
            return []

        transitions = self._detector.detect(new_state)
        for t in transitions:
            t.end_time = time.time()

        self._transition_history.extend(transitions)
        if transitions:
            self._last_transition_time = time.time()

        return transitions

    def is_stable(self) -> bool:
        """Check if UI is currently stable.

        Returns:
            True if no transitions detected recently.
        """
        if not self._is_monitoring:
            return True
        elapsed_ms = (time.time() - self._last_transition_time) * 1000
        return elapsed_ms >= self.stability_threshold_ms

    def wait_for_stability(
        self,
        timeout_ms: float = 10000,
    ) -> bool:
        """Wait for UI to become stable.

        Args:
            timeout_ms: Maximum time to wait.

        Returns:
            True if stability was achieved within timeout.
        """
        start = time.time()
        while self._is_monitoring:
            if self.is_stable():
                return True
            if (time.time() - start) * 1000 > timeout_ms:
                return False
            time.sleep(0.05)

        return True

    def get_metrics(self) -> TransitionMetrics:
        """Get metrics for all recorded transitions.

        Returns:
            TransitionMetrics summary.
        """
        metrics = TransitionMetrics()
        metrics.total_transitions = len(self._transition_history)

        durations = []
        for t in self._transition_history:
            t_type = t.transition_type
            metrics.transition_types[t_type] = metrics.transition_types.get(t_type, 0) + 1

            if t.duration_ms is not None:
                durations.append(t.duration_ms)
                metrics.longest_transition_ms = max(metrics.longest_transition_ms, t.duration_ms)
                metrics.shortest_transition_ms = min(metrics.shortest_transition_ms, t.duration_ms)

        if durations:
            metrics.average_duration_ms = sum(durations) / len(durations)

        return metrics
