"""
Visual State Comparison Utilities

Provides utilities for comparing visual states of UI elements,
including difference scoring, change detection, and state matching.
"""

from typing import List, Tuple, Optional, Dict, Callable, Any
from dataclasses import dataclass
from enum import Enum
import math


class ComparisonMode(Enum):
    """Modes for visual comparison."""
    PIXEL_DIFF = "pixel_diff"
    HISTOGRAM = "histogram"
    FEATURE_MATCH = "feature_match"
    TEMPLATE_MATCH = "template_match"
    STRUCTURAL = "structural"


@dataclass
class VisualState:
    """Represents a visual state snapshot."""
    state_id: str
    timestamp: float
    width: int
    height: int
    pixel_hash: str
    histogram: List[float]
    features: Dict[str, Any]


@dataclass
class ComparisonResult:
    """Result of a visual state comparison."""
    similarity_score: float
    difference_score: float
    changed_regions: List[Tuple[float, float, float, float]]
    change_percentage: float
    confidence: float
    mode: ComparisonMode
    
    @property
    def is_match(self) -> bool:
        """Check if the comparison indicates a match."""
        return self.similarity_score >= 0.95
    
    @property
    def has_changes(self) -> bool:
        """Check if significant changes were detected."""
        return self.change_percentage > 0.05


@dataclass
class ChangeRegion:
    """Represents a region where changes were detected."""
    x: float
    y: float
    width: float
    height: float
    intensity: float
    change_type: str


class VisualStateComparator:
    """
    Compares visual states for automation workflow validation.
    
    Supports multiple comparison modes for different accuracy and
    performance requirements.
    
    Example:
        >>> comparator = VisualStateComparator(ComparisonMode.PIXEL_DIFF)
        >>> result = comparator.compare(state1, state2)
        >>> print(f"Similarity: {result.similarity_score:.2%}")
    """
    
    def __init__(
        self,
        mode: ComparisonMode = ComparisonMode.PIXEL_DIFF,
        threshold: float = 0.05
    ) -> None:
        """
        Initialize visual state comparator.
        
        Args:
            mode: Comparison mode to use.
            threshold: Difference threshold for considering a change.
        """
        self._mode = mode
        self._threshold = threshold
    
    def compare(
        self,
        state1: VisualState,
        state2: VisualState
    ) -> ComparisonResult:
        """
        Compare two visual states.
        
        Args:
            state1: First visual state.
            state2: Second visual state.
            
        Returns:
            ComparisonResult with similarity and change information.
        """
        if self._mode == ComparisonMode.PIXEL_DIFF:
            return self._pixel_diff_compare(state1, state2)
        elif self._mode == ComparisonMode.HISTOGRAM:
            return self._histogram_compare(state1, state2)
        elif self._mode == ComparisonMode.FEATURE_MATCH:
            return self._feature_compare(state1, state2)
        else:
            return self._pixel_diff_compare(state1, state2)
    
    def _pixel_diff_compare(
        self,
        state1: VisualState,
        state2: VisualState
    ) -> ComparisonResult:
        """Pixel-by-pixel difference comparison."""
        if state1.pixel_hash == state2.pixel_hash:
            return ComparisonResult(
                similarity_score=1.0,
                difference_score=0.0,
                changed_regions=[],
                change_percentage=0.0,
                confidence=1.0,
                mode=self._mode
            )
        
        total_pixels = state1.width * state1.height
        changed_pixels = self._count_different_pixels(state1, state2)
        change_pct = changed_pixels / total_pixels if total_pixels > 0 else 0.0
        similarity = 1.0 - change_pct
        
        changed_regions = self._find_changed_regions(state1, state2)
        
        return ComparisonResult(
            similarity_score=max(0.0, similarity),
            difference_score=min(1.0, change_pct),
            changed_regions=changed_regions,
            change_percentage=change_pct,
            confidence=0.9,
            mode=self._mode
        )
    
    def _histogram_compare(
        self,
        state1: VisualState,
        state2: VisualState
    ) -> ComparisonResult:
        """Histogram-based comparison using Bhattacharyya distance."""
        hist1 = state1.histogram
        hist2 = state2.histogram
        
        if len(hist1) != len(hist2):
            min_len = min(len(hist1), len(hist2))
            hist1 = hist1[:min_len]
            hist2 = hist2[:min_len]
        
        bc_coefficient = self._bhattacharyya_coefficient(hist1, hist2)
        similarity = bc_coefficient
        difference = 1.0 - bc_coefficient
        
        return ComparisonResult(
            similarity_score=similarity,
            difference_score=difference,
            changed_regions=[],
            change_percentage=difference,
            confidence=0.75,
            mode=self._mode
        )
    
    def _feature_compare(
        self,
        state1: VisualState,
        state2: VisualState
    ) -> ComparisonResult:
        """Feature-based comparison."""
        features1 = state1.features
        features2 = state2.features
        
        common_keys = set(features1.keys()) & set(features2.keys())
        if not common_keys:
            return ComparisonResult(
                similarity_score=0.0,
                difference_score=1.0,
                changed_regions=[],
                change_percentage=1.0,
                confidence=0.5,
                mode=self._mode
            )
        
        match_count = sum(
            1 for k in common_keys
            if features1[k] == features2[k]
        )
        similarity = match_count / len(common_keys)
        
        return ComparisonResult(
            similarity_score=similarity,
            difference_score=1.0 - similarity,
            changed_regions=[],
            change_percentage=1.0 - similarity,
            confidence=0.85,
            mode=self._mode
        )
    
    def _count_different_pixels(
        self,
        state1: VisualState,
        state2: VisualState
    ) -> int:
        """Count number of different pixels between states."""
        if state1.width != state2.width or state1.height != state2.height:
            return state1.width * state1.height
        
        return sum(
            1 for i in range(len(state1.pixel_hash))
            if state1.pixel_hash[i] != state2.pixel_hash[i]
        )
    
    def _find_changed_regions(
        self,
        state1: VisualState,
        state2: VisualState,
        grid_size: int = 8
    ) -> List[Tuple[float, float, float, float]]:
        """Find rectangular regions with significant changes."""
        regions: List[Tuple[float, float, float, float]] = []
        
        cell_w = state1.width / grid_size
        cell_h = state1.height / grid_size
        
        for row in range(grid_size):
            for col in range(grid_size):
                if self._is_cell_changed(state1, state2, row, col, grid_size):
                    x = col * cell_w
                    y = row * cell_h
                    regions.append((x, y, cell_w, cell_h))
        
        return self._merge_adjacent_regions(regions)
    
    def _is_cell_changed(
        self,
        state1: VisualState,
        state2: VisualState,
        row: int,
        col: int,
        grid_size: int
    ) -> bool:
        """Check if a grid cell has changed."""
        hash_len = len(state1.pixel_hash)
        idx = row * grid_size + col
        hash_idx = (idx * hash_len) // (grid_size * grid_size)
        
        if hash_idx >= hash_len:
            return False
        
        return state1.pixel_hash[hash_idx] != state2.pixel_hash[hash_idx]
    
    def _merge_adjacent_regions(
        self,
        regions: List[Tuple[float, float, float, float]]
    ) -> List[Tuple[float, float, float, float]]:
        """Merge adjacent changed regions."""
        if not regions:
            return []
        
        merged = list(regions)
        changed = True
        
        while changed:
            changed = False
            new_merged: List[Tuple[float, float, float, float]] = []
            used = set()
            
            for i, (x1, y1, w1, h1) in enumerate(merged):
                if i in used:
                    continue
                
                for j, (x2, y2, w2, h2) in enumerate(merged):
                    if i >= j or j in used:
                        continue
                    
                    if (abs(x1 - x2) <= max(w1, w2) and
                        abs(y1 - y2) <= max(h1, h2)):
                        
                        nx = min(x1, x2)
                        ny = min(y1, y2)
                        nx2 = max(x1 + w1, x2 + w2)
                        ny2 = max(y1 + h1, y2 + h2)
                        new_merged.append((nx, ny, nx2 - nx, ny2 - ny))
                        used.add(i)
                        used.add(j)
                        changed = True
                        break
                
                if i not in used:
                    new_merged.append((x1, y1, w1, h1))
            
            merged = new_merged
        
        return merged
    
    @staticmethod
    def _bhattacharyya_coefficient(hist1: List[float], hist2: List[float]) -> float:
        """Calculate Bhattacharyya coefficient between histograms."""
        if not hist1 or not hist2:
            return 0.0
        
        sum_sq = math.sqrt(sum(a * b for a, b in zip(hist1, hist2)))
        return sum_sq


class StateChangeDetector:
    """
    Detects and tracks visual state changes over time.
    
    Maintains a history of visual states and detects when significant
    changes occur.
    """
    
    def __init__(
        self,
        comparator: Optional[VisualStateComparator] = None,
        change_threshold: float = 0.1
    ) -> None:
        """
        Initialize state change detector.
        
        Args:
            comparator: Comparator to use for state comparison.
            change_threshold: Threshold for considering a change significant.
        """
        self._comparator = comparator or VisualStateComparator()
        self._change_threshold = change_threshold
        self._state_history: List[VisualState] = []
        self._last_significant_change: Optional[VisualState] = None
    
    def add_state(self, state: VisualState) -> Optional[ComparisonResult]:
        """
        Add a new state and check for changes.
        
        Args:
            state: New visual state to add.
            
        Returns:
            ComparisonResult if compared with previous state, None otherwise.
        """
        self._state_history.append(state)
        
        if len(self._state_history) < 2:
            return None
        
        prev_state = self._state_history[-2]
        result = self._comparator.compare(prev_state, state)
        
        if result.change_percentage > self._change_threshold:
            self._last_significant_change = state
        
        return result
    
    def get_state_history(self) -> List[VisualState]:
        """Get all recorded states."""
        return self._state_history.copy()
    
    def get_last_change(self) -> Optional[VisualState]:
        """Get the last state with significant change."""
        return self._last_significant_change
    
    def clear_history(self) -> None:
        """Clear state history."""
        self._state_history.clear()
        self._last_significant_change = None


def create_visual_state(
    state_id: str,
    pixels: List[int],
    width: int,
    height: int,
    timestamp: Optional[float] = None
) -> VisualState:
    """
    Create a VisualState from pixel data.
    
    Args:
        state_id: Unique identifier for this state.
        pixels: List of pixel values (e.g., RGB triplets).
        width: Image width in pixels.
        height: Image height in pixels.
        timestamp: Optional timestamp (defaults to current time).
        
    Returns:
        VisualState instance.
    """
    import hashlib
    
    if timestamp is None:
        import time
        timestamp = time.time()
    
    pixel_str = "".join(str(p) for p in pixels)
    pixel_hash = hashlib.md5(pixel_str.encode()).hexdigest()[:64]
    
    histogram = _compute_histogram(pixels, bins=32)
    
    return VisualState(
        state_id=state_id,
        timestamp=timestamp,
        width=width,
        height=height,
        pixel_hash=pixel_hash,
        histogram=histogram,
        features={}
    )


def _compute_histogram(pixels: List[int], bins: int = 32) -> List[float]:
    """Compute a simple histogram from pixel values."""
    if not pixels:
        return [0.0] * bins
    
    hist = [0] * bins
    bin_size = 256 / bins
    
    for pixel in pixels:
        normalized = min(255, max(0, pixel))
        bin_idx = min(bins - 1, int(normalized / bin_size))
        hist[bin_idx] += 1
    
    total = sum(hist)
    if total == 0:
        return [0.0] * bins
    
    return [h / total for h in hist]


def find_state_by_id(
    states: List[VisualState],
    state_id: str
) -> Optional[VisualState]:
    """
    Find a state by its ID.
    
    Args:
        states: List of visual states to search.
        state_id: State ID to find.
        
    Returns:
        Matching VisualState or None.
    """
    for state in states:
        if state.state_id == state_id:
            return state
    return None
