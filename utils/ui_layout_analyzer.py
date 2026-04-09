"""UI layout analyzer for analyzing and describing UI layouts."""
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class LayoutRegion:
    """A region in the layout."""
    x: int
    y: int
    width: int
    height: int
    elements: List[str]
    density: float = 0.0


@dataclass
class LayoutAnalysis:
    """Complete layout analysis result."""
    total_elements: int
    regions: List[LayoutRegion]
    grid_cols: int
    grid_rows: int
    dominant_direction: str
    alignment_score: float


class UILayoutAnalyzer:
    """Analyzes UI layouts to understand structure and organization.
    
    Examines element positions and sizes to identify layout patterns,
    regions, grids, and alignment characteristics.
    
    Example:
        analyzer = UILayoutAnalyzer()
        elements = [{"id": "a", "x": 0, "y": 0, "w": 100, "h": 50}, ...]
        analysis = analyzer.analyze(elements)
        print(f"Grid: {analysis.grid_cols}x{analysis.grid_rows}")
    """

    def __init__(self, grid_threshold: float = 20.0) -> None:
        self._grid_threshold = grid_threshold

    def analyze(self, elements: List[Dict]) -> LayoutAnalysis:
        """Analyze a list of elements and return layout information."""
        if not elements:
            return LayoutAnalysis(
                total_elements=0,
                regions=[],
                grid_cols=0,
                grid_rows=0,
                dominant_direction="none",
                alignment_score=0.0,
            )
        
        x_positions = [e.get("x", 0) for e in elements]
        y_positions = [e.get("y", 0) for e in elements]
        
        grid_cols = self._detect_grid_spacing(x_positions)
        grid_rows = self._detect_grid_spacing(y_positions)
        
        dominant = self._detect_dominant_direction(elements)
        alignment = self._calculate_alignment_score(elements)
        regions = self._identify_regions(elements)
        
        return LayoutAnalysis(
            total_elements=len(elements),
            regions=regions,
            grid_cols=grid_cols,
            grid_rows=grid_rows,
            dominant_direction=dominant,
            alignment_score=alignment,
        )

    def _detect_grid_spacing(self, positions: List[int]) -> int:
        """Detect number of grid columns or rows from position alignment."""
        unique_positions = sorted(set(positions))
        if len(unique_positions) <= 1:
            return 1
        
        gaps = []
        for i in range(1, len(unique_positions)):
            gap = unique_positions[i] - unique_positions[i-1]
            if gap <= self._grid_threshold:
                gaps.append(gap)
        
        avg_gap = sum(gaps) / len(gaps) if gaps else 0
        
        if avg_gap == 0:
            return len(unique_positions)
        
        total_range = unique_positions[-1] - unique_positions[0]
        estimated = max(1, int(round(total_range / avg_gap)) + 1)
        
        return min(estimated, len(unique_positions))

    def _detect_dominant_direction(self, elements: List[Dict]) -> str:
        """Detect if layout is primarily horizontal or vertical."""
        widths = [e.get("w", 0) for e in elements]
        heights = [e.get("h", 0) for e in elements]
        
        total_width = sum(widths)
        total_height = sum(heights)
        
        if total_width > total_height * 1.5:
            return "horizontal"
        elif total_height > total_width * 1.5:
            return "vertical"
        return "mixed"

    def _calculate_alignment_score(self, elements: List[Dict]) -> float:
        """Calculate how well elements are aligned to grid."""
        if len(elements) < 2:
            return 1.0
        
        x_positions = [e.get("x", 0) for e in elements]
        y_positions = [e.get("y", 0) for e in elements]
        
        x_variance = self._variance(x_positions)
        y_variance = self._variance(y_positions)
        
        x_range = max(x_positions) - min(x_positions) if x_positions else 1
        y_range = max(y_positions) - min(y_positions) if y_positions else 1
        
        x_score = max(0, 1 - x_variance / (x_range * 0.5 + 1))
        y_score = max(0, 1 - y_variance / (y_range * 0.5 + 1))
        
        return (x_score + y_score) / 2

    def _identify_regions(self, elements: List[Dict]) -> List[LayoutRegion]:
        """Identify distinct regions in the layout."""
        if not elements:
            return []
        
        regions: List[LayoutRegion] = []
        
        for i, elem in enumerate(elements):
            x = elem.get("x", 0)
            y = elem.get("y", 0)
            w = elem.get("w", 100)
            h = elem.get("h", 50)
            elem_id = elem.get("id", f"elem_{i}")
            
            regions.append(LayoutRegion(
                x=x, y=y, width=w, height=h,
                elements=[elem_id],
                density=1.0,
            ))
        
        return regions

    def _variance(self, values: List[float]) -> float:
        """Calculate variance of values."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((v - mean) ** 2 for v in values) / len(values)
