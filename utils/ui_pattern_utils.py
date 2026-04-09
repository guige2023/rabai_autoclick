"""UI pattern matching utilities for automation.

Provides utilities for pattern matching in UI elements,
image recognition support, and template matching.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple


@dataclass
class PatternMatch:
    """Result of a pattern match."""
    x: float
    y: float
    width: float
    height: float
    confidence: float
    matched: bool = True


@dataclass
class Pattern:
    """A pattern template for matching."""
    name: str
    width: float
    height: float
    data: List[List[float]]
    threshold: float = 0.8


class PatternMatcher:
    """Matches patterns within UI elements.
    
    Provides template matching and pattern
    recognition for UI automation.
    """
    
    def __init__(self, threshold: float = 0.8) -> None:
        """Initialize the pattern matcher.
        
        Args:
            threshold: Match confidence threshold.
        """
        self.threshold = threshold
        self._patterns: Dict[str, Pattern] = {}
    
    def register_pattern(
        self,
        name: str,
        width: float,
        height: float,
        data: List[List[float]]
    ) -> None:
        """Register a pattern for matching.
        
        Args:
            name: Pattern name.
            width: Pattern width.
            height: Pattern height.
            data: Pattern data matrix.
        """
        pattern = Pattern(
            name=name,
            width=width,
            height=height,
            data=data,
            threshold=self.threshold
        )
        self._patterns[name] = pattern
    
    def match(
        self,
        pattern_name: str,
        search_area: List[List[float]],
        x: float = 0.0,
        y: float = 0.0
    ) -> Optional[PatternMatch]:
        """Match a registered pattern.
        
        Args:
            pattern_name: Name of pattern to match.
            search_area: Search area data.
            x: X position of search area.
            y: Y position of search area.
            
        Returns:
            PatternMatch or None.
        """
        pattern = self._patterns.get(pattern_name)
        if not pattern:
            return None
        
        confidence = self._calculate_match(pattern, search_area)
        
        if confidence >= pattern.threshold:
            return PatternMatch(
                x=x,
                y=y,
                width=pattern.width,
                height=pattern.height,
                confidence=confidence,
                matched=True
            )
        
        return None
    
    def _calculate_match(
        self,
        pattern: Pattern,
        search_area: List[List[float]]
    ) -> float:
        """Calculate match confidence.
        
        Args:
            pattern: Pattern to match.
            search_area: Search area data.
            
        Returns:
            Confidence score.
        """
        if not pattern.data or not search_area:
            return 0.0
        
        pattern_flat = [v for row in pattern.data for v in row]
        search_flat = [v for row in search_area for v in row]
        
        if len(pattern_flat) != len(search_flat):
            return 0.0
        
        dot_product = sum(p * s for p, s in zip(pattern_flat, search_flat))
        pattern_mag = math.sqrt(sum(p * p for p in pattern_flat))
        search_mag = math.sqrt(sum(s * s for s in search_flat))
        
        if pattern_mag == 0 or search_mag == 0:
            return 0.0
        
        return dot_product / (pattern_mag * search_mag)
    
    def find_best_match(
        self,
        pattern_name: str,
        search_areas: List[Tuple[float, float, List[List[float]]]]
    ) -> Optional[PatternMatch]:
        """Find best match among multiple areas.
        
        Args:
            pattern_name: Name of pattern to match.
            search_areas: List of (x, y, data) tuples.
            
        Returns:
            Best PatternMatch or None.
        """
        best_match: Optional[PatternMatch] = None
        best_confidence = 0.0
        
        for x, y, data in search_areas:
            match = self.match(pattern_name, data, x, y)
            if match and match.confidence > best_confidence:
                best_confidence = match.confidence
                best_match = match
        
        return best_match


class ImageRegionMatcher:
    """Matches image regions using feature detection.
    
    Provides utilities for finding image regions
    and feature-based matching.
    """
    
    def __init__(self) -> None:
        """Initialize the image region matcher."""
        self._regions: List[Tuple[str, float, float, float, float]] = []
    
    def add_region(
        self,
        region_id: str,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> None:
        """Add a region to match.
        
        Args:
            region_id: Region identifier.
            x: X position.
            y: Y position.
            width: Region width.
            height: Region height.
        """
        self._regions.append((region_id, x, y, width, height))
    
    def find_at_point(
        self,
        x: float,
        y: float
    ) -> Optional[str]:
        """Find region containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            Region ID or None.
        """
        for region_id, rx, ry, rw, rh in reversed(self._regions):
            if rx <= x <= rx + rw and ry <= y <= ry + rh:
                return region_id
        return None
    
    def find_in_area(
        self,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> List[str]:
        """Find all regions within an area.
        
        Args:
            x: Area X.
            y: Area Y.
            width: Area width.
            height: Area height.
            
        Returns:
            List of region IDs.
        """
        matches = []
        area_right = x + width
        area_bottom = y + height
        
        for region_id, rx, ry, rw, rh in self._regions:
            region_right = rx + rw
            region_bottom = ry + rh
            
            if (x <= rx < area_right and y <= ry < area_bottom and
                x < region_right <= area_right and
                y < region_bottom <= area_bottom):
                matches.append(region_id)
        
        return matches


def calculate_similarity(
    data1: List[List[float]],
    data2: List[List[float]]
) -> float:
    """Calculate similarity between two data matrices.
    
    Args:
        data1: First matrix.
        data2: Second matrix.
        
    Returns:
        Similarity score.
    """
    if len(data1) != len(data2) or len(data1[0]) != len(data2[0]):
        return 0.0
    
    flat1 = [v for row in data1 for v in row]
    flat2 = [v for row in data2 for v in row]
    
    dot_product = sum(a * b for a, b in zip(flat1, flat2))
    mag1 = math.sqrt(sum(a * a for a in flat1))
    mag2 = math.sqrt(sum(b * b for b in flat2))
    
    if mag1 == 0 or mag2 == 0:
        return 0.0
    
    return dot_product / (mag1 * mag2)
