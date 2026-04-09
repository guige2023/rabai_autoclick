"""
Automation Element Detection Action Module

Detects and matches UI elements for automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
import time
import hashlib


class MatchStrategy(Enum):
    """Element matching strategy."""
    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"
    FUZZY = "fuzzy"
    INDEX = "index"


@dataclass
class ElementCriteria:
    """Criteria for element matching."""
    text: Optional[str] = None
    text_pattern: Optional[str] = None
    element_type: Optional[str] = None  # button, input, link, etc.
    id: Optional[str] = None
    css_class: Optional[str] = None
    xpath: Optional[str] = None
    role: Optional[str] = None
    aria_label: Optional[str] = None
    placeholder: Optional[str] = None
    attributes: Optional[Dict[str, str]] = None
    index: Optional[int] = None  # 0-based index of matching elements


@dataclass
class DetectedElement:
    """Detected UI element."""
    id: str
    element_type: str
    text: str
    bounds: Tuple[int, int, int, int]  # x, y, width, height
    attributes: Dict[str, str]
    confidence: float  # 0.0 to 1.0
    matched_criteria: List[str]


@dataclass
class DetectionResult:
    """Result of element detection."""
    success: bool
    element: Optional[DetectedElement] = None
    candidates: List[DetectedElement] = field(default_factory=list)
    error: Optional[str] = None
    detection_time_ms: float = 0.0


class FuzzyMatcher:
    """Fuzzy string matching for element text."""

    @staticmethod
    def similarity(s1: str, s2: str) -> float:
        """Calculate similarity between two strings (0.0 to 1.0)."""
        if not s1 or not s2:
            return 0.0
        if s1 == s2:
            return 1.0

        # Simple Levenshtein-based similarity
        len1, len2 = len(s1), len(s2)
        max_len = max(len1, len2)
        if max_len == 0:
            return 1.0

        # Quick check: if lengths differ too much, low similarity
        if abs(len1 - len2) > max_len * 0.5:
            return 0.0

        # Create distance matrix
        distances = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        for i in range(len1 + 1):
            distances[i][0] = i
        for j in range(len2 + 1):
            distances[0][j] = j

        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                distances[i][j] = min(
                    distances[i-1][j] + 1,
                    distances[i][j-1] + 1,
                    distances[i-1][j-1] + cost
                )

        distance = distances[len1][len2]
        return 1.0 - (distance / max_len)

    @classmethod
    def contains_fuzzy(cls, text: str, pattern: str, threshold: float = 0.7) -> float:
        """Check if pattern is contained in text with fuzzy matching."""
        text_lower = text.lower()
        pattern_lower = pattern.lower()

        if pattern_lower in text_lower:
            return 1.0

        # Try matching pattern as subsequence
        p_idx = 0
        for char in text_lower:
            if p_idx < len(pattern_lower) and char == pattern_lower[p_idx]:
                p_idx += 1

        if p_idx == len(pattern_lower):
            return threshold

        # Word-by-word fuzzy match
        text_words = text_lower.split()
        pattern_words = pattern_lower.split()

        matched = 0
        for pw in pattern_words:
            best_sim = 0
            for tw in text_words:
                sim = cls.similarity(tw, pw)
                if sim > best_sim:
                    best_sim = sim
            if best_sim >= threshold:
                matched += 1

        return matched / len(pattern_words) if pattern_words else 0.0


class ElementMatcher:
    """Matches elements against criteria."""

    def __init__(self):
        self.fuzzy_matcher = FuzzyMatcher()

    def matches(
        self,
        element: DetectedElement,
        criteria: ElementCriteria,
        strategy: MatchStrategy = MatchStrategy.CONTAINS
    ) -> Tuple[bool, float, List[str]]:
        """
        Check if element matches criteria.

        Returns:
            Tuple of (matches, confidence, list_of_matched_criteria)
        """
        if not criteria:
            return True, 1.0, []

        matched = []
        confidences = []

        # Check text
        if criteria.text:
            if strategy == MatchStrategy.EXACT:
                if element.text == criteria.text:
                    matched.append("text")
                    confidences.append(1.0)
            elif strategy == MatchStrategy.CONTAINS:
                if criteria.text.lower() in element.text.lower():
                    matched.append("text")
                    confidences.append(0.9)
            elif strategy == MatchStrategy.REGEX:
                if re.search(criteria.text, element.text):
                    matched.append("text")
                    confidences.append(0.95)
            elif strategy == MatchStrategy.FUZZY:
                sim = self.fuzzy_matcher.similarity(element.text, criteria.text)
                if sim >= 0.7:
                    matched.append("text")
                    confidences.append(sim)

        # Check text pattern
        if criteria.text_pattern:
            if re.search(criteria.text_pattern, element.text):
                matched.append("text_pattern")
                confidences.append(0.95)

        # Check element type
        if criteria.element_type:
            if element.element_type.lower() == criteria.element_type.lower():
                matched.append("element_type")
                confidences.append(1.0)

        # Check id
        if criteria.id:
            elem_id = element.attributes.get("id", "")
            if elem_id == criteria.id:
                matched.append("id")
                confidences.append(1.0)

        # Check CSS class
        if criteria.css_class:
            classes = element.attributes.get("class", "").split()
            if criteria.css_class in classes:
                matched.append("css_class")
                confidences.append(0.95)

        # Check role
        if criteria.role:
            role = element.attributes.get("role", "")
            if role.lower() == criteria.role.lower():
                matched.append("role")
                confidences.append(1.0)

        # Check aria_label
        if criteria.aria_label:
            aria = element.attributes.get("aria-label", "")
            if criteria.aria_label.lower() in aria.lower():
                matched.append("aria_label")
                confidences.append(0.95)

        # Check placeholder
        if criteria.placeholder:
            ph = element.attributes.get("placeholder", "")
            if criteria.placeholder.lower() in ph.lower():
                matched.append("placeholder")
                confidences.append(0.9)

        # Check custom attributes
        if criteria.attributes:
            for key, value in criteria.attributes.items():
                if element.attributes.get(key) != value:
                    return False, 0.0, []
            matched.append("attributes")
            confidences.append(0.95)

        success = len(matched) > 0
        confidence = sum(confidences) / len(confidences) if confidences else 0.0

        return success, confidence, matched


class ElementDetector:
    """
    Simulated element detector for UI automation.

    In production, this would integrate with actual UI automation
    frameworks like Playwright, Appium, or accessibility APIs.
    """

    def __init__(self):
        self.elements: List[DetectedElement] = []
        self.matcher = ElementMatcher()
        self._initialized = False

    def load_elements(self, elements: List[Dict[str, Any]]) -> None:
        """Load elements from accessibility tree or DOM."""
        self.elements = []
        for elem_data in elements:
            bounds = elem_data.get("bounds", (0, 0, 100, 100))
            attrs = elem_data.get("attributes", {})

            elem = DetectedElement(
                id=elem_data.get("id", hashlib.md5(str(bounds).encode()).hexdigest()[:8]),
                element_type=elem_data.get("type", "unknown"),
                text=elem_data.get("text", ""),
                bounds=bounds,
                attributes=attrs,
                confidence=1.0,
                matched_criteria=[]
            )
            self.elements.append(elem)
        self._initialized = True

    def add_mock_element(
        self,
        element_type: str,
        text: str,
        bounds: Tuple[int, int, int, int],
        attributes: Optional[Dict[str, str]] = None
    ) -> None:
        """Add a mock element for testing."""
        elem_id = hashlib.md5(f"{text}{bounds}".encode()).hexdigest()[:8]
        elem = DetectedElement(
            id=elem_id,
            element_type=element_type,
            text=text,
            bounds=bounds,
            attributes=attributes or {},
            confidence=1.0,
            matched_criteria=[]
        )
        self.elements.append(elem)
        self._initialized = True

    def find_all(
        self,
        criteria: ElementCriteria,
        strategy: MatchStrategy = MatchStrategy.CONTAINS
    ) -> List[DetectedElement]:
        """Find all elements matching criteria."""
        results = []

        for elem in self.elements:
            matches, confidence, _ = self.matcher.matches(elem, criteria, strategy)
            if matches:
                elem.confidence = confidence
                results.append(elem)

        # Sort by confidence
        results.sort(key=lambda e: e.confidence, reverse=True)
        return results

    def find_first(
        self,
        criteria: ElementCriteria,
        strategy: MatchStrategy = MatchStrategy.CONTAINS
    ) -> Optional[DetectedElement]:
        """Find first element matching criteria."""
        results = self.find_all(criteria, strategy)
        return results[0] if results else None


class AutomationElementDetectionAction:
    """
    Detects and matches UI elements for automation workflows.

    Supports multiple matching strategies including exact, contains,
    regex, fuzzy, and index-based matching.

    Example:
        detector = AutomationElementDetectionAction()
        detector.add_mock_element("button", "Submit", (100, 200, 80, 40))
        detector.add_mock_element("input", "Username", (100, 250, 200, 30))

        result = detector.detect(ElementCriteria(text="Submit"))
        if result.success:
            elem = result.element
            print(f"Found at ({elem.bounds[0]}, {elem.bounds[1]})")

        # Find all buttons
        buttons = detector.find_all(ElementCriteria(element_type="button"))
    """

    def __init__(self):
        """Initialize element detection action."""
        self.detector = ElementDetector()
        self.default_strategy = MatchStrategy.CONTAINS

    def set_strategy(self, strategy: MatchStrategy) -> None:
        """Set default matching strategy."""
        self.default_strategy = strategy

    def detect(
        self,
        criteria: ElementCriteria,
        strategy: Optional[MatchStrategy] = None,
        timeout_ms: int = 5000
    ) -> DetectionResult:
        """
        Detect element matching criteria.

        Args:
            criteria: Element matching criteria
            strategy: Matching strategy to use
            timeout_ms: Detection timeout

        Returns:
            DetectionResult with matched element or candidates
        """
        start = time.time()
        strat = strategy or self.default_strategy

        # Find candidates
        candidates = self.detector.find_all(criteria, strat)

        # Handle index criteria
        if criteria.index is not None and criteria.index < len(candidates):
            element = candidates[criteria.index]
            element.matched_criteria = ["index"]
            return DetectionResult(
                success=True,
                element=element,
                candidates=candidates,
                detection_time_ms=(time.time() - start) * 1000
            )

        if candidates:
            best = candidates[0]
            return DetectionResult(
                success=True,
                element=best,
                candidates=candidates,
                detection_time_ms=(time.time() - start) * 1000
            )

        return DetectionResult(
            success=False,
            error="No matching element found",
            candidates=[],
            detection_time_ms=(time.time() - start) * 1000
        )

    def detect_nearest(
        self,
        from_point: Tuple[int, int],
        criteria: Optional[ElementCriteria] = None
    ) -> Optional[DetectedElement]:
        """
        Find nearest element to a point.

        Args:
            from_point: (x, y) coordinates
            criteria: Optional criteria to filter elements

        Returns:
            Nearest DetectedElement or None
        """
        candidates = self.detector.elements
        if criteria:
            candidates = self.detector.find_all(criteria)

        if not candidates:
            return None

        fx, fy = from_point
        nearest = None
        min_dist = float("inf")

        for elem in candidates:
            ex, ey, ew, eh = elem.bounds
            # Distance to element center
            dx = ex + ew / 2 - fx
            dy = ey + eh / 2 - fy
            dist = (dx * dx + dy * dy) ** 0.5
            if dist < min_dist:
                min_dist = dist
                nearest = elem

        return nearest

    def wait_for(
        self,
        criteria: ElementCriteria,
        timeout_ms: int = 10000,
        poll_interval_ms: int = 500,
        strategy: Optional[MatchStrategy] = None
    ) -> DetectionResult:
        """
        Wait for element to appear.

        Args:
            criteria: Element matching criteria
            timeout_ms: Maximum wait time
            poll_interval_ms: Polling interval
            strategy: Matching strategy

        Returns:
            DetectionResult when element found or timeout
        """
        start = time.time()
        deadline = start + timeout_ms / 1000

        while time.time() < deadline:
            result = self.detect(criteria, strategy)
            if result.success:
                return result
            time.sleep(poll_interval_ms / 1000)

        return DetectionResult(
            success=False,
            error="Timeout waiting for element",
            detection_time_ms=timeout_ms
        )

    def get_element_at_position(
        self,
        x: int,
        y: int
    ) -> Optional[DetectedElement]:
        """Get element at specific coordinates."""
        for elem in self.detector.elements:
            ex, ey, ew, eh = elem.bounds
            if ex <= x <= ex + ew and ey <= y <= ey + eh:
                return elem
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get detection statistics."""
        return {
            "total_elements": len(self.detector.elements),
            "default_strategy": self.default_strategy.value,
            "initialized": self.detector._initialized
        }
