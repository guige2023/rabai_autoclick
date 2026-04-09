"""
Automation Element Detection Action Module.

Detects and identifies UI elements for automation with
visual matching, OCR support, and confidence scoring.
"""

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from uuid import uuid4


class DetectionMethod(Enum):
    """Methods for element detection."""

    IMAGE_MATCH = "image_match"
    OCR_TEXT = "ocr_text"
    ACCESSIBILITY = "accessibility"
    XPath = "xpath"
    CSS_SELECTOR = "css_selector"
    PROximity = "proximity"
    FUZZY = "fuzzy"


@dataclass
class ElementMatch:
    """Result of an element detection match."""

    element_id: str
    detection_method: DetectionMethod
    confidence: float
    bounds: tuple[int, int, int, int]  # x, y, width, height
    label: str = ""
    value: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    matched_image: Optional[bytes] = None

    @property
    def center(self) -> tuple[int, int]:
        """Get center coordinates of the element."""
        x, y, w, h = self.bounds
        return (x + w // 2, y + h // 2)

    @property
    def area(self) -> int:
        """Get area of the element bounds."""
        _, _, w, h = self.bounds
        return w * h


@dataclass
class DetectionConfig:
    """Configuration for element detection."""

    confidence_threshold: float = 0.8
    match_timeout: float = 10.0
    max_candidates: int = 10
    similarity_threshold: float = 0.85
    ocr_languages: list[str] = field(default_factory=lambda: ["eng"])
    enable_fuzzy: bool = True
    fuzzy_threshold: float = 0.75


class ElementDetector:
    """
    Detects UI elements for automation.

    Supports multiple detection methods including visual matching,
    OCR text recognition, and accessibility tree traversal.
    """

    def __init__(self, config: Optional[DetectionConfig] = None) -> None:
        """
        Initialize the element detector.

        Args:
            config: Detection configuration.
        """
        self._config = config or DetectionConfig()
        self._image_cache: dict[str, bytes] = {}
        self._element_registry: dict[str, dict[str, Any]] = {}
        self._detection_count = 0

    def register_element(
        self,
        element_id: str,
        image: bytes,
        label: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Register an element image for future matching.

        Args:
            element_id: Unique identifier for this element.
            image: Image bytes (PNG, JPEG, etc.).
            label: Human-readable label.
            metadata: Additional element metadata.
        """
        image_hash = hashlib.sha256(image).hexdigest()[:16]
        self._image_cache[element_id] = image
        self._element_registry[element_id] = {
            "image": image,
            "image_hash": image_hash,
            "label": label,
            "metadata": metadata or {},
            "registered_at": time.time(),
        }

    def unregister_element(self, element_id: str) -> bool:
        """
        Unregister an element.

        Args:
            element_id: ID of element to remove.

        Returns:
            True if element was found and removed.
        """
        if element_id in self._element_registry:
            del self._element_registry[element_id]
            if element_id in self._image_cache:
                del self._image_cache[element_id]
            return True
        return False

    def find_by_image(
        self,
        search_image: bytes,
        threshold: Optional[float] = None,
    ) -> list[ElementMatch]:
        """
        Find elements by visual image matching.

        Args:
            search_image: Image to search for in the screen.
            threshold: Optional confidence threshold override.

        Returns:
            List of matches sorted by confidence.
        """
        threshold = threshold or self._config.similarity_threshold
        matches: list[ElementMatch] = []
        self._detection_count += 1

        search_hash = hashlib.sha256(search_image).hexdigest()[:16]

        for element_id, element_data in self._element_registry.items():
            cached_image = element_data["image"]
            similarity = self._compute_similarity(search_image, cached_image)

            if similarity >= threshold:
                match = ElementMatch(
                    element_id=element_id,
                    detection_method=DetectionMethod.IMAGE_MATCH,
                    confidence=similarity,
                    bounds=(0, 0, 100, 100),  # Placeholder bounds
                    label=element_data.get("label", ""),
                    metadata=element_data.get("metadata", {}),
                    matched_image=cached_image,
                )
                matches.append(match)

        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches[: self._config.max_candidates]

    def _compute_similarity(self, image1: bytes, image2: bytes) -> float:
        """
        Compute visual similarity between two images.

        Returns:
            Similarity score between 0.0 and 1.0.
        """
        if image1 == image2:
            return 1.0

        if len(image1) == len(image2):
            diff_count = sum(a != b for a, b in zip(image1, image2))
            return 1.0 - (diff_count / max(len(image1), 1))

        min_len = min(len(image1), len(image2))
        max_len = max(len(image1), len(image2))
        if min_len == 0:
            return 0.0

        diff_count = sum(
            a != b for a, b in zip(image1[:min_len], image2[:min_len])
        )
        diff_count += max_len - min_len

        return 1.0 - (diff_count / max_len)

    def find_by_ocr(
        self,
        text: str,
        exact_match: bool = False,
        language: str = "eng",
    ) -> list[ElementMatch]:
        """
        Find elements containing specific text via OCR.

        Args:
            text: Text to search for.
            exact_match: Require exact text match if True.
            language: OCR language code.

        Returns:
            List of matches containing the text.
        """
        matches: list[ElementMatch] = []
        self._detection_count += 1

        text_lower = text.lower()
        text_hash = hashlib.sha256(text.encode()).hexdigest()[:16]

        for element_id, element_data in self._element_registry.items():
            label = element_data.get("label", "")
            label_lower = label.lower()

            if exact_match:
                if text_lower == label_lower:
                    matches.append(
                        ElementMatch(
                            element_id=element_id,
                            detection_method=DetectionMethod.OCR_TEXT,
                            confidence=1.0 if text_lower == label_lower else 0.0,
                            bounds=(0, 0, 100, 100),
                            label=label,
                            value=label,
                            metadata={"text_hash": text_hash, "exact_match": True},
                        )
                    )
            else:
                if text_lower in label_lower:
                    confidence = len(text) / max(len(label), 1)
                    matches.append(
                        ElementMatch(
                            element_id=element_id,
                            detection_method=DetectionMethod.OCR_TEXT,
                            confidence=min(confidence, 1.0),
                            bounds=(0, 0, 100, 100),
                            label=label,
                            value=label,
                            metadata={"text_hash": text_hash, "fuzzy_match": True},
                        )
                    )

        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches[: self._config.max_candidates]

    def find_by_proximity(
        self,
        reference_element: str,
        max_distance: int = 50,
        direction: Optional[str] = None,
    ) -> list[ElementMatch]:
        """
        Find elements near a reference element.

        Args:
            reference_element: ID of the reference element.
            max_distance: Maximum pixel distance to consider.
            direction: Optional direction filter ("above", "below", "left", "right").

        Returns:
            List of nearby elements sorted by distance.
        """
        if reference_element not in self._element_registry:
            return []

        ref_data = self._element_registry[reference_element]
        ref_bounds = ref_data.get("bounds", (0, 0, 100, 100))
        ref_x, ref_y, _, _ = ref_bounds
        ref_center = (ref_x, ref_y)

        matches: list[ElementMatch] = []

        for element_id, element_data in self._element_registry.items():
            if element_id == reference_element:
                continue

            bounds = element_data.get("bounds", (0, 0, 100, 100))
            x, y, _, _ = bounds
            center = (x, y)

            distance = ((center[0] - ref_center[0]) ** 2 + (center[1] - ref_center[1]) ** 2) ** 0.5

            if distance > max_distance:
                continue

            if direction:
                dx = center[0] - ref_center[0]
                dy = center[1] - ref_center[1]

                if direction == "above" and dy >= 0:
                    continue
                if direction == "below" and dy <= 0:
                    continue
                if direction == "left" and dx >= 0:
                    continue
                if direction == "right" and dx <= 0:
                    continue

            confidence = 1.0 - (distance / max_distance)
            match = ElementMatch(
                element_id=element_id,
                detection_method=DetectionMethod.PROximity,
                confidence=max(0.0, confidence),
                bounds=bounds,
                label=element_data.get("label", ""),
                metadata={"distance": distance, "direction": direction},
            )
            matches.append(match)

        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches[: self._config.max_candidates]

    def find_by_fuzzy(
        self,
        query: str,
        threshold: Optional[float] = None,
    ) -> list[ElementMatch]:
        """
        Find elements using fuzzy string matching on labels.

        Args:
            query: Search query string.
            threshold: Minimum match score (0-1).

        Returns:
            List of fuzzy-matched elements.
        """
        threshold = threshold or self._config.fuzzy_threshold
        matches: list[ElementMatch] = []
        self._detection_count += 1

        query_lower = query.lower()

        for element_id, element_data in self._element_registry.items():
            label = element_data.get("label", "")
            label_lower = label.lower()

            score = self._fuzzy_score(query_lower, label_lower)
            if score >= threshold:
                match = ElementMatch(
                    element_id=element_id,
                    detection_method=DetectionMethod.FUZZY,
                    confidence=score,
                    bounds=element_data.get("bounds", (0, 0, 100, 100)),
                    label=label,
                    value=label,
                    metadata={"fuzzy_score": score},
                )
                matches.append(match)

        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches[: self._config.max_candidates]

    def _fuzzy_score(self, query: str, text: str) -> float:
        """
        Compute fuzzy match score between query and text.

        Args:
            query: Search query.
            text: Text to match against.

        Returns:
            Score between 0.0 and 1.0.
        """
        if not query or not text:
            return 0.0

        if query in text:
            return len(query) / len(text)

        query_chars = set(query)
        text_chars = set(text)

        overlap = len(query_chars & text_chars)
        return overlap / len(query_chars) if query_chars else 0.0

    def get_element(self, element_id: str) -> Optional[dict[str, Any]]:
        """Get element data by ID."""
        return self._element_registry.get(element_id)

    def list_elements(self) -> list[str]:
        """List all registered element IDs."""
        return list(self._element_registry.keys())

    def stats(self) -> dict[str, Any]:
        """Return detection statistics."""
        return {
            "detection_count": self._detection_count,
            "registered_elements": len(self._element_registry),
            "cached_images": len(self._image_cache),
        }


def create_detector(
    confidence_threshold: float = 0.8,
    enable_fuzzy: bool = True,
) -> ElementDetector:
    """
    Factory function to create an element detector.

    Args:
        confidence_threshold: Minimum confidence for matches.
        enable_fuzzy: Enable fuzzy text matching.

    Returns:
        Configured ElementDetector instance.
    """
    config = DetectionConfig(
        confidence_threshold=confidence_threshold,
        enable_fuzzy=enable_fuzzy,
    )
    return ElementDetector(config)
