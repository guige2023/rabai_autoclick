"""Screen content extraction utilities for UI automation.

Provides utilities for extracting and parsing content from screen regions,
including text, UI elements, and visual information.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ContentType(Enum):
    """Types of content that can be extracted."""
    TEXT = auto()
    ELEMENT = auto()
    IMAGE = auto()
    ICON = auto()
    LINK = auto()
    TABLE = auto()
    LIST = auto()
    FORM = auto()


@dataclass
class ExtractedText:
    """A block of extracted text from a screen region.

    Attributes:
        text: The actual text content.
        bounds: Bounding box as (x, y, width, height).
        font_size: Estimated font size in pixels.
        font_weight: Estimated font weight.
        color: Text color as hex string.
        background_color: Background color as hex string.
        is_bold: Whether text appears bold.
        is_italic: Whether text appears italic.
        language: Detected language code.
        confidence: Extraction confidence (0.0-1.0).
    """
    text: str
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    font_size: float = 0.0
    font_weight: float = 400.0
    color: str = "#000000"
    background_color: str = "#FFFFFF"
    is_bold: bool = False
    is_italic: bool = False
    language: str = "en"
    confidence: float = 1.0

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    def is_empty(self) -> bool:
        """Return True if text is empty or whitespace only."""
        return not self.text or self.text.strip() == ""


@dataclass
class ExtractedElement:
    """A UI element extracted from a screen region.

    Attributes:
        role: Element role/kind (button, textfield, etc.).
        label: Element label or accessible name.
        value: Current value if applicable.
        bounds: Bounding box as (x, y, width, height).
        states: Set of active states (focused, selected, etc.).
        children: Child elements.
        metadata: Additional extracted properties.
    """
    role: str
    label: str = ""
    value: Any = None
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    states: set[str] = field(default_factory=set)
    children: list[ExtractedElement] = field(default_factory=list)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    def has_state(self, state: str) -> bool:
        """Check if element has a given state."""
        return state.lower() in {s.lower() for s in self.states}

    def get_text_content(self) -> str:
        """Get all text content recursively."""
        parts = []
        if self.label:
            parts.append(self.label)
        if self.value and isinstance(self.value, str):
            parts.append(self.value)
        for child in self.children:
            parts.append(child.get_text_content())
        return " ".join(parts)


@dataclass
class ScreenRegionContent:
    """Content extracted from a screen region.

    Attributes:
        region_id: Identifier for the source region.
        texts: List of extracted text blocks.
        elements: List of extracted UI elements.
        image_data: Raw image data if captured.
        ocr_text: Full OCR text (all texts joined).
        language_hint: Hint for language detection.
    """
    region_id: str
    texts: list[ExtractedText] = field(default_factory=list)
    elements: list[ExtractedElement] = field(default_factory=list)
    image_data: Optional[bytes] = None
    ocr_text: str = ""
    language_hint: str = "en"

    def add_text(self, text: ExtractedText) -> None:
        """Add an extracted text block."""
        self.texts.append(text)

    def add_element(self, element: ExtractedElement) -> None:
        """Add an extracted element."""
        self.elements.append(element)

    def get_all_text(self) -> str:
        """Return all text blocks joined by newlines."""
        return "\n".join(t.text for t in self.texts if not t.is_empty())

    def get_text_at_point(self, x: float, y: float) -> Optional[ExtractedText]:
        """Find the text block containing a given point."""
        for text in self.texts:
            bx, by, bw, bh = text.bounds
            if bx <= x < bx + bw and by <= y < by + bh:
                return text
        return None

    def get_element_at_point(self, x: float, y: float) -> Optional[ExtractedElement]:
        """Find the smallest element containing a given point."""
        candidates = [
            e for e in self.elements
            if e.x <= x < e.x + e.width and e.y <= y < e.y + e.height
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda e: e.width * e.height)

    def find_text(self, query: str, exact: bool = False) -> list[ExtractedText]:
        """Find text blocks matching a query."""
        query_lower = query.lower()
        if exact:
            return [t for t in self.texts if t.text == query]
        return [
            t for t in self.texts
            if query_lower in t.text.lower()
        ]

    def find_elements_by_role(self, role: str) -> list[ExtractedElement]:
        """Find elements by role."""
        role_lower = role.lower()
        return [
            e for e in self.elements
            if e.role.lower() == role_lower
        ]

    def find_elements_by_label(
        self, label: str, exact: bool = False
    ) -> list[ExtractedElement]:
        """Find elements by label."""
        label_lower = label.lower()
        if exact:
            return [e for e in self.elements if e.label == label]
        return [
            e for e in self.elements
            if label_lower in e.label.lower()
        ]

    def get_buttons(self) -> list[ExtractedElement]:
        """Get all button elements."""
        return self.find_elements_by_role("button")

    def get_textfields(self) -> list[ExtractedElement]:
        """Get all text field elements."""
        return self.find_elements_by_role("textfield") + \
            self.find_elements_by_role("textbox")

    def get_links(self) -> list[ExtractedElement]:
        """Get all link elements."""
        return self.find_elements_by_role("link")


class ScreenContentExtractor:
    """Extracts content from screen regions.

    Provides a unified interface for OCR, element detection,
    and content parsing from captured screen regions.
    """

    def __init__(self) -> None:
        """Initialize extractor."""
        self._preprocessors: list[Callable[[bytes], bytes]] = []
        self._postprocessors: list[Callable[[ScreenRegionContent], None]] = []

    def add_preprocessor(self, fn: Callable[[bytes], bytes]) -> None:
        """Add an image preprocessing function."""
        self._preprocessors.append(fn)

    def add_postprocessor(self, fn: Callable[[ScreenRegionContent], None]) -> None:
        """Add a post-processing function."""
        self._postprocessors.append(fn)

    def extract_from_image(
        self,
        image_data: bytes,
        region_id: str = "",
        use_ocr: bool = True,
        detect_elements: bool = True,
    ) -> ScreenRegionContent:
        """Extract content from an image.

        Args:
            image_data: Raw image bytes.
            region_id: Identifier for this region.
            use_ocr: Whether to perform OCR text extraction.
            detect_elements: Whether to detect UI elements.

        Returns:
            ScreenRegionContent with extracted texts and elements.
        """
        processed = image_data
        for preprocessor in self._preprocessors:
            processed = preprocessor(processed)

        content = ScreenRegionContent(region_id=region_id, image_data=processed)

        if use_ocr:
            self._extract_text(content)

        if detect_elements:
            self._detect_elements(content)

        for postprocessor in self._postprocessors:
            postprocessor(content)

        return content

    def _extract_text(self, content: ScreenRegionContent) -> None:
        """Internal OCR text extraction (placeholder for actual OCR)."""
        pass

    def _detect_elements(self, content: ScreenRegionContent) -> None:
        """Internal element detection (placeholder for actual detection)."""
        pass

    def merge_contents(
        self, contents: list[ScreenRegionContent]
    ) -> ScreenRegionContent:
        """Merge multiple region contents into one."""
        merged = ScreenRegionContent(region_id="merged")
        for c in contents:
            merged.texts.extend(c.texts)
            merged.elements.extend(c.elements)
        merged.ocr_text = merged.get_all_text()
        return merged


# Utility functions
def filter_by_bounds(
    texts: list[ExtractedText],
    min_x: float = 0,
    min_y: float = 0,
    max_x: float = float("inf"),
    max_y: float = float("inf"),
) -> list[ExtractedText]:
    """Filter text blocks by bounding box constraints."""
    return [
        t for t in texts
        if t.x >= min_x and t.y >= min_y
        and t.x + t.width <= max_x
        and t.y + t.height <= max_y
    ]


def sort_by_reading_order(
    texts: list[ExtractedText],
) -> list[ExtractedText]:
    """Sort text blocks in reading order (top-to-bottom, left-to-right)."""
    return sorted(
        texts,
        key=lambda t: (int(t.y / 20) * 1000 + int(t.x / 20), t.y, t.x),
    )
