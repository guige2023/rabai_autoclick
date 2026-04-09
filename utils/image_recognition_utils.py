"""
Image Recognition Utilities for UI Automation.

This module provides utilities for image-based element recognition,
template matching, and visual search in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Any
from collections import defaultdict


class MatchMethod(Enum):
    """Template matching methods."""
    TM_CCOEFF_NORMED = auto()
    TM_CCORR_NORMED = auto()
    TM_SQDIFF = auto()


@dataclass
class ImageMatch:
    """
    Result of an image template match.
    
    Attributes:
        found: Whether the template was found
        confidence: Match confidence (0.0 - 1.0)
        x: X coordinate of match center
        y: Y coordinate of match center
        width: Match region width
        height: Match region height
    """
    found: bool
    confidence: float = 0.0
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0
    search_time_ms: float = 0.0
    
    @property
    def center(self) -> tuple[float, float]:
        """Get the center point of the match."""
        return (self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """Get the bounding box (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)


@dataclass
class Template:
    """
    Image template for matching.
    
    Attributes:
        template_id: Unique identifier
        name: Template name
        image_data: Template image bytes
        width: Template width
        height: Template height
        hash: Image hash for quick comparison
    """
    template_id: str
    name: str
    image_data: bytes
    width: int
    height: int
    hash: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.hash and self.image_data:
            self.hash = hashlib.sha256(self.image_data).hexdigest()


class ImageRecognizer:
    """
    Image-based recognition for UI elements.
    
    Example:
        recognizer = ImageRecognizer()
        match = recognizer.find_template(template, screenshot)
    """
    
    def __init__(self):
        self._templates: dict[str, Template] = {}
        self._match_method = MatchMethod.TM_CCOEFF_NORMED
        self._default_threshold = 0.8
    
    def register_template(
        self,
        name: str,
        image_data: bytes,
        width: int,
        height: int
    ) -> str:
        """
        Register an image template.
        
        Args:
            name: Template name
            image_data: Template image bytes
            width: Template width
            height: Template height
            
        Returns:
            Template ID
        """
        import uuid
        template_id = str(uuid.uuid4())
        
        self._templates[template_id] = Template(
            template_id=template_id,
            name=name,
            image_data=image_data,
            width=width,
            height=height
        )
        
        return template_id
    
    def find_template(
        self,
        template_id: str,
        screenshot: bytes,
        screenshot_width: int,
        screenshot_height: int,
        threshold: Optional[float] = None
    ) -> ImageMatch:
        """
        Find a registered template in a screenshot.
        
        Args:
            template_id: Template identifier
            screenshot: Screenshot image bytes
            screenshot_width: Screenshot width
            screenshot_height: Screenshot height
            threshold: Match threshold (0.0 - 1.0)
            
        Returns:
            ImageMatch result
        """
        start_time = time.time()
        
        template = self._templates.get(template_id)
        if not template:
            return ImageMatch(
                found=False,
                search_time_ms=(time.time() - start_time) * 1000,
                confidence=0.0
            )
        
        threshold = threshold or self._default_threshold
        
        # Placeholder - actual implementation would use OpenCV or similar
        # For now, return a "not found" result
        return ImageMatch(
            found=False,
            search_time_ms=(time.time() - start_time) * 1000,
            confidence=0.0
        )
    
    def find_all_template(
        self,
        template_id: str,
        screenshot: bytes,
        screenshot_width: int,
        screenshot_height: int,
        threshold: Optional[float] = None
    ) -> list[ImageMatch]:
        """
        Find all occurrences of a template in a screenshot.
        
        Args:
            template_id: Template identifier
            screenshot: Screenshot image bytes
            screenshot_width: Screenshot width
            screenshot_height: Screenshot height
            threshold: Match threshold
            
        Returns:
            List of ImageMatch results
        """
        # Placeholder
        return []
    
    def get_template(self, template_id: str) -> Optional[Template]:
        """Get a registered template."""
        return self._templates.get(template_id)
    
    def list_templates(self) -> list[Template]:
        """List all registered templates."""
        return list(self._templates.values())
    
    def remove_template(self, template_id: str) -> bool:
        """Remove a registered template."""
        if template_id in self._templates:
            del self._templates[template_id]
            return True
        return False


class VisualSearcher:
    """
    Visual search for finding elements by visual characteristics.
    """
    
    def __init__(self):
        self._color_index: dict[str, list[str]] = defaultdict(list)  # color -> template_ids
        self._recognizer = ImageRecognizer()
    
    def index_template(self, template: Template, dominant_colors: list[str]) -> None:
        """
        Index a template by its dominant colors for faster search.
        
        Args:
            template: Template to index
            dominant_colors: List of dominant color hex values
        """
        for color in dominant_colors:
            self._color_index[color].append(template.template_id)
    
    def find_by_color(
        self,
        color: str,
        screenshot: bytes,
        screenshot_width: int,
        screenshot_height: int
    ) -> list[ImageMatch]:
        """
        Find templates matching a specific color.
        
        Args:
            color: Color hex value
            screenshot: Screenshot to search
            screenshot_width: Screenshot width
            screenshot_height: Screenshot height
            
        Returns:
            List of matching ImageMatch results
        """
        # Find templates with this color
        template_ids = self._color_index.get(color, [])
        
        matches = []
        for template_id in template_ids:
            match = self._recognizer.find_template(
                template_id,
                screenshot,
                screenshot_width,
                screenshot_height
            )
            if match.found:
                matches.append(match)
        
        return matches


class OCRHelper:
    """
    Optical Character Recognition helper.
    
    Example:
        ocr = OCRHelper()
        text = ocr.extract_text(screenshot, region=(x, y, w, h))
    """
    
    def __init__(self):
        self._initialized = False
    
    def initialize(self) -> bool:
        """Initialize the OCR engine."""
        # Placeholder - actual would initialize Tesseract or similar
        self._initialized = True
        return True
    
    def extract_text(
        self,
        image_data: bytes,
        region: Optional[tuple[int, int, int, int]] = None,
        languages: Optional[list[str]] = None
    ) -> str:
        """
        Extract text from an image.
        
        Args:
            image_data: Image bytes
            region: Optional (x, y, width, height) region to extract from
            languages: Optional list of languages to use
            
        Returns:
            Extracted text
        """
        if not self._initialized:
            self.initialize()
        
        # Placeholder - actual OCR implementation
        return ""
    
    def extract_text_regions(
        self,
        image_data: bytes
    ) -> list[tuple[str, tuple[int, int, int, int]]]:
        """
        Extract all text regions with their bounding boxes.
        
        Returns:
            List of (text, bounding_box) tuples
        """
        if not self._initialized:
            self.initialize()
        
        # Placeholder
        return []
