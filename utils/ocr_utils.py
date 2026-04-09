"""Optical Character Recognition (OCR) Utilities.

This module provides OCR capabilities using Tesseract for extracting
text from images, screenshots, and screen regions for automation
and accessibility purposes.

Example:
    >>> from ocr_utils import OCRProcessor, TextRegion
    >>> processor = OCRProcessor()
    >>> regions = processor.extract_from_image('/tmp/screenshot.png')
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple, Any


class OCREngine(Enum):
    """Available OCR engines."""
    TESSERACT = auto()
    macOS = auto()


@dataclass
class BoundingBox:
    """Bounding box coordinates."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def x1(self) -> int:
        return self.x
    
    @property
    def y1(self) -> int:
        return self.y
    
    @property
    def x2(self) -> int:
        return self.x + self.width
    
    @property
    def y2(self) -> int:
        return self.y + self.height
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def area(self) -> int:
        return self.width * self.height
    
    def contains_point(self, x: int, y: int) -> bool:
        return self.x <= x < self.x2 and self.y <= y < self.y2
    
    def intersects(self, other: BoundingBox) -> bool:
        return not (self.x2 <= other.x or other.x2 <= self.x or
                    self.y2 <= other.y or other.y2 <= self.y)
    
    def intersection(self, other: BoundingBox) -> Optional[BoundingBox]:
        if not self.intersects(other):
            return None
        
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        w = min(self.x2, other.x2) - x
        h = min(self.y2, other.y2) - y
        
        return BoundingBox(x, y, w, h)


@dataclass
class TextRegion:
    """A recognized text region with bounding box.
    
    Attributes:
        text: Recognized text content
        bounding_box: Region bounds
        confidence: Recognition confidence (0-100)
        language: Detected language
    """
    text: str
    bounding_box: BoundingBox
    confidence: float = 0.0
    language: str = "eng"
    
    @property
    def center(self) -> Tuple[int, int]:
        return self.bounding_box.center
    
    @property
    def area(self) -> int:
        return self.bounding_box.area
    
    def overlaps_with(self, other: TextRegion) -> bool:
        return self.bounding_box.intersects(other.bounding_box)
    
    def distance_to(self, x: int, y: int) -> float:
        """Calculate distance from center to point."""
        cx, cy = self.center
        return ((cx - x) ** 2 + (cy - y) ** 2) ** 0.5
    
    def is_close_to(self, x: int, y: int, threshold: float) -> bool:
        """Check if center is within threshold distance."""
        return self.distance_to(x, y) <= threshold


@dataclass
class OCRResult:
    """Result of OCR processing.
    
    Attributes:
        regions: List of recognized text regions
        full_text: Concatenated text of all regions
        processing_time: Time taken for processing
        image_path: Source image path
    """
    regions: List[TextRegion] = field(default_factory=list)
    processing_time: float = 0.0
    image_path: Optional[str] = None
    engine: OCREngine = OCREngine.TESSERACT
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def full_text(self) -> str:
        return '\n'.join(r.text for r in self.regions)
    
    @property
    def is_empty(self) -> bool:
        return len(self.regions) == 0
    
    def find_at_point(self, x: int, y: int) -> Optional[TextRegion]:
        """Find the text region containing a point."""
        for region in self.regions:
            if region.bounding_box.contains_point(x, y):
                return region
        return None
    
    def find_nearest(self, x: int, y: int, max_distance: float = 50) -> Optional[TextRegion]:
        """Find nearest text region to a point."""
        nearest = None
        min_dist = max_distance
        
        for region in self.regions:
            dist = region.distance_to(x, y)
            if dist < min_dist:
                min_dist = dist
                nearest = region
        
        return nearest
    
    def find_by_text(self, query: str, case_sensitive: bool = False) -> List[TextRegion]:
        """Find regions containing specific text."""
        results = []
        query_str = query if case_sensitive else query.lower()
        
        for region in self.regions:
            text = region.text if case_sensitive else region.text.lower()
            if query_str in text:
                results.append(region)
        
        return results


class OCRProcessor:
    """Main OCR processing interface.
    
    Provides high-level OCR operations with support for
    multiple engines and configurations.
    
    Attributes:
        engine: OCR engine to use
        language: Language pack to use
        confidence_threshold: Minimum confidence to include
    """
    
    def __init__(
        self,
        engine: OCREngine = OCREngine.TESSERACT,
        language: str = "eng",
        confidence_threshold: float = 0.0,
    ):
        self.engine = engine
        self.language = language
        self.confidence_threshold = confidence_threshold
        
        self._preprocess_callbacks: List[Callable[[str], str]] = []
        self._postprocess_callbacks: List[Callable[[TextRegion], TextRegion]] = []
    
    def extract_from_image(
        self,
        image_path: str,
        region: Optional[BoundingBox] = None,
    ) -> OCRResult:
        """Extract text from an image file.
        
        Args:
            image_path: Path to image file
            region: Optional sub-region to process
            
        Returns:
            OCRResult with recognized text
        """
        start_time = time.time()
        
        if self.engine == OCREngine.TESSERACT:
            result = self._tesseract_extract(image_path, region)
        else:
            result = OCRResult(image_path=image_path)
        
        result.processing_time = time.time() - start_time
        return result
    
    def extract_from_screenshot(self, region: Optional[BoundingBox] = None) -> OCRResult:
        """Extract text from a screenshot.
        
        Args:
            region: Optional screen region to capture
            
        Returns:
            OCRResult with recognized text
        """
        import subprocess
        import tempfile
        
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            temp_path = f.name
        
        try:
            capture_cmd = ['screencapture', '-x', temp_path]
            subprocess.run(capture_cmd, check=True, capture_output=True)
            
            if region:
                from PIL import Image
                img = Image.open(temp_path)
                cropped = img.crop((
                    region.x, region.y,
                    region.x + region.width,
                    region.y + region.height,
                ))
                cropped.save(temp_path)
            
            return self.extract_from_image(temp_path)
        
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def _tesseract_extract(
        self,
        image_path: str,
        region: Optional[BoundingBox] = None,
    ) -> OCRResult:
        """Extract using Tesseract OCR engine."""
        cmd = [
            'tesseract',
            image_path,
            'stdout',
            '-l', self.language,
            '--psm', '6',
        ]
        
        if region:
            cmd.extend(['--crop', f'{region.x},{region.y},{region.width},{region.height}'])
        
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            
            output = proc.stdout
            
            for callback in self._preprocess_callbacks:
                output = callback(output)
            
            regions = self._parse_tesseract_output(output)
            
            for callback in self._postprocess_callbacks:
                regions = [callback(r) for r in regions]
            
            if self.confidence_threshold > 0:
                regions = [r for r in regions if r.confidence >= self.confidence_threshold]
            
            return OCRResult(
                regions=regions,
                image_path=image_path,
                engine=OCREngine.TESSERACT,
            )
            
        except Exception as e:
            return OCRResult(image_path=image_path, engine=OCREngine.TESSERACT)
    
    def _parse_tesseract_output(self, output: str) -> List[TextRegion]:
        """Parse Tesseract output into TextRegion objects."""
        regions = []
        
        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(None, 4)
            
            if len(parts) >= 5:
                try:
                    x, y, w, h, text = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), parts[4]
                    regions.append(TextRegion(
                        text=text,
                        bounding_box=BoundingBox(x, y, w, h),
                        confidence=0.0,
                    ))
                except ValueError:
                    continue
        
        return regions
    
    def add_preprocess(self, callback: Callable[[str], str]) -> None:
        """Add preprocessing callback."""
        self._preprocess_callbacks.append(callback)
    
    def add_postprocess(self, callback: Callable[[TextRegion], TextRegion]) -> None:
        """Add postprocessing callback."""
        self._postprocess_callbacks.append(callback)


class TextMatcher:
    """Matches text patterns in OCR results."""
    
    def __init__(self, result: OCRResult):
        self.result = result
        self._cache: Dict[str, List[TextRegion]] = {}
    
    def find_all(self, text: str, case_sensitive: bool = False) -> List[TextRegion]:
        """Find all regions containing text."""
        key = f"{text}:{case_sensitive}"
        
        if key not in self._cache:
            self._cache[key] = self.result.find_by_text(text, case_sensitive)
        
        return self._cache[key]
    
    def find_exact(self, text: str, case_sensitive: bool = False) -> Optional[TextRegion]:
        """Find region with exact text match."""
        regions = self.find_all(text, case_sensitive)
        
        for region in regions:
            compare = region.text if case_sensitive else region.text.lower()
            if compare == (text if case_sensitive else text.lower()):
                return region
        
        return None
    
    def find_at_position(self, x: int, y: int) -> Optional[TextRegion]:
        """Find region at screen position."""
        return self.result.find_at_point(x, y)
    
    def find_nearest(self, x: int, y: int, threshold: float = 50) -> Optional[TextRegion]:
        """Find nearest region to position."""
        return self.result.find_nearest(x, y, threshold)


class ScreenTextReader:
    """High-level screen text reading utility."""
    
    def __init__(self, processor: Optional[OCRProcessor] = None):
        self.processor = processor or OCRProcessor()
    
    def read_all(self) -> OCRResult:
        """Read all text from screen."""
        return self.processor.extract_from_screenshot()
    
    def read_region(self, x: int, y: int, width: int, height: int) -> OCRResult:
        """Read text from a screen region."""
        box = BoundingBox(x, y, width, height)
        return self.processor.extract_from_screenshot(region=box)
    
    def read_at_cursor(self, cursor_x: int, cursor_y: int, radius: int = 50) -> OCRResult:
        """Read text near cursor position."""
        box = BoundingBox(
            max(0, cursor_x - radius),
            max(0, cursor_y - radius),
            radius * 2,
            radius * 2,
        )
        return self.processor.extract_from_screenshot(region=box)
    
    def wait_for_text(
        self,
        text: str,
        timeout: float = 10.0,
        poll_interval: float = 0.5,
    ) -> Optional[TextRegion]:
        """Wait for specific text to appear on screen.
        
        Args:
            text: Text to wait for
            timeout: Maximum wait time
            poll_interval: Time between polls
            
        Returns:
            TextRegion if found, None if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = self.read_all()
            matcher = TextMatcher(result)
            region = matcher.find_exact(text)
            
            if region:
                return region
            
            time.sleep(poll_interval)
        
        return None
