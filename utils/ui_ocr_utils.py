"""
UI OCR Utilities - OCR-based element detection and text extraction.

This module provides utilities for using OCR (Optical Character Recognition)
to detect and interact with UI elements based on their text content.
It supports text extraction, text-based element finding, and OCR
result caching.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class OCRResult:
    """Represents a result from OCR processing.
    
    Attributes:
        id: Unique identifier for this result.
        text: Recognized text.
        confidence: Recognition confidence (0.0 to 1.0).
        bounds: (x, y, width, height) of text region.
        language: Detected language.
        timestamp: When OCR was performed.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    text: str = ""
    confidence: float = 0.0
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)
    language: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    
    @property
    def x(self) -> int:
        """Get X coordinate."""
        return self.bounds[0]
    
    @property
    def y(self) -> int:
        """Get Y coordinate."""
        return self.bounds[1]
    
    @property
    def width(self) -> int:
        """Get width."""
        return self.bounds[2]
    
    @property
    def height(self) -> int:
        """Get height."""
        return self.bounds[3]
    
    @property
    def center(self) -> tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains_point(self, px: int, py: int) -> bool:
        """Check if point is inside text bounds."""
        return (
            self.x <= px < self.x + self.width
            and self.y <= py < self.y + self.height
        )


@dataclass
class OCRConfig:
    """Configuration for OCR operations.
    
    Attributes:
        language: Primary language for OCR.
        additional_languages: Additional languages to support.
        confidence_threshold: Minimum confidence to include results.
        preprocessing: Whether to preprocess images.
        scale_factor: Image scale factor for better accuracy.
    """
    language: str = "eng"
    additional_languages: list[str] = field(default_factory=list)
    confidence_threshold: float = 0.5
    preprocessing: bool = True
    scale_factor: float = 2.0


class OCRCache:
    """Caches OCR results for performance.
    
    Provides methods for caching and retrieving OCR results
    based on image hashes.
    
    Example:
        >>> cache = OCRCache(max_entries=100)
        >>> cached = cache.get(image_hash)
    """
    
    def __init__(self, max_entries: int = 100) -> None:
        """Initialize OCR cache.
        
        Args:
            max_entries: Maximum number of entries to cache.
        """
        self.max_entries = max_entries
        self._cache: dict[str, list[OCRResult]] = {}
        self._access_times: dict[str, float] = {}
    
    def get(self, image_hash: str) -> Optional[list[OCRResult]]:
        """Get cached OCR results.
        
        Args:
            image_hash: Hash of the image.
            
        Returns:
            Cached results, or None if not found.
        """
        if image_hash in self._cache:
            self._access_times[image_hash] = time.time()
            return self._cache[image_hash]
        return None
    
    def put(
        self,
        image_hash: str,
        results: list[OCRResult]
    ) -> None:
        """Cache OCR results.
        
        Args:
            image_hash: Hash of the image.
            results: OCR results to cache.
        """
        if image_hash not in self._cache:
            self._cache[image_hash] = results
            self._access_times[image_hash] = time.time()
            
            while len(self._cache) > self.max_entries:
                oldest = min(self._access_times, key=self._access_times.get)
                del self._cache[oldest]
                del self._access_times[oldest]
    
    def invalidate(self, image_hash: str) -> None:
        """Invalidate a cache entry.
        
        Args:
            image_hash: Hash to invalidate.
        """
        self._cache.pop(image_hash, None)
        self._access_times.pop(image_hash, None)
    
    def clear(self) -> None:
        """Clear all cached results."""
        self._cache.clear()
        self._access_times.clear()


class OCRTextFinder:
    """Finds UI elements based on OCR text.
    
    Provides methods for searching screen content using
    OCR and returning text positions.
    
    Example:
        >>> finder = OCRTextFinder(ocr_engine)
        >>> results = finder.find_text("Submit")
    """
    
    def __init__(
        self,
        ocr_func: Callable[[bytes, OCRConfig], list[OCRResult]],
        cache: Optional[OCRCache] = None
    ) -> None:
        """Initialize text finder.
        
        Args:
            ocr_func: Function to perform OCR.
            cache: Optional OCR cache.
        """
        self._ocr_func = ocr_func
        self._cache = cache or OCRCache()
        self._config = OCRConfig()
    
    def set_config(self, config: OCRConfig) -> None:
        """Set OCR configuration.
        
        Args:
            config: OCR configuration.
        """
        self._config = config
    
    def find_text(
        self,
        image: bytes,
        text: str,
        exact: bool = False
    ) -> list[OCRResult]:
        """Find text on screen.
        
        Args:
            image: Screenshot image data.
            text: Text to search for.
            exact: Whether to match exactly.
            
        Returns:
            List of matching OCRResults.
        """
        import hashlib
        image_hash = hashlib.md5(image).hexdigest()
        
        cached = self._cache.get(image_hash)
        if cached:
            return self._filter_results(cached, text, exact)
        
        results = self._ocr_func(image, self._config)
        self._cache.put(image_hash, results)
        
        return self._filter_results(results, text, exact)
    
    def find_texts(
        self,
        image: bytes,
        texts: list[str]
    ) -> dict[str, list[OCRResult]]:
        """Find multiple texts on screen.
        
        Args:
            image: Screenshot image data.
            texts: List of texts to search for.
            
        Returns:
            Dictionary mapping text to results.
        """
        import hashlib
        image_hash = hashlib.md5(image).hexdigest()
        
        cached = self._cache.get(image_hash)
        if not cached:
            cached = self._ocr_func(image, self._config)
            self._cache.put(image_hash, cached)
        
        return {
            text: self._filter_results(cached, text, False)
            for text in texts
        }
    
    def find_all_text(
        self,
        image: bytes
    ) -> list[OCRResult]:
        """Find all text on screen.
        
        Args:
            image: Screenshot image data.
            
        Returns:
            List of all OCRResults.
        """
        import hashlib
        image_hash = hashlib.md5(image).hexdigest()
        
        cached = self._cache.get(image_hash)
        if cached:
            return cached
        
        results = self._ocr_func(image, self._config)
        self._cache.put(image_hash, results)
        
        return results
    
    def _filter_results(
        self,
        results: list[OCRResult],
        text: str,
        exact: bool
    ) -> list[OCRResult]:
        """Filter OCR results by text."""
        text_lower = text.lower()
        
        if exact:
            return [
                r for r in results
                if r.text.lower() == text_lower
                and r.confidence >= self._config.confidence_threshold
            ]
        else:
            return [
                r for r in results
                if text_lower in r.text.lower()
                and r.confidence >= self._config.confidence_threshold
            ]


@dataclass
class OCRRegion:
    """Represents a text region for OCR targeting.
    
    Attributes:
        name: Region name.
        bounds: (x, y, width, height) of region.
        expected_text: Optional expected text content.
    """
    name: str
    bounds: tuple[int, int, int, int]
    expected_text: Optional[str] = None


class OCRRegionDetector:
    """Detects text regions on screen.
    
    Provides methods for identifying and classifying
    text regions based on position and content.
    
    Example:
        >>> detector = OCRRegionDetector()
        >>> regions = detector.detect_regions(image)
    """
    
    def __init__(self) -> None:
        """Initialize region detector."""
        self._regions: list[OCRRegion] = []
    
    def add_region(
        self,
        name: str,
        bounds: tuple[int, int, int, int],
        expected_text: Optional[str] = None
    ) -> None:
        """Add a region to monitor.
        
        Args:
            name: Region name.
            bounds: Region bounds.
            expected_text: Optional expected text.
        """
        self._regions.append(OCRRegion(
            name=name,
            bounds=bounds,
            expected_text=expected_text
        ))
    
    def detect_text_in_regions(
        self,
        ocr_results: list[OCRResult]
    ) -> dict[str, list[OCRResult]]:
        """Detect text within defined regions.
        
        Args:
            ocr_results: OCR results to analyze.
            
        Returns:
            Dictionary mapping region name to results.
        """
        region_results: dict[str, list[OCRResult]] = {
            r.name: [] for r in self._regions
        }
        
        for ocr_result in ocr_results:
            for region in self._regions:
                if self._bounds_contain(region.bounds, ocr_result.bounds):
                    region_results[region.name].append(ocr_result)
        
        return region_results
    
    def find_missing_text(
        self,
        ocr_results: list[OCRResult],
        region_name: str
    ) -> list[str]:
        """Find expected text that's missing from region.
        
        Args:
            ocr_results: OCR results.
            region_name: Region to check.
            
        Returns:
            List of missing expected texts.
        """
        region = next((r for r in self._regions if r.name == region_name), None)
        if not region or not region.expected_text:
            return []
        
        found_texts = [
            r.text for r in ocr_results
            if self._bounds_contain(region.bounds, r.bounds)
        ]
        
        missing = []
        for expected in region.expected_text.split(","):
            expected = expected.strip()
            if not any(expected.lower() in found.lower() for found in found_texts):
                missing.append(expected)
        
        return missing
    
    @staticmethod
    def _bounds_contain(
        outer: tuple[int, int, int, int],
        inner: tuple[int, int, int, int]
    ) -> bool:
        """Check if outer bounds contain inner bounds."""
        ox, oy, ow, oh = outer
        ix, iy, iw, ih = inner
        
        return (
            ox <= ix
            and oy <= iy
            and ox + ow >= ix + iw
            and oy + oh >= iy + ih
        )
