"""
OCR Utilities

Optical character recognition for extracting text from screenshots.
Supports multiple OCR engines and language configuration.

Example:
    >>> ocr = OCREngine()
    >>> text = ocr.extract_text(screenshot_path)
    >>> regions = ocr.extract_regions(screenshot_path)
    >>> for region in regions:
    ...     print(region.text, region.bounds)
"""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from typing import Optional


@dataclass
class TextRegion:
    """A text region detected by OCR."""
    text: str
    confidence: float
    bounds: tuple[int, int, int, int]  # x, y, width, height
    language: str = "en"


class OCREngine:
    """
    OCR engine wrapper for text extraction from images.

    Supports:
        - Tesseract (system installation)
        - Native macOS OCR via Vision framework
    """

    def __init__(
        self,
        engine: str = "vision",  # "vision" or "tesseract"
        language: str = "eng",
    ) -> None:
        self.engine = engine
        self.language = language

    def extract_text(
        self,
        image_path: str,
        language: Optional[str] = None,
    ) -> str:
        """
        Extract all text from an image.

        Args:
            image_path: Path to image file.
            language: Override language code.

        Returns:
            Extracted text, empty string on failure.
        """
        lang = language or self.language

        if self.engine == "vision":
            return self._extract_vision(image_path)
        elif self.engine == "tesseract":
            return self._extract_tesseract(image_path, lang)

        return ""

    def extract_regions(
        self,
        image_path: str,
        language: Optional[str] = None,
    ) -> list[TextRegion]:
        """
        Extract text regions with bounding boxes.

        Args:
            image_path: Path to image file.
            language: Override language code.

        Returns:
            List of TextRegion objects.
        """
        lang = language or self.language

        if self.engine == "vision":
            return self._extract_regions_vision(image_path)
        elif self.engine == "tesseract":
            return self._extract_regions_tesseract(image_path, lang)

        return []

    def _extract_vision(self, image_path: str) -> str:
        """Use macOS Vision framework for OCR."""
        try:
            script = f'''
            use framework "Vision"
            use framework "AppKit"

            set img to current application's NSImage's alloc()'s initWithContentsOfFile:"{image_path}"
            if img is missing value then return ""

            set handler to current application's VNRecognizeTextRequest's new()
            handler's setRecognitionLevel:(current application's VNRecognizeTextRecognitionLevelAccurate)
            handler's setRecognitionLanguages:{{"{self.language}"}}

            set request to current application's VNImageRequestRequest's alloc()'s initWithImage:img
            set requests to current application's NSArray's arrayWithObject:request

            current application's VNImageRequestHandler's alloc()'s initWithCVPixelBuffer:null options:(current application's NSDictionary's dictionary())'s performSelectorOnMainThread:"featuresInRequest:error:" withObject:requests waitUntilDone:true

            set observations to request's results()
            if observations is missing value then return ""

            set resultText to current application's NSMutableString's string()
            repeat with observation in observations
                set topCandidate to observation's topCandidates(1)'s firstObject()
                if topCandidate is not missing value then
                    (resultText's appendString:(topCandidate's string()))
                    (resultText's appendString:(character id 10))
                end if
            end repeat

            return resultText as text
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=30.0,
            )

            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return ""

    def _extract_tesseract(self, image_path: str, language: str) -> str:
        """Use Tesseract for OCR."""
        try:
            result = subprocess.run(
                ["tesseract", image_path, "stdout", "-l", language],
                capture_output=True,
                text=True,
                timeout=30.0,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return ""

    def _extract_regions_vision(self, image_path: str) -> list[TextRegion]:
        """Extract text regions using Vision framework."""
        regions: list[TextRegion] = []

        try:
            script = f'''
            use framework "Vision"
            use framework "AppKit"

            set img to current application's NSImage's alloc()'s initWithContentsOfFile:"{image_path}"
            if img is missing value then return ""

            set request to current application's VNRecognizeTextRequest's new()
            request's setRecognitionLevel:(current application's VNRecognizeTextRecognitionLevelAccurate)
            request's setRecognitionLanguages:{{"{self.language}"}}

            set handler to current application's VNImageRequestHandler's alloc()'s initWithURL:(current application's NSURL's fileURLWithPath:"{image_path}") options:(current application's NSDictionary's dictionary())
            handler's performRequests:{{request}} error:(missing value)

            set observations to request's results()
            if observations is missing value then return ""

            set output to current application's NSMutableString's string()
            repeat with observation in observations
                set box to observation's boundingBox()
                set text to (observation's topCandidates(1)'s firstObject()'s string()) as text
                set conf to (observation's topCandidates(1)'s firstObject()'s confidence()) as number
                set imgW to (img's size)'s width
                set imgH to (img's size)'s height

                set x to (box's origin)'s x * imgW
                set y to (1.0 - (box's origin)'s y - (box's size)'s height) * imgH
                set w to (box's size)'s width * imgW
                set h to (box's size)'s height * imgH

                (output's appendString:(text & "|" & (x as text) & "|" & (y as text) & "|" & (w as text) & "|" & (h as text) & "|" & (conf as text) & "\\n"))
            end repeat

            return output as text
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=30.0,
            )

            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    parts = line.split("|")
                    if len(parts) >= 6:
                        try:
                            regions.append(TextRegion(
                                text=parts[0],
                                confidence=float(parts[5]),
                                bounds=(
                                    int(float(parts[1])),
                                    int(float(parts[2])),
                                    int(float(parts[3])),
                                    int(float(parts[4])),
                                ),
                            ))
                        except (ValueError, IndexError):
                            pass
        except Exception:
            pass

        return regions

    def _extract_regions_tesseract(
        self,
        image_path: str,
        language: str,
    ) -> list[TextRegion]:
        """Extract text regions using Tesseract."""
        regions: list[TextRegion] = []
        try:
            with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
                tsv_path = f.name

            result = subprocess.run(
                ["tesseract", image_path, "stdout", "-l", language, "tsv"],
                capture_output=True,
                text=True,
                timeout=30.0,
            )

            if result.returncode == 0:
                # Parse TSV output
                import csv
                import io

                reader = csv.reader(io.StringIO(result.stdout), delimiter="\t")
                next(reader, None)  # Skip header
                for row in reader:
                    if len(row) >= 7:
                        try:
                            regions.append(TextRegion(
                                text=row[11],  # text column
                                confidence=float(row[10]) / 100.0,
                                bounds=(
                                    int(float(row[6])),
                                    int(float(row[7])),
                                    int(float(row[8])),
                                    int(float(row[9])),
                                ),
                            ))
                        except (ValueError, IndexError):
                            pass

            import os
            os.remove(tsv_path)
        except Exception:
            pass

        return regions
