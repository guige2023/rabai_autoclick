"""Metadata extractor action for extracting file and content metadata.

This module provides metadata extraction from files,
images, documents, and web pages.

Example:
    >>> action = MetadataExtractorAction()
    >>> result = action.execute(path="/tmp/image.jpg")
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class MetadataEntry:
    """Represents a metadata entry."""
    key: str
    value: Any
    source: str = "file"


class MetadataExtractorAction:
    """Metadata extraction action.

    Extracts metadata from files, images, documents,
    and web content.

    Example:
        >>> action = MetadataExtractorAction()
        >>> result = action.execute(
        ...     source="image",
        ...     path="/tmp/photo.jpg"
        ... )
    """

    def __init__(self) -> None:
        """Initialize metadata extractor."""
        self._last_metadata: dict[str, Any] = {}

    def execute(
        self,
        source: str,
        path: Optional[str] = None,
        url: Optional[str] = None,
        data: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute metadata extraction.

        Args:
            source: Source type (file, image, document, web).
            path: File path.
            url: Web URL.
            data: Raw data.
            **kwargs: Additional parameters.

        Returns:
            Extraction result dictionary.

        Raises:
            ValueError: If source is invalid.
        """
        source = source.lower()
        result: dict[str, Any] = {"source": source, "success": True}

        if source == "file":
            if not path:
                raise ValueError("path required for 'file' source")
            result.update(self._extract_file_metadata(path))

        elif source == "image":
            if not path:
                raise ValueError("path required for 'image' source")
            result.update(self._extract_image_metadata(path))

        elif source == "document":
            if not path:
                raise ValueError("path required for 'document' source")
            result.update(self._extract_document_metadata(path))

        elif source == "web":
            if not url:
                raise ValueError("url required for 'web' source")
            result.update(self._extract_web_metadata(url))

        elif source == "exif":
            if not path:
                raise ValueError("path required for 'exif' source")
            result.update(self._extract_exif(path))

        else:
            raise ValueError(f"Unknown source: {source}")

        return result

    def _extract_file_metadata(self, path: str) -> dict[str, Any]:
        """Extract file metadata.

        Args:
            path: File path.

        Returns:
            Result dictionary.
        """
        try:
            stat = os.stat(path)
            return {
                "name": os.path.basename(path),
                "path": path,
                "directory": os.path.dirname(path),
                "extension": os.path.splitext(path)[1],
                "size": stat.st_size,
                "created": stat.st_ctime,
                "modified": stat.st_mtime,
                "accessed": stat.st_atime,
                "is_file": os.path.isfile(path),
                "is_dir": os.path.isdir(path),
                "is_link": os.path.islink(path),
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _extract_image_metadata(self, path: str) -> dict[str, Any]:
        """Extract image metadata.

        Args:
            path: Image file path.

        Returns:
            Result dictionary.
        """
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
        except ImportError:
            return {
                "success": False,
                "error": "Pillow not installed. Run: pip install pillow",
            }

        try:
            img = Image.open(path)
            metadata = {
                "format": img.format,
                "mode": img.mode,
                "width": img.width,
                "height": img.height,
                "aspect_ratio": img.width / img.height if img.height > 0 else 0,
            }

            # Extract EXIF if available
            exif = img.getexif()
            if exif:
                exif_data = {}
                for tag_id, value in exif.items():
                    tag = TAGS.get(tag_id, tag_id)
                    exif_data[str(tag)] = str(value)
                metadata["exif"] = exif_data

            return metadata

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _extract_document_metadata(self, path: str) -> dict[str, Any]:
        """Extract document metadata.

        Args:
            path: Document file path.

        Returns:
            Result dictionary.
        """
        ext = os.path.splitext(path)[1].lower()
        metadata = self._extract_file_metadata(path)

        if ext in (".pdf",):
            metadata.update(self._extract_pdf_metadata(path))
        elif ext in (".doc", ".docx"):
            metadata.update(self._extract_word_metadata(path))

        return metadata

    def _extract_pdf_metadata(self, path: str) -> dict[str, Any]:
        """Extract PDF metadata.

        Args:
            path: PDF file path.

        Returns:
            Result dictionary.
        """
        try:
            import PyPDF2
        except ImportError:
            return {"pdf_metadata": "PyPDF2 not installed"}

        try:
            with open(path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                metadata = {
                    "pages": len(reader.pages),
                }
                if reader.metadata:
                    metadata.update({
                        k: str(v) for k, v in reader.metadata.items()
                    })
                return metadata
        except Exception as e:
            return {"pdf_error": str(e)}

    def _extract_word_metadata(self, path: str) -> dict[str, Any]:
        """Extract Word document metadata.

        Args:
            path: Word file path.

        Returns:
            Result dictionary.
        """
        # Basic metadata from file
        return {
            "type": "word_document",
            "extension": os.path.splitext(path)[1],
        }

    def _extract_web_metadata(self, url: str) -> dict[str, Any]:
        """Extract metadata from web page.

        Args:
            url: Web URL.

        Returns:
            Result dictionary.
        """
        import requests
        from urllib.parse import urlparse

        try:
            response = requests.get(url, timeout=30)
            parsed = urlparse(url)

            metadata: dict[str, Any] = {
                "url": url,
                "domain": parsed.netloc,
                "status_code": response.status_code,
                "content_type": response.headers.get("Content-Type", ""),
                "server": response.headers.get("Server", ""),
                "last_modified": response.headers.get("Last-Modified", ""),
            }

            # Extract meta tags
            import re
            meta_pattern = re.compile(
                r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']+)["\']',
                re.IGNORECASE,
            )
            for match in meta_pattern.finditer(response.text):
                metadata[match.group(1)] = match.group(2)

            # Extract Open Graph
            og_pattern = re.compile(
                r'<meta[^>]+property=["\']og:([^"\']+)["\'][^>]+content=["\']([^"\']+)["\']',
                re.IGNORECASE,
            )
            for match in og_pattern.finditer(response.text):
                metadata[f"og_{match.group(1)}"] = match.group(2)

            return metadata

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _extract_exif(self, path: str) -> dict[str, Any]:
        """Extract EXIF data from image.

        Args:
            path: Image file path.

        Returns:
            Result dictionary.
        """
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
        except ImportError:
            return {"success": False, "error": "Pillow not installed"}

        try:
            img = Image.open(path)
            exif = img.getexif()
            if not exif:
                return {"exif": "No EXIF data found"}

            exif_data = {}
            for tag_id, value in exif.items():
                tag = TAGS.get(tag_id, tag_id)
                exif_data[str(tag)] = str(value)

            return {"exif": exif_data}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_last_metadata(self) -> dict[str, Any]:
        """Get last extracted metadata.

        Returns:
            Metadata dictionary.
        """
        return self._last_metadata
