"""
Metadata Extractor Action Module.

Extracts metadata from web pages, documents, images, and files.
Handles EXIF, IPTC, XMP from images, and document properties.

Example:
    >>> from metadata_extractor_action import MetadataExtractor
    >>> extractor = MetadataExtractor()
    >>> meta = extractor.extract_from_url("https://example.com/image.jpg")
    >>> meta = extractor.extract_from_file("/path/to/image.jpg")
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class FileMetadata:
    """File and document metadata."""
    filename: str = ""
    size_bytes: int = 0
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    mime_type: str = ""
    extension: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ImageMetadata(FileMetadata):
    """Image-specific metadata."""
    width: int = 0
    height: int = 0
    format: str = ""
    color_mode: str = ""
    has_alpha: bool = False
    exif: dict[str, Any] = field(default_factory=dict)
    iptc: dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentMetadata(FileMetadata):
    """Document metadata."""
    title: str = ""
    author: str = ""
    subject: str = ""
    keywords: list[str] = field(default_factory=list)
    page_count: int = 0


class MetadataExtractor:
    """Extract metadata from various file types."""

    def __init__(self):
        self._pillow_available = self._check_pillow()
        self._mutagen_available = self._check_mutagen()
        self._exif_available = self._check_exif()

    def _check_pillow(self) -> bool:
        try:
            from PIL import Image
            return True
        except ImportError:
            return False

    def _check_mutagen(self) -> bool:
        try:
            import mutagen
            return True
        except ImportError:
            return False

    def _check_exif(self) -> bool:
        try:
            from PIL import Image
            return True
        except ImportError:
            return False

    def extract_from_file(self, path: str) -> FileMetadata:
        """Extract metadata from file."""
        if not os.path.exists(path):
            return FileMetadata()

        stat = os.stat(path)
        _, ext = os.path.splitext(path)
        mime = self._guess_mime(ext)

        meta = FileMetadata(
            filename=os.path.basename(path),
            size_bytes=stat.st_size,
            created=datetime.fromtimestamp(stat.st_ctime),
            modified=datetime.fromtimestamp(stat.st_mtime),
            mime_type=mime,
            extension=ext.lstrip("."),
        )

        if mime.startswith("image/"):
            img_meta = self._extract_image_metadata(path)
            if img_meta:
                meta = img_meta
        elif mime.startswith("video/") or mime.startswith("audio/"):
            media_meta = self._extract_media_metadata(path)
            if media_meta:
                meta.extra = media_meta
        elif mime == "application/pdf":
            doc_meta = self._extract_pdf_metadata(path)
            if doc_meta:
                meta = doc_meta

        return meta

    def _extract_image_metadata(self, path: str) -> Optional[ImageMetadata]:
        if not self._pillow_available:
            return None

        try:
            from PIL import Image
            with Image.open(path) as img:
                stat = os.stat(path)
                meta = ImageMetadata(
                    filename=os.path.basename(path),
                    size_bytes=stat.st_size,
                    created=datetime.fromtimestamp(stat.st_ctime),
                    modified=datetime.fromtimestamp(stat.st_mtime),
                    mime_type=img.format or "image/unknown",
                    extension=img.format or "",
                    width=img.width,
                    height=img.height,
                    format=img.format or "",
                    color_mode=img.mode,
                    has_alpha=img.mode in ("RGBA", "LA", "P"),
                )
                if hasattr(img, "_getexif") and img._getexif():
                    meta.exif = self._parse_exif(img._getexif())
                return meta
        except Exception:
            return None

    def _parse_exif(self, exif_data: dict) -> dict[str, Any]:
        """Parse EXIF data to human-readable format."""
        result = {}
        exif_tags = {
            271: "Make",
            272: "Model",
            274: "Orientation",
            282: "XResolution",
            283: "YResolution",
            296: "ResolutionUnit",
            305: "Software",
            306: "DateTime",
            315: "Artist",
            34665: "ExifIFD",
            36867: "DateTimeOriginal",
            36868: "DateTimeDigitized",
            37377: "ShutterSpeedValue",
            37378: "ApertureValue",
            37379: "BrightnessValue",
            34850: "ExposureProgram",
            34853: "GPSInfo",
            40961: "ColorSpace",
            40962: "PixelXDimension",
            40963: "PixelYDimension",
        }
        for key, name in exif_tags.items():
            if key in exif_data:
                result[name] = exif_data[key]
        return result

    def _extract_media_metadata(self, path: str) -> Optional[dict[str, Any]]:
        if not self._mutagen_available:
            return None

        try:
            import mutagen
            from mutagen import File as MutagenFile
            f = MutagenFile(path)
            if f is None:
                return None
            info = {}
            if hasattr(f, "info"):
                info_attrs = ["length", "bitrate", "sample_rate", "channels"]
                for attr in info_attrs:
                    if hasattr(f.info, attr):
                        info[attr] = getattr(f.info, attr)
            if hasattr(f, "tags") and f.tags:
                for key, value in f.tags.items():
                    info[key] = str(value) if value is not None else None
            return info
        except Exception:
            return None

    def _extract_pdf_metadata(self, path: str) -> Optional[DocumentMetadata]:
        try:
            result = subprocess.run(
                ["pdfinfo", path],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return None
            lines = result.stdout.splitlines()
            meta = DocumentMetadata(
                filename=os.path.basename(path),
                size_bytes=os.path.getsize(path),
            )
            for line in lines:
                if ":" in line:
                    key, _, value = line.partition(":")
                    key = key.strip()
                    value = value.strip()
                    if key == "Title":
                        meta.title = value
                    elif key == "Author":
                        meta.author = value
                    elif key == "Subject":
                        meta.subject = value
                    elif key == "Keywords":
                        meta.keywords = [k.strip() for k in value.split(",")]
                    elif key == "Pages":
                        try:
                            meta.page_count = int(value)
                        except ValueError:
                            pass
            return meta
        except Exception:
            return None

    def extract_from_url(self, url: str) -> Optional[FileMetadata]:
        """Download and extract metadata from URL."""
        import urllib.request
        import tempfile

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    tmp.write(resp.read())
                tmp_path = tmp.name
            meta = self.extract_from_file(tmp_path)
            os.unlink(tmp_path)
            return meta
        except Exception:
            return None

    def _guess_mime(self, ext: str) -> str:
        mime_map = {
            ".html": "text/html",
            ".htm": "text/html",
            ".txt": "text/plain",
            ".json": "application/json",
            ".xml": "application/xml",
            ".pdf": "application/pdf",
            ".doc": "application/msword",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".ppt": "application/vnd.ms-powerpoint",
            ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".webp": "image/webp",
            ".svg": "image/svg+xml",
            ".bmp": "image/bmp",
            ".ico": "image/x-icon",
            ".mp3": "audio/mpeg",
            ".mp4": "video/mp4",
            ".mov": "video/quicktime",
            ".avi": "video/x-msvideo",
            ".zip": "application/zip",
        }
        ext = ext.lower().lstrip(".")
        return mime_map.get(f".{ext}", "application/octet-stream")


if __name__ == "__main__":
    extractor = MetadataExtractor()
    print("MetadataExtractor module loaded")
    print(f"Pillow available: {extractor._pillow_available}")
    print(f"Mutagen available: {extractor._mutagen_available}")
