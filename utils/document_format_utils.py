"""
Document Format Utilities for UI Automation.

This module provides utilities for detecting and converting
document formats in file handling workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from enum import Enum
import mimetypes


class DocumentFormat(Enum):
    """Supported document formats."""
    PDF = "pdf"
    DOCX = "docx"
    XLSX = "xlsx"
    PPTX = "pptx"
    TXT = "txt"
    HTML = "html"
    MARKDOWN = "md"
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    PNG = "png"
    JPG = "jpg"
    GIF = "gif"
    SVG = "svg"
    UNKNOWN = "unknown"


@dataclass
class FormatInfo:
    """Information about a document format."""
    format: DocumentFormat
    mime_type: str
    extensions: List[str]
    description: str


class FormatRegistry:
    """Registry of document formats."""

    FORMATS: Dict[DocumentFormat, FormatInfo] = {
        DocumentFormat.PDF: FormatInfo(
            format=DocumentFormat.PDF,
            mime_type="application/pdf",
            extensions=[".pdf"],
            description="Portable Document Format"
        ),
        DocumentFormat.DOCX: FormatInfo(
            format=DocumentFormat.DOCX,
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            extensions=[".docx"],
            description="Microsoft Word Document"
        ),
        DocumentFormat.XLSX: FormatInfo(
            format=DocumentFormat.XLSX,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            extensions=[".xlsx"],
            description="Microsoft Excel Spreadsheet"
        ),
        DocumentFormat.PPTX: FormatInfo(
            format=DocumentFormat.PPTX,
            mime_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            extensions=[".pptx"],
            description="Microsoft PowerPoint Presentation"
        ),
        DocumentFormat.TXT: FormatInfo(
            format=DocumentFormat.TXT,
            mime_type="text/plain",
            extensions=[".txt", ".text"],
            description="Plain Text"
        ),
        DocumentFormat.HTML: FormatInfo(
            format=DocumentFormat.HTML,
            mime_type="text/html",
            extensions=[".html", ".htm"],
            description="HyperText Markup Language"
        ),
        DocumentFormat.MARKDOWN: FormatInfo(
            format=DocumentFormat.MARKDOWN,
            mime_type="text/markdown",
            extensions=[".md", ".markdown"],
            description="Markdown"
        ),
        DocumentFormat.JSON: FormatInfo(
            format=DocumentFormat.JSON,
            mime_type="application/json",
            extensions=[".json"],
            description="JavaScript Object Notation"
        ),
        DocumentFormat.XML: FormatInfo(
            format=DocumentFormat.XML,
            mime_type="application/xml",
            extensions=[".xml"],
            description="Extensible Markup Language"
        ),
        DocumentFormat.CSV: FormatInfo(
            format=DocumentFormat.CSV,
            mime_type="text/csv",
            extensions=[".csv"],
            description="Comma-Separated Values"
        ),
        DocumentFormat.PNG: FormatInfo(
            format=DocumentFormat.PNG,
            mime_type="image/png",
            extensions=[".png"],
            description="Portable Network Graphics"
        ),
        DocumentFormat.JPG: FormatInfo(
            format=DocumentFormat.JPG,
            mime_type="image/jpeg",
            extensions=[".jpg", ".jpeg"],
            description="JPEG Image"
        ),
        DocumentFormat.GIF: FormatInfo(
            format=DocumentFormat.GIF,
            mime_type="image/gif",
            extensions=[".gif"],
            description="Graphics Interchange Format"
        ),
        DocumentFormat.SVG: FormatInfo(
            format=DocumentFormat.SVG,
            mime_type="image/svg+xml",
            extensions=[".svg"],
            description="Scalable Vector Graphics"
        ),
    }


def detect_format_from_path(path: str) -> DocumentFormat:
    """
    Detect document format from file path.

    Args:
        path: File path

    Returns:
        DocumentFormat
    """
    ext = os.path.splitext(path)[1].lower()
    for fmt, info in FormatRegistry.FORMATS.items():
        if ext in info.extensions:
            return fmt
    return DocumentFormat.UNKNOWN


def detect_format_from_mime(mime_type: str) -> DocumentFormat:
    """
    Detect document format from MIME type.

    Args:
        mime_type: MIME type string

    Returns:
        DocumentFormat
    """
    for fmt, info in FormatRegistry.FORMATS.items():
        if info.mime_type == mime_type:
            return fmt
    return DocumentFormat.UNKNOWN


def get_format_info(fmt: DocumentFormat) -> Optional[FormatInfo]:
    """
    Get format information.

    Args:
        fmt: Document format

    Returns:
        FormatInfo or None
    """
    return FormatRegistry.FORMATS.get(fmt)


def is_text_format(fmt: DocumentFormat) -> bool:
    """Check if format is text-based."""
    return fmt in (
        DocumentFormat.TXT,
        DocumentFormat.HTML,
        DocumentFormat.MARKDOWN,
        DocumentFormat.JSON,
        DocumentFormat.XML,
        DocumentFormat.CSV,
    )


def is_image_format(fmt: DocumentFormat) -> bool:
    """Check if format is an image."""
    return fmt in (
        DocumentFormat.PNG,
        DocumentFormat.JPG,
        DocumentFormat.GIF,
        DocumentFormat.SVG,
    )


def is_office_format(fmt: DocumentFormat) -> bool:
    """Check if format is an Office document."""
    return fmt in (
        DocumentFormat.DOCX,
        DocumentFormat.XLSX,
        DocumentFormat.PPTX,
    )


def get_mime_type(path: str) -> str:
    """
    Get MIME type for a file.

    Args:
        path: File path

    Returns:
        MIME type string
    """
    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"


def normalize_extension(ext: str) -> str:
    """
    Normalize file extension.

    Args:
        ext: Extension with or without dot

    Returns:
        Normalized extension with leading dot
    """
    ext = ext.strip().lower()
    if not ext.startswith("."):
        ext = "." + ext
    return ext
