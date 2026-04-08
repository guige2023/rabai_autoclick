"""
MIME type detection and mapping utilities.

Provides MIME type detection from file extensions and file content.
"""

from __future__ import annotations

import os
import mimetypes
from typing import Literal


# Extended MIME type map for common extensions
EXTENSION_MIME_MAP: dict[str, str] = {
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".cjs": "application/javascript",
    ".jsx": "text/javascript",
    ".ts": "application/typescript",
    ".tsx": "text/typescript",
    ".jsonc": "application/json",
    ".m3u": "audio/x-mpegurl",
    ".m3u8": "application/vnd.apple.mpegurl",
    ".webp": "image/webp",
    ".avif": "image/avif",
    ".apng": "image/apng",
    ".bmp": "image/bmp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".msg": "application/vnd.ms-outlook",
    ".ics": "text/calendar",
    ".vcf": "text/vcard",
    ".vcard": "text/vcard",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".toml": "application/toml",
    ".sh": "application/x-sh",
    ".bash": "application/x-sh",
    ".zsh": "application/x-sh",
    ".ps1": "application/x-powershell",
    ".bat": "application/x-bat",
    ".cmd": "application/x-bat",
    ".log": "text/plain",
    ".rst": "text/x-rst",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".db": "application/x-sqlite3",
    ".sqlite": "application/x-sqlite3",
    ".sqlite3": "application/x-sqlite3",
    ".wasm": "application/wasm",
}


def get_mime_type(file_path: str | Path) -> str:
    """
    Get MIME type from file path.

    Args:
        file_path: File path or Path object

    Returns:
        MIME type string
    """
    path = str(file_path)
    ext = os.path.splitext(path)[1].lower()

    if ext in EXTENSION_MIME_MAP:
        return EXTENSION_MIME_MAP[ext]

    mime_type, _ = mimetypes.guess_type(path)
    return mime_type or "application/octet-stream"


def get_extension_for_mime(mime_type: str) -> str | None:
    """
    Get file extension for a MIME type.

    Args:
        mime_type: MIME type string

    Returns:
        File extension with dot or None
    """
    ext = mimetypes.guess_extension(mime_type)
    if ext in (".jse", ".mjs"):
        return ".js"
    return ext


def is_text_mime_type(mime_type: str) -> bool:
    """Check if MIME type is text."""
    return (
        mime_type.startswith("text/")
        or mime_type in ("application/json", "application/javascript",
                         "application/xml", "application/yaml",
                         "application/typescript", "application/wasm")
    )


def is_image_mime_type(mime_type: str) -> bool:
    """Check if MIME type is an image."""
    return mime_type.startswith("image/")


def is_audio_mime_type(mime_type: str) -> bool:
    """Check if MIME type is audio."""
    return mime_type.startswith("audio/")


def is_video_mime_type(mime_type: str) -> bool:
    """Check if MIME type is video."""
    return mime_type.startswith("video/")


def categorize_mime_type(mime_type: str) -> Literal["image", "audio", "video", "text", "application", "other"]:
    """Categorize MIME type into broad category."""
    if mime_type.startswith("image/"):
        return "image"
    if mime_type.startswith("audio/"):
        return "audio"
    if mime_type.startswith("video/"):
        return "video"
    if mime_type.startswith("text/"):
        return "text"
    if mime_type.startswith("application/"):
        return "application"
    return "other"


def build_content_type_header(
    file_path: str,
    charset: str | None = None,
) -> str:
    """
    Build Content-Type header value.

    Args:
        file_path: File path
        charset: Optional charset (e.g. utf-8)

    Returns:
        Content-Type header value
    """
    mime_type = get_mime_type(file_path)
    if charset and is_text_mime_type(mime_type):
        return f"{mime_type}; charset={charset}"
    return mime_type


def add_mime_type_aliases(aliases: dict[str, str]) -> None:
    """
    Add custom MIME type aliases.

    Args:
        aliases: Mapping of extensions to MIME types
    """
    EXTENSION_MIME_MAP.update({k.lower(): v for k, v in aliases.items()})


def get_common_mime_types() -> dict[str, str]:
    """
    Get all commonly used MIME types.

    Returns:
        Dictionary of extension -> MIME type
    """
    result = dict(EXTENSION_MIME_MAP)
    for ext, mime in mimetypes.types_map.items():
        if ext not in result:
            result[ext] = mime
    return result


def is_browser_renderable(mime_type: str) -> bool:
    """Check if MIME type can be rendered in a browser."""
    return mime_type in (
        "text/html",
        "text/plain",
        "text/css",
        "text/javascript",
        "application/javascript",
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "image/svg+xml",
        "application/json",
        "application/xml",
    )


def select_content_type(
    accepted_types: list[str],
    available_types: list[str],
    default: str = "application/octet-stream",
) -> str:
    """
    Select best matching Content-Type using content negotiation.

    Args:
        accepted_types: Accepted MIME types (e.g. from Accept header)
        available_types: Available MIME types
        default: Default if no match

    Returns:
        Best matching MIME type
    """
    for accepted in accepted_types:
        accepted = accepted.strip().split(";")[0]
        if accepted == "*/*":
            return available_types[0] if available_types else default
        if accepted.endswith("/*"):
            prefix = accepted[:-1]
            for avail in available_types:
                if avail.startswith(prefix):
                    return avail
        if accepted in available_types:
            return accepted
    return default
