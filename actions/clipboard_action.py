"""
Clipboard Action Module

Manages system clipboard operations for automation workflows,
including text, files, images, and custom format data.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class ClipboardFormat(Enum):
    """Clipboard data formats."""

    TEXT = "text"
    HTML = "html"
    RTF = "rtf"
    IMAGE = "image"
    FILES = "files"
    CUSTOM = "custom"


@dataclass
class ClipboardContent:
    """Represents clipboard content."""

    format: ClipboardFormat
    data: Any
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None


@dataclass
class ClipboardConfig:
    """Configuration for clipboard operations."""

    max_history: int = 20
    auto_convert: bool = True
    preserve_formatting: bool = True
    enable_monitoring: bool = False


class ClipboardManager:
    """
    Manages system clipboard for automation.

    Supports reading, writing, format conversion,
    and clipboard history tracking.
    """

    def __init__(
        self,
        config: Optional[ClipboardConfig] = None,
        clipboard_handler: Optional[Callable[[str, Any], Any]] = None,
    ):
        self.config = config or ClipboardConfig()
        self.clipboard_handler = clipboard_handler or self._default_handler
        self._history: List[ClipboardContent] = []

    def _default_handler(self, action: str, data: Any = None) -> Any:
        """Default clipboard handler using platform tools."""
        import subprocess

        if action == "read":
            try:
                result = subprocess.run(
                    ["pbpaste"],
                    capture_output=True,
                    text=True,
                )
                return result.stdout
            except Exception:
                return None
        elif action == "write":
            try:
                subprocess.run(
                    ["pbcopy"],
                    input=data,
                    text=True,
                )
                return True
            except Exception:
                return False
        return None

    def copy_text(self, text: str) -> bool:
        """
        Copy text to clipboard.

        Args:
            text: Text to copy

        Returns:
            True if successful
        """
        result = self.clipboard_handler("write", text)

        if result:
            content = ClipboardContent(
                format=ClipboardFormat.TEXT,
                data=text,
                source="copy_text",
            )
            self._add_to_history(content)

        return bool(result)

    def paste_text(self) -> Optional[str]:
        """
        Get text from clipboard.

        Returns:
            Clipboard text or None
        """
        text = self.clipboard_handler("read")
        return text

    def copy_image(self, image_data: Any, format: str = "png") -> bool:
        """
        Copy image to clipboard.

        Args:
            image_data: Image data
            format: Image format

        Returns:
            True if successful
        """
        content = ClipboardContent(
            format=ClipboardFormat.IMAGE,
            data=image_data,
            source="copy_image",
        )
        self._add_to_history(content)

        if self.clipboard_handler("write_image"):
            return True

        logger.warning("Image copy not fully implemented")
        return False

    def paste_image(self) -> Optional[Any]:
        """Get image from clipboard."""
        return self.clipboard_handler("read_image")

    def copy_files(self, file_paths: List[str]) -> bool:
        """
        Copy files to clipboard.

        Args:
            file_paths: List of file paths

        Returns:
            True if successful
        """
        content = ClipboardContent(
            format=ClipboardFormat.FILES,
            data=file_paths,
            source="copy_files",
        )
        self._add_to_history(content)
        return self.clipboard_handler("write_files", file_paths)

    def paste_files(self) -> List[str]:
        """Get file paths from clipboard."""
        return self.clipboard_handler("read_files") or []

    def get_current_content(self) -> Optional[ClipboardContent]:
        """Get current clipboard content."""
        text = self.paste_text()
        if text:
            return ClipboardContent(
                format=ClipboardFormat.TEXT,
                data=text,
            )

        image = self.paste_image()
        if image:
            return ClipboardContent(
                format=ClipboardFormat.IMAGE,
                data=image,
            )

        files = self.paste_files()
        if files:
            return ClipboardContent(
                format=ClipboardFormat.FILES,
                data=files,
            )

        return None

    def _add_to_history(self, content: ClipboardContent) -> None:
        """Add content to history."""
        self._history.append(content)

        if len(self._history) > self.config.max_history:
            self._history = self._history[-self.config.max_history:]

    def get_history(self, limit: int = 10) -> List[ClipboardContent]:
        """Get clipboard history."""
        return self._history[-limit:]

    def clear_history(self) -> None:
        """Clear clipboard history."""
        self._history.clear()

    def clear(self) -> bool:
        """Clear the clipboard."""
        return bool(self.clipboard_handler("clear"))

    def is_supported_format(self, format: ClipboardFormat) -> bool:
        """Check if a format is supported."""
        handlers = ["read", "write"]
        for handler in handlers:
            if hasattr(self.clipboard_handler, f"{handler}_{format.value}"):
                return True
        return False


def create_clipboard_manager(
    config: Optional[ClipboardConfig] = None,
) -> ClipboardManager:
    """Factory function to create ClipboardManager."""
    return ClipboardManager(config=config)
