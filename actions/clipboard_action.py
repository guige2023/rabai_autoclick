"""Clipboard action for clipboard management.

This module provides clipboard operations including
get, set, history, and format conversion.

Example:
    >>> action = ClipboardAction()
    >>> result = action.execute(command="get")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ClipboardEntry:
    """Represents a clipboard entry."""
    content: str
    timestamp: float
    format: str = "text"


class ClipboardAction:
    """Clipboard management action.

    Provides clipboard operations with history,
    format detection, and content conversion.

    Example:
        >>> action = ClipboardAction()
        >>> result = action.execute(command="set", text="Hello World")
    """

    def __init__(self) -> None:
        """Initialize clipboard action."""
        self._history: list[ClipboardEntry] = []
        self._max_history = 50

    def execute(
        self,
        command: str,
        text: Optional[str] = None,
        index: Optional[int] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute clipboard command.

        Args:
            command: Command (get, set, clear, history, search).
            text: Text to set.
            index: Index for history access.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd == "get":
            result.update(self._get_clipboard())

        elif cmd == "set":
            if text is None:
                raise ValueError("text required for 'set' command")
            result.update(self._set_clipboard(text))

        elif cmd == "clear":
            result.update(self._clear_clipboard())

        elif cmd == "history":
            result.update(self._get_history())

        elif cmd == "search":
            query = kwargs.get("query", "")
            result.update(self._search_history(query))

        elif cmd == "restore":
            if index is None:
                raise ValueError("index required for 'restore' command")
            result.update(self._restore_from_history(index))

        elif cmd == "copy_file":
            path = kwargs.get("path")
            result.update(self._copy_file_to_clipboard(path))

        elif cmd == "paste_image":
            result.update(self._paste_image())

        elif cmd == "get_formats":
            result.update(self._get_formats())

        elif cmd == "detect":
            result.update(self._detect_content_type())

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def _get_clipboard(self) -> dict[str, Any]:
        """Get current clipboard content.

        Returns:
            Result dictionary.
        """
        try:
            import subprocess
            content = subprocess.run(
                ["pbpaste"],
                capture_output=True,
                text=True,
            ).stdout
            return {"text": content, "length": len(content)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _set_clipboard(self, text: str) -> dict[str, Any]:
        """Set clipboard content.

        Args:
            text: Text to set.

        Returns:
            Result dictionary.
        """
        try:
            import subprocess
            subprocess.run(
                ["pbcopy"],
                input=text,
                text=True,
            )
            # Add to history
            self._add_to_history(text)
            return {"set": True, "length": len(text)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _clear_clipboard(self) -> dict[str, Any]:
        """Clear clipboard.

        Returns:
            Result dictionary.
        """
        return self._set_clipboard("")

    def _get_history(self) -> dict[str, Any]:
        """Get clipboard history.

        Returns:
            Result dictionary with history entries.
        """
        entries = [
            {
                "index": i,
                "content": e.content[:100],  # Truncate for display
                "timestamp": e.timestamp,
                "length": len(e.content),
            }
            for i, e in enumerate(self._history)
        ]
        return {
            "history": entries,
            "count": len(entries),
        }

    def _search_history(self, query: str) -> dict[str, Any]:
        """Search clipboard history.

        Args:
            query: Search query.

        Returns:
            Result dictionary.
        """
        matches = [
            {
                "index": i,
                "content": e.content[:100],
                "timestamp": e.timestamp,
            }
            for i, e in enumerate(self._history)
            if query.lower() in e.content.lower()
        ]
        return {
            "matches": matches,
            "count": len(matches),
        }

    def _restore_from_history(self, index: int) -> dict[str, Any]:
        """Restore clipboard from history.

        Args:
            index: History index.

        Returns:
            Result dictionary.
        """
        if index < 0 or index >= len(self._history):
            return {"success": False, "error": "Index out of range"}

        entry = self._history[index]
        return self._set_clipboard(entry.content)

    def _add_to_history(self, content: str) -> None:
        """Add content to history.

        Args:
            content: Content to add.
        """
        entry = ClipboardEntry(
            content=content,
            timestamp=time.time(),
        )
        self._history.insert(0, entry)

        # Limit history size
        if len(self._history) > self._max_history:
            self._history = self._history[:self._max_history]

    def _copy_file_to_clipboard(self, path: Optional[str]) -> dict[str, Any]:
        """Copy file to clipboard.

        Args:
            path: File path.

        Returns:
            Result dictionary.
        """
        if not path:
            return {"success": False, "error": "path required"}

        try:
            import subprocess
            subprocess.run(["cp", path, "/tmp/clipboard_file"])
            return {"copied": True, "path": path}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _paste_image(self) -> dict[str, Any]:
        """Paste image from clipboard.

        Returns:
            Result dictionary.
        """
        # Would need platform-specific implementation
        return {"pasted": False, "error": "Not implemented"}

    def _get_formats(self) -> dict[str, Any]:
        """Get available clipboard formats.

        Returns:
            Result dictionary.
        """
        return {
            "formats": ["public.utf8-plain-text"],
            "has_image": False,
            "has_files": False,
        }

    def _detect_content_type(self) -> dict[str, Any]:
        """Detect content type in clipboard.

        Returns:
            Result dictionary.
        """
        content_result = self._get_clipboard()
        text = content_result.get("text", "")

        import re
        detected = []

        # Email
        if re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text):
            detected.append("email")
        # URL
        if re.search(r"https?://", text):
            detected.append("url")
        # Phone
        if re.search(r"\d{3}[-.\s]?\d{3}[-.\s]?\d{4}", text):
            detected.append("phone")
        # Number
        if text.strip().isdigit():
            detected.append("number")
        # JSON
        try:
            import json
            json.loads(text)
            detected.append("json")
        except (ValueError, TypeError):
            pass

        return {
            "detected": detected,
            "is_url": "url" in detected,
            "is_email": "email" in detected,
            "is_json": "json" in detected,
        }

    def clear_history(self) -> None:
        """Clear clipboard history."""
        self._history.clear()
