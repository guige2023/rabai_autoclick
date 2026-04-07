"""Clipboard utilities for RabAI AutoClick.

Provides:
- Clipboard read/write
- Clipboard history
- Format detection
"""

import threading
from typing import Any, Callable, List, Optional


class ClipboardFormat:
    """Clipboard format types."""
    TEXT = "text"
    HTML = "html"
    IMAGE = "image"
    FILES = "files"


class Clipboard:
    """Clipboard operations."""

    @staticmethod
    def get_text() -> Optional[str]:
        """Get text from clipboard.

        Returns:
            Clipboard text or None.
        """
        try:
            import win32clipboard
            win32clipboard.OpenClipboard()
            if win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_TEXT):
                data = win32clipboard.GetClipboardData(win32clipboard.CF_TEXT)
                win32clipboard.CloseClipboard()
                return data.decode("utf-8")
            win32clipboard.CloseClipboard()
            return None
        except Exception:
            return None

    @staticmethod
    def set_text(text: str) -> bool:
        """Set text to clipboard.

        Args:
            text: Text to set.

        Returns:
            True if successful.
        """
        try:
            import win32clipboard
            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardData(win32clipboard.CF_TEXT, text.encode("utf-8"))
            win32clipboard.CloseClipboard()
            return True
        except Exception:
            return False

    @staticmethod
    def get_image() -> Optional[Any]:
        """Get image from clipboard.

        Returns:
            PIL Image or None.
        """
        try:
            import win32clipboard
            from PIL import Image
            import io

            win32clipboard.OpenClipboard()
            if win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_BITMAP):
                data = win32clipboard.GetClipboardData(win32clipboard.CF_BITMAP)
                win32clipboard.CloseClipboard()
                # Convert to PIL Image
                return Image.frombytes("RGB", (1, 1), data)
            win32clipboard.CloseClipboard()
            return None
        except Exception:
            return None

    @staticmethod
    def clear() -> bool:
        """Clear clipboard.

        Returns:
            True if successful.
        """
        try:
            import win32clipboard
            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.CloseClipboard()
            return True
        except Exception:
            return False


class ClipboardHistory:
    """Clipboard history with maximum size."""

    def __init__(self, max_size: int = 50) -> None:
        """Initialize history.

        Args:
            max_size: Maximum entries to keep.
        """
        self._max_size = max_size
        self._history: List[str] = []
        self._lock = threading.Lock()

    def add(self, text: str) -> None:
        """Add entry to history.

        Args:
            text: Text to add.
        """
        with self._lock:
            if text in self._history:
                self._history.remove(text)
            self._history.insert(0, text)
            if len(self._history) > self._max_size:
                self._history.pop()

    def get(self, index: int) -> Optional[str]:
        """Get entry by index.

        Args:
            index: History index (0 = most recent).

        Returns:
            Text or None.
        """
        with self._lock:
            if 0 <= index < len(self._history):
                return self._history[index]
            return None

    def search(self, query: str) -> List[str]:
        """Search history for query.

        Args:
            query: Search query.

        Returns:
            Matching entries.
        """
        with self._lock:
            return [h for h in self._history if query.lower() in h.lower()]

    def clear(self) -> None:
        """Clear history."""
        with self._lock:
            self._history.clear()

    @property
    def size(self) -> int:
        """Get history size."""
        with self._lock:
            return len(self._history)


class ClipboardMonitor:
    """Monitor clipboard changes."""

    def __init__(self, interval: float = 0.5) -> None:
        """Initialize monitor.

        Args:
            interval: Check interval in seconds.
        """
        self._interval = interval
        self._last_content: Optional[str] = None
        self._callbacks: List[Callable[[str], None]] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def on_change(self, callback: Callable[[str], None]) -> None:
        """Register change callback.

        Args:
            callback: Function to call on change.
        """
        with self._lock:
            self._callbacks.append(callback)

    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return

        self._running = True
        self._last_content = Clipboard.get_text()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _monitor_loop(self) -> None:
        """Monitoring loop."""
        while self._running:
            try:
                current = Clipboard.get_text()
                if current != self._last_content:
                    self._last_content = current
                    with self._lock:
                        for callback in self._callbacks:
                            callback(current or "")
            except Exception:
                pass
            threading.Event().wait(self._interval)

    @property
    def is_running(self) -> bool:
        """Check if monitoring."""
        return self._running


class ClipboardFormatter:
    """Format clipboard content for different targets."""

    @staticmethod
    def to_plain_text(html: str) -> str:
        """Convert HTML to plain text.

        Args:
            html: HTML content.

        Returns:
            Plain text.
        """
        try:
            from html.parser import HTMLParser

            class HTMLToText(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.result = []
                    self.skip_data = False

                def handle_starttag(self, tag, attrs):
                    if tag in ("script", "style"):
                        self.skip_data = True

                def handle_endtag(self, tag):
                    if tag in ("script", "style"):
                        self.skip_data = False

                def handle_data(self, data):
                    if not self.skip_data:
                        self.result.append(data)

            parser = HTMLToText()
            parser.feed(html)
            return "".join(parser.result).strip()
        except Exception:
            return html

    @staticmethod
    def to_html(text: str) -> str:
        """Convert plain text to HTML.

        Args:
            text: Plain text.

        Returns:
            HTML content.
        """
        import html
        return f"<html><body><p>{html.escape(text)}</p></body></html>"

    @staticmethod
    def strip_formatting(text: str) -> str:
        """Remove formatting from text.

        Args:
            text: Text with formatting.

        Returns:
            Plain text.
        """
        import re
        # Remove common formatting characters
        text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", "", text)
        return text.strip()
