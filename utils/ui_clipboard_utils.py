"""
UI Clipboard Utilities - Clipboard operations for UI automation.

This module provides utilities for managing clipboard operations
during UI automation, including text, image, and file clipboard
formats, clipboard history, and synchronization.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


ClipboardFormat = str

FORMAT_TEXT = "text"
FORMAT_HTML = "html"
FORMAT_IMAGE = "image"
FORMAT_FILES = "files"
FORMAT_RTF = "rtf"


@dataclass
class ClipboardEntry:
    """Represents a clipboard entry.
    
    Attributes:
        id: Unique identifier for this entry.
        format: Clipboard format type.
        content: Entry content.
        timestamp: When entry was created.
        source: Optional source application.
        metadata: Additional entry data.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    format: ClipboardFormat = FORMAT_TEXT
    content: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class ClipboardHistorySettings:
    """Settings for clipboard history management.
    
    Attributes:
        max_entries: Maximum number of entries to keep.
        max_age: Maximum age in seconds before entry expires.
        include_duplicates: Whether to keep duplicate entries.
        formats: List of formats to record.
    """
    max_entries: int = 100
    max_age: float = 3600.0
    include_duplicates: bool = True
    formats: list[ClipboardFormat] = field(default_factory=list)


class ClipboardHistory:
    """Manages clipboard history for automation.
    
    Provides methods for recording, searching, and retrieving
    clipboard history entries.
    
    Example:
        >>> history = ClipboardHistory()
        >>> history.record("text", "hello world")
        >>> entries = history.search("hello")
    """
    
    def __init__(
        self,
        settings: Optional[ClipboardHistorySettings] = None
    ) -> None:
        """Initialize clipboard history.
        
        Args:
            settings: History settings.
        """
        self.settings = settings or ClipboardHistorySettings()
        self._entries: list[ClipboardEntry] = []
    
    def record(
        self,
        format: ClipboardFormat,
        content: str,
        source: Optional[str] = None
    ) -> ClipboardEntry:
        """Record a clipboard entry.
        
        Args:
            format: Clipboard format.
            content: Content to record.
            source: Optional source application.
            
        Returns:
            The created ClipboardEntry.
        """
        self._prune_expired()
        
        if not self.settings.include_duplicates:
            last = self._entries[-1] if self._entries else None
            if last and last.content == content:
                return last
        
        entry = ClipboardEntry(
            format=format,
            content=content,
            source=source
        )
        self._entries.append(entry)
        
        while len(self._entries) > self.settings.max_entries:
            self._entries.pop(0)
        
        return entry
    
    def search(
        self,
        query: str,
        format: Optional[ClipboardFormat] = None
    ) -> list[ClipboardEntry]:
        """Search clipboard history.
        
        Args:
            query: Search query.
            format: Optional format filter.
            
        Returns:
            List of matching entries.
        """
        self._prune_expired()
        query_lower = query.lower()
        
        results = []
        for entry in reversed(self._entries):
            if entry.content and query_lower in entry.content.lower():
                if format is None or entry.format == format:
                    results.append(entry)
        
        return results
    
    def get_recent(
        self,
        count: int = 10,
        format: Optional[ClipboardFormat] = None
    ) -> list[ClipboardEntry]:
        """Get recent clipboard entries.
        
        Args:
            count: Number of entries to return.
            format: Optional format filter.
            
        Returns:
            List of recent entries.
        """
        self._prune_expired()
        entries = self._entries[-count:] if count > 0 else self._entries
        entries = list(reversed(entries))
        
        if format:
            entries = [e for e in entries if e.format == format]
        
        return entries
    
    def get_by_id(self, entry_id: str) -> Optional[ClipboardEntry]:
        """Get entry by ID.
        
        Args:
            entry_id: Entry identifier.
            
        Returns:
            ClipboardEntry if found.
        """
        for entry in self._entries:
            if entry.id == entry_id:
                return entry
        return None
    
    def clear(self) -> None:
        """Clear all history."""
        self._entries.clear()
    
    def _prune_expired(self) -> None:
        """Remove expired entries."""
        current_time = time.time()
        self._entries = [
            e for e in self._entries
            if current_time - e.timestamp <= self.settings.max_age
        ]
    
    def __len__(self) -> int:
        """Get number of entries."""
        self._prune_expired()
        return len(self._entries)


class ClipboardFormatter:
    """Formats content for clipboard operations.
    
    Provides methods for converting content between different
    clipboard formats.
    
    Example:
        >>> formatter = ClipboardFormatter()
        >>> html = formatter.text_to_html("Hello", "#000000")
    """
    
    @staticmethod
    def text_to_html(
        text: str,
        font_family: str = "Arial",
        font_size: int = 12,
        color: str = "#000000"
    ) -> str:
        """Convert plain text to HTML clipboard format.
        
        Args:
            text: Plain text content.
            font_family: Font family.
            font_size: Font size in points.
            color: Text color hex code.
            
        Returns:
            HTML formatted string.
        """
        html = f"""<html>
<body>
<!--[if gte mso 9]>
<xml>
<o:OfficeDocumentSettings>
<o:AllowPNG/>
</o:OfficeDocumentSettings>
</xml>
<![endif]-->
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<style type="text/css">
body {{ font-family: {font_family}; font-size: {font_size}pt; color: {color}; }}
</style>
<span style="font-family: {font_family}; font-size: {font_size}pt; color: {color};">
{text}
</span>
</body>
</html>"""
        return html
    
    @staticmethod
    def text_to_rtf(
        text: str,
        font_family: str = "Arial",
        font_size: int = 12
    ) -> str:
        """Convert plain text to RTF format.
        
        Args:
            text: Plain text content.
            font_family: Font family.
            font_size: Font size in half-points.
            
        Returns:
            RTF formatted string.
        """
        escaped = text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")
        return f"{{\\rtf1\\ansi\\deff0{{\\fonttbl{{\\f0 {font_family};}}}}{{\\f0\\fs{font_size * 2} {escaped}}}}}"
    
    @staticmethod
    def html_to_text(html: str) -> str:
        """Extract plain text from HTML.
        
        Args:
            html: HTML content.
            
        Returns:
            Plain text.
        """
        import re
        text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    
    @staticmethod
    def sanitize_text(text: str) -> str:
        """Sanitize text for clipboard.
        
        Args:
            text: Input text.
            
        Returns:
            Sanitized text.
        """
        import re
        text = text.replace('\x00', '')
        text = re.sub(r'[\x01-\x08\x0b\x0c\x0e-\x1f]', '', text)
        return text


class ClipboardLock:
    """Manages clipboard access synchronization.
    
    Prevents clipboard conflicts between multiple automation
    tasks or applications.
    
    Example:
        >>> lock = ClipboardLock()
        >>> with lock.acquire(timeout=5.0):
        ...     # clipboard operations
    """
    
    def __init__(self) -> None:
        """Initialize clipboard lock."""
        self._locked: bool = False
        self._holder: Optional[str] = None
        self._acquire_time: Optional[float] = None
    
    def acquire(
        self,
        holder: str,
        timeout: float = 0.0
    ) -> bool:
        """Acquire clipboard lock.
        
        Args:
            holder: Identifier for lock holder.
            timeout: Max wait time in seconds (0 = no wait).
            
        Returns:
            True if lock was acquired.
        """
        if self._locked and timeout > 0:
            start = time.time()
            while self._locked:
                if time.time() - start >= timeout:
                    return False
                time.sleep(0.01)
        
        self._locked = True
        self._holder = holder
        self._acquire_time = time.time()
        return True
    
    def release(self, holder: str) -> bool:
        """Release clipboard lock.
        
        Args:
            holder: Identifier for lock holder.
            
        Returns:
            True if lock was released.
        """
        if self._locked and self._holder == holder:
            self._locked = False
            self._holder = None
            self._acquire_time = None
            return True
        return False
    
    def is_locked(self) -> bool:
        """Check if clipboard is locked."""
        return self._locked
    
    def get_holder(self) -> Optional[str]:
        """Get current lock holder."""
        return self._holder
    
    def get_hold_duration(self) -> Optional[float]:
        """Get how long lock has been held."""
        if self._acquire_time:
            return time.time() - self._acquire_time
        return None


@dataclass
class ClipboardState:
    """Represents current clipboard state.
    
    Attributes:
        content: Current content.
        format: Content format.
        timestamp: Last update time.
        source: Last source application.
    """
    content: Optional[str] = None
    format: ClipboardFormat = FORMAT_TEXT
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None


class ClipboardMonitor:
    """Monitors clipboard changes.
    
    Provides callback-based monitoring of clipboard changes
    for automation workflows.
    
    Example:
        >>> monitor = ClipboardMonitor()
        >>> monitor.on_change(callback)
        >>> monitor.start()
    """
    
    def __init__(self) -> None:
        """Initialize clipboard monitor."""
        self._callbacks: list[Callable[[ClipboardState], None]] = []
        self._current: Optional[ClipboardState] = None
        self._running: bool = False
    
    def on_change(
        self,
        callback: Callable[[ClipboardState], None]
    ) -> None:
        """Register a callback for clipboard changes.
        
        Args:
            callback: Function to call on change.
        """
        self._callbacks.append(callback)
    
    def notify_change(self, state: ClipboardState) -> None:
        """Notify callbacks of a clipboard change.
        
        Args:
            state: New clipboard state.
        """
        self._current = state
        for callback in self._callbacks:
            callback(state)
    
    def get_current(self) -> Optional[ClipboardState]:
        """Get current clipboard state."""
        return self._current
