"""Clipboard history management utilities.

This module provides utilities for managing clipboard history,
allowing tracking and retrieval of previously copied content.
"""

from __future__ import annotations

import platform
import subprocess
import time
from dataclasses import dataclass, field
from typing import Optional
from collections import deque


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


MAX_HISTORY_SIZE = 100


@dataclass
class ClipboardEntry:
    """A single clipboard history entry."""
    content: str
    timestamp: float
    content_type: str = "text"  # 'text', 'image', 'file'
    
    def __repr__(self) -> str:
        preview = self.content[:50] + "..." if len(self.content) > 50 else self.content
        return f"ClipboardEntry({self.content_type!r}, {preview!r})"


class ClipboardHistory:
    """Manages a history of clipboard entries."""
    
    def __init__(self, max_size: int = MAX_HISTORY_SIZE):
        self.max_size = max_size
        self._history: deque[ClipboardEntry] = deque(maxlen=max_size)
        self._last_content: Optional[str] = None
    
    def add(self, content: str, content_type: str = "text") -> None:
        """Add an entry to the clipboard history.
        
        Args:
            content: The clipboard content.
            content_type: Type of content ('text', 'image', 'file').
        """
        # Don't add duplicates
        if content == self._last_content:
            return
        
        entry = ClipboardEntry(
            content=content,
            timestamp=time.time(),
            content_type=content_type,
        )
        self._history.append(entry)
        self._last_content = content
    
    def get_all(self) -> list[ClipboardEntry]:
        """Get all clipboard history entries, newest first."""
        return list(reversed(list(self._history)))
    
    def search(self, query: str) -> list[ClipboardEntry]:
        """Search clipboard history for entries containing query."""
        query_lower = query.lower()
        return [
            entry for entry in reversed(self._history)
            if query_lower in entry.content.lower()
        ]
    
    def get_recent(self, count: int = 10) -> list[ClipboardEntry]:
        """Get the N most recent entries."""
        return list(self._history)[-count:]
    
    def clear(self) -> None:
        """Clear all clipboard history."""
        self._history.clear()
        self._last_content = None
    
    def __len__(self) -> int:
        return len(self._history)


# Global clipboard history instance
_global_history = ClipboardHistory()


def get_current_text() -> Optional[str]:
    """Get the current text from the clipboard.
    
    Returns:
        Clipboard text content, or None if unavailable.
    """
    if IS_MACOS:
        return _get_clipboard_text_macos()
    elif IS_LINUX:
        return _get_clipboard_text_linux()
    elif IS_WINDOWS:
        return _get_clipboard_text_windows()
    return None


def _get_clipboard_text_macos() -> Optional[str]:
    """Get clipboard text on macOS using pbpaste."""
    try:
        result = subprocess.run(
            ["pbpaste"],
            capture_output=True,
            timeout=3
        )
        if result.returncode == 0:
            return result.stdout
    except Exception:
        pass
    return None


def _get_clipboard_text_linux() -> Optional[str]:
    """Get clipboard text on Linux using xclip or xsel."""
    try:
        result = subprocess.run(
            ["xclip", "-selection", "clipboard", "-o"],
            capture_output=True,
            timeout=3
        )
        if result.returncode == 0:
            return result.stdout
    except FileNotFoundError:
        try:
            result = subprocess.run(
                ["xsel", "--clipboard", "--output"],
                capture_output=True,
                timeout=3
            )
            if result.returncode == 0:
                return result.stdout
        except Exception:
            pass
    return None


def _get_clipboard_text_windows() -> Optional[str]:
    """Get clipboard text on Windows."""
    try:
        import ctypes
        from ctypes import c_void_p, POINTER
        
        CF_TEXT = 1
        
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        
        user32.OpenClipboard(None)
        
        h_data = user32.GetClipboardData(CF_TEXT)
        if h_data:
            data = kernel32.GlobalLock(h_data)
            text = ctypes.c_char_p(data).value.decode("cp1252")
            kernel32.GlobalUnlock(h_data)
            user32.CloseClipboard()
            return text
        
        user32.CloseClipboard()
    except Exception:
        pass
    return None


def set_clipboard_text(text: str) -> bool:
    """Set the clipboard text content.
    
    Args:
        text: Text to put on clipboard.
    
    Returns:
        True if successful.
    """
    if IS_MACOS:
        return _set_clipboard_text_macos(text)
    elif IS_LINUX:
        return _set_clipboard_text_linux(text)
    elif IS_WINDOWS:
        return _set_clipboard_text_windows(text)
    return False


def _set_clipboard_text_macos(text: str) -> bool:
    """Set clipboard text on macOS using pbcopy."""
    try:
        result = subprocess.run(
            ["pbcopy"],
            input=text.encode(),
            capture_output=True,
            timeout=3
        )
        return result.returncode == 0
    except Exception:
        return False


def _set_clipboard_text_linux(text: str) -> bool:
    """Set clipboard text on Linux using xclip or xsel."""
    try:
        result = subprocess.run(
            ["xclip", "-selection", "clipboard", "-i"],
            input=text.encode(),
            capture_output=True,
            timeout=3
        )
        if result.returncode == 0:
            return True
    except FileNotFoundError:
        pass
    
    try:
        result = subprocess.run(
            ["xsel", "--clipboard", "--input"],
            input=text.encode(),
            capture_output=True,
            timeout=3
        )
        return result.returncode == 0
    except Exception:
        pass
    return False


def _set_clipboard_text_windows(text: str) -> bool:
    """Set clipboard text on Windows."""
    try:
        import ctypes
        
        CF_TEXT = 1
        
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        
        user32.OpenClipboard(None)
        user32.EmptyClipboard()
        
        data = text.encode("cp1252")
        h_data = kernel32.GlobalAlloc(0x0002, len(data) + 1)  # GMEM_MOVEABLE
        p_data = kernel32.GlobalLock(h_data)
        ctypes.memmove(p_data, data, len(data))
        kernel32.GlobalUnlock(h_data)
        
        user32.SetClipboardData(CF_TEXT, h_data)
        user32.CloseClipboard()
        return True
    except Exception:
        return False


def copy_with_history(text: str) -> None:
    """Copy text to clipboard and add to global history.
    
    Args:
        text: Text to copy.
    """
    if set_clipboard_text(text):
        _global_history.add(text)


def get_history() -> ClipboardHistory:
    """Get the global clipboard history instance."""
    return _global_history
