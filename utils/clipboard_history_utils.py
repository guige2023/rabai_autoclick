"""Clipboard History Utilities.

This module provides clipboard history management, including persistent
clipboard storage, text/image clipboard handling, and clipboard filtering
for macOS desktop applications.

Example:
    >>> from clipboard_history_utils import ClipboardManager, ClipboardEntry
    >>> manager = ClipboardManager()
    >>> entries = manager.get_history()
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Union


class ClipboardContentType(Enum):
    """Types of clipboard content."""
    UNKNOWN = auto()
    TEXT = auto()
    IMAGE = auto()
    HTML = auto()
    RTF = auto()
    FILE = auto()
    URL = auto()


@dataclass
class ClipboardEntry:
    """Represents a clipboard history entry.
    
    Attributes:
        id: Unique entry identifier
        content_type: Type of clipboard content
        text_content: Text content if applicable
        timestamp: When entry was copied
        source_app: Application that provided the content
        is_favorite: Whether entry is pinned
        preview: Short preview of content
    """
    id: str
    content_type: ClipboardContentType = ClipboardContentType.UNKNOWN
    text_content: Optional[str] = None
    data: Optional[bytes] = None
    timestamp: float = field(default_factory=time.time)
    source_app: Optional[str] = None
    is_favorite: bool = False
    use_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def preview(self) -> str:
        """Get short text preview."""
        if self.text_content:
            return self.text_content[:100] + ('...' if len(self.text_content) > 100 else '')
        return f"[{self.content_type.name}]"
    
    @property
    def age(self) -> float:
        """Get age in seconds since entry was created."""
        return time.time() - self.timestamp
    
    def matches_filter(self, query: str) -> bool:
        """Check if entry matches a text filter query."""
        if not query:
            return True
        
        query_lower = query.lower()
        
        if self.text_content and query_lower in self.text_content.lower():
            return True
        
        if self.source_app and query_lower in self.source_app.lower():
            return True
        
        return False


class ClipboardManager:
    """Manages clipboard history and operations.
    
    Provides access to clipboard contents, history tracking,
    and clipboard filtering.
    
    Attributes:
        max_history: Maximum entries to keep in history
        auto_save: Whether to auto-save clipboard changes
    """
    
    def __init__(
        self,
        max_history: int = 100,
        auto_save: bool = True,
        storage_path: Optional[str] = None,
    ):
        self.max_history = max_history
        self.auto_save = auto_save
        self.storage_path = storage_path
        
        self._history: List[ClipboardEntry] = []
        self._last_content_hash: Optional[str] = None
        self._filter_keywords: Set[str] = set()
        self._blocked_apps: Set[str] = set()
        self._favorite_ids: Set[str] = set()
    
    def get_current_content(self) -> Optional[ClipboardEntry]:
        """Get current clipboard content.
        
        Returns:
            ClipboardEntry or None
        """
        try:
            result = subprocess.run(
                ['pbpaste'],
                capture_output=True,
                timeout=2,
            )
            
            if result.returncode == 0 and result.stdout:
                text = result.stdout.decode('utf-8', errors='replace')
                
                content_hash = str(hash(text))
                if content_hash == self._last_content_hash:
                    return None
                
                self._last_content_hash = content_hash
                
                entry = ClipboardEntry(
                    id=self._generate_id(),
                    content_type=self._detect_content_type(text),
                    text_content=text,
                    data=result.stdout,
                )
                
                return entry
                
        except Exception:
            pass
        
        return None
    
    def _detect_content_type(self, text: str) -> ClipboardContentType:
        """Detect content type from text."""
        text_lower = text.lower().strip()
        
        if text_lower.startswith('http://') or text_lower.startswith('https://'):
            return ClipboardContentType.URL
        
        if text_lower.startswith('<!doctype html') or '<html' in text_lower[:200]:
            return ClipboardContentType.HTML
        
        if text_lower.startswith('{\\rtf'):
            return ClipboardContentType.RTF
        
        if '\n' in text or len(text) > 50:
            return ClipboardContentType.TEXT
        
        return ClipboardContentType.TEXT
    
    def _generate_id(self) -> str:
        """Generate unique entry ID."""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def add_to_history(self, entry: ClipboardEntry) -> bool:
        """Add entry to clipboard history.
        
        Args:
            entry: Entry to add
            
        Returns:
            True if added successfully
        """
        if entry.source_app in self._blocked_apps:
            return False
        
        if entry.text_content:
            for keyword in self._filter_keywords:
                if keyword.lower() in entry.text_content.lower():
                    return False
        
        self._history.insert(0, entry)
        
        if len(self._history) > self.max_history:
            excess = self._history[self.max_history:]
            self._history = self._history[:self.max_history]
        
        return True
    
    def get_history(
        self,
        limit: Optional[int] = None,
        content_type: Optional[ClipboardContentType] = None,
        filter_query: Optional[str] = None,
        favorites_only: bool = False,
    ) -> List[ClipboardEntry]:
        """Get clipboard history with filtering.
        
        Args:
            limit: Maximum entries to return
            content_type: Filter by content type
            filter_query: Text filter query
            favorites_only: Only return favorites
            
        Returns:
            Filtered list of entries
        """
        entries = self._history
        
        if content_type:
            entries = [e for e in entries if e.content_type == content_type]
        
        if filter_query:
            entries = [e for e in entries if e.matches_filter(filter_query)]
        
        if favorites_only:
            entries = [e for e in entries if e.is_favorite or e.id in self._favorite_ids]
        
        if limit:
            entries = entries[:limit]
        
        return entries
    
    def search(self, query: str, limit: int = 10) -> List[ClipboardEntry]:
        """Search clipboard history.
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            Matching entries
        """
        return self.get_history(filter_query=query, limit=limit)
    
    def copy_entry(self, entry_id: str) -> bool:
        """Copy a history entry back to clipboard.
        
        Args:
            entry_id: ID of entry to copy
            
        Returns:
            True if successful
        """
        for entry in self._history:
            if entry.id == entry_id:
                try:
                    content = entry.text_content or ''
                    process = subprocess.Popen(
                        ['pbcopy'],
                        stdin=subprocess.PIPE,
                    )
                    process.communicate(input=content.encode('utf-8'))
                    
                    entry.use_count += 1
                    self._last_content_hash = str(hash(content))
                    
                    return True
                except Exception:
                    pass
        
        return False
    
    def delete_entry(self, entry_id: str) -> bool:
        """Delete a history entry.
        
        Args:
            entry_id: ID of entry to delete
            
        Returns:
            True if deleted
        """
        for i, entry in enumerate(self._history):
            if entry.id == entry_id:
                self._history.pop(i)
                return True
        
        return False
    
    def toggle_favorite(self, entry_id: str) -> bool:
        """Toggle favorite status for an entry.
        
        Args:
            entry_id: ID of entry
            
        Returns:
            New favorite status
        """
        for entry in self._history:
            if entry.id == entry_id:
                entry.is_favorite = not entry.is_favorite
                if entry.is_favorite:
                    self._favorite_ids.add(entry_id)
                else:
                    self._favorite_ids.discard(entry_id)
                return entry.is_favorite
        
        return False
    
    def clear_history(self, keep_favorites: bool = True) -> None:
        """Clear clipboard history.
        
        Args:
            keep_favorites: Whether to keep favorite entries
        """
        if keep_favorites:
            self._history = [
                e for e in self._history
                if e.is_favorite or e.id in self._favorite_ids
            ]
        else:
            self._history.clear()
            self._favorite_ids.clear()
    
    def add_filter_keyword(self, keyword: str) -> None:
        """Add a filter keyword (entries containing it won't be saved)."""
        self._filter_keywords.add(keyword)
    
    def remove_filter_keyword(self, keyword: str) -> None:
        """Remove a filter keyword."""
        self._filter_keywords.discard(keyword)
    
    def block_app(self, app_name: str) -> None:
        """Block clipboard entries from an app."""
        self._blocked_apps.add(app_name)
    
    def unblock_app(self, app_name: str) -> None:
        """Unblock clipboard entries from an app."""
        self._blocked_apps.discard(app_name)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get clipboard usage statistics."""
        total = len(self._history)
        
        type_counts: Dict[str, int] = {}
        for entry in self._history:
            type_name = entry.content_type.name
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
        
        return {
            'total_entries': total,
            'favorites': len(self._favorite_ids),
            'by_type': type_counts,
            'filter_keywords': list(self._filter_keywords),
            'blocked_apps': list(self._blocked_apps),
        }


class ClipboardMonitor:
    """Monitors clipboard for changes."""
    
    def __init__(self, manager: ClipboardManager, poll_interval: float = 0.5):
        self.manager = manager
        self.poll_interval = poll_interval
        self._is_running = False
        self._callbacks: List[callable] = []
    
    def start(self) -> None:
        """Start monitoring clipboard."""
        self._is_running = True
    
    def stop(self) -> None:
        """Stop monitoring clipboard."""
        self._is_running = False
    
    def poll(self) -> Optional[ClipboardEntry]:
        """Poll for new clipboard content."""
        if not self._is_running:
            return None
        
        entry = self.manager.get_current_content()
        
        if entry:
            self.manager.add_to_history(entry)
            
            for callback in self._callbacks:
                try:
                    callback(entry)
                except Exception:
                    pass
        
        return entry
    
    def add_callback(self, callback: callable) -> None:
        """Add callback for new clipboard entries."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: callable) -> None:
        """Remove a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
