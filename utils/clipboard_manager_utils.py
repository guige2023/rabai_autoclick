"""
Clipboard management and history utilities.

Provides utilities for reading, writing, and managing clipboard
content with history tracking and content transformation.
"""

from __future__ import annotations

import subprocess
import time
from typing import List, Optional, Dict, Any, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json


class ClipboardContentType(Enum):
    """Types of clipboard content."""
    TEXT = "text"
    RTF = "rtf"
    HTML = "html"
    IMAGE = "image"
    FILE = "file"
    UNKNOWN = "unknown"


@dataclass
class ClipboardItem:
    """Represents a clipboard item."""
    content: Any
    content_type: ClipboardContentType
    timestamp: datetime
    source: Optional[str] = None
    preview: Optional[str] = None
    
    def __post_init__(self):
        if self.preview is None:
            if isinstance(self.content, str):
                self.preview = self.content[:100] if len(self.content) > 100 else self.content
            else:
                self.preview = f"<{self.content_type.value}>"


@dataclass
class ClipboardHistoryConfig:
    """Configuration for clipboard history."""
    max_items: int = 100
    max_age_hours: float = 24.0
    store_images: bool = True
    store_files: bool = True
    duplicate_check: bool = True
    on_change_callback: Optional[Callable[[ClipboardItem], None]] = None


class ClipboardManager:
    """Manages clipboard operations and history."""
    
    def __init__(self, config: Optional[ClipboardHistoryConfig] = None):
        """Initialize clipboard manager.
        
        Args:
            config: Optional history configuration
        """
        self.config = config or ClipboardHistoryConfig()
        self._history: List[ClipboardItem] = []
        self._last_content_hash: Optional[int] = None
        self._monitoring = False
        self._monitor_callback: Optional[Callable] = None
    
    def copy_text(self, text: str, store_history: bool = True) -> bool:
        """Copy text to clipboard.
        
        Args:
            text: Text to copy
            store_history: Whether to add to history
            
        Returns:
            True if successful
        """
        try:
            result = subprocess.run(
                ["pbcopy"],
                input=text.encode("utf-8"),
                capture_output=True,
                timeout=2
            )
            
            if store_history:
                item = ClipboardItem(
                    content=text,
                    content_type=ClipboardContentType.TEXT,
                    timestamp=datetime.now(),
                    preview=text[:200]
                )
                self._add_to_history(item)
            
            return result.returncode == 0
        except Exception:
            return False
    
    def paste_text(self) -> Optional[str]:
        """Get text from clipboard.
        
        Returns:
            Clipboard text or None
        """
        try:
            result = subprocess.run(
                ["pbpaste"],
                capture_output=True,
                timeout=2
            )
            
            if result.returncode == 0:
                return result.stdout.decode("utf-8")
        except Exception:
            pass
        
        return None
    
    def copy_image(self, image_path: str, store_history: bool = True) -> bool:
        """Copy image file to clipboard.
        
        Args:
            image_path: Path to image file
            store_history: Whether to add to history
            
        Returns:
            True if successful
        """
        try:
            script = f'''
            set theImage to POSIX file "{image_path}"
            set theClipboard to (read theImage as TIFF)
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            
            if store_history:
                with open(image_path, "rb") as f:
                    image_data = f.read()
                
                item = ClipboardItem(
                    content=image_path,
                    content_type=ClipboardContentType.IMAGE,
                    timestamp=datetime.now()
                )
                self._add_to_history(item)
            
            return result.returncode == 0
        except Exception:
            return False
    
    def copy_file(self, file_path: str, store_history: bool = True) -> bool:
        """Copy file to clipboard.
        
        Args:
            file_path: Path to file
            store_history: Whether to add to history
            
        Returns:
            True if successful
        """
        try:
            script = f'''
            set theFile to POSIX file "{file_path}"
            set the clipboard to theFile
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            
            if store_history:
                item = ClipboardItem(
                    content=file_path,
                    content_type=ClipboardContentType.FILE,
                    timestamp=datetime.now(),
                    preview=file_path.split("/")[-1]
                )
                self._add_to_history(item)
            
            return result.returncode == 0
        except Exception:
            return False
    
    def get_content_type(self) -> ClipboardContentType:
        """Get the type of content currently on clipboard.
        
        Returns:
            ClipboardContentType
        """
        try:
            # Check for file
            result = subprocess.run(
                ["osascript", "-e", 
                 'tell application "System Events" to get class of clipboard'],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if "file" in result.stdout.lower():
                return ClipboardContentType.FILE
            
            # Check for image
            result = subprocess.run(
                ["osascript", "-e",
                 'try\ntell me to do shell script "file $(pbpaste | head -c 100)"\nend try'],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if "image" in result.stdout.lower():
                return ClipboardContentType.IMAGE
            
        except Exception:
            pass
        
        return ClipboardContentType.TEXT
    
    def clear(self) -> bool:
        """Clear the clipboard.
        
        Returns:
            True if successful
        """
        try:
            subprocess.run(
                ["pbcopy"],
                input=b"",
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def has_content(self) -> bool:
        """Check if clipboard has content.
        
        Returns:
            True if clipboard has content
        """
        try:
            result = subprocess.run(
                ["osascript", "-e", 
                 'tell application "System Events" to clipboard_size'],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                size = int(result.stdout.strip())
                return size > 0
        except Exception:
            pass
        
        return False
    
    def _add_to_history(self, item: ClipboardItem) -> None:
        """Add item to history.
        
        Args:
            item: ClipboardItem to add
        """
        # Check for duplicates
        if self.config.duplicate_check:
            content_hash = hash(str(item.content)[:1000])
            if content_hash == self._last_content_hash:
                return
            self._last_content_hash = content_hash
        
        # Add to history
        self._history.insert(0, item)
        
        # Trim history
        if len(self._history) > self.config.max_items:
            self._history = self._history[:self.config.max_items]
        
        # Run callback
        if self.config.on_change_callback:
            try:
                self.config.on_change_callback(item)
            except Exception:
                pass
    
    def get_history(
        self,
        content_type: Optional[ClipboardContentType] = None,
        limit: int = 10
    ) -> List[ClipboardItem]:
        """Get clipboard history.
        
        Args:
            content_type: Filter by content type
            limit: Maximum items to return
            
        Returns:
            List of ClipboardItem
        """
        items = self._history
        
        if content_type:
            items = [i for i in items if i.content_type == content_type]
        
        return items[:limit]
    
    def search_history(
        self,
        query: str,
        content_type: Optional[ClipboardContentType] = None
    ) -> List[ClipboardItem]:
        """Search clipboard history.
        
        Args:
            query: Search query
            content_type: Optional content type filter
            
        Returns:
            Matching items
        """
        results = []
        query_lower = query.lower()
        
        for item in self._history:
            if content_type and item.content_type != content_type:
                continue
            
            if isinstance(item.content, str):
                if query_lower in item.content.lower():
                    results.append(item)
            elif item.preview and query_lower in item.preview.lower():
                results.append(item)
        
        return results
    
    def paste_from_history(self, index: int) -> bool:
        """Paste item from history by index.
        
        Args:
            index: History index
            
        Returns:
            True if successful
        """
        if index < 0 or index >= len(self._history):
            return False
        
        item = self._history[index]
        
        if item.content_type == ClipboardContentType.TEXT:
            return self.copy_text(item.content, store_history=False)
        elif item.content_type == ClipboardContentType.FILE:
            return self.copy_file(item.content, store_history=False)
        
        return False
    
    def start_monitoring(
        self,
        callback: Callable[[ClipboardItem], None],
        interval: float = 0.5
    ) -> None:
        """Start monitoring clipboard changes.
        
        Args:
            callback: Function to call on clipboard change
            interval: Check interval in seconds
        """
        self._monitor_callback = callback
        self._monitoring = True
        self._monitor_loop(callback, interval)
    
    def _monitor_loop(
        self,
        callback: Callable,
        interval: float
    ) -> None:
        """Internal monitoring loop."""
        import threading
        
        last_hash = None
        
        def check():
            nonlocal last_hash
            try:
                content = self.paste_text()
                if content:
                    content_hash = hash(content[:1000])
                    if content_hash != last_hash:
                        last_hash = content_hash
                        item = ClipboardItem(
                            content=content,
                            content_type=ClipboardContentType.TEXT,
                            timestamp=datetime.now()
                        )
                        self._add_to_history(item)
                        callback(item)
            except Exception:
                pass
        
        def loop():
            while self._monitoring:
                check()
                time.sleep(interval)
        
        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
    
    def stop_monitoring(self) -> None:
        """Stop monitoring clipboard."""
        self._monitoring = False
    
    def clear_history(self) -> None:
        """Clear clipboard history."""
        self._history.clear()
        self._last_content_hash = None


class ClipboardTransformer:
    """Transforms clipboard content."""
    
    def __init__(self):
        """Initialize transformer."""
        self._transforms: Dict[str, Callable[[str], str]] = {}
        self._setup_default_transforms()
    
    def _setup_default_transforms(self) -> None:
        """Setup default transformations."""
        self.register("uppercase", lambda s: s.upper())
        self.register("lowercase", lambda s: s.lower())
        self.register("titlecase", lambda s: s.title())
        self.register("trim", lambda s: s.strip())
        self.register("collapse_whitespace", lambda s: " ".join(s.split()))
        self.register("remove_newlines", lambda s: s.replace("\n", " ").replace("\r", ""))
        self.register("slugify", lambda s: "-".join(s.lower().split()))
    
    def register(self, name: str, transform: Callable[[str], str]) -> None:
        """Register a transformation.
        
        Args:
            name: Transform name
            transform: Transform function
        """
        self._transforms[name] = transform
    
    def apply(self, text: str, transform_name: str) -> str:
        """Apply a transformation.
        
        Args:
            text: Input text
            transform_name: Name of transform
            
        Returns:
            Transformed text
        """
        if transform_name not in self._transforms:
            raise ValueError(f"Unknown transform: {transform_name}")
        
        return self._transforms[transform_name](text)
    
    def apply_chain(self, text: str, transform_names: List[str]) -> str:
        """Apply multiple transformations in sequence.
        
        Args:
            text: Input text
            transform_names: List of transform names
            
        Returns:
            Transformed text
        """
        result = text
        for name in transform_names:
            result = self.apply(result, name)
        return result
    
    def list_transforms(self) -> List[str]:
        """List available transforms.
        
        Returns:
            List of transform names
        """
        return list(self._transforms.keys())


# Convenience functions
def copy(text: str) -> bool:
    """Copy text to clipboard."""
    manager = ClipboardManager()
    return manager.copy_text(text)


def paste() -> Optional[str]:
    """Get text from clipboard."""
    manager = ClipboardManager()
    return manager.paste_text()


def copy_and_transform(text: str, transform: str) -> bool:
    """Copy text with transformation."""
    manager = ClipboardManager()
    transformer = ClipboardTransformer()
    transformed = transformer.apply(text, transform)
    return manager.copy_text(transformed)


def get_clipboard_history(limit: int = 10) -> List[ClipboardItem]:
    """Get clipboard history."""
    manager = ClipboardManager()
    return manager.get_history(limit=limit)
