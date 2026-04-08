"""
Shortcut registry utilities for managing automation shortcuts.

Provides registry of shortcuts with descriptions,
categories, and execution tracking.
"""

from __future__ import annotations

import json
import time
from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class ShortcutCategory(Enum):
    """Shortcut categories."""
    NAVIGATION = "navigation"
    EDITING = "editing"
    SYSTEM = "system"
    APPLICATION = "application"
    CUSTOM = "custom"


@dataclass
class Shortcut:
    """Automation shortcut definition."""
    id: str
    name: str
    hotkey: str
    category: ShortcutCategory
    description: str
    handler: Optional[Callable] = None
    tags: List[str] = field(default_factory=list)
    usage_count: int = 0
    last_used: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShortcutResult:
    """Result of shortcut execution."""
    shortcut: Shortcut
    success: bool
    message: str
    duration: float


class ShortcutRegistry:
    """Registry for automation shortcuts."""
    
    def __init__(self, name: str = "default"):
        """
        Initialize shortcut registry.
        
        Args:
            name: Registry name.
        """
        self.name = name
        self._shortcuts: Dict[str, Shortcut] = {}
        self._hotkey_index: Dict[str, str] = {}
    
    def register(self, shortcut: Shortcut) -> None:
        """
        Register a shortcut.
        
        Args:
            shortcut: Shortcut to register.
        """
        self._shortcuts[shortcut.id] = shortcut
        self._hotkey_index[shortcut.hotkey.lower()] = shortcut.id
    
    def unregister(self, shortcut_id: str) -> bool:
        """
        Unregister a shortcut.
        
        Args:
            shortcut_id: Shortcut ID.
            
        Returns:
            True if unregistered, False if not found.
        """
        if shortcut_id not in self._shortcuts:
            return False
        
        shortcut = self._shortcuts[shortcut_id]
        del self._hotkey_index[shortcut.hotkey.lower()]
        del self._shortcuts[shortcut_id]
        return True
    
    def get(self, shortcut_id: str) -> Optional[Shortcut]:
        """
        Get shortcut by ID.
        
        Args:
            shortcut_id: Shortcut ID.
            
        Returns:
            Shortcut or None.
        """
        return self._shortcuts.get(shortcut_id)
    
    def get_by_hotkey(self, hotkey: str) -> Optional[Shortcut]:
        """
        Get shortcut by hotkey.
        
        Args:
            hotkey: Hotkey string.
            
        Returns:
            Shortcut or None.
        """
        shortcut_id = self._hotkey_index.get(hotkey.lower())
        if shortcut_id:
            return self._shortcuts.get(shortcut_id)
        return None
    
    def execute(self, shortcut_id: str) -> ShortcutResult:
        """
        Execute shortcut by ID.
        
        Args:
            shortcut_id: Shortcut ID.
            
        Returns:
            ShortcutResult.
        """
        start = time.time()
        
        shortcut = self._shortcuts.get(shortcut_id)
        if not shortcut:
            return ShortcutResult(
                shortcut=Shortcut(id="", name="", hotkey="", category=ShortcutCategory.CUSTOM, description=""),
                success=False,
                message=f"Shortcut not found: {shortcut_id}",
                duration=time.time() - start
            )
        
        if not shortcut.handler:
            return ShortcutResult(
                shortcut=shortcut,
                success=False,
                message=f"No handler for: {shortcut.name}",
                duration=time.time() - start
            )
        
        try:
            shortcut.handler()
            shortcut.usage_count += 1
            shortcut.last_used = time.time()
            
            return ShortcutResult(
                shortcut=shortcut,
                success=True,
                message=f"Executed: {shortcut.name}",
                duration=time.time() - start
            )
        except Exception as e:
            return ShortcutResult(
                shortcut=shortcut,
                success=False,
                message=f"Execution error: {e}",
                duration=time.time() - start
            )
    
    def list_all(self) -> List[Shortcut]:
        """List all registered shortcuts."""
        return list(self._shortcuts.values())
    
    def list_by_category(self, category: ShortcutCategory) -> List[Shortcut]:
        """
        List shortcuts by category.
        
        Args:
            category: Category filter.
            
        Returns:
            List of Shortcut.
        """
        return [s for s in self._shortcuts.values() if s.category == category]
    
    def search(self, query: str) -> List[Shortcut]:
        """
        Search shortcuts by name, hotkey, or description.
        
        Args:
            query: Search query.
            
        Returns:
            List of matching Shortcut.
        """
        query_lower = query.lower()
        results = []
        
        for shortcut in self._shortcuts.values():
            if (query_lower in shortcut.name.lower() or
                query_lower in shortcut.hotkey.lower() or
                query_lower in shortcut.description.lower() or
                any(query_lower in tag.lower() for tag in shortcut.tags)):
                results.append(shortcut)
        
        return results
    
    def get_most_used(self, limit: int = 10) -> List[Shortcut]:
        """
        Get most used shortcuts.
        
        Args:
            limit: Max results.
            
        Returns:
            List of Shortcut sorted by usage.
        """
        return sorted(self._shortcuts.values(), 
                     key=lambda s: s.usage_count, reverse=True)[:limit]
    
    def export_json(self, path: str) -> bool:
        """
        Export registry to JSON.
        
        Args:
            path: Output file path.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            data = {
                'name': self.name,
                'shortcuts': [
                    {
                        'id': s.id,
                        'name': s.name,
                        'hotkey': s.hotkey,
                        'category': s.category.value,
                        'description': s.description,
                        'tags': s.tags,
                        'usage_count': s.usage_count,
                        'created_at': s.created_at,
                    }
                    for s in self._shortcuts.values()
                ]
            }
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False
    
    def import_json(self, path: str) -> int:
        """
        Import registry from JSON.
        
        Args:
            path: Input file path.
            
        Returns:
            Number of shortcuts imported.
        """
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            count = 0
            for item in data.get('shortcuts', []):
                shortcut = Shortcut(
                    id=item['id'],
                    name=item['name'],
                    hotkey=item['hotkey'],
                    category=ShortcutCategory(item.get('category', 'custom')),
                    description=item.get('description', ''),
                    tags=item.get('tags', []),
                    usage_count=item.get('usage_count', 0),
                    created_at=item.get('created_at', time.time()),
                )
                self.register(shortcut)
                count += 1
            
            return count
        except Exception:
            return 0


def create_shortcut(
    id: str,
    name: str,
    hotkey: str,
    category: ShortcutCategory,
    description: str = "",
    handler: Optional[Callable] = None,
    tags: Optional[List[str]] = None
) -> Shortcut:
    """
    Create a shortcut.
    
    Args:
        id: Shortcut ID.
        name: Shortcut name.
        hotkey: Hotkey string.
        category: Category.
        description: Description.
        handler: Optional callback.
        tags: Optional tags.
        
    Returns:
        New Shortcut.
    """
    return Shortcut(
        id=id,
        name=name,
        hotkey=hotkey,
        category=category,
        description=description,
        handler=handler,
        tags=tags or []
    )
