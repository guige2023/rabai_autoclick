"""Automation Registry Action.

Registry for managing and discovering automation workflows.
"""
from typing import Any, Callable, Dict, List, Optional, Type
from dataclasses import dataclass, field
import time


@dataclass
class AutomationEntry:
    automation_id: str
    name: str
    description: str
    category: str
    version: str
    fn: Callable
    tags: List[str] = field(default_factory=list)
    author: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationRegistryAction:
    """Registry for discovering and managing automations."""

    def __init__(self) -> None:
        self.entries: Dict[str, AutomationEntry] = {}
        self._categories: Dict[str, List[str]] = {}

    def register(
        self,
        automation_id: str,
        name: str,
        fn: Callable,
        description: str = "",
        category: str = "general",
        version: str = "1.0.0",
        tags: Optional[List[str]] = None,
        author: Optional[str] = None,
        **metadata,
    ) -> AutomationEntry:
        entry = AutomationEntry(
            automation_id=automation_id,
            name=name,
            description=description,
            category=category,
            version=version,
            fn=fn,
            tags=tags or [],
            author=author,
            metadata=metadata,
        )
        self.entries[automation_id] = entry
        self._categories.setdefault(category, []).append(automation_id)
        return entry

    def get(self, automation_id: str) -> Optional[AutomationEntry]:
        return self.entries.get(automation_id)

    def find(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        query: Optional[str] = None,
    ) -> List[AutomationEntry]:
        results = list(self.entries.values())
        if category:
            results = [e for e in results if e.category == category]
        if tags:
            results = [e for e in results if any(t in e.tags for t in tags)]
        if query:
            q = query.lower()
            results = [e for e in results if q in e.name.lower() or q in e.description.lower()]
        return results

    def list_categories(self) -> List[str]:
        return list(self._categories.keys())

    def get_by_category(self, category: str) -> List[AutomationEntry]:
        ids = self._categories.get(category, [])
        return [self.entries[i] for i in ids if i in self.entries]
