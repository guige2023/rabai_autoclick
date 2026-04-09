"""Automation Catalog Action Module.

Provides a catalog of automation capabilities, workflows, and templates
with search, discovery, and versioning capabilities.

Example:
    >>> from actions.automation.automation_catalog_action import AutomationCatalog
    >>> catalog = AutomationCatalog()
    >>> entry = await catalog.register_entry(workflow_def)
"""

from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading


class EntryStatus(Enum):
    """Status of a catalog entry."""
    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class EntryType(Enum):
    """Types of catalog entries."""
    WORKFLOW = "workflow"
    TEMPLATE = "template"
    ACTION = "action"
    FUNCTION = "function"
    SCRIPT = "script"
    INTEGRATION = "integration"


@dataclass
class Tag:
    """Tag for categorizing catalog entries.
    
    Attributes:
        name: Tag name
        category: Tag category
        description: Tag description
    """
    name: str
    category: Optional[str] = None
    description: str = ""


@dataclass
class Version:
    """Version information for a catalog entry.
    
    Attributes:
        version: Version string (e.g., '1.2.3')
        changelog: Changes in this version
        created_at: When version was created
        created_by: Who created this version
        is_stable: Whether this is a stable release
    """
    version: str
    changelog: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    is_stable: bool = True


@dataclass
class CatalogEntry:
    """An entry in the automation catalog.
    
    Attributes:
        entry_id: Unique entry identifier
        name: Entry name
        entry_type: Type of entry
        description: Human-readable description
        version: Current version
        tags: Set of tags
        status: Current status
        author: Entry author
        created_at: Creation timestamp
        modified_at: Last modification timestamp
        definition: Entry definition (workflow/template code)
        dependencies: List of dependency entry IDs
        metadata: Additional metadata
    """
    entry_id: str
    name: str
    entry_type: EntryType
    description: str = ""
    version: str = "1.0.0"
    tags: Set[str] = field(default_factory=set)
    status: EntryStatus = EntryStatus.DRAFT
    author: str = "system"
    created_at: datetime = field(default_factory=datetime.now)
    modified_at: datetime = field(default_factory=datetime.now)
    definition: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    versions: List[Version] = field(default_factory=list)


@dataclass
class CatalogConfig:
    """Configuration for catalog operations.
    
    Attributes:
        auto_index: Whether to auto-index new entries
        versioning_enabled: Whether to track versions
        allow_duplicates: Whether to allow duplicate names
        max_entries: Maximum entries (0 = unlimited)
        search_fuzzy: Enable fuzzy search
    """
    auto_index: bool = True
    versioning_enabled: bool = True
    allow_duplicates: bool = False
    max_entries: int = 0
    search_fuzzy: bool = True


class AutomationCatalog:
    """Manages an automation capability catalog.
    
    Provides discovery, versioning, and management of
    automation workflows and templates.
    
    Attributes:
        config: Catalog configuration
    
    Example:
        >>> catalog = AutomationCatalog()
        >>> entry = await catalog.register_entry(workflow_def)
        >>> results = await catalog.search("data processing")
    """
    
    def __init__(self, config: Optional[CatalogConfig] = None):
        """Initialize the automation catalog.
        
        Args:
            config: Catalog configuration
        """
        self.config = config or CatalogConfig()
        self._entries: Dict[str, CatalogEntry] = {}
        self._name_index: Dict[str, List[str]] = defaultdict(list)
        self._tag_index: Dict[str, List[str]] = defaultdict(list)
        self._type_index: Dict[EntryType, List[str]] = defaultdict(list)
        self._search_index: Dict[str, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()
        self._entry_counter = 0
    
    def _generate_id(self, name: str, entry_type: EntryType) -> str:
        """Generate a unique entry ID.
        
        Args:
            name: Entry name
            entry_type: Entry type
        
        Returns:
            Unique ID
        """
        self._entry_counter += 1
        raw = f"{name}_{entry_type.value}_{self._entry_counter}_{int(time.time())}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    
    async def register_entry(
        self,
        name: str,
        entry_type: EntryType,
        definition: Dict[str, Any],
        description: str = "",
        tags: Optional[Set[str]] = None,
        author: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ) -> CatalogEntry:
        """Register a new catalog entry.
        
        Args:
            name: Entry name
            entry_type: Type of entry
            definition: Entry definition
            description: Human-readable description
            tags: Set of tags
            author: Entry author
            metadata: Additional metadata
        
        Returns:
            Created catalog entry
        
        Raises:
            ValueError: If entry with same name exists and duplicates not allowed
        """
        entry_id = self._generate_id(name, entry_type)
        now = datetime.now()
        
        # Check for duplicates
        if not self.config.allow_duplicates:
            if name in self._name_index:
                existing = [self._entries[eid] for eid in self._name_index[name]]
                if any(e.status != EntryStatus.ARCHIVED for e in existing):
                    raise ValueError(f"Entry with name '{name}' already exists")
        
        entry = CatalogEntry(
            entry_id=entry_id,
            name=name,
            entry_type=entry_type,
            description=description,
            definition=definition,
            tags=tags or set(),
            author=author,
            metadata=metadata or {},
            versions=[Version(version="1.0.0", changelog="Initial version")]
        )
        
        with self._lock:
            self._entries[entry_id] = entry
            self._index_entry(entry)
        
        return entry
    
    def _index_entry(self, entry: CatalogEntry) -> None:
        """Index an entry for search.
        
        Args:
            entry: Entry to index
        """
        # Name index
        self._name_index[entry.name.lower()].append(entry.entry_id)
        
        # Type index
        self._type_index[entry.entry_type].append(entry.entry_id)
        
        # Tag index
        for tag in entry.tags:
            self._tag_index[tag.lower()].append(entry.entry_id)
        
        # Search index - index words from name, description, and tags
        words = self._extract_words(entry.name)
        words.extend(self._extract_words(entry.description))
        words.extend([t.lower() for t in entry.tags])
        
        for word in words:
            if len(word) >= 2:  # Skip single characters
                self._search_index[word.lower()].add(entry.entry_id)
    
    def _extract_words(self, text: str) -> List[str]:
        """Extract searchable words from text.
        
        Args:
            text: Text to extract from
        
        Returns:
            List of words
        """
        import re
        words = re.findall(r'\b\w+\b', text.lower())
        return [w for w in words if len(w) >= 2]
    
    async def update_entry(
        self,
        entry_id: str,
        updates: Dict[str, Any]
    ) -> CatalogEntry:
        """Update an existing catalog entry.
        
        Args:
            entry_id: Entry to update
            updates: Fields to update
        
        Returns:
            Updated entry
        
        Raises:
            ValueError: If entry not found
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                raise ValueError(f"Entry not found: {entry_id}")
        
        # Apply updates
        if "description" in updates:
            entry.description = updates["description"]
        if "tags" in updates:
            entry.tags = updates["tags"]
        if "definition" in updates:
            entry.definition = updates["definition"]
        if "metadata" in updates:
            entry.metadata.update(updates["metadata"])
        
        entry.modified_at = datetime.now()
        
        with self._lock:
            self._entries[entry_id] = entry
        
        return entry
    
    async def publish_entry(self, entry_id: str) -> CatalogEntry:
        """Publish a draft entry.
        
        Args:
            entry_id: Entry to publish
        
        Returns:
            Published entry
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                raise ValueError(f"Entry not found: {entry_id}")
        
        entry.status = EntryStatus.PUBLISHED
        entry.modified_at = datetime.now()
        
        return entry
    
    async def deprecate_entry(self, entry_id: str, reason: str = "") -> CatalogEntry:
        """Deprecate an entry.
        
        Args:
            entry_id: Entry to deprecate
            reason: Deprecation reason
        
        Returns:
            Deprecated entry
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                raise ValueError(f"Entry not found: {entry_id}")
        
        entry.status = EntryStatus.DEPRECATED
        entry.modified_at = datetime.now()
        entry.metadata["deprecation_reason"] = reason
        
        return entry
    
    async def archive_entry(self, entry_id: str) -> CatalogEntry:
        """Archive an entry.
        
        Args:
            entry_id: Entry to archive
        
        Returns:
            Archived entry
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                raise ValueError(f"Entry not found: {entry_id}")
        
        entry.status = EntryStatus.ARCHIVED
        entry.modified_at = datetime.now()
        
        return entry
    
    async def add_version(
        self,
        entry_id: str,
        version: str,
        changelog: str = "",
        author: str = "system"
    ) -> CatalogEntry:
        """Add a new version to an entry.
        
        Args:
            entry_id: Entry to version
            version: New version string
            changelog: Changes in this version
            author: Who created this version
        
        Returns:
            Updated entry
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                raise ValueError(f"Entry not found: {entry_id}")
        
        entry.version = version
        entry.modified_at = datetime.now()
        
        version_obj = Version(
            version=version,
            changelog=changelog,
            created_by=author
        )
        entry.versions.append(version_obj)
        
        return entry
    
    async def get_entry(self, entry_id: str) -> Optional[CatalogEntry]:
        """Get a catalog entry by ID.
        
        Args:
            entry_id: Entry identifier
        
        Returns:
            CatalogEntry or None
        """
        with self._lock:
            return self._entries.get(entry_id)
    
    async def get_by_name(self, name: str) -> List[CatalogEntry]:
        """Get entries by name.
        
        Args:
            name: Entry name
        
        Returns:
            List of matching entries
        """
        with self._lock:
            entry_ids = self._name_index.get(name.lower(), [])
            return [self._entries[eid] for eid in entry_ids if eid in self._entries]
    
    async def search(
        self,
        query: str,
        entry_type: Optional[EntryType] = None,
        tags: Optional[Set[str]] = None,
        status: Optional[EntryStatus] = None,
        limit: int = 50
    ) -> List[CatalogEntry]:
        """Search catalog entries.
        
        Args:
            query: Search query
            entry_type: Filter by entry type
            tags: Filter by tags
            status: Filter by status
            limit: Maximum results
        
        Returns:
            List of matching entries with relevance scores
        """
        query_words = self._extract_words(query)
        
        if not query_words:
            return []
        
        # Find matching entry IDs
        matching_ids: Set[str] = None
        
        for word in query_words:
            word_ids = self._search_index.get(word.lower(), set())
            if matching_ids is None:
                matching_ids = word_ids.copy()
            else:
                matching_ids &= word_ids
        
        if matching_ids is None:
            matching_ids = set()
        
        # Score and filter results
        scored_results: List[Tuple[str, float]] = []
        
        with self._lock:
            for entry_id in matching_ids:
                entry = self._entries[entry_id]
                
                # Apply filters
                if entry_type and entry.entry_type != entry_type:
                    continue
                if status and entry.status != status:
                    continue
                if tags and not (entry.tags & tags):
                    continue
                
                # Calculate relevance score
                score = self._calculate_relevance(entry, query_words)
                scored_results.append((entry_id, score))
        
        # Sort by score
        scored_results.sort(key=lambda x: x[1], reverse=True)
        
        # Return top results
        results = []
        for entry_id, score in scored_results[:limit]:
            with self._lock:
                results.append(self._entries[entry_id])
        
        return results
    
    def _calculate_relevance(self, entry: CatalogEntry, query_words: List[str]) -> float:
        """Calculate search relevance score.
        
        Args:
            entry: Entry to score
            query_words: Query words
        
        Returns:
            Relevance score (higher = more relevant)
        """
        score = 0.0
        
        entry_text = f"{entry.name} {entry.description}".lower()
        
        for word in query_words:
            word_lower = word.lower()
            
            # Exact match in name (highest weight)
            if word_lower in entry.name.lower():
                score += 10.0
            
            # Exact match in description
            if word_lower in entry.description.lower():
                score += 5.0
            
            # Tag match
            if word_lower in [t.lower() for t in entry.tags]:
                score += 3.0
            
            # Partial match
            if word_lower in entry_text:
                score += 1.0
        
        # Boost published entries
        if entry.status == EntryStatus.PUBLISHED:
            score *= 1.5
        elif entry.status == EntryStatus.DEPRECATED:
            score *= 0.5
        
        return score
    
    async def get_by_tag(self, tag: str) -> List[CatalogEntry]:
        """Get all entries with a specific tag.
        
        Args:
            tag: Tag to search for
        
        Returns:
            List of entries with the tag
        """
        with self._lock:
            entry_ids = self._tag_index.get(tag.lower(), [])
            return [self._entries[eid] for eid in entry_ids if eid in self._entries]
    
    async def get_by_type(self, entry_type: EntryType) -> List[CatalogEntry]:
        """Get all entries of a specific type.
        
        Args:
            entry_type: Entry type to filter
        
        Returns:
            List of entries
        """
        with self._lock:
            entry_ids = self._type_index.get(entry_type, [])
            return [self._entries[eid] for eid in entry_ids if eid in self._entries]
    
    async def get_recent(self, limit: int = 10) -> List[CatalogEntry]:
        """Get recently modified entries.
        
        Args:
            limit: Maximum results
        
        Returns:
            List of recent entries
        """
        with self._lock:
            sorted_entries = sorted(
                self._entries.values(),
                key=lambda e: e.modified_at,
                reverse=True
            )
            return sorted_entries[:limit]
    
    async def get_dependencies(self, entry_id: str) -> List[CatalogEntry]:
        """Get all dependencies of an entry.
        
        Args:
            entry_id: Entry ID
        
        Returns:
            List of dependency entries
        """
        with self._lock:
            entry = self._entries.get(entry_id)
            if not entry:
                return []
            
            dependencies = []
            for dep_id in entry.dependencies:
                if dep_id in self._entries:
                    dependencies.append(self._entries[dep_id])
            
            return dependencies
    
    async def find_usages(self, entry_id: str) -> List[CatalogEntry]:
        """Find all entries that depend on this entry.
        
        Args:
            entry_id: Entry to find usages for
        
        Returns:
            List of entries that depend on this one
        """
        with self._lock:
            usages = []
            for entry in self._entries.values():
                if entry_id in entry.dependencies:
                    usages.append(entry)
            return usages
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics.
        
        Returns:
            Statistics dictionary
        """
        with self._lock:
            by_type = {}
            by_status = {}
            
            for entry in self._entries.values():
                type_key = entry.entry_type.value
                by_type[type_key] = by_type.get(type_key, 0) + 1
                
                status_key = entry.status.value
                by_status[status_key] = by_status.get(status_key, 0) + 1
            
            return {
                "total_entries": len(self._entries),
                "by_type": by_type,
                "by_status": by_status,
                "total_tags": len(self._tag_index),
                "total_versions": sum(len(e.versions) for e in self._entries.values())
            }
    
    async def export_catalog(self) -> Dict[str, Any]:
        """Export the full catalog.
        
        Returns:
            Catalog data
        """
        with self._lock:
            return {
                "export_time": datetime.now().isoformat(),
                "entries": [
                    {
                        "entry_id": e.entry_id,
                        "name": e.name,
                        "entry_type": e.entry_type.value,
                        "description": e.description,
                        "version": e.version,
                        "tags": list(e.tags),
                        "status": e.status.value,
                        "author": e.author,
                        "created_at": e.created_at.isoformat(),
                        "modified_at": e.modified_at.isoformat(),
                        "definition": e.definition,
                        "dependencies": e.dependencies,
                        "metadata": e.metadata
                    }
                    for e in self._entries.values()
                ]
            }
    
    async def import_catalog(self, data: Dict[str, Any]) -> int:
        """Import catalog entries.
        
        Args:
            data: Export data to import
        
        Returns:
            Number of entries imported
        """
        count = 0
        
        for entry_data in data.get("entries", []):
            try:
                entry = CatalogEntry(
                    entry_id=entry_data["entry_id"],
                    name=entry_data["name"],
                    entry_type=EntryType(entry_data["entry_type"]),
                    description=entry_data.get("description", ""),
                    version=entry_data.get("version", "1.0.0"),
                    tags=set(entry_data.get("tags", [])),
                    status=EntryStatus(entry_data.get("status", "draft")),
                    author=entry_data.get("author", "system"),
                    definition=entry_data.get("definition", {}),
                    dependencies=entry_data.get("dependencies", []),
                    metadata=entry_data.get("metadata", {})
                )
                
                with self._lock:
                    self._entries[entry.entry_id] = entry
                    self._index_entry(entry)
                
                count += 1
            except Exception:
                continue
        
        return count
