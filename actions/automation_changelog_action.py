"""
Automation Changelog Action Module.

Provides changelog management and version tracking
for automated workflow releases.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of changes."""
    ADDED = "added"
    CHANGED = "changed"
    DEPRECATED = "deprecated"
    REMOVED = "removed"
    FIXED = "fixed"
    SECURITY = "security"


@dataclass
class Change:
    """Single change entry."""
    change_id: str
    change_type: ChangeType
    description: str
    component: str
    breaking: bool = False
    issues: List[str] = field(default_factory=list)


@dataclass
class ChangelogEntry:
    """Changelog entry for a version."""
    version: str
    release_date: datetime
    changes: List[Change] = field(default_factory=list)
    summary: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Changelog:
    """Complete changelog."""
    changelog_id: str
    name: str
    entries: List[ChangelogEntry] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)


class ChangelogManager:
    """Manages changelogs."""

    def __init__(self, name: str = "Project"):
        self.name = name
        self.entries: Dict[str, ChangelogEntry] = {}
        self._handlers: List[Callable] = []

    def add_entry(self, entry: ChangelogEntry):
        """Add changelog entry."""
        self.entries[entry.version] = entry
        self._notify_handlers(entry)

    def remove_entry(self, version: str) -> bool:
        """Remove changelog entry."""
        if version in self.entries:
            del self.entries[version]
            return True
        return False

    def get_entry(self, version: str) -> Optional[ChangelogEntry]:
        """Get changelog entry."""
        return self.entries.get(version)

    def get_latest(self) -> Optional[ChangelogEntry]:
        """Get latest changelog entry."""
        if not self.entries:
            return None

        sorted_entries = sorted(
            self.entries.values(),
            key=lambda e: e.release_date,
            reverse=True
        )
        return sorted_entries[0]

    def add_change(
        self,
        version: str,
        change_type: ChangeType,
        description: str,
        component: str,
        breaking: bool = False
    ):
        """Add change to version entry."""
        if version not in self.entries:
            self.entries[version] = ChangelogEntry(
                version=version,
                release_date=datetime.now()
            )

        change = Change(
            change_id=str(id(self)),
            change_type=change_type,
            description=description,
            component=component,
            breaking=breaking
        )
        self.entries[version].changes.append(change)

    def generate_markdown(self) -> str:
        """Generate changelog in Markdown format."""
        lines = [f"# Changelog - {self.name}", ""]

        sorted_entries = sorted(
            self.entries.values(),
            key=lambda e: e.release_date,
            reverse=True
        )

        for entry in sorted_entries:
            lines.append(f"## {entry.version} ({entry.release_date.strftime('%Y-%m-%d')})")
            lines.append("")

            if entry.summary:
                lines.append(f"{entry.summary}")
                lines.append("")

            changes_by_type: Dict[ChangeType, List[Change]] = {}
            for change in entry.changes:
                if change.change_type not in changes_by_type:
                    changes_by_type[change.change_type] = []
                changes_by_type[change.change_type].append(change)

            for change_type in [ChangeType.ADDED, ChangeType.CHANGED, ChangeType.DEPRECATED,
                              ChangeType.REMOVED, ChangeType.FIXED, ChangeType.SECURITY]:
                changes = changes_by_type.get(change_type, [])
                if changes:
                    lines.append(f"### {change_type.value.upper()}")
                    lines.append("")
                    for change in changes:
                        breaking_marker = " **[BREAKING]**" if change.breaking else ""
                        lines.append(f"- {change.description}{breaking_marker}")
                    lines.append("")

        return "\n".join(lines)

    def register_handler(self, handler: Callable):
        """Register changelog update handler."""
        self._handlers.append(handler)

    def _notify_handlers(self, entry: ChangelogEntry):
        """Notify handlers of new entry."""
        for handler in self._handlers:
            try:
                handler(entry)
            except Exception as e:
                logger.error(f"Changelog handler error: {e}")


def main():
    """Demonstrate changelog management."""
    manager = ChangelogManager(name="My Project")

    manager.add_change("1.0.0", ChangeType.ADDED, "Initial release", "core")
    manager.add_change("1.0.0", ChangeType.ADDED, "User authentication", "auth")
    manager.add_change("1.1.0", ChangeType.CHANGED, "Improved performance", "core", breaking=True)
    manager.add_change("1.1.0", ChangeType.FIXED, "Fixed login bug", "auth")

    entry = manager.get_latest()
    print(f"Latest version: {entry.version}")
    print(f"Changes: {len(entry.changes)}")

    markdown = manager.generate_markdown()
    print(markdown)


if __name__ == "__main__":
    main()
