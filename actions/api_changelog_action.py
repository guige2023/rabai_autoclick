"""
API Changelog Management Action.

Tracks, formats, and manages API changelogs with support for
semantic versioning, breaking change detection, and multi-format export.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class ChangeType(Enum):
    """Categories of API changes."""
    ADDED = auto()
    CHANGED = auto()
    DEPRECATED = auto()
    REMOVED = auto()
    FIXED = auto()
    SECURITY = auto()
    BREAKING = auto()
    PERFORMANCE = auto()


class ChangeScope(Enum):
    """Scope of the change."""
    ENDPOINT = auto()
    PARAMETER = auto()
    RESPONSE = auto()
    AUTHENTICATION = auto()
    AUTHORIZATION = auto()
    SCHEMA = auto()
    ERROR_CODE = auto()
    SDK = auto()
    DOCUMENTATION = auto()


@dataclass
class ChangelogEntry:
    """A single changelog entry."""
    version: str
    change_type: ChangeType
    scope: ChangeScope
    description: str
    breaking: bool = False
    affected_endpoints: List[str] = field(default_factory=list)
    migration_guide: Optional[str] = None
    date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    author: str = "unknown"
    ticket_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.breaking or self.change_type in (ChangeType.BREAKING, ChangeType.REMOVED):
            self.breaking = True


@dataclass
class APIVersion:
    """A complete API version with all changes."""
    version: str
    release_date: datetime
    entries: List[ChangelogEntry] = field(default_factory=list)
    summary: str = ""
    download_url: Optional[str] = None
    swagger_url: Optional[str] = None
    deprecation_date: Optional[datetime] = None
    sunset_date: Optional[datetime] = None

    def is_deprecated(self) -> bool:
        if self.deprecation_date and datetime.now(timezone.utc) >= self.deprecation_date:
            return True
        return False

    def is_sunset(self) -> bool:
        if self.sunset_date and datetime.now(timezone.utc) >= self.sunset_date:
            return True
        return False

    @property
    def breaking_changes(self) -> List[ChangelogEntry]:
        return [e for e in self.entries if e.breaking]


class APIChangelogManager:
    """
    Manages API changelogs with semantic versioning support.

    Example:
        manager = APIChangelogManager("my-api")
        manager.add_version("2.0.0", "Major release with breaking changes")
        manager.add_change("2.0.0", ChangeType.BREAKING, ChangeScope.ENDPOINT,
                          "Authentication method changed from API Key to OAuth 2.0",
                          migration_guide="Update your client to use OAuth 2.0 tokens")
        changelog = manager.generate_markdown()
    """

    CHANGE_TYPE_SYMBOLS = {
        ChangeType.ADDED: "✨",
        ChangeType.CHANGED: "🔄",
        ChangeType.DEPRECATED: "⚠️",
        ChangeType.REMOVED: "🗑️",
        ChangeType.FIXED: "🐛",
        ChangeType.SECURITY: "🔒",
        ChangeType.BREAKING: "💥",
        ChangeType.PERFORMANCE: "⚡",
    }

    def __init__(self, api_name: str) -> None:
        self.api_name = api_name
        self._versions: Dict[str, APIVersion] = {}
        self._current_version: Optional[str] = None

    def add_version(
        self,
        version: str,
        release_date: Optional[datetime] = None,
        summary: str = "",
    ) -> Self:
        """Add a new version to the changelog."""
        self._versions[version] = APIVersion(
            version=version,
            release_date=release_date or datetime.now(timezone.utc),
            summary=summary,
        )
        self._current_version = version
        return self

    def add_change(
        self,
        version: str,
        change_type: ChangeType,
        scope: ChangeScope,
        description: str,
        *,
        breaking: bool = False,
        endpoints: Optional[List[str]] = None,
        migration_guide: Optional[str] = None,
        author: str = "unknown",
        ticket_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Self:
        """Add a change to a specific version."""
        if version not in self._versions:
            self.add_version(version)

        entry = ChangelogEntry(
            version=version,
            change_type=change_type,
            scope=scope,
            description=description,
            breaking=breaking,
            affected_endpoints=endpoints or [],
            migration_guide=migration_guide,
            author=author,
            ticket_id=ticket_id,
            tags=tags or [],
        )
        self._versions[version].entries.append(entry)
        return self

    def get_version(self, version: str) -> Optional[APIVersion]:
        """Get a specific version."""
        return self._versions.get(version)

    def latest_version(self) -> Optional[str]:
        """Get the latest version string."""
        if not self._versions:
            return None
        return max(self._versions.keys(), key=lambda v: self._parse_version(v))

    def generate_markdown(self, include_deprecated: bool = True) -> str:
        """Generate changelog in Markdown format."""
        lines = [f"# {self.api_name} API Changelog", ""]

        sorted_versions = sorted(
            self._versions.values(),
            key=lambda v: self._parse_version(v.version),
            reverse=True,
        )

        for version in sorted_versions:
            if not include_deprecated and version.is_sunset():
                continue

            date_str = version.release_date.strftime("%Y-%m-%d")
            lines.append(f"## Version {version.version} ({date_str})")
            if version.summary:
                lines.append(f"**{version.summary}**")
            lines.append("")

            # Group entries by change type
            by_type: Dict[ChangeType, List[ChangelogEntry]] = {}
            for entry in version.entries:
                by_type.setdefault(entry.change_type, []).append(entry)

            # Output in standard order
            for ct in ChangeType:
                if ct in by_type:
                    symbol = self.CHANGE_TYPE_SYMBOLS.get(ct, "•")
                    lines.append(f"### {symbol} {ct.name}")
                    for entry in by_type[ct]:
                        lines.append(f"- {entry.description}")
                        if entry.affected_endpoints:
                            lines.append(f"  - Endpoints: `{', '.join(entry.affected_endpoints)}`")
                        if entry.breaking and entry.migration_guide:
                            lines.append(f"  - Migration: {entry.migration_guide}")
                        if entry.ticket_id:
                            lines.append(f"  - Ticket: `{entry.ticket_id}`")
                    lines.append("")

            if version.is_deprecated():
                lines.append(f"⚠️ **DEPRECATED** (since {version.deprecation_date.strftime('%Y-%m-%d')})")
            if version.is_sunset():
                lines.append(f"🗑️ **SUNSET** (since {version.sunset_date.strftime('%Y-%m-%d')})")
            lines.append("---")
            lines.append("")

        return "\n".join(lines)

    def generate_json(self) -> Dict[str, Any]:
        """Generate changelog in JSON-serializable format."""
        return {
            "api_name": self.api_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "versions": [
                {
                    "version": v.version,
                    "release_date": v.release_date.isoformat(),
                    "summary": v.summary,
                    "entries": [
                        {
                            "type": e.change_type.name,
                            "scope": e.scope.name,
                            "description": e.description,
                            "breaking": e.breaking,
                            "affected_endpoints": e.affected_endpoints,
                            "migration_guide": e.migration_guide,
                            "author": e.author,
                            "ticket_id": e.ticket_id,
                        }
                        for e in v.entries
                    ],
                }
                for v in sorted(self._versions.values(),
                               key=lambda x: self._parse_version(x.version),
                               reverse=True)
            ],
        }

    def detect_breaking_changes(
        self,
        old_version: str,
        new_version: str,
    ) -> List[str]:
        """Detect breaking changes between two versions."""
        breaking = []
        old = self._versions.get(old_version)
        new = self._versions.get(new_version)
        if not old or not new:
            return breaking

        for entry in new.entries:
            if entry.breaking:
                breaking.append(
                    f"[{entry.version}] {entry.change_type.name}: {entry.description}"
                )
        return breaking

    def _parse_version(self, version: str) -> Tuple[int, int, int]:
        """Parse semver string into tuple for comparison."""
        match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
        if match:
            return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return (0, 0, 0)
