"""
Automation Changelog Action Module

Provides changelog generation, version tracking, and release management.
"""
from typing import Any, Optional, Literal
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import json


class ChangeType(Enum):
    """Type of change."""
    FEATURE = "feature"
    BUGFIX = "bugfix"
    REFACTOR = "refactor"
    DOCS = "docs"
    PERFORMANCE = "performance"
    SECURITY = "security"
    DEPRECATION = "deprecation"
    BREAKING = "breaking"
    OTHER = "other"


class ReleaseStatus(Enum):
    """Release status."""
    DRAFT = "draft"
    PREVIEW = "preview"
    STABLE = "stable"
    DEPRECATED = "deprecated"
    YANKED = "yanked"


@dataclass
class Change:
    """A single change entry."""
    change_type: ChangeType
    description: str
    component: Optional[str] = None
    breaking: bool = False
    issues: list[str] = field(default_factory=list)
    contributors: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Version:
    """Semantic version."""
    major: int
    minor: int
    patch: int
    prerelease: Optional[str] = None
    build: Optional[str] = None
    
    def __str__(self) -> str:
        v = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            v += f"-{self.prerelease}"
        if self.build:
            v += f"+{self.build}"
        return v
    
    @classmethod
    def parse(cls, version_str: str) -> "Version":
        """Parse version from string."""
        import re
        
        match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?(?:\+([a-zA-Z0-9.]+))?", version_str)
        if not match:
            raise ValueError(f"Invalid version string: {version_str}")
        
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
            prerelease=match.group(4),
            build=match.group(5)
        )
    
    def bump_major(self) -> "Version":
        return Version(self.major + 1, 0, 0)
    
    def bump_minor(self) -> "Version":
        return Version(self.major, self.minor + 1, 0)
    
    def bump_patch(self) -> "Version":
        return Version(self.major, self.minor, self.patch + 1)
    
    def is_compatible_with(self, other: "Version") -> bool:
        """Check if versions are semver compatible."""
        return self.major == other.major and self.minor >= other.minor


@dataclass
class Release:
    """A release entry."""
    version: Version
    status: ReleaseStatus
    changes: list[Change]
    release_date: datetime
    notes: Optional[str] = None
    artifacts: dict[str, str] = field(default_factory=dict)  # name -> url


@dataclass
class ChangelogResult:
    """Result of changelog generation."""
    content: str
    format: str
    versions: list[str]
    total_changes: int


class AutomationChangelogAction:
    """Main changelog action handler."""
    
    def __init__(self, project_name: str = "project"):
        self.project_name = project_name
        self._releases: dict[str, Release] = {}
        self._unreleased: list[Change] = []
        self._contributors: dict[str, set[str]] = defaultdict(set)
        self._categories = {
            ChangeType.BREAKING: ("Breaking Changes", "🔴"),
            ChangeType.FEATURE: ("Features", "✨"),
            ChangeType.BUGFIX: ("Bug Fixes", "🐛"),
            ChangeType.PERFORMANCE: ("Performance Improvements", "⚡"),
            ChangeType.REFACTOR: ("Refactoring", "♻️"),
            ChangeType.DEPRECATION: ("Deprecations", "⚠️"),
            ChangeType.SECURITY: ("Security", "🔒"),
            ChangeType.DOCS: ("Documentation", "📝"),
            ChangeType.OTHER: ("Other Changes", "📦"),
        }
    
    def add_change(
        self,
        change_type: ChangeType,
        description: str,
        component: Optional[str] = None,
        breaking: bool = False,
        issues: Optional[list[str]] = None,
        contributors: Optional[list[str]] = None
    ) -> "AutomationChangelogAction":
        """Add a change to the changelog."""
        change = Change(
            change_type=change_type,
            description=description,
            component=component,
            breaking=breaking,
            issues=issues or [],
            contributors=contributors or []
        )
        
        self._unreleased.append(change)
        
        for contributor in (contributors or []):
            self._contributors[contributor].add(change_type.value)
        
        return self
    
    def add_release(
        self,
        version: Version,
        changes: Optional[list[Change]] = None,
        status: ReleaseStatus = ReleaseStatus.STABLE,
        notes: Optional[str] = None,
        artifacts: Optional[dict[str, str]] = None
    ) -> "AutomationChangelogAction":
        """Add a release."""
        # Combine unreleased changes with provided changes
        all_changes = list(self._unreleased)
        if changes:
            all_changes.extend(changes)
        
        release = Release(
            version=version,
            status=status,
            changes=all_changes,
            release_date=datetime.now(),
            notes=notes,
            artifacts=artifacts or {}
        )
        
        self._releases[str(version)] = release
        self._unreleased.clear()
        
        return self
    
    async def generate_changelog(
        self,
        format: Literal["markdown", "json", "html"] = "markdown",
        include_unreleased: bool = True,
        version_range: Optional[tuple[Version, Version]] = None
    ) -> ChangelogResult:
        """
        Generate changelog content.
        
        Args:
            format: Output format (markdown, json, html)
            include_unreleased: Include unreleased changes
            version_range: Optional (from, to) version range
            
        Returns:
            ChangelogResult with generated content
        """
        if format == "json":
            return await self._generate_json_changelog(include_unreleased, version_range)
        elif format == "html":
            return await self._generate_html_changelog(include_unreleased, version_range)
        else:
            return await self._generate_markdown_changelog(include_unreleased, version_range)
    
    async def _generate_markdown_changelog(
        self,
        include_unreleased: bool,
        version_range: Optional[tuple[Version, Version]]
    ) -> ChangelogResult:
        """Generate markdown changelog."""
        lines = [f"# {self.project_name} Changelog\n"]
        
        all_versions = self._get_sorted_versions()
        total_changes = 0
        
        # Unreleased section
        if include_unreleased and self._unreleased:
            lines.append(f"## [Unreleased]\n")
            lines.extend(self._format_changes_markdown(self._unreleased))
            total_changes += len(self._unreleased)
            lines.append("")
        
        # Released versions
        for version_str in all_versions:
            release = self._releases[version_str]
            
            if version_range:
                v_from, v_to = version_range
                if not (v_from.is_compatible_with(release.version) or
                        v_to.is_compatible_with(release.version)):
                    continue
            
            lines.append(f"## [{version_str}]")
            if release.release_date:
                lines.append(f" - *{release.release_date.strftime('%Y-%m-%d')}*")
            lines.append("")
            
            if release.notes:
                lines.append(f"{release.notes}\n")
            
            lines.extend(self._format_changes_markdown(release.changes))
            total_changes += len(release.changes)
            lines.append("")
        
        return ChangelogResult(
            content="\n".join(lines),
            format="markdown",
            versions=all_versions,
            total_changes=total_changes
        )
    
    def _format_changes_markdown(self, changes: list[Change]) -> list[str]:
        """Format changes as markdown list."""
        lines = []
        
        # Group by category
        by_category: dict[ChangeType, list[Change]] = defaultdict(list)
        for change in changes:
            by_category[change.change_type].append(change)
        
        for change_type, (category_name, emoji) in self._categories.items():
            category_changes = by_category.get(change_type, [])
            if not category_changes:
                continue
            
            lines.append(f"### {emoji} {category_name}\n")
            
            for change in category_changes:
                line = f"- {change.description}"
                if change.component:
                    line += f" ({change.component})"
                if change.issues:
                    line += f" - closes {', '.join(f'#{i}' for i in change.issues)}"
                if change.contributors:
                    line += f" - thanks @{(', '.join(change.contributors))}"
                lines.append(line)
            
            lines.append("")
        
        return lines
    
    async def _generate_json_changelog(
        self,
        include_unreleased: bool,
        version_range: Optional[tuple[Version, Version]]
    ) -> ChangelogResult:
        """Generate JSON changelog."""
        data = {
            "project": self.project_name,
            "generated_at": datetime.now().isoformat(),
            "versions": []
        }
        
        if include_unreleased and self._unreleased:
            data["unreleased"] = [self._change_to_dict(c) for c in self._unreleased]
        
        for version_str in self._get_sorted_versions():
            release = self._releases[version_str]
            data["versions"].append({
                "version": version_str,
                "status": release.status.value,
                "release_date": release.release_date.isoformat(),
                "notes": release.notes,
                "changes": [self._change_to_dict(c) for c in release.changes]
            })
        
        return ChangelogResult(
            content=json.dumps(data, indent=2),
            format="json",
            versions=self._get_sorted_versions(),
            total_changes=sum(len(r.changes) for r in self._releases.values())
        )
    
    async def _generate_html_changelog(
        self,
        include_unreleased: bool,
        version_range: Optional[tuple[Version, Version]]
    ) -> ChangelogResult:
        """Generate HTML changelog."""
        lines = [
            f"<!DOCTYPE html>",
            f"<html><head><title>{self.project_name} Changelog</title>",
            f"<style>",
            f"body {{ font-family: -apple-system, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}",
            f"h1 {{ border-bottom: 2px solid #333; padding-bottom: 10px; }}",
            f"h2 {{ color: #666; margin-top: 30px; }}",
            f".change {{ margin: 10px 0; }}",
            f".feature {{ color: #22863a; }}",
            f".bugfix {{ color: #cb2431; }}",
            f".breaking {{ color: #d73a49; font-weight: bold; }}",
            f"</style></head><body>",
            f"<h1>{self.project_name} Changelog</h1>"
        ]
        
        for version_str in self._get_sorted_versions():
            release = self._releases[version_str]
            lines.append(f"<h2>Version {version_str}</h2>")
            lines.append(f"<p><em>{release.release_date.strftime('%Y-%m-%d')}</em></p>")
            
            for change in release.changes:
                css_class = change.change_type.value
                lines.append(f'<div class="change {css_class}">{change.description}</div>')
        
        lines.append("</body></html>")
        
        return ChangelogResult(
            content="\n".join(lines),
            format="html",
            versions=self._get_sorted_versions(),
            total_changes=sum(len(r.changes) for r in self._releases.values())
        )
    
    def _change_to_dict(self, change: Change) -> dict:
        """Convert change to dictionary."""
        return {
            "type": change.change_type.value,
            "description": change.description,
            "component": change.component,
            "breaking": change.breaking,
            "issues": change.issues,
            "contributors": change.contributors,
            "timestamp": change.timestamp.isoformat()
        }
    
    def _get_sorted_versions(self) -> list[str]:
        """Get versions sorted by semantic version order."""
        versions = list(self._releases.keys())
        return sorted(
            versions,
            key=lambda v: Version.parse(v),
            reverse=True
        )
    
    async def get_release_info(self, version: str) -> Optional[Release]:
        """Get information about a specific release."""
        return self._releases.get(version)
    
    async def compare_versions(
        self,
        from_version: str,
        to_version: str
    ) -> dict[str, Any]:
        """Compare two versions and return difference summary."""
        from_rel = self._releases.get(from_version)
        to_rel = self._releases.get(to_version)
        
        if not from_rel or not to_rel:
            return {"error": "Version not found"}
        
        from_changes = set(c.description for c in from_rel.changes)
        to_changes = set(c.description for c in to_rel.changes)
        
        added = to_changes - from_changes
        removed = from_changes - to_changes
        
        return {
            "from_version": from_version,
            "to_version": to_version,
            "added_count": len(added),
            "removed_count": len(removed),
            "added_changes": list(added),
            "removed_changes": list(removed),
            "breaking_changes": [
                c.description for c in to_rel.changes if c.breaking
            ]
        }
    
    async def get_upcoming_version(
        self,
        bump_type: Literal["major", "minor", "patch"] = "patch"
    ) -> Version:
        """Get the next version based on bump type."""
        all_versions = self._get_sorted_versions()
        
        if all_versions:
            current = Version.parse(all_versions[0])
        else:
            current = Version(0, 0, 0)
        
        if bump_type == "major":
            return current.bump_major()
        elif bump_type == "minor":
            return current.bump_minor()
        else:
            return current.bump_patch()
    
    def get_change_stats(self) -> dict[str, Any]:
        """Get statistics about changes."""
        all_changes = list(self._unreleased)
        for release in self._releases.values():
            all_changes.extend(release.changes)
        
        by_type: dict[str, int] = defaultdict(int)
        for change in all_changes:
            by_type[change.change_type.value] += 1
        
        return {
            "total_changes": len(all_changes),
            "unreleased": len(self._unreleased),
            "total_releases": len(self._releases),
            "by_type": dict(by_type),
            "contributors": list(self._contributors.keys()),
            "contributor_count": len(self._contributors)
        }
