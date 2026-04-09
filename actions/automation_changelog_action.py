"""
Automation Changelog Generator Module.

Generates changelogs from git history, commit messages,
and project metadata for release notes and documentation.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, Pattern, Match
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Type of change."""
    FEATURE = auto()
    BUGFIX = auto()
    ENHANCEMENT = auto()
    DOCUMENTATION = auto()
    REFACTORING = auto()
    PERFORMANCE = auto()
    SECURITY = auto()
    DEPRECATION = auto()
    BREAKING_CHANGE = auto()
    OTHER = auto()


class CommitScope(Enum):
    """Scope of the change."""
    CORE = auto()
    API = auto()
    UI = auto()
    DATABASE = auto()
    AUTH = auto()
    CONFIG = auto()
    DEPLOYMENT = auto()
    TESTING = auto()
    DOCUMENTATION = auto()
    INFRASTRUCTURE = auto()


@dataclass
class CommitInfo:
    """Parsed commit information."""
    hash: str
    short_hash: str
    message: str
    full_message: str
    author: str
    author_email: str
    date: datetime
    change_type: ChangeType
    scope: Optional[CommitScope] = None
    breaking: bool = False
    ticket_id: Optional[str] = None


@dataclass
class ChangeEntry:
    """Single change entry for changelog."""
    change_type: ChangeType
    scope: Optional[str]
    description: str
    commit_hash: str
    author: Optional[str] = None
    breaking: bool = False
    ticket_id: Optional[str] = None


@dataclass
class ReleaseSection:
    """Section for a release version."""
    version: str
    date: datetime
    changes: List[ChangeEntry] = field(default_factory=list)
    breaking_changes: List[ChangeEntry] = field(default_factory=list)
    deprecated: List[ChangeEntry] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChangelogReport:
    """Complete changelog report."""
    title: str
    releases: List[ReleaseSection]
    unreleased: Optional[ReleaseSection] = None
    generated_at: datetime = field(default_factory=datetime.now)


class CommitParser:
    """Parses commit messages into structured data."""
    
    CONVENTIONAL_PATTERN = re.compile(
        r"^(?P<type>feat|fix|docs|style|refactor|perf|test|chore|build|ci|breaking)"
        r"(?:\((?P<scope>[^)]+)\))?"
        r"(?P<breaking>!)?"
        r":\s"
        r"(?P<message>.+)"
    )
    
    TICKET_PATTERN = re.compile(r"#(\d+)")
    BUGFIX_KEYWORDS = ["fix", "bug", "hotfix", "patch"]
    FEATURE_KEYWORDS = ["feat", "feature", "add", "new"]
    
    @classmethod
    def parse_commit(
        cls,
        commit_hash: str,
        message: str,
        author: str,
        author_email: str,
        date: datetime
    ) -> CommitInfo:
        """Parse a commit into structured format."""
        short_hash = commit_hash[:8]
        
        change_type, scope, breaking, ticket = cls._parse_message(message)
        
        return CommitInfo(
            hash=commit_hash,
            short_hash=short_hash,
            message=message.strip(),
            full_message=message.strip(),
            author=author,
            author_email=author_email,
            date=date,
            change_type=change_type,
            scope=scope,
            breaking=breaking,
            ticket_id=ticket
        )
    
    @classmethod
    def _parse_message(
        cls,
        message: str
    ) -> Tuple[ChangeType, Optional[CommitScope], bool, Optional[str]]:
        """Parse message to extract type, scope, breaking, ticket."""
        # Try conventional commit format
        match = cls.CONVENTIONAL_PATTERN.match(message.strip())
        
        if match:
            type_str = match.group("type").lower()
            scope_str = match.group("scope")
            breaking = match.group("breaking") == "!"
            msg_body = match.group("message")
        else:
            type_str = message.lower()
            scope_str = None
            breaking = "breaking" in message.lower()
            msg_body = message
        
        # Extract ticket ID
        ticket_match = cls.TICKET_PATTERN.search(message)
        ticket_id = ticket_match.group(1) if ticket_match else None
        
        # Map type string to ChangeType
        change_type = cls._map_type(type_str)
        
        # Map scope
        scope = cls._map_scope(scope_str) if scope_str else None
        
        return change_type, scope, breaking, ticket_id
    
    @classmethod
    def _map_type(cls, type_str: str) -> ChangeType:
        """Map type string to ChangeType enum."""
        type_mapping = {
            "feat": ChangeType.FEATURE,
            "feature": ChangeType.FEATURE,
            "fix": ChangeType.BUGFIX,
            "bug": ChangeType.BUGFIX,
            "hotfix": ChangeType.BUGFIX,
            "enhancement": ChangeType.ENHANCEMENT,
            "perf": ChangeType.PERFORMANCE,
            "performance": ChangeType.PERFORMANCE,
            "docs": ChangeType.DOCUMENTATION,
            "documentation": ChangeType.DOCUMENTATION,
            "refactor": ChangeType.REFACTORING,
            "security": ChangeType.SECURITY,
            "breaking": ChangeType.BREAKING_CHANGE,
            "deprecation": ChangeType.DEPRECATION,
            "breaking_change": ChangeType.BREAKING_CHANGE,
        }
        
        return type_mapping.get(type_str, ChangeType.OTHER)
    
    @classmethod
    def _map_scope(cls, scope_str: str) -> Optional[CommitScope]:
        """Map scope string to CommitScope enum."""
        scope_mapping = {
            "core": CommitScope.CORE,
            "api": CommitScope.API,
            "ui": CommitScope.UI,
            "frontend": CommitScope.UI,
            "backend": CommitScope.API,
            "db": CommitScope.DATABASE,
            "database": CommitScope.DATABASE,
            "auth": CommitScope.AUTH,
            "config": CommitScope.CONFIG,
            "deploy": CommitScope.DEPLOYMENT,
            "deployment": CommitScope.DEPLOYMENT,
            "test": CommitScope.TESTING,
            "testing": CommitScope.TESTING,
            "docs": CommitScope.DOCUMENTATION,
            "infra": CommitScope.INFRASTRUCTURE,
            "infrastructure": CommitScope.INFRASTRUCTURE,
        }
        
        return scope_mapping.get(scope_str.lower(), CommitScope.CORE)


class ChangelogGenerator:
    """
    Generates changelogs from commit history.
    
    Supports semantic versioning, conventional commits,
    and multiple output formats.
    """
    
    def __init__(
        self,
        project_name: str,
        version_prefix: str = "v"
    ) -> None:
        self.project_name = project_name
        self.version_prefix = version_prefix
        self.parser = CommitParser()
    
    def generate(
        self,
        commits: List[CommitInfo],
        since_version: Optional[str] = None,
        until_version: Optional[str] = None
    ) -> ChangelogReport:
        """
        Generate changelog from commits.
        
        Args:
            commits: List of parsed commits
            since_version: Only include commits since this version
            until_version: Include commits up to this version
            
        Returns:
            ChangelogReport
        """
        # Group commits by version/release
        releases = self._group_by_version(commits)
        
        # Sort releases by date (newest first)
        releases.sort(key=lambda r: r.date, reverse=True)
        
        # Find unreleased changes
        unreleased = None
        for release in releases:
            if release.metadata.get("is_released"):
                continue
            unreleased = release
            break
        
        return ChangelogReport(
            title=self.project_name,
            releases=releases,
            unreleased=unreleased
        )
    
    def _group_by_version(
        self,
        commits: List[CommitInfo]
    ) -> List[ReleaseSection]:
        """Group commits into version releases."""
        releases: Dict[str, ReleaseSection] = {}
        
        current_release = None
        
        for commit in commits:
            # Check for version tags
            version = self._extract_version(commit.message)
            
            if version:
                if current_release:
                    releases[str(version)] = current_release
                
                current_release = ReleaseSection(
                    version=version,
                    date=commit.date
                )
                current_release.metadata["is_released"] = True
            elif current_release:
                self._add_to_release(current_release, commit)
            else:
                if "unreleased" not in releases:
                    releases["unreleased"] = ReleaseSection(
                        version="Unreleased",
                        date=datetime.now()
                    )
                self._add_to_release(releases["unreleased"], commit)
        
        return list(releases.values())
    
    def _extract_version(self, message: str) -> Optional[str]:
        """Extract version from commit/tag message."""
        patterns = [
            rf"{re.escape(self.version_prefix)}(\d+\.\d+\.\d+)",
            r"release\s+(\d+\.\d+\.\d+)",
            r"version\s+(\d+\.\d+\.\d+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _add_to_release(
        self,
        release: ReleaseSection,
        commit: CommitInfo
    ) -> None:
        """Add commit to release section."""
        entry = ChangeEntry(
            change_type=commit.change_type,
            scope=commit.scope.name if commit.scope else None,
            description=commit.message,
            commit_hash=commit.short_hash,
            author=commit.author,
            breaking=commit.breaking,
            ticket_id=commit.ticket_id
        )
        
        if commit.breaking:
            release.breaking_changes.append(entry)
        elif commit.change_type == ChangeType.DEPRECATION:
            release.deprecated.append(entry)
        else:
            release.changes.append(entry)
    
    def format_markdown(
        self,
        report: ChangelogReport,
        include_author: bool = False
    ) -> str:
        """Format changelog as Markdown."""
        lines = [
            f"# {report.title}",
            "",
        ]
        
        # Unreleased changes
        if report.unreleased:
            lines.extend(self._format_section(
                report.unreleased, "Unreleased", include_author
            ))
        
        # Released versions
        for release in report.releases:
            if not release.metadata.get("is_released"):
                continue
            lines.extend(self._format_section(
                release, f"v{release.version}", include_author
            ))
        
        return "\n".join(lines)
    
    def _format_section(
        self,
        release: ReleaseSection,
        version: str,
        include_author: bool
    ) -> List[str]:
        """Format a release section."""
        lines = [
            f"## {version}",
            f"*{release.date.strftime('%Y-%m-%d')}*",
            ""
        ]
        
        if release.breaking_changes:
            lines.append("### Breaking Changes")
            for change in release.breaking_changes:
                lines.extend(self._format_change(change, include_author))
            lines.append("")
        
        if release.deprecated:
            lines.append("### Deprecated")
            for change in release.deprecated:
                lines.extend(self._format_change(change, include_author))
            lines.append("")
        
        # Group by change type
        by_type: Dict[ChangeType, List[ChangeEntry]] = defaultdict(list)
        for change in release.changes:
            by_type[change.change_type].append(change)
        
        for change_type in [ChangeType.FEATURE, ChangeType.ENHANCEMENT, ChangeType.BUGFIX]:
            if change_type in by_type:
                type_name = change_type.name.lower()
                if change_type == ChangeType.FEATURE:
                    type_name = "Features"
                elif change_type == ChangeType.ENHANCEMENT:
                    type_name = "Enhancements"
                elif change_type == ChangeType.BUGFIX:
                    type_name = "Bug Fixes"
                
                lines.append(f"### {type_name}")
                for change in by_type[change_type]:
                    lines.extend(self._format_change(change, include_author))
                lines.append("")
        
        return lines
    
    def _format_change(
        self,
        change: ChangeEntry,
        include_author: bool
    ) -> List[str]:
        """Format a single change entry."""
        scope = f"**{change.scope}**: " if change.scope else ""
        ticket = f" ([#{change.ticket_id}])" if change.ticket_id else ""
        author = f" - {change.author}" if include_author and change.author else ""
        
        return [f"- {scope}{change.description}{ticket}{author}"]


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample commits
    commits = [
        CommitInfo(
            hash="abc123def456",
            short_hash="abc123d",
            message="feat(api): add user authentication",
            full_message="feat(api): add user authentication\n\nImplement JWT-based auth",
            author="Alice",
            author_email="alice@example.com",
            date=datetime(2024, 1, 15),
            change_type=ChangeType.FEATURE,
            scope=CommitScope.API,
            breaking=False,
            ticket_id="123"
        ),
        CommitInfo(
            hash="def456abc789",
            short_hash="def456a",
            message="fix(ui): correct button alignment",
            full_message="fix(ui): correct button alignment",
            author="Bob",
            author_email="bob@example.com",
            date=datetime(2024, 1, 14),
            change_type=ChangeType.BUGFIX,
            scope=CommitScope.UI,
            breaking=False,
            ticket_id="124"
        ),
        CommitInfo(
            hash="789xyzabc012",
            short_hash="789xyzab",
            message="perf(database): optimize query performance",
            full_message="perf(database): optimize query performance",
            author="Carol",
            author_email="carol@example.com",
            date=datetime(2024, 1, 13),
            change_type=ChangeType.PERFORMANCE,
            scope=CommitScope.DATABASE,
            breaking=False
        ),
    ]
    
    generator = ChangelogGenerator(project_name="MyProject")
    
    print("=== Changelog Generation Demo ===\n")
    
    report = generator.generate(commits)
    
    changelog_md = generator.format_markdown(report, include_author=True)
    
    print(changelog_md)
