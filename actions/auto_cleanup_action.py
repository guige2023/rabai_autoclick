"""Auto-cleanup automation action module.

Provides scheduled and triggered cleanup of temporary files,
caches, old logs, and other disposable data.
"""

from __future__ import annotations

import os
import time
import logging
import hashlib
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CleanupRule:
    """A cleanup rule defining what and when to clean."""
    name: str
    path_pattern: str
    age_days: float = 1.0
    size_threshold_mb: Optional[float] = None
    pattern: Optional[str] = None
    exclude_patterns: List[str] = field(default_factory=list)
    dry_run: bool = False
    recursive: bool = True
    description: str = ""


@dataclass
class CleanupResult:
    """Result of a cleanup operation."""
    rule_name: str
    files_removed: int = 0
    bytes_freed: int = 0
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    dry_run: bool = False


class AutoCleanupAction:
    """Automated cleanup engine.

    Manages cleanup of temporary files, caches, logs, and other
    disposable data based on age, size, or patterns.

    Example:
        cleanup = AutoCleanupAction()
        cleanup.add_rule("temp_files", "/tmp/*.tmp", age_days=1)
        cleanup.add_rule("old_logs", "/var/log/*.log", age_days=7)
        cleanup.add_rule("large_cache", "/tmp/cache", size_threshold_mb=100)
        cleanup.run_all()
    """

    def __init__(self) -> None:
        """Initialize auto-cleanup action."""
        self._rules: Dict[str, CleanupRule] = {}
        self._last_run: Dict[str, float] = {}

    def add_rule(
        self,
        name: str,
        path_pattern: str,
        age_days: float = 1.0,
        size_threshold_mb: Optional[float] = None,
        pattern: Optional[str] = None,
        exclude_patterns: Optional[List[str]] = None,
        recursive: bool = True,
        description: str = "",
    ) -> "AutoCleanupAction":
        """Add a cleanup rule.

        Args:
            name: Unique rule name.
            path_pattern: Path or glob pattern for files to clean.
            age_days: Files older than this many days will be cleaned.
            size_threshold_mb: If set, clean when directory exceeds this size.
            pattern: Additional filename pattern to match.
            exclude_patterns: Glob patterns to exclude from cleanup.
            recursive: Whether to search subdirectories.
            description: Rule description.

        Returns:
            Self for chaining.
        """
        import fnmatch
        self._rules[name] = CleanupRule(
            name=name,
            path_pattern=path_pattern,
            age_days=age_days,
            size_threshold_mb=size_threshold_mb,
            pattern=pattern,
            exclude_patterns=exclude_patterns or [],
            recursive=recursive,
            description=description,
        )
        logger.debug("Added cleanup rule: %s (%s)", name, path_pattern)
        return self

    def remove_rule(self, name: str) -> bool:
        """Remove a cleanup rule.

        Args:
            name: Rule name.

        Returns:
            True if removed, False if not found.
        """
        if name in self._rules:
            del self._rules[name]
            return True
        return False

    def run_rule(self, name: str, dry_run: Optional[bool] = None) -> CleanupResult:
        """Run a specific cleanup rule.

        Args:
            name: Rule name.
            dry_run: Override rule's dry_run setting.

        Returns:
            CleanupResult with operation details.
        """
        rule = self._rules.get(name)
        if not rule:
            raise ValueError(f"Rule not found: {name}")

        is_dry_run = dry_run if dry_run is not None else rule.dry_run
        start_time = time.time()
        result = CleanupResult(rule_name=name, dry_run=is_dry_run)

        try:
            dir_path = str(Path(rule.path_pattern).parent)
            file_pattern = Path(rule.path_pattern).name

            if not os.path.exists(dir_path):
                result.errors.append(f"Path does not exist: {dir_path}")
                return result

            if rule.size_threshold_mb:
                freed = self._cleanup_by_size(
                    dir_path,
                    rule.size_threshold_mb * 1024 * 1024,
                    rule,
                    is_dry_run,
                    result,
                )
                result.bytes_freed += freed
            else:
                self._cleanup_by_age(dir_path, file_pattern, rule, is_dry_run, result)

        except Exception as e:
            result.errors.append(str(e))
            logger.error("Cleanup rule '%s' failed: %s", name, e)

        result.duration_ms = (time.time() - start_time) * 1000
        self._last_run[name] = time.time()
        return result

    def run_all(self, dry_run: bool = False) -> Dict[str, CleanupResult]:
        """Run all cleanup rules.

        Args:
            dry_run: If True, don't actually delete files.

        Returns:
            Dict of rule_name -> CleanupResult.
        """
        results = {}
        for name in self._rules:
            results[name] = self.run_rule(name, dry_run=dry_run)
        return results

    def _cleanup_by_age(
        self,
        dir_path: str,
        file_pattern: str,
        rule: CleanupRule,
        dry_run: bool,
        result: CleanupResult,
    ) -> None:
        """Clean files by age."""
        import fnmatch
        cutoff = time.time() - (rule.age_days * 86400)

        for root, dirs, files in os.walk(dir_path):
            if not rule.recursive and root != dir_path:
                continue

            for filename in files:
                if not fnmatch.fnmatch(filename, rule.pattern or file_pattern):
                    continue

                if any(fnmatch.fnmatch(filename, p) for p in rule.exclude_patterns):
                    continue

                filepath = os.path.join(root, filename)
                try:
                    mtime = os.path.getmtime(filepath)
                    if mtime < cutoff:
                        size = os.path.getsize(filepath)
                        if not dry_run:
                            os.remove(filepath)
                        result.files_removed += 1
                        result.bytes_freed += size
                        logger.debug("Would remove: %s (age: %.1f days)", filepath, (time.time() - mtime) / 86400)
                except OSError as e:
                    result.errors.append(f"Error removing {filepath}: {e}")

            if not rule.recursive:
                break

    def _cleanup_by_size(
        self,
        dir_path: str,
        size_threshold: int,
        rule: CleanupRule,
        dry_run: bool,
        result: CleanupResult,
    ) -> int:
        """Clean oldest files until directory is under size threshold."""
        total_size = self._get_dir_size(dir_path)
        if total_size <= size_threshold:
            return 0

        files = []
        for root, _, filenames in os.walk(dir_path):
            for filename in filenames:
                if any(fnmatch.fnmatch(filename, p) for p in rule.exclude_patterns):
                    continue
                filepath = os.path.join(root, filename)
                try:
                    mtime = os.path.getmtime(filepath)
                    size = os.path.getsize(filepath)
                    files.append((mtime, filepath, size))
                except OSError:
                    pass

        files.sort()
        freed = 0
        for mtime, filepath, size in files:
            if total_size - freed <= size_threshold:
                break
            if not dry_run:
                os.remove(filepath)
            result.files_removed += 1
            freed += size
            logger.debug("Removed %s to free %d bytes", filepath, size)

        return freed

    def _get_dir_size(self, dir_path: str) -> int:
        """Get total size of a directory."""
        total = 0
        for root, _, files in os.walk(dir_path):
            for filename in files:
                try:
                    total += os.path.getsize(os.path.join(root, filename))
                except OSError:
                    pass
        return total

    def get_last_run(self, name: str) -> Optional[float]:
        """Get timestamp of last run for a rule."""
        return self._last_run.get(name)

    def list_rules(self) -> List[Dict[str, Any]]:
        """List all cleanup rules."""
        return [
            {
                "name": r.name,
                "path_pattern": r.path_pattern,
                "age_days": r.age_days,
                "size_threshold_mb": r.size_threshold_mb,
                "dry_run": r.dry_run,
                "recursive": r.recursive,
                "description": r.description,
            }
            for r in self._rules.values()
        ]
