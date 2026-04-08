"""Automation Cleanup Action.

Cleans up temporary files, stale data, and expired resources.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import time
import os


@dataclass
class CleanupResult:
    scanned_count: int
    cleaned_count: int
    freed_bytes: int
    errors: List[str] = field(default_factory=list)


@dataclass
class CleanupRule:
    name: str
    filter_fn: Callable[[str, float], bool]
    action: Callable[[str], None] = lambda _: None
    description: str = ""


class AutomationCleanupAction:
    """Cleans up stale files, cache, and temporary resources."""

    def __init__(
        self,
        dry_run: bool = False,
        rules: Optional[List[CleanupRule]] = None,
    ) -> None:
        self.dry_run = dry_run
        self.rules = rules or []
        self.stats = {"scanned": 0, "cleaned": 0, "freed_bytes": 0}

    def add_rule(
        self,
        name: str,
        filter_fn: Callable[[str, float], bool],
        action: Optional[Callable[[str], None]] = None,
        description: str = "",
    ) -> None:
        rule = CleanupRule(
            name=name,
            filter_fn=filter_fn,
            action=action or (lambda _: None),
            description=description,
        )
        self.rules.append(rule)

    def add_age_rule(
        self,
        name: str,
        max_age_sec: float,
        path_pattern: Optional[str] = None,
        description: str = "",
    ) -> None:
        def age_filter(path: str, modified_time: float) -> bool:
            if path_pattern and path_pattern not in path:
                return False
            return time.time() - modified_time > max_age_sec
        self.add_rule(name, age_filter, description=description)

    def scan(
        self,
        paths: List[str],
        recursive: bool = True,
    ) -> List[str]:
        found = []
        for base_path in paths:
            if not os.path.exists(base_path):
                continue
            if os.path.isfile(base_path):
                found.append(base_path)
            elif os.path.isdir(base_path):
                for root, dirs, files in os.walk(base_path):
                    for fname in files:
                        found.append(os.path.join(root, fname))
                    if not recursive:
                        break
        self.stats["scanned"] += len(found)
        return found

    def cleanup(
        self,
        paths: List[str],
        recursive: bool = True,
    ) -> CleanupResult:
        files = self.scan(paths, recursive)
        cleaned = 0
        freed = 0
        errors = []
        for fpath in files:
            try:
                modified = os.path.getmtime(fpath)
                for rule in self.rules:
                    if rule.filter_fn(fpath, modified):
                        if not self.dry_run:
                            size = os.path.getsize(fpath) if os.path.exists(fpath) else 0
                            rule.action(fpath)
                            freed += size
                            cleaned += 1
                        break
            except Exception as e:
                errors.append(f"{fpath}: {str(e)}")
        self.stats["cleaned"] += cleaned
        self.stats["freed_bytes"] += freed
        return CleanupResult(
            scanned_count=len(files),
            cleaned_count=cleaned,
            freed_bytes=freed,
            errors=errors,
        )

    def get_stats(self) -> Dict[str, Any]:
        return dict(self.stats)
