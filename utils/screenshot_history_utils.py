"""Screenshot History Manager.

Manages a rolling history of screenshots for UI automation debugging and comparison.
Captures, stores, and retrieves historical screenshots with metadata.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class ScreenshotRecord:
    """Record of a single screenshot capture."""

    timestamp: datetime
    file_path: str
    checksum: str
    width: int
    height: int
    tags: list[str] = field(default_factory=list)
    description: Optional[str] = None
    element_bounds: Optional[tuple[int, int, int, int]] = None

    def to_dict(self) -> dict:
        """Convert record to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "file_path": self.file_path,
            "checksum": self.checksum,
            "width": self.width,
            "height": self.height,
            "tags": self.tags,
            "description": self.description,
            "element_bounds": self.element_bounds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ScreenshotRecord":
        """Create record from dictionary."""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            file_path=data["file_path"],
            checksum=data["checksum"],
            width=data["width"],
            height=data["height"],
            tags=data.get("tags", []),
            description=data.get("description"),
            element_bounds=data.get("element_bounds"),
        )


class ScreenshotHistoryManager:
    """Manages a rolling history of screenshots.

    Stores screenshots with metadata for debugging and comparison workflows.
    Automatically prunes old screenshots when the history limit is exceeded.

    Example:
        manager = ScreenshotHistoryManager("/tmp/screenshots", max_history=50)
        record = manager.capture("homepage_initial", width=1920, height=1080)
        history = manager.get_history(tag="homepage_initial")
    """

    def __init__(
        self,
        storage_dir: str | Path,
        max_history: int = 100,
        max_storage_mb: int = 500,
    ):
        """Initialize the screenshot history manager.

        Args:
            storage_dir: Directory to store screenshots and metadata.
            max_history: Maximum number of screenshots to retain.
            max_storage_mb: Maximum storage space in megabytes.
        """
        self.storage_dir = Path(storage_dir)
        self.screenshots_dir = self.storage_dir / "images"
        self.meta_dir = self.storage_dir / "meta"
        self.max_history = max_history
        self.max_storage_bytes = max_storage_mb * 1024 * 1024
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        """Create storage directories if they don't exist."""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(exist_ok=True)
        self.meta_dir.mkdir(exist_ok=True)

    def _compute_checksum(self, file_path: Path) -> str:
        """Compute SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _get_meta_file(self) -> Path:
        """Get the metadata index file path."""
        return self.meta_dir / "history.json"

    def _load_history(self) -> list[ScreenshotRecord]:
        """Load screenshot history from disk."""
        meta_file = self._get_meta_file()
        if not meta_file.exists():
            return []
        try:
            with open(meta_file) as f:
                data = json.load(f)
            return [ScreenshotRecord.from_dict(item) for item in data]
        except (json.JSONDecodeError, KeyError):
            return []

    def _save_history(self, history: list[ScreenshotRecord]) -> None:
        """Save screenshot history to disk."""
        meta_file = self._get_meta_file()
        with open(meta_file, "w") as f:
            json.dump([r.to_dict() for r in history], f, indent=2)

    def capture(
        self,
        tag: str,
        source_path: str | Path,
        width: int,
        height: int,
        description: Optional[str] = None,
        element_bounds: Optional[tuple[int, int, int, int]] = None,
    ) -> ScreenshotRecord:
        """Capture and store a screenshot.

        Args:
            tag: Tag to categorize the screenshot (e.g., "login_page").
            source_path: Path to the screenshot file.
            width: Screenshot width in pixels.
            height: Screenshot height in pixels.
            description: Optional description of the screenshot.
            element_bounds: Optional (x, y, w, h) of a specific element captured.

        Returns:
            ScreenshotRecord with capture metadata.
        """
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Screenshot file not found: {source}")

        history = self._load_history()
        timestamp = datetime.now()
        checksum = self._compute_checksum(source)

        # Generate unique filename
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S_%f")
        safe_tag = "".join(c if c.isalnum() else "_" for c in tag)
        dest_name = f"{timestamp_str}_{safe_tag}.png"
        dest_path = self.screenshots_dir / dest_name

        # Copy file to storage
        shutil.copy2(source, dest_path)

        record = ScreenshotRecord(
            timestamp=timestamp,
            file_path=str(dest_path),
            checksum=checksum,
            width=width,
            height=height,
            tags=[tag],
            description=description,
            element_bounds=element_bounds,
        )

        history.insert(0, record)
        self._prune_history(history)
        self._save_history(history)

        return record

    def _prune_history(self, history: list[ScreenshotRecord]) -> None:
        """Prune history to respect max_history limit and storage size."""
        # Prune by count
        while len(history) > self.max_history:
            removed = history.pop()
            path = Path(removed.file_path)
            if path.exists():
                path.unlink()

        # Prune by storage size
        total_size = sum(Path(r.file_path).stat().st_size for r in history if Path(r.file_path).exists())
        while total_size > self.max_storage_bytes and history:
            removed = history.pop()
            path = Path(removed.file_path)
            if path.exists():
                total_size -= path.stat().st_size
                path.unlink()

    def get_history(
        self,
        tag: Optional[str] = None,
        limit: int = 20,
        since: Optional[datetime] = None,
    ) -> list[ScreenshotRecord]:
        """Retrieve screenshot history with optional filtering.

        Args:
            tag: Filter by tag (exact match).
            limit: Maximum number of records to return.
            since: Only return screenshots after this timestamp.

        Returns:
            List of matching ScreenshotRecord objects.
        """
        history = self._load_history()
        results = history

        if tag:
            results = [r for r in results if tag in r.tags]

        if since:
            results = [r for r in results if r.timestamp >= since]

        return results[:limit]

    def find_by_checksum(self, checksum: str) -> Optional[ScreenshotRecord]:
        """Find a screenshot by its checksum.

        Args:
            checksum: SHA256 checksum to search for.

        Returns:
            Matching ScreenshotRecord or None.
        """
        history = self._load_history()
        for record in history:
            if record.checksum == checksum:
                return record
        return None

    def find_duplicates(self) -> list[tuple[ScreenshotRecord, ScreenshotRecord]]:
        """Find duplicate screenshots in history.

        Returns:
            List of (record1, record2) tuples for duplicates.
        """
        history = self._load_history()
        seen: dict[str, ScreenshotRecord] = {}
        duplicates: list[tuple[ScreenshotRecord, ScreenshotRecord]] = []

        for record in history:
            if record.checksum in seen:
                duplicates.append((seen[record.checksum], record))
            else:
                seen[record.checksum] = record

        return duplicates

    def clear(self, before: Optional[datetime] = None, tags: Optional[list[str]] = None) -> int:
        """Clear screenshots from history.

        Args:
            before: Clear only screenshots before this timestamp.
            tags: Clear only screenshots with these tags.

        Returns:
            Number of screenshots cleared.
        """
        history = self._load_history()
        to_remove: list[ScreenshotRecord] = []
        remaining: list[ScreenshotRecord] = []

        for record in history:
            should_remove = True
            if before and record.timestamp >= before:
                should_remove = False
            if tags and not any(t in record.tags for t in tags):
                should_remove = False
            if should_remove:
                to_remove.append(record)
            else:
                remaining.append(record)

        for record in to_remove:
            path = Path(record.file_path)
            if path.exists():
                path.unlink()

        self._save_history(remaining)
        return len(to_remove)
