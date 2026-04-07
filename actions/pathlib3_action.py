"""Pathlib extensions v3 - specialized file operations.

Specialized pathlib utilities for archives,
 compression, and file comparisons.
"""

from __future__ import annotations

import os
import shutil
import zipfile
import tarfile
from pathlib import Path
from typing import Callable, Iterator

__all__ = [
    "extract_archive",
    "create_archive",
    "list_archive",
    "is_archive",
    "safe_remove",
    "safe_move",
    "safe_copy",
    "diff_trees",
    "sync_trees",
    "watch_tree",
    "FileWatcher",
    "ArchiveHandler",
]


def extract_archive(archive_path: str | Path, dest_dir: str | Path) -> list[Path]:
    """Extract archive to destination.

    Args:
        archive_path: Path to archive file.
        dest_dir: Destination directory.

    Returns:
        List of extracted file paths.
    """
    p = Path(archive_path)
    dest = Path(dest_dir)
    if p.suffix == ".zip" or p.suffix == ".zip":
        with zipfile.ZipFile(p, "r") as zf:
            return [dest / f.name for f in zf.extractall(dest)]
    elif p.suffix == ".tar" or ".tar" in p.suffixes:
        with tarfile.open(p, "r:*") as tf:
            tf.extractall(dest)
            return [dest / m.name for m in tf.getmembers()]
    else:
        raise ValueError(f"Unsupported archive format: {p.suffix}")


def create_archive(source_dir: str | Path, archive_path: str | Path, format: str = "zip") -> Path:
    """Create archive from directory.

    Args:
        source_dir: Source directory.
        archive_path: Output archive path.
        format: Archive format (zip, tar, gztar, bztar, xztar).

    Returns:
        Path to created archive.
    """
    src = Path(source_dir)
    dst = Path(archive_path)
    shutil.make_archive(str(dst.with_suffix("")), format, str(src))
    return Path(f"{dst}.{format}")


def list_archive(archive_path: str | Path) -> list[str]:
    """List contents of archive.

    Args:
        archive_path: Path to archive.

    Returns:
        List of file names in archive.
    """
    p = Path(archive_path)
    if p.suffix == ".zip":
        with zipfile.ZipFile(p, "r") as zf:
            return zf.namelist()
    elif ".tar" in p.suffixes:
        with tarfile.open(p, "r:*") as tf:
            return [m.name for m in tf.getmembers()]
    raise ValueError(f"Unsupported archive format: {p.suffix}")


def is_archive(path: str | Path) -> bool:
    """Check if path is an archive file.

    Args:
        path: Path to check.

    Returns:
        True if archive file.
    """
    p = Path(path)
    archive_extensions = {".zip", ".tar", ".gz", ".bz2", ".xz", ".tgz", ".tbz2", ".txz"}
    return p.suffix.lower() in archive_extensions or any(s in p.suffixes for s in archive_extensions)


def safe_remove(path: str | Path) -> bool:
    """Safely remove file or directory.

    Args:
        path: Path to remove.

    Returns:
        True if removed.
    """
    p = Path(path)
    if not p.exists():
        return False
    try:
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()
        return True
    except Exception:
        return False


def safe_move(src: str | Path, dst: str | Path, overwrite: bool = False) -> bool:
    """Safely move file or directory.

    Args:
        src: Source path.
        dst: Destination path.
        overwrite: Whether to overwrite.

    Returns:
        True if moved.
    """
    s = Path(src)
    d = Path(dst)
    if not s.exists():
        return False
    if d.exists() and not overwrite:
        return False
    try:
        shutil.move(str(s), str(d))
        return True
    except Exception:
        return False


def safe_copy(src: str | Path, dst: str | Path, overwrite: bool = False) -> bool:
    """Safely copy file or directory.

    Args:
        src: Source path.
        dst: Destination path.
        overwrite: Whether to overwrite.

    Returns:
        True if copied.
    """
    s = Path(src)
    d = Path(dst)
    if not s.exists():
        return False
    if d.exists() and not overwrite:
        return False
    try:
        if s.is_dir():
            shutil.copytree(str(s), str(d), dirs_exist_ok=overwrite)
        else:
            shutil.copy2(str(s), str(d))
        return True
    except Exception:
        return False


def diff_trees(dir1: str | Path, dir2: str | Path) -> dict[str, list[str]]:
    """Compare two directory trees.

    Args:
        dir1: First directory.
        dir2: Second directory.

    Returns:
        Dict with 'only1', 'only2', 'different'.
    """
    d1 = Path(dir1)
    d2 = Path(dir2)
    files1 = {f.relative_to(d1): f for f in d1.rglob("*") if f.is_file()}
    files2 = {f.relative_to(d2): f for f in d2.rglob("*") if f.is_file()}
    all_files = set(files1.keys()) | set(files2.keys())
    only1 = [str(f) for f in files1 if f not in files2]
    only2 = [str(f) for f in files2 if f not in files1]
    different = []
    for f in files1 & files2:
        if files1[f].stat().st_size != files2[f].stat().st_size:
            different.append(str(f))
    return {"only1": only1, "only2": only2, "different": different}


def sync_trees(src: str | Path, dst: str | Path, delete: bool = False) -> dict:
    """Sync source to destination.

    Args:
        src: Source directory.
        dst: Destination directory.
        delete: Whether to delete extras in dst.

    Returns:
        Dict with sync statistics.
    """
    s = Path(src)
    d = Path(dst)
    copied = []
    deleted = []
    for f in s.rglob("*"):
        if f.is_file():
            rel = f.relative_to(s)
            dst_file = d / rel
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            if not dst_file.exists() or f.stat().st_mtime > dst_file.stat().st_mtime:
                shutil.copy2(f, dst_file)
                copied.append(str(rel))
    if delete:
        for f in d.rglob("*"):
            if f.is_file():
                rel = f.relative_to(d)
                if not (s / rel).exists():
                    f.unlink()
                    deleted.append(str(rel))
    return {"copied": copied, "deleted": deleted}


def watch_tree(path: str | Path, callback: Callable[[Path, str], None]) -> None:
    """Watch directory tree for changes.

    Args:
        path: Directory to watch.
        callback: Function(path, event_type).
    """
    raise NotImplementedError("Use watchdog library for production")


class FileWatcher:
    """Watch files for changes."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._files: dict[Path, float] = {}

    def scan(self) -> dict[Path, float]:
        """Scan and return changed files."""
        changes = {}
        for f in self._path.rglob("*"):
            if f.is_file():
                mtime = f.stat().st_mtime
                if f not in self._files or self._files[f] != mtime:
                    changes[f] = mtime
                    self._files[f] = mtime
        return changes


class ArchiveHandler:
    """Handle archive operations."""

    def __init__(self, archive_path: str | Path) -> None:
        self._path = Path(archive_path)

    def extract_all(self, dest: str | Path) -> list[Path]:
        """Extract all contents."""
        return extract_archive(self._path, dest)

    def extract_member(self, member: str, dest: str | Path) -> Path | None:
        """Extract single member."""
        p = Path(dest)
        if self._path.suffix == ".zip":
            with zipfile.ZipFile(self._path, "r") as zf:
                zf.extract(member, p)
                return p / member
        return None

    def list_contents(self) -> list[str]:
        """List archive contents."""
        return list_archive(self._path)
