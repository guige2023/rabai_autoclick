"""pathlib extra action extensions for rabai_autoclick.

Provides additional pathlib utilities beyond the basic
path operations for complex file system tasks.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

__all__ = [
    "Path",
    "ensure_dir",
    "ensure_parent",
    "safe_mkdir",
    "safe_remove",
    "safe_rmtree",
    "safe_copy",
    "safe_move",
    "safe_rename",
    "traverse",
    "walk",
    "glob_recursive",
    "find_files",
    "find_dirs",
    "find_by_pattern",
    "find_by_size",
    "find_by_date",
    "find_duplicates",
    "is_empty_dir",
    "is_hidden",
    "is_symlink",
    "is_readable",
    "is_writable",
    "is_executable",
    "make_executable",
    "make_readonly",
    "make_writable",
    "get_size",
    "get_total_size",
    "get_file_count",
    "get_dir_count",
    "get_extension",
    "change_extension",
    "strip_extension",
    "add_extension",
    "split_path",
    "join_path",
    "normalize_path",
    "expand_path",
    "resolve_symlink",
    "relative_to",
    "common_path",
    "temp_dir",
    "temp_file",
    "chdir",
    "FilePattern",
    "PathFilter",
    "PathWalker",
]


def ensure_dir(path: Path | str) -> Path:
    """Ensure directory exists.

    Args:
        path: Directory path.

    Returns:
        Path object.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_parent(path: Path | str) -> Path:
    """Ensure parent directory of path exists.

    Args:
        path: File path.

    Returns:
        Path object.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def safe_mkdir(path: Path | str, mode: int = 0o777, exist_ok: bool = True) -> Path:
    """Safely create directory.

    Args:
        path: Directory path.
        mode: Directory mode.
        exist_ok: Don't raise if exists.

    Returns:
        Path object.
    """
    return Path(path).mkdir(mode=mode, exist_ok=exist_ok)


def safe_remove(path: Path | str) -> bool:
    """Safely remove a file.

    Args:
        path: File path.

    Returns:
        True if removed, False if didn't exist.
    """
    p = Path(path)
    if p.exists() and p.is_file():
        p.unlink()
        return True
    return False


def safe_rmtree(path: Path | str, ignore_errors: bool = False) -> bool:
    """Safely remove directory tree.

    Args:
        path: Directory path.
        ignore_errors: Ignore removal errors.

    Returns:
        True if removed, False if didn't exist.
    """
    p = Path(path)
    if p.exists():
        shutil.rmtree(p, ignore_errors=ignore_errors)
        return True
    return False


def safe_copy(src: Path | str, dst: Path | str) -> Path:
    """Safely copy file.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        Destination path.
    """
    s = Path(src)
    d = Path(dst)
    if d.is_dir():
        d = d / s.name
    ensure_parent(d)
    shutil.copy2(s, d)
    return d


def safe_move(src: Path | str, dst: Path | str) -> Path:
    """Safely move file.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        Destination path.
    """
    s = Path(src)
    d = Path(dst)
    if d.is_dir():
        d = d / s.name
    ensure_parent(d)
    shutil.move(str(s), str(d))
    return d


def safe_rename(src: Path | str, dst: Path | str) -> Path:
    """Safely rename file.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        Destination path.
    """
    s = Path(src)
    d = Path(dst)
    ensure_parent(d)
    s.rename(d)
    return d


def traverse(
    root: Path | str,
    filter_func: Callable[[Path], bool] | None = None,
    include_files: bool = True,
    include_dirs: bool = True,
) -> Iterator[Path]:
    """Traverse directory tree.

    Args:
        root: Root directory.
        filter_func: Optional filter function.
        include_files: Include files.
        include_dirs: Include directories.

    Yields:
        Path objects.
    """
    root_path = Path(root)
    for item in root_path.rglob("*"):
        if filter_func and not filter_func(item):
            continue
        if item.is_file() and include_files:
            yield item
        elif item.is_dir() and include_dirs:
            yield item


def walk(
    root: Path | str,
    top_down: bool = True,
) -> Iterator[tuple[Path, list[Path], list[Path]]]:
    """Walk directory tree.

    Args:
        root: Root directory.
        top_down: Walk top-down or bottom-up.

    Yields:
        Tuples of (dirpath, dirnames, filenames).
    """
    root_path = Path(root)
    for dirpath, dirnames, filenames in os.walk(root_path, topdown=top_up):
        dp = Path(dirpath)
        yield (dp, [dp / d for d in dirnames], [dp / f for f in filenames])


def glob_recursive(
    root: Path | str,
    pattern: str,
    include_hidden: bool = False,
) -> list[Path]:
    """Glob recursively.

    Args:
        root: Root directory.
        pattern: Glob pattern.
        include_hidden: Include hidden files.

    Returns:
        List of matching paths.
    """
    root_path = Path(root)
    matches = list(root_path.rglob(pattern))
    if not include_hidden:
        matches = [p for p in matches if not any(part.startswith(".") for part in p.parts)]
    return matches


def find_files(
    root: Path | str,
    pattern: str = "*",
    recursive: bool = True,
) -> list[Path]:
    """Find files matching pattern.

    Args:
        root: Root directory.
        pattern: Glob pattern.
        recursive: Search recursively.

    Returns:
        List of matching files.
    """
    root_path = Path(root)
    if recursive:
        return [p for p in root_path.rglob(pattern) if p.is_file()]
    return [p for p in root_path.glob(pattern) if p.is_file()]


def find_dirs(
    root: Path | str,
    pattern: str = "*",
    recursive: bool = True,
) -> list[Path]:
    """Find directories matching pattern.

    Args:
        root: Root directory.
        pattern: Glob pattern.
        recursive: Search recursively.

    Returns:
        List of matching directories.
    """
    root_path = Path(root)
    if recursive:
        return [p for p in root_path.rglob(pattern) if p.is_dir()]
    return [p for p in root_path.glob(pattern) if p.is_dir()]


def find_by_pattern(
    root: Path | str,
    name_pattern: str,
    content_pattern: str | None = None,
) -> list[Path]:
    """Find files by name and optionally content pattern.

    Args:
        root: Root directory.
        name_pattern: Name glob pattern.
        content_pattern: Content regex pattern (optional).

    Returns:
        List of matching paths.
    """
    import re

    matches = find_files(root, name_pattern, recursive=True)
    if content_pattern:
        compiled = re.compile(content_pattern)
        filtered = []
        for p in matches:
            try:
                content = p.read_text()
                if compiled.search(content):
                    filtered.append(p)
            except Exception:
                pass
        return filtered
    return matches


def find_by_size(
    root: Path | str,
    min_size: int = 0,
    max_size: int = float("inf"),
) -> list[Path]:
    """Find files by size range.

    Args:
        root: Root directory.
        min_size: Minimum size in bytes.
        max_size: Maximum size in bytes.

    Returns:
        List of matching files.
    """
    matches = []
    for p in find_files(root, "*", recursive=True):
        try:
            size = p.stat().st_size
            if min_size <= size <= max_size:
                matches.append(p)
        except OSError:
            pass
    return matches


def find_by_date(
    root: Path | str,
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[Path]:
    """Find files by modification date.

    Args:
        root: Root directory.
        after: Modified after this date.
        before: Modified before this date.

    Returns:
        List of matching files.
    """
    from datetime import datetime

    matches = []
    for p in find_files(root, "*", recursive=True):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if after and mtime < after:
                continue
            if before and mtime > before:
                continue
            matches.append(p)
        except OSError:
            pass
    return matches


def find_duplicates(root: Path | str) -> dict[str, list[Path]]:
    """Find duplicate files by content hash.

    Args:
        root: Root directory.

    Returns:
        Dict of hash to list of duplicate paths.
    """
    import hashlib

    hashes: dict[str, list[Path]] = {}
    for p in find_files(root, "*", recursive=True):
        try:
            content = p.read_bytes()
            h = hashlib.sha256(content).hexdigest()
            if h not in hashes:
                hashes[h] = []
            hashes[h].append(p)
        except OSError:
            pass
    return {h: paths for h, paths in hashes.items() if len(paths) > 1}


def is_empty_dir(path: Path | str) -> bool:
    """Check if directory is empty.

    Args:
        path: Directory path.

    Returns:
        True if empty.
    """
    p = Path(path)
    if not p.is_dir():
        return False
    return not any(p.iterdir())


def is_hidden(path: Path | str) -> bool:
    """Check if path is hidden.

    Args:
        path: Path to check.

    Returns:
        True if hidden.
    """
    p = Path(path)
    return any(part.startswith(".") for part in p.parts)


def is_symlink(path: Path | str) -> bool:
    """Check if path is symlink.

    Args:
        path: Path to check.

    Returns:
        True if symlink.
    """
    return Path(path).is_symlink()


def is_readable(path: Path | str) -> bool:
    """Check if path is readable.

    Args:
        path: Path to check.

    Returns:
        True if readable.
    """
    return os.access(path, os.R_OK)


def is_writable(path: Path | str) -> bool:
    """Check if path is writable.

    Args:
        path: Path to check.

    Returns:
        True if writable.
    """
    return os.access(path, os.W_OK)


def is_executable(path: Path | str) -> bool:
    """Check if path is executable.

    Args:
        path: Path to check.

    Returns:
        True if executable.
    """
    return os.access(path, os.X_OK)


def make_executable(path: Path | str) -> Path:
    """Make file executable.

    Args:
        path: File path.

    Returns:
        Path object.
    """
    p = Path(path)
    import stat
    p.chmod(p.stat().st_mode | stat.S_IXUSR)
    return p


def make_readonly(path: Path | str) -> Path:
    """Make file read-only.

    Args:
        path: File path.

    Returns:
        Path object.
    """
    p = Path(path)
    import stat
    p.chmod(p.stat().st_mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)
    return p


def make_writable(path: Path | str) -> Path:
    """Make file writable.

    Args:
        path: File path.

    Returns:
        Path object.
    """
    p = Path(path)
    import stat
    p.chmod(p.stat().st_mode | stat.S_IWUSR)
    return p


def get_size(path: Path | str) -> int:
    """Get size of file in bytes.

    Args:
        path: File path.

    Returns:
        Size in bytes.
    """
    return Path(path).stat().st_size


def get_total_size(paths: Sequence[Path | str]) -> int:
    """Get total size of multiple paths.

    Args:
        paths: Paths to measure.

    Returns:
        Total size in bytes.
    """
    total = 0
    for p in paths:
        pp = Path(p)
        if pp.is_file():
            total += pp.stat().st_size
        elif pp.is_dir():
            for f in pp.rglob("*"):
                if f.is_file():
                    total += f.stat().st_size
    return total


def get_file_count(path: Path | str) -> int:
    """Count files in directory.

    Args:
        path: Directory path.

    Returns:
        File count.
    """
    return len([p for p in Path(path).rglob("*") if p.is_file()])


def get_dir_count(path: Path | str) -> int:
    """Count directories in path.

    Args:
        path: Directory path.

    Returns:
        Directory count.
    """
    return len([p for p in Path(path).rglob("*") if p.is_dir()])


def get_extension(path: Path | str) -> str:
    """Get file extension.

    Args:
        path: File path.

    Returns:
        Extension including dot.
    """
    return Path(path).suffix


def change_extension(path: Path | str, new_ext: str) -> Path:
    """Change file extension.

    Args:
        path: File path.
        new_ext: New extension (with or without dot).

    Returns:
        Path with new extension.
    """
    p = Path(path)
    ext = new_ext if new_ext.startswith(".") else f".{new_ext}"
    return p.with_suffix(ext)


def strip_extension(path: Path | str) -> Path:
    """Remove file extension.

    Args:
        path: File path.

    Returns:
        Path without extension.
    """
    p = Path(path)
    return p.with_suffix("")


def add_extension(path: Path | str, ext: str) -> Path:
    """Add extension to file.

    Args:
        path: File path.
        ext: Extension to add (with or without dot).

    Returns:
        Path with new extension.
    """
    p = Path(path)
    if not ext.startswith("."):
        ext = f".{ext}"
    return p.with_suffix(p.suffix + ext)


def split_path(path: Path | str) -> tuple[list[str], str]:
    """Split path into directories and filename.

    Args:
        path: Path to split.

    Returns:
        Tuple of (dir_parts, filename).
    """
    p = Path(path)
    return list(p.parts[:-1]), p.parts[-1]


def join_path(*parts: Path | str) -> Path:
    """Join path parts.

    Args:
        *parts: Path parts to join.

    Returns:
        Joined path.
    """
    result = Path("")
    for part in parts:
        result = result / part
    return result


def normalize_path(path: Path | str) -> str:
    """Normalize path string.

    Args:
        path: Path to normalize.

    Returns:
        Normalized path string.
    """
    return os.path.normpath(str(path))


def expand_path(path: Path | str) -> Path:
    """Expand user and environment variables.

    Args:
        path: Path to expand.

    Returns:
        Expanded path.
    """
    return Path(os.path.expanduser(os.path.expandvars(str(path))))


def resolve_symlink(path: Path | str) -> Path:
    """Resolve symlink to real path.

    Args:
        path: Symlink path.

    Returns:
        Resolved real path.
    """
    return Path(path).resolve()


def relative_to(path: Path | str, base: Path | str) -> Path:
    """Get relative path.

    Args:
        path: Target path.
        base: Base path.

    Returns:
        Relative path.
    """
    return Path(path).relative_to(base)


def common_path(*paths: Path | str) -> Path | None:
    """Get common path prefix.

    Args:
        *paths: Paths to compare.

    Returns:
        Common path or None.
    """
    if not paths:
        return None
    return os.path.commonpath([str(p) for p in paths])


from contextlib import contextmanager
from datetime import datetime


@contextmanager
def temp_dir(suffix: str = "", prefix: str = "tmp", dir: Path | str | None = None):
    """Context manager for temporary directory.

    Args:
        suffix: Directory name suffix.
        prefix: Directory name prefix.
        dir: Parent directory.

    Yields:
        Path to temporary directory.
    """
    import tempfile
    td = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    yield Path(td)
    shutil.rmtree(td)


@contextmanager
def temp_file(suffix: str = "", prefix: str = "tmp", dir: Path | str | None = None):
    """Context manager for temporary file.

    Args:
        suffix: File name suffix.
        prefix: File name prefix.
        dir: Parent directory.

    Yields:
        Path to temporary file.
    """
    import tempfile
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    os.close(fd)
    yield Path(path)
    os.unlink(path)


@contextmanager
def chdir(path: Path | str):
    """Context manager for changing directory.

    Args:
        path: Directory to change to.

    Yields:
        Path to directory.
    """
    old = os.getcwd()
    os.chdir(str(path))
    try:
        yield Path(path)
    finally:
        os.chdir(old)


class FilePattern:
    """Match files by pattern."""

    def __init__(self, pattern: str) -> None:
        self._pattern = pattern
        self._regex = self._pattern_to_regex(pattern)

    def _pattern_to_regex(self, pattern: str) -> str:
        """Convert glob pattern to regex."""
        import re
        pattern = re.escape(pattern)
        pattern = pattern.replace(r"\*", ".*")
        pattern = pattern.replace(r"\?", ".")
        return f"^{pattern}$"

    def matches(self, path: Path | str) -> bool:
        """Check if path matches pattern."""
        import re
        return bool(re.match(self._regex, str(path)))


class PathFilter:
    """Filter paths by various criteria."""

    @staticmethod
    def by_name(pattern: str) -> Callable[[Path], bool]:
        """Filter by name pattern."""
        return lambda p: FilePattern(pattern).matches(p.name)

    @staticmethod
    def by_extension(*exts: str) -> Callable[[Path], bool]:
        """Filter by extension."""
        ext_set = set(e if e.startswith(".") else f".{e}" for e in exts)
        return lambda p: p.suffix in ext_set

    @staticmethod
    def by_size(min_size: int = 0, max_size: int = float("inf") | None) -> Callable[[Path], bool]:
        """Filter by file size."""
        return lambda p: min_size <= p.stat().st_size <= max_size

    @staticmethod
    def by_date(after: datetime | None = None, before: datetime | None = None) -> Callable[[Path], bool]:
        """Filter by modification date."""
        def filter_func(p: Path) -> bool:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if after and mtime < after:
                return False
            if before and mtime > before:
                return False
            return True
        return filter_func


class PathWalker:
    """Advanced path walking with filtering."""

    def __init__(
        self,
        root: Path | str,
        pattern: str = "*",
        filter_func: Callable[[Path], bool] | None = None,
        recursive: bool = True,
    ) -> None:
        self.root = Path(root)
        self.pattern = pattern
        self.filter_func = filter_func
        self.recursive = recursive

    def walk(self) -> Iterator[Path]:
        """Walk with filters."""
        if self.recursive:
            paths = self.root.rglob(self.pattern)
        else:
            paths = self.root.glob(self.pattern)

        for p in paths:
            if self.filter_func is None or self.filter_func(p):
                yield p

    def files(self) -> Iterator[Path]:
        """Walk only files."""
        def is_file(p: Path) -> bool:
            return p.is_file()
        original = self.filter_func
        if original:
            self.filter_func = lambda p: is_file(p) and original(p)
        else:
            self.filter_func = is_file
        yield from self.walk()
        self.filter_func = original

    def dirs(self) -> Iterator[Path]:
        """Walk only directories."""
        def is_dir(p: Path) -> bool:
            return p.is_dir()
        original = self.filter_func
        if original:
            self.filter_func = lambda p: is_dir(p) and original(p)
        else:
            self.filter_func = is_dir
        yield from self.walk()
        self.filter_func = original
