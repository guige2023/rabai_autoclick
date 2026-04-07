"""pathlib action extensions for rabai_autoclick.

Provides high-level utilities for common filesystem operations using pathlib.
Includes file search, tree walking, size calculations, and path transformations.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

__all__ = [
    "ensure_dir",
    "ensure_parent",
    "copy_tree",
    "move_tree",
    "remove_tree",
    "find_files",
    "find_dirs",
    "walk_tree",
    "file_size",
    "total_size",
    "file_hash",
    "file_modified_time",
    "file_created_time",
    "file_extension",
    "change_extension",
    "relative_to",
    "glob_recurse",
    "touch",
    "make_executable",
    "is_empty_dir",
    "count_files",
    "count_lines",
    "read_text_lines",
    "write_text_lines",
    "expanduser",
    "resolve_path",
    "normalize_path",
    "split_path",
    "join_paths",
    "PathContext",
]


def ensure_dir(path: Path | str) -> Path:
    """Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path to ensure.

    Returns:
        Path object of the ensured directory.

    Raises:
        OSError: If directory creation fails.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_parent(path: Path | str) -> Path:
    """Ensure the parent directory of a file path exists.

    Args:
        path: File path whose parent should be ensured.

    Returns:
        Path object of the parent directory.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def copy_tree(src: Path | str, dst: Path | str, ignore_patterns: Sequence[str] | None = None) -> Path:
    """Copy an entire directory tree.

    Args:
        src: Source directory path.
        dst: Destination directory path.
        ignore_patterns: Optional glob patterns to ignore (e.g., ["__pycache__", "*.pyc"]).

    Returns:
        Destination path.

    Raises:
        FileNotFoundError: If source directory does not exist.
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(f"Source directory not found: {src}")

    if ignore_patterns:
        ignore = shutil.ignore_patterns(*ignore_patterns)
    else:
        ignore = None

    shutil.copytree(src_path, dst_path, ignore=ignore, dirs_exist_ok=True)
    return dst_path


def move_tree(src: Path | str, dst: Path | str) -> Path:
    """Move an entire directory tree.

    Args:
        src: Source directory path.
        dst: Destination path.

    Returns:
        Destination path.

    Raises:
        FileNotFoundError: If source does not exist.
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(f"Source not found: {src}")

    shutil.move(str(src_path), str(dst_path))
    return dst_path


def remove_tree(path: Path | str) -> None:
    """Remove a directory tree and all its contents.

    Args:
        path: Directory path to remove.
    """
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)


def find_files(
    root: Path | str,
    pattern: str = "*",
    recursive: bool = True,
    include_hidden: bool = False,
) -> list[Path]:
    """Find files matching a glob pattern.

    Args:
        root: Root directory to search from.
        pattern: Glob pattern to match (default "*").
        recursive: If True, search recursively.
        include_hidden: If True, include hidden files/directories.

    Returns:
        List of matching file paths.
    """
    root_path = Path(root)
    if recursive:
        matches = list(root_path.rglob(pattern))
    else:
        matches = list(root_path.glob(pattern))

    result = []
    for p in matches:
        if not include_hidden and any(part.startswith(".") for part in p.parts):
            continue
        if p.is_file():
            result.append(p)

    return result


def find_dirs(
    root: Path | str,
    pattern: str = "*",
    recursive: bool = True,
    include_hidden: bool = False,
) -> list[Path]:
    """Find directories matching a glob pattern.

    Args:
        root: Root directory to search from.
        pattern: Glob pattern to match.
        recursive: If True, search recursively.
        include_hidden: If True, include hidden directories.

    Returns:
        List of matching directory paths.
    """
    root_path = Path(root)
    if recursive:
        matches = list(root_path.rglob(pattern))
    else:
        matches = list(root_path.glob(pattern))

    result = []
    for p in matches:
        if not include_hidden and any(part.startswith(".") for part in p.parts):
            continue
        if p.is_dir():
            result.append(p)

    return result


def walk_tree(
    root: Path | str,
    filter_func: Callable[[Path], bool] | None = None,
    include_files: bool = True,
    include_dirs: bool = True,
) -> Iterator[Path]:
    """Walk a directory tree with optional filtering.

    Args:
        root: Root directory to walk.
        filter_func: Optional function that returns True to include a path.
        include_files: If True, yield file paths.
        include_dirs: If True, yield directory paths.

    Yields:
        Path objects for each matching item.
    """
    root_path = Path(root)
    for item in root_path.rglob("*"):
        if filter_func and not filter_func(item):
            continue
        if item.is_file() and include_files:
            yield item
        elif item.is_dir() and include_dirs:
            yield item


def file_size(path: Path | str) -> int:
    """Get the size of a file in bytes.

    Args:
        path: File path.

    Returns:
        File size in bytes.

    Raises:
        FileNotFoundError: If file does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return p.stat().st_size


def total_size(paths: Sequence[Path | str]) -> int:
    """Calculate total size of multiple files/directories.

    Args:
        paths: Sequence of file or directory paths.

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


def file_hash(path: Path | str, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
    """Calculate hash of a file.

    Args:
        path: File path.
        algorithm: Hash algorithm ("md5", "sha1", "sha256", "sha512").
        chunk_size: Read chunk size in bytes.

    Returns:
        Hexadecimal hash string.

    Raises:
        FileNotFoundError: If file does not exist.
        ValueError: If algorithm is unsupported.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")

    try:
        hasher = hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with open(p, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)

    return hasher.hexdigest()


def file_modified_time(path: Path | str) -> datetime:
    """Get the last modified time of a file.

    Args:
        path: File path.

    Returns:
        datetime object of last modification.
    """
    p = Path(path)
    return datetime.fromtimestamp(p.stat().st_mtime)


def file_created_time(path: Path | str) -> datetime:
    """Get the creation time of a file.

    Args:
        path: File path.

    Returns:
        datetime object of creation time.
    """
    p = Path(path)
    return datetime.fromtimestamp(p.stat().st_ctime)


def file_extension(path: Path | str) -> str:
    """Get the file extension (including the dot).

    Args:
        path: File path.

    Returns:
        File extension string (e.g., ".txt") or empty string.
    """
    return Path(path).suffix


def change_extension(path: Path | str, new_ext: str) -> Path:
    """Change the extension of a file path.

    Args:
        path: Original file path.
        new_ext: New extension (with or without leading dot).

    Returns:
        Path with changed extension.
    """
    p = Path(path)
    ext = new_ext if new_ext.startswith(".") else f".{new_ext}"
    return p.with_suffix(ext)


def relative_to(path: Path | str, base: Path | str) -> Path:
    """Get relative path from base to path.

    Args:
        path: Target path.
        base: Base path.

    Returns:
        Relative path.

    Raises:
        ValueError: If path is not relative to base.
    """
    return Path(path).relative_to(Path(base))


def glob_recurse(root: Path | str, pattern: str, include_hidden: bool = False) -> list[Path]:
    """Recursively glob files matching pattern.

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


def touch(path: Path | str) -> Path:
    """Create an empty file or update its timestamp.

    Args:
        path: File path to touch.

    Returns:
        Path object of the file.
    """
    p = Path(path)
    p.touch()
    return p


def make_executable(path: Path | str) -> Path:
    """Make a file executable (add user execute bit).

    Args:
        path: File path.

    Returns:
        Path object.
    """
    p = Path(path)
    mode = p.stat().st_mode
    import stat

    p.chmod(mode | stat.S_IXUSR)
    return p


def is_empty_dir(path: Path | str) -> bool:
    """Check if a directory is empty.

    Args:
        path: Directory path.

    Returns:
        True if directory is empty, False otherwise.
    """
    p = Path(path)
    if not p.is_dir():
        return False
    return not any(p.iterdir())


def count_files(path: Path | str, pattern: str = "*", recursive: bool = True) -> int:
    """Count files matching pattern.

    Args:
        path: Directory path.
        pattern: Glob pattern.
        recursive: Search recursively.

    Returns:
        Number of matching files.
    """
    return len(find_files(path, pattern, recursive))


def count_lines(path: Path | str) -> int:
    """Count lines in a text file.

    Args:
        path: File path.

    Returns:
        Number of lines.
    """
    p = Path(path)
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f)


def read_text_lines(path: Path | str, strip: bool = True) -> list[str]:
    """Read text file as list of lines.

    Args:
        path: File path.
        strip: Strip whitespace from each line.

    Returns:
        List of lines.
    """
    p = Path(path)
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    if strip:
        return [line.rstrip("\n\r") for line in lines]
    return lines


def write_text_lines(path: Path | str, lines: Sequence[str]) -> Path:
    """Write lines to a text file.

    Args:
        path: File path.
        lines: Sequence of lines to write.

    Returns:
        Path object.
    """
    p = Path(path)
    ensure_parent(p)
    with open(p, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line)
            if not line.endswith("\n"):
                f.write("\n")
    return p


def expanduser(path: Path | str) -> Path:
    """Expand ~ and ~user constructs.

    Args:
        path: Path with potential tilde.

    Returns:
        Expanded path.
    """
    return Path(path).expanduser()


def resolve_path(path: Path | str) -> Path:
    """Resolve path to absolute, resolving symlinks.

    Args:
        path: Path to resolve.

    Returns:
        Resolved absolute path.
    """
    return Path(path).resolve()


def normalize_path(path: Path | str) -> str:
    """Normalize a path to a canonical string.

    Args:
        path: Path to normalize.

    Returns:
        Normalized path string.
    """
    return os.path.normpath(str(path))


def split_path(path: Path | str) -> tuple[list[str], str]:
    """Split path into directory parts and the final name.

    Args:
        path: Path to split.

    Returns:
        Tuple of (list of directory parts, filename).
    """
    p = Path(path)
    return list(p.parts[:-1]), p.parts[-1]


def join_paths(*parts: Path | str) -> Path:
    """Join multiple path parts.

    Args:
        *parts: Path parts to join.

    Returns:
        Joined path.
    """
    result = Path("")
    for part in parts:
        result = result / Path(part)
    return result


class PathContext:
    """Context manager for temporary directory changes.

    Example:
        with PathContext("/tmp/work"):
            # work in /tmp/work
        # back to original directory
    """

    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path else None
        self._original: Path | None = None

    def __enter__(self) -> Path:
        if self.path:
            self._original = Path.cwd()
            import os

            os.chdir(self.path)
        return self.path or Path.cwd()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._original:
            import os

            os.chdir(self._original)
