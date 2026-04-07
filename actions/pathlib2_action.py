"""Pathlib extensions v2 - advanced path operations.

Extended pathlib utilities including glob patterns,
 temporary files, and path comparisons.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable, Iterator

__all__ = [
    "glob_recursive",
    "glob_pattern",
    "rglob",
    "find_files",
    "find_dirs",
    "walk_tree",
    "copy_tree",
    "move_tree",
    "atomic_write",
    "safe_write",
    " TemporaryDirectory",
    " TemporaryFile",
    "chdir",
    "path_relative_to",
    "common_ancestor",
    "is_subpath",
    "path_diff",
    "PathStats",
    "FileHasher",
]


def glob_recursive(pattern: str | Path) -> list[Path]:
    """Glob with ** recursive pattern.

    Args:
        pattern: Glob pattern with ** for recursion.

    Returns:
        List of matching paths.
    """
    p = Path(pattern)
    return list(p.parent.glob(p.name))


def glob_pattern(directory: str | Path, pattern: str, recursive: bool = False) -> list[Path]:
    """Glob with pattern in directory.

    Args:
        directory: Directory to search.
        pattern: Glob pattern.
        recursive: Whether to recurse.

    Returns:
        List of matching paths.
    """
    d = Path(directory)
    if recursive:
        return list(d.rglob(pattern))
    return list(d.glob(pattern))


def rglob(directory: str | Path, pattern: str) -> list[Path]:
    """Recursive glob (alias for rglob).

    Args:
        directory: Directory to search.
        pattern: Pattern to match.

    Returns:
        List of matching paths.
    """
    return list(Path(directory).rglob(pattern))


def find_files(directory: str | Path, predicate: Callable[[Path], bool] | None = None, max_depth: int | None = None) -> list[Path]:
    """Find files matching predicate.

    Args:
        directory: Root directory.
        predicate: Function returning bool for Path.
        max_depth: Maximum recursion depth.

    Returns:
        List of matching file paths.
    """
    results = []
    root = Path(directory)
    for item in _walk_depth(root, max_depth):
        if item.is_file() and (predicate is None or predicate(item)):
            results.append(item)
    return results


def _walk_depth(path: Path, max_depth: int | None) -> Iterator[Path]:
    """Walk directory with depth limit."""
    if max_depth is not None and max_depth < 0:
        return
    yield path
    if path.is_dir():
        for child in path.iterdir():
            yield from _walk_depth(child, max_depth - 1 if max_depth is not None else None)


def find_dirs(directory: str | Path, predicate: Callable[[Path], bool] | None = None) -> list[Path]:
    """Find directories matching predicate.

    Args:
        directory: Root directory.
        predicate: Function returning bool for Path.

    Returns:
        List of matching directories.
    """
    results = []
    for item in Path(directory).rglob("*"):
        if item.is_dir() and (predicate is None or predicate(item)):
            results.append(item)
    return results


def walk_tree(directory: str | Path, topdown: bool = True) -> Iterator[tuple[Path, list[Path], list[Path]]]:
    """Walk directory tree.

    Args:
        directory: Root directory.
        topdown: If True, walk top-down.

    Returns:
        Generator of (dirpath, dirnames, filenames).
    """
    for root, dirs, files in os.walk(directory, topdown=topdown):
        yield (Path(root), dirs, files)


def copy_tree(src: str | Path, dst: str | Path, overwrite: bool = False) -> None:
    """Copy entire directory tree.

    Args:
        src: Source directory.
        dst: Destination directory.
        overwrite: Whether to overwrite existing files.

    Raises:
        FileExistsError: If dst exists and overwrite is False.
    """
    src_p = Path(src)
    dst_p = Path(dst)
    if dst_p.exists() and not overwrite:
        raise FileExistsError(f"{dst} already exists")
    shutil.copytree(src_p, dst_p, dirs_exist_ok=overwrite)


def move_tree(src: str | Path, dst: str | Path, overwrite: bool = False) -> None:
    """Move directory tree.

    Args:
        src: Source directory.
        dst: Destination directory.
        overwrite: Whether to overwrite existing.

    Raises:
        FileExistsError: If dst exists and overwrite is False.
    """
    dst_p = Path(dst)
    if dst_p.exists() and not overwrite:
        raise FileExistsError(f"{dst} already exists")
    shutil.move(str(src), str(dst))


def atomic_write(path: str | Path, content: str | bytes, encoding: str = "utf-8") -> None:
    """Atomically write content to file.

    Args:
        path: Target file path.
        content: Content to write.
        encoding: Text encoding.

    Raises:
        ValueError: If content is str with binary mode.
    """
    p = Path(path)
    if isinstance(content, str):
        with tempfile.NamedTemporaryFile(mode="w", encoding=encoding, dir=p.parent, delete=False) as f:
            f.write(content)
            temp_path = f.name
    else:
        with tempfile.NamedTemporaryFile(mode="wb", dir=p.parent, delete=False) as f:
            f.write(content)
            temp_path = f.name
    os.replace(temp_path, p)


def safe_write(path: str | Path, content: str | bytes, encoding: str = "utf-8", backup: bool = True) -> Path | None:
    """Safely write with optional backup.

    Args:
        path: Target path.
        content: Content to write.
        encoding: Text encoding.
        backup: Whether to create backup.

    Returns:
        Path to backup if created.
    """
    p = Path(path)
    backup_path = None
    if backup and p.exists():
        backup_path = p.with_suffix(p.suffix + ".bak")
        shutil.copy2(p, backup_path)
    atomic_write(p, content, encoding)
    return backup_path


class TemporaryDirectory:
    """Context manager for temporary directory."""

    def __init__(self, suffix: str = "", prefix: str = "tmp", dir: str | Path | None = None) -> None:
        self._suffix = suffix
        self._prefix = prefix
        self._dir = Path(dir) if dir else None
        self._path: Path | None = None

    def __enter__(self) -> Path:
        self._path = Path(tempfile.mkdtemp(suffix=self._suffix, prefix=self._prefix, dir=str(self._dir) if self._dir else None))
        return self._path

    def __exit__(self, *args) -> None:
        if self._path and self._path.exists():
            shutil.rmtree(self._path)

    @property
    def path(self) -> Path | None:
        """Get path if entered."""
        return self._path


class TemporaryFile:
    """Context manager for temporary file."""

    def __init__(self, suffix: str = "", prefix: str = "tmp", dir: str | Path | None = None, binary: bool = False) -> None:
        self._suffix = suffix
        self._prefix = prefix
        self._dir = Path(dir) if dir else None
        self._binary = binary
        self._path: Path | None = None
        self._file = None

    def __enter__(self) -> tuple[Path, object]:
        mode = "wb" if self._binary else "w"
        self._file = tempfile.NamedTemporaryFile(mode=mode, suffix=self._suffix, prefix=self._prefix, dir=str(self._dir) if self._dir else None, delete=False)
        self._path = Path(self._file.name)
        return (self._path, self._file)

    def __exit__(self, *args) -> None:
        if self._file:
            self._file.close()
        if self._path and self._path.exists():
            os.unlink(self._path)

    @property
    def path(self) -> Path | None:
        return self._path


@contextmanager
def chdir(path: str | Path):
    """Context manager to change directory temporarily.

    Args:
        path: Directory to change to.

    Yields:
        Path of the directory.
    """
    old_dir = os.getcwd()
    try:
        os.chdir(str(path))
        yield Path(path)
    finally:
        os.chdir(old_dir)


def path_relative_to(path: str | Path, base: str | Path) -> Path:
    """Get path relative to base.

    Args:
        path: Path to make relative.
        base: Base path.

    Returns:
        Relative path.

    Raises:
        ValueError: If path is not under base.
    """
    p = Path(path).resolve()
    b = Path(base).resolve()
    try:
        return p.relative_to(b)
    except ValueError:
        raise ValueError(f"{path} is not relative to {base}")


def common_ancestor(*paths: str | Path) -> Path:
    """Find common ancestor of paths.

    Args:
        *paths: Paths to find common ancestor of.

    Returns:
        Common ancestor path.
    """
    if not paths:
        raise ValueError("At least one path required")
    resolved = [Path(p).resolve() for p in paths]
    if len(resolved) == 1:
        return resolved[0].parent
    result = resolved[0]
    for p in resolved[1:]:
        while not str(p).startswith(str(result)) and result != result.parent:
            result = result.parent
    return result


def is_subpath(path: str | Path, parent: str | Path) -> bool:
    """Check if path is under parent.

    Args:
        path: Path to check.
        parent: Parent path.

    Returns:
        True if path is under parent.
    """
    try:
        Path(path).resolve().relative_to(Path(parent).resolve())
        return True
    except ValueError:
        return False


def path_diff(path1: str | Path, path2: str | Path) -> dict[str, list[str]]:
    """Compare two directory trees.

    Args:
        path1: First directory.
        path2: Second directory.

    Returns:
        Dict with 'only1', 'only2', 'different' keys.
    """
    p1 = Path(path1)
    p2 = Path(path2)
    files1 = {p.relative_to(p1): p for p in p1.rglob("*") if p.is_file()}
    files2 = {p.relative_to(p2): p for p in p2.rglob("*") if p.is_file()}
    all_keys = set(files1.keys()) | set(files2.keys())
    only1 = [str(k) for k in files1 if k not in files2]
    only2 = [str(k) for k in files2 if k not in files1]
    different = [str(k) for k in files1 & files2 if not _files_equal(files1[k], files2[k])]
    return {"only1": only1, "only2": only2, "different": different}


def _files_equal(p1: Path, p2: Path) -> bool:
    """Check if two files are equal."""
    if p1.stat().st_size != p2.stat().st_size:
        return False
    with open(p1, "rb") as f1, open(p2, "rb") as f2:
        return f1.read() == f2.read()


class PathStats:
    """Path statistics collector."""

    def __init__(self) -> None:
        self.total_files = 0
        self.total_dirs = 0
        self.total_size = 0
        self.extensions: dict[str, int] = {}

    def collect(self, path: str | Path) -> None:
        """Collect stats from path."""
        p = Path(path)
        if p.is_file():
            self._add_file(p)
        else:
            for item in p.rglob("*"):
                if item.is_file():
                    self._add_file(item)

    def _add_file(self, path: Path) -> None:
        """Add file to stats."""
        self.total_files += 1
        self.total_size += path.stat().st_size
        ext = path.suffix or "(no extension)"
        self.extensions[ext] = self.extensions.get(ext, 0) + 1

    def summary(self) -> dict:
        """Get stats summary."""
        return {
            "total_files": self.total_files,
            "total_dirs": self.total_dirs,
            "total_size": self.total_size,
            "extensions": dict(sorted(self.extensions.items(), key=lambda x: x[1], reverse=True)),
        }


class FileHasher:
    """Compute file hashes."""

    def __init__(self, algorithm: str = "sha256") -> None:
        import hashlib
        self._algorithm = algorithm
        self._hash = hashlib.new(algorithm)

    def hash_file(self, path: str | Path) -> str:
        """Hash entire file.

        Args:
            path: File to hash.

        Returns:
            Hex digest of file.
        """
        import hashlib
        h = hashlib.new(self._algorithm)
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def hash_path(self, path: str | Path) -> dict[str, str]:
        """Hash all files in path recursively.

        Args:
            path: Directory or file to hash.

        Returns:
            Dict mapping relative paths to hashes.
    """
        p = Path(path)
        results = {}
        if p.is_file():
            results[str(p)] = self.hash_file(p)
        else:
            for f in p.rglob("*"):
                if f.is_file():
                    results[str(f.relative_to(p))] = self.hash_file(f)
        return results
