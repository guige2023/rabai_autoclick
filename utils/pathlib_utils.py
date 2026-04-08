"""Path utilities for RabAI AutoClick.

Provides:
- Path manipulation and construction
- File/directory checks
- Path resolution and normalization
"""

from __future__ import annotations

import os
import pathlib
from typing import (
    List,
    Optional,
)


def ensure_ext(
    path: str,
    extension: str,
) -> str:
    """Ensure a path has a given extension.

    Args:
        path: File path.
        extension: Extension (with or without leading dot).

    Returns:
        Path with extension.
    """
    if not extension.startswith("."):
        extension = "." + extension
    p = pathlib.Path(path)
    if p.suffix != extension:
        return str(p.with_suffix(extension))
    return path


def add_suffix(
    path: str,
    suffix: str,
) -> str:
    """Add a suffix to a file path (before extension).

    Args:
        path: File path.
        suffix: Suffix to add.

    Returns:
        Path with suffix added.
    """
    p = pathlib.Path(path)
    return str(p.parent / f"{p.stem}{suffix}{p.suffix}")


def change_ext(
    path: str,
    new_ext: str,
) -> str:
    """Change the extension of a file path.

    Args:
        path: File path.
        new_ext: New extension (with or without leading dot).

    Returns:
        Path with new extension.
    """
    if not new_ext.startswith("."):
        new_ext = "." + new_ext
    p = pathlib.Path(path)
    return str(p.with_suffix(new_ext))


def get_relative_path(
    path: str,
    base: str,
) -> str:
    """Get path relative to a base directory.

    Args:
        path: Target path.
        base: Base directory path.

    Returns:
        Relative path string.
    """
    return str(pathlib.Path(path).relative_to(base))


def common_path(*paths: str) -> str:
    """Get the common parent directory of paths.

    Args:
        *paths: Path strings.

    Returns:
        Common parent directory path.
    """
    if not paths:
        return ""
    parsed = [pathlib.Path(p) for p in paths]
    return str(pathlib.Path(*pathlib.Path(*parsed[0].parts).joinpath(*parsed[1:]).parts[:1]))  # type: ignore


def is_subpath(path: str, parent: str) -> bool:
    """Check if path is a subpath of parent.

    Args:
        path: Potential subpath.
        parent: Parent directory.

    Returns:
        True if path is under parent.
    """
    try:
        pathlib.Path(path).relative_to(parent)
        return True
    except ValueError:
        return False


def expand_user(path: str) -> str:
    """Expand ~ in a path.

    Args:
        path: Path with possible ~.

    Returns:
        Expanded path.
    """
    return str(pathlib.Path(path).expanduser())


def resolve_symlinks(path: str) -> str:
    """Resolve symlinks in a path.

    Args:
        path: Path to resolve.

    Returns:
        Resolved absolute path.
    """
    return str(pathlib.Path(path).resolve())


def list_files(
    directory: str,
    pattern: str = "*",
    recursive: bool = False,
) -> List[str]:
    """List files in a directory.

    Args:
        directory: Directory path.
        pattern: Glob pattern.
        recursive: Whether to recurse subdirectories.

    Returns:
        List of file paths.
    """
    p = pathlib.Path(directory)
    if recursive:
        return [str(f) for f in p.rglob(pattern) if f.is_file()]
    return [str(f) for f in p.glob(pattern) if f.is_file()]


def list_dirs(
    directory: str,
    recursive: bool = False,
) -> List[str]:
    """List subdirectories in a directory.

    Args:
        directory: Directory path.
        recursive: Whether to recurse subdirectories.

    Returns:
        List of directory paths.
    """
    p = pathlib.Path(directory)
    if recursive:
        return [str(d) for d in p.rglob("*") if d.is_dir()]
    return [str(d) for d in p.glob("*") if d.is_dir()]


def path_parts(path: str) -> List[str]:
    """Split a path into its component parts.

    Args:
        path: Path to split.

    Returns:
        List of path components.
    """
    return list(pathlib.Path(path).parts)


def with_parents(path: str) -> str:
    """Ensure all parent directories exist.

    Args:
        path: File or directory path.

    Returns:
        The path (parents are created).
    """
    p = pathlib.Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


__all__ = [
    "ensure_ext",
    "add_suffix",
    "change_ext",
    "get_relative_path",
    "common_path",
    "is_subpath",
    "expand_user",
    "resolve_symlinks",
    "list_files",
    "list_dirs",
    "path_parts",
    "with_parents",
]
