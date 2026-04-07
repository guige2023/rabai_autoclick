"""Pathlib utilities v4 - path validation and manipulation.

Path validation, normalization, and manipulation utilities.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = [
    "normalize_path",
    "is_safe_path",
    "expand_user",
    "expand_vars",
    "make_absolute",
    "relative_to_unsafe",
    "join_unsafe",
]


def normalize_path(path: str | Path) -> Path:
    """Normalize a path.

    Args:
        path: Path to normalize.

    Returns:
        Normalized Path.
    """
    return Path(path).expanduser().resolve()


def is_safe_path(path: str | Path, base: str | Path) -> bool:
    """Check if path is safely within base.

    Args:
        path: Path to check.
        base: Base directory.

    Returns:
        True if safe.
    """
    try:
        p = Path(path).resolve()
        b = Path(base).resolve()
        return str(p).startswith(str(b))
    except Exception:
        return False


def expand_user(path: str | Path) -> Path:
    """Expand ~ in path.

    Args:
        path: Path with possible ~.

    Returns:
        Expanded Path.
    """
    return Path(path).expanduser()


def expand_vars(path: str | Path) -> Path:
    """Expand environment variables.

    Args:
        path: Path with possible $VAR or ${VAR}.

    Returns:
        Path with expanded vars.
    """
    import os
    return Path(os.path.expandvars(str(path)))


def make_absolute(path: str | Path, base: str | Path | None = None) -> Path:
    """Make path absolute.

    Args:
        path: Path to make absolute.
        base: Optional base directory.

    Returns:
        Absolute Path.
    """
    p = Path(path)
    if p.is_absolute():
        return p
    if base:
        return Path(base) / p
    return p.resolve()


def relative_to_unsafe(path: str | Path, base: str | Path) -> Path:
    """Get relative path (unsafe version).

    Args:
        path: Target path.
        base: Base path.

    Returns:
        Relative path.
    """
    return Path(path).relative_to(base)


def join_unsafe(*parts: str | Path) -> Path:
    """Join path parts (unsafe).

    Args:
        *parts: Path parts to join.

    Returns:
        Joined Path.
    """
    result = Path(parts[0]) if parts else Path(".")
    for p in parts[1:]:
        result = result / p
    return result
