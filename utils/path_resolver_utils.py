"""
Path Resolver Utilities

Provides utilities for resolving and normalizing
file paths in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import os
import pathlib


class PathResolver:
    """
    Resolves and normalizes file system paths.
    
    Handles relative paths, environment variables,
    and path normalization.
    """

    def __init__(self, base_dir: str | None = None) -> None:
        self._base_dir = base_dir or os.getcwd()

    def resolve(self, path: str) -> str:
        """
        Resolve a path relative to base directory.
        
        Args:
            path: Path to resolve.
            
        Returns:
            Absolute resolved path.
        """
        if os.path.isabs(path):
            return os.path.normpath(path)
        return os.path.normpath(os.path.join(self._base_dir, path))

    def expand(self, path: str) -> str:
        """Expand environment variables and resolve path."""
        expanded = os.path.expandvars(os.path.expanduser(path))
        return self.resolve(expanded)

    def normalize(self, path: str) -> str:
        """Normalize path separators and references."""
        return os.path.normpath(path)

    def relative_to(self, path: str, base: str | None = None) -> str:
        """Get path relative to base directory."""
        base = base or self._base_dir
        return os.path.relpath(path, base)

    def is_subpath(self, path: str, parent: str) -> bool:
        """Check if path is a subpath of parent."""
        path = self.resolve(path)
        parent = self.resolve(parent)
        return path.startswith(parent + os.sep)
