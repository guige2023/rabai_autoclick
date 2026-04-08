"""
Navigation Path Utilities

Provides utilities for managing navigation paths
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NavStep:
    """A single navigation step."""
    action: str
    target: str
    params: dict[str, Any] | None = None


@dataclass
class NavigationPath:
    """Represents a navigation path."""
    name: str
    steps: list[NavStep] = field(default_factory=list)
    description: str = ""


class NavigationPathManager:
    """
    Manages navigation paths for automation workflows.
    
    Stores and executes multi-step navigation sequences.
    """

    def __init__(self) -> None:
        self._paths: dict[str, NavigationPath] = {}

    def register_path(self, path: NavigationPath) -> None:
        """Register a navigation path."""
        self._paths[path.name] = path

    def get_path(self, name: str) -> NavigationPath | None:
        """Get a path by name."""
        return self._paths.get(name)

    def remove_path(self, name: str) -> bool:
        """Remove a path."""
        return self._paths.pop(name, None) is not None

    def list_paths(self) -> list[str]:
        """List all path names."""
        return list(self._paths.keys())

    def create_linear_path(
        self,
        name: str,
        steps: list[tuple[str, str]],
    ) -> NavigationPath:
        """
        Create a simple linear path.
        
        Args:
            name: Path name.
            steps: List of (action, target) tuples.
            
        Returns:
            Created NavigationPath.
        """
        nav_steps = [NavStep(action=a, target=t) for a, t in steps]
        path = NavigationPath(name=name, steps=nav_steps)
        self.register_path(path)
        return path
