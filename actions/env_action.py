"""Environment action for environment variable management.

This module provides environment variable operations
including get, set, list, and export.

Example:
    >>> action = EnvAction()
    >>> result = action.execute(command="get", key="PATH")
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class EnvVariable:
    """Represents an environment variable."""
    key: str
    value: str


class EnvAction:
    """Environment variable management action.

    Provides environment variable operations
    including get, set, list, and export.

    Example:
        >>> action = EnvAction()
        >>> result = action.execute(
        ...     command="set",
        ...     key="MY_VAR",
        ...     value="test"
        ... )
    """

    def __init__(self) -> None:
        """Initialize environment action."""
        pass

    def execute(
        self,
        command: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute environment command.

        Args:
            command: Command (get, set, list, unset, expand).
            key: Variable name.
            value: Variable value.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd == "get":
            if not key:
                raise ValueError("key required for 'get'")
            result["value"] = os.environ.get(key, "")
            result["exists"] = key in os.environ

        elif cmd == "set":
            if not key:
                raise ValueError("key required for 'set'")
            if value is None:
                raise ValueError("value required for 'set'")
            os.environ[key] = value
            result["set"] = True
            result["key"] = key

        elif cmd == "unset":
            if not key:
                raise ValueError("key required for 'unset'")
            if key in os.environ:
                del os.environ[key]
            result["unset"] = True

        elif cmd == "list":
            result["variables"] = [
                {"key": k, "value": v}
                for k, v in os.environ.items()
            ]
            result["count"] = len(os.environ)

        elif cmd == "list_filtered":
            if not key:
                raise ValueError("key pattern required")
            pattern = key.lower()
            result["variables"] = [
                {"key": k, "value": v}
                for k, v in os.environ.items()
                if pattern in k.lower()
            ]
            result["count"] = len(result["variables"])

        elif cmd == "expand":
            if not key:
                raise ValueError("key required for 'expand'")
            path_str = os.environ.get(key, key)
            result["value"] = os.path.expandvars(path_str)
            result["expanded"] = True

        elif cmd == "exists":
            if not key:
                raise ValueError("key required for 'exists'")
            result["exists"] = key in os.environ

        elif cmd == "copy":
            src = kwargs.get("source")
            dst = kwargs.get("destination")
            if not src or not dst:
                raise ValueError("source and destination required")
            os.environ[dst] = os.environ.get(src, "")
            result["copied"] = True

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def get_all(self) -> dict[str, str]:
        """Get all environment variables.

        Returns:
            Dictionary of all variables.
        """
        return dict(os.environ)

    def get_path(self) -> list[str]:
        """Get PATH variable as list.

        Returns:
            List of path directories.
        """
        path_value = os.environ.get("PATH", "")
        return [p for p in path_value.split(os.pathsep) if p]

    def set_from_dict(self, variables: dict[str, str]) -> None:
        """Set multiple variables from dictionary.

        Args:
            variables: Dictionary of variables.
        """
        for key, value in variables.items():
            os.environ[key] = value
