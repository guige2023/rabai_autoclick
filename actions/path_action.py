"""Path utilities action for file path manipulation.

This module provides path manipulation utilities including
resolution, normalization, and component extraction.

Example:
    >>> action = PathAction()
    >>> result = action.execute(operation="normalize", path="~/Documents/../file.txt")
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional


class PathAction:
    """Path manipulation action.

    Provides path operations including normalization,
    component extraction, and path joining.

    Example:
        >>> action = PathAction()
        >>> result = action.execute(
        ...     operation="join",
        ...     parts=["/home", "user", "documents"]
        ... )
    """

    def __init__(self) -> None:
        """Initialize path action."""
        pass

    def execute(
        self,
        operation: str,
        path: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute path operation.

        Args:
            operation: Operation (normalize, join, split, etc.).
            path: File path.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "normalize":
            if not path:
                raise ValueError("path required")
            result["path"] = os.path.normpath(path)
            result["expanded"] = os.path.expanduser(path)
            result["absolute"] = os.path.abspath(path)

        elif op == "join":
            parts = kwargs.get("parts", [])
            if not parts:
                raise ValueError("parts required")
            result["path"] = os.path.join(*parts)

        elif op == "split":
            if not path:
                raise ValueError("path required")
            directory, filename = os.path.split(path)
            name, ext = os.path.splitext(filename)
            result["directory"] = directory
            result["filename"] = filename
            result["name"] = name
            result["extension"] = ext

        elif op == "dirname":
            if not path:
                raise ValueError("path required")
            result["directory"] = os.path.dirname(path)

        elif op == "basename":
            if not path:
                raise ValueError("path required")
            result["filename"] = os.path.basename(path)

        elif op == "exists":
            if not path:
                raise ValueError("path required")
            result["exists"] = os.path.exists(path)
            result["is_file"] = os.path.isfile(path)
            result["is_dir"] = os.path.isdir(path)

        elif op == "expand":
            if not path:
                raise ValueError("path required")
            result["expanded"] = os.path.expanduser(path)
            result["absolute"] = os.path.abspath(result["expanded"])

        elif op == "absolute":
            if not path:
                raise ValueError("path required")
            result["absolute"] = os.path.abspath(path)

        elif op == "is_absolute":
            if not path:
                raise ValueError("path required")
            result["is_absolute"] = os.path.isabs(path)

        elif op == "components":
            if not path:
                raise ValueError("path required")
            parts = []
            while True:
                head, tail = os.path.split(path)
                if tail:
                    parts.insert(0, tail)
                    path = head
                else:
                    if head:
                        parts.insert(0, head)
                    break
            result["components"] = parts
            result["count"] = len(parts)

        elif op == "size":
            if not path:
                raise ValueError("path required")
            if os.path.isfile(path):
                result["size"] = os.path.getsize(path)
            else:
                result["error"] = "Not a file"

        elif op == "list_dir":
            dir_path = kwargs.get("path", path or ".")
            if not os.path.isdir(dir_path):
                return {"success": False, "error": "Not a directory"}
            items = os.listdir(dir_path)
            result["items"] = items
            result["count"] = len(items)

        elif op == "parent":
            if not path:
                raise ValueError("path required")
            result["parent"] = os.path.dirname(path)

        elif op == "split_ext":
            if not path:
                raise ValueError("path required")
            name, ext = os.path.splitext(path)
            result["name"] = name
            result["extension"] = ext

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result
