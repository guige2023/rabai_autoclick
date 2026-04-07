"""Dictionary utilities action for dictionary manipulation.

This module provides dictionary operations including
key/value operations, merging, and nested access.

Example:
    >>> action = DictAction()
    >>> result = action.execute(operation="get", data={"a": 1}, key="a")
"""

from __future__ import annotations

from typing import Any, Optional


class DictAction:
    """Dictionary manipulation action.

    Provides dictionary operations including
    key/value access, merging, and nested operations.

    Example:
        >>> action = DictAction()
        >>> result = action.execute(
        ...     operation="merge",
        ...     a={"x": 1},
        ...     b={"y": 2}
        ... )
    """

    def __init__(self) -> None:
        """Initialize dict action."""
        pass

    def execute(
        self,
        operation: str,
        data: Optional[dict] = None,
        a: Optional[dict] = None,
        b: Optional[dict] = None,
        key: Optional[str] = None,
        value: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute dictionary operation.

        Args:
            operation: Operation (get, set, merge, etc.).
            data: Dictionary to operate on.
            a: First dictionary.
            b: Second dictionary.
            key: Key to access.
            value: Value to set.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "get":
            if not data or key is None:
                raise ValueError("data and key required for 'get'")
            result["value"] = data.get(key)
            result["exists"] = key in data

        elif op == "set":
            if not data or key is None:
                raise ValueError("data and key required for 'set'")
            data[key] = value
            result["data"] = data

        elif op == "delete":
            if not data or key is None:
                raise ValueError("data and key required for 'delete'")
            if key in data:
                del data[key]
            result["data"] = data

        elif op == "merge":
            if not a:
                raise ValueError("a required for 'merge'")
            other = b or kwargs.get("other", {})
            result["data"] = {**a, **other}

        elif op == "keys":
            if not data:
                raise ValueError("data required for 'keys'")
            result["keys"] = list(data.keys())
            result["count"] = len(data)

        elif op == "values":
            if not data:
                raise ValueError("data required for 'values'")
            result["values"] = list(data.values())
            result["count"] = len(data)

        elif op == "items":
            if not data:
                raise ValueError("data required for 'items'")
            result["items"] = list(data.items())
            result["count"] = len(data)

        elif op == "has_key":
            if not data or key is None:
                raise ValueError("data and key required for 'has_key'")
            result["has_key"] = key in data

        elif op == "update":
            if not data:
                raise ValueError("data required for 'update'")
            other = b or kwargs.get("other", {})
            data.update(other)
            result["data"] = data

        elif op == "pop":
            if not data or key is None:
                raise ValueError("data and key required for 'pop'")
            result["value"] = data.pop(key, None)
            result["data"] = data

        elif op == "clear":
            if not data:
                raise ValueError("data required for 'clear'")
            data.clear()
            result["data"] = data

        elif op == "copy":
            if not data:
                raise ValueError("data required for 'copy'")
            result["data"] = data.copy()

        elif op == "get_nested":
            if not data or not key:
                raise ValueError("data and key required")
            path = key.split(".")
            current = data
            for p in path:
                if isinstance(current, dict) and p in current:
                    current = current[p]
                else:
                    result["value"] = None
                    result["exists"] = False
                    return result
            result["value"] = current
            result["exists"] = True
            return result

        elif op == "set_nested":
            if not data or not key:
                raise ValueError("data and key required")
            path = key.split(".")
            current = data
            for p in path[:-1]:
                if p not in current:
                    current[p] = {}
                current = current[p]
            current[path[-1]] = value
            result["data"] = data

        elif op == "flatten":
            if not data:
                raise ValueError("data required for 'flatten'")
            result["data"] = self._flatten_dict(data)
            result["count"] = len(result["data"])

        elif op == "unflatten":
            if not data:
                raise ValueError("data required for 'unflatten'")
            result["data"] = self._unflatten_dict(data)

        elif op == "filter":
            if not data:
                raise ValueError("data required for 'filter'")
            predicate = kwargs.get("predicate")
            if predicate:
                result["data"] = {k: v for k, v in data.items() if predicate(k, v)}
            else:
                result["data"] = data.copy()

        elif op == "invert":
            if not data:
                raise ValueError("data required for 'invert'")
            result["data"] = {v: k for k, v in data.items()}

        elif op == "size":
            if not data:
                raise ValueError("data required for 'size'")
            result["size"] = len(data)

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten nested dictionary.

        Args:
            d: Dictionary to flatten.
            parent_key: Parent key prefix.
            sep: Separator.

        Returns:
            Flattened dictionary.
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _unflatten_dict(self, d: dict, sep: str = ".") -> dict:
        """Unflatten dictionary.

        Args:
            d: Dictionary to unflatten.
            sep: Separator.

        Returns:
            Nested dictionary.
        """
        result = {}
        for key, value in d.items():
            parts = key.split(sep)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result
