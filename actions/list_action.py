"""List utilities action for list manipulation.

This module provides list operations including
sorting, filtering, grouping, and set operations.

Example:
    >>> action = ListAction()
    >>> result = action.execute(operation="sort", data=[3, 1, 2])
"""

from __future__ import annotations

from typing import Any, Callable, Optional


class ListAction:
    """List manipulation action.

    Provides list operations including sorting,
    filtering, grouping, and transformations.

    Example:
        >>> action = ListAction()
        >>> result = action.execute(
        ...     operation="filter",
        ...     data=[1, 2, 3, 4, 5],
        ...     condition="x > 2"
        ... )
    """

    def __init__(self) -> None:
        """Initialize list action."""
        pass

    def execute(
        self,
        operation: str,
        data: Optional[list] = None,
        key: Optional[str] = None,
        reverse: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute list operation.

        Args:
            operation: Operation (sort, filter, reverse, etc.).
            data: List to operate on.
            key: Sort/filter key.
            reverse: Reverse order.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        if data is None:
            data = []

        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "sort":
            if key:
                result["data"] = sorted(data, key=lambda x: x.get(key) if isinstance(x, dict) else getattr(x, key, None), reverse=reverse)
            else:
                result["data"] = sorted(data, reverse=reverse)

        elif op == "reverse":
            result["data"] = list(reversed(data))

        elif op == "filter":
            condition = kwargs.get("condition")
            if condition:
                result["data"] = self._filter_items(data, condition)
            else:
                result["data"] = data

        elif op == "map":
            func = kwargs.get("func")
            if func:
                result["data"] = [func(item) for item in data]
            else:
                result["data"] = data

        elif op == "flatten":
            result["data"] = self._flatten_list(data)

        elif op == "unique":
            seen = set()
            unique = []
            for item in data:
                if item not in seen:
                    seen.add(item)
                    unique.append(item)
            result["data"] = unique
            result["removed"] = len(data) - len(unique)

        elif op == "chunk":
            size = kwargs.get("size", 2)
            result["data"] = self._chunk_list(data, size)

        elif op == "zip":
            other = kwargs.get("other", [])
            result["data"] = list(zip(data, other))

        elif op == "enumerate":
            result["data"] = list(enumerate(data))

        elif op == "first":
            result["item"] = data[0] if data else None

        elif op == "last":
            result["item"] = data[-1] if data else None

        elif op == "take":
            count = kwargs.get("count", 1)
            result["data"] = data[:count]

        elif op == "drop":
            count = kwargs.get("count", 1)
            result["data"] = data[count:]

        elif op == "count":
            result["count"] = len(data)

        elif op == "sum":
            try:
                result["sum"] = sum(data)
            except TypeError:
                result["sum"] = None

        elif op == "avg":
            try:
                result["avg"] = sum(data) / len(data)
            except TypeError:
                result["avg"] = None

        elif op == "min":
            result["min"] = min(data) if data else None

        elif op == "max":
            result["max"] = max(data) if data else None

        elif op == "group_by":
            if not key:
                raise ValueError("key required for 'group_by'")
            result["groups"] = self._group_by_key(data, key)

        elif op == "partition":
            predicate = kwargs.get("predicate")
            if not predicate:
                raise ValueError("predicate required")
            result["matching"] = [item for item in data if predicate(item)]
            result["non_matching"] = [item for item in data if not predicate(item)]

        elif op == "shuffle":
            import random
            shuffled = list(data)
            random.shuffle(shuffled)
            result["data"] = shuffled

        elif op == "intersection":
            other = kwargs.get("other", [])
            result["data"] = list(set(data) & set(other))

        elif op == "union":
            other = kwargs.get("other", [])
            result["data"] = list(set(data) | set(other))

        elif op == "difference":
            other = kwargs.get("other", [])
            result["data"] = list(set(data) - set(other))

        else:
            raise ValueError(f"Unknown operation: {operation}")

        if "data" in result:
            result["count"] = len(result["data"])

        return result

    def _filter_items(self, data: list, condition: str) -> list:
        """Filter items by condition.

        Args:
            data: List to filter.
            condition: Condition string.

        Returns:
            Filtered list.
        """
        import re

        # Parse simple conditions like "x > 2" or "x == 'test'"
        match = re.match(r"(\w+)\s*(>=|<=|!=|>|<|==)\s*(.+)", condition)
        if not match:
            return data

        field, op, value_str = match.groups()

        # Try to parse value
        try:
            if value_str.startswith(("'", '"')) and value_str.endswith(("'", '"')):
                value = value_str[1:-1]
            else:
                value = int(value_str)
        except ValueError:
            try:
                value = float(value_str)
            except ValueError:
                value = value_str

        filtered = []
        for item in data:
            item_value = item.get(field) if isinstance(item, dict) else getattr(item, field, None)
            if self._check_condition(item_value, op, value):
                filtered.append(item)

        return filtered

    def _check_condition(self, left: Any, op: str, right: Any) -> bool:
        """Check condition."""
        if op == "==":
            return left == right
        elif op == "!=":
            return left != right
        elif op == ">":
            return left > right
        elif op == "<":
            return left < right
        elif op == ">=":
            return left >= right
        elif op == "<=":
            return left <= right
        return False

    def _flatten_list(self, data: list) -> list:
        """Flatten nested lists.

        Args:
            data: List to flatten.

        Returns:
            Flattened list.
        """
        result = []
        for item in data:
            if isinstance(item, list):
                result.extend(self._flatten_list(item))
            else:
                result.append(item)
        return result

    def _chunk_list(self, data: list, size: int) -> list:
        """Split list into chunks.

        Args:
            data: List to chunk.
            size: Chunk size.

        Returns:
            List of chunks.
        """
        return [data[i:i + size] for i in range(0, len(data), size)]

    def _group_by_key(self, data: list, key: str) -> dict:
        """Group items by key.

        Args:
            data: List to group.
            key: Group key.

        Returns:
            Groups dictionary.
        """
        groups = {}
        for item in data:
            item_key = item.get(key) if isinstance(item, dict) else getattr(item, key, None)
            if item_key not in groups:
                groups[item_key] = []
            groups[item_key].append(item)
        return groups
