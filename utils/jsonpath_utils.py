"""
JSONPath query implementation for Python dictionaries.

Provides JSONPath 0.3 specification support for querying
nested Python dictionaries with expressions like $.store.book[*].author
and $..title.

Example:
    >>> from utils.jsonpath_utils import jsonpath_query, find_all, find_first
    >>> data = {"store": {"book": [{"author": "A"}, {"author": "B"}]}}
    >>> authors = jsonpath_query(data, "$.store.book[*].author")
    >>> print(authors)  # ["A", "B"]
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Union


class JsonPathNode:
    """Represents a node in a JSONPath query result."""

    def __init__(
        self,
        path: str,
        value: Any,
        key: Optional[str] = None,
        parent: Optional["JsonPathNode"] = None,
    ) -> None:
        """
        Initialize a JSONPath node.

        Args:
            path: Full path to this node.
            value: Node value.
            key: Key/index that led to this node.
            parent: Parent node.
        """
        self.path = path
        self.value = value
        self.key = key
        self.parent = parent

    def __repr__(self) -> str:
        return f"JsonPathNode({self.path!r}, {self.value!r})"


class JsonPathQuery:
    """
    JSONPath query engine for Python dictionaries.

    Supports JSONPath 0.3 specification including:
    - Root: $
    - Recursive descent: ..
    - Child: .
    - Wildcard: [*]
    - Index: [0]
    - Slice: [1:3]
    - Union: [0,2]
    - Filter: [?(@.price < 10)]
    """

    RECURSIVE_PATTERN = re.compile(r"\.\.")

    def __init__(self, data: Dict[str, Any]) -> None:
        """
        Initialize the query engine.

        Args:
            data: Root dictionary to query.
        """
        self.data = data

    def query(self, path: str) -> List[Any]:
        """
        Execute a JSONPath query.

        Args:
            path: JSONPath expression.

        Returns:
            List of matching values.
        """
        if not path:
            return [self.data]

        results: List[tuple[str, Any]] = []
        self._evaluate(path, "$", self.data, results, None)
        return [value for _, value in results]

    def query_with_paths(self, path: str) -> List[JsonPathNode]:
        """
        Execute a JSONPath query and return nodes with paths.

        Args:
            path: JSONPath expression.

        Returns:
            List of JsonPathNode objects.
        """
        if not path:
            return [JsonPathNode("$", self.data)]

        results: List[tuple[str, Any]] = []
        self._evaluate(path, "$", self.data, results, None)

        nodes: List[JsonPathNode] = []
        seen_paths: set = set()
        for p, v in results:
            if p not in seen_paths:
                seen_paths.add(p)
                nodes.append(JsonPathNode(p, v))
        return nodes

    def _evaluate(
        self,
        path: str,
        current_path: str,
        current_value: Any,
        results: List[tuple[str, Any]],
        parent_key: Optional[str],
    ) -> None:
        """Recursively evaluate a JSONPath expression."""
        if not path:
            results.append((current_path, current_value))
            return

        if path.startswith("$"):
            rest = path[1:]
            if not rest:
                results.append(("$", self.data))
                return
            if rest.startswith("."):
                self._evaluate(rest[1:], "$", self.data, results, None)
            elif rest.startswith("["):
                self._evaluate(rest, "$", self.data, results, None)
            elif rest.startswith(".."):
                self._evaluate(rest[2:], "$", self.data, results, None)
            return

        if path.startswith(".."):
            rest = path[2:]
            self._recursive_descent(rest, current_path, current_value, results)
            return

        if path.startswith("."):
            remaining = path[1:]
            if remaining.startswith("*"):
                if isinstance(current_value, dict):
                    for key in current_value:
                        new_path = f"{current_path}.{key}"
                        self._evaluate(
                            remaining[1:], new_path, current_value[key], results, key
                        )
                elif isinstance(current_value, list):
                    for i, item in enumerate(current_value):
                        new_path = f"{current_path}[{i}]"
                        self._evaluate(
                            remaining[1:], new_path, item, results, str(i)
                        )
            elif remaining.startswith("["):
                self._evaluate(remaining, current_path, current_value, results, parent_key)
            else:
                match = re.match(r"^([^.[]+)(.*)$", remaining)
                if match:
                    key = match.group(1)
                    rest = match.group(2)
                    new_path = f"{current_path}.{key}"

                    if isinstance(current_value, dict) and key in current_value:
                        self._evaluate(rest, new_path, current_value[key], results, key)
        elif path.startswith("["):
            end = self._find_bracket_end(path)
            bracket_expr = path[:end]
            rest = path[end:]

            if isinstance(current_value, (dict, list)):
                indices = self._parse_bracket_expr(bracket_expr, current_value)

                for idx in indices:
                    if isinstance(current_value, list):
                        if 0 <= idx < len(current_value):
                            new_path = f"{current_path}[{idx}]"
                            self._evaluate(rest, new_path, current_value[idx], results, str(idx))
                    elif isinstance(current_value, dict):
                        keys = list(current_value.keys())
                        if 0 <= idx < len(keys):
                            key = keys[idx]
                            new_path = f"{current_path}[{idx}]"
                            self._evaluate(rest, new_path, current_value[key], results, key)

    def _recursive_descent(
        self,
        path: str,
        current_path: str,
        current_value: Any,
        results: List[tuple[str, Any]],
    ) -> None:
        """Handle recursive descent (..) operator."""
        if not path:
            results.append((current_path, current_value))
            return

        if isinstance(current_value, dict):
            for key, value in current_value.items():
                new_path = f"{current_path}..{key}"
                if path.startswith(key):
                    rest = path[len(key) :]
                    if not rest:
                        results.append((new_path, value))
                    elif rest.startswith("."):
                        self._evaluate(rest[1:], new_path, value, results, key)
                    elif rest.startswith("["):
                        self._evaluate(rest, new_path, value, results, key)
                    elif rest.startswith(".."):
                        self._recursive_descent(rest[2:], new_path, value, results)
                else:
                    self._recursive_descent(path, new_path, value, results)
        elif isinstance(current_value, list):
            for i, item in enumerate(current_value):
                new_path = f"{current_path}[{i}]"
                self._recursive_descent(path, new_path, item, results)

    def _find_bracket_end(self, path: str) -> int:
        """Find the matching closing bracket."""
        depth = 0
        for i, char in enumerate(path):
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    return i + 1
        return len(path)

    def _parse_bracket_expr(
        self,
        expr: str,
        data: Union[dict, list]
    ) -> List[int]:
        """Parse a bracket expression and return indices."""
        expr = expr[1:-1]

        if expr == "*":
            if isinstance(data, list):
                return list(range(len(data)))
            elif isinstance(data, dict):
                return list(range(len(data)))

        if expr.startswith("?"):
            return self._parse_filter(expr[1:], data)

        if "," in expr:
            parts = expr.split(",")
            indices = []
            for part in parts:
                indices.extend(self._parse_single_index(part.strip(), data))
            return indices

        return self._parse_single_index(expr, data)

    def _parse_single_index(
        self,
        expr: str,
        data: Union[dict, list]
    ) -> List[int]:
        """Parse a single index expression."""
        if ":" in expr:
            parts = expr.split(":")
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if parts[1] else len(data) if isinstance(data, list) else len(data)
            step = int(parts[2]) if len(parts) > 2 else 1
            return list(range(start, end, step))
        try:
            return [int(expr)]
        except ValueError:
            return []

    def _parse_filter(
        self,
        expr: str,
        data: Union[dict, list]
    ) -> List[int]:
        """Parse a filter expression."""
        match = re.match(r"@\.(.+?)\s*([<>!=]+)\s*(.+)", expr)
        if not match:
            return []

        field = match.group(1)
        op = match.group(2)
        value = match.group(3).strip()

        if value.startswith(("'", '"')):
            value = value[1:-1]

        indices: List[int] = []

        if isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    item_val = item.get(field)
                else:
                    item_val = getattr(item, field, None)

                try:
                    item_val_num = float(item_val) if item_val is not None else None
                    value_num = float(value)
                except (ValueError, TypeError):
                    item_val_num = None
                    value_num = None

                if op == "<":
                    if item_val_num is not None and value_num is not None and item_val_num < value_num:
                        indices.append(i)
                elif op == ">":
                    if item_val_num is not None and value_num is not None and item_val_num > value_num:
                        indices.append(i)
                elif op == "<=":
                    if item_val_num is not None and value_num is not None and item_val_num <= value_num:
                        indices.append(i)
                elif op == ">=":
                    if item_val_num is not None and value_num is not None and item_val_num >= value_num:
                        indices.append(i)
                elif op == "==":
                    if str(item_val) == value:
                        indices.append(i)
                elif op == "!=":
                    if str(item_val) != value:
                        indices.append(i)

        return indices


def jsonpath_query(
    data: Dict[str, Any],
    path: str
) -> List[Any]:
    """
    Query a dictionary using JSONPath.

    Args:
        data: Dictionary to query.
        path: JSONPath expression.

    Returns:
        List of matching values.
    """
    return JsonPathQuery(data).query(path)


def find_all(
    data: Dict[str, Any],
    path: str
) -> List[Any]:
    """
    Find all values matching a JSONPath expression.

    Args:
        data: Dictionary to query.
        path: JSONPath expression.

    Returns:
        List of all matching values.
    """
    return JsonPathQuery(data).query(path)


def find_first(
    data: Dict[str, Any],
    path: str,
    default: Any = None
) -> Any:
    """
    Find the first value matching a JSONPath expression.

    Args:
        data: Dictionary to query.
        path: JSONPath expression.
        default: Default value if no match.

    Returns:
        First matching value or default.
    """
    results = JsonPathQuery(data).query(path)
    return results[0] if results else default


def find_paths(
    data: Dict[str, Any],
    path: str
) -> List[str]:
    """
    Find all paths matching a JSONPath expression.

    Args:
        data: Dictionary to query.
        path: JSONPath expression.

    Returns:
        List of matching paths.
    """
    nodes = JsonPathQuery(data).query_with_paths(path)
    return [node.path for node in nodes]
