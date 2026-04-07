"""JSON data extractor action for structured data parsing.

This module provides JSON parsing and extraction with support
for JSONPath queries, schema validation, and nested data navigation.

Example:
    >>> action = JSONExtractorAction()
    >>> result = action.execute(data='{"users": [{"name": "John"}]}', path="$.users[*].name")
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ExtractionResult:
    """Result from JSON extraction."""
    success: bool
    value: Any = None
    matches: list[Any] = field(default_factory=list)
    count: int = 0


class JSONExtractorAction:
    """JSON data extraction action with JSONPath support.

    Provides structured JSON extraction with dot notation,
    bracket notation, and wildcard support.

    Example:
        >>> action = JSONExtractorAction()
        >>> result = action.execute(
        ...     data={"users": [{"name": "Alice"}, {"name": "Bob"}]},
        ...     path="users[*].name"
        ... )
    """

    def __init__(self) -> None:
        """Initialize JSON extractor."""
        self._last_data: Optional[dict] = None

    def execute(
        self,
        data: Any,
        path: Optional[str] = None,
        query: Optional[str] = None,
        filter_func: Optional[Callable[[dict], bool]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute JSON extraction.

        Args:
            data: JSON data (string, dict, or list).
            path: Dot-notation path (e.g., "users.0.name").
            query: JSONPath-style query.
            filter_func: Optional filter function.
            **kwargs: Additional parameters.

        Returns:
            Extraction result dictionary.

        Raises:
            ValueError: If data is invalid JSON.
        """
        result: dict[str, Any] = {"success": True}

        # Parse data if string
        if isinstance(data, str):
            try:
                self._last_data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {e}") from e
        else:
            self._last_data = data

        # Execute path extraction
        if path:
            value = self._extract_path(self._last_data, path)
            result["value"] = value
            result["count"] = 1 if value is not None else 0
        elif query:
            matches = self._extract_query(self._last_data, query)
            result["matches"] = matches
            result["count"] = len(matches)
            result["value"] = matches[0] if matches else None
        elif filter_func:
            matches = self._filter_data(self._last_data, filter_func)
            result["matches"] = matches
            result["count"] = len(matches)
        else:
            result["value"] = self._last_data

        return result

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract value using dot notation path.

        Args:
            data: Data to extract from.
            path: Dot-notation path.

        Returns:
            Extracted value or None.
        """
        current = data
        parts = path.split(".")

        for part in parts:
            if current is None:
                return None

            # Handle array index
            if part.isdigit():
                index = int(part)
                if isinstance(current, (list, tuple)) and index < len(current):
                    current = current[index]
                else:
                    return None
            # Handle bracket notation
            elif "[" in part and part.endswith("]"):
                base, bracket = part.split("[", 1)
                idx = int(bracket.rstrip("]"))
                if base and base != "$":
                    if isinstance(current, dict):
                        current = current.get(base, [])
                if isinstance(current, (list, tuple)) and idx < len(current):
                    current = current[idx]
                else:
                    return None
            # Handle dictionary key
            elif isinstance(current, dict):
                current = current.get(part)
            # Handle list with wildcard
            elif isinstance(current, (list, tuple)) and part == "*":
                return current
            else:
                return None

        return current

    def _extract_query(self, data: Any, query: str) -> list[Any]:
        """Extract using JSONPath-like query.

        Args:
            data: Data to query.
            query: JSONPath query string.

        Returns:
            List of matched values.
        """
        matches: list[Any] = []

        # Support common patterns
        if query.startswith("$.):
            query = query[2:]

        # Root array iteration
        if query.startswith("*."):
            field_path = query[2:]
            if isinstance(data, list):
                for item in data:
                    val = self._extract_path(item, field_path)
                    if val is not None:
                        if isinstance(val, list):
                            matches.extend(val)
                        else:
                            matches.append(val)
            elif isinstance(data, dict):
                val = self._extract_path(data, field_path)
                if val is not None:
                    if isinstance(val, list):
                        matches.extend(val)
                    else:
                        matches.append(val)

        # Filter by condition
        elif "[?(" in query:
            match = re.match(r"(.+?)\[?\?\((\w+)([<>=!]+)(.+)\)", query)
            if match:
                base_path, field_name, op, value = match.groups()
                base = self._extract_path(data, base_path) if base_path else data
                if isinstance(base, list):
                    for item in base:
                        field_val = item.get(field_name) if isinstance(item, dict) else getattr(item, field_name, None)
                        if self._evaluate_condition(field_val, op, value.strip("'\"")):
                            matches.append(item)

        return matches

    def _evaluate_condition(self, left: Any, op: str, right: str) -> bool:
        """Evaluate a condition.

        Args:
            left: Left operand.
            op: Operator.
            right: Right operand.

        Returns:
            Boolean result.
        """
        try:
            right_val = int(right) if right.isdigit() else right.strip("'\"")
        except ValueError:
            right_val = right.strip("'\"")

        if op == "==":
            return left == right_val
        elif op == "!=":
            return left != right_val
        elif op == ">":
            return left > right_val
        elif op == "<":
            return left < right_val
        elif op == ">=":
            return left >= right_val
        elif op == "<=":
            return left <= right_val
        return False

    def _filter_data(
        self,
        data: Any,
        filter_func: Callable[[dict], bool],
    ) -> list[Any]:
        """Filter data with function.

        Args:
            data: Data to filter.
            filter_func: Filter function.

        Returns:
            Filtered list.
        """
        if isinstance(data, list):
            return [item for item in data if filter_func(item)]
        return []

    def flatten(
        self,
        data: Any,
        prefix: str = "",
        separator: str = ".",
    ) -> dict[str, Any]:
        """Flatten nested JSON structure.

        Args:
            data: JSON data to flatten.
            prefix: Key prefix.
            separator: Key separator.

        Returns:
            Flattened dictionary.
        """
        result: dict[str, Any] = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    result.update(self.flatten(value, new_key, separator))
                else:
                    result[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}[{i}]"
                if isinstance(item, (dict, list)):
                    result.update(self.flatten(item, new_key, separator))
                else:
                    result[new_key] = item
        else:
            result[prefix] = data

        return result

    def unflatten(self, data: dict[str, Any]) -> Any:
        """Unflatten dictionary to nested structure.

        Args:
            data: Flattened dictionary.

        Returns:
            Nested data structure.
        """
        result: dict[str, Any] = {}

        for key, value in data.items():
            parts = re.split(r"[.\[\]]+", key)
            parts = [p for p in parts if p]

            current = result
            for i, part in enumerate(parts[:-1]):
                if part.isdigit():
                    idx = int(part)
                    if isinstance(current, list):
                        while len(current) <= idx:
                            current.append({})
                        current = current[idx]
                    else:
                        if part not in current:
                            current[part] = []
                        current = current[part]
                else:
                    if part not in current:
                        current[part] = {}
                    current = current[part]

            last_part = parts[-1]
            if last_part.isdigit():
                idx = int(last_part)
                if isinstance(current, list):
                    while len(current) <= idx:
                        current.append(None)
                    current[idx] = value
                else:
                    if last_part not in current:
                        current[last_part] = []
                    current[last_part].append(value) if isinstance(current[last_part], list) else None
            else:
                current[last_part] = value

        return result

    def validate_schema(
        self,
        data: Any,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate JSON against schema.

        Args:
            data: Data to validate.
            schema: JSON schema definition.

        Returns:
            Validation result.
        """
        errors: list[str] = []

        def validate(obj: Any, sch: dict[str, Any], path: str = "") -> None:
            if "type" in sch:
                expected_type = sch["type"]
                if expected_type == "object" and not isinstance(obj, dict):
                    errors.append(f"{path}: expected object, got {type(obj).__name__}")
                elif expected_type == "array" and not isinstance(obj, list):
                    errors.append(f"{path}: expected array, got {type(obj).__name__}")
                elif expected_type == "string" and not isinstance(obj, str):
                    errors.append(f"{path}: expected string, got {type(obj).__name__}")
                elif expected_type == "number" and not isinstance(obj, (int, float)):
                    errors.append(f"{path}: expected number, got {type(obj).__name__}")
                elif expected_type == "boolean" and not isinstance(obj, bool):
                    errors.append(f"{path}: expected boolean, got {type(obj).__name__}")

            if "required" in sch and isinstance(obj, dict):
                for required_field in sch["required"]:
                    if required_field not in obj:
                        errors.append(f"{path}: missing required field '{required_field}'")

            if "properties" in sch and isinstance(obj, dict):
                for prop_name, prop_schema in sch["properties"].items():
                    if prop_name in obj:
                        validate(obj[prop_name], prop_schema, f"{path}.{prop_name}")

            if "items" in sch and isinstance(obj, list):
                for i, item in enumerate(obj):
                    validate(item, sch["items"], f"{path}[{i}]")

        validate(data, schema)

        return {
            "valid": len(errors) == 0,
            "errors": errors,
        }
