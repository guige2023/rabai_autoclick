"""
Data Flatten Action Module.

Flattens nested data structures into flat dictionaries
with configurable separator and handling strategies.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import json
import re

logger = logging.getLogger(__name__)


class FlattenStrategy(Enum):
    """Flatten key notation strategy."""
    DOT = "dot"
    BRACKET = "bracket"
    UNDERSCORE = "underscore"


@dataclass
class FlattenConfig:
    """Flatten configuration options."""
    separator: str = "."
    max_depth: Optional[int] = None
    include_lists: bool = True
    list_index_format: str = "[{index}]"
    preserve_types: bool = False


class DataFlattenAction:
    """
    Flattens nested structures to flat key-value dicts.

    Supports dot, bracket, and underscore notation for keys.
    Handles lists, dicts, and mixed structures.

    Example:
        flattener = DataFlattenAction(separator=".")
        flat = flattener.flatten(nested_data)
        # {"user.profile.name": "Alice", "user.tags[0]": "dev"}
    """

    def __init__(
        self,
        separator: str = ".",
        max_depth: Optional[int] = None,
        include_lists: bool = True,
    ) -> None:
        self.separator = separator
        self.max_depth = max_depth
        self.include_lists = include_lists

    def flatten(
        self,
        data: Any,
        parent_key: str = "",
        depth: int = 0,
    ) -> dict[str, Any]:
        """Flatten a nested data structure."""
        if self.max_depth is not None and depth >= self.max_depth:
            return {parent_key: data} if parent_key else {"_value": data}

        items: dict[str, Any] = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{self.separator}{key}" if parent_key else key
                items.update(self.flatten(value, new_key, depth + 1))

        elif isinstance(data, (list, tuple)) and self.include_lists:
            for idx, item in enumerate(data):
                new_key = f"{parent_key}[{idx}]"
                items.update(self.flatten(item, new_key, depth + 1))

        else:
            if parent_key:
                items[parent_key] = data
            else:
                items["_value"] = data

        return items

    def unflatten(self, data: dict[str, Any]) -> Any:
        """Reverse a flatten operation to restore nested structure."""
        result: Any = {}
        list_pattern = __import__('re').compile(r'\[(\d+)\]$')

        for flat_key, value in data.items():
            self._set_nested(result, flat_key, value)

        return result

    def _set_nested(self, data: dict, path: str, value: Any) -> None:
        """Set value in nested dict from dot/bracket notation."""
        parts = re.split(r'[.\[\]]+', path)
        parts = [p for p in parts if p != ""]

        current = data
        for part in parts[:-1]:
            if part not in current:
                next_part = parts[parts.index(part) + 1]
                if next_part.isdigit():
                    current[part] = []
                else:
                    current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def flatten_to_string(self, data: Any) -> str:
        """Flatten and serialize to JSON string."""
        flat = self.flatten(data)
        return json.dumps(flat)

    @staticmethod
    def unflatten_from_string(data: str) -> dict[str, Any]:
        """Deserialize from JSON and unflatten."""
        parsed = json.loads(data)
        flattener = DataFlattenAction()
        return flattener.unflatten(parsed)
