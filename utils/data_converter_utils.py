"""
Data Converter Utilities

Provides utilities for converting between different
data formats in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import json
import base64
import urllib.parse


class DataConverter:
    """
    Converts data between various formats.
    
    Supports JSON, base64, URL encoding, and
    other common data transformations.
    """

    @staticmethod
    def to_json(data: Any, pretty: bool = False) -> str:
        """Convert data to JSON string."""
        if pretty:
            return json.dumps(data, indent=2, ensure_ascii=False)
        return json.dumps(data, ensure_ascii=False)

    @staticmethod
    def from_json(json_str: str) -> Any:
        """Parse JSON string to data."""
        return json.loads(json_str)

    @staticmethod
    def to_base64(data: str | bytes) -> str:
        """Encode data to base64."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return base64.b64encode(data).decode("ascii")

    @staticmethod
    def from_base64(encoded: str) -> bytes:
        """Decode base64 string to bytes."""
        return base64.b64decode(encoded)

    @staticmethod
    def to_url_params(data: dict[str, Any]) -> str:
        """Convert dict to URL query parameters."""
        return urllib.parse.urlencode(data, safe="")

    @staticmethod
    def from_url_params(query: str) -> dict[str, str]:
        """Parse URL query string to dict."""
        return dict(urllib.parse.parse_qsl(query))

    @staticmethod
    def flatten_dict(
        data: dict[str, Any],
        parent_key: str = "",
        sep: str = ".",
    ) -> dict[str, Any]:
        """Flatten nested dictionary."""
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(DataConverter.flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def unflatten_dict(
        data: dict[str, Any],
        sep: str = ".",
    ) -> dict[str, Any]:
        """Unflatten dictionary to nested structure."""
        result = {}
        for key, value in data.items():
            parts = key.split(sep)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result

    @staticmethod
    def bytes_to_human(size_bytes: int) -> str:
        """Convert bytes to human readable string."""
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_idx = 0
        while size >= 1024 and unit_idx < len(units) - 1:
            size /= 1024
            unit_idx += 1
        return f"{size:.2f} {units[unit_idx]}"

    @staticmethod
    def human_to_bytes(size_str: str) -> int:
        """Convert human readable size to bytes."""
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        size_str = size_str.strip().upper()
        for unit, multiplier in units.items():
            if size_str.endswith(unit):
                num = float(size_str[: -len(unit)].strip())
                return int(num * multiplier)
        return int(size_str)
