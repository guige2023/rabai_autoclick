# Copyright (c) 2024. coded by claude
"""Data Builder Action Module.

Builds complex API request payloads with fluent interface
for nested structures, arrays, and conditional fields.
"""
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class DataBuilder:
    def __init__(self):
        self._data: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> "DataBuilder":
        self._data[key] = value
        return self

    def set_nested(self, path: str, value: Any) -> "DataBuilder":
        parts = path.split(".")
        current = self._data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
        return self

    def append(self, key: str, value: Any) -> "DataBuilder":
        if key not in self._data:
            self._data[key] = []
        if not isinstance(self._data[key], list):
            self._data[key] = [self._data[key]]
        self._data[key].append(value)
        return self

    def remove(self, key: str) -> "DataBuilder":
        if key in self._data:
            del self._data[key]
        return self

    def build(self) -> Dict[str, Any]:
        return self._data.copy()

    def clear(self) -> "DataBuilder":
        self._data.clear()
        return self

    def merge(self, other: Dict[str, Any]) -> "DataBuilder":
        self._deep_merge(self._data, other)
        return self

    def _deep_merge(self, target: Dict, source: Dict) -> None:
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    def to_json(self) -> str:
        import json
        return json.dumps(self._data)

    def __repr__(self) -> str:
        return f"DataBuilder({self._data})"
