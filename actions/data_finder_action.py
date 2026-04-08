# Copyright (c) 2024. coded by claude
"""Data Finder Action Module.

Finds data within API responses using flexible query expressions
with support for nested paths, array indexing, and filtering.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    found: bool
    value: Any
    path: str


class DataFinder:
    def __init__(self):
        self._cache: Dict[str, Any] = {}

    def find(self, data: Any, path: str) -> QueryResult:
        if not path:
            return QueryResult(found=True, value=data, path="")
        parts = self._parse_path(path)
        current = data
        current_path = ""
        for part in parts:
            if current is None:
                return QueryResult(found=False, value=None, path=current_path)
            current_path = f"{current_path}.{part}" if current_path else part
            if isinstance(current, dict):
                if part not in current:
                    return QueryResult(found=False, value=None, path=current_path)
                current = current[part]
            elif isinstance(current, list):
                try:
                    index = int(part)
                    if index < 0 or index >= len(current):
                        return QueryResult(found=False, value=None, path=current_path)
                    current = current[index]
                except ValueError:
                    return QueryResult(found=False, value=None, path=current_path)
            else:
                return QueryResult(found=False, value=None, path=current_path)
        return QueryResult(found=True, value=current, path=current_path)

    def find_all(self, data: Any, path: str) -> List[Any]:
        results = []
        self._find_allRecursive(data, self._parse_path(path), 0, results)
        return results

    def _find_allRecursive(self, data: Any, parts: List[str], index: int, results: List) -> None:
        if index >= len(parts):
            results.append(data)
            return
        part = parts[index]
        if data is None:
            return
        if isinstance(data, dict):
            if part in data:
                self._find_allRecursive(data[part], parts, index + 1, results)
        elif isinstance(data, list):
            try:
                idx = int(part)
                if 0 <= idx < len(data):
                    self._find_allRecursive(data[idx], parts, index + 1, results)
            except ValueError:
                for item in data:
                    self._find_allRecursive(item, parts, index, results)

    def _parse_path(self, path: str) -> List[str]:
        return [p.strip() for p in path.split(".") if p.strip()]

    def exists(self, data: Any, path: str) -> bool:
        return self.find(data, path).found

    def get_or_default(self, data: Any, path: str, default: Any) -> Any:
        result = self.find(data, path)
        return result.value if result.found else default
