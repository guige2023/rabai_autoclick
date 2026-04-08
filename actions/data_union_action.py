"""Data Union Action.

Unions multiple data sources into a single stream.
"""
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class DataSource:
    name: str
    data: List[Any]
    source_type: str = "list"
    metadata: Dict[str, Any]


class DataUnionAction:
    """Unions multiple data sources."""

    def __init__(self, dedupe: bool = False) -> None:
        self.sources: Dict[str, DataSource] = {}
        self.dedupe = dedupe
        self._seen: Optional[set] = None
        if dedupe:
            self._seen = set()

    def add_source(self, name: str, data: List[Any], source_type: str = "list", **metadata) -> None:
        self.sources[name] = DataSource(name=name, data=data, source_type=source_type, metadata=metadata)

    def union_all(self) -> List[Any]:
        if self.dedupe:
            seen = set()
            result = []
            for source in self.sources.values():
                for item in source.data:
                    key = str(item) if not isinstance(item, (str, int, float)) else item
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
            return result
        result = []
        for source in self.sources.values():
            result.extend(source.data)
        return result

    def union_selective(self, source_names: List[str]) -> List[Any]:
        result = []
        for name in source_names:
            source = self.sources.get(name)
            if source:
                result.extend(source.data)
        return result

    def get_stats(self) -> Dict[str, Any]:
        return {
            "source_count": len(self.sources),
            "total_items": sum(len(s.data) for s in self.sources.values()),
            "sources": {name: len(s.data) for name, s in self.sources.items()},
        }
