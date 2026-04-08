"""
Data Deduplication Action - Removes duplicate records.

This module provides deduplication capabilities using
various strategies including exact match and fuzzy matching.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class DeduplicationConfig:
    """Configuration for deduplication."""
    key_fields: list[str] | None = None
    use_hash: bool = False
    fuzzy_threshold: float = 0.8


@dataclass
class DeduplicationStats:
    """Statistics for deduplication."""
    original_count: int = 0
    unique_count: int = 0
    duplicates_removed: int = 0


class DataDeduplicator:
    """Deduplicates data records."""
    
    def __init__(self, config: DeduplicationConfig | None = None) -> None:
        self.config = config or DeduplicationConfig()
    
    def deduplicate(
        self,
        data: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], DeduplicationStats]:
        """Remove duplicate records."""
        seen = set()
        unique = []
        duplicates_removed = 0
        
        for record in data:
            key = self._get_key(record)
            if key not in seen:
                seen.add(key)
                unique.append(record)
            else:
                duplicates_removed += 1
        
        stats = DeduplicationStats(
            original_count=len(data),
            unique_count=len(unique),
            duplicates_removed=duplicates_removed,
        )
        
        return unique, stats
    
    def _get_key(self, record: dict[str, Any]) -> str:
        """Generate key for record."""
        if self.config.use_hash:
            content = json.dumps(record, sort_keys=True, default=str)
            return hashlib.sha256(content.encode()).hexdigest()
        elif self.config.key_fields:
            values = [str(record.get(f, "")) for f in self.config.key_fields]
            return "|".join(values)
        else:
            content = json.dumps(record, sort_keys=True, default=str)
            return hashlib.sha256(content.encode()).hexdigest()


class DataDeduplicationAction:
    """Data deduplication action for automation workflows."""
    
    def __init__(self, key_fields: list[str] | None = None) -> None:
        config = DeduplicationConfig(key_fields=key_fields)
        self.deduplicator = DataDeduplicator(config)
    
    async def deduplicate(self, data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], DeduplicationStats]:
        """Remove duplicate records."""
        return self.deduplicator.deduplicate(data)


__all__ = ["DeduplicationConfig", "DeduplicationStats", "DataDeduplicator", "DataDeduplicationAction"]
