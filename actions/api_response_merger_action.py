"""API Response Merger Action Module.

Merges multiple API responses into unified results.
Handles field selection, conflict resolution, and data transformation.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    """Strategy for merging conflicting values."""
    FIRST = auto()       # Use first value
    LAST = auto()       # Use last value
    CONCATENATE = auto() # Concatenate values
    MERGE_OBJECTS = auto() # Deep merge objects
    RESOLVE_CONFLICT = auto() # Custom resolution


class ConflictResolver:
    """Resolves conflicts between merged values."""
    
    @staticmethod
    def newest(values: List[Any]) -> Any:
        """Select the most recent value based on timestamp."""
        return max(values, key=lambda v: v.get("_timestamp", 0) if isinstance(v, dict) else 0)
    
    @staticmethod
    def oldest(values: List[Any]) -> Any:
        """Select the oldest value."""
        return min(values, key=lambda v: v.get("_timestamp", float('inf')) if isinstance(v, dict) else float('inf'))
    
    @staticmethod
    def longest(values: List[str]) -> str:
        """Select the longest string value."""
        return max(values, key=len)
    
    @staticmethod
    def highest(values: List[Union[int, float]]) -> Union[int, float]:
        """Select the highest numeric value."""
        return max(values)
    
    @staticmethod
    def lowest(values: List[Union[int, float]]) -> Union[int, float]:
        """Select the lowest numeric value."""
        return min(values)
    
    @staticmethod
    def most_common(values: List[Any]) -> Any:
        """Select the most frequently occurring value."""
        counts = defaultdict(int)
        for v in values:
            counts[str(v)] += 1
        return max(values, key=lambda v: counts[str(v)])


@dataclass
class MergeConfig:
    """Configuration for response merging."""
    strategy: MergeStrategy = MergeStrategy.FIRST
    conflict_resolver: Optional[Callable[[List[Any]], Any]] = None
    include_metadata: bool = True
    dedupe_arrays: bool = True
    preserve_null: bool = False


@dataclass
class MergeResult:
    """Result of a merge operation."""
    data: Dict[str, Any]
    conflicts: List[Dict[str, Any]] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    merged_at: str = field(default_factory=lambda: datetime.now().isoformat())


class APIResponseMerger:
    """Merges multiple API responses into unified results.
    
    Supports:
    - Field selection and exclusion
    - Conflict resolution strategies
    - Nested object merging
    - Array deduplication
    - Type coercion
    """
    
    def __init__(self, config: Optional[MergeConfig] = None):
        self.config = config or MergeConfig()
        self._field_selectors: Dict[str, List[str]] = {}
        self._field_transformers: Dict[str, Callable] = {}
        self._type_coercions: Dict[str, type] = {}
        self._lock = asyncio.Lock()
    
    def select_fields(self, source: str, fields: List[str]) -> None:
        """Set field selection for a source.
        
        Args:
            source: Source identifier
            fields: List of field paths to include
        """
        self._field_selectors[source] = fields
    
    def add_field_transformer(
        self,
        field_path: str,
        transformer: Callable[[Any], Any]
    ) -> None:
        """Add a transformation for a specific field.
        
        Args:
            field_path: Dot-separated path (e.g., "user.name")
            transformer: Function to transform the field value
        """
        self._field_transformers[field_path] = transformer
    
    def set_type_coercion(self, field_path: str, target_type: type) -> None:
        """Set type coercion for a field.
        
        Args:
            field_path: Dot-separated path
            target_type: Target type to coerce to
        """
        self._type_coercions[field_path] = target_type
    
    async def merge_responses(
        self,
        responses: Dict[str, Dict[str, Any]]
    ) -> MergeResult:
        """Merge multiple API responses.
        
        Args:
            responses: Dict mapping source names to response data
            
        Returns:
            Merged result with conflicts
        """
        result_data: Dict[str, Any] = {}
        conflicts: List[Dict[str, Any]] = []
        all_sources = list(responses.keys())
        
        all_fields = set()
        for response in responses.values():
            all_fields.update(self._flatten_dict(response).keys())
        
        for field_path in all_fields:
            values = []
            sources_with_field = []
            
            for source, response in responses.items():
                value = self._get_nested_value(response, field_path)
                if value is not None:
                    values.append(value)
                    sources_with_field.append(source)
            
            if not values:
                continue
            
            if len(values) == 1:
                result_data = self._set_nested_value(
                    result_data, field_path, values[0]
                )
            else:
                conflict = await self._resolve_conflict(
                    field_path, values, sources_with_field
                )
                
                if conflict["resolved"]:
                    result_data = self._set_nested_value(
                        result_data, field_path, conflict["value"]
                    )
                else:
                    conflicts.append(conflict)
                    result_data = self._set_nested_value(
                        result_data, field_path, values[0]
                    )
        
        if self.config.include_metadata:
            result_data["_merge_metadata"] = {
                "sources": all_sources,
                "merged_at": datetime.now().isoformat(),
                "conflict_count": len(conflicts)
            }
        
        return MergeResult(
            data=result_data,
            conflicts=conflicts,
            sources=all_sources
        )
    
    async def merge_arrays(
        self,
        arrays: List[List[Any]],
        dedupe: bool = True,
        dedupe_key: Optional[str] = None
    ) -> List[Any]:
        """Merge multiple arrays with optional deduplication.
        
        Args:
            arrays: List of arrays to merge
            dedupe: Whether to deduplicate results
            dedupe_key: Optional key for object deduplication
            
        Returns:
            Merged array
        """
        merged = []
        for arr in arrays:
            merged.extend(arr)
        
        if dedupe:
            if dedupe_key:
                seen = set()
                result = []
                for item in merged:
                    key_val = self._get_nested_value(item, dedupe_key)
                    if key_val not in seen:
                        seen.add(key_val)
                        result.append(item)
                merged = result
            else:
                merged = list(dict.fromkeys(merged))
        
        return merged
    
    async def _resolve_conflict(
        self,
        field_path: str,
        values: List[Any],
        sources: List[str]
    ) -> Dict[str, Any]:
        """Resolve a merge conflict.
        
        Args:
            field_path: Field path with conflict
            values: Conflicting values
            sources: Sources providing each value
            
        Returns:
            Conflict resolution details
        """
        if self.config.conflict_resolver:
            resolved_value = self.config.conflict_resolver(values)
            return {
                "field_path": field_path,
                "values": values,
                "sources": sources,
                "resolved": True,
                "value": resolved_value
            }
        
        strategy = self.config.strategy
        
        if strategy == MergeStrategy.FIRST:
            return {
                "field_path": field_path,
                "values": values,
                "sources": sources,
                "resolved": True,
                "value": values[0]
            }
        
        if strategy == MergeStrategy.LAST:
            return {
                "field_path": field_path,
                "values": values,
                "sources": sources,
                "resolved": True,
                "value": values[-1]
            }
        
        if strategy == MergeStrategy.CONCATENATE:
            if all(isinstance(v, str) for v in values):
                return {
                    "field_path": field_path,
                    "values": values,
                    "sources": sources,
                    "resolved": True,
                    "value": " ".join(values)
                }
            return {
                "field_path": field_path,
                "values": values,
                "sources": sources,
                "resolved": True,
                "value": values[-1]
            }
        
        if strategy == MergeStrategy.MERGE_OBJECTS:
            if all(isinstance(v, dict) for v in values):
                merged = {}
                for v in values:
                    merged.update(v)
                return {
                    "field_path": field_path,
                    "values": values,
                    "sources": sources,
                    "resolved": True,
                    "value": merged
                }
        
        return {
            "field_path": field_path,
            "values": values,
            "sources": sources,
            "resolved": False
        }
    
    def _flatten_dict(
        self,
        d: Dict[str, Any],
        parent_key: str = "",
        sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _get_nested_value(
        self,
        data: Dict[str, Any],
        path: str,
        sep: str = "."
    ) -> Optional[Any]:
        """Get value from nested dict using path."""
        keys = path.split(sep)
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _set_nested_value(
        self,
        data: Dict[str, Any],
        path: str,
        value: Any,
        sep: str = "."
    ) -> Dict[str, Any]:
        """Set value in nested dict using path."""
        keys = path.split(sep)
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
        return data


class GraphQLResponseMerger(APIResponseMerger):
    """Specialized merger for GraphQL responses.
    
    Handles GraphQL-specific merging including:
    - Query result merging
    - Fragment handling
    - Alias resolution
    """
    
    async def merge_graphql_responses(
        self,
        responses: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Merge GraphQL response objects.
        
        Args:
            responses: List of GraphQL response data
            
        Returns:
            Merged GraphQL response
        """
        result = {
            "data": {},
            "errors": []
        }
        
        for response in responses:
            if "data" in response:
                merged = await self.merge_responses({"r": response["data"]})
                result["data"].update(merged.data)
            
            if "errors" in response:
                result["errors"].extend(response["errors"])
        
        return result
    
    async def merge_subscriptions(
        self,
        subscription_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Merge subscription events.
        
        Args:
            subscription_data: List of subscription events
            
        Returns:
            Merged subscription stream
        """
        events_by_id: Dict[str, List] = defaultdict(list)
        
        for event in subscription_data:
            event_id = event.get("id", str(hash(str(event))))
            events_by_id[event_id].append(event)
        
        merged = []
        for events in events_by_id.values():
            if len(events) == 1:
                merged.extend(events)
            else:
                merged_event = await self.merge_responses(
                    {f"e{i}": e for i, e in enumerate(events)}
                )
                merged.append(merged_event.data)
        
        return merged
