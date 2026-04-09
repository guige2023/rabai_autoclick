"""Data Grouper Action Module.

Provides data grouping capabilities for organizing datasets
into groups based on keys, functions, or size.

Example:
    >>> from actions.data.data_grouper_action import DataGrouperAction
    >>> action = DataGrouperAction()
    >>> groups = action.group_by(data, key_func=lambda x: x["category"])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
import threading


class GroupingStrategy(Enum):
    """Grouping strategy types."""
    BY_KEY = "by_key"
    BY_SIZE = "by_size"
    BY_RANGE = "by_range"
    CUSTOM = "custom"


@dataclass
class Group:
    """Data group.
    
    Attributes:
        group_id: Unique group identifier
        key: Group key
        data: Grouped items
        size: Number of items
    """
    group_id: str
    key: Any
    data: List[Any]
    size: int


@dataclass
class GrouperConfig:
    """Configuration for grouping.
    
    Attributes:
        strategy: Grouping strategy
        group_size: Size of each group (for by_size)
        preserve_order: Preserve original ordering
        sort_groups: Sort groups by key
    """
    strategy: GroupingStrategy = GroupingStrategy.BY_KEY
    group_size: int = 100
    preserve_order: bool = True
    sort_groups: bool = True


@dataclass
class GroupingResult:
    """Result of grouping operation.
    
    Attributes:
        groups: List of groups
        num_groups: Number of groups
        original_size: Original dataset size
    """
    groups: List[Group]
    num_groups: int
    original_size: int
    duration: float = 0.0


class DataGrouperAction:
    """Data grouper for organizing datasets.
    
    Provides flexible grouping with multiple strategies
    and configurable group properties.
    
    Attributes:
        config: Grouper configuration
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[GrouperConfig] = None,
    ) -> None:
        """Initialize grouper action.
        
        Args:
            config: Grouper configuration
        """
        self.config = config or GrouperConfig()
        self._lock = threading.Lock()
    
    def group_by(
        self,
        data: List[Any],
        key_func: Callable[[Any], Any],
        strategy: Optional[GroupingStrategy] = None,
    ) -> GroupingResult:
        """Group data by key function.
        
        Args:
            data: Data to group
            key_func: Function to extract group key
            strategy: Override grouping strategy
        
        Returns:
            GroupingResult
        """
        import time
        start = time.time()
        
        strategy = strategy or self.config.strategy
        
        if strategy == GroupingStrategy.BY_KEY:
            return self._group_by_key(data, key_func, start)
        elif strategy == GroupingStrategy.BY_SIZE:
            return self._group_by_size(data, key_func, start)
        elif strategy == GroupingStrategy.BY_RANGE:
            return self._group_by_range(data, key_func, start)
        else:
            return self._group_by_key(data, key_func, start)
    
    def _group_by_key(
        self,
        data: List[Any],
        key_func: Callable[[Any], Any],
        start: float,
    ) -> GroupingResult:
        """Group by key function.
        
        Args:
            data: Data to group
            key_func: Key extraction function
            start: Start time
        
        Returns:
            GroupingResult
        """
        groups_dict: Dict[Any, List[Any]] = {}
        
        for item in data:
            key = key_func(item)
            if key not in groups_dict:
                groups_dict[key] = []
            groups_dict[key].append(item)
        
        groups: List[Group] = []
        group_id = 0
        
        keys = list(groups_dict.keys())
        if self.config.sort_groups:
            try:
                keys = sorted(keys)
            except TypeError:
                keys = sorted(keys, key=str)
        
        for key in keys:
            items = groups_dict[key]
            groups.append(Group(
                group_id=f"group_{group_id}",
                key=key,
                data=items,
                size=len(items),
            ))
            group_id += 1
        
        return GroupingResult(
            groups=groups,
            num_groups=len(groups),
            original_size=len(data),
            duration=time.time() - start,
        )
    
    def _group_by_size(
        self,
        data: List[Any],
        key_func: Callable[[Any], Any],
        start: float,
    ) -> GroupingResult:
        """Group by fixed size.
        
        Args:
            data: Data to group
            key_func: Key function (used for metadata)
            start: Start time
        
        Returns:
            GroupingResult
        """
        groups: List[Group] = []
        
        for i in range(0, len(data), self.config.group_size):
            batch = data[i:i + self.config.group_size]
            
            key_values = [key_func(item) for item in batch]
            representative_key = key_values[0] if key_values else None
            
            groups.append(Group(
                group_id=f"group_{len(groups)}",
                key=representative_key,
                data=batch,
                size=len(batch),
            ))
        
        return GroupingResult(
            groups=groups,
            num_groups=len(groups),
            original_size=len(data),
            duration=time.time() - start,
        )
    
    def _group_by_range(
        self,
        data: List[Any],
        key_func: Callable[[Any], Any],
        start: float,
    ) -> GroupingResult:
        """Group by numeric ranges.
        
        Args:
            data: Data to group
            key_func: Numeric key function
            start: Start time
        
        Returns:
            GroupingResult
        """
        numeric_data = [(item, key_func(item)) for item in data]
        
        values = [v for _, v in numeric_data]
        
        if not values:
            return GroupingResult(
                groups=[],
                num_groups=0,
                original_size=len(data),
                duration=time.time() - start,
            )
        
        min_val = min(values)
        max_val = max(values)
        value_range = max_val - min_val
        
        num_ranges = max(1, len(data) // self.config.group_size)
        range_size = value_range / num_ranges if value_range > 0 else 1.0
        
        groups_dict: Dict[int, List[Any]] = {i: [] for i in range(num_ranges)}
        
        for item, value in numeric_data:
            if range_size > 0:
                range_idx = min(int((value - min_val) / range_size), num_ranges - 1)
            else:
                range_idx = 0
            groups_dict[range_idx].append(item)
        
        groups: List[Group] = []
        range_start = min_val
        
        for i in range(num_ranges):
            items = groups_dict[i]
            if items:
                range_start_i = min_val + (i * range_size)
                range_end_i = range_start_i + range_size
                
                groups.append(Group(
                    group_id=f"group_{len(groups)}",
                    key=f"[{range_start_i:.2f}, {range_end_i:.2f})",
                    data=items,
                    size=len(items),
                ))
        
        return GroupingResult(
            groups=groups,
            num_groups=len(groups),
            original_size=len(data),
            duration=time.time() - start,
        )
    
    def group_by_buckets(
        self,
        data: List[Any],
        num_buckets: int,
        bucket_func: Optional[Callable[[Any], Any]] = None,
    ) -> GroupingResult:
        """Group data into fixed number of buckets.
        
        Args:
            data: Data to group
            num_buckets: Number of buckets
            bucket_func: Optional bucket assignment function
        
        Returns:
            GroupingResult
        """
        import time
        start = time.time()
        
        if bucket_func:
            key_func = bucket_func
        else:
            key_func = lambda x: hash(str(x)) % num_buckets
        
        groups_dict: Dict[int, List[Any]] = {i: [] for i in range(num_buckets)}
        
        for item in data:
            bucket = key_func(item) % num_buckets
            groups_dict[bucket].append(item)
        
        groups: List[Group] = []
        for i in range(num_buckets):
            items = groups_dict[i]
            if items:
                groups.append(Group(
                    group_id=f"group_{len(groups)}",
                    key=f"bucket_{i}",
                    data=items,
                    size=len(items),
                ))
        
        return GroupingResult(
            groups=groups,
            num_groups=len(groups),
            original_size=len(data),
            duration=time.time() - start,
        )
    
    def flatten_groups(
        self,
        groups: List[Group],
    ) -> List[Any]:
        """Flatten groups back to single list.
        
        Args:
            groups: Groups to flatten
        
        Returns:
            Flattened list
        """
        result: List[Any] = []
        
        for group in groups:
            result.extend(group.data)
        
        return result
    
    def filter_groups(
        self,
        groups: List[Group],
        filter_func: Callable[[Group], bool],
    ) -> List[Group]:
        """Filter groups by condition.
        
        Args:
            groups: Groups to filter
            filter_func: Filter function
        
        Returns:
            Filtered groups
        """
        return [g for g in groups if filter_func(g)]
    
    def merge_groups(
        self,
        groups: List[Group],
        group_ids: List[str],
    ) -> Group:
        """Merge multiple groups into one.
        
        Args:
            groups: All groups
            group_ids: IDs of groups to merge
        
        Returns:
            Merged group
        """
        to_merge = [g for g in groups if g.group_id in group_ids]
        
        all_data: List[Any] = []
        for g in to_merge:
            all_data.extend(g.data)
        
        return Group(
            group_id=f"merged_{'_'.join(group_ids)}",
            key="merged",
            data=all_data,
            size=len(all_data),
        )
