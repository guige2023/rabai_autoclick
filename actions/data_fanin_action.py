"""Data fanin action module for RabAI AutoClick.

Provides fanin operations for collecting data from multiple sources:
- DataFaninCollector: Collect and merge data from multiple sources
- MultiSourceAggregator: Aggregate data from multiple sources
- DataMergeHandler: Handle merging of data from multiple inputs
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MergeStrategy(Enum):
    """Data merge strategies."""
    FIRST = "first"
    LAST = "last"
    CONCAT = "concat"
    MERGE = "merge"
    UNION = "union"
    INTERSECTION = "intersection"


class ConflictResolution(Enum):
    """Conflict resolution strategies."""
    SKIP = "skip"
    OVERWRITE = "overwrite"
    MERGE_DEEP = "merge_deep"
    PREFER_SOURCE = "prefer_source"
    CUSTOM = "custom"


@dataclass
class DataSource:
    """Data source definition."""
    name: str
    fetcher: Callable
    transform: Optional[Callable] = None
    priority: int = 0
    enabled: bool = True
    timeout: Optional[float] = None


@dataclass
class DataFaninConfig:
    """Configuration for data fanin."""
    merge_strategy: MergeStrategy = MergeStrategy.MERGE
    conflict_resolution: ConflictResolution = ConflictResolution.OVERWRITE
    timeout: float = 30.0
    require_all_sources: bool = False
    partial_results_ok: bool = True
    min_sources: int = 1
    deduplicate: bool = True
    max_results: Optional[int] = None


class DataFaninCollector:
    """Collect data from multiple sources (fanin)."""
    
    def __init__(self, name: str, config: Optional[DataFaninConfig] = None):
        self.name = name
        self.config = config or DataFaninConfig()
        self._sources: Dict[str, DataSource] = {}
        self._buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = threading.RLock()
        self._stats = {"total_collections": 0, "successful_collections": 0, "partial_collections": 0, "failed_collections": 0}
    
    def register_source(self, source: DataSource):
        """Register a data source."""
        with self._lock:
            self._sources[source.name] = source
    
    def unregister_source(self, name: str):
        """Unregister a source."""
        with self._lock:
            self._sources.pop(name, None)
    
    def _fetch_from_source(self, source: DataSource) -> Tuple[bool, Any, Optional[str]]:
        """Fetch data from a single source."""
        try:
            if source.timeout:
                result = [None]
                error = [None]
                
                def worker():
                    try:
                        result[0] = source.fetcher()
                    except Exception as e:
                        error[0] = e
                
                t = threading.Thread(target=worker)
                t.daemon = True
                t.start()
                t.join(timeout=source.timeout)
                
                if t.is_alive():
                    return False, None, f"Source {source.name} timed out"
                if error[0]:
                    return False, None, str(error[0])
                data = result[0]
            else:
                data = source.fetcher()
            
            if source.transform:
                data = source.transform(data)
            
            return True, data, None
            
        except Exception as e:
            return False, None, str(e)
    
    def collect_from_all(self) -> Tuple[bool, Dict[str, Any], List[str]]:
        """Collect data from all registered sources."""
        with self._lock:
            self._stats["total_collections"] += 1
            enabled_sources = [(name, s) for name, s in self._sources.items() if s.enabled]
        
        if not enabled_sources:
            return False, {}, ["No sources registered"]
        
        results = {}
        errors = []
        success_count = 0
        
        threads = []
        
        def fetch_source(name: str, source: DataSource):
            success, data, error = self._fetch_from_source(source)
            results[name] = data
            if success:
                nonlocal success_count
                success_count += 1
            else:
                errors.append(error or f"Unknown error from {name}")
        
        for name, source in enabled_sources:
            t = threading.Thread(target=fetch_source, args=(name, source))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join(timeout=self.config.timeout)
        
        with self._lock:
            if success_count == len(enabled_sources):
                self._stats["successful_collections"] += 1
                return True, results, []
            elif success_count >= self.config.min_sources:
                self._stats["partial_collections"] += 1
                return self.config.partial_results_ok, results, errors
            else:
                self._stats["failed_collections"] += 1
                return False, {}, errors
    
    def merge_results(self, results: Dict[str, Any]) -> Any:
        """Merge results from multiple sources."""
        if not results:
            return None
        
        active_results = {k: v for k, v in results.items() if v is not None}
        
        if not active_results:
            return None
        
        if self.config.merge_strategy == MergeStrategy.FIRST:
            sorted_sources = sorted(active_results.keys(), key=lambda n: self._sources[n].priority if n in self._sources else 0)
            return active_results[sorted_sources[0]]
        
        if self.config.merge_strategy == MergeStrategy.LAST:
            sorted_sources = sorted(active_results.keys(), key=lambda n: self._sources[n].priority if n in self._sources else 0, reverse=True)
            return active_results[sorted_sources[0]]
        
        if self.config.merge_strategy == MergeStrategy.CONCAT:
            concat_result = []
            for key in sorted(active_results.keys()):
                val = active_results[key]
                if isinstance(val, list):
                    concat_result.extend(val)
                else:
                    concat_result.append(val)
            if self.config.max_results:
                concat_result = concat_result[:self.config.max_results]
            return concat_result
        
        if self.config.merge_strategy == MergeStrategy.UNION:
            if self.config.deduplicate:
                seen = set()
                union_result = []
                for key in sorted(active_results.keys()):
                    val = active_results[key]
                    if isinstance(val, (list, set)):
                        for item in val:
                            item_key = hash(str(item))
                            if item_key not in seen:
                                seen.add(item_key)
                                union_result.append(item)
                    else:
                        item_key = hash(str(val))
                        if item_key not in seen:
                            seen.add(item_key)
                            union_result.append(val)
                return union_result
            else:
                union_result = []
                for val in active_results.values():
                    if isinstance(val, list):
                        union_result.extend(val)
                    else:
                        union_result.append(val)
                return union_result
        
        if self.config.merge_strategy == MergeStrategy.MERGE:
            merged = {}
            for key in sorted(active_results.keys()):
                val = active_results[key]
                if isinstance(val, dict):
                    for k, v in val.items():
                        if self.config.conflict_resolution == ConflictResolution.OVERWRITE:
                            merged[k] = v
                        elif self.config.conflict_resolution == ConflictResolution.SKIP:
                            if k not in merged:
                                merged[k] = v
                        elif self.config.conflict_resolution == ConflictResolution.MERGE_DEEP:
                            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                                merged[k] = {**merged[k], **v}
                            else:
                                merged[k] = v
                else:
                    merged[key] = val
            return merged
        
        return list(active_results.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fanin statistics."""
        with self._lock:
            return {
                "name": self.name,
                "source_count": len(self._sources),
                **{k: v for k, v in self._stats.items()},
            }


class DataFaninAction(BaseAction):
    """Data fanin action."""
    action_type = "data_fanin"
    display_name = "数据收集"
    description = "多数据源收集与合并"
    
    def __init__(self):
        super().__init__()
        self._collectors: Dict[str, DataFaninCollector] = {}
        self._lock = threading.Lock()
    
    def _get_collector(self, name: str, config: Optional[DataFaninConfig] = None) -> DataFaninCollector:
        """Get or create collector."""
        with self._lock:
            if name not in self._collectors:
                self._collectors[name] = DataFaninCollector(name, config)
            return self._collectors[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute fanin operation."""
        try:
            collector_name = params.get("collector", "default")
            command = params.get("command", "collect")
            
            config = DataFaninConfig(
                merge_strategy=MergeStrategy[params.get("merge_strategy", "merge").upper()],
                conflict_resolution=ConflictResolution[params.get("conflict_resolution", "overwrite").upper()],
                timeout=params.get("timeout", 30.0),
                require_all_sources=params.get("require_all", False),
                min_sources=params.get("min_sources", 1),
            )
            
            collector = self._get_collector(collector_name, config)
            
            if command == "register":
                source = DataSource(
                    name=params.get("source_name"),
                    fetcher=params.get("fetcher"),
                    transform=params.get("transform"),
                    priority=params.get("priority", 0),
                    timeout=params.get("source_timeout"),
                )
                collector.register_source(source)
                return ActionResult(success=True, message=f"Source {source.name} registered")
            
            elif command == "collect":
                success, results, errors = collector.collect_from_all()
                merged = collector.merge_results(results)
                return ActionResult(
                    success=success,
                    message=f"Collected from {len([r for r in results.values() if r is not None])} sources",
                    data={"results": results, "merged": merged, "errors": errors}
                )
            
            elif command == "merge":
                results = params.get("results", {})
                merged = collector.merge_results(results)
                return ActionResult(success=True, data={"merged": merged})
            
            elif command == "stats":
                stats = collector.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataFaninAction error: {str(e)}")
