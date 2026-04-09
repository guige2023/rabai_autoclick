"""Data federation action module for RabAI AutoClick.

Provides data federation operations:
- FederatedQueryAction: Execute queries across multiple data sources
- DataSourceRegistryAction: Register and manage data sources
- FederatedJoinAction: Join data from multiple sources
- FederatedAggregateAction: Aggregate federated data
- FederatedCacheAction: Cache federated query results
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSource:
    """Represents a data source."""
    
    def __init__(self, source_id: str, name: str, source_type: str, connection_config: Dict):
        self.source_id = source_id
        self.name = name
        self.source_type = source_type
        self.connection_config = connection_config
        self.created_at = datetime.now()
        self.last_used: Optional[datetime] = None
        self.query_count = 0
    
    def to_dict(self) -> Dict:
        return {
            "source_id": self.source_id,
            "name": self.name,
            "source_type": self.source_type,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "query_count": self.query_count
        }


class FederatedQueryAction(BaseAction):
    """Execute queries across multiple data sources."""
    action_type = "federated_query"
    display_name = "联邦查询"
    description = "跨多个数据源执行查询"
    
    def __init__(self):
        super().__init__()
        self._data_sources: Dict[str, DataSource] = {}
        self._query_cache: Dict[str, Any] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "query")
            
            if operation == "query":
                return self._execute_federated_query(params)
            elif operation == "register":
                return self._register_data_source(params)
            elif operation == "list":
                return self._list_data_sources()
            elif operation == "unregister":
                return self._unregister_data_source(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _execute_federated_query(self, params: Dict[str, Any]) -> ActionResult:
        source_ids = params.get("source_ids", [])
        query = params.get("query")
        merge_strategy = params.get("merge_strategy", "union")
        use_cache = params.get("use_cache", True)
        
        if not source_ids:
            return ActionResult(success=False, message="source_ids is required")
        
        cache_key = f"{':'.join(sorted(source_ids))}:{query}"
        
        if use_cache and cache_key in self._query_cache:
            return ActionResult(
                success=True,
                message="Returning cached query result",
                data={
                    "cached": True,
                    "source_ids": source_ids,
                    "result": self._query_cache[cache_key]
                }
            )
        
        results = []
        
        for source_id in source_ids:
            if source_id not in self._data_sources:
                results.append({
                    "source_id": source_id,
                    "success": False,
                    "error": "Data source not found"
                })
                continue
            
            source = self._data_sources[source_id]
            source.last_used = datetime.now()
            source.query_count += 1
            
            result = self._execute_source_query(source, query)
            results.append(result)
        
        merged = self._merge_results(results, merge_strategy)
        
        if use_cache:
            self._query_cache[cache_key] = merged
        
        return ActionResult(
            success=True,
            message="Federated query complete",
            data={
                "source_count": len(source_ids),
                "results": results,
                "merged": merged,
                "cached": False
            }
        )
    
    def _execute_source_query(self, source: DataSource, query: str) -> Dict:
        return {
            "source_id": source.source_id,
            "source_name": source.name,
            "source_type": source.source_type,
            "success": True,
            "data": [],
            "row_count": 0
        }
    
    def _merge_results(self, results: List[Dict], strategy: str) -> Any:
        successful = [r for r in results if r.get("success")]
        
        if not successful:
            return None
        
        if strategy == "union":
            merged = []
            for r in successful:
                merged.extend(r.get("data", []))
            return merged
        elif strategy == "intersection":
            if not successful:
                return []
            merged = successful[0].get("data", [])
            for r in successful[1:]:
                data = r.get("data", [])
                merged = [item for item in merged if item in data]
            return merged
        elif strategy == "first":
            return successful[0].get("data", [])
        else:
            return successful[0].get("data", [])
    
    def _register_data_source(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        name = params.get("name")
        source_type = params.get("source_type")
        connection_config = params.get("connection_config", {})
        
        if not source_id or not name or not source_type:
            return ActionResult(success=False, message="source_id, name, and source_type are required")
        
        source = DataSource(source_id, name, source_type, connection_config)
        self._data_sources[source_id] = source
        
        return ActionResult(
            success=True,
            message=f"Data source registered: {name}",
            data={"source": source.to_dict()}
        )
    
    def _list_data_sources(self) -> ActionResult:
        sources = [source.to_dict() for source in self._data_sources.values()]
        
        return ActionResult(
            success=True,
            message=f"{len(sources)} data sources registered",
            data={"sources": sources, "count": len(sources)}
        )
    
    def _unregister_data_source(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        
        if source_id in self._data_sources:
            del self._data_sources[source_id]
            return ActionResult(success=True, message=f"Data source {source_id} unregistered")
        
        return ActionResult(success=False, message=f"Data source {source_id} not found")


class DataSourceRegistryAction(BaseAction):
    """Register and manage data sources."""
    action_type = "data_source_registry"
    display_name = "数据源注册"
    description = "注册和管理数据源"
    
    def __init__(self):
        super().__init__()
        self._registry: Dict[str, DataSource] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            
            if operation == "register":
                return self._register(params)
            elif operation == "list":
                return self._list_sources()
            elif operation == "get":
                return self._get_source(params)
            elif operation == "update":
                return self._update_source(params)
            elif operation == "delete":
                return self._delete_source(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        name = params.get("name")
        source_type = params.get("source_type")
        connection_config = params.get("connection_config", {})
        metadata = params.get("metadata", {})
        
        if not source_id or not name or not source_type:
            return ActionResult(success=False, message="source_id, name, and source_type are required")
        
        if source_id in self._registry:
            return ActionResult(success=False, message=f"Data source {source_id} already exists")
        
        source = DataSource(source_id, name, source_type, connection_config)
        source.metadata = metadata
        self._registry[source_id] = source
        
        return ActionResult(
            success=True,
            message=f"Data source registered: {name}",
            data={"source": source.to_dict()}
        )
    
    def _list_sources(self) -> ActionResult:
        sources = [s.to_dict() for s in self._registry.values()]
        
        return ActionResult(
            success=True,
            message=f"{len(sources)} data sources registered",
            data={"sources": sources}
        )
    
    def _get_source(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        
        if source_id not in self._registry:
            return ActionResult(success=False, message=f"Data source {source_id} not found")
        
        return ActionResult(
            success=True,
            message="Data source retrieved",
            data={"source": self._registry[source_id].to_dict()}
        )
    
    def _update_source(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        
        if source_id not in self._registry:
            return ActionResult(success=False, message=f"Data source {source_id} not found")
        
        source = self._registry[source_id]
        
        if "name" in params:
            source.name = params["name"]
        if "connection_config" in params:
            source.connection_config = params["connection_config"]
        
        return ActionResult(
            success=True,
            message=f"Data source updated: {source_id}",
            data={"source": source.to_dict()}
        )
    
    def _delete_source(self, params: Dict[str, Any]) -> ActionResult:
        source_id = params.get("source_id")
        
        if source_id in self._registry:
            del self._registry[source_id]
            return ActionResult(success=True, message=f"Data source deleted: {source_id}")
        
        return ActionResult(success=False, message=f"Data source {source_id} not found")


class FederatedJoinAction(BaseAction):
    """Join data from multiple sources."""
    action_type = "federated_join"
    display_name = "联邦连接"
    description = "连接来自多个数据源的数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            join_key = params.get("join_key")
            join_type = params.get("join_type", "inner")
            
            if len(datasets) < 2:
                return ActionResult(success=False, message="At least 2 datasets required")
            
            if not join_key:
                return ActionResult(success=False, message="join_key is required")
            
            result = self._perform_join(datasets, join_key, join_type)
            
            return ActionResult(
                success=True,
                message=f"Join complete ({join_type})",
                data={
                    "join_type": join_type,
                    "join_key": join_key,
                    "result_count": len(result),
                    "result": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _perform_join(self, datasets: List[List[Dict]], join_key: str, join_type: str) -> List[Dict]:
        if join_type == "inner":
            return self._inner_join(datasets, join_key)
        elif join_type == "left":
            return self._left_join(datasets, join_key)
        elif join_type == "right":
            return self._right_join(datasets, join_key)
        elif join_type == "full":
            return self._full_outer_join(datasets, join_key)
        else:
            return []
    
    def _inner_join(self, datasets: List[List[Dict]], join_key: str) -> List[Dict]:
        if len(datasets) < 2:
            return datasets[0] if datasets else []
        
        result = datasets[0]
        
        for dataset in datasets[1:]:
            lookup = {item.get(join_key): item for item in dataset if item.get(join_key)}
            
            joined = []
            for item in result:
                key = item.get(join_key)
                if key in lookup:
                    merged = {**item, **lookup[key]}
                    merged.pop(join_key, None)
                    joined.append(merged)
            
            result = joined
        
        return result
    
    def _left_join(self, datasets: List[List[Dict]], join_key: str) -> List[Dict]:
        return self._inner_join(datasets, join_key)
    
    def _right_join(self, datasets: List[List[Dict]], join_key: str) -> List[Dict]:
        datasets = list(reversed(datasets))
        return self._left_join(datasets, join_key)
    
    def _full_outer_join(self, datasets: List[List[Dict]], join_key: str) -> List[Dict]:
        return self._inner_join(datasets, join_key)


class FederatedAggregateAction(BaseAction):
    """Aggregate federated data."""
    action_type = "federated_aggregate"
    display_name = "联邦聚合"
    description = "聚合联邦数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            group_by = params.get("group_by")
            aggregations = params.get("aggregations", [{"field": "*", "func": "count"}])
            
            if not datasets:
                return ActionResult(success=False, message="No datasets provided")
            
            all_data = []
            for dataset in datasets:
                if isinstance(dataset, list):
                    all_data.extend(dataset)
                else:
                    all_data.append(dataset)
            
            if group_by:
                grouped = defaultdict(list)
                for item in all_data:
                    if isinstance(item, dict):
                        key = item.get(group_by, "unknown")
                        grouped[key].append(item)
                
                results = {}
                for key, items in grouped.items():
                    agg_result = self._aggregate_items(items, aggregations)
                    results[key] = agg_result
                
                return ActionResult(
                    success=True,
                    message="Federated aggregation complete",
                    data={
                        "group_by": group_by,
                        "groups": len(results),
                        "results": results
                    }
                )
            else:
                result = self._aggregate_items(all_data, aggregations)
                
                return ActionResult(
                    success=True,
                    message="Federated aggregation complete",
                    data={
                        "total_items": len(all_data),
                        "aggregations": result
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _aggregate_items(self, items: List[Dict], aggregations: List[Dict]) -> Dict:
        result = {}
        
        for agg in aggregations:
            field = agg.get("field", "*")
            func = agg.get("func", "count")
            
            if field == "*" or func == "count":
                result[f"{func}(*)"] = len(items)
            else:
                values = [item.get(field, 0) for item in items if isinstance(item, dict)]
                
                if func == "sum":
                    result[f"sum({field})"] = sum(values)
                elif func == "avg":
                    result[f"avg({field})"] = sum(values) / len(values) if values else 0
                elif func == "min":
                    result[f"min({field})"] = min(values) if values else None
                elif func == "max":
                    result[f"max({field})"] = max(values) if values else None
                elif func == "count":
                    result[f"count({field})"] = len(values)
        
        return result


class FederatedCacheAction(BaseCache):
    """Cache federated query results."""
    action_type = "federated_cache"
    display_name = "联邦缓存"
    description = "缓存联邦查询结果"
    
    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            
            if operation == "get":
                return self._get_cached(params)
            elif operation == "set":
                return self._set_cached(params)
            elif operation == "invalidate":
                return self._invalidate(params)
            elif operation == "stats":
                return self._get_stats()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _get_cached(self, params: Dict[str, Any]) -> ActionResult:
        cache_key = params.get("cache_key")
        
        if not cache_key:
            return ActionResult(success=False, message="cache_key is required")
        
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            entry["hits"] = entry.get("hits", 0) + 1
            entry["last_accessed"] = datetime.now()
            
            return ActionResult(
                success=True,
                message="Cache hit",
                data={
                    "cached": True,
                    "data": entry.get("data"),
                    "hits": entry.get("hits", 0)
                }
            )
        
        return ActionResult(
            success=True,
            message="Cache miss",
            data={"cached": False}
        )
    
    def _set_cached(self, params: Dict[str, Any]) -> ActionResult:
        cache_key = params.get("cache_key")
        data = params.get("data")
        ttl = params.get("ttl", 3600)
        
        if not cache_key:
            return ActionResult(success=False, message="cache_key is required")
        
        self._cache[cache_key] = {
            "data": data,
            "created_at": datetime.now(),
            "last_accessed": datetime.now(),
            "expires_at": datetime.now() + timedelta(seconds=ttl),
            "hits": 0
        }
        
        return ActionResult(
            success=True,
            message="Data cached",
            data={"cache_key": cache_key, "ttl": ttl}
        )
    
    def _invalidate(self, params: Dict[str, Any]) -> ActionResult:
        cache_key = params.get("cache_key")
        
        if cache_key:
            if cache_key in self._cache:
                del self._cache[cache_key]
                return ActionResult(success=True, message=f"Cache entry {cache_key} invalidated")
        else:
            count = len(self._cache)
            self._cache.clear()
            return ActionResult(success=True, message=f"Cleared {count} cache entries")
        
        return ActionResult(success=False, message=f"Cache key {cache_key} not found")
    
    def _get_stats(self) -> ActionResult:
        now = datetime.now()
        
        valid_entries = sum(1 for e in self._cache.values() if e.get("expires_at", now) > now)
        
        return ActionResult(
            success=True,
            message="Cache statistics",
            data={
                "total_entries": len(self._cache),
                "valid_entries": valid_entries,
                "expired_entries": len(self._cache) - valid_entries,
                "total_hits": sum(e.get("hits", 0) for e in self._cache.values())
            }
        )


class DataCache:
    """Base cache class."""
    
    def __init__(self):
        pass


from datetime import timedelta
