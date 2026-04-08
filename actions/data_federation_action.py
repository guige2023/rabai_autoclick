"""
Data Federation Action Module

Provides data federation, cross-source queries, and unified data access.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import asyncio


class DataSourceType(Enum):
    """Type of data source."""
    SQL = "sql"
    NOSQL = "nosql"
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    FILE = "file"
    STREAM = "stream"
    CACHE = "cache"


@dataclass
class DataSource:
    """A federated data source."""
    name: str
    source_type: DataSourceType
    connection_config: dict[str, Any]
    connector: Optional[Any] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    timeout_seconds: float = 30.0
    retry_attempts: int = 3


@dataclass
class FederatedQuery:
    """A query across multiple data sources."""
    sources: list[str]
    query_template: str
    parameters: dict[str, Any]
    join_strategy: str = "union"  # union, join, broadcast
    aggregation: Optional[str] = None


@dataclass
class QueryResult:
    """Result from a federated query."""
    source: str
    data: list[dict]
    row_count: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)


@dataclass
class FederatedResult:
    """Combined result from federated query."""
    results: list[QueryResult]
    combined_data: list[dict]
    total_rows: int
    duration_ms: float
    errors: list[str]


class DataFederationAction:
    """Main data federation action handler."""
    
    def __init__(self):
        self._sources: dict[str, DataSource] = {}
        self._query_cache: dict[str, tuple[Any, datetime]] = {}
        self._cache_ttl_seconds = 300
        self._federation_stats: dict[str, dict] = defaultdict(lambda: {
            "queries": 0, "rows": 0, "errors": 0, "avg_duration_ms": 0
        })
    
    def register_source(
        self,
        source: DataSource
    ) -> "DataFederationAction":
        """Register a data source for federation."""
        self._sources[source.name] = source
        return self
    
    def unregister_source(self, name: str) -> bool:
        """Unregister a data source."""
        if name in self._sources:
            del self._sources[name]
            return True
        return False
    
    async def execute_federated_query(
        self,
        federated_query: FederatedQuery
    ) -> FederatedResult:
        """
        Execute a query across multiple data sources.
        
        Args:
            federated_query: Query definition with source list and parameters
            
        Returns:
            FederatedResult with combined data from all sources
        """
        start_time = datetime.now()
        errors = []
        
        # Validate sources
        valid_sources = [
            s for s in federated_query.sources
            if s in self._sources
        ]
        
        if not valid_sources:
            return FederatedResult(
                results=[],
                combined_data=[],
                total_rows=0,
                duration_ms=0,
                errors=[f"No valid sources found from {federated_query.sources}"]
            )
        
        # Execute queries in parallel
        tasks = [
            self._execute_source_query(source_name, federated_query)
            for source_name in valid_sources
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        query_results = []
        all_data = []
        
        for source_name, result in zip(valid_sources, results):
            if isinstance(result, Exception):
                errors.append(f"{source_name}: {str(result)}")
                self._federation_stats[source_name]["errors"] += 1
            else:
                query_results.append(result)
                all_data.extend(result.data)
                self._federation_stats[source_name]["queries"] += 1
                self._federation_stats[source_name]["rows"] += result.row_count
        
        # Apply aggregation if specified
        if federated_query.aggregation:
            all_data = await self._apply_aggregation(
                all_data,
                federated_query.aggregation
            )
        
        # Apply join strategy
        combined_data = await self._apply_join_strategy(
            [r.data for r in query_results],
            federated_query.join_strategy
        )
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return FederatedResult(
            results=query_results,
            combined_data=combined_data,
            total_rows=len(combined_data),
            duration_ms=duration_ms,
            errors=errors
        )
    
    async def _execute_source_query(
        self,
        source_name: str,
        federated_query: FederatedQuery
    ) -> QueryResult:
        """Execute query on a single source."""
        start_time = datetime.now()
        source = self._sources[source_name]
        
        # Check cache
        cache_key = f"{source_name}:{federated_query.query_template}:{hash(frozenset(federated_query.parameters.items()))}"
        if cache_key in self._query_cache:
            cached_data, cached_at = self._query_cache[cache_key]
            age = (datetime.now() - cached_at).total_seconds()
            if age < self._cache_ttl_seconds:
                return QueryResult(
                    source=source_name,
                    data=cached_data,
                    row_count=len(cached_data),
                    duration_ms=0,
                    errors=[]
                )
        
        # Simulate query execution
        # In real implementation, this would use the connector
        await asyncio.sleep(0.01)  # Simulate latency
        
        data = [federated_query.parameters]  # Simplified
        
        # Cache result
        self._query_cache[cache_key] = (data, datetime.now())
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResult(
            source=source_name,
            data=data,
            row_count=len(data),
            duration_ms=duration_ms
        )
    
    async def _apply_aggregation(
        self,
        data: list[dict],
        aggregation: str
    ) -> list[dict]:
        """Apply aggregation to combined data."""
        if not data:
            return []
        
        if aggregation == "sum":
            # Sum numeric fields
            result = {}
            for record in data:
                for key, value in record.items():
                    if isinstance(value, (int, float)):
                        result[key] = result.get(key, 0) + value
                    else:
                        result[key] = value
            return [result]
        
        elif aggregation == "avg":
            # Average numeric fields
            counts = defaultdict(int)
            result = defaultdict(float)
            
            for record in data:
                for key, value in record.items():
                    if isinstance(value, (int, float)):
                        result[key] += value
                        counts[key] += 1
                    else:
                        result[key] = value
            
            for key in counts:
                result[key] /= counts[key]
            
            return [dict(result)]
        
        elif aggregation == "count":
            return [{"total_count": len(data)}]
        
        elif aggregation == "group_by":
            # Group by first string field
            groups = defaultdict(list)
            group_key = None
            
            for record in data:
                if not group_key:
                    for k, v in record.items():
                        if isinstance(v, str):
                            group_key = k
                            break
                
                if group_key and group_key in record:
                    groups[record[group_key]].append(record)
            
            return [{"group": k, "count": len(v)} for k, v in groups.items()]
        
        return data
    
    async def _apply_join_strategy(
        self,
        data_list: list[list[dict]],
        strategy: str
    ) -> list[dict]:
        """Apply join strategy to combine results from multiple sources."""
        if not data_list:
            return []
        
        if strategy == "union":
            # Combine all records
            combined = []
            for data in data_list:
                combined.extend(data)
            return combined
        
        elif strategy == "broadcast":
            # Broadcast first source to others
            if len(data_list) < 2:
                return data_list[0] if data_list else []
            
            base = data_list[0]
            result = []
            
            for record in base:
                for i, data in enumerate(data_list[1:], 1):
                    merged = dict(record)
                    merged[f"source_{i}"] = data[0] if data else {}
                    result.append(merged)
            
            return result
        
        elif strategy == "join":
            # Join records by key
            if len(data_list) < 2:
                return data_list[0] if data_list else []
            
            result = []
            for i, data in enumerate(data_list):
                for record in data:
                    joined = {f"source_{i}_{k}": v for k, v in record.items()}
                    result.append(joined)
            
            return result
        
        return data_list[0] if data_list else []
    
    async def federated_search(
        self,
        query: str,
        sources: Optional[list[str]] = None,
        filters: Optional[dict[str, Any]] = None
    ) -> FederatedResult:
        """Search across federated sources."""
        sources = sources or list(self._sources.keys())
        
        federated_query = FederatedQuery(
            sources=sources,
            query_template=query,
            parameters=filters or {},
            join_strategy="union"
        )
        
        return await self.execute_federated_query(federated_query)
    
    async def get_data_summary(
        self,
        source_name: Optional[str] = None
    ) -> dict[str, Any]:
        """Get summary of federated data sources."""
        if source_name:
            if source_name not in self._sources:
                return {"error": "Source not found"}
            
            source = self._sources[source_name]
            stats = self._federation_stats.get(source_name, {})
            
            return {
                "name": source.name,
                "type": source.source_type.value,
                "metadata": source.metadata,
                "stats": stats
            }
        
        return {
            "total_sources": len(self._sources),
            "sources": {
                name: {
                    "type": s.source_type.value,
                    "priority": s.priority,
                    "stats": self._federation_stats.get(name, {})
                }
                for name, s in self._sources.items()
            },
            "cache_size": len(self._query_cache)
        }
    
    async def clear_cache(self, source_name: Optional[str] = None):
        """Clear query cache."""
        if source_name:
            keys_to_remove = [
                k for k in self._query_cache
                if k.startswith(f"{source_name}:")
            ]
            for key in keys_to_remove:
                del self._query_cache[key]
        else:
            self._query_cache.clear()
    
    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_size = len(self._query_cache)
        oldest_entry = None
        newest_entry = None
        
        if self._query_cache:
            timestamps = [t for _, t in self._query_cache.values()]
            oldest_entry = min(timestamps).isoformat()
            newest_entry = max(timestamps).isoformat()
        
        return {
            "cached_queries": total_size,
            "oldest_entry": oldest_entry,
            "newest_entry": newest_entry,
            "ttl_seconds": self._cache_ttl_seconds
        }
