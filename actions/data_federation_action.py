"""Data Federation Action Module.

Provides data federation across multiple sources with:
- Unified query interface
- Source routing
- Result merging
- Query optimization
- Data virtualization

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Federated query types."""
    SELECT = auto()
    AGGREGATE = auto()
    JOIN = auto()
    UNION = auto()


@dataclass
class DataSource:
    """Data source configuration."""
    id: str
    name: str
    source_type: str
    connection_config: Dict[str, Any] = field(default_factory=dict)
    tables: Set[str] = field(default_factory=set)
    capabilities: Set[str] = field(default_factory=set)
    priority: int = 100
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FederatedQuery:
    """Federated query specification."""
    id: str
    query_type: QueryType
    sources: List[str] = field(default_factory=list)
    select_fields: List[str] = field(default_factory=list)
    filters: Dict[str, Any] = field(default_factory=dict)
    aggregations: Optional[Dict[str, str]] = None
    joins: Optional[List[Dict[str, Any]]] = None
    union_queries: Optional[List["FederatedQuery"]] = None
    timeout_seconds: float = 30.0


@dataclass
class QueryResult:
    """Result from a federated query."""
    query_id: str
    success: bool
    data: List[Dict[str, Any]] = field(default_factory=list)
    sources_queried: List[str] = field(default_factory=list)
    total_records: int = 0
    execution_time_ms: float = 0.0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataFederationEngine:
    """Federates queries across multiple data sources.
    
    Features:
    - Multi-source querying
    - Query planning and optimization
    - Result merging and deduplication
    - Source selection based on capabilities
    - Parallel query execution
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self._sources: Dict[str, DataSource] = {}
        self._source_handlers: Dict[str, Callable] = {}
        self._query_handlers: Dict[QueryType, Callable] = {}
        self._lock = asyncio.Lock()
        self._metrics = {
            "total_queries": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "total_records_fetched": 0,
            "sources_registered": 0
        }
    
    def register_source(
        self,
        source: DataSource,
        handler: Callable
    ) -> None:
        """Register a data source.
        
        Args:
            source: Data source configuration
            handler: Query handler function for this source
        """
        self._sources[source.id] = source
        self._source_handlers[source.id] = handler
        self._metrics["sources_registered"] += 1
        logger.info(f"Registered data source: {source.id}")
    
    def unregister_source(self, source_id: str) -> bool:
        """Unregister a data source.
        
        Args:
            source_id: Source ID to remove
            
        Returns:
            True if removed
        """
        if source_id in self._sources:
            del self._sources[source_id]
            if source_id in self._source_handlers:
                del self._source_handlers[source_id]
            return True
        return False
    
    def register_query_handler(
        self,
        query_type: QueryType,
        handler: Callable
    ) -> None:
        """Register a query handler for a specific type.
        
        Args:
            query_type: Query type to handle
            handler: Handler function
        """
        self._query_handlers[query_type] = handler
    
    async def execute_query(
        self,
        query: FederatedQuery
    ) -> QueryResult:
        """Execute a federated query.
        
        Args:
            query: Federated query specification
            
        Returns:
            Query result
        """
        self._metrics["total_queries"] += 1
        start_time = asyncio.get_event_loop().time()
        
        try:
            sources_to_query = await self._select_sources(query)
            
            if not sources_to_query:
                return QueryResult(
                    query_id=query.id,
                    success=False,
                    errors=["No suitable sources found for query"]
                )
            
            if query.query_type in self._query_handlers:
                handler = self._query_handlers[query.query_type]
                result = await handler(query, sources_to_query)
            else:
                result = await self._execute_default_query(query, sources_to_query)
            
            result.query_id = query.id
            result.sources_queried = [s.id for s in sources_to_query]
            result.total_records = len(result.data)
            result.execution_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            self._metrics["total_records_fetched"] += result.total_records
            self._metrics["successful_queries"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Federated query error: {e}")
            self._metrics["failed_queries"] += 1
            return QueryResult(
                query_id=query.id,
                success=False,
                errors=[str(e)]
            )
    
    async def _select_sources(
        self,
        query: FederatedQuery
    ) -> List[DataSource]:
        """Select optimal sources for a query.
        
        Args:
            query: Query to select sources for
            
        Returns:
            List of selected sources
        """
        if query.sources:
            return [
                self._sources[sid] for sid in query.sources
                if sid in self._sources
            ]
        
        all_sources = list(self._sources.values())
        all_sources.sort(key=lambda s: s.priority, reverse=True)
        
        return all_sources
    
    async def _execute_default_query(
        self,
        query: FederatedQuery,
        sources: List[DataSource]
    ) -> QueryResult:
        """Execute query with default behavior.
        
        Args:
            query: Query to execute
            sources: Sources to query
            
        Returns:
            Query result
        """
        results = []
        errors = []
        
        for source in sources:
            try:
                handler = self._source_handlers.get(source.id)
                if not handler:
                    continue
                
                source_query = await self._adapt_query_for_source(query, source)
                
                if asyncio.iscoroutinefunction(handler):
                    source_result = await asyncio.wait_for(
                        handler(source_query),
                        timeout=query.timeout_seconds
                    )
                else:
                    source_result = handler(source_query)
                
                if isinstance(source_result, list):
                    results.extend(source_result)
                elif isinstance(source_result, dict):
                    results.append(source_result)
                    
            except asyncio.TimeoutError:
                errors.append(f"Timeout querying source {source.id}")
            except Exception as e:
                errors.append(f"Error querying {source.id}: {e}")
        
        if query.filters:
            results = await self._apply_filters(results, query.filters)
        
        return QueryResult(
            query_id=query.id,
            success=len(errors) < len(sources),
            data=results,
            errors=errors
        )
    
    async def _adapt_query_for_source(
        self,
        query: FederatedQuery,
        source: DataSource
    ) -> Dict[str, Any]:
        """Adapt query for a specific source.
        
        Args:
            query: Original query
            source: Target source
            
        Returns:
            Adapted query for source
        """
        return {
            "type": query.query_type.name,
            "select": query.select_fields,
            "filters": query.filters,
            "source_name": source.name,
            "source_type": source.source_type
        }
    
    async def _apply_filters(
        self,
        data: List[Dict[str, Any]],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply filters to result data.
        
        Args:
            data: Data to filter
            filters: Filter specifications
            
        Returns:
            Filtered data
        """
        filtered = data
        
        for field_name, condition in filters.items():
            if isinstance(condition, dict):
                op = condition.get("op", "eq")
                value = condition.get("value")
                
                filtered = [
                    row for row in filtered
                    if self._apply_filter_op(row.get(field_name), op, value)
                ]
            else:
                filtered = [
                    row for row in filtered
                    if row.get(field_name) == condition
                ]
        
        return filtered
    
    def _apply_filter_op(
        self,
        value: Any,
        op: str,
        target: Any
    ) -> bool:
        """Apply a filter operator.
        
        Args:
            value: Value to compare
            op: Operator (eq, ne, gt, lt, gte, lte, in, contains)
            target: Target value
            
        Returns:
            True if filter passes
        """
        if op == "eq":
            return value == target
        elif op == "ne":
            return value != target
        elif op == "gt":
            return value > target
        elif op == "lt":
            return value < target
        elif op == "gte":
            return value >= target
        elif op == "lte":
            return value <= target
        elif op == "in":
            return value in target
        elif op == "contains":
            return target in str(value)
        return True
    
    async def execute_join(
        self,
        left_query: FederatedQuery,
        right_query: FederatedQuery,
        join_key: str,
        join_type: str = "inner"
    ) -> QueryResult:
        """Execute a federated join query.
        
        Args:
            left_query: Left side query
            right_query: Right side query
            join_key: Join key field
            join_type: Type of join (inner, left, right, outer)
            
        Returns:
            Join result
        """
        left_result = await self.execute_query(left_query)
        right_result = await self.execute_query(right_query)
        
        if not left_result.success or not right_result.success:
            return QueryResult(
                query_id=left_query.id,
                success=False,
                errors=[*left_result.errors, *right_result.errors]
            )
        
        joined = await self._perform_join(
            left_result.data,
            right_result.data,
            join_key,
            join_type
        )
        
        return QueryResult(
            query_id=left_query.id,
            success=True,
            data=joined,
            sources_queried=[*left_result.sources_queried, *right_result.sources_queried]
        )
    
    async def _perform_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        join_key: str,
        join_type: str
    ) -> List[Dict[str, Any]]:
        """Perform the actual join operation.
        
        Args:
            left_data: Left relation data
            right_data: Right relation data
            join_key: Join key field
            join_type: Type of join
            
        Returns:
            Joined data
        """
        right_index = defaultdict(list)
        for row in right_data:
            key_val = row.get(join_key)
            if key_val is not None:
                right_index[key_val].append(row)
        
        results = []
        
        for left_row in left_data:
            key_val = left_row.get(join_key)
            matching_right = right_index.get(key_val, [])
            
            if matching_right:
                for right_row in matching_right:
                    merged = {**left_row, **right_row}
                    results.append(merged)
            elif join_type in ("left", "outer"):
                results.append(left_row)
        
        if join_type == "outer":
            left_keys = {row.get(join_key) for row in left_data}
            for right_row in right_data:
                key_val = right_row.get(join_key)
                if key_val not in left_keys:
                    results.append(right_row)
        
        return results
    
    def get_sources(self) -> List[DataSource]:
        """Get all registered data sources."""
        return list(self._sources.values())
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get federation metrics."""
        return {
            **self._metrics,
            "sources_available": len(self._sources)
        }
