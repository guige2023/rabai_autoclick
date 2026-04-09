"""Data Federation Action Module.

Provides data federation utilities: multi-source queries, query planning,
result merging, schema reconciliation, and data virtualization.

Example:
    result = execute(context, {"action": "federated_query", "sources": [...]})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict


@dataclass
class DataSource:
    """A federated data source."""
    
    id: str
    name: str
    source_type: str
    connection_info: dict[str, Any]
    priority: int = 1
    latency_ms: float = 0.0
    available: bool = True
    
    def __post_init__(self) -> None:
        """Validate source type."""
        valid_types = ["database", "api", "file", "cache", "stream"]
        if self.source_type not in valid_types:
            raise ValueError(f"Invalid source_type: {self.source_type}")


@dataclass
class FederatedQuery:
    """A query spanning multiple data sources."""
    
    id: str
    query: str
    sources: list[str]
    timeout_seconds: float = 30.0
    merge_strategy: str = "union"
    
    def __post_init__(self) -> None:
        """Validate merge strategy."""
        valid_strategies = ["union", "join", "broadcast", "scatter"]
        if self.merge_strategy not in valid_strategies:
            raise ValueError(f"Invalid merge_strategy: {self.merge_strategy}")


@dataclass
class QueryResult:
    """Result from a federated query."""
    
    query_id: str
    source_id: str
    data: list[dict[str, Any]]
    row_count: int
    execution_time_ms: float
    timestamp: datetime
    error: Optional[str] = None


class SchemaResolver:
    """Resolves schemas across federated data sources."""
    
    def __init__(self) -> None:
        """Initialize schema resolver."""
        self._schemas: dict[str, dict[str, Any]] = {}
    
    def register_schema(self, source_id: str, schema: dict[str, Any]) -> None:
        """Register a schema for a source.
        
        Args:
            source_id: Data source identifier
            schema: Schema definition {table: {columns: [...]}}
        """
        self._schemas[source_id] = schema
    
    def resolve_field(
        self,
        field_name: str,
        sources: list[str],
    ) -> list[tuple[str, str]]:
        """Resolve a field across sources.
        
        Args:
            field_name: Field to resolve
            sources: Source IDs to search
            
        Returns:
            List of (source_id, qualified_name) tuples
        """
        matches = []
        
        for source_id in sources:
            schema = self._schemas.get(source_id, {})
            for table, table_schema in schema.items():
                columns = table_schema.get("columns", [])
                for column in columns:
                    if column.get("name") == field_name:
                        qualified = f"{table}.{column['name']}"
                        matches.append((source_id, qualified))
        
        return matches
    
    def reconcile_schemas(
        self,
        schemas: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Reconcile multiple schemas into a unified schema.
        
        Args:
            schemas: List of schemas to reconcile
            
        Returns:
            Unified schema definition
        """
        unified: dict[str, set] = defaultdict(set)
        
        for schema in schemas:
            for table, table_schema in schema.items():
                columns = table_schema.get("columns", [])
                for column in columns:
                    unified[table].add(column.get("name", ""))
        
        return {
            table: {"columns": [{"name": col} for col in cols]}
            for table, cols in unified.items()
        }


class QueryPlanner:
    """Plans federated query execution across sources."""
    
    def __init__(self) -> None:
        """Initialize query planner."""
        self._sources: dict[str, DataSource] = {}
    
    def register_source(self, source: DataSource) -> None:
        """Register a data source.
        
        Args:
            source: Data source to register
        """
        self._sources[source.id] = source
    
    def plan_query(self, query: FederatedQuery) -> list[dict[str, Any]]:
        """Create execution plan for federated query.
        
        Args:
            query: Federated query to plan
            
        Returns:
            List of execution steps
        """
        plan = []
        
        available_sources = [
            self._sources[sid] for sid in query.sources
            if sid in self._sources and self._sources[sid].available
        ]
        
        if not available_sources:
            return [{"error": "No available sources"}]
        
        if query.merge_strategy == "scatter":
            for source in available_sources:
                plan.append({
                    "step": "execute",
                    "source_id": source.id,
                    "query": query.query,
                    "priority": source.priority,
                })
        
        elif query.merge_strategy == "broadcast":
            primary = min(available_sources, key=lambda s: s.priority)
            plan.append({
                "step": "execute",
                "source_id": primary.id,
                "query": query.query,
                "priority": primary.priority,
            })
            plan.append({"step": "broadcast_results"})
        
        elif query.merge_strategy == "join":
            for i, source in enumerate(available_sources):
                plan.append({
                    "step": "execute",
                    "source_id": source.id,
                    "query": query.query,
                    "priority": source.priority,
                    "order": i,
                })
            plan.append({"step": "join_results"})
        
        else:
            for source in available_sources:
                plan.append({
                    "step": "execute",
                    "source_id": source.id,
                    "query": query.query,
                    "priority": source.priority,
                })
            plan.append({"step": "union_results"})
        
        return plan


class ResultMerger:
    """Merges results from federated queries."""
    
    def __init__(self, strategy: str = "union") -> None:
        """Initialize result merger.
        
        Args:
            strategy: Merge strategy
        """
        self.strategy = strategy
        self._results: list[QueryResult] = []
    
    def add_result(self, result: QueryResult) -> None:
        """Add a query result.
        
        Args:
            result: Query result to add
        """
        self._results.append(result)
    
    def merge(self) -> dict[str, Any]:
        """Merge all results.
        
        Returns:
            Merged result data
        """
        if not self._results:
            return {"data": [], "row_count": 0}
        
        if self.strategy == "union":
            return self._union_merge()
        elif self.strategy == "join":
            return self._join_merge()
        elif self.strategy == "broadcast":
            return self._broadcast_merge()
        else:
            return self._union_merge()
    
    def _union_merge(self) -> dict[str, Any]:
        """Union all result sets."""
        all_data: list[dict[str, Any]] = []
        
        for result in self._results:
            if result.error is None:
                all_data.extend(result.data)
        
        return {
            "data": all_data,
            "row_count": len(all_data),
            "source_count": len(self._results),
        }
    
    def _join_merge(self) -> dict[str, Any]:
        """Join result sets."""
        if not self._results:
            return {"data": [], "row_count": 0}
        
        joined = self._results[0].data
        
        for result in self._results[1:]:
            if result.error is None and result.data:
                joined = self._join_two_sets(joined, result.data)
        
        return {
            "data": joined,
            "row_count": len(joined),
            "source_count": len(self._results),
        }
    
    def _join_two_sets(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Join two result sets."""
        result = []
        for l in left:
            for r in right:
                merged = {**l, **r}
                result.append(merged)
        return result
    
    def _broadcast_merge(self) -> dict[str, Any]:
        """Broadcast first result to others."""
        if not self._results:
            return {"data": [], "row_count": 0}
        
        return {
            "data": self._results[0].data,
            "row_count": self._results[0].row_count,
            "source_count": len(self._results),
            "strategy": "broadcast",
        }


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute data federation action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "register_source":
        source = DataSource(
            id=params.get("id", ""),
            name=params.get("name", ""),
            source_type=params.get("source_type", "database"),
            connection_info=params.get("connection_info", {}),
            priority=params.get("priority", 1),
        )
        result["data"] = {
            "source_id": source.id,
            "source_type": source.source_type,
        }
    
    elif action == "create_query":
        query = FederatedQuery(
            id=params.get("id", ""),
            query=params.get("query", ""),
            sources=params.get("sources", []),
            timeout_seconds=params.get("timeout_seconds", 30.0),
            merge_strategy=params.get("merge_strategy", "union"),
        )
        result["data"] = {
            "query_id": query.id,
            "source_count": len(query.sources),
        }
    
    elif action == "plan_query":
        planner = QueryPlanner()
        query = FederatedQuery(
            id="temp",
            query="",
            sources=params.get("sources", []),
        )
        plan = planner.plan_query(query)
        result["data"] = {"plan": plan}
    
    elif action == "merge_results":
        merger = ResultMerger(strategy=params.get("strategy", "union"))
        merged = merger.merge()
        result["data"] = merged
    
    elif action == "resolve_schema":
        resolver = SchemaResolver()
        field_name = params.get("field_name", "")
        sources = params.get("sources", [])
        resolved = resolver.resolve_field(field_name, sources)
        result["data"] = {"resolved": resolved}
    
    elif action == "reconcile_schemas":
        resolver = SchemaResolver()
        schemas = params.get("schemas", [])
        unified = resolver.reconcile_schemas(schemas)
        result["data"] = {"unified_schema": unified}
    
    elif action == "source_status":
        sources = params.get("sources", [])
        result["data"] = {
            "sources": sources,
            "available_count": len(sources),
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
