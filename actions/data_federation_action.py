"""
Data Federation Action Module.

Provides data federation capabilities for querying and combining data from
multiple sources into a unified view with support for various data connectors.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import threading
from datetime import datetime


class DataSourceType(Enum):
    """Types of data sources."""
    REST_API = "rest_api"
    DATABASE = "database"
    FILE = "file"
    CACHE = "cache"
    MEMORY = "memory"


@dataclass
class DataSource:
    """Represents a data source configuration."""
    id: str
    name: str
    source_type: DataSourceType
    connection_config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FederatedQuery:
    """Represents a federated query across sources."""
    id: str
    sources: List[str]  # Source IDs
    join_spec: Dict[str, Any] = field(default_factory=dict)
    filter_spec: Dict[str, Any] = field(default_factory=dict)
    projection: List[str] = field(default_factory=list)


@dataclass
class QueryResult:
    """Result of a federated query."""
    query_id: str
    data: List[Dict]
    source_results: Dict[str, int]
    duration_ms: float
    timestamp: datetime


class DataConnector(ABC):
    """Abstract base class for data connectors."""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection."""
        pass
    
    @abstractmethod
    def query(self, query_spec: Dict) -> List[Dict]:
        """Execute query and return results."""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if connector is healthy."""
        pass


class MemoryConnector(DataConnector):
    """
    In-memory data connector for testing and caching.
    
    Example:
        connector = MemoryConnector()
        connector.connect()
        connector.load_data([{"id": 1, "name": "test"}])
        results = connector.query({"filter": {"id": 1}})
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data: List[Dict] = []
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to memory store."""
        self._connected = True
        return True
    
    def disconnect(self):
        """Disconnect from memory store."""
        self._connected = False
    
    def load_data(self, data: List[Dict]):
        """Load data into memory store."""
        self.data = list(data)
    
    def query(self, query_spec: Dict) -> List[Dict]:
        """Query memory store."""
        results = list(self.data)
        
        # Apply filter
        if "filter" in query_spec:
            filters = query_spec["filter"]
            results = self._apply_filter(results, filters)
        
        # Apply projection
        if "projection" in query_spec:
            projections = query_spec["projection"]
            results = self._apply_projection(results, projections)
        
        # Apply limit
        if "limit" in query_spec:
            results = results[:query_spec["limit"]]
        
        return results
    
    def health_check(self) -> bool:
        """Check connector health."""
        return self._connected
    
    def _apply_filter(self, data: List[Dict], filters: Dict) -> List[Dict]:
        """Apply filter to data."""
        results = []
        for item in data:
            if self._matches_filter(item, filters):
                results.append(item)
        return results
    
    def _matches_filter(self, item: Dict, filters: Dict) -> bool:
        """Check if item matches filter."""
        for key, value in filters.items():
            if "." in key:
                keys = key.split(".")
                item_val = item
                for k in keys:
                    if isinstance(item_val, dict):
                        item_val = item_val.get(k)
                    else:
                        return False
                if item_val != value:
                    return False
            else:
                if item.get(key) != value:
                    return False
        return True
    
    def _apply_projection(self, data: List[Dict], projections: List[str]) -> List[Dict]:
        """Apply projection to data."""
        return [{k: item.get(k) for k in projections} for item in data]


class RESTAPIConnector(DataConnector):
    """
    REST API data connector.
    
    Example:
        connector = RESTAPIConnector({
            "base_url": "https://api.example.com",
            "headers": {"Authorization": "Bearer token"}
        })
        connector.connect()
        results = connector.query({"endpoint": "/users", "params": {"limit": 10}})
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = config.get("base_url", "")
        self.headers = config.get("headers", {})
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to REST API."""
        self._connected = True
        return True
    
    def disconnect(self):
        """Disconnect from REST API."""
        self._connected = False
    
    def query(self, query_spec: Dict) -> List[Dict]:
        """Query REST API."""
        endpoint = query_spec.get("endpoint", "/")
        params = query_spec.get("params", {})
        
        # This is a placeholder - in real implementation would make HTTP request
        # For now, return empty result
        return []
    
    def health_check(self) -> bool:
        """Check connector health."""
        return self._connected


class DataFederationEngine:
    """
    Main data federation engine.
    
    Example:
        engine = DataFederationEngine()
        engine.register_source("users", MemoryConnector({}))
        engine.register_source("orders", MemoryConnector({}))
        
        result = engine.execute({
            "sources": ["users", "orders"],
            "join": {"on": "user_id", "type": "inner"}
        })
    """
    
    def __init__(self):
        self.sources: Dict[str, DataConnector] = {}
        self.source_configs: Dict[str, DataSource] = {}
        self._lock = threading.RLock()
    
    def register_source(
        self,
        source_id: str,
        source_type: DataSourceType,
        config: Dict[str, Any]
    ) -> "DataFederationEngine":
        """Register a data source."""
        with self._lock:
            source_config = DataSource(
                id=source_id,
                name=source_id,
                source_type=source_type,
                connection_config=config
            )
            self.source_configs[source_id] = source_config
            
            # Create connector
            if source_type == DataSourceType.MEMORY:
                connector = MemoryConnector(config)
            elif source_type == DataSourceType.REST_API:
                connector = RESTAPIConnector(config)
            else:
                connector = MemoryConnector(config)
            
            connector.connect()
            self.sources[source_id] = connector
        
        return self
    
    def unregister_source(self, source_id: str) -> bool:
        """Unregister a data source."""
        with self._lock:
            if source_id in self.sources:
                self.sources[source_id].disconnect()
                del self.sources[source_id]
                del self.source_configs[source_id]
                return True
            return False
    
    def execute(self, query: FederatedQuery) -> QueryResult:
        """Execute a federated query."""
        import time
        start_time = time.monotonic()
        
        source_results: Dict[str, int] = {}
        all_data: List[Dict] = []
        
        # Query each source
        for source_id in query.sources:
            if source_id not in self.sources:
                continue
            
            connector = self.sources[source_id]
            
            query_spec = {
                "filter": query.filter_spec,
                "projection": query.projection if query.projection else None
            }
            
            try:
                results = connector.query(query_spec)
                source_results[source_id] = len(results)
                all_data.extend(results)
            except Exception:
                source_results[source_id] = 0
        
        # Apply join if specified
        if query.join_spec:
            all_data = self._apply_join(all_data, query.join_spec)
        
        duration_ms = (time.monotonic() - start_time) * 1000
        
        return QueryResult(
            query_id=query.id,
            data=all_data,
            source_results=source_results,
            duration_ms=duration_ms,
            timestamp=datetime.now()
        )
    
    def _apply_join(self, data: List[Dict], join_spec: Dict) -> List[Dict]:
        """Apply join operation to data."""
        join_type = join_spec.get("type", "inner")
        join_key = join_spec.get("on")
        
        if not join_key:
            return data
        
        # Group by join key
        groups: Dict[Any, List[Dict]] = {}
        for item in data:
            key_value = item.get(join_key)
            if key_value not in groups:
                groups[key_value] = []
            groups[key_value].append(item)
        
        # Combine groups
        results = []
        for key, items in groups.items():
            if len(items) > 1 or join_type == "left":
                # Create combined record
                combined = {}
                for item in items:
                    combined.update(item)
                results.append(combined)
            elif len(items) == 1 and join_type == "inner":
                results.append(items[0])
        
        return results
    
    def health_check(self) -> Dict[str, bool]:
        """Check health of all sources."""
        health = {}
        for source_id, connector in self.sources.items():
            health[source_id] = connector.health_check()
        return health


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataFederationAction(BaseAction):
    """
    Data federation action for multi-source queries.
    
    Parameters:
        operation: Operation type (register/execute/health)
        source_id: Source identifier
        source_type: Type of data source
        config: Source configuration
        query: Query definition
    
    Example:
        action = DataFederationAction()
        result = action.execute({}, {
            "operation": "register",
            "source_id": "users",
            "source_type": "memory",
            "config": {}
        })
    """
    
    _engine: Optional[DataFederationEngine] = None
    _lock = threading.Lock()
    
    def _get_engine(self) -> DataFederationEngine:
        """Get or create federation engine."""
        with self._lock:
            if self._engine is None:
                self._engine = DataFederationEngine()
            return self._engine
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute federation operation."""
        operation = params.get("operation", "register")
        engine = self._get_engine()
        
        if operation == "register":
            source_id = params.get("source_id")
            source_type_str = params.get("source_type", "memory")
            config = params.get("config", {})
            
            source_type = DataSourceType(source_type_str)
            
            engine.register_source(source_id, source_type, config)
            
            return {
                "success": True,
                "operation": "register",
                "source_id": source_id,
                "source_type": source_type_str,
                "registered_at": datetime.now().isoformat()
            }
        
        elif operation == "unregister":
            source_id = params.get("source_id")
            success = engine.unregister_source(source_id)
            
            return {
                "success": success,
                "operation": "unregister",
                "source_id": source_id
            }
        
        elif operation == "execute":
            query_id = params.get("query_id", str(datetime.now().timestamp()))
            sources = params.get("sources", [])
            join_spec = params.get("join", {})
            filter_spec = params.get("filter", {})
            projection = params.get("projection", [])
            
            query = FederatedQuery(
                id=query_id,
                sources=sources,
                join_spec=join_spec,
                filter_spec=filter_spec,
                projection=projection
            )
            
            result = engine.execute(query)
            
            return {
                "success": True,
                "operation": "execute",
                "query_id": query_id,
                "result_count": len(result.data),
                "source_results": result.source_results,
                "duration_ms": result.duration_ms,
                "executed_at": result.timestamp.isoformat()
            }
        
        elif operation == "health":
            health = engine.health_check()
            
            return {
                "success": True,
                "operation": "health",
                "health": health
            }
        
        elif operation == "load_data":
            source_id = params.get("source_id")
            data = params.get("data", [])
            
            if source_id in engine.sources:
                connector = engine.sources[source_id]
                if isinstance(connector, MemoryConnector):
                    connector.load_data(data)
                    return {
                        "success": True,
                        "operation": "load_data",
                        "source_id": source_id,
                        "records_loaded": len(data)
                    }
            
            return {
                "success": False,
                "error": "Source not found or not a memory connector"
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
