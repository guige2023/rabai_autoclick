"""
API Federation Action Module

Provides API federation, cross-service queries, and unified API gateway.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio


class FederationStrategy(Enum):
    """Federation strategies."""
    DIRECT = "direct"
    CACHED = "cached"
    AGGREGATED = "aggregated"
    FAN_OUT = "fan_out"


@dataclass
class FederatedService:
    """A service in the federation."""
    service_id: str
    name: str
    endpoint: str
    priority: int = 0
    healthy: bool = True
    latency_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FederationQuery:
    """A query to federated services."""
    query_id: str
    services: list[str]
    endpoint: str
    parameters: dict[str, Any] = field(default_factory=dict)
    strategy: FederationStrategy = FederationStrategy.DIRECT


@dataclass
class FederationResult:
    """Result from federated query."""
    service_id: str
    success: bool
    data: Any = None
    latency_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class AggregatedResult:
    """Aggregated result from multiple services."""
    query_id: str
    results: list[FederationResult]
    aggregated_data: Any
    total_latency_ms: float
    successful_services: int
    failed_services: int


class ApiFederationAction:
    """Main API federation action handler."""
    
    def __init__(self):
        self._services: dict[str, FederatedService] = {}
        self._query_cache: dict[str, tuple[Any, datetime]] = {}
        self._cache_ttl_seconds = 300
        self._stats: dict[str, Any] = {}
    
    def add_service(self, service: FederatedService) -> "ApiFederationAction":
        """Add a service to the federation."""
        self._services[service.service_id] = service
        return self
    
    def remove_service(self, service_id: str) -> bool:
        """Remove a service from the federation."""
        if service_id in self._services:
            del self._services[service_id]
            return True
        return False
    
    async def execute_query(
        self,
        query: FederationQuery
    ) -> AggregatedResult:
        """
        Execute a federated query.
        
        Args:
            query: FederationQuery with target services and parameters
            
        Returns:
            AggregatedResult with combined results
        """
        start_time = datetime.now()
        
        # Check cache first
        if query.strategy == FederationStrategy.CACHED:
            cache_key = self._get_cache_key(query)
            if cache_key in self._query_cache:
                cached_data, cached_at = self._query_cache[cache_key]
                age = (datetime.now() - cached_at).total_seconds()
                if age < self._cache_ttl_seconds:
                    return AggregatedResult(
                        query_id=query.query_id,
                        results=[],
                        aggregated_data=cached_data,
                        total_latency_ms=0,
                        successful_services=0,
                        failed_services=0
                    )
        
        # Execute based on strategy
        if query.strategy == FederationStrategy.DIRECT:
            results = await self._execute_direct(query)
        elif query.strategy == FederationStrategy.FAN_OUT:
            results = await self._execute_fan_out(query)
        elif query.strategy == FederationStrategy.AGGREGATED:
            results = await self._execute_aggregated(query)
        else:
            results = await self._execute_direct(query)
        
        # Aggregate results
        aggregated = self._aggregate_results(results)
        
        # Cache if applicable
        if query.strategy == FederationStrategy.CACHED:
            self._query_cache[self._get_cache_key(query)] = (aggregated, datetime.now())
        
        total_latency = (datetime.now() - start_time).total_seconds() * 1000
        
        return AggregatedResult(
            query_id=query.query_id,
            results=results,
            aggregated_data=aggregated,
            total_latency_ms=total_latency,
            successful_services=len([r for r in results if r.success]),
            failed_services=len([r for r in results if not r.success])
        )
    
    async def _execute_direct(self, query: FederationQuery) -> list[FederationResult]:
        """Execute query on first available service."""
        results = []
        
        for service_id in query.services:
            service = self._services.get(service_id)
            if not service or not service.healthy:
                continue
            
            result = await self._call_service(service, query)
            results.append(result)
            
            if result.success:
                break
        
        return results
    
    async def _execute_fan_out(self, query: FederationQuery) -> list[FederationResult]:
        """Execute query on all services in parallel."""
        tasks = []
        
        for service_id in query.services:
            service = self._services.get(service_id)
            if service and service.healthy:
                tasks.append(self._call_service(service, query))
        
        if not tasks:
            return []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return [
            r if isinstance(r, FederationResult) else FederationResult(
                service_id="unknown",
                success=False,
                error=str(r)
            )
            for r in results
        ]
    
    async def _execute_aggregated(self, query: FederationQuery) -> list[FederationResult]:
        """Execute and aggregate results from all services."""
        # Similar to fan_out but results will be merged
        return await self._execute_fan_out(query)
    
    async def _call_service(
        self,
        service: FederatedService,
        query: FederationQuery
    ) -> FederationResult:
        """Call a federated service."""
        start_time = datetime.now()
        
        try:
            # Simulate service call
            await asyncio.sleep(service.latency_ms / 1000)
            
            return FederationResult(
                service_id=service.service_id,
                success=True,
                data={"result": "ok"},
                latency_ms=(datetime.now() - start_time).total_seconds() * 1000
            )
            
        except Exception as e:
            return FederationResult(
                service_id=service.service_id,
                success=False,
                error=str(e),
                latency_ms=(datetime.now() - start_time).total_seconds() * 1000
            )
    
    def _aggregate_results(self, results: list[FederationResult]) -> Any:
        """Aggregate results from multiple services."""
        successful = [r for r in results if r.success]
        
        if not successful:
            return None
        
        # Simple aggregation: combine all successful results
        aggregated = {
            "results": [r.data for r in successful],
            "count": len(successful),
            "sources": [r.service_id for r in successful]
        }
        
        return aggregated
    
    def _get_cache_key(self, query: FederationQuery) -> str:
        """Generate cache key for query."""
        import hashlib
        import json
        
        content = json.dumps({
            "services": query.services,
            "endpoint": query.endpoint,
            "parameters": query.parameters
        }, sort_keys=True)
        
        return hashlib.md5(content.encode()).hexdigest()
    
    async def health_check_services(self) -> dict[str, bool]:
        """Run health checks on all services."""
        results = {}
        
        for service_id, service in self._services.items():
            try:
                # Simulate health check
                await asyncio.sleep(0.01)
                service.healthy = True
                results[service_id] = True
            except:
                service.healthy = False
                results[service_id] = False
        
        return results
    
    def get_service_status(self, service_id: str) -> Optional[dict[str, Any]]:
        """Get status of a federated service."""
        service = self._services.get(service_id)
        if not service:
            return None
        
        return {
            "service_id": service.service_id,
            "name": service.name,
            "endpoint": service.endpoint,
            "healthy": service.healthy,
            "latency_ms": service.latency_ms,
            "priority": service.priority
        }
    
    def list_services(self, healthy_only: bool = False) -> list[dict[str, Any]]:
        """List all federated services."""
        services = list(self._services.values())
        
        if healthy_only:
            services = [s for s in services if s.healthy]
        
        return [
            {
                "service_id": s.service_id,
                "name": s.name,
                "endpoint": s.endpoint,
                "healthy": s.healthy,
                "latency_ms": s.latency_ms
            }
            for s in services
        ]
    
    def get_stats(self) -> dict[str, Any]:
        """Get federation statistics."""
        return {
            "total_services": len(self._services),
            "healthy_services": len([s for s in self._services.values() if s.healthy]),
            "cached_queries": len(self._query_cache),
            **dict(self._stats)
        }
