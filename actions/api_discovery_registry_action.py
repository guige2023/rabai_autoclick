"""API Discovery Registry Action Module.

Provides service discovery and registry management for API endpoints with:
- Dynamic registration/deregistration
- Health monitoring
- Service catalog
- Endpoint metadata management

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Service health status."""
    HEALTHY = auto()
    DEGRADED = auto()
    UNHEALTHY = auto()
    UNKNOWN = auto()


@dataclass
class ServiceEndpoint:
    """Service endpoint registration."""
    id: str
    name: str
    host: str
    port: int
    protocol: str = "http"
    version: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    health_url: Optional[str] = None
    status: HealthStatus = HealthStatus.UNKNOWN
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    weight: int = 100


@dataclass
class ServiceCatalog:
    """Service catalog entry."""
    name: str
    description: Optional[str] = None
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    tags: Set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class APIRegistry:
    """Service registry for API discovery and management.
    
    Features:
    - Dynamic service registration
    - Heartbeat-based health tracking
    - Service catalog management
    - Endpoint filtering and lookup
    - Health monitoring
    """
    
    def __init__(
        self,
        name: str = "default",
        heartbeat_interval: float = 30.0,
        health_check_interval: float = 60.0,
        unhealthy_threshold: int = 3
    ):
        self.name = name
        self.heartbeat_interval = heartbeat_interval
        self.health_check_interval = health_check_interval
        self.unhealthy_threshold = unhealthy_threshold
        self._services: Dict[str, ServiceCatalog] = {}
        self._endpoints: Dict[str, ServiceEndpoint] = {}
        self._health_tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._metrics = {
            "total_registrations": 0,
            "total_heartbeats": 0,
            "health_checks": 0,
            "unhealthy_detected": 0
        }
    
    async def register(
        self,
        service_name: str,
        host: str,
        port: int,
        protocol: str = "http",
        version: Optional[str] = None,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        health_url: Optional[str] = None
    ) -> str:
        """Register a service endpoint.
        
        Args:
            service_name: Service name
            host: Service host
            port: Service port
            protocol: Protocol (http/https)
            version: API version
            tags: Service tags
            metadata: Additional metadata
            health_url: Health check URL
            
        Returns:
            Endpoint ID
        """
        endpoint_id = f"{service_name}_{host}_{port}_{int(time.time() * 1000)}"
        
        endpoint = ServiceEndpoint(
            id=endpoint_id,
            name=service_name,
            host=host,
            port=port,
            protocol=protocol,
            version=version,
            tags=tags or set(),
            metadata=metadata or {},
            health_url=health_url
        )
        
        async with self._lock:
            if service_name not in self._services:
                self._services[service_name] = ServiceCatalog(
                    name=service_name,
                    endpoints=[]
                )
            
            self._services[service_name].endpoints.append(endpoint)
            self._services[service_name].updated_at = time.time()
            self._endpoints[endpoint_id] = endpoint
        
        self._metrics["total_registrations"] += 1
        
        logger.info(f"Registered endpoint: {endpoint_id}")
        
        if health_url and service_name not in self._health_tasks:
            self._health_tasks[service_name] = asyncio.create_task(
                self._monitor_service_health(service_name)
            )
        
        return endpoint_id
    
    async def deregister(self, endpoint_id: str) -> bool:
        """Deregister a service endpoint.
        
        Args:
            endpoint_id: Endpoint ID to deregister
            
        Returns:
            True if deregistered
        """
        async with self._lock:
            if endpoint_id not in self._endpoints:
                return False
            
            endpoint = self._endpoints[endpoint_id]
            service_name = endpoint.name
            
            if service_name in self._services:
                catalog = self._services[service_name]
                catalog.endpoints = [
                    e for e in catalog.endpoints if e.id != endpoint_id
                ]
                catalog.updated_at = time.time()
            
            del self._endpoints[endpoint_id]
            
            logger.info(f"Deregistered endpoint: {endpoint_id}")
            return True
    
    async def heartbeat(self, endpoint_id: str) -> bool:
        """Send heartbeat for an endpoint.
        
        Args:
            endpoint_id: Endpoint ID
            
        Returns:
            True if heartbeat recorded
        """
        async with self._lock:
            if endpoint_id not in self._endpoints:
                return False
            
            endpoint = self._endpoints[endpoint_id]
            endpoint.last_heartbeat = time.time()
            
            if endpoint.status == HealthStatus.UNKNOWN:
                endpoint.status = HealthStatus.HEALTHY
            
            self._metrics["total_heartbeats"] += 1
            return True
    
    async def discover(
        self,
        service_name: str,
        tags: Optional[Set[str]] = None,
        healthy_only: bool = False
    ) -> List[ServiceEndpoint]:
        """Discover service endpoints.
        
        Args:
            service_name: Service name to discover
            tags: Optional required tags
            healthy_only: Only return healthy endpoints
            
        Returns:
            List of matching endpoints
        """
        async with self._lock:
            if service_name not in self._services:
                return []
            
            endpoints = self._services[service_name].endpoints
            
            if tags:
                endpoints = [
                    e for e in endpoints
                    if tags.issubset(e.tags)
                ]
            
            if healthy_only:
                endpoints = [
                    e for e in endpoints
                    if e.status == HealthStatus.HEALTHY
                ]
            
            return list(endpoints)
    
    async def discover_all(
        self,
        tags: Optional[Set[str]] = None,
        healthy_only: bool = False
    ) -> Dict[str, List[ServiceEndpoint]]:
        """Discover all services matching criteria.
        
        Args:
            tags: Optional required tags
            healthy_only: Only return healthy endpoints
            
        Returns:
            Dict mapping service names to endpoints
        """
        async with self._lock:
            result = {}
            
            for service_name, catalog in self._services.items():
                endpoints = catalog.endpoints
                
                if tags:
                    endpoints = [
                        e for e in endpoints
                        if tags.issubset(e.tags)
                    ]
                
                if healthy_only:
                    endpoints = [
                        e for e in endpoints
                        if e.status == HealthStatus.HEALTHY
                    ]
                
                if endpoints:
                    result[service_name] = endpoints
            
            return result
    
    async def get_catalog(self, service_name: str) -> Optional[ServiceCatalog]:
        """Get service catalog entry.
        
        Args:
            service_name: Service name
            
        Returns:
            Service catalog or None
        """
        async with self._lock:
            return self._services.get(service_name)
    
    async def list_services(self) -> List[str]:
        """List all registered service names.
        
        Returns:
            List of service names
        """
        async with self._lock:
            return list(self._services.keys())
    
    async def update_endpoint_status(
        self,
        endpoint_id: str,
        status: HealthStatus
    ) -> bool:
        """Update endpoint health status.
        
        Args:
            endpoint_id: Endpoint ID
            status: New health status
            
        Returns:
            True if updated
        """
        async with self._lock:
            if endpoint_id not in self._endpoints:
                return False
            
            endpoint = self._endpoints[endpoint_id]
            endpoint.status = status
            
            if status != HealthStatus.HEALTHY:
                self._metrics["unhealthy_detected"] += 1
            
            return True
    
    async def _monitor_service_health(self, service_name: str) -> None:
        """Monitor service health.
        
        Args:
            service_name: Service to monitor
        """
        while self._running:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                async with self._lock:
                    if service_name not in self._services:
                        continue
                    
                    endpoints = self._services[service_name].endpoints
                
                for endpoint in endpoints:
                    if not endpoint.health_url:
                        continue
                    
                    try:
                        healthy = await self._check_health(endpoint)
                        
                        async with self._lock:
                            endpoint.status = (
                                HealthStatus.HEALTHY if healthy
                                else HealthStatus.UNHEALTHY
                            )
                        
                        self._metrics["health_checks"] += 1
                        
                    except Exception as e:
                        logger.error(f"Health check failed for {endpoint.id}: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error for {service_name}: {e}")
    
    async def _check_health(self, endpoint: ServiceEndpoint) -> bool:
        """Perform health check on endpoint.
        
        Args:
            endpoint: Endpoint to check
            
        Returns:
            True if healthy
        """
        import aiohttp
        
        if not endpoint.health_url:
            return True
        
        timeout = aiohttp.ClientTimeout(total=5)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"{endpoint.protocol}://{endpoint.host}:{endpoint.port}{endpoint.health_url}"
                async with session.get(url) as response:
                    return response.status < 500
        except Exception:
            return False
    
    async def start(self) -> None:
        """Start the registry."""
        self._running = True
        logger.info(f"API Registry '{self.name}' started")
    
    async def stop(self) -> None:
        """Stop the registry."""
        self._running = False
        
        for task in self._health_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._health_tasks.clear()
        logger.info(f"API Registry '{self.name}' stopped")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get registry metrics."""
        return {
            **self._metrics,
            "total_services": len(self._services),
            "total_endpoints": len(self._endpoints),
            "healthy_endpoints": sum(
                1 for e in self._endpoints.values()
                if e.status == HealthStatus.HEALTHY
            )
        }
