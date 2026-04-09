"""
API Load Balancing Action Module.

Provides load balancing capabilities for API requests including multiple
balancing strategies, health-aware routing, and session affinity.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import random
import hashlib
from datetime import datetime
from collections import defaultdict


class BalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    CONSISTENT_HASH = "consistent_hash"


@dataclass
class Server:
    """Represents a backend server."""
    id: str
    host: str
    port: int
    weight: int = 1
    max_connections: int = 1000
    current_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    last_check: datetime = field(default_factory=datetime.now)
    healthy: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Request:
    """Represents a load-balanced request."""
    id: str
    client_ip: str
    path: str
    method: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoadBalancerConfig:
    """Configuration for load balancer."""
    strategy: BalancingStrategy = BalancingStrategy.ROUND_ROBIN
    health_check_interval: float = 30.0
    health_check_timeout: float = 5.0
    max_retries: int = 3
    connection_timeout: float = 10.0
    read_timeout: float = 30.0


class ServerPool:
    """Manages a pool of backend servers."""
    
    def __init__(self):
        self.servers: Dict[str, Server] = {}
        self._lock = threading.RLock()
    
    def add_server(
        self,
        server_id: str,
        host: str,
        port: int,
        weight: int = 1,
        **metadata
    ) -> Server:
        """Add a server to the pool."""
        with self._lock:
            server = Server(
                id=server_id,
                host=host,
                port=port,
                weight=weight,
                metadata=metadata
            )
            self.servers[server_id] = server
            return server
    
    def remove_server(self, server_id: str) -> bool:
        """Remove a server from the pool."""
        with self._lock:
            if server_id in self.servers:
                del self.servers[server_id]
                return True
            return False
    
    def get_server(self, server_id: str) -> Optional[Server]:
        """Get a server by ID."""
        with self._lock:
            return self.servers.get(server_id)
    
    def get_all_servers(self) -> List[Server]:
        """Get all servers."""
        with self._lock:
            return list(self.servers.values())
    
    def get_healthy_servers(self) -> List[Server]:
        """Get all healthy servers."""
        with self._lock:
            return [s for s in self.servers.values() if s.healthy]
    
    def update_server_health(self, server_id: str, healthy: bool):
        """Update server health status."""
        with self._lock:
            if server_id in self.servers:
                self.servers[server_id].healthy = healthy
                self.servers[server_id].last_check = datetime.now()
    
    def record_request(self, server_id: str, response_time: float, success: bool):
        """Record request metrics for a server."""
        with self._lock:
            if server_id not in self.servers:
                return
            
            server = self.servers[server_id]
            server.total_requests += 1
            server.current_connections += 1
            
            if not success:
                server.failed_requests += 1
            
            # Update average response time
            n = server.total_requests
            server.avg_response_time = (
                (server.avg_response_time * (n - 1) + response_time) / n
            )
    
    def release_connection(self, server_id: str):
        """Release a connection from a server."""
        with self._lock:
            if server_id in self.servers:
                if self.servers[server_id].current_connections > 0:
                    self.servers[server_id].current_connections -= 1


class LoadBalancer:
    """
    Main load balancer implementation.
    
    Example:
        lb = LoadBalancer(strategy=BalancingStrategy.ROUND_ROBIN)
        lb.add_server("server1", "192.168.1.1", 8000, weight=2)
        lb.add_server("server2", "192.168.1.2", 8000, weight=1)
        
        server = lb.select_server(client_ip="192.168.1.100")
        response = forward_request(server, request)
        lb.record_result(server.id, response)
    """
    
    def __init__(self, config: Optional[LoadBalancerConfig] = None):
        self.config = config or LoadBalancerConfig()
        self.pool = ServerPool()
        
        self._round_robin_counters: Dict[str, int] = defaultdict(int)
        self._hash_ring: Dict[int, str] = {}
        self._ring_lock = threading.Lock()
        
        self._initialize_hash_ring()
    
    def add_server(
        self,
        server_id: str,
        host: str,
        port: int,
        weight: int = 1,
        **metadata
    ) -> "LoadBalancer":
        """Add a server to the load balancer."""
        self.pool.add_server(server_id, host, port, weight, **metadata)
        self._update_hash_ring()
        return self
    
    def remove_server(self, server_id: str) -> bool:
        """Remove a server from the load balancer."""
        result = self.pool.remove_server(server_id)
        self._update_hash_ring()
        return result
    
    def select_server(
        self,
        client_ip: Optional[str] = None,
        request_path: Optional[str] = None
    ) -> Optional[Server]:
        """Select a server based on the balancing strategy."""
        healthy = self.pool.get_healthy_servers()
        
        if not healthy:
            return None
        
        strategy = self.config.strategy
        
        if strategy == BalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        elif strategy == BalancingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin(healthy)
        elif strategy == BalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy)
        elif strategy == BalancingStrategy.LEAST_RESPONSE_TIME:
            return self._least_response_time(healthy)
        elif strategy == BalancingStrategy.IP_HASH:
            return self._ip_hash(healthy, client_ip or "")
        elif strategy == BalancingStrategy.RANDOM:
            return self._random(healthy)
        elif strategy == BalancingStrategy.CONSISTENT_HASH:
            return self._consistent_hash(request_path or "")
        else:
            return healthy[0]
    
    def _round_robin(self, servers: List[Server]) -> Server:
        """Round robin selection."""
        server = servers[0]
        idx = self._round_robin_counters[server.id] % len(servers)
        self._round_robin_counters[server.id] += 1
        return servers[idx]
    
    def _weighted_round_robin(self, servers: List[Server]) -> Server:
        """Weighted round robin selection."""
        total_weight = sum(s.weight for s in servers)
        r = random.randint(1, total_weight)
        
        cumsum = 0
        for server in servers:
            cumsum += server.weight
            if r <= cumsum:
                return server
        
        return servers[-1]
    
    def _least_connections(self, servers: List[Server]) -> Server:
        """Select server with least connections."""
        return min(servers, key=lambda s: s.current_connections)
    
    def _least_response_time(self, servers: List[Server]) -> Server:
        """Select server with least response time."""
        return min(servers, key=lambda s: s.avg_response_time)
    
    def _ip_hash(self, servers: List[Server], client_ip: str) -> Server:
        """IP hash based selection."""
        hash_value = int(hashlib.md5(client_ip.encode()).hexdigest())
        idx = hash_value % len(servers)
        return servers[idx]
    
    def _random(self, servers: List[Server]) -> Server:
        """Random selection."""
        return random.choice(servers)
    
    def _consistent_hash(self, key: str) -> Server:
        """Consistent hash selection."""
        with self._ring_lock:
            if not self._hash_ring:
                return random.choice(servers) if servers else None
            
            hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
            
            # Find nearest server in ring
            keys = sorted(self._hash_ring.keys())
            for k in keys:
                if k >= hash_value:
                    server_id = self._hash_ring[k]
                    return self.pool.get_server(server_id)
            
            # Wrap around
            if keys:
                server_id = self._hash_ring[keys[0]]
                return self.pool.get_server(server_id)
            
            return None
    
    def _initialize_hash_ring(self):
        """Initialize consistent hash ring."""
        self._update_hash_ring()
    
    def _update_hash_ring(self):
        """Update the consistent hash ring."""
        with self._ring_lock:
            self._hash_ring.clear()
            
            for server in self.pool.get_all_servers():
                # Add multiple points for better distribution
                for i in range(server.weight * 10):
                    key = f"{server.id}:{i}"
                    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
                    self._hash_ring[hash_value] = server.id
    
    def record_result(
        self,
        server_id: str,
        response_time: float,
        success: bool
    ):
        """Record the result of a request."""
        self.pool.record_request(server_id, response_time, success)
    
    def release(self, server_id: str):
        """Release a connection to a server."""
        self.pool.release_connection(server_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        servers = self.pool.get_all_servers()
        
        return {
            "total_servers": len(servers),
            "healthy_servers": len(self.pool.get_healthy_servers()),
            "strategy": self.config.strategy.value,
            "servers": [
                {
                    "id": s.id,
                    "host": s.host,
                    "port": s.port,
                    "healthy": s.healthy,
                    "connections": s.current_connections,
                    "total_requests": s.total_requests,
                    "avg_response_time": s.avg_response_time,
                    "error_rate": s.failed_requests / max(1, s.total_requests)
                }
                for s in servers
            ]
        }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APILoadBalancingAction(BaseAction):
    """
    Load balancing action for API routing.
    
    Parameters:
        operation: Operation type (add_server/remove_server/select/stats)
        server_id: Server identifier
        host: Server host
        port: Server port
        strategy: Balancing strategy
    
    Example:
        action = APILoadBalancingAction()
        result = action.execute({}, {
            "operation": "add_server",
            "server_id": "server1",
            "host": "192.168.1.1",
            "port": 8000
        })
    """
    
    _balancer: Optional[LoadBalancer] = None
    _lock = threading.Lock()
    
    def _get_balancer(self) -> LoadBalancer:
        """Get or create load balancer."""
        with self._lock:
            if self._balancer is None:
                config = LoadBalancerConfig()
                self._balancer = LoadBalancer(config)
            return self._balancer
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute load balancing operation."""
        operation = params.get("operation", "add_server")
        balancer = self._get_balancer()
        
        if operation == "add_server":
            server_id = params.get("server_id")
            host = params.get("host")
            port = params.get("port", 8000)
            weight = params.get("weight", 1)
            
            balancer.add_server(server_id, host, port, weight)
            
            return {
                "success": True,
                "operation": "add_server",
                "server_id": server_id,
                "host": host,
                "port": port,
                "added_at": datetime.now().isoformat()
            }
        
        elif operation == "remove_server":
            server_id = params.get("server_id")
            success = balancer.remove_server(server_id)
            
            return {
                "success": success,
                "operation": "remove_server",
                "server_id": server_id
            }
        
        elif operation == "select":
            client_ip = params.get("client_ip")
            request_path = params.get("path")
            
            server = balancer.select_server(client_ip, request_path)
            
            if server:
                return {
                    "success": True,
                    "operation": "select",
                    "server_id": server.id,
                    "host": server.host,
                    "port": server.port
                }
            
            return {
                "success": False,
                "operation": "select",
                "error": "No healthy server available"
            }
        
        elif operation == "record":
            server_id = params.get("server_id")
            response_time = params.get("response_time", 0)
            success = params.get("success", True)
            
            balancer.record_result(server_id, response_time, success)
            
            return {
                "success": True,
                "operation": "record",
                "server_id": server_id
            }
        
        elif operation == "release":
            server_id = params.get("server_id")
            balancer.release(server_id)
            
            return {
                "success": True,
                "operation": "release",
                "server_id": server_id
            }
        
        elif operation == "stats":
            stats = balancer.get_stats()
            
            return {
                "success": True,
                "operation": "stats",
                "stats": stats
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
