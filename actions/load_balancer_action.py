"""
Load Balancer Action Module.

Provides intelligent request distribution across multiple backends
with strategies including round-robin, least connections, and weighted routing.
"""

from typing import Optional, Dict, List, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    ADAPTIVE = "adaptive"


class BackendHealth(Enum):
    """Health status of a backend."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"


@dataclass
class Backend:
    """Represents a backend server."""
    host: str
    port: int
    weight: int = 1
    max_connections: int = 1000
    health: BackendHealth = BackendHealth.HEALTHY
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"
        
    @property
    def is_available(self) -> bool:
        return self.health in (BackendHealth.HEALTHY, BackendHealth.DEGRADED)


@dataclass
class BackendMetrics:
    """Metrics for a backend server."""
    requests_total: int = 0
    requests_active: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0
    latency_sum: float = 0.0
    latency_count: int = 0
    last_request_time: float = 0.0
    last_error_time: float = 0.0
    consecutive_failures: int = 0
    
    @property
    def avg_latency(self) -> float:
        return self.latency_sum / self.latency_count if self.latency_count > 0 else 0
        
    @property
    def success_rate(self) -> float:
        total = self.errors + self.requests_total
        return (total - self.errors) / total if total > 0 else 1.0


@dataclass
class LoadBalancerConfig:
    """Configuration for load balancer."""
    strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN
    health_check_interval: float = 30.0
    health_check_timeout: float = 5.0
    max_failures: int = 3
    recovery_threshold: int = 2
    connection_timeout: float = 10.0
    idle_timeout: float = 120.0
    enable_health_checks: bool = True


class LoadBalancer:
    """
    Load balancer for distributing requests across backends.
    
    Example:
        lb = LoadBalancer(strategy=LoadBalanceStrategy.ROUND_ROBIN)
        
        lb.add_backend("server1.example.com", 8080, weight=2)
        lb.add_backend("server2.example.com", 8080, weight=1)
        
        backend = lb.select_backend()
        response = lb.forward_request(backend, request)
    """
    
    def __init__(self, config: Optional[LoadBalancerConfig] = None):
        self.config = config or LoadBalancerConfig()
        self.backends: Dict[str, Backend] = {}
        self.metrics: Dict[str, BackendMetrics] = {}
        self._lock = threading.RLock()
        self._round_robin_counters: Dict[str, int] = defaultdict(int)
        self._active_backends: List[str] = []
        
    def add_backend(
        self,
        host: str,
        port: int,
        weight: int = 1,
        max_connections: int = 1000,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a backend server.
        
        Args:
            host: Backend hostname
            port: Backend port
            weight: Weight for weighted routing
            max_connections: Max concurrent connections
            metadata: Optional metadata
            
        Returns:
            Backend identifier
        """
        backend = Backend(
            host=host,
            port=port,
            weight=weight,
            max_connections=max_connections,
            metadata=metadata or {},
        )
        
        with self._lock:
            self.backends[backend.address] = backend
            self.metrics[backend.address] = BackendMetrics()
            self._update_active_backends()
            
        logger.info(f"Added backend: {backend.address}")
        return backend.address
        
    def remove_backend(self, address: str) -> bool:
        """Remove a backend from the load balancer."""
        with self._lock:
            if address in self.backends:
                self.backends[address].health = BackendHealth.DRAINING
                del self.backends[address]
                self._update_active_backends()
                logger.info(f"Removed backend: {address}")
                return True
        return False
        
    def get_backend(self, address: str) -> Optional[Backend]:
        """Get backend by address."""
        return self.backends.get(address)
        
    def select_backend(
        self,
        request_hash: Optional[int] = None,
        client_ip: Optional[str] = None,
    ) -> Optional[Backend]:
        """
        Select a backend based on configured strategy.
        
        Args:
            request_hash: Optional hash for consistent hashing
            client_ip: Client IP for IP hash strategy
            
        Returns:
            Selected Backend or None if no backends available
        """
        with self._lock:
            if not self._active_backends:
                return None
                
            if self.config.strategy == LoadBalanceStrategy.ROUND_ROBIN:
                return self._select_round_robin()
            elif self.config.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
                return self._select_least_connections()
            elif self.config.strategy == LoadBalanceStrategy.WEIGHTED:
                return self._select_weighted()
            elif self.config.strategy == LoadBalanceStrategy.IP_HASH:
                return self._select_ip_hash(client_ip or "")
            elif self.config.strategy == LoadBalanceStrategy.RANDOM:
                return self._select_random()
            elif self.config.strategy == LoadBalanceStrategy.ADAPTIVE:
                return self._select_adaptive()
                
            return self._active_backends[0]
            
    def _select_round_robin(self) -> Backend:
        """Round-robin selection."""
        available = [addr for addr in self._active_backends 
                     if self.backends[addr].is_available]
        if not available:
            return None
            
        idx = self._round_robin_counters["rr"] % len(available)
        self._round_robin_counters["rr"] += 1
        return self.backends[available[idx]]
        
    def _select_least_connections(self) -> Backend:
        """Select backend with least active connections."""
        available = [(addr, self.metrics[addr].requests_active) 
                      for addr in self._active_backends 
                      if self.backends[addr].is_available]
        if not available:
            return None
        return self.backends[min(available, key=lambda x: x[1])[0]]
        
    def _select_weighted(self) -> Backend:
        """Weighted selection based on backend weights."""
        available = [addr for addr in self._active_backends 
                     if self.backends[addr].is_available]
        if not available:
            return None
            
        weights = [self.backends[addr].weight for addr in available]
        total_weight = sum(weights)
        
        import random
        r = random.randint(1, total_weight)
        cumsum = 0
        for i, w in enumerate(weights):
            cumsum += w
            if r <= cumsum:
                return self.backends[available[i]]
                
        return self.backends[available[-1]]
        
    def _select_ip_hash(self, client_ip: str) -> Backend:
        """IP-based consistent hash selection."""
        if not self._active_backends:
            return None
            
        hash_value = hash(client_ip)
        idx = hash_value % len(self._active_backends)
        return self.backends[self._active_backends[idx]]
        
    def _select_random(self) -> Backend:
        """Random selection."""
        import random
        return self.backends[random.choice(self._active_backends)]
        
    def _select_adaptive(self) -> Backend:
        """Adaptive selection based on health and latency."""
        available = [addr for addr in self._active_backends 
                     if self.backends[addr].is_available]
        if not available:
            return None
            
        scores = {}
        for addr in available:
            backend = self.backends[addr]
            m = self.metrics[addr]
            
            health_score = 1.0 if backend.health == BackendHealth.HEALTHY else 0.5
            latency_score = max(0, 1.0 - m.avg_latency / 5.0)
            success_score = m.success_rate
            
            scores[addr] = (health_score * 0.4 + latency_score * 0.3 + success_score * 0.3)
            
        return self.backends[max(scores, key=scores.get)]
        
    def _update_active_backends(self) -> None:
        """Update list of active backend addresses."""
        self._active_backends = [
            addr for addr, backend in self.backends.items()
            if backend.is_available
        ]
        
    def record_request_start(self, address: str) -> None:
        """Record start of request to backend."""
        with self._lock:
            if address in self.metrics:
                self.metrics[address].requests_active += 1
                self.metrics[address].requests_total += 1
                self.metrics[address].last_request_time = time.time()
                
    def record_request_end(
        self,
        address: str,
        latency: float,
        bytes_sent: int = 0,
        bytes_received: int = 0,
        error: bool = False,
    ) -> None:
        """Record end of request to backend."""
        with self._lock:
            if address not in self.metrics:
                return
                
            m = self.metrics[address]
            m.requests_active = max(0, m.requests_active - 1)
            m.latency_sum += latency
            m.latency_count += 1
            m.bytes_sent += bytes_sent
            m.bytes_received += bytes_received
            
            if error:
                m.errors += 1
                m.consecutive_failures += 1
                m.last_error_time = time.time()
                
                if m.consecutive_failures >= self.config.max_failures:
                    if address in self.backends:
                        self.backends[address].health = BackendHealth.UNHEALTHY
                        self._update_active_backends()
            else:
                m.consecutive_failures = 0
                
    def record_health_check(
        self,
        address: str,
        healthy: bool,
        latency: Optional[float] = None,
    ) -> None:
        """Record health check result."""
        with self._lock:
            if address not in self.metrics:
                return
                
            m = self.metrics[address]
            backend = self.backends.get(address)
            
            if backend and healthy:
                if backend.health == BackendHealth.UNHEALTHY:
                    m.consecutive_failures = 0
                    backend.health = BackendHealth.HEALTHY
                    self._update_active_backends()
            elif not healthy:
                m.consecutive_failures += 1
                if backend and m.consecutive_failures >= self.config.max_failures:
                    backend.health = BackendHealth.UNHEALTHY
                    self._update_active_backends()
                    
    def get_backend_stats(self, address: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a backend."""
        if address not in self.metrics:
            return None
            
        m = self.metrics[address]
        backend = self.backends.get(address)
        
        return {
            "address": address,
            "health": backend.health.value if backend else "unknown",
            "requests_total": m.requests_total,
            "requests_active": m.requests_active,
            "avg_latency": m.avg_latency,
            "success_rate": m.success_rate,
            "consecutive_failures": m.consecutive_failures,
            "last_request_time": m.last_request_time,
        }
        
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all backends."""
        return {
            "strategy": self.config.strategy.value,
            "backends": {
                addr: self.get_backend_stats(addr)
                for addr in self.backends
            },
            "total_active_backends": len(self._active_backends),
        }


class LoadBalancerPool:
    """
    Pool of load balancers for different services.
    
    Example:
        pool = LoadBalancerPool()
        
        pool.create("api", strategy=LoadBalanceStrategy.WEIGHTED)
        pool.get("api").add_backend("server1", 8080)
    """
    
    def __init__(self):
        self._balancers: Dict[str, LoadBalancer] = {}
        self._lock = threading.Lock()
        
    def create(
        self,
        name: str,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        config: Optional[LoadBalancerConfig] = None,
    ) -> LoadBalancer:
        """Create a new load balancer in the pool."""
        with self._lock:
            if name in self._balancers:
                return self._balancers[name]
                
            if config:
                cfg = config
            else:
                cfg = LoadBalancerConfig(strategy=strategy)
                
            self._balancers[name] = LoadBalancer(cfg)
            return self._balancers[name]
            
    def get(self, name: str) -> Optional[LoadBalancer]:
        """Get load balancer by name."""
        return self._balancers.get(name)
        
    def remove(self, name: str) -> bool:
        """Remove a load balancer from the pool."""
        with self._lock:
            if name in self._balancers:
                del self._balancers[name]
                return True
        return False
        
    def get_all_stats(self) -> Dict[str, Any]:
        """Get stats for all load balancers."""
        return {
            name: lb.get_all_stats()
            for name, lb in self._balancers.items()
        }
