"""API Load Balancer V2 Action Module.

Provides advanced load balancing with weighted routing and health-aware distribution.
"""

import time
import random
import hashlib
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APILoadBalancerV2Action(BaseAction):
    """Advanced load balancer with multiple algorithms.
    
    Supports weighted round-robin, least connections, IP hash, and random routing.
    """
    action_type = "api_load_balancer_v2"
    display_name = "API负载均衡V2"
    description = "高级负载均衡算法支持"
    
    def __init__(self):
        super().__init__()
        self._connection_counts = {}
        self._weights = {}
        self._current_index = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route request using load balancer.
        
        Args:
            context: Execution context.
            params: Dict with keys: algorithm, backends, request_key, weights.
        
        Returns:
            ActionResult with selected backend.
        """
        algorithm = params.get('algorithm', 'round_robin')
        backends = params.get('backends', [])
        request_key = params.get('request_key', '')
        weights = params.get('weights', {})
        
        if not backends:
            return ActionResult(
                success=False,
                data=None,
                error="No backends configured"
            )
        
        selected_backend = self._select_backend(
            algorithm, backends, request_key, weights
        )
        
        return ActionResult(
            success=True,
            data={
                "backend": selected_backend,
                "algorithm": algorithm,
                "request_key": request_key
            },
            error=None
        )
    
    def _select_backend(
        self,
        algorithm: str,
        backends: List[Dict],
        request_key: str,
        weights: Dict
    ) -> Optional[Dict]:
        """Select backend based on algorithm."""
        if algorithm == 'round_robin':
            return self._round_robin(backends)
        elif algorithm == 'weighted_round_robin':
            return self._weighted_round_robin(backends, weights)
        elif algorithm == 'least_connections':
            return self._least_connections(backends)
        elif algorithm == 'ip_hash':
            return self._ip_hash(backends, request_key)
        elif algorithm == 'random':
            return self._random_backend(backends)
        elif algorithm == 'weighted_random':
            return self._weighted_random(backends, weights)
        else:
            return backends[0] if backends else None
    
    def _round_robin(self, backends: List[Dict]) -> Optional[Dict]:
        """Round-robin selection."""
        if not backends:
            return None
        idx = len(backends)
        return backends[idx % len(backends)]
    
    def _weighted_round_robin(self, backends: List[Dict], weights: Dict) -> Optional[Dict]:
        """Weighted round-robin selection."""
        if not backends:
            return None
        
        # Build weighted list
        weighted_list = []
        for backend in backends:
            backend_id = backend.get('id', '')
            weight = weights.get(backend_id, 1)
            weighted_list.extend([backend] * weight)
        
        if not weighted_list:
            return backends[0]
        
        return weighted_list[len(weighted_list) % len(weighted_list)]
    
    def _least_connections(self, backends: List[Dict]) -> Optional[Dict]:
        """Select backend with least connections."""
        if not backends:
            return None
        
        min_connections = float('inf')
        selected = backends[0]
        
        for backend in backends:
            backend_id = backend.get('id', '')
            conn_count = self._connection_counts.get(backend_id, 0)
            if conn_count < min_connections:
                min_connections = conn_count
                selected = backend
        
        return selected
    
    def _ip_hash(self, backends: List[Dict], request_key: str) -> Optional[Dict]:
        """IP hash-based selection."""
        if not backends:
            return None
        
        hash_value = int(hashlib.md5(request_key.encode()).hexdigest(), 16)
        idx = hash_value % len(backends)
        return backends[idx]
    
    def _random_backend(self, backends: List[Dict]) -> Optional[Dict]:
        """Random selection."""
        if not backends:
            return None
        return random.choice(backends)
    
    def _weighted_random(self, backends: List[Dict], weights: Dict) -> Optional[Dict]:
        """Weighted random selection."""
        if not backends:
            return None
        
        weighted_list = []
        for backend in backends:
            backend_id = backend.get('id', '')
            weight = weights.get(backend_id, 1)
            weighted_list.extend([backend] * weight)
        
        if not weighted_list:
            return backends[0]
        
        return random.choice(weighted_list)
    
    def record_connection(self, backend_id: str, increment: bool = True):
        """Record connection count for a backend."""
        if increment:
            self._connection_counts[backend_id] = self._connection_counts.get(backend_id, 0) + 1
        else:
            self._connection_counts[backend_id] = max(0, self._connection_counts.get(backend_id, 1) - 1)


class APIBackendPoolAction(BaseAction):
    """Manage backend server pool.
    
    Add, remove, and monitor backend servers in the pool.
    """
    action_type = "api_backend_pool"
    display_name = "API后端池管理"
    description = "管理后端服务器池"
    
    def __init__(self):
        super().__init__()
        self._pool = {}
        self._health_status = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage backend pool.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (add/remove/list/update), backend_info.
        
        Returns:
            ActionResult with pool status or operation result.
        """
        action = params.get('action', 'list')
        
        if action == 'add':
            return self._add_backend(params)
        elif action == 'remove':
            return self._remove_backend(params)
        elif action == 'list':
            return self._list_backends()
        elif action == 'update':
            return self._update_backend(params)
        elif action == 'health_check':
            return self._health_check_all()
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _add_backend(self, params: Dict) -> ActionResult:
        """Add backend to pool."""
        backend_info = params.get('backend_info', {})
        backend_id = backend_info.get('id', '')
        
        if not backend_id:
            return ActionResult(
                success=False,
                data=None,
                error="Backend ID required"
            )
        
        self._pool[backend_id] = backend_info
        self._health_status[backend_id] = {
            "status": "unknown",
            "last_check": None
        }
        
        return ActionResult(
            success=True,
            data={
                "action": "add",
                "backend_id": backend_id,
                "pool_size": len(self._pool)
            },
            error=None
        )
    
    def _remove_backend(self, params: Dict) -> ActionResult:
        """Remove backend from pool."""
        backend_id = params.get('backend_id', '')
        
        if backend_id not in self._pool:
            return ActionResult(
                success=False,
                data=None,
                error=f"Backend {backend_id} not found"
            )
        
        del self._pool[backend_id]
        if backend_id in self._health_status:
            del self._health_status[backend_id]
        
        return ActionResult(
            success=True,
            data={
                "action": "remove",
                "backend_id": backend_id,
                "pool_size": len(self._pool)
            },
            error=None
        )
    
    def _list_backends(self) -> ActionResult:
        """List all backends in pool."""
        return ActionResult(
            success=True,
            data={
                "pool": self._pool,
                "health_status": self._health_status,
                "pool_size": len(self._pool)
            },
            error=None
        )
    
    def _update_backend(self, params: Dict) -> ActionResult:
        """Update backend info."""
        backend_id = params.get('backend_id', '')
        backend_info = params.get('backend_info', {})
        
        if backend_id not in self._pool:
            return ActionResult(
                success=False,
                data=None,
                error=f"Backend {backend_id} not found"
            )
        
        self._pool[backend_id].update(backend_info)
        
        return ActionResult(
            success=True,
            data={
                "action": "update",
                "backend_id": backend_id
            },
            error=None
        )
    
    def _health_check_all(self) -> ActionResult:
        """Perform health check on all backends."""
        results = {}
        
        for backend_id, backend_info in self._pool.items():
            url = backend_info.get('health_url', backend_info.get('url', ''))
            is_healthy = self._check_backend_health(url)
            
            self._health_status[backend_id] = {
                "status": "healthy" if is_healthy else "unhealthy",
                "last_check": time.time()
            }
            results[backend_id] = is_healthy
        
        healthy_count = sum(1 for v in results.values() if v)
        
        return ActionResult(
            success=True,
            data={
                "total_backends": len(self._pool),
                "healthy_count": healthy_count,
                "unhealthy_count": len(self._pool) - healthy_count,
                "health_status": self._health_status
            },
            error=None
        )
    
    def _check_backend_health(self, url: str) -> bool:
        """Check if backend is healthy."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=5)
            return True
        except Exception:
            return False


def register_actions():
    """Register all API Load Balancer V2 actions."""
    return [
        APILoadBalancerV2Action,
        APIBackendPoolAction,
    ]
