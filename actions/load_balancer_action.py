"""Load balancer action module for RabAI AutoClick.

Provides load balancing across multiple endpoints with
health checking and failover support.
"""

import sys
import os
import time
import random
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_LOADED = "least_loaded"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"
    ADAPTIVE = "adaptive"


@dataclass
class Endpoint:
    """A backend endpoint."""
    id: str
    url: str
    weight: int = 1
    healthy: bool = True
    last_check: float = 0
    response_time: float = 0
    failures: int = 0
    requests: int = 0


class LoadBalancerAction(BaseAction):
    """Balance load across multiple backend endpoints.
    
    Supports multiple balancing strategies, health checking,
    automatic failover, and endpoint weighting.
    """
    action_type = "load_balancer"
    display_name = "负载均衡器"
    description = "多后端负载均衡和健康检查"
    
    def __init__(self):
        super().__init__()
        self._endpoints: Dict[str, Endpoint] = {}
        self._strategy = BalanceStrategy.ROUND_ROBIN
        self._round_robin_index: Dict[str, int] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute load balancer operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'register', 'deregister', 'select', 'health', 'status'
                - endpoint: Endpoint config (for register)
                - strategy: Balancing strategy (for register)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'select').lower()
        
        if operation == 'register':
            return self._register(params)
        elif operation == 'deregister':
            return self._deregister(params)
        elif operation == 'select':
            return self._select(params)
        elif operation == 'health':
            return self._health_check(params)
        elif operation == 'status':
            return self._status(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a backend endpoint."""
        endpoint_id = params.get('endpoint_id')
        url = params.get('url')
        weight = params.get('weight', 1)
        strategy = params.get('strategy', 'round_robin').lower()
        
        if not endpoint_id or not url:
            return ActionResult(
                success=False,
                message="endpoint_id and url are required"
            )
        
        endpoint = Endpoint(
            id=endpoint_id,
            url=url,
            weight=weight,
            last_check=time.time()
        )
        
        with self._lock:
            self._endpoints[endpoint_id] = endpoint
            if endpoint_id not in self._round_robin_index:
                self._round_robin_index[endpoint_id] = 0
        
        if strategy:
            try:
                self._strategy = BalanceStrategy(strategy)
            except ValueError:
                pass
        
        return ActionResult(
            success=True,
            message=f"Registered endpoint '{endpoint_id}'",
            data={'endpoint_id': endpoint_id, 'url': url}
        )
    
    def _deregister(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a backend endpoint."""
        endpoint_id = params.get('endpoint_id')
        
        with self._lock:
            if endpoint_id in self._endpoints:
                del self._endpoints[endpoint_id]
                return ActionResult(
                    success=True,
                    message=f"Removed endpoint '{endpoint_id}'"
                )
        
        return ActionResult(
            success=False,
            message=f"Endpoint '{endpoint_id}' not found"
        )
    
    def _select(self, params: Dict[str, Any]) -> ActionResult:
        """Select an endpoint using configured strategy."""
        client_ip = params.get('client_ip')
        
        with self._lock:
            healthy_endpoints = [
                e for e in self._endpoints.values() if e.healthy
            ]
        
        if not healthy_endpoints:
            return ActionResult(
                success=False,
                message="No healthy endpoints available"
            )
        
        selected = self._apply_strategy(
            healthy_endpoints,
            client_ip
        )
        
        # Update stats
        with self._lock:
            selected.requests += 1
        
        return ActionResult(
            success=True,
            message=f"Selected endpoint '{selected.id}'",
            data={
                'endpoint_id': selected.id,
                'url': selected.url,
                'strategy': self._strategy.value
            }
        )
    
    def _apply_strategy(
        self,
        endpoints: List[Endpoint],
        client_ip: Optional[str]
    ) -> Endpoint:
        """Apply load balancing strategy."""
        if self._strategy == BalanceStrategy.ROUND_ROBIN:
            # Simple round-robin across all endpoints
            index = 0
            for ep in endpoints:
                if ep.id in self._round_robin_index:
                    idx = self._round_robin_index[ep.id]
                    if idx > index:
                        index = idx
            # Select endpoint with highest index
            result = endpoints[0]
            max_idx = self._round_robin_index.get(result.id, 0)
            for ep in endpoints:
                idx = self._round_robin_index.get(ep.id, 0)
                if idx >= max_idx:
                    max_idx = idx
                    result = ep
            # Increment for next time
            self._round_robin_index[result.id] = max_idx + 1
            return result
        
        elif self._strategy == BalanceStrategy.RANDOM:
            return random.choice(endpoints)
        
        elif self._strategy == BalanceStrategy.LEAST_LOADED:
            return min(endpoints, key=lambda e: e.requests)
        
        elif self._strategy == BalanceStrategy.WEIGHTED:
            weighted = []
            for ep in endpoints:
                weighted.extend([ep] * ep.weight)
            return random.choice(weighted)
        
        elif self._strategy == BalanceStrategy.IP_HASH and client_ip:
            hash_val = sum(ord(c) for c in client_ip)
            index = hash_val % len(endpoints)
            return endpoints[index]
        
        elif self._strategy == BalanceStrategy.ADAPTIVE:
            # Choose by response time
            return min(endpoints, key=lambda e: e.response_time)
        
        return endpoints[0]
    
    def _health_check(self, params: Dict[str, Any]) -> ActionResult:
        """Perform health check on endpoints."""
        endpoint_id = params.get('endpoint_id')
        check_func = params.get('check_func')
        healthy_threshold = params.get('healthy_threshold', 1.0)
        
        results = {}
        
        with self._lock:
            endpoints_to_check = (
                [self._endpoints[endpoint_id]] if endpoint_id
                else list(self._endpoints.values())
            )
        
        for ep in endpoints_to_check:
            if check_func and callable(check_func):
                try:
                    start = time.time()
                    is_healthy = check_func(ep.url)
                    ep.response_time = (time.time() - start) * 1000
                    ep.healthy = is_healthy
                except Exception:
                    ep.healthy = False
                    ep.failures += 1
            else:
                # Default: mark as healthy if no failures
                ep.healthy = ep.failures < 3
            
            ep.last_check = time.time()
            results[ep.id] = ep.healthy
        
        return ActionResult(
            success=True,
            message="Health check complete",
            data={'results': results}
        )
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get load balancer status."""
        with self._lock:
            endpoints = [
                {
                    'id': e.id,
                    'url': e.url,
                    'healthy': e.healthy,
                    'weight': e.weight,
                    'requests': e.requests,
                    'failures': e.failures,
                    'response_time': round(e.response_time, 2),
                    'last_check': e.last_check
                }
                for e in self._endpoints.values()
            ]
        
        return ActionResult(
            success=True,
            message=f"{len(endpoints)} endpoints",
            data={
                'endpoints': endpoints,
                'strategy': self._strategy.value,
                'healthy_count': sum(1 for e in endpoints if e['healthy'])
            }
        )


class FailoverAction(BaseAction):
    """Handle failover between endpoints."""
    action_type = "failover"
    display_name = "故障转移"
    description = "后端故障自动转移"
    
    def __init__(self):
        super().__init__()
        self._primary: Optional[str] = None
        self._backups: List[str] = []
        self._current: Optional[str] = None
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute failover operation."""
        operation = params.get('operation', 'failover').lower()
        
        if operation == 'setup':
            return self._setup(params)
        elif operation == 'failover':
            return self._do_failover(params)
        elif operation == 'reset':
            return self._reset(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _setup(self, params: Dict[str, Any]) -> ActionResult:
        """Setup primary and backup endpoints."""
        primary = params.get('primary')
        backups = params.get('backups', [])
        
        if not primary:
            return ActionResult(success=False, message="primary is required")
        
        self._primary = primary
        self._backups = backups
        self._current = primary
        
        return ActionResult(
            success=True,
            message=f"Setup complete: primary={primary}, backups={backups}"
        )
    
    def _do_failover(self, params: Dict[str, Any]) -> ActionResult:
        """Perform failover to next backup."""
        if not self._backups:
            return ActionResult(
                success=False,
                message="No backup endpoints available"
            )
        
        failed_endpoint = params.get('failed_endpoint', self._current)
        
        if failed_endpoint == self._primary and self._backups:
            self._current = self._backups[0]
            self._backups = self._backups[1:]
            self._backups.append(self._primary)
            return ActionResult(
                success=True,
                message=f"Failed over to {self._current}",
                data={'current': self._current}
            )
        
        return ActionResult(
            success=False,
            message="No failover target available"
        )
    
    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset to primary."""
        if self._primary:
            self._current = self._primary
            return ActionResult(
                success=True,
                message=f"Reset to primary {self._primary}",
                data={'current': self._current}
            )
        
        return ActionResult(
            success=False,
            message="No primary configured"
        )
