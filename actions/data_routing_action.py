"""Data routing action module for RabAI AutoClick.

Provides data routing capabilities:
- DataRouter: Route data based on rules
- ConditionalRouter: Route based on conditions
- ContentBasedRouter: Route based on data content
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Set
import time
import threading
import logging
import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RoutingStrategy(Enum):
    """Routing strategies."""
    EXACT = "exact"
    PREFIX = "prefix"
    PATTERN = "pattern"
    CONTENT = "content"
    HASH = "hash"
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"


@dataclass
class Route:
    """Route definition."""
    name: str
    target: str
    handler: Optional[Callable] = None
    condition: Optional[Callable] = None
    pattern: Optional[str] = None
    priority: int = 0
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataRoutingConfig:
    """Configuration for data routing."""
    default_route: Optional[str] = None
    routing_strategy: RoutingStrategy = RoutingStrategy.EXACT
    case_sensitive: bool = True
    log_routing: bool = True
    fallback_to_default: bool = True


class DataRouter:
    """Route data to targets based on rules."""
    
    def __init__(self, name: str, config: Optional[DataRoutingConfig] = None):
        self.name = name
        self.config = config or DataRoutingConfig()
        self._routes: Dict[str, Route] = {}
        self._route_patterns: List[Tuple[re.Pattern, Route]] = []
        self._route_conditions: List[Tuple[Callable, Route]] = []
        self._hash_ring: Dict[int, str] = {}
        self._ring_keys: List[int] = []
        self._round_robin_index: int = 0
        self._total_weight: float = 0.0
        self._lock = threading.RLock()
        self._stats = {"total_routes": 0, "total_matches": 0, "no_match": 0}
    
    def add_route(self, route: Route):
        """Add a route."""
        with self._lock:
            self._routes[route.name] = route
            
            if route.pattern:
                flags = 0 if self.config.case_sensitive else re.IGNORECASE
                compiled = re.compile(route.pattern, flags)
                self._route_patterns.append((compiled, route))
                self._route_patterns.sort(key=lambda x: -x[1].priority)
            
            if route.condition:
                self._route_conditions.append((route.condition, route))
                self._route_conditions.sort(key=lambda x: -x[1].priority)
            
            if route.weight != 1.0:
                self._rebuild_hash_ring()
    
    def remove_route(self, name: str):
        """Remove a route."""
        with self._lock:
            self._routes.pop(name, None)
            self._route_patterns = [(p, r) for p, r in self._route_patterns if r.name != name]
            self._route_conditions = [(c, r) for c, r in self._route_conditions if r.name != name]
            self._rebuild_hash_ring()
    
    def _rebuild_hash_ring(self):
        """Rebuild consistent hash ring."""
        self._hash_ring.clear()
        self._ring_keys.clear()
        self._total_weight = sum(r.weight for r in self._routes.values())
        
        if self._total_weight <= 0:
            return
        
        points = 360
        for route in self._routes.values():
            if route.weight <= 0:
                continue
            num_points = int(points * (route.weight / self._total_weight))
            for i in range(num_points):
                key = int(hashlib.md5(f"{route.name}:{i}".encode()).hexdigest(), 16) % (2 ** 32)
                self._hash_ring[key] = route.name
                self._ring_keys.append(key)
        
        self._ring_keys.sort()
    
    def _match_pattern(self, data: Any) -> Optional[Route]:
        """Match data against pattern routes."""
        data_str = str(data)
        
        for pattern, route in self._route_patterns:
            if pattern.search(data_str):
                return route
        
        return None
    
    def _match_condition(self, data: Any) -> Optional[Route]:
        """Match data against condition routes."""
        for condition, route in self._route_conditions:
            try:
                if condition(data):
                    return route
            except Exception:
                continue
        return None
    
    def _hash_route(self, data: Any) -> Optional[str]:
        """Route using consistent hashing."""
        if not self._ring_keys:
            return None
        
        data_hash = int(hashlib.md5(str(data).encode()).hexdigest()) % (2 ** 32)
        
        for key in self._ring_keys:
            if data_hash <= key:
                return self._hash_ring.get(key)
        
        return self._hash_ring.get(self._ring_keys[0])
    
    def _round_robin_route(self) -> Optional[str]:
        """Route using round robin."""
        with self._lock:
            if not self._routes:
                return None
            route_names = list(self._routes.keys())
            idx = self._round_robin_index % len(route_names)
            self._round_robin_index += 1
            return route_names[idx]
    
    def route(self, data: Any, routing_key: Optional[str] = None) -> Tuple[Optional[str], Optional[Route]]:
        """Route data and return target and route."""
        with self._lock:
            self._stats["total_routes"] += 1
            
            if routing_key and routing_key in self._routes:
                self._stats["total_matches"] += 1
                return routing_key, self._routes[routing_key]
            
            matched = self._match_condition(data)
            if matched:
                self._stats["total_matches"] += 1
                return matched.target, matched
            
            matched = self._match_pattern(data)
            if matched:
                self._stats["total_matches"] += 1
                return matched.target, matched
            
            if self.config.routing_strategy == RoutingStrategy.HASH:
                target = self._hash_route(data)
                if target:
                    self._stats["total_matches"] += 1
                    return target, self._routes.get(target)
            
            elif self.config.routing_strategy == RoutingStrategy.ROUND_ROBIN:
                target = self._round_robin_route()
                if target:
                    self._stats["total_matches"] += 1
                    return target, self._routes.get(target)
            
            if self.config.default_route and self.config.fallback_to_default:
                self._stats["total_matches"] += 1
                return self.config.default_route, self._routes.get(self.config.default_route)
            
            self._stats["no_match"] += 1
            return None, None
    
    def execute_route(self, data: Any, routing_key: Optional[str] = None) -> Tuple[bool, Any]:
        """Route data and execute handler."""
        target, route = self.route(data, routing_key)
        
        if not route:
            return False, None
        
        if route.handler:
            try:
                result = route.handler(data)
                return True, result
            except Exception as e:
                return False, e
        
        return True, data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get routing statistics."""
        with self._lock:
            return {
                "name": self.name,
                "route_count": len(self._routes),
                **{k: v for k, v in self._stats.items()},
            }


class DataRoutingAction(BaseAction):
    """Data routing action."""
    action_type = "data_routing"
    display_name = "数据路由"
    description = "基于规则的数据路由"
    
    def __init__(self):
        super().__init__()
        self._routers: Dict[str, DataRouter] = {}
        self._lock = threading.Lock()
    
    def _get_router(self, name: str, config: Optional[DataRoutingConfig] = None) -> DataRouter:
        """Get or create router."""
        with self._lock:
            if name not in self._routers:
                self._routers[name] = DataRouter(name, config)
            return self._routers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute routing operation."""
        try:
            router_name = params.get("router", "default")
            command = params.get("command", "route")
            
            config = DataRoutingConfig(
                routing_strategy=RoutingStrategy[params.get("strategy", "exact").upper()],
                default_route=params.get("default_route"),
                case_sensitive=params.get("case_sensitive", True),
                fallback_to_default=params.get("fallback_to_default", True),
            )
            
            router = self._get_router(router_name, config)
            
            if command == "add_route":
                route = Route(
                    name=params.get("route_name"),
                    target=params.get("target"),
                    handler=params.get("handler"),
                    condition=params.get("condition"),
                    pattern=params.get("pattern"),
                    priority=params.get("priority", 0),
                    weight=params.get("weight", 1.0),
                )
                router.add_route(route)
                return ActionResult(success=True, message=f"Route {route.name} added")
            
            elif command == "route":
                data = params.get("data")
                routing_key = params.get("routing_key")
                target, route = router.route(data, routing_key)
                
                if target:
                    return ActionResult(success=True, data={"target": target, "route": route.name if route else None})
                return ActionResult(success=False, message="No matching route found")
            
            elif command == "execute":
                data = params.get("data")
                routing_key = params.get("routing_key")
                success, result = router.execute_route(data, routing_key)
                return ActionResult(success=success, data={"result": result})
            
            elif command == "stats":
                stats = router.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataRoutingAction error: {str(e)}")
