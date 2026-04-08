# Copyright (c) 2024. coded by claude
"""Data Router Action Module.

Routes data to different destinations based on rules and conditions
with support for fan-out, filtering, and transformation.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RouteStrategy(Enum):
    FIRST_MATCH = "first_match"
    ALL_MATCH = "all_match"
    WEIGHTED = "weighted"


@dataclass
class Route:
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    destination: str
    transform: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    priority: int = 0


@dataclass
class RoutingResult:
    routed_count: int
    undelivered_count: int
    destinations: Dict[str, int]


class DataRouter:
    def __init__(self, strategy: RouteStrategy = RouteStrategy.FIRST_MATCH):
        self.strategy = strategy
        self._routes: List[Route] = []

    def add_route(self, route: Route) -> None:
        self._routes.append(route)
        self._routes.sort(key=lambda r: -r.priority)

    def route(self, data: List[Dict[str, Any]]) -> RoutingResult:
        destinations: Dict[str, int] = defaultdict(int)
        routed_count = 0
        undelivered_count = 0
        for item in data:
            delivered = False
            for route in self._routes:
                if route.condition(item):
                    transformed = route.transform(item) if route.transform else item
                    destinations[route.destination] += 1
                    routed_count += 1
                    delivered = True
                    if self.strategy == RouteStrategy.FIRST_MATCH:
                        break
            if not delivered:
                undelivered_count += 1
        return RoutingResult(
            routed_count=routed_count,
            undelivered_count=undelivered_count,
            destinations=dict(destinations),
        )

    def route_single(self, item: Dict[str, Any]) -> Optional[str]:
        for route in self._routes:
            if route.condition(item):
                transformed = route.transform(item) if route.transform else item
                return route.destination
        return None

    def remove_route(self, name: str) -> bool:
        for i, route in enumerate(self._routes):
            if route.name == name:
                self._routes.pop(i)
                return True
        return False

    def clear_routes(self) -> None:
        self._routes.clear()
