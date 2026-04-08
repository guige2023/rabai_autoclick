"""Data router action module for RabAI AutoClick.

Provides data routing with conditional logic,
fan-out, content-based routing, and routing rules.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataRouterAction(BaseAction):
    """Route data to different destinations based on rules.
    
    Supports conditional routing, content-based routing,
    fan-out to multiple destinations, and routing rules.
    """
    action_type = "data_router"
    display_name = "数据路由"
    description = "数据路由，根据规则分发到不同目标"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute routing operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, routes (list of
                   route configs), default_route.
        
        Returns:
            ActionResult with routing results.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        if isinstance(records, dict):
            records = [records]
        
        routes = params.get('routes', [])
        if not routes:
            return ActionResult(success=False, message="No routes defined")
        
        default_route = params.get('default_route')
        fan_out = params.get('fan_out', False)
        
        results = {route.get('name', f'route_{i}'): [] for i, route in enumerate(routes)}
        if default_route:
            results[default_route] = []
        results['unmatched'] = []
        
        unmatched_count = 0
        
        for record in records:
            matched = False
            
            for route in routes:
                route_name = route.get('name', 'unnamed')
                conditions = route.get('conditions', [])
                
                if self._check_conditions(record, conditions):
                    results[route_name].append(record)
                    matched = True
                    
                    if not fan_out:
                        break
            
            if not matched:
                if default_route:
                    results[default_route].append(record)
                else:
                    results['unmatched'].append(record)
                unmatched_count += 1
        
        total_routed = sum(len(v) for v in results.values())
        
        return ActionResult(
            success=unmatched_count == 0,
            message=f"Routed {total_routed} records to {len(routes)} routes",
            data={
                'routing_results': results,
                'total': len(records),
                'routed': total_routed,
                'unmatched': unmatched_count
            }
        )
    
    def _check_conditions(
        self,
        record: Dict[str, Any],
        conditions: List[Dict[str, Any]]
    ) -> bool:
        """Check if record matches all conditions."""
        if not conditions:
            return True
        
        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator', 'eq')
            value = condition.get('value')
            values = condition.get('values', [])
            
            if not field:
                continue
            
            record_value = record.get(field)
            
            if operator == 'eq':
                if record_value != value:
                    return False
            elif operator == 'ne':
                if record_value == value:
                    return False
            elif operator == 'gt':
                if not (isinstance(record_value, (int, float)) and record_value > value):
                    return False
            elif operator == 'gte':
                if not (isinstance(record_value, (int, float)) and record_value >= value):
                    return False
            elif operator == 'lt':
                if not (isinstance(record_value, (int, float)) and record_value < value):
                    return False
            elif operator == 'lte':
                if not (isinstance(record_value, (int, float)) and record_value <= value):
                    return False
            elif operator == 'in':
                if record_value not in values:
                    return False
            elif operator == 'not_in':
                if record_value in values:
                    return False
            elif operator == 'contains':
                if value not in str(record_value):
                    return False
            elif operator == 'starts_with':
                if not str(record_value).startswith(str(value)):
                    return False
            elif operator == 'ends_with':
                if not str(record_value).endswith(str(value)):
                    return False
            elif operator == 'regex':
                import re
                if not re.match(value, str(record_value)):
                    return False
            elif operator == 'exists':
                if (value and field not in record) or (not value and field in record):
                    return False
            elif operator == 'is_null':
                if value and record_value is not None:
                    return False
                if not value and record_value is None:
                    return False
            elif operator == 'is_empty':
                is_empty = (record_value is None or record_value == '' or 
                           (isinstance(record_value, (list, dict)) and len(record_value) == 0))
                if value and not is_empty:
                    return False
                if not value and is_empty:
                    return False
        
        return True


class ConditionalRouter:
    """Conditional router with pre-built rules."""
    
    def __init__(self, rules: List[Dict[str, Any]]):
        self.rules = rules
    
    def route(self, record: Dict[str, Any]) -> Optional[str]:
        """Route a single record and return destination."""
        for rule in self.rules:
            conditions = rule.get('conditions', [])
            destination = rule.get('destination')
            
            router = DataRouterAction()
            if router._check_conditions(record, conditions):
                return destination
        
        return None


class RoundRobinRouter:
    """Round-robin router for load balancing."""
    
    def __init__(self, destinations: List[str]):
        self.destinations = destinations
        self.current_index = 0
        self.lock = threading.Lock()
    
    def route(self, record: Dict[str, Any]) -> str:
        """Route to next destination in round-robin."""
        with self.lock:
            destination = self.destinations[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.destinations)
            return destination


class WeightedRouter:
    """Weighted router for load distribution."""
    
    def __init__(self, weights: Dict[str, float]):
        self.destinations = list(weights.keys())
        self.weights = weights
        total = sum(weights.values())
        self.cumulative = []
        cum = 0
        for dest in self.destinations:
            cum += weights[dest] / total
            self.cumulative.append(cum)
    
    def route(self, record: Dict[str, Any]) -> str:
        """Route based on weighted probability."""
        import random
        value = random.random()
        
        for i, threshold in enumerate(self.cumulative):
            if value <= threshold:
                return self.destinations[i]
        
        return self.destinations[-1]
