"""
Workflow Routing Action.

Provides content-based workflow routing.
Supports:
- Rule-based routing
- Condition evaluation
- Multi-destination routing
- Fallback routing
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
import logging
import json
import re

logger = logging.getLogger(__name__)


@dataclass
class RouteRule:
    """Routing rule definition."""
    rule_id: str
    name: str
    condition: Callable[[Dict], bool]
    destination: str
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RouteResult:
    """Result of routing."""
    matched_rule: Optional[RouteRule]
    destination: Optional[str]
    evaluated_rules: List[str]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def matched(self) -> bool:
        return self.matched_rule is not None


class WorkflowRoutingAction:
    """
    Workflow Routing Action.
    
    Provides content-based routing with support for:
    - Rule-based routing
    - Priority ordering
    - Default/fallback routes
    - Multiple destinations
    """
    
    def __init__(self, default_destination: Optional[str] = None):
        """
        Initialize the Workflow Routing Action.
        
        Args:
            default_destination: Default destination if no rules match
        """
        self.default_destination = default_destination
        self._rules: List[RouteRule] = []
        self._stats: Dict[str, int] = {}
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict], bool],
        destination: str,
        priority: int = 0,
        rule_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "WorkflowRoutingAction":
        """
        Add a routing rule.
        
        Args:
            name: Rule name
            condition: Function that returns True if condition matches
            destination: Destination to route to
            priority: Rule priority (higher = evaluated first)
            rule_id: Unique rule ID
            metadata: Additional metadata
        
        Returns:
            Self for chaining
        """
        rule = RouteRule(
            rule_id=rule_id or f"rule-{len(self._rules)}",
            name=name,
            condition=condition,
            destination=destination,
            priority=priority,
            metadata=metadata or {}
        )
        
        self._rules.append(rule)
        self._rules.sort(key=lambda r: -r.priority)  # Sort by priority descending
        
        logger.info(f"Added routing rule: {name} -> {destination}")
        return self
    
    def add_rule_regex(
        self,
        name: str,
        field_name: str,
        pattern: str,
        destination: str,
        priority: int = 0
    ) -> "WorkflowRoutingAction":
        """Add a regex-based routing rule."""
        compiled_pattern = re.compile(pattern)
        
        def condition(data: Dict) -> bool:
            value = data.get(field_name, "")
            return bool(compiled_pattern.search(str(value)))
        
        return self.add_rule(name, condition, destination, priority)
    
    def add_rule_range(
        self,
        name: str,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        destination: str = "",
        priority: int = 0
    ) -> "WorkflowRoutingAction":
        """Add a range-based routing rule."""
        def condition(data: Dict) -> bool:
            try:
                value = float(data.get(field_name, 0))
                
                if min_value is not None and value < min_value:
                    return False
                if max_value is not None and value >= max_value:
                    return False
                return True
            except (ValueError, TypeError):
                return False
        
        return self.add_rule(name, condition, destination, priority)
    
    def add_rule_value(
        self,
        name: str,
        field_name: str,
        values: List[Any],
        destination: str = "",
        priority: int = 0
    ) -> "WorkflowRoutingAction":
        """Add a value-based routing rule."""
        value_set = set(values)
        
        def condition(data: Dict) -> bool:
            return data.get(field_name) in value_set
        
        return self.add_rule(name, condition, destination, priority)
    
    def route(self, data: Dict[str, Any]) -> RouteResult:
        """
        Route data based on rules.
        
        Args:
            data: Data to route
        
        Returns:
            RouteResult with matched destination
        """
        evaluated = []
        
        for rule in self._rules:
            evaluated.append(rule.rule_id)
            
            try:
                if rule.condition(data):
                    self._stats[rule.rule_id] = self._stats.get(rule.rule_id, 0) + 1
                    
                    logger.debug(f"Rule '{rule.name}' matched for data")
                    return RouteResult(
                        matched_rule=rule,
                        destination=rule.destination,
                        evaluated_rules=evaluated
                    )
            except Exception as e:
                logger.error(f"Error evaluating rule '{rule.name}': {e}")
        
        # No rule matched, use default
        if self.default_destination:
            return RouteResult(
                matched_rule=None,
                destination=self.default_destination,
                evaluated_rules=evaluated
            )
        
        return RouteResult(
            matched_rule=None,
            destination=None,
            evaluated_rules=evaluated
        )
    
    def route_multiple(
        self,
        data: Dict[str, Any]
    ) -> List[str]:
        """
        Route to multiple destinations (fan-out).
        
        Args:
            data: Data to route
        
        Returns:
            List of matching destinations
        """
        destinations = []
        
        for rule in self._rules:
            try:
                if rule.condition(data):
                    destinations.append(rule.destination)
            except Exception as e:
                logger.error(f"Error evaluating rule '{rule.name}': {e}")
        
        return destinations
    
    def remove_rule(self, rule_id: str) -> bool:
        """Remove a routing rule."""
        for i, rule in enumerate(self._rules):
            if rule.rule_id == rule_id:
                self._rules.pop(i)
                logger.info(f"Removed rule: {rule_id}")
                return True
        return False
    
    def clear_rules(self) -> None:
        """Clear all routing rules."""
        count = len(self._rules)
        self._rules = []
        logger.info(f"Cleared {count} routing rules")
    
    def get_rules(self) -> List[Dict[str, Any]]:
        """Get all routing rules."""
        return [
            {
                "rule_id": r.rule_id,
                "name": r.name,
                "destination": r.destination,
                "priority": r.priority,
                "metadata": r.metadata
            }
            for r in self._rules
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get routing statistics."""
        return {
            "total_rules": len(self._rules),
            "total_routes": sum(self._stats.values()),
            "by_rule": self._stats.copy(),
            "default_destination": self.default_destination
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    router = WorkflowRoutingAction(default_destination="default_queue")
    
    # Add rules
    router.add_rule(
        "high_priority",
        lambda d: d.get("priority") == "high",
        "high_priority_queue",
        priority=100
    )
    
    router.add_rule_regex(
        "email_routing",
        "type",
        r"email|notification",
        "email_queue",
        priority=50
    )
    
    router.add_rule_range(
        "amount_routing",
        "amount",
        min_value=10000,
        destination="large_transaction_queue",
        priority=75
    )
    
    router.add_rule_value(
        "vip_routing",
        "customer_tier",
        ["gold", "platinum"],
        "vip_queue",
        priority=90
    )
    
    # Test routing
    test_cases = [
        {"priority": "high", "type": "order"},
        {"type": "email_notification", "content": "Hello"},
        {"amount": 15000, "type": "payment"},
        {"customer_tier": "platinum", "purchase": 500},
        {"type": "order", "status": "pending"},
    ]
    
    for data in test_cases:
        result = router.route(data)
        print(f"Data: {json.dumps(data)[:50]}...")
        print(f"  -> {result.destination} (matched: {result.matched})")
    
    print(f"\nStats: {json.dumps(router.get_stats(), indent=2)}")
