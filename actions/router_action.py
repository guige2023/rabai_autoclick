"""Router action module for RabAI AutoClick.

Provides request routing utilities:
- Router: Route requests based on rules
- RouteRule: Define routing rules
- RouterRegistry: Manage routers
"""

from typing import Any, Callable, Dict, List, Optional, Pattern
import re
import threading
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RouteRule:
    """Routing rule."""

    RULE_EXACT = "exact"
    RULE_PREFIX = "prefix"
    RULE_REGEX = "regex"
    RULE_GLOB = "glob"

    def __init__(
        self,
        name: str,
        pattern: str,
        handler: Callable,
        rule_type: str = RULE_EXACT,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.pattern = pattern
        self.handler = handler
        self.rule_type = rule_type
        self.priority = priority
        self.metadata = metadata or {}

        if rule_type == self.RULE_REGEX:
            self._compiled: Optional[Pattern] = re.compile(pattern)
        else:
            self._compiled = None

    def matches(self, path: str) -> bool:
        """Check if path matches rule."""
        if self.rule_type == self.RULE_EXACT:
            return path == self.pattern
        elif self.rule_type == self.RULE_PREFIX:
            return path.startswith(self.pattern)
        elif self.rule_type == self.RULE_REGEX:
            return bool(self._compiled.match(path))
        elif self.rule_type == self.RULE_GLOB:
            return self._glob_match(self.pattern, path)
        return False

    def _glob_match(self, pattern: str, path: str) -> bool:
        """Simple glob matching."""
        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
        return bool(re.match(f"^{regex_pattern}$", path))


class Router:
    """Request router."""

    def __init__(self):
        self._rules: List[RouteRule] = []
        self._lock = threading.RLock()
        self._not_found_handler: Optional[Callable] = None

    def add_rule(
        self,
        name: str,
        pattern: str,
        handler: Callable,
        rule_type: str = RouteRule.RULE_EXACT,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add a routing rule."""
        with self._lock:
            rule_id = str(uuid.uuid4())
            rule = RouteRule(name, pattern, handler, rule_type, priority, metadata)
            self._rules.append(rule)
            self._rules.sort(key=lambda r: r.priority, reverse=True)
            return rule_id

    def set_not_found_handler(self, handler: Callable) -> None:
        """Set handler for not found."""
        self._not_found_handler = handler

    def route(self, path: str) -> Any:
        """Route a path to matching handler."""
        with self._lock:
            for rule in self._rules:
                if rule.matches(path):
                    return rule.handler(path, rule.metadata)
            if self._not_found_handler:
                return self._not_found_handler(path)
            return None

    def remove_rule(self, name: str) -> bool:
        """Remove rule by name."""
        with self._lock:
            for i, rule in enumerate(self._rules):
                if rule.name == name:
                    self._rules.pop(i)
                    return True
            return False

    def get_rules(self) -> List[Dict[str, Any]]:
        """Get all rules."""
        with self._lock:
            return [
                {
                    "name": r.name,
                    "pattern": r.pattern,
                    "rule_type": r.rule_type,
                    "priority": r.priority,
                }
                for r in self._rules
            ]


class RouterRegistry:
    """Registry for routers."""

    def __init__(self):
        self._routers: Dict[str, Router] = {}
        self._lock = threading.Lock()

    def create(self, name: str) -> Router:
        """Create a router."""
        with self._lock:
            router = Router()
            self._routers[name] = router
            return router

    def get(self, name: str) -> Optional[Router]:
        """Get router by name."""
        with self._lock:
            return self._routers.get(name)

    def delete(self, name: str) -> bool:
        """Delete a router."""
        with self._lock:
            if name in self._routers:
                del self._routers[name]
                return True
            return False


class RouterAction(BaseAction):
    """Router management action."""
    action_type = "router"
    display_name = "路由管理"
    description = "请求路由"

    def __init__(self):
        super().__init__()
        self._registry = RouterRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_rule":
                return self._add_rule(params)
            elif operation == "route":
                return self._route(params)
            elif operation == "remove_rule":
                return self._remove_rule(params)
            elif operation == "list_rules":
                return self._list_rules(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Router error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a router."""
        name = params.get("name", str(uuid.uuid4()))

        router = self._registry.create(name)

        return ActionResult(success=True, message=f"Router created: {name}", data={"name": name})

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add a routing rule."""
        router_name = params.get("router_name", "default")
        name = params.get("name")
        pattern = params.get("pattern")
        rule_type = params.get("rule_type", "exact")
        priority = params.get("priority", 0)

        router = self._registry.get(router_name)
        if not router:
            return ActionResult(success=False, message=f"Router not found: {router_name}")

        if not name or not pattern:
            return ActionResult(success=False, message="name and pattern are required")

        def handler(path, metadata):
            return {"routed": True, "path": path}

        rule_id = router.add_rule(name, pattern, handler, rule_type, priority)

        return ActionResult(success=True, message=f"Rule added: {name}", data={"rule_id": rule_id})

    def _route(self, params: Dict[str, Any]) -> ActionResult:
        """Route a path."""
        router_name = params.get("router_name", "default")
        path = params.get("path")

        router = self._registry.get(router_name)
        if not router:
            return ActionResult(success=False, message=f"Router not found: {router_name}")

        if not path:
            return ActionResult(success=False, message="path is required")

        result = router.route(path)

        if result is None:
            return ActionResult(success=False, message="No matching rule")

        return ActionResult(success=True, message="Routed", data={"result": result})

    def _remove_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a rule."""
        router_name = params.get("router_name", "default")
        name = params.get("name")

        router = self._registry.get(router_name)
        if not router:
            return ActionResult(success=False, message=f"Router not found: {router_name}")

        success = router.remove_rule(name)

        return ActionResult(success=success, message="Removed" if success else "Rule not found")

    def _list_rules(self, params: Dict[str, Any]) -> ActionResult:
        """List all rules."""
        router_name = params.get("router_name", "default")

        router = self._registry.get(router_name)
        if not router:
            return ActionResult(success=False, message=f"Router not found: {router_name}")

        rules = router.get_rules()

        return ActionResult(success=True, message=f"{len(rules)} rules", data={"rules": rules})
