"""Event router action module for RabAI AutoClick.

Provides event-driven routing with pattern matching,
conditional routing, and event transformation.
"""

import sys
import os
import re
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Pattern
from dataclasses import dataclass, field
from enum import Enum
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MatchType(Enum):
    """Event matching types."""
    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    GLOB = "glob"


@dataclass
class Route:
    """An event route definition."""
    name: str
    pattern: str
    match_type: str = "exact"
    action_name: str = ""
    action_params: Dict[str, Any] = field(default_factory=dict)
    transform: Optional[str] = None
    priority: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class EventRouterAction(BaseAction):
    """Route events to actions based on pattern matching.
    
    Supports exact, contains, regex, prefix, suffix,
    and glob pattern matching with event transformation.
    """
    action_type = "event_router"
    display_name = "事件路由"
    description = "基于模式匹配的事件路由，支持条件转发和转换"

    _routes: Dict[str, List[Route]] = {}
    _route_locks: Dict[str, threading.Lock] = {}
    _event_history: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Route an event to matching actions.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (route/add_route/remove_route/list_routes/clear)
                - event_type: str, event type/name
                - event_data: any, event payload
                - event_key: str, key to match against
                - routes: list of route specs (for add_route)
                - route_name: str (for remove_route)
                - max_history: int, max event history size
                - save_to_var: str
        
        Returns:
            ActionResult with routing result.
        """
        operation = params.get('operation', 'route')

        if operation == 'route':
            return self._route_event(context, params)
        elif operation == 'add_route':
            return self._add_route(context, params)
        elif operation == 'remove_route':
            return self._remove_route(context, params)
        elif operation == 'list_routes':
            return self._list_routes(context, params)
        elif operation == 'clear':
            return self._clear_routes(context, params)
        elif operation == 'get_history':
            return self._get_history(context, params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _route_event(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Route an event to matching routes."""
        event_type = params.get('event_type', 'default')
        event_data = params.get('event_data', None)
        event_key = params.get('event_key', '')
        max_history = params.get('max_history', 100)
        save_to_var = params.get('save_to_var', None)

        self._ensure_router(event_type)

        matched_routes = []
        with self._route_locks.get(event_type, threading.Lock()):
            routes = self._routes.get(event_type, [])
            for route in sorted(routes, key=lambda r: r.priority, reverse=True):
                if not route.enabled:
                    continue
                if self._match(route.pattern, event_key, route.match_type):
                    matched_routes.append(route)

        results = []
        for route in matched_routes:
            action_params = dict(route.action_params)
            if event_data is not None:
                action_params['_event_data'] = event_data
            if route.transform:
                action_params = self._transform(action_params, route.transform)

            result = self._execute_route_action(context, route.action_name, action_params)
            results.append({
                'route': route.name,
                'action': route.action_name,
                'result': result
            })

        # Store in history
        event_record = {
            'event_type': event_type,
            'event_key': event_key,
            'event_data': event_data,
            'matched': len(matched_routes),
            'timestamp': time.time(),
            'results': results
        }
        self._add_to_history(event_type, event_record, max_history)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'matched_count': len(matched_routes),
                'routes': [r.name for r in matched_routes],
                'results': results
            }

        return ActionResult(
            success=len(matched_routes) > 0,
            message=f"Routed to {len(matched_routes)} route(s)",
            data={'matched_routes': [r.name for r in matched_routes], 'results': results}
        )

    def _match(self, pattern: str, value: str, match_type: str) -> bool:
        """Check if value matches pattern."""
        if not value and not pattern:
            return True
        if not value:
            return False

        if match_type == 'exact':
            return str(value) == pattern
        elif match_type == 'contains':
            return str(pattern) in str(value)
        elif match_type == 'prefix':
            return str(value).startswith(str(pattern))
        elif match_type == 'suffix':
            return str(value).endswith(str(pattern))
        elif match_type == 'regex':
            try:
                return bool(re.search(pattern, str(value)))
            except re.error:
                return False
        elif match_type == 'glob':
            return self._glob_match(pattern, str(value))
        return False

    def _glob_match(self, pattern: str, value: str) -> bool:
        """Simple glob matching (**, *, ?)."""
        # Convert glob to regex
        regex_pattern = pattern
        regex_pattern = regex_pattern.replace('.', '\\.')
        regex_pattern = regex_pattern.replace('**/', '.*')
        regex_pattern = regex_pattern.replace('**', '.*')
        regex_pattern = regex_pattern.replace('*', '[^/]*')
        regex_pattern = regex_pattern.replace('?', '.')
        regex_pattern = f'^{regex_pattern}$'
        try:
            return bool(re.match(regex_pattern, value))
        except re.error:
            return False

    def _transform(self, params: Dict[str, Any], transform_type: str) -> Dict[str, Any]:
        """Transform event data."""
        if transform_type == 'flatten':
            return self._flatten_dict(params)
        elif transform_type == 'extract_keys':
            return {k: v for k, v in params.items() if not k.startswith('_')}
        elif transform_type == 'wrap':
            return {'data': params}
        return params

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _execute_route_action(
        self, context: Any, action_name: str, params: Dict[str, Any]
    ) -> ActionResult:
        """Execute the routed action."""
        action = self._find_action(action_name)
        if action is None:
            return ActionResult(success=False, message=f"Action not found: {action_name}")
        try:
            return action.execute(context, params)
        except Exception as e:
            return ActionResult(success=False, message=f"Route action error: {e}")

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction, 'type': TypeAction,
                'key_press': KeyPressAction, 'image_match': ImageMatchAction,
                'find_image': FindImageAction, 'ocr': OCRAction,
                'scroll': ScrollAction, 'mouse_move': MouseMoveAction,
                'drag': DragAction, 'script': ScriptAction,
                'delay': DelayAction, 'condition': ConditionAction,
                'loop': LoopAction, 'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction, 'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            return action_cls() if action_cls else None
        except Exception:
            return None

    def _ensure_router(self, event_type: str) -> None:
        """Ensure router exists for event type."""
        if event_type not in self._routes:
            with threading.Lock():
                if event_type not in self._routes:
                    self._routes[event_type] = []
                    self._route_locks[event_type] = threading.Lock()
                    self._event_history[event_type] = []

    def _add_route(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Add a route to an event type."""
        event_type = params.get('event_type', 'default')
        route_name = params.get('route_name', '')
        pattern = params.get('pattern', '')
        match_type = params.get('match_type', 'exact')
        action_name = params.get('action_name', '')
        action_params = params.get('action_params', {})
        priority = params.get('priority', 0)
        transform = params.get('transform', None)
        enabled = params.get('enabled', True)

        if not route_name or not pattern or not action_name:
            return ActionResult(
                success=False,
                message="route_name, pattern, and action_name are required"
            )

        self._ensure_router(event_type)
        route = Route(
            name=route_name,
            pattern=pattern,
            match_type=match_type,
            action_name=action_name,
            action_params=action_params,
            transform=transform,
            priority=priority,
            enabled=enabled
        )
        with self._route_locks[event_type]:
            self._routes[event_type].append(route)

        return ActionResult(
            success=True,
            message=f"Added route '{route_name}' to '{event_type}'",
            data={'route_name': route_name, 'event_type': event_type}
        )

    def _remove_route(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Remove a route."""
        event_type = params.get('event_type', 'default')
        route_name = params.get('route_name', '')

        self._ensure_router(event_type)
        with self._route_locks[event_type]:
            original = len(self._routes[event_type])
            self._routes[event_type] = [
                r for r in self._routes[event_type] if r.name != route_name
            ]
            removed = original - len(self._routes[event_type])

        return ActionResult(
            success=removed > 0,
            message=f"Removed {removed} route(s) named '{route_name}'",
            data={'removed': removed}
        )

    def _list_routes(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """List all routes."""
        event_type = params.get('event_type', None)
        save_to_var = params.get('save_to_var', None)

        if event_type:
            self._ensure_router(event_type)
            result = {
                r.name: {
                    'pattern': r.pattern,
                    'match_type': r.match_type,
                    'action': r.action_name,
                    'priority': r.priority,
                    'enabled': r.enabled
                }
                for r in self._routes.get(event_type, [])
            }
        else:
            result = {
                et: [r.name for r in routes]
                for et, routes in self._routes.items()
            }

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(success=True, message=f"Routes: {len(result)}", data=result)

    def _clear_routes(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Clear routes."""
        event_type = params.get('event_type', None)
        if event_type:
            self._ensure_router(event_type)
            with self._route_locks[event_type]:
                count = len(self._routes[event_type])
                self._routes[event_type].clear()
        else:
            with threading.Lock():
                count = sum(len(v) for v in self._routes.values())
                self._routes.clear()
                self._route_locks.clear()

        return ActionResult(success=True, message=f"Cleared {count} routes", data={'cleared': count})

    def _add_to_history(
        self, event_type: str, record: Dict, max_history: int
    ) -> None:
        """Add event to history."""
        if event_type not in self._event_history:
            self._event_history[event_type] = []
        self._event_history[event_type].append(record)
        if len(self._event_history[event_type]) > max_history:
            self._event_history[event_type] = self._event_history[event_type][-max_history:]

    def _get_history(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get event history."""
        event_type = params.get('event_type', 'default')
        limit = params.get('limit', 50)
        save_to_var = params.get('save_to_var', None)

        history = self._event_history.get(event_type, [])[-limit:]
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = history

        return ActionResult(
            success=True,
            message=f"History for '{event_type}': {len(history)} events",
            data=history
        )

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'event_type': 'default',
            'event_data': None,
            'event_key': '',
            'route_name': '',
            'pattern': '',
            'match_type': 'exact',
            'action_name': '',
            'action_params': {},
            'priority': 0,
            'transform': None,
            'enabled': True,
            'max_history': 100,
            'max_items': 50,
            'save_to_var': None,
        }
