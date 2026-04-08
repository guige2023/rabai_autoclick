"""Data Hook Action Module for RabAI AutoClick.

Provides hook/callback system for data pipeline events,
allowing custom handlers to be triggered on data changes.
"""

import time
import re
import sys
import os
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataHookAction(BaseAction):
    """Hook system for data pipeline event handling.

    Register hooks on data events such as transformation,
    validation, filtering, and routing. Supports priority-based
    hook ordering and conditional triggering.
    """
    action_type = "data_hook"
    display_name = "数据钩子"
    description = "数据管道事件钩子系统"

    _hooks: Dict[str, List[Dict[str, Any]]] = {
        'pre_process': [],
        'post_process': [],
        'on_error': [],
        'on_data': [],
        'pre_validate': [],
        'post_validate': [],
        'pre_transform': [],
        'post_transform': [],
        'pre_filter': [],
        'post_filter': [],
    }
    _hook_stats: Dict[str, Dict[str, int]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hook operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'register', 'unregister', 'trigger',
                               'list', 'clear', 'stats'
                - event: str - event name (e.g., 'pre_process', 'on_data')
                - handler: callable (optional) - handler function
                - handler_name: str (optional) - named handler to register
                - priority: int (optional) - hook priority (higher=first)
                - condition: str (optional) - regex condition for triggering
                - data: Any (optional) - data to pass through hooks

        Returns:
            ActionResult with hook operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'trigger')

            if operation == 'register':
                return self._register_hook(params, start_time)
            elif operation == 'unregister':
                return self._unregister_hook(params, start_time)
            elif operation == 'trigger':
                return self._trigger_hooks(params, start_time)
            elif operation == 'list':
                return self._list_hooks(params, start_time)
            elif operation == 'clear':
                return self._clear_hooks(params, start_time)
            elif operation == 'stats':
                return self._get_stats(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hook action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _register_hook(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register a hook handler."""
        event = params.get('event', 'on_data')
        handler_name = params.get('handler_name', f'handler_{time.time()}')
        priority = params.get('priority', 0)
        condition = params.get('condition', '')

        if event not in self._hooks:
            self._hooks[event] = []

        self._hooks[event].append({
            'name': handler_name,
            'priority': priority,
            'condition': condition,
            'condition_regex': re.compile(condition) if condition else None,
            'registered_at': time.time(),
            'call_count': 0,
            'error_count': 0
        })
        self._hooks[event].sort(key=lambda h: -h['priority'])

        if event not in self._hook_stats:
            self._hook_stats[event] = {'total': 0, 'success': 0, 'errors': 0}

        return ActionResult(
            success=True,
            message=f"Hook registered: {handler_name} on {event}",
            data={
                'event': event,
                'handler_name': handler_name,
                'priority': priority,
                'total_hooks': len(self._hooks[event])
            },
            duration=time.time() - start_time
        )

    def _unregister_hook(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Unregister a hook handler."""
        event = params.get('event', 'on_data')
        handler_name = params.get('handler_name', '')

        if event not in self._hooks:
            return ActionResult(
                success=False,
                message=f"Event not found: {event}",
                duration=time.time() - start_time
            )

        original_count = len(self._hooks[event])
        self._hooks[event] = [
            h for h in self._hooks[event] if h['name'] != handler_name
        ]

        removed = original_count - len(self._hooks[event])
        return ActionResult(
            success=removed > 0,
            message=f"Hook removed: {handler_name} from {event}" if removed else "Hook not found",
            data={'removed': removed, 'remaining': len(self._hooks[event])},
            duration=time.time() - start_time
        )

    def _trigger_hooks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Trigger all hooks for an event."""
        event = params.get('event', 'on_data')
        data = params.get('data', None)

        if event not in self._hooks or not self._hooks[event]:
            return ActionResult(
                success=True,
                message=f"No hooks registered for: {event}",
                data={'hooks_triggered': 0, 'data': data},
                duration=time.time() - start_time
            )

        hook_results = []
        for hook in self._hooks[event]:
            if hook['condition_regex']:
                data_str = str(data) if data is not None else ''
                if not hook['condition_regex'].search(data_str):
                    continue

            hook['call_count'] += 1

            try:
                result_data = self._invoke_handler(hook, data)
                hook_results.append({
                    'name': hook['name'],
                    'success': True,
                    'data': result_data
                })
                if event in self._hook_stats:
                    self._hook_stats[event]['success'] += 1
            except Exception as e:
                hook['error_count'] += 1
                hook_results.append({
                    'name': hook['name'],
                    'success': False,
                    'error': str(e)
                })
                if event in self._hook_stats:
                    self._hook_stats[event]['errors'] += 1

        if event in self._hook_stats:
            self._hook_stats[event]['total'] += 1

        return ActionResult(
            success=True,
            message=f"Triggered {len(hook_results)} hooks for {event}",
            data={
                'event': event,
                'hooks_triggered': len(hook_results),
                'results': hook_results,
                'data': data
            },
            duration=time.time() - start_time
        )

    def _invoke_handler(self, hook: Dict[str, Any], data: Any) -> Any:
        """Invoke a hook handler."""
        handler_name = hook['name']

        if handler_name.startswith('upper_'):
            if isinstance(data, str):
                return data.upper()
        elif handler_name.startswith('lower_'):
            if isinstance(data, str):
                return data.lower()
        elif handler_name.startswith('filter_'):
            if isinstance(data, str):
                return data
        else:
            return data

        return data

    def _list_hooks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all registered hooks."""
        event = params.get('event', None)

        if event:
            hooks = self._hooks.get(event, [])
            return ActionResult(
                success=True,
                message=f"Hooks for {event}: {len(hooks)}",
                data={
                    'event': event,
                    'hooks': [
                        {
                            'name': h['name'],
                            'priority': h['priority'],
                            'condition': h['condition'],
                            'call_count': h['call_count']
                        }
                        for h in hooks
                    ]
                },
                duration=time.time() - start_time
            )
        else:
            all_hooks = {}
            for evt, hooks in self._hooks.items():
                if hooks:
                    all_hooks[evt] = len(hooks)

            return ActionResult(
                success=True,
                message=f"Total events with hooks: {len(all_hooks)}",
                data={'events': all_hooks},
                duration=time.time() - start_time
            )

    def _clear_hooks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear hooks for an event or all events."""
        event = params.get('event', None)

        if event:
            count = len(self._hooks.get(event, []))
            self._hooks[event] = []
            return ActionResult(
                success=True,
                message=f"Cleared {count} hooks for {event}",
                data={'cleared': count, 'event': event},
                duration=time.time() - start_time
            )
        else:
            total = sum(len(h) for h in self._hooks.values())
            for key in self._hooks:
                self._hooks[key] = []
            return ActionResult(
                success=True,
                message=f"Cleared all {total} hooks",
                data={'cleared': total},
                duration=time.time() - start_time
            )

    def _get_stats(self, start_time: float) -> ActionResult:
        """Get hook execution statistics."""
        return ActionResult(
            success=True,
            message="Hook statistics",
            data={'stats': self._hook_stats},
            duration=time.time() - start_time
        )
