"""Automation Observer action module for RabAI AutoClick.

Observer pattern for monitoring automation execution,
subscribing to events, and reactive automation.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationObserverAction(BaseAction):
    """Observer pattern for automation events.

    Subscribe to events, react to state changes,
    and implement reactive automation flows.
    """
    action_type = "automation_observer"
    display_name = "自动化观察者"
    description = "自动化事件的观察者模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage observers.

        Args:
            context: Execution context.
            params: Dict with keys: action (subscribe/unsubscribe/notify),
                   event_type, observer, event_data.

        Returns:
            ActionResult with observer result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'notify')
            event_type = params.get('event_type', '')
            observer = params.get('observer')
            event_data = params.get('event_data', {})

            if not hasattr(context, '_automation_observers'):
                context._automation_observers = defaultdict(list)
            observers = context._automation_observers

            if action == 'subscribe':
                if not observer or not event_type:
                    return ActionResult(
                        success=False,
                        message="observer and event_type are required",
                        duration=time.time() - start_time,
                    )
                observers[event_type].append(observer)
                return ActionResult(
                    success=True,
                    message=f"Subscribed observer to {event_type}",
                    data={'event_type': event_type, 'subscriber_count': len(observers[event_type])},
                    duration=time.time() - start_time,
                )

            elif action == 'unsubscribe':
                if event_type in observers and observer in observers[event_type]:
                    observers[event_type].remove(observer)
                return ActionResult(
                    success=True,
                    message=f"Unsubscribed from {event_type}",
                    data={'subscriber_count': len(observers.get(event_type, []))},
                    duration=time.time() - start_time,
                )

            elif action == 'notify':
                if not event_type:
                    return ActionResult(
                        success=False,
                        message="event_type is required",
                        duration=time.time() - start_time,
                    )

                notified = []
                errors = []
                for obs in observers.get(event_type, []):
                    try:
                        if callable(obs):
                            result = obs(event_data, context)
                            notified.append({'observer': 'callable', 'result': result})
                        elif hasattr(context, 'execute_action'):
                            result = context.execute_action(obs, event_data)
                            notified.append({'observer': obs, 'result': result})
                    except Exception as e:
                        errors.append({'observer': str(obs), 'error': str(e)})

                # Also notify wildcard subscribers
                for obs in observers.get('*', []):
                    try:
                        if callable(obs):
                            obs({'event_type': event_type, 'data': event_data}, context)
                            notified.append({'observer': 'wildcard', 'result': True})
                    except Exception as e:
                        errors.append({'observer': 'wildcard', 'error': str(e)})

                return ActionResult(
                    success=len(errors) == 0,
                    message=f"Notified {len(notified)} observers for {event_type}",
                    data={
                        'event_type': event_type,
                        'notified_count': len(notified),
                        'errors_count': len(errors),
                        'notified': notified,
                        'errors': errors,
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'list':
                return ActionResult(
                    success=True,
                    message=f"Registered {len(observers)} event types",
                    data={'events': {k: len(v) for k, v in observers.items()}},
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Observer error: {str(e)}",
                duration=duration,
            )


class ReactiveAutomationAction(BaseAction):
    """Reactive automation with dependency tracking.

    Automatically triggers actions when dependencies
    change, implementing dataflow automation.
    """
    action_type = "reactive_automation"
    display_name = "响应式自动化"
    description = "带依赖追踪的响应式自动化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """React to data changes.

        Args:
            context: Execution context.
            params: Dict with keys: data_key, old_value, new_value,
                   reactions, check_fn.

        Returns:
            ActionResult with reaction results.
        """
        start_time = time.time()
        try:
            data_key = params.get('data_key', '')
            old_value = params.get('old_value')
            new_value = params.get('new_value')
            reactions = params.get('reactions', [])
            check_fn = params.get('check_fn')

            if not data_key:
                return ActionResult(
                    success=False,
                    message="data_key is required",
                    duration=time.time() - start_time,
                )

            # Check if condition is met
            should_react = False
            if callable(check_fn):
                try:
                    should_react = check_fn(old_value, new_value, context)
                except Exception:
                    should_react = False
            elif new_value != old_value:
                should_react = True

            if not should_react:
                return ActionResult(
                    success=True,
                    message="No reaction triggered",
                    data={'key': data_key, 'changed': new_value != old_value, 'reacted': False},
                    duration=time.time() - start_time,
                )

            # Execute reactions
            results = []
            for reaction in reactions:
                reaction_name = reaction.get('name', 'reaction')
                action = reaction.get('action')
                action_params = reaction.get('params', {})

                try:
                    if callable(action):
                        result = action({'key': data_key, 'old': old_value, 'new': new_value}, context)
                        results.append({'name': reaction_name, 'success': True, 'result': result})
                    elif hasattr(context, 'execute_action'):
                        result = context.execute_action(action, action_params)
                        results.append({'name': reaction_name, 'success': result.success if isinstance(result, ActionResult) else False})
                except Exception as e:
                    results.append({'name': reaction_name, 'success': False, 'error': str(e)})

            success_count = sum(1 for r in results if r.get('success'))

            duration = time.time() - start_time
            return ActionResult(
                success=success_count == len(results),
                message=f"Reacted to {data_key} change: {success_count}/{len(results)} succeeded",
                data={
                    'key': data_key,
                    'reacted': True,
                    'reactions': results,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Reactive automation error: {str(e)}",
                duration=duration,
            )
