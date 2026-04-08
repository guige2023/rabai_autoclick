"""Automation Observer Action Module.

Provides observer pattern implementation for automation
event handling, notification dispatching, and state change monitoring.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventObserverAction(BaseAction):
    """Implement observer pattern for event handling.
    
    Supports event subscription, notification dispatching, and observer management.
    """
    action_type = "event_observer"
    display_name = "事件观察者"
    description = "实现事件处理观察者模式"

    def __init__(self):
        super().__init__()
        self._observers: Dict[str, List[Dict]] = defaultdict(list)
        self._event_history: List[Dict] = []
        self._observer_id_counter = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage event observers.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'subscribe', 'unsubscribe', 'notify', 'get_observers'.
                - event_type: Type of event to observe.
                - observer_id: Observer identifier.
                - handler_var: Variable containing handler function.
                - event_data: Event data to notify.
                - priority: Observer priority.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with observer result or error.
        """
        operation = params.get('operation', 'subscribe')
        event_type = params.get('event_type', '')
        observer_id = params.get('observer_id', '')
        handler_var = params.get('handler_var', '')
        event_data = params.get('event_data', {})
        priority = params.get('priority', 0)
        output_var = params.get('output_var', 'observer_result')

        try:
            if operation == 'subscribe':
                return self._subscribe(
                    event_type, observer_id, handler_var, priority, context, output_var
                )
            elif operation == 'unsubscribe':
                return self._unsubscribe(event_type, observer_id, output_var)
            elif operation == 'notify':
                return self._notify(event_type, event_data, context, output_var)
            elif operation == 'get_observers':
                return self._get_observers(event_type, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Event observer failed: {str(e)}"
            )

    def _subscribe(
        self,
        event_type: str,
        observer_id: str,
        handler_var: str,
        priority: int,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Subscribe an observer to an event type."""
        if not event_type or not observer_id:
            return ActionResult(
                success=False,
                message="event_type and observer_id are required"
            )

        handler = None
        if handler_var:
            handler = context.variables.get(handler_var)

        observer = {
            'id': observer_id,
            'handler': handler,
            'priority': priority,
            'subscribed_at': datetime.now().isoformat(),
            'notification_count': 0
        }

        self._observers[event_type].append(observer)
        # Sort by priority (higher first)
        self._observers[event_type].sort(key=lambda x: x['priority'], reverse=True)

        result = {
            'event_type': event_type,
            'observer_id': observer_id,
            'subscribed': True
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Observer '{observer_id}' subscribed to '{event_type}'"
        )

    def _unsubscribe(
        self, event_type: str, observer_id: str, output_var: str
    ) -> ActionResult:
        """Unsubscribe an observer from an event type."""
        if event_type not in self._observers:
            return ActionResult(
                success=False,
                message=f"No observers for event type '{event_type}'"
            )

        initial_count = len(self._observers[event_type])
        self._observers[event_type] = [
            obs for obs in self._observers[event_type]
            if obs['id'] != observer_id
        ]
        removed = initial_count - len(self._observers[event_type])

        context.variables[output_var] = {
            'event_type': event_type,
            'observer_id': observer_id,
            'unsubscribed': removed > 0
        }
        return ActionResult(
            success=removed > 0,
            data={'unsubscribed': removed > 0},
            message=f"Observer '{observer_id}' unsubscribed from '{event_type}'"
        )

    def _notify(
        self,
        event_type: str,
        event_data: Dict,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Notify all observers of an event."""
        observers = self._observers.get(event_type, [])

        if not observers:
            return ActionResult(
                success=True,
                data={'notified': 0},
                message=f"No observers for event '{event_type}'"
            )

        # Record event
        event_record = {
            'type': event_type,
            'data': event_data,
            'timestamp': datetime.now().isoformat(),
            'observer_count': len(observers)
        }
        self._event_history.append(event_record)

        # Keep last 1000 events
        if len(self._event_history) > 1000:
            self._event_history = self._event_history[-1000:]

        # Notify observers
        results = []
        for observer in observers:
            try:
                if observer.get('handler'):
                    result = observer['handler'](event_data)
                    observer['notification_count'] += 1
                    results.append({
                        'observer_id': observer['id'],
                        'success': True,
                        'result': result
                    })
            except Exception as e:
                results.append({
                    'observer_id': observer['id'],
                    'success': False,
                    'error': str(e)
                })

        success_count = sum(1 for r in results if r.get('success', False))

        result = {
            'event_type': event_type,
            'notified': len(observers),
            'successful': success_count,
            'failed': len(observers) - success_count,
            'results': results
        }

        context.variables[output_var] = result
        return ActionResult(
            success=success_count == len(observers),
            data=result,
            message=f"Notified {success_count}/{len(observers)} observers for '{event_type}'"
        )

    def _get_observers(self, event_type: str, output_var: str) -> ActionResult:
        """Get all observers for an event type."""
        observers = self._observers.get(event_type, [])

        result = {
            'event_type': event_type,
            'observers': [
                {
                    'id': obs['id'],
                    'priority': obs['priority'],
                    'notification_count': obs.get('notification_count', 0)
                }
                for obs in observers
            ],
            'count': len(observers)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved {len(observers)} observers for '{event_type}'"
        )


class NotificationDispatcherAction(BaseAction):
    """Dispatch notifications to various channels.
    
    Supports email, SMS, webhook, and in-app notifications.
    """
    action_type = "notification_dispatcher"
    display_name = "通知分发"
    description = "向各种渠道分发通知"

    def __init__(self):
        super().__init__()
        self._channels: Dict[str, Dict] = {}
        self._notification_history: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Dispatch notifications.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'configure_channel', 'send', 'send_batch', 'get_history'.
                - channel: Channel type ('email', 'sms', 'webhook').
                - channel_config: Channel configuration.
                - recipient: Recipient identifier.
                - message: Notification message.
                - priority: Notification priority.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with notification result or error.
        """
        operation = params.get('operation', 'send')
        channel = params.get('channel', 'webhook')
        channel_config = params.get('channel_config', {})
        recipient = params.get('recipient', '')
        message = params.get('message', '')
        priority = params.get('priority', 'normal')
        output_var = params.get('output_var', 'notification_result')

        try:
            if operation == 'configure_channel':
                return self._configure_channel(channel, channel_config, output_var)
            elif operation == 'send':
                return self._send_notification(
                    channel, recipient, message, priority, output_var
                )
            elif operation == 'send_batch':
                return self._send_batch_notifications(
                    channel, params.get('notifications', []), output_var
                )
            elif operation == 'get_history':
                return self._get_notification_history(
                    params.get('limit', 100), output_var
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Notification dispatcher failed: {str(e)}"
            )

    def _configure_channel(
        self, channel: str, config: Dict, output_var: str
    ) -> ActionResult:
        """Configure a notification channel."""
        self._channels[channel] = {
            'channel': channel,
            'config': config,
            'enabled': config.get('enabled', True),
            'configured_at': datetime.now().isoformat()
        }

        context.variables[output_var] = {
            'channel': channel,
            'configured': True
        }
        return ActionResult(
            success=True,
            data={'channel': channel, 'configured': True},
            message=f"Channel '{channel}' configured"
        )

    def _send_notification(
        self,
        channel: str,
        recipient: str,
        message: str,
        priority: str,
        output_var: str
    ) -> ActionResult:
        """Send a single notification."""
        if channel not in self._channels:
            return ActionResult(
                success=False,
                message=f"Channel '{channel}' not configured"
            )

        # Simulate notification sending
        notification = {
            'id': f"notif_{int(time.time() * 1000)}",
            'channel': channel,
            'recipient': recipient,
            'message': message,
            'priority': priority,
            'sent_at': datetime.now().isoformat(),
            'status': 'sent'
        }

        self._notification_history.append(notification)

        # Keep last 1000 notifications
        if len(self._notification_history) > 1000:
            self._notification_history = self._notification_history[-1000:]

        context.variables[output_var] = notification
        return ActionResult(
            success=True,
            data=notification,
            message=f"Notification sent via '{channel}' to '{recipient}'"
        )

    def _send_batch_notifications(
        self, channel: str, notifications: List[Dict], output_var: str
    ) -> ActionResult:
        """Send batch notifications."""
        if channel not in self._channels:
            return ActionResult(
                success=False,
                message=f"Channel '{channel}' not configured"
            )

        results = []
        for notif in notifications:
            result = self._send_notification(
                channel,
                notif.get('recipient', ''),
                notif.get('message', ''),
                notif.get('priority', 'normal'),
                'temp_notif'
            )
            results.append(result.data if result.data else {})

        sent_count = sum(1 for r in results if r.get('status') == 'sent')

        result = {
            'channel': channel,
            'total': len(notifications),
            'sent': sent_count,
            'failed': len(notifications) - sent_count
        }

        context.variables[output_var] = result
        return ActionResult(
            success=sent_count == len(notifications),
            data=result,
            message=f"Batch notification: {sent_count}/{len(notifications)} sent"
        )

    def _get_notification_history(
        self, limit: int, output_var: str
    ) -> ActionResult:
        """Get notification history."""
        history = self._notification_history[-limit:]

        result = {
            'history': history,
            'count': len(history)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved {len(history)} notification records"
        )


class StateObserverAction(BaseAction):
    """Observe and track state changes.
    
    Supports state diff calculation and change notification.
    """
    action_type = "state_observer"
    display_name = "状态观察"
    description = "观察和跟踪状态变化"

    def __init__(self):
        super().__init__()
        self._states: Dict[str, Any] = {}
        self._state_history: Dict[str, List[Dict]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Observe state changes.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'set_state', 'get_state', 'observe_changes', 'get_history'.
                - state_id: State identifier.
                - state_data: State data to set.
                - track_history: Whether to track history.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with state observation result or error.
        """
        operation = params.get('operation', 'set_state')
        state_id = params.get('state_id', '')
        state_data = params.get('state_data', {})
        track_history = params.get('track_history', True)
        output_var = params.get('output_var', 'state_result')

        try:
            if operation == 'set_state':
                return self._set_state(state_id, state_data, track_history, output_var)
            elif operation == 'get_state':
                return self._get_state(state_id, output_var)
            elif operation == 'observe_changes':
                return self._observe_changes(state_id, state_data, track_history, output_var)
            elif operation == 'get_history':
                return self._get_state_history(state_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"State observer failed: {str(e)}"
            )

    def _set_state(
        self,
        state_id: str,
        state_data: Any,
        track_history: bool,
        output_var: str
    ) -> ActionResult:
        """Set state data."""
        old_state = self._states.get(state_id)

        self._states[state_id] = state_data

        if track_history and old_state is not None:
            self._state_history[state_id].append({
                'previous_state': old_state,
                'new_state': state_data,
                'changed_at': datetime.now().isoformat()
            })

            # Keep last 100 changes
            if len(self._state_history[state_id]) > 100:
                self._state_history[state_id] = self._state_history[state_id][-100:]

        result = {
            'state_id': state_id,
            'set': True,
            'previous_state': old_state
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"State '{state_id}' updated"
        )

    def _get_state(self, state_id: str, output_var: str) -> ActionResult:
        """Get current state."""
        state = self._states.get(state_id)

        if state is None:
            return ActionResult(
                success=False,
                message=f"State '{state_id}' not found"
            )

        context.variables[output_var] = {
            'state_id': state_id,
            'state': state
        }
        return ActionResult(
            success=True,
            data={'state_id': state_id, 'state': state},
            message=f"Retrieved state '{state_id}'"
        )

    def _observe_changes(
        self,
        state_id: str,
        new_data: Any,
        track_history: bool,
        output_var: str
    ) -> ActionResult:
        """Observe state changes and return diff."""
        old_state = self._states.get(state_id)
        changes = self._calculate_diff(old_state, new_data)

        self._states[state_id] = new_data

        if track_history:
            self._state_history[state_id].append({
                'previous_state': old_state,
                'new_state': new_data,
                'changes': changes,
                'changed_at': datetime.now().isoformat()
            })

        result = {
            'state_id': state_id,
            'has_changes': len(changes) > 0,
            'changes': changes,
            'previous_state': old_state,
            'new_state': new_data
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"State '{state_id}' changes: {len(changes)} fields changed"
        )

    def _calculate_diff(self, old: Any, new: Any) -> Dict:
        """Calculate difference between old and new state."""
        if old is None:
            return {'full_replacement': True}

        if not isinstance(old, dict) or not isinstance(new, dict):
            if old != new:
                return {'value_changed': {'old': old, 'new': new}}
            return {}

        changes = {}
        all_keys = set(old.keys()) | set(new.keys())

        for key in all_keys:
            if key not in old:
                changes[key] = {'added': new[key]}
            elif key not in new:
                changes[key] = {'removed': old[key]}
            elif old[key] != new[key]:
                changes[key] = {'changed': {'old': old[key], 'new': new[key]}}

        return changes

    def _get_state_history(self, state_id: str, output_var: str) -> ActionResult:
        """Get state change history."""
        history = self._state_history.get(state_id, [])

        context.variables[output_var] = {
            'state_id': state_id,
            'history': history,
            'count': len(history)
        }
        return ActionResult(
            success=True,
            data={'state_id': state_id, 'history': history, 'count': len(history)},
            message=f"Retrieved {len(history)} state changes for '{state_id}'"
        )
