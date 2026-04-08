"""Data Listener Action Module for RabAI AutoClick.

Event listener system for monitoring and reacting to
data changes, file modifications, and system events.
"""

import time
import threading
import os
import sys
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventType:
    """Event types for the listener system."""
    DATA_CHANGED = "data_changed"
    FILE_CREATED = "file_created"
    FILE_MODIFIED = "file_modified"
    FILE_DELETED = "file_deleted"
    TIMER = "timer"
    CUSTOM = "custom"


class DataListenerAction(BaseAction):
    """Event listener system for data and system monitoring.

    Register listeners for various event types and react to
    changes in data, files, or time-based triggers. Supports
    both polling and callback-based notification.
    """
    action_type = "data_listener"
    display_name = "数据监听器"
    description = "数据变更和系统事件监听"

    _listeners: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    _listener_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {'triggered': 0, 'errors': 0})
    _running_listeners: Dict[str, threading.Event] = {}
    _listener_threads: Dict[str, threading.Thread] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute listener operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'listen', 'stop', 'trigger', 'list', 'stats'
                - event_type: str - event type to listen for
                - listener_id: str - unique listener identifier
                - callback: callable (optional) - callback function
                - handler: str (optional) - handler name for registered callbacks
                - path: str (optional) - file/directory path for file events
                - interval: float (optional) - poll interval for timer events
                - data: Any (optional) - data to pass with trigger

        Returns:
            ActionResult with listener operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'listen')

            if operation == 'listen':
                return self._start_listener(params, start_time)
            elif operation == 'stop':
                return self._stop_listener(params, start_time)
            elif operation == 'trigger':
                return self._trigger_listeners(params, start_time)
            elif operation == 'list':
                return self._list_listeners(params, start_time)
            elif operation == 'stats':
                return self._get_stats(start_time)
            elif operation == 'stop_all':
                return self._stop_all_listeners(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Listener action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _start_listener(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Start an event listener."""
        listener_id = params.get('listener_id', f'listener_{time.time()}')
        event_type = params.get('event_type', EventType.CUSTOM)
        handler = params.get('handler', '')
        path = params.get('path', '')
        interval = params.get('interval', 5.0)
        persistent = params.get('persistent', False)

        if listener_id in self._running_listeners:
            return ActionResult(
                success=False,
                message=f"Listener already running: {listener_id}",
                data={'listener_id': listener_id},
                duration=time.time() - start_time
            )

        listener_config = {
            'listener_id': listener_id,
            'event_type': event_type,
            'handler': handler,
            'path': path,
            'interval': interval,
            'persistent': persistent,
            'started_at': time.time()
        }

        self._listeners[event_type].append(listener_config)

        stop_event = threading.Event()
        self._running_listeners[listener_id] = stop_event

        if event_type == EventType.TIMER and persistent:
            thread = threading.Thread(
                target=self._timer_loop,
                args=(listener_id, listener_config, stop_event),
                daemon=True
            )
            thread.start()
            self._listener_threads[listener_id] = thread

        return ActionResult(
            success=True,
            message=f"Listener started: {listener_id}",
            data={
                'listener_id': listener_id,
                'event_type': event_type,
                'persistent': persistent
            },
            duration=time.time() - start_time
        )

    def _stop_listener(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stop a running listener."""
        listener_id = params.get('listener_id', '')

        if listener_id not in self._running_listeners:
            return ActionResult(
                success=False,
                message=f"Listener not running: {listener_id}",
                duration=time.time() - start_time
            )

        self._running_listeners[listener_id].set()

        if listener_id in self._listener_threads:
            self._listener_threads[listener_id].join(timeout=2.0)
            del self._listener_threads[listener_id]

        del self._running_listeners[listener_id]

        return ActionResult(
            success=True,
            message=f"Listener stopped: {listener_id}",
            duration=time.time() - start_time
        )

    def _trigger_listeners(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Manually trigger listeners for an event type."""
        event_type = params.get('event_type', EventType.CUSTOM)
        data = params.get('data')

        if event_type not in self._listeners or not self._listeners[event_type]:
            return ActionResult(
                success=True,
                message=f"No listeners for event type: {event_type}",
                data={'triggered': 0},
                duration=time.time() - start_time
            )

        triggered = 0
        errors = 0
        results = []

        for listener in self._listeners[event_type]:
            listener_id = listener['listener_id']

            if listener_id in self._running_listeners:
                if self._running_listeners[listener_id].is_set():
                    continue

            try:
                result = self._invoke_listener(listener, event_type, data)
                results.append({'listener_id': listener_id, 'success': True, 'result': result})
                triggered += 1
                self._listener_stats[event_type]['triggered'] += 1
            except Exception as e:
                results.append({'listener_id': listener_id, 'success': False, 'error': str(e)})
                errors += 1
                self._listener_stats[event_type]['errors'] += 1

        return ActionResult(
            success=True,
            message=f"Triggered {triggered} listeners, {errors} errors",
            data={
                'event_type': event_type,
                'triggered': triggered,
                'errors': errors,
                'results': results
            },
            duration=time.time() - start_time
        )

    def _list_listeners(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all registered listeners."""
        event_type = params.get('event_type')

        if event_type:
            listeners = self._listeners.get(event_type, [])
            return ActionResult(
                success=True,
                message=f"Listeners for {event_type}: {len(listeners)}",
                data={
                    'event_type': event_type,
                    'listeners': [
                        {
                            'listener_id': l['listener_id'],
                            'handler': l['handler'],
                            'started_at': l['started_at']
                        }
                        for l in listeners
                    ]
                },
                duration=time.time() - start_time
            )
        else:
            all_listeners = {
                et: len(listeners) for et, listeners in self._listeners.items()
            }
            return ActionResult(
                success=True,
                message=f"Total event types with listeners: {len(all_listeners)}",
                data={'event_types': all_listeners},
                duration=time.time() - start_time
            )

    def _get_stats(self, start_time: float) -> ActionResult:
        """Get listener statistics."""
        return ActionResult(
            success=True,
            message="Listener statistics",
            data={'stats': dict(self._listener_stats)},
            duration=time.time() - start_time
        )

    def _stop_all_listeners(self, start_time: float) -> ActionResult:
        """Stop all running listeners."""
        count = len(self._running_listeners)
        for listener_id in list(self._running_listeners.keys()):
            self._running_listeners[listener_id].set()

        for thread in self._listener_threads.values():
            thread.join(timeout=2.0)

        self._running_listeners.clear()
        self._listener_threads.clear()

        return ActionResult(
            success=True,
            message=f"Stopped {count} listeners",
            data={'stopped': count},
            duration=time.time() - start_time
        )

    def _timer_loop(
        self,
        listener_id: str,
        config: Dict[str, Any],
        stop_event: threading.Event
    ) -> None:
        """Timer event loop."""
        interval = config['interval']
        while not stop_event.is_set():
            stop_event.wait(timeout=interval)
            if not stop_event.is_set():
                self._trigger_listeners(
                    {'event_type': EventType.TIMER, 'data': {'listener_id': listener_id}},
                    time.time()
                )

    def _invoke_listener(
        self,
        listener: Dict[str, Any],
        event_type: str,
        data: Any
    ) -> Any:
        """Invoke a listener's handler."""
        handler = listener['handler']

        if handler == 'log':
            return f"Event logged: {event_type} with data {data}"
        elif handler == 'count':
            return {'count': 1}
        elif handler == 'timestamp':
            return {'timestamp': time.time()}
        else:
            return {'handled': True}
