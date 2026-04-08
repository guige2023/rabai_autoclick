"""Data Window action module for RabAI AutoClick.

Time-window and count-window aggregations for streaming
and batch data processing.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional
from collections import deque
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataWindowAction(BaseAction):
    """Window data for aggregation operations.

    Tumbling, sliding, and session windows with
    time-based and count-based boundaries.
    """
    action_type = "data_window"
    display_name = "数据窗口"
    description = "用于聚合操作的数据窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply window operation.

        Args:
            context: Execution context.
            params: Dict with keys: action (add/get/clear), window_id,
                   window_type (tumbling/sliding/session),
                   window_size, window_data, aggregate_fn.

        Returns:
            ActionResult with window result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'add')
            window_id = params.get('window_id', 'default')
            window_type = params.get('window_type', 'tumbling')
            window_size = params.get('window_size', 10)
            window_data = params.get('window_data')
            aggregate_fn = params.get('aggregate_fn')
            window_ttl = params.get('window_ttl_seconds', 3600)

            if not hasattr(context, '_data_windows'):
                context._data_windows = {}

            if window_id not in context._data_windows:
                context._data_windows[window_id] = {
                    'type': window_type,
                    'size': window_size,
                    'data': deque(maxlen=window_size if window_type == 'tumbling' else window_size * 2),
                    'created_at': time.time(),
                    'windows': [],
                }

            w = context._data_windows[window_id]
            now = time.time()

            if action == 'add':
                if window_data is None:
                    return ActionResult(success=False, message="window_data required", duration=time.time() - start_time)
                w['data'].append({'value': window_data, 'timestamp': now})
                return ActionResult(success=True, message=f"Added to window {window_id}", data={'window_size': len(w['data']), 'total': window_size}, duration=time.time() - start_time)

            elif action == 'get':
                items = list(w['data'])
                if callable(aggregate_fn):
                    result = aggregate_fn(items, context)
                elif window_type == 'tumbling':
                    result = items[-window_size:] if len(items) >= window_size else items
                else:
                    result = items
                return ActionResult(success=True, message=f"Window {window_id}: {len(items)} items", data={'items': result, 'count': len(items)}, duration=time.time() - start_time)

            elif action == 'clear':
                w['data'].clear()
                return ActionResult(success=True, message=f"Cleared window {window_id}", duration=time.time() - start_time)

            elif action == 'aggregate':
                items = list(w['data'])
                if not items:
                    return ActionResult(success=True, message="Empty window", data={'result': None}, duration=time.time() - start_time)
                agg_type = params.get('agg_type', 'sum')
                values = [item['value'] for item in items if isinstance(item['value'], (int, float))]
                if agg_type == 'sum':
                    result = sum(values) if values else None
                elif agg_type == 'avg':
                    result = sum(values) / len(values) if values else None
                elif agg_type == 'min':
                    result = min(values) if values else None
                elif agg_type == 'max':
                    result = max(values) if values else None
                elif agg_type == 'count':
                    result = len(values)
                else:
                    result = values
                return ActionResult(success=True, message=f"Aggregated ({agg_type}): {result}", data={'result': result, 'type': agg_type, 'window_count': len(items)}, duration=time.time() - start_time)

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Window error: {str(e)}", duration=time.time() - start_time)


class DataSessionWindowAction(BaseAction):
    """Session window with gap-based window closing.

    Groups events within gaps of inactivity.
    """
    action_type = "data_session_window"
    display_name = "会话窗口"
    description = "基于非活跃间隔的会话窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage session window.

        Args:
            context: Execution context.
            params: Dict with keys: action (add_event/close_session/get),
                   session_id, event, gap_seconds.

        Returns:
            ActionResult with session data.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'add_event')
            session_id = params.get('session_id', 'default')
            event = params.get('event')
            gap_seconds = params.get('gap_seconds', 300)

            if not hasattr(context, '_session_windows'):
                context._session_windows = {}
            if session_id not in context._session_windows:
                context._session_windows[session_id] = {'events': [], 'last_event_time': None, 'closed': False}

            sess = context._session_windows[session_id]
            now = time.time()

            if action == 'add_event':
                if event is None:
                    return ActionResult(success=False, message="event required", duration=time.time() - start_time)
                if sess['last_event_time'] and (now - sess['last_event_time']) > gap_seconds:
                    sess['closed'] = True
                    sess['events'] = []
                sess['events'].append({'event': event, 'timestamp': now})
                sess['last_event_time'] = now
                sess['closed'] = False
                return ActionResult(success=True, message=f"Event added to session {session_id}", data={'events': len(sess['events']), 'session_closed': sess['closed']}, duration=time.time() - start_time)

            elif action == 'close_session':
                sess['closed'] = True
                events = sess['events']
                sess['events'] = []
                return ActionResult(success=True, message=f"Session {session_id} closed", data={'events': events, 'count': len(events)}, duration=time.time() - start_time)

            elif action == 'get':
                return ActionResult(success=True, message=f"Session {session_id}: {len(sess['events'])} events", data={'events': sess['events'], 'count': len(sess['events']), 'closed': sess['closed']}, duration=time.time() - start_time)

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Session window error: {str(e)}", duration=time.time() - start_time)
