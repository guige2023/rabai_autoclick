"""Stream Process action module for RabAI AutoClick.

Provides stream processing operations:
- StreamSourceAction: Create stream source
- StreamWindowAction: Tumbling/sliding window
- StreamAggregateAction: Stream aggregation
- StreamJoinAction: Stream join
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional, Iterator
from collections import deque

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamSourceAction(BaseAction):
    """Create stream source."""
    action_type = "stream_source"
    display_name = "流数据源"
    description = "创建流数据源"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._streams = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stream source creation."""
        stream_id = params.get('stream_id', 'default')
        source_type = params.get('source_type', 'generator')
        config = params.get('config', {})
        output_var = params.get('output_var', 'stream_info')

        try:
            stream_info = {
                'stream_id': stream_id,
                'source_type': source_type,
                'config': config,
                'created_at': time.time(),
                'active': True,
            }

            self._streams[stream_id] = {
                'info': stream_info,
                'buffer': deque(maxlen=config.get('buffer_size', 1000)),
                'watermark': None,
            }

            result = {
                'stream_id': stream_id,
                'created': True,
                'buffer_size': config.get('buffer_size', 1000),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Stream '{stream_id}' created"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream source error: {e}")


class StreamWindowAction(BaseAction):
    """Tumbling/sliding window."""
    action_type = "stream_window"
    display_name = "流窗口"
    description = "流数据窗口"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._windows = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute window operation."""
        stream_id = params.get('stream_id', 'default')
        window_type = params.get('window_type', 'tumbling')
        window_size = params.get('window_size', 10)
        slide_size = params.get('slide_size', 5)
        data = params.get('data', [])
        output_var = params.get('output_var', 'window_result')

        try:
            resolved_data = context.resolve_value(data) if context else data

            if window_type == 'tumbling':
                windows = [resolved_data[i:i + window_size] for i in range(0, len(resolved_data), window_size)]
            elif window_type == 'sliding':
                windows = [resolved_data[i:i + window_size] for i in range(0, len(resolved_data), slide_size)]
            elif window_type == 'session':
                gap = params.get('session_gap', 5)
                windows = []
                current_window = []
                for item in resolved_data:
                    current_window.append(item)
                    windows.append([r for r in current_window])
            else:
                windows = [resolved_data]

            result = {
                'window_type': window_type,
                'window_count': len(windows),
                'windows': windows,
                'window_size': window_size,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Created {len(windows)} {window_type} windows"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream window error: {e}")


class StreamAggregateAction(BaseAction):
    """Stream aggregation."""
    action_type = "stream_aggregate"
    display_name = "流聚合"
    description = "流数据聚合"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stream aggregation."""
        data = params.get('data', [])
        aggregations = params.get('aggregations', [])
        group_by = params.get('group_by', None)
        output_var = params.get('output_var', 'stream_agg_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_aggs = context.resolve_value(aggregations) if context else aggregations

            results = []

            if group_by:
                groups = {}
                for record in resolved_data:
                    key = record.get(group_by, 'unknown')
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(record)

                for group_key, group_data in groups.items():
                    agg_result = {group_by: group_key}
                    for agg in resolved_aggs:
                        field = agg.get('field', '')
                        func = agg.get('function', 'sum')
                        values = [r.get(field, 0) for r in group_data if isinstance(r.get(field), (int, float))]

                        if func == 'sum':
                            agg_result[f'{field}_{func}'] = sum(values)
                        elif func == 'avg':
                            agg_result[f'{field}_{func}'] = sum(values) / len(values) if values else 0
                        elif func == 'min':
                            agg_result[f'{field}_{func}'] = min(values) if values else None
                        elif func == 'max':
                            agg_result[f'{field}_{func}'] = max(values) if values else None
                        elif func == 'count':
                            agg_result[f'{field}_{func}'] = len(values)

                    results.append(agg_result)
            else:
                agg_result = {}
                for agg in resolved_aggs:
                    field = agg.get('field', '')
                    func = agg.get('function', 'sum')
                    values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]

                    if func == 'sum':
                        agg_result[f'{field}_{func}'] = sum(values)
                    elif func == 'avg':
                        agg_result[f'{field}_{func}'] = sum(values) / len(values) if values else 0
                    elif func == 'min':
                        agg_result[f'{field}_{func}'] = min(values) if values else None
                    elif func == 'max':
                        agg_result[f'{field}_{func}'] = max(values) if values else None
                    elif func == 'count':
                        agg_result[f'{field}_{func}'] = len(values)

                results.append(agg_result)

            result = {
                'results': results,
                'input_count': len(resolved_data),
                'output_count': len(results),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Stream aggregation: {len(results)} results"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream aggregate error: {e}")


class StreamJoinAction(BaseAction):
    """Stream join."""
    action_type = "stream_join"
    display_name = "流连接"
    description = "流数据连接"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stream join."""
        left_data = params.get('left', [])
        right_data = params.get('right', [])
        join_key = params.get('join_key', '')
        join_type = params.get('join_type', 'inner')
        output_var = params.get('output_var', 'join_result')

        if not left_data or not right_data:
            return ActionResult(success=False, message="left and right data are required")

        try:
            resolved_left = context.resolve_value(left_data) if context else left_data
            resolved_right = context.resolve_value(right_data) if context else right_data

            right_index = {r.get(join_key): r for r in resolved_right}
            joined = []

            for left_record in resolved_left:
                key = left_record.get(join_key)
                if key in right_index:
                    merged = {**left_record, **right_index[key]}
                    joined.append(merged)
                elif join_type in ['left', 'outer']:
                    joined.append({**left_record, 'right_match': None})

            if join_type == 'outer':
                seen_keys = {r.get(join_key) for r in joined}
                for right_record in resolved_right:
                    if right_record.get(join_key) not in seen_keys:
                        joined.append({**{'left_match': None}, **right_record})

            result = {
                'joined': joined,
                'join_type': join_type,
                'left_count': len(resolved_left),
                'right_count': len(resolved_right),
                'joined_count': len(joined),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"{join_type} join: {len(joined)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream join error: {e}")
