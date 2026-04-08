"""Window action module for RabAI AutoClick.

Provides windowing operations for sequence data:
sliding windows, tumbling windows, session windows,
and windowed aggregations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SlidingWindowAction(BaseAction):
    """Create sliding windows over a sequence.
    
    Slide a fixed-size window step by step over the data,
    producing overlapping windows.
    """
    action_type = "sliding_window"
    display_name = "滑动窗口"
    description = "创建滑动窗口，支持步进和重叠窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create sliding windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - size: int (window size)
                - step: int (step size, default 1)
                - drop_incomplete: bool (drop last incomplete window)
                - save_to_var: str
        
        Returns:
            ActionResult with windowed data.
        """
        data = params.get('data', [])
        size = params.get('size', 5)
        step = params.get('step', 1)
        drop_incomplete = params.get('drop_incomplete', False)
        save_to_var = params.get('save_to_var', 'window_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        if size <= 0 or step <= 0:
            return ActionResult(success=False, message="size and step must be positive")

        windows = []
        i = 0
        while i + size <= len(data):
            window = data[i:i + size]
            windows.append({
                'index': len(windows),
                'start': i,
                'end': i + size - 1,
                'data': window,
                'size': len(window),
            })
            i += step

        if not drop_incomplete and i < len(data):
            # Last incomplete window
            window = data[i:]
            windows.append({
                'index': len(windows),
                'start': i,
                'end': len(data) - 1,
                'data': window,
                'size': len(window),
                'incomplete': True,
            })

        if context and save_to_var:
            context.variables[save_to_var] = windows

        return ActionResult(
            success=True,
            data={'window_count': len(windows), 'windows': windows},
            message=f"Created {len(windows)} sliding windows of size {size}"
        )


class TumblingWindowAction(BaseAction):
    """Create non-overlapping tumbling windows.
    
    Partition data into fixed-size, non-overlapping chunks.
    """
    action_type = "tumbling_window"
    display_name = "滚动窗口"
    description = "创建非重叠滚动窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create tumbling windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - size: int (window size)
                - save_to_var: str
        
        Returns:
            ActionResult with windowed data.
        """
        data = params.get('data', [])
        size = params.get('size', 5)
        save_to_var = params.get('save_to_var', 'window_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        windows = []
        for i in range(0, len(data), size):
            chunk = data[i:i + size]
            windows.append({
                'index': len(windows),
                'start': i,
                'end': min(i + size - 1, len(data) - 1),
                'data': chunk,
                'size': len(chunk),
            })

        if context and save_to_var:
            context.variables[save_to_var] = windows

        return ActionResult(
            success=True,
            data={'window_count': len(windows), 'windows': windows},
            message=f"Created {len(windows)} tumbling windows of size {size}"
        )


class SessionWindowAction(BaseAction):
    """Create session windows based on gap detection.
    
    Group consecutive items into windows separated by gaps
    larger than a threshold.
    """
    action_type = "session_window"
    display_name = "会话窗口"
    description = "基于间隔检测创建会话窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create session windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items (must have timestamp or index)
                - gap: int or float (gap threshold)
                - gap_unit: str (items/ms/s/min/h, default items)
                - time_field: str (timestamp field name, optional)
                - save_to_var: str
        
        Returns:
            ActionResult with session windows.
        """
        data = params.get('data', [])
        gap = params.get('gap', 1)
        gap_unit = params.get('gap_unit', 'items')
        time_field = params.get('time_field', '')
        save_to_var = params.get('save_to_var', 'session_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        sessions = []
        current_session = [data[0]]
        
        for i in range(1, len(data)):
            item = data[i]
            prev = data[i - 1]
            
            if time_field:
                # Time-based gap detection
                t1 = self._get_timestamp(prev, time_field)
                t2 = self._get_timestamp(item, time_field)
                if t1 is not None and t2 is not None:
                    diff = t2 - t1
                    threshold = self._to_ms(gap, gap_unit)
                    if diff > threshold:
                        sessions.append(current_session)
                        current_session = [item]
                    else:
                        current_session.append(item)
                else:
                    current_session.append(item)
            else:
                # Index-based gap
                if gap_unit == 'items':
                    if i - (len(current_session) + sessions.count([])) > gap:
                        sessions.append(current_session)
                        current_session = [item]
                    else:
                        current_session.append(item)
                else:
                    current_session.append(item)

        if current_session:
            sessions.append(current_session)

        # Build result
        result = []
        for idx, session in enumerate(sessions):
            result.append({
                'index': idx,
                'session_id': f'session_{idx}',
                'data': session,
                'size': len(session),
                'start_idx': data.index(session[0]) if session else 0,
            })

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'session_count': len(result), 'sessions': result},
            message=f"Created {len(result)} session windows"
        )

    def _get_timestamp(self, record: Any, field: str):
        """Extract timestamp from record."""
        if isinstance(record, dict):
            val = record.get(field)
            if isinstance(val, (int, float)):
                return val
            elif isinstance(val, str):
                try:
                    return datetime.fromisoformat(val).timestamp()
                except:
                    return None
        elif isinstance(record, (int, float)):
            return record
        return None

    def _to_ms(self, value: float, unit: str) -> float:
        """Convert value to milliseconds."""
        unit = unit.lower()
        if unit == 'ms':
            return value
        elif unit == 's':
            return value * 1000
        elif unit == 'min':
            return value * 60000
        elif unit == 'h':
            return value * 3600000
        return value


class WindowAggregateAction(BaseAction):
    """Apply aggregation over windows.
    
    Take sliding/tumbling windows and compute
    per-window aggregations (sum, avg, etc.).
    """
    action_type = "window_aggregate"
    display_name = "窗口聚合"
    description = "对窗口数据应用聚合计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate over windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - windows: list of window dicts (from window actions)
                - value_field: str (field to aggregate)
                - agg_func: str (sum/avg/count/min/max/first/last)
                - save_to_var: str
        
        Returns:
            ActionResult with aggregated windows.
        """
        windows = params.get('windows', [])
        value_field = params.get('value_field', '')
        agg_func = params.get('agg_func', 'sum')
        save_to_var = params.get('save_to_var', 'agg_result')

        if not windows:
            return ActionResult(success=False, message="No windows provided")

        results = []
        for w in windows:
            window_data = w.get('data', [])
            
            if not value_field:
                # Count items
                agg_value = len(window_data)
            else:
                values = []
                for item in window_data:
                    val = item.get(value_field) if isinstance(item, dict) else item
                    if val is not None:
                        try:
                            values.append(float(val))
                        except (ValueError, TypeError):
                            pass
                
                agg_value = self._compute_agg(values, agg_func)

            results.append({
                'index': w.get('index', len(results)),
                'start': w.get('start'),
                'end': w.get('end'),
                'window_size': w.get('size', len(window_data)),
                'aggregate': agg_value,
                'agg_func': agg_func,
            })

        total_agg = self._compute_agg(
            [r['aggregate'] for r in results if r['aggregate'] is not None],
            agg_func
        )

        if context and save_to_var:
            context.variables[save_to_var] = {
                'windows': results,
                'total': total_agg,
            }

        return ActionResult(
            success=True,
            data={'window_count': len(results), 'results': results, 'total': total_agg},
            message=f"Aggregated {len(results)} windows"
        )

    def _compute_agg(self, values: List, func: str) -> Any:
        """Compute aggregation."""
        if not values:
            return None
        try:
            nums = [float(v) for v in values]
        except:
            nums = values

        if func == 'sum':
            return sum(nums) if nums else None
        elif func == 'avg' or func == 'mean':
            return sum(nums) / len(nums) if nums else None
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(nums) if nums else None
        elif func == 'max':
            return max(nums) if nums else None
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        return values[0] if values else None


class TimeWindowAction(BaseAction):
    """Create time-based windows (tumbling time windows).
    
    Partition events by fixed time intervals.
    """
    action_type = "time_window"
    display_name = "时间窗口"
    description = "基于时间间隔创建窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create time-based windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts with timestamp field
                - time_field: str (timestamp field name)
                - window_size: int (window duration)
                - window_unit: str (ms/s/min/h)
                - start_time: str (ISO timestamp, optional)
                - save_to_var: str
        
        Returns:
            ActionResult with time windows.
        """
        data = params.get('data', [])
        time_field = params.get('time_field', 'timestamp')
        window_size = params.get('window_size', 1)
        window_unit = params.get('window_unit', 'h')
        start_time = params.get('start_time', None)
        save_to_var = params.get('save_to_var', 'time_window_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Determine time range
        timestamps = []
        for record in data:
            ts = self._parse_timestamp(record.get(time_field))
            if ts is not None:
                timestamps.append((ts, record))

        if not timestamps:
            return ActionResult(success=False, message="No valid timestamps found")

        timestamps.sort(key=lambda x: x[0])
        
        # Determine window start
        if start_time:
            window_start = self._parse_timestamp(start_time)
        else:
            window_start = self._align_time(timestamps[0][0], window_size, window_unit)

        window_ms = self._to_ms(window_size, window_unit)
        windows: Dict[int, List] = {}

        for ts, record in timestamps:
            # Find which window this timestamp belongs to
            window_key = int((ts - window_start) / window_ms)
            if window_key < 0:
                window_key = 0
            if window_key not in windows:
                windows[window_key] = []
            windows[window_key].append(record)

        # Build result
        results = []
        for key in sorted(windows.keys()):
            ws = window_start + key * window_ms
            we = ws + window_ms
            results.append({
                'window_id': key,
                'window_start': datetime.fromtimestamp(ws / 1000).isoformat() if ws > 0 else None,
                'window_end': datetime.fromtimestamp(we / 1000).isoformat(),
                'count': len(windows[key]),
                'data': windows[key],
            })

        if context and save_to_var:
            context.variables[save_to_var] = results

        return ActionResult(
            success=True,
            data={'window_count': len(results), 'windows': results},
            message=f"Created {len(results)} time windows"
        )

    def _parse_timestamp(self, val: Any) -> Optional[float]:
        """Parse a timestamp value to ms."""
        if val is None:
            return None
        if isinstance(val, (int, float)):
            # Assume seconds if < 10^12, else ms
            return val * 1000 if val < 10**12 else val
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace('Z', '+00:00')).timestamp() * 1000
            except:
                return None
        return None

    def _align_time(self, ts: float, size: int, unit: str) -> float:
        """Align timestamp to window boundary."""
        window_ms = self._to_ms(size, unit)
        return int(ts / window_ms) * window_ms

    def _to_ms(self, value: float, unit: str) -> float:
        """Convert to milliseconds."""
        unit = unit.lower()
        if unit == 'ms':
            return value
        elif unit == 's':
            return value * 1000
        elif unit == 'min':
            return value * 60000
        elif unit == 'h':
            return value * 3600000
        return value
