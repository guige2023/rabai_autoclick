"""Time utilities action module for RabAI AutoClick.

Provides time and date manipulation actions including
timestamp conversion, formatting, parsing, and calculations.
"""

import sys
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TimestampNowAction(BaseAction):
    """Get current timestamp in various formats.
    
    Supports Unix timestamp, ISO format, custom format,
    and timezone-aware timestamps.
    """
    action_type = "timestamp_now"
    display_name = "当前时间戳"
    description = "获取当前时间戳，支持多种格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get current timestamp.
        
        Args:
            context: Execution context.
            params: Dict with keys: format, timezone, precision,
                   save_to_var.
        
        Returns:
            ActionResult with timestamp data.
        """
        format_type = params.get('format', 'unix')
        timezone_str = params.get('timezone', 'local')
        precision = params.get('precision', 'seconds')
        save_to_var = params.get('save_to_var', None)

        now = datetime.now()

        if timezone_str != 'local':
            try:
                tz = timezone(timedelta(hours=float(timezone_str)))
                now = datetime.now(tz)
            except ValueError:
                return ActionResult(
                    success=False,
                    message=f"Invalid timezone offset: {timezone_str}"
                )

        result_data = {
            'unix': int(now.timestamp()),
            'unix_ms': int(now.timestamp() * 1000),
            'iso': now.isoformat(),
            'datetime': now.strftime('%Y-%m-%d %H:%M:%S'),
            'date': now.strftime('%Y-%m-%d'),
            'time': now.strftime('%H:%M:%S')
        }

        # Apply precision
        if precision == 'seconds':
            result_data['timestamp'] = result_data['unix']
        elif precision == 'milliseconds':
            result_data['timestamp'] = result_data['unix_ms']
        elif precision == 'minutes':
            result_data['timestamp'] = result_data['unix'] // 60
        elif precision == 'hours':
            result_data['timestamp'] = result_data['unix'] // 3600
        else:
            result_data['timestamp'] = result_data['unix']

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"当前时间戳: {result_data['timestamp']}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format': 'unix',
            'timezone': 'local',
            'precision': 'seconds',
            'save_to_var': None
        }


class TimestampConvertAction(BaseAction):
    """Convert between timestamp formats.
    
    Supports Unix timestamp to datetime, ISO string parsing,
    and custom format parsing.
    """
    action_type = "timestamp_convert"
    display_name = "时间戳转换"
    description = "转换时间戳格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Convert timestamp.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, from_format,
                   to_format, timezone, save_to_var.
        
        Returns:
            ActionResult with converted timestamp.
        """
        value = params.get('value', None)
        from_format = params.get('from_format', 'unix')
        to_format = params.get('to_format', 'iso')
        timezone_str = params.get('timezone', 'local')
        save_to_var = params.get('save_to_var', None)

        if value is None:
            return ActionResult(success=False, message="Value cannot be None")

        # Parse input to datetime
        try:
            if from_format == 'unix':
                ts = float(value)
                if ts > 1e12:  # milliseconds
                    ts = ts / 1000
                dt = datetime.fromtimestamp(ts)
            elif from_format == 'unix_ms':
                dt = datetime.fromtimestamp(float(value) / 1000)
            elif from_format == 'iso':
                dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            elif from_format == 'str':
                fmt = params.get('parse_format', '%Y-%m-%d %H:%M:%S')
                dt = datetime.strptime(str(value), fmt)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown from_format: {from_format}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳解析失败: {str(e)}"
            )

        # Apply timezone
        if timezone_str != 'local':
            try:
                tz = timezone(timedelta(hours=float(timezone_str)))
                dt = dt.replace(tzinfo=tz)
            except ValueError:
                pass

        # Convert to output format
        result_data = {
            'unix': int(dt.timestamp()),
            'unix_ms': int(dt.timestamp() * 1000),
            'iso': dt.isoformat(),
            'datetime': dt.strftime('%Y-%m-%d %H:%M:%S'),
            'date': dt.strftime('%Y-%m-%d'),
            'time': dt.strftime('%H:%M:%S'),
            'weekday': dt.strftime('%A'),
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'minute': dt.minute,
            'second': dt.second
        }

        if to_format == 'unix':
            result_data['result'] = result_data['unix']
        elif to_format == 'iso':
            result_data['result'] = result_data['iso']
        elif to_format == 'str':
            fmt = params.get('output_format', '%Y-%m-%d %H:%M:%S')
            result_data['result'] = dt.strftime(fmt)
        else:
            result_data['result'] = result_data['unix']

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"时间戳转换成功: {result_data['result']}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_format', 'to_format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'timezone': 'local',
            'parse_format': '%Y-%m-%d %H:%M:%S',
            'output_format': '%Y-%m-%d %H:%M:%S',
            'save_to_var': None
        }


class TimeSleepAction(BaseAction):
    """Pause execution for specified duration.
    
    Supports seconds and milliseconds, with interrupt option.
    """
    action_type = "time_sleep"
    display_name = "延时等待"
    description = "暂停执行指定时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sleep for duration.
        
        Args:
            context: Execution context.
            params: Dict with keys: duration, unit, save_to_var.
        
        Returns:
            ActionResult after sleep completes.
        """
        duration = params.get('duration', 1)
        unit = params.get('unit', 'seconds')
        save_to_var = params.get('save_to_var', None)

        try:
            dur = float(duration)
            if unit == 'milliseconds':
                dur = dur / 1000
            elif unit == 'minutes':
                dur = dur * 60
            elif unit == 'hours':
                dur = dur * 3600

            if dur <= 0 or dur > 86400:  # Max 24 hours
                return ActionResult(
                    success=False,
                    message=f"Invalid duration: {duration}"
                )

            start = time.time()
            time.sleep(dur)
            elapsed = time.time() - start

            result_data = {
                'duration': dur,
                'elapsed': elapsed,
                'unit': unit
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"延时完成: {duration} {unit} (实际 {elapsed:.3f}s)",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"延时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['duration']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'unit': 'seconds',
            'save_to_var': None
        }


class TimeDiffAction(BaseAction):
    """Calculate time difference between two timestamps.
    
    Supports various output units and handles date boundaries.
    """
    action_type = "time_diff"
    display_name = "时间差计算"
    description = "计算两个时间戳之间的差值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate time difference.
        
        Args:
            context: Execution context.
            params: Dict with keys: start, end, start_format,
                   end_format, output_unit, save_to_var.
        
        Returns:
            ActionResult with difference in requested unit.
        """
        start = params.get('start', None)
        end = params.get('end', None)
        start_format = params.get('start_format', 'unix')
        end_format = params.get('end_format', 'unix')
        output_unit = params.get('output_unit', 'seconds')
        save_to_var = params.get('save_to_var', None)

        # Parse start time
        try:
            if start_format == 'unix':
                start_ts = float(start)
                if start_ts > 1e12:
                    start_ts = start_ts / 1000
                start_dt = datetime.fromtimestamp(start_ts)
            elif start_format == 'iso':
                start_dt = datetime.fromisoformat(str(start).replace('Z', '+00:00'))
            else:
                fmt = params.get('start_parse_fmt', '%Y-%m-%d %H:%M:%S')
                start_dt = datetime.strptime(str(start), fmt)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Start time parsing failed: {e}"
            )

        # Parse end time
        try:
            if end_format == 'unix':
                end_ts = float(end)
                if end_ts > 1e12:
                    end_ts = end_ts / 1000
                end_dt = datetime.fromtimestamp(end_ts)
            elif end_format == 'iso':
                end_dt = datetime.fromisoformat(str(end).replace('Z', '+00:00'))
            else:
                fmt = params.get('end_parse_fmt', '%Y-%m-%d %H:%M:%S')
                end_dt = datetime.strptime(str(end), fmt)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"End time parsing failed: {e}"
            )

        # Calculate difference
        delta = end_dt - start_dt
        diff_seconds = delta.total_seconds()

        # Convert to requested unit
        if output_unit == 'seconds':
            result = diff_seconds
        elif output_unit == 'minutes':
            result = diff_seconds / 60
        elif output_unit == 'hours':
            result = diff_seconds / 3600
        elif output_unit == 'days':
            result = diff_seconds / 86400
        else:
            result = diff_seconds

        result_data = {
            'difference': round(result, 3),
            'seconds': round(diff_seconds, 3),
            'minutes': round(diff_seconds / 60, 3),
            'hours': round(diff_seconds / 3600, 3),
            'days': round(diff_seconds / 86400, 3),
            'output_unit': output_unit,
            'start': str(start_dt),
            'end': str(end_dt)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"时间差: {result} {output_unit}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['start', 'end', 'output_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'start_format': 'unix',
            'end_format': 'unix',
            'start_parse_fmt': '%Y-%m-%d %H:%M:%S',
            'end_parse_fmt': '%Y-%m-%d %H:%M:%S',
            'save_to_var': None
        }
