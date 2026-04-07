"""Timezone conversion action module for RabAI AutoClick.

Provides timezone operations:
- TimezoneConvertAction: Convert time between timezones
- TimezoneNowAction: Get current time in timezone
- TimezoneListAction: List available timezones
- TimezoneOffsetAction: Get UTC offset
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimezoneConvertAction(BaseAction):
    """Convert time between timezones."""
    action_type = "timezone_convert"
    display_name = "时区转换"
    description = "时区时间转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timezone conversion."""
        time_str = params.get('time', '')
        from_tz = params.get('from_tz', 'UTC')
        to_tz = params.get('to_tz', 'Asia/Shanghai')
        time_format = params.get('time_format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'converted_time')

        if not time_str:
            return ActionResult(success=False, message="time is required")

        try:
            import pytz

            resolved_time = context.resolve_value(time_str) if context else time_str
            resolved_from = context.resolve_value(from_tz) if context else from_tz
            resolved_to = context.resolve_value(to_tz) if context else to_tz

            from_tz_obj = pytz.timezone(resolved_from)
            to_tz_obj = pytz.timezone(resolved_to)

            # Parse the time string
            try:
                dt = from_tz_obj.localize(datetime.fromisoformat(resolved_time))
            except (ValueError, pytz.exceptions.UnknownTimeZoneError):
                dt = datetime.fromisoformat(resolved_time).replace(tzinfo=from_tz_obj)

            converted = dt.astimezone(to_tz_obj)
            formatted = converted.strftime(time_format)

            result = {
                'original': resolved_time,
                'from_tz': resolved_from,
                'to_tz': resolved_to,
                'converted': formatted,
                'utc_offset': converted.strftime('%z'),
            }

            if context:
                context.set(output_var, formatted)
            return ActionResult(success=True, message=f"{resolved_time} ({resolved_from}) -> {formatted} ({resolved_to})", data=result)
        except ImportError:
            return ActionResult(success=False, message="pytz not installed. Run: pip install pytz")
        except Exception as e:
            return ActionResult(success=False, message=f"Timezone convert error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['time']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'from_tz': 'UTC', 'to_tz': 'Asia/Shanghai', 'time_format': '%Y-%m-%d %H:%M:%S', 'output_var': 'converted_time'}


class TimezoneNowAction(BaseAction):
    """Get current time in timezone."""
    action_type = "timezone_now"
    display_name = "时区当前时间"
    description = "获取时区当前时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timezone now."""
        timezone_name = params.get('timezone', 'Asia/Shanghai')
        time_format = params.get('time_format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'current_time')

        try:
            import pytz

            resolved_tz = context.resolve_value(timezone_name) if context else timezone_name
            tz = pytz.timezone(resolved_tz)
            now = datetime.now(tz)
            formatted = now.strftime(time_format)

            result = {'timezone': resolved_tz, 'now': formatted, 'timestamp': now.timestamp()}
            if context:
                context.set(output_var, formatted)
            return ActionResult(success=True, message=f"Now in {resolved_tz}: {formatted}", data=result)
        except ImportError:
            return ActionResult(success=False, message="pytz not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Timezone now error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timezone': 'Asia/Shanghai', 'time_format': '%Y-%m-%d %H:%M:%S', 'output_var': 'current_time'}


class TimezoneListAction(BaseAction):
    """List available timezones."""
    action_type = "timezone_list"
    display_name = "时区列表"
    description = "列出可用时区"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timezone list."""
        region = params.get('region', None)  # e.g. 'Asia', 'America', None for all
        output_var = params.get('output_var', 'timezone_list')

        try:
            import pytz

            resolved_region = context.resolve_value(region) if context else region

            if resolved_region:
                tzs = pytz.timezone(resolved_region)
                zones = [resolved_region]
            else:
                zones = pytz.all_timezones

            result = {'timezones': zones, 'count': len(zones)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Found {len(zones)} timezones", data=result)
        except ImportError:
            return ActionResult(success=False, message="pytz not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Timezone list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'region': None, 'output_var': 'timezone_list'}


class TimezoneOffsetAction(BaseAction):
    """Get UTC offset for timezone."""
    action_type = "timezone_offset"
    display_name = "UTC偏移量"
    description = "获取时区UTC偏移"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timezone offset."""
        timezone_name = params.get('timezone', 'Asia/Shanghai')
        output_var = params.get('output_var', 'utc_offset')

        try:
            import pytz

            resolved_tz = context.resolve_value(timezone_name) if context else timezone_name

            tz = pytz.timezone(resolved_tz)
            now = datetime.now(tz)
            offset = now.strftime('%z')

            hours = int(offset[1:3])
            minutes = int(offset[3:5])
            total_minutes = hours * 60 + minutes
            if offset[0] == '-':
                total_minutes = -total_minutes

            result = {'timezone': resolved_tz, 'offset': offset, 'total_minutes': total_minutes}
            if context:
                context.set(output_var, offset)
            return ActionResult(success=True, message=f"{resolved_tz} UTC offset: {offset}", data=result)
        except ImportError:
            return ActionResult(success=False, message="pytz not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Timezone offset error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timezone': 'Asia/Shanghai', 'output_var': 'utc_offset'}
