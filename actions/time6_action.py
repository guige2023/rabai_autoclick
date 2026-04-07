"""Time6 action module for RabAI AutoClick.

Provides additional time operations:
- TimeSleepAction: Sleep for duration
- TimeTimestampAction: Get current timestamp
- TimeFromTimestampAction: Convert timestamp to datetime
- TimeParseFormatAction: Parse time with format
- TimeIsTodayAction: Check if date is today
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeSleepAction(BaseAction):
    """Sleep for duration."""
    action_type = "time6_sleep"
    display_name = "等待"
    description = "等待指定时间"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sleep.

        Args:
            context: Execution context.
            params: Dict with seconds, output_var.

        Returns:
            ActionResult with sleep status.
        """
        seconds = params.get('seconds', 1)
        output_var = params.get('output_var', 'sleep_status')

        try:
            import time

            resolved_seconds = float(context.resolve_value(seconds)) if seconds else 1

            time.sleep(resolved_seconds)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"等待完成: {resolved_seconds}秒",
                data={
                    'seconds': resolved_seconds,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sleep_status'}


class TimeTimestampAction(BaseAction):
    """Get current timestamp."""
    action_type = "time6_timestamp"
    display_name = "获取时间戳"
    description = "获取当前Unix时间戳"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timestamp.

        Args:
            context: Execution context.
            params: Dict with milliseconds, output_var.

        Returns:
            ActionResult with timestamp.
        """
        milliseconds = params.get('milliseconds', False)
        output_var = params.get('output_var', 'timestamp')

        try:
            import time

            resolved_ms = bool(context.resolve_value(milliseconds)) if milliseconds else False

            if resolved_ms:
                result = int(time.time() * 1000)
            else:
                result = int(time.time())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间戳: {result}",
                data={
                    'timestamp': result,
                    'milliseconds': resolved_ms,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时间戳失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'milliseconds': False, 'output_var': 'timestamp'}


class TimeFromTimestampAction(BaseAction):
    """Convert timestamp to datetime."""
    action_type = "time6_from_timestamp"
    display_name = "时间戳转日期"
    description = "将Unix时间戳转换为日期"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute from timestamp.

        Args:
            context: Execution context.
            params: Dict with timestamp, milliseconds, output_var.

        Returns:
            ActionResult with datetime.
        """
        timestamp = params.get('timestamp', 'now')
        milliseconds = params.get('milliseconds', False)
        output_var = params.get('output_var', 'datetime')

        try:
            from datetime import datetime

            resolved_ts = context.resolve_value(timestamp)
            resolved_ms = bool(context.resolve_value(milliseconds)) if milliseconds else False

            if resolved_ts == 'now' or resolved_ts is None:
                dt = datetime.now()
            else:
                ts = float(resolved_ts)
                if resolved_ms:
                    ts = ts / 1000
                dt = datetime.fromtimestamp(ts)

            result = dt.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期时间: {result}",
                data={
                    'timestamp': resolved_ts,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'milliseconds': False, 'output_var': 'datetime'}


class TimeParseFormatAction(BaseAction):
    """Parse time with format."""
    action_type = "time6_parse"
    display_name = "解析时间格式"
    description = "按指定格式解析时间字符串"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse format.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_datetime')

        try:
            from datetime import datetime

            resolved_str = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_str, resolved_format)
            result = dt.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间解析: {result}",
                data={
                    'original': resolved_str,
                    'format': resolved_format,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_datetime'}


class TimeIsTodayAction(BaseAction):
    """Check if date is today."""
    action_type = "time6_is_today"
    display_name = "判断是否今天"
    description = "判断日期是否是今天"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is today.

        Args:
            context: Execution context.
            params: Dict with date, output_var.

        Returns:
            ActionResult with is today result.
        """
        date = params.get('date', 'now')
        output_var = params.get('output_var', 'is_today')

        try:
            from datetime import datetime, date as date_type

            resolved = context.resolve_value(date)

            if resolved == 'now':
                input_date = date_type.today()
            else:
                if isinstance(resolved, str):
                    input_date = datetime.fromisoformat(resolved).date()
                else:
                    input_date = resolved

            today = date_type.today()
            result = input_date == today

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"是否今天: {'是' if result else '否'}",
                data={
                    'date': str(input_date),
                    'is_today': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断是否今天失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_today'}