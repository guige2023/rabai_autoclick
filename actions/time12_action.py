"""Time12 action module for RabAI AutoClick.

Provides additional time operations:
- TimeNowAction: Get current time
- TimeSleepAction: Sleep for seconds
- TimeTimestampAction: Get current timestamp
- TimeFromTimestampAction: Convert timestamp to datetime
- TimeFormatAction: Format time
- TimeParseAction: Parse time string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeNowAction(BaseAction):
    """Get current time."""
    action_type = "time12_now"
    display_name = "当前时间"
    description = "获取当前时间"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with current time.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'current_time')

        try:
            from datetime import datetime

            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            now = datetime.now()
            result = now.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前时间: {result}",
                data={
                    'datetime': now.isoformat(),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'current_time'}


class TimeSleepAction(BaseAction):
    """Sleep for seconds."""
    action_type = "time12_sleep"
    display_name = "休眠"
    description = "休眠指定秒数"
    version = "12.0"

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
                message=f"休眠完成: {resolved_seconds}秒",
                data={
                    'seconds': resolved_seconds,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"休眠失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sleep_status'}


class TimeTimestampAction(BaseAction):
    """Get current timestamp."""
    action_type = "time12_timestamp"
    display_name = "获取时间戳"
    description = "获取当前时间戳"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timestamp.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with timestamp.
        """
        output_var = params.get('output_var', 'timestamp')

        try:
            import time

            result = time.time()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间戳: {result}",
                data={
                    'timestamp': result,
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
        return {'output_var': 'timestamp'}


class TimeFromTimestampAction(BaseAction):
    """Convert timestamp to datetime."""
    action_type = "time12_from_timestamp"
    display_name = "从时间戳转换"
    description = "从时间戳转换为日期时间"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute from timestamp.

        Args:
            context: Execution context.
            params: Dict with timestamp, output_var.

        Returns:
            ActionResult with datetime.
        """
        timestamp = params.get('timestamp', 0)
        output_var = params.get('output_var', 'datetime')

        try:
            from datetime import datetime

            resolved_timestamp = float(context.resolve_value(timestamp)) if timestamp else time.time()

            dt = datetime.fromtimestamp(resolved_timestamp)
            result = dt.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期时间: {result}",
                data={
                    'timestamp': resolved_timestamp,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从时间戳转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime'}


class TimeFormatAction(BaseAction):
    """Format time."""
    action_type = "time12_format"
    display_name: "格式化时间"
    description: "格式化时间"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format.

        Args:
            context: Execution context.
            params: Dict with datetime, format, output_var.

        Returns:
            ActionResult with formatted time.
        """
        datetime_str = params.get('datetime', 'now')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_time')

        try:
            from datetime import datetime

            resolved_datetime = context.resolve_value(datetime_str) if datetime_str else 'now'
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            if resolved_datetime == 'now':
                dt = datetime.now()
            elif isinstance(resolved_datetime, str):
                dt = datetime.fromisoformat(resolved_datetime)
            else:
                dt = resolved_datetime

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化时间: {result}",
                data={
                    'datetime': dt.isoformat(),
                    'format': resolved_format,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_time'}


class TimeParseAction(BaseAction):
    """Parse time string."""
    action_type = "time12_parse"
    display_name = "解析时间"
    description = "解析时间字符串"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_time')

        try:
            from datetime import datetime

            resolved_str = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_str, resolved_format)
            result = dt.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析时间: {result}",
                data={
                    'time_str': resolved_str,
                    'format': resolved_format,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"时间格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_time'}