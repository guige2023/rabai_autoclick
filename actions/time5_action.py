"""Time5 action module for RabAI AutoClick.

Provides additional time operations:
- TimeNowAction: Get current time
- TimeTimestampAction: Get current timestamp
- TimeFromTimestampAction: Convert timestamp to datetime
- TimeParseAction: Parse time string
- TimeNowUtcAction: Get current UTC time
"""

from datetime import datetime
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeNowAction(BaseAction):
    """Get current time."""
    action_type = "time5_now"
    display_name = "当前时间"
    description = "获取当前时间"
    version = "5.0"

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
        output_var = params.get('output_var', 'now_result')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            result = datetime.now().strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前时间: {result}",
                data={
                    'time': result,
                    'format': resolved_format,
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
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'now_result'}


class TimeTimestampAction(BaseAction):
    """Get current timestamp."""
    action_type = "time5_timestamp"
    display_name = "当前时间戳"
    description = "获取当前Unix时间戳"
    version = "5.0"

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
        output_var = params.get('output_var', 'timestamp_result')

        try:
            import time
            result = int(time.time())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前时间戳: {result}",
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
        return {'output_var': 'timestamp_result'}


class TimeFromTimestampAction(BaseAction):
    """Convert timestamp to datetime."""
    action_type = "time5_from_timestamp"
    display_name = "时间戳转时间"
    description = "将Unix时间戳转换为时间"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute from timestamp.

        Args:
            context: Execution context.
            params: Dict with timestamp, format, output_var.

        Returns:
            ActionResult with datetime.
        """
        timestamp = params.get('timestamp', 0)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'datetime_result')

        try:
            resolved_ts = int(context.resolve_value(timestamp))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.fromtimestamp(resolved_ts)
            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间戳转时间: {result}",
                data={
                    'timestamp': resolved_ts,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳转时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'datetime_result'}


class TimeParseAction(BaseAction):
    """Parse time string."""
    action_type = "time5_parse"
    display_name = "时间解析"
    description = "解析时间字符串"
    version = "5.0"

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
            ActionResult with parsed time.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_time')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_time, resolved_format)
            context.set(output_var, resolved_time)

            return ActionResult(
                success=True,
                message=f"时间解析成功",
                data={
                    'time_str': resolved_time,
                    'format': resolved_format,
                    'datetime': dt.isoformat(),
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
        return {'output_var': 'parsed_time'}


class TimeNowUtcAction(BaseAction):
    """Get current UTC time."""
    action_type = "time5_now_utc"
    display_name = "当前UTC时间"
    description = "获取当前UTC时间"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now UTC.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with current UTC time.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'utc_result')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            result = datetime.utcnow().strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前UTC时间: {result}",
                data={
                    'time': result,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取UTC时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'utc_result'}