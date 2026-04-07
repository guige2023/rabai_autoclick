"""Time2 action module for RabAI AutoClick.

Provides additional time operations:
- TimeSleepAction: Sleep for seconds
- TimeNowAction: Get current datetime
- TimeTimestampAction: Get current timestamp
- TimeFromTimestampAction: Create datetime from timestamp
- TimeFormatAction: Format datetime
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeSleepAction(BaseAction):
    """Sleep for seconds."""
    action_type = "time_sleep"
    display_name = "延时"
    description = "延时执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sleep.

        Args:
            context: Execution context.
            params: Dict with seconds.

        Returns:
            ActionResult indicating sleep.
        """
        seconds = params.get('seconds', 1)

        try:
            resolved_seconds = float(context.resolve_value(seconds))
            time.sleep(resolved_seconds)

            return ActionResult(
                success=True,
                message=f"延时: {resolved_seconds} 秒",
                data={'seconds': resolved_seconds}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"延时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'seconds': 1}


class TimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "time_now"
    display_name = "获取当前时间"
    description = "获取当前日期时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current datetime.
        """
        output_var = params.get('output_var', 'current_time')

        try:
            now = datetime.now()
            iso_str = now.isoformat()
            context.set(output_var, iso_str)

            return ActionResult(
                success=True,
                message=f"当前时间: {iso_str}",
                data={
                    'datetime': iso_str,
                    'timestamp': now.timestamp(),
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
        return {'output_var': 'current_time'}


class TimeTimestampAction(BaseAction):
    """Get current timestamp."""
    action_type = "time_timestamp"
    display_name = "获取时间戳"
    description = "获取当前Unix时间戳"

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
        output_var = params.get('output_var', 'current_timestamp')

        try:
            ts = time.time()
            context.set(output_var, ts)

            return ActionResult(
                success=True,
                message=f"当前时间戳: {ts}",
                data={
                    'timestamp': ts,
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
        return {'output_var': 'current_timestamp'}


class TimeFromTimestampAction(BaseAction):
    """Create datetime from timestamp."""
    action_type = "time_from_timestamp"
    display_name = "从时间戳创建"
    description = "从Unix时间戳创建日期时间"

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
        output_var = params.get('output_var', 'datetime_from_ts')

        try:
            resolved_ts = float(context.resolve_value(timestamp))

            dt = datetime.fromtimestamp(resolved_ts)
            iso_str = dt.isoformat()
            context.set(output_var, iso_str)

            return ActionResult(
                success=True,
                message=f"从时间戳创建: {iso_str}",
                data={
                    'timestamp': resolved_ts,
                    'datetime': iso_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从时间戳创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_from_ts'}


class TimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "time_format"
    display_name = "格式化时间"
    description = "格式化日期时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format_str, output_var.

        Returns:
            ActionResult with formatted string.
        """
        datetime_str = params.get('datetime_str', '')
        format_str = params.get('format_str', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_time')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str)

            dt = datetime.fromisoformat(resolved_dt)
            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化时间: {result}",
                data={
                    'datetime': resolved_dt,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'format_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_time'}
