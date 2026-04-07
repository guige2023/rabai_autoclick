"""Datetime5 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeNowAction: Get current datetime
- DatetimeTodayAction: Get today's date
- DatetimeUtcnowAction: Get current UTC datetime
- DatetimeFromtimestampAction: Create datetime from timestamp
- DatetimeStrftimeAction: Format datetime
"""

from datetime import datetime
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime5_now"
    display_name = "当前日期时间"
    description = "获取当前日期时间"

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
            ActionResult with current datetime.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'current_datetime')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            now = datetime.now()
            result = now.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前日期时间: {result}",
                data={
                    'datetime': result,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前日期时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'current_datetime'}


class DatetimeTodayAction(BaseAction):
    """Get today's date."""
    action_type = "datetime5_today"
    display_name = "今日日期"
    description = "获取今天的日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute today.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with today's date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'today_date')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'
            today = datetime.today()
            result = today.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"今日日期: {result}",
                data={
                    'date': result,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取今日日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'today_date'}


class DatetimeUtcnowAction(BaseAction):
    """Get current UTC datetime."""
    action_type = "datetime5_utcnow"
    display_name = "UTC当前时间"
    description = "获取当前UTC时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UTC now.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with UTC datetime.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'utc_datetime')

        try:
            from datetime import timezone
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            utc_now = datetime.now(timezone.utc)
            result = utc_now.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UTC当前时间: {result}",
                data={
                    'datetime': result,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取UTC当前时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'utc_datetime'}


class DatetimeFromtimestampAction(BaseAction):
    """Create datetime from timestamp."""
    action_type = "datetime5_fromtimestamp"
    display_name = "时间戳转日期时间"
    description = "从时间戳创建日期时间"

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
            result = dt.isoformat()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间戳转日期时间: {result}",
                data={
                    'timestamp': resolved_ts,
                    'datetime': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳转日期时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_from_ts'}


class DatetimeStrftimeAction(BaseAction):
    """Format datetime."""
    action_type = "datetime5_strftime"
    display_name = "格式化日期时间"
    description = "格式化日期时间为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strftime.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format_str, output_var.

        Returns:
            ActionResult with formatted string.
        """
        datetime_str = params.get('datetime_str', '')
        format_str = params.get('format_str', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.fromisoformat(resolved_dt)
            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化日期时间: {result}",
                data={
                    'datetime': resolved_dt,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日期时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format_str': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_datetime'}
