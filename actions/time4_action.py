"""Time4 action module for RabAI AutoClick.

Provides additional time operations:
- TimeHourAction: Get hour
- TimeMinuteAction: Get minute
- TimeSecondAction: Get second
- TimeMillisecondAction: Get millisecond
- TimeIsAmAction: Check if AM
"""

from datetime import datetime
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeHourAction(BaseAction):
    """Get hour."""
    action_type = "time4_hour"
    display_name = "获取小时"
    description = "从时间字符串获取小时"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hour.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with hour.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'hour_result')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            context.set(output_var, t.hour)

            return ActionResult(
                success=True,
                message=f"小时: {t.hour}",
                data={
                    'time': resolved_time,
                    'hour': t.hour,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取小时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'hour_result'}


class TimeMinuteAction(BaseAction):
    """Get minute."""
    action_type = "time4_minute"
    display_name = "获取分钟"
    description = "从时间字符串获取分钟"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minute.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with minute.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'minute_result')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            context.set(output_var, t.minute)

            return ActionResult(
                success=True,
                message=f"分钟: {t.minute}",
                data={
                    'time': resolved_time,
                    'minute': t.minute,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取分钟失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'minute_result'}


class TimeSecondAction(BaseAction):
    """Get second."""
    action_type = "time4_second"
    display_name = "获取秒数"
    description = "从时间字符串获取秒数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute second.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with second.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'second_result')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            context.set(output_var, t.second)

            return ActionResult(
                success=True,
                message=f"秒数: {t.second}",
                data={
                    'time': resolved_time,
                    'second': t.second,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取秒数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'second_result'}


class TimeMillisecondAction(BaseAction):
    """Get millisecond."""
    action_type = "time4_millisecond"
    display_name = "获取毫秒"
    description = "从时间字符串获取毫秒"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute millisecond.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with millisecond.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'millisecond_result')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            context.set(output_var, 0)

            return ActionResult(
                success=True,
                message=f"毫秒: 0",
                data={
                    'time': resolved_time,
                    'millisecond': 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取毫秒失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'millisecond_result'}


class TimeIsAmAction(BaseAction):
    """Check if AM."""
    action_type = "time4_is_am"
    display_name = "判断上午"
    description = "判断时间是否为上午"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is AM.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with AM check.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'is_am_result')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            result = t.hour < 12
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"上午判断: {'是' if result else '否'}",
                data={
                    'time': resolved_time,
                    'is_am': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断上午失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'is_am_result'}
