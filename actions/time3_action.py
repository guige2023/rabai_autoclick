"""Time3 action module for RabAI AutoClick.

Provides additional time operations:
- TimeDiffSecondsAction: Calculate difference in seconds
- TimeParseAction: Parse time string
- TimeIsAfterAction: Check if time is after another
- TimeIsBeforeAction: Check if time is before another
- TimeComponentsAction: Extract hour, minute, second components
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeDiffSecondsAction(BaseAction):
    """Calculate difference in seconds."""
    action_type = "time3_diff_seconds"
    display_name = "时间差秒数"
    description = "计算两个时间之间的秒数差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diff seconds.

        Args:
            context: Execution context.
            params: Dict with time1, time2, output_var.

        Returns:
            ActionResult with difference in seconds.
        """
        time1 = params.get('time1', '')
        time2 = params.get('time2', '')
        output_var = params.get('output_var', 'diff_seconds')

        valid, msg = self.validate_type(time1, str, 'time1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(time2, str, 'time2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_time1 = context.resolve_value(time1)
            resolved_time2 = context.resolve_value(time2)

            t1 = datetime.strptime(resolved_time1, '%H:%M:%S')
            t2 = datetime.strptime(resolved_time2, '%H:%M:%S')

            diff = (t1 - t2).total_seconds()
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"时间差: {diff} 秒",
                data={
                    'time1': resolved_time1,
                    'time2': resolved_time2,
                    'diff_seconds': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算时间差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time1', 'time2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'diff_seconds'}


class TimeParseAction(BaseAction):
    """Parse time string."""
    action_type = "time3_parse"
    display_name = "解析时间"
    description = "按指定格式解析时间字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse time.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with parsed time components.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'parsed_time')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            components = {
                'hour': t.hour,
                'minute': t.minute,
                'second': t.second
            }
            context.set(output_var, components)

            return ActionResult(
                success=True,
                message=f"解析时间: {t.hour}:{t.minute}:{t.second}",
                data={
                    'original': resolved_time,
                    'format': resolved_format,
                    'components': components,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'parsed_time'}


class TimeIsAfterAction(BaseAction):
    """Check if time is after another."""
    action_type = "time3_is_after"
    display_name = "时间先后判断"
    description = "判断一个时间是否在另一个时间之后"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is after.

        Args:
            context: Execution context.
            params: Dict with time1, time2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        time1 = params.get('time1', '')
        time2 = params.get('time2', '')
        output_var = params.get('output_var', 'is_after')

        valid, msg = self.validate_type(time1, str, 'time1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(time2, str, 'time2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_time1 = context.resolve_value(time1)
            resolved_time2 = context.resolve_value(time2)

            t1 = datetime.strptime(resolved_time1, '%H:%M:%S')
            t2 = datetime.strptime(resolved_time2, '%H:%M:%S')

            is_after = t1 > t2
            context.set(output_var, is_after)

            return ActionResult(
                success=True,
                message=f"{resolved_time1} 在 {resolved_time2} 之后: {'是' if is_after else '否'}",
                data={
                    'time1': resolved_time1,
                    'time2': resolved_time2,
                    'is_after': is_after,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断时间先后失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time1', 'time2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_after'}


class TimeIsBeforeAction(BaseAction):
    """Check if time is before another."""
    action_type = "time3_is_before"
    display_name = "时间先后判断2"
    description = "判断一个时间是否在另一个时间之前"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is before.

        Args:
            context: Execution context.
            params: Dict with time1, time2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        time1 = params.get('time1', '')
        time2 = params.get('time2', '')
        output_var = params.get('output_var', 'is_before')

        valid, msg = self.validate_type(time1, str, 'time1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(time2, str, 'time2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_time1 = context.resolve_value(time1)
            resolved_time2 = context.resolve_value(time2)

            t1 = datetime.strptime(resolved_time1, '%H:%M:%S')
            t2 = datetime.strptime(resolved_time2, '%H:%M:%S')

            is_before = t1 < t2
            context.set(output_var, is_before)

            return ActionResult(
                success=True,
                message=f"{resolved_time1} 在 {resolved_time2} 之前: {'是' if is_before else '否'}",
                data={
                    'time1': resolved_time1,
                    'time2': resolved_time2,
                    'is_before': is_before,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断时间先后失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time1', 'time2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_before'}


class TimeComponentsAction(BaseAction):
    """Extract hour, minute, second components."""
    action_type = "time3_components"
    display_name = "提取时间组件"
    description = "提取时间的时、分、秒组件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute time components.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with components.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%H:%M:%S')
        output_var = params.get('output_var', 'time_components')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%H:%M:%S'

            t = datetime.strptime(resolved_time, resolved_format)
            components = {
                'hour': t.hour,
                'minute': t.minute,
                'second': t.second
            }
            context.set(output_var, components)

            return ActionResult(
                success=True,
                message=f"时间组件: {t.hour}:{t.minute}:{t.second}",
                data={
                    'original': resolved_time,
                    'components': components,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取时间组件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%H:%M:%S', 'output_var': 'time_components'}
