"""Time action module for RabAI AutoClick.

Provides time operations:
- TimeSleepAction: Sleep for duration
- TimeNowAction: Get current time
- TimeTimestampAction: Get current timestamp
- TimeCountdownAction: Countdown timer
- TimeMeasureAction: Measure elapsed time
"""

import time
import datetime
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeSleepAction(BaseAction):
    """Sleep for duration."""
    action_type = "time_sleep"
    display_name = "延时等待"
    description = "等待指定时间"

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
            ActionResult indicating completion.
        """
        seconds = params.get('seconds', 1)

        valid, msg = self.validate_type(seconds, (int, float), 'seconds')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(seconds)
            time.sleep(float(resolved))

            return ActionResult(
                success=True,
                message=f"延时完成: {resolved} 秒",
                data={'seconds': resolved}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"延时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class TimeNowAction(BaseAction):
    """Get current time."""
    action_type = "time_now"
    display_name = "获取当前时间"
    description = "获取当前时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting current time.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with current time.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'current_time')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'
            result = datetime.datetime.now().strftime(resolved_format)
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
                message=f"获取时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'current_time'}


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
        """Execute getting timestamp.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with timestamp.
        """
        output_var = params.get('output_var', 'timestamp')

        try:
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


class TimeCountdownAction(BaseAction):
    """Countdown timer."""
    action_type = "time_countdown"
    display_name = "倒计时"
    description = "倒计时器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute countdown.

        Args:
            context: Execution context.
            params: Dict with seconds, output_var.

        Returns:
            ActionResult with remaining seconds.
        """
        seconds = params.get('seconds', 10)
        output_var = params.get('output_var', 'countdown')

        valid, msg = self.validate_type(seconds, (int, float), 'seconds')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(seconds)
            start = time.time()
            end = start + float(resolved)

            remaining = resolved
            while time.time() < end and remaining > 0:
                remaining = end - time.time()
                time.sleep(min(0.1, remaining))

            context.set(output_var, max(0, remaining))

            return ActionResult(
                success=True,
                message=f"倒计时完成",
                data={
                    'remaining': max(0, remaining),
                    'original': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"倒计时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'countdown'}


class TimeMeasureAction(BaseAction):
    """Measure elapsed time."""
    action_type = "time_measure"
    display_name = "计时器"
    description = "测量经过的时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute time measurement.

        Args:
            context: Execution context.
            params: Dict with start_time, output_var.

        Returns:
            ActionResult with elapsed time.
        """
        start_time = params.get('start_time', None)
        output_var = params.get('output_var', 'elapsed')

        if start_time is None:
            # Start new measurement
            result = time.time()
            context.set(output_var, result)
            return ActionResult(
                success=True,
                message=f"计时开始: {result}",
                data={
                    'start_time': result,
                    'output_var': output_var
                }
            )
        else:
            # Calculate elapsed
            try:
                resolved_start = context.resolve_value(start_time)
                elapsed = time.time() - float(resolved_start)
                context.set(output_var, elapsed)

                return ActionResult(
                    success=True,
                    message=f"经过时间: {elapsed:.2f} 秒",
                    data={
                        'elapsed': elapsed,
                        'start_time': resolved_start,
                        'output_var': output_var
                    }
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"计时测量失败: {str(e)}"
                )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start_time': None, 'output_var': 'elapsed'}


class TimeFormatAction(BaseAction):
    """Format timestamp."""
    action_type = "time_format"
    display_name = "格式化时间"
    description = "格式化时间戳"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute time formatting.

        Args:
            context: Execution context.
            params: Dict with timestamp, format, output_var.

        Returns:
            ActionResult with formatted time.
        """
        timestamp = params.get('timestamp', None)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_time')

        try:
            resolved_format = context.resolve_value(format_str)

            if timestamp is None:
                dt = datetime.datetime.now()
            else:
                resolved_ts = context.resolve_value(timestamp)
                dt = datetime.datetime.fromtimestamp(float(resolved_ts))

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化时间: {result}",
                data={
                    'time': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timestamp': None, 'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_time'}


class TimeParseAction(BaseAction):
    """Parse time string."""
    action_type = "time_parse"
    display_name = "解析时间"
    description = "解析时间字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute time parsing.

        Args:
            context: Execution context.
            params: Dict with time_str, format, output_var.

        Returns:
            ActionResult with parsed timestamp.
        """
        time_str = params.get('time_str', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_timestamp')

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_time = context.resolve_value(time_str)
            resolved_format = context.resolve_value(format_str)

            dt = datetime.datetime.strptime(resolved_time, resolved_format)
            timestamp = dt.timestamp()

            context.set(output_var, timestamp)

            return ActionResult(
                success=True,
                message=f"时间解析完成: {timestamp}",
                data={
                    'timestamp': timestamp,
                    'datetime': dt.isoformat(),
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"时间解析失败: 无效的格式或时间 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['time_str', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_timestamp'}