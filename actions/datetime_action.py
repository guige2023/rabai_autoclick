"""Date and time action module for RabAI AutoClick.

Provides date/time operations:
- DateTimeNowAction: Get current datetime
- DateTimeTodayAction: Get today's date
- DateTimeParseAction: Parse datetime string
- DateTimeFormatAction: Format datetime
- DateTimeAddAction: Add time to datetime
- DateTimeDiffAction: Calculate time difference
- DateTimeConvertAction: Convert timezone
- DateTimeTimestampAction: Get/parse timestamp
- DateTimeRangeAction: Generate date range
- DateTimeWeekdayAction: Get weekday
- DateTimeAgeAction: Calculate age
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    from datetime import datetime, timedelta, date, timezone
    import time
    DT_AVAILABLE = True
except ImportError:
    DT_AVAILABLE = False


class DateTimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime_now"
    display_name = "获取当前时间"
    description = "获取当前日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get current datetime.

        Args:
            context: Execution context.
            params: Dict with format, timezone, output_var.

        Returns:
            ActionResult with current datetime.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        fmt = params.get('format', '%Y-%m-%d %H:%M:%S')
        timezone_str = params.get('timezone', 'local')
        output_var = params.get('output_var', 'now_result')

        try:
            if timezone_str == 'utc':
                now = datetime.now(timezone.utc)
            elif timezone_str == 'local':
                now = datetime.now()
            else:
                now = datetime.now()

            formatted = now.strftime(fmt)

            context.set(output_var, {
                'datetime': now.isoformat(),
                'formatted': formatted,
                'timestamp': now.timestamp()
            })

            return ActionResult(
                success=True,
                message=f"当前时间: {formatted}",
                data={'datetime': now.isoformat(), 'formatted': formatted, 'timestamp': now.timestamp()}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前时间失败: {str(e)}"
            )


class DateTimeTodayAction(BaseAction):
    """Get today's date."""
    action_type = "datetime_today"
    display_name = "获取今日日期"
    description = "获取今天的日期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get today's date.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with today's date.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        fmt = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'today_result')

        try:
            today = date.today()

            formatted = today.strftime(fmt)

            context.set(output_var, {
                'date': today.isoformat(),
                'formatted': formatted,
                'year': today.year,
                'month': today.month,
                'day': today.day
            })

            return ActionResult(
                success=True,
                message=f"今日日期: {formatted}",
                data={'date': today.isoformat(), 'formatted': formatted, 'year': today.year, 'month': today.month, 'day': today.day}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取今日日期失败: {str(e)}"
            )


class DateTimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime_parse"
    display_name = "解析时间字符串"
    description = "解析日期时间字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime parsing.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime_str = params.get('datetime_str', '')
        fmt = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_result')

        if not datetime_str:
            return ActionResult(success=False, message="日期时间字符串不能为空")

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            parsed = datetime.strptime(datetime_str, fmt)

            context.set(output_var, {
                'datetime': parsed.isoformat(),
                'formatted': parsed.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': parsed.timestamp(),
                'year': parsed.year,
                'month': parsed.month,
                'day': parsed.day,
                'hour': parsed.hour,
                'minute': parsed.minute,
                'second': parsed.second
            })

            return ActionResult(
                success=True,
                message=f"解析成功: {parsed.isoformat()}",
                data={'datetime': parsed.isoformat(), 'timestamp': parsed.timestamp()}
            )

        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期时间格式不匹配: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析日期时间失败: {str(e)}"
            )


class DateTimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "datetime_format"
    display_name = "格式化时间"
    description = "格式化日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime formatting.

        Args:
            context: Execution context.
            params: Dict with datetime_input, format, output_var.

        Returns:
            ActionResult with formatted datetime.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime_input = params.get('datetime_input', None)
        fmt = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'format_result')

        if datetime_input is None:
            return ActionResult(success=False, message="日期时间输入不能为空")

        try:
            if isinstance(datetime_input, str):
                dt = datetime.fromisoformat(datetime_input)
            elif isinstance(datetime_input, (int, float)):
                dt = datetime.fromtimestamp(datetime_input)
            elif isinstance(datetime_input, datetime):
                dt = datetime_input
            else:
                return ActionResult(success=False, message=f"不支持的时间输入类型: {type(datetime_input)}")

            formatted = dt.strftime(fmt)

            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"格式化成功: {formatted}",
                data={'formatted': formatted}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日期时间失败: {str(e)}"
            )


class DateTimeAddAction(BaseAction):
    """Add time to datetime."""
    action_type = "datetime_add"
    display_name = "时间加减"
    description = "在日期时间上增加或减少时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime addition.

        Args:
            context: Execution context.
            params: Dict with datetime_input, days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with result datetime.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime_input = params.get('datetime_input', None)
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'add_result')

        if datetime_input is None:
            return ActionResult(success=False, message="日期时间输入不能为空")

        try:
            if isinstance(datetime_input, str):
                try:
                    dt = datetime.fromisoformat(datetime_input)
                except ValueError:
                    dt = datetime.strptime(datetime_input, '%Y-%m-%d %H:%M:%S')
            elif isinstance(datetime_input, (int, float)):
                dt = datetime.fromtimestamp(datetime_input)
            elif isinstance(datetime_input, datetime):
                dt = datetime_input
            else:
                return ActionResult(success=False, message=f"不支持的时间输入类型: {type(datetime_input)}")

            delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
            result = dt + delta

            context.set(output_var, {
                'datetime': result.isoformat(),
                'formatted': result.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': result.timestamp()
            })

            return ActionResult(
                success=True,
                message=f"时间计算成功: {result.strftime('%Y-%m-%d %H:%M:%S')}",
                data={'datetime': result.isoformat(), 'timestamp': result.timestamp()}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间加减失败: {str(e)}"
            )


class DateTimeDiffAction(BaseAction):
    """Calculate time difference."""
    action_type = "datetime_diff"
    display_name = "时间差计算"
    description = "计算两个日期时间的差值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime difference.

        Args:
            context: Execution context.
            params: Dict with datetime1, datetime2, unit, output_var.

        Returns:
            ActionResult with time difference.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime1 = params.get('datetime1', None)
        datetime2 = params.get('datetime2', None)
        unit = params.get('unit', 'seconds')
        output_var = params.get('output_var', 'diff_result')

        if datetime1 is None or datetime2 is None:
            return ActionResult(success=False, message="两个日期时间都不能为空")

        try:
            def parse_dt(dt_input):
                if isinstance(dt_input, str):
                    try:
                        return datetime.fromisoformat(dt_input)
                    except ValueError:
                        return datetime.strptime(dt_input, '%Y-%m-%d %H:%M:%S')
                elif isinstance(dt_input, (int, float)):
                    return datetime.fromtimestamp(dt_input)
                elif isinstance(dt_input, datetime):
                    return dt_input
                else:
                    raise ValueError(f"不支持的时间输入类型: {type(dt_input)}")

            dt1 = parse_dt(datetime1)
            dt2 = parse_dt(datetime2)

            diff = abs(dt1 - dt2)
            diff_seconds = diff.total_seconds()

            if unit == 'seconds':
                result_value = diff_seconds
            elif unit == 'minutes':
                result_value = diff_seconds / 60
            elif unit == 'hours':
                result_value = diff_seconds / 3600
            elif unit == 'days':
                result_value = diff.days
            else:
                result_value = diff_seconds

            context.set(output_var, result_value)

            return ActionResult(
                success=True,
                message=f"时间差计算成功: {result_value} {unit}",
                data={
                    'difference': result_value,
                    'unit': unit,
                    'seconds': diff_seconds,
                    'days': diff.days
                }
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间差计算失败: {str(e)}"
            )


class DateTimeTimestampAction(BaseAction):
    """Get/parse timestamp."""
    action_type = "datetime_timestamp"
    display_name = "时间戳操作"
    description = "获取或解析时间戳"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timestamp operation.

        Args:
            context: Execution context.
            params: Dict with datetime_input, to_timestamp, output_var.

        Returns:
            ActionResult with timestamp.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime_input = params.get('datetime_input', None)
        to_timestamp = params.get('to_timestamp', True)
        output_var = params.get('output_var', 'timestamp_result')

        try:
            if to_timestamp:
                if datetime_input is None:
                    ts = time.time()
                else:
                    if isinstance(datetime_input, str):
                        try:
                            dt = datetime.fromisoformat(datetime_input)
                        except ValueError:
                            dt = datetime.strptime(datetime_input, '%Y-%m-%d %H:%M:%S')
                    elif isinstance(datetime_input, (int, float)):
                        context.set(output_var, datetime_input)
                        return ActionResult(
                            success=True,
                            message=f"时间戳: {datetime_input}",
                            data={'timestamp': datetime_input}
                        )
                    else:
                        dt = datetime_input

                    ts = dt.timestamp()

                context.set(output_var, ts)

                return ActionResult(
                    success=True,
                    message=f"时间戳: {ts}",
                    data={'timestamp': ts}
                )

            else:
                if datetime_input is None:
                    return ActionResult(success=False, message="时间戳输入不能为空")

                ts = float(datetime_input)
                dt = datetime.fromtimestamp(ts)

                context.set(output_var, {
                    'datetime': dt.isoformat(),
                    'formatted': dt.strftime('%Y-%m-%d %H:%M:%S')
                })

                return ActionResult(
                    success=True,
                    message=f"转换结果: {dt.strftime('%Y-%m-%d %H:%M:%S')}",
                    data={'datetime': dt.isoformat(), 'formatted': dt.strftime('%Y-%m-%d %H:%M:%S')}
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳操作失败: {str(e)}"
            )


class DateTimeRangeAction(BaseAction):
    """Generate date range."""
    action_type = "datetime_range"
    display_name = "生成日期范围"
    description = "生成日期范围列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute date range generation.

        Args:
            context: Execution context.
            params: Dict with start_date, end_date, format, step_days, output_var.

        Returns:
            ActionResult with date range.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        start_date = params.get('start_date', None)
        end_date = params.get('end_date', None)
        fmt = params.get('format', '%Y-%m-%d')
        step_days = params.get('step_days', 1)
        output_var = params.get('output_var', 'range_result')

        if start_date is None or end_date is None:
            return ActionResult(success=False, message="开始和结束日期都不能为空")

        try:
            def parse_date(d):
                if isinstance(d, str):
                    return datetime.strptime(d, '%Y-%m-%d').date()
                elif isinstance(d, date):
                    return d
                else:
                    return date.fromtimestamp(d) if isinstance(d, (int, float)) else None

            start = parse_date(start_date)
            end = parse_date(end_date)

            if start is None or end is None:
                return ActionResult(success=False, message="日期格式无效")

            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime(fmt))
                current += timedelta(days=step_days)

            context.set(output_var, dates)

            return ActionResult(
                success=True,
                message=f"生成日期范围: {len(dates)} 个日期",
                data={'dates': dates, 'count': len(dates)}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成日期范围失败: {str(e)}"
            )


class DateTimeWeekdayAction(BaseAction):
    """Get weekday."""
    action_type = "datetime_weekday"
    display_name = "获取星期几"
    description = "获取日期是星期几"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute weekday get.

        Args:
            context: Execution context.
            params: Dict with datetime_input, output_var.

        Returns:
            ActionResult with weekday info.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        datetime_input = params.get('datetime_input', None)
        output_var = params.get('output_var', 'weekday_result')

        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekdays_cn = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日']

        try:
            if datetime_input is None:
                dt = datetime.now()
            elif isinstance(datetime_input, str):
                try:
                    dt = datetime.fromisoformat(datetime_input)
                except ValueError:
                    dt = datetime.strptime(datetime_input, '%Y-%m-%d')
            elif isinstance(datetime_input, (int, float)):
                dt = datetime.fromtimestamp(datetime_input)
            else:
                dt = datetime_input

            weekday_idx = dt.weekday()

            context.set(output_var, {
                'weekday': weekdays[weekday_idx],
                'weekday_cn': weekdays_cn[weekday_idx],
                'weekday_index': weekday_idx,
                'is_weekend': weekday_idx >= 5
            })

            return ActionResult(
                success=True,
                message=f"今天是: {weekdays_cn[weekday_idx]} ({weekdays[weekday_idx]})",
                data={'weekday': weekdays[weekday_idx], 'weekday_cn': weekdays_cn[weekday_idx], 'weekday_index': weekday_idx}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取星期几失败: {str(e)}"
            )


class DateTimeAgeAction(BaseAction):
    """Calculate age."""
    action_type = "datetime_age"
    display_name = "计算年龄"
    description = "根据出生日期计算年龄"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute age calculation.

        Args:
            context: Execution context.
            params: Dict with birth_date, reference_date, output_var.

        Returns:
            ActionResult with age.
        """
        if not DT_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        birth_date = params.get('birth_date', None)
        reference_date = params.get('reference_date', None)
        output_var = params.get('output_var', 'age_result')

        if birth_date is None:
            return ActionResult(success=False, message="出生日期不能为空")

        try:
            def parse_date(d):
                if isinstance(d, str):
                    return datetime.strptime(d, '%Y-%m-%d').date()
                elif isinstance(d, date):
                    return d
                else:
                    return date.fromtimestamp(d) if isinstance(d, (int, float)) else None

            birth = parse_date(birth_date)
            if birth is None:
                return ActionResult(success=False, message="出生日期格式无效")

            if reference_date is None:
                ref = date.today()
            else:
                ref = parse_date(reference_date)
                if ref is None:
                    return ActionResult(success=False, message="参考日期格式无效")

            age = ref.year - birth.year
            if (ref.month, ref.day) < (birth.month, birth.day):
                age -= 1

            context.set(output_var, age)

            return ActionResult(
                success=True,
                message=f"年龄: {age} 岁",
                data={'age': age, 'birth_date': birth.isoformat(), 'reference_date': ref.isoformat()}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算年龄失败: {str(e)}"
            )
