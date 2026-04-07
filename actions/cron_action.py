"""Cron scheduling action module for RabAI AutoClick.

Provides cron operations:
- CronParseAction: Parse cron expression
- CronNextAction: Get next run times
- CronValidateAction: Validate cron expression
- CronScheduleAction: Schedule action to run
- CronListAction: List scheduled actions
- CronRemoveAction: Remove scheduled action
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CronParseAction(BaseAction):
    """Parse cron expression."""
    action_type = "cron_parse"
    display_name = "Cron解析"
    description = "解析Cron表达式"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cron parse."""
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'cron_parsed')

        if not expression:
            return ActionResult(success=False, message="expression is required")

        try:
            resolved_expr = context.resolve_value(expression) if context else expression

            parts = resolved_expr.split()
            if len(parts) not in (5, 6):
                return ActionResult(success=False, message="Cron must have 5 or 6 fields")

            field_names = ['minute', 'hour', 'day_of_month', 'month', 'day_of_week']
            if len(parts) == 6:
                field_names.insert(0, 'second')

            result = dict(zip(field_names, parts))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cron parsed: {resolved_expr}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cron parse error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cron_parsed'}


class CronNextAction(BaseAction):
    """Get next cron run times."""
    action_type = "cron_next"
    display_name = "Cron下次运行"
    description = "计算Cron下次运行时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cron next."""
        expression = params.get('expression', '')
        count = params.get('count', 5)
        base_time = params.get('base_time', None)
        output_var = params.get('output_var', 'cron_next_runs')

        if not expression:
            return ActionResult(success=False, message="expression is required")

        try:
            from croniter import croniter

            resolved_expr = context.resolve_value(expression) if context else expression
            resolved_count = context.resolve_value(count) if context else count

            if base_time:
                resolved_base = context.resolve_value(base_time) if context else base_time
                if isinstance(resolved_base, str):
                    resolved_base = datetime.fromisoformat(resolved_base)
            else:
                resolved_base = datetime.now()

            cron = croniter(resolved_expr, resolved_base)
            next_runs = [cron.get_next(datetime) for _ in range(resolved_count)]

            result = {'next_runs': [dt.isoformat() for dt in next_runs], 'count': len(next_runs)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Next {len(next_runs)} runs calculated", data=result)
        except ImportError:
            return ActionResult(success=False, message="croniter not installed. Run: pip install croniter")
        except Exception as e:
            return ActionResult(success=False, message=f"Cron next error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 5, 'base_time': None, 'output_var': 'cron_next_runs'}


class CronValidateAction(BaseAction):
    """Validate cron expression."""
    action_type = "cron_validate"
    display_name = "Cron验证"
    description = "验证Cron表达式"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cron validate."""
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'cron_valid')

        if not expression:
            return ActionResult(success=False, message="expression is required")

        try:
            from croniter import croniter

            resolved_expr = context.resolve_value(expression) if context else expression

            base = datetime.now()
            cron = croniter(resolved_expr, base)
            next_run = cron.get_next(datetime)

            result = {'valid': True, 'expression': resolved_expr, 'next_run': next_run.isoformat()}
            if context:
                context.set(output_var, True)
            return ActionResult(success=True, message=f"Valid cron expression. Next run: {next_run.isoformat()}", data=result)
        except ImportError:
            return ActionResult(success=False, message="croniter not installed")
        except Exception as e:
            result = {'valid': False, 'error': str(e), 'expression': resolved_expr}
            if context:
                context.set(output_var, False)
            return ActionResult(success=False, message=f"Invalid cron: {str(e)}", data=result)

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cron_valid'}


class CronDescribeAction(BaseAction):
    """Describe cron expression in human-readable terms."""
    action_type = "cron_describe"
    display_name = "Cron描述"
    description = "将Cron表达式转为自然语言"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cron describe."""
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'cron_description')

        if not expression:
            return ActionResult(success=False, message="expression is required")

        try:
            from croniter import croniter

            resolved_expr = context.resolve_value(expression) if context else expression

            parts = resolved_expr.split()
            if len(parts) != 5:
                return ActionResult(success=False, message="Cron must have exactly 5 fields")

            minute, hour, dom, month, dow = parts

            desc_parts = []
            if minute == '*':
                desc_parts.append("every minute")
            elif minute.startswith('*/'):
                desc_parts.append(f"every {minute[2:]} minutes")
            else:
                desc_parts.append(f"at minute {minute}")

            if hour == '*':
                desc_parts.append("every hour")
            elif hour.startswith('*/'):
                desc_parts.append(f"every {hour[2:]} hours")
            else:
                desc_parts.append(f"at hour {hour}")

            if dom != '*':
                desc_parts.append(f"on day {dom}")
            if month != '*':
                desc_parts.append(f"in month {month}")
            if dow != '*':
                day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                try:
                    dow_num = int(dow)
                    desc_parts.append(f"on {day_names[dow_num - 1]}")
                except (ValueError, IndexError):
                    desc_parts.append(f"on {dow}")

            description = " ".join(desc_parts)

            result = {'expression': resolved_expr, 'description': description}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=description, data=result)
        except ImportError:
            return ActionResult(success=False, message="croniter not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Cron describe error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cron_description'}
