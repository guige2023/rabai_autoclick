"""Cron action module for RabAI AutoClick.

Provides cron/scheduling operations:
- CronCreateAction: Create cron job
- CronListAction: List cron jobs
- CronDeleteAction: Delete cron job
- CronNextRunAction: Calculate next run time
- CronParseAction: Parse cron expression
- CronValidateAction: Validate cron expression
"""

import subprocess
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CronCreateAction(BaseAction):
    """Create cron job."""
    action_type = "cron_create"
    display_name = "创建定时任务"
    description = "创建Cron定时任务"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with expression, command, user.

        Returns:
            ActionResult indicating success.
        """
        expression = params.get('expression', '')
        command = params.get('command', '')
        user = params.get('user', '')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_expr = context.resolve_value(expression)
            resolved_cmd = context.resolve_value(command)
            resolved_user = context.resolve_value(user) if user else None

            # Validate expression
            parts = resolved_expr.split()
            if len(parts) < 5:
                return ActionResult(
                    success=False,
                    message=f"Cron表达式无效: 需要5个字段 (分 时 日 月 周)"
                )

            # Read existing crontab
            existing = ''
            try:
                result = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    existing = result.stdout
            except:
                pass

            # Build new crontab entry
            new_entry = f"{resolved_expr} {resolved_cmd}\n"

            if resolved_user:
                new_entry = f"{resolved_expr} {resolved_user} {resolved_cmd}\n"

            new_crontab = existing.rstrip('\n') + '\n' + new_entry

            # Write new crontab
            proc = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate(new_crontab.encode('utf-8'))

            if proc.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Cron创建失败: {stderr.decode('utf-8')}"
                )

            return ActionResult(
                success=True,
                message=f"Cron任务已创建: {resolved_expr} {resolved_cmd}",
                data={'expression': resolved_expr, 'command': resolved_cmd}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Crontab命令超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['expression', 'command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'user': ''}


class CronListAction(BaseAction):
    """List cron jobs."""
    action_type = "cron_list"
    display_name = "列出定时任务"
    description = "列出所有Cron定时任务"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with cron list.
        """
        output_var = params.get('output_var', 'cron_jobs')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5)

            if result.returncode != 0:
                if 'no crontab' in result.stderr.lower():
                    context.set(output_var, [])
                    return ActionResult(
                        success=True,
                        message="无Cron任务",
                        data={'jobs': [], 'output_var': output_var}
                    )
                return ActionResult(
                    success=False,
                    message=f"Cron列出失败: {result.stderr}"
                )

            lines = result.stdout.strip().split('\n')
            jobs = []
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                parts = line.split(None, 5)
                if len(parts) >= 6:
                    jobs.append({
                        'expression': ' '.join(parts[:5]),
                        'command': parts[5]
                    })
                elif len(parts) >= 1:
                    jobs.append({
                        'raw': line
                    })

            context.set(output_var, jobs)

            return ActionResult(
                success=True,
                message=f"Cron任务: {len(jobs)} 个",
                data={'count': len(jobs), 'jobs': jobs, 'output_var': output_var}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Crontab命令超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron列出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cron_jobs'}


class CronDeleteAction(BaseAction):
    """Delete cron job."""
    action_type = "cron_delete"
    display_name = "删除定时任务"
    description = "删除Cron定时任务"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with command_pattern.

        Returns:
            ActionResult indicating success.
        """
        command_pattern = params.get('command_pattern', '')

        valid, msg = self.validate_type(command_pattern, str, 'command_pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(command_pattern)

            # Get current crontab
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5)

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message="无法读取当前Crontab"
                )

            lines = result.stdout.strip().split('\n')
            new_lines = []
            deleted = 0

            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    new_lines.append(stripped)
                    continue

                if resolved_pattern in stripped:
                    deleted += 1
                    continue

                new_lines.append(stripped)

            new_crontab = '\n'.join(new_lines) + '\n'

            proc = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate(new_crontab.encode('utf-8'))

            if proc.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Cron删除失败: {stderr.decode('utf-8')}"
                )

            return ActionResult(
                success=True,
                message=f"已删除 {deleted} 个Cron任务",
                data={'deleted': deleted}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Crontab命令超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command_pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class CronNextRunAction(BaseAction):
    """Calculate next run time."""
    action_type = "cron_next_run"
    display_name = "计算下次运行时间"
    description = "计算Cron表达式的下次执行时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute next run.

        Args:
            context: Execution context.
            params: Dict with expression, from_time, count, output_var.

        Returns:
            ActionResult with next run times.
        """
        expression = params.get('expression', '')
        from_time = params.get('from_time', '')
        count = params.get('count', 5)
        output_var = params.get('output_var', 'cron_next_runs')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_expr = context.resolve_value(expression)
            resolved_count = context.resolve_value(count)
            resolved_from = context.resolve_value(from_time) if from_time else None

            if resolved_from:
                current = datetime.fromisoformat(resolved_from.replace('Z', '+00:00'))
            else:
                current = datetime.now()

            parts = resolved_expr.split()
            if len(parts) < 5:
                return ActionResult(
                    success=False,
                    message=f"Cron表达式无效"
                )

            next_runs = []
            for _ in range(int(resolved_count)):
                current = self._next_run(current, parts)
                next_runs.append(current.isoformat())
                current += timedelta(minutes=1)

            context.set(output_var, next_runs)

            return ActionResult(
                success=True,
                message=f"下次运行: {next_runs[0]}",
                data={'next_runs': next_runs, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算下次运行时间失败: {str(e)}"
            )

    def _next_run(self, dt: datetime, parts: List[str]) -> datetime:
        """Calculate next run time from current time."""
        minute, hour, day, month, dow = parts

        # Simple next run - advance minute by 1 and find matching
        for _ in range(60 * 24 * 31):
            dt += timedelta(minutes=1)

            if not self._match_field(dt.minute, minute):
                continue
            if not self._match_field(dt.hour, hour):
                continue
            if not self._match_field(dt.day, day):
                continue
            if not self._match_field(dt.month, month):
                continue
            if not self._match_field(dt.weekday(), dow):
                continue

            return dt

        return dt + timedelta(days=31)

    def _match_field(self, value: int, field: str) -> bool:
        """Check if value matches cron field."""
        if field == '*':
            return True

        for part in field.split(','):
            if '/' in part:
                step_parts = part.split('/')
                range_part = step_parts[0]
                step = int(step_parts[1])

                if range_part == '*':
                    start, end = 0, 59 if value < 24 else 23
                elif '-' in range_part:
                    start, end = map(int, range_part.split('-'))
                else:
                    start = int(range_part)
                    end = 59 if value < 24 else 23

                if value >= start and value <= end and (value - start) % step == 0:
                    return True
            elif '-' in part:
                start, end = map(int, part.split('-'))
                if start <= value <= end:
                    return True
            elif int(part) == value:
                return True

        return False

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'from_time': '', 'count': 5, 'output_var': 'cron_next_runs'}


class CronValidateAction(BaseAction):
    """Validate cron expression."""
    action_type = "cron_validate"
    display_name = "验证Cron表达式"
    description = "验证Cron表达式是否有效"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with expression, output_var.

        Returns:
            ActionResult with validation result.
        """
        expression = params.get('expression', '')
        output_var = params.get('output_var', 'cron_valid')

        valid, msg = self.validate_type(expression, str, 'expression')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_expr = context.resolve_value(expression)

            parts = resolved_expr.split()
            if len(parts) < 5:
                context.set(output_var, False)
                return ActionResult(
                    success=False,
                    message=f"表达式无效: 需要5个字段",
                    data={'valid': False, 'expression': resolved_expr, 'output_var': output_var}
                )

            # Validate each field
            field_names = ['minute', 'hour', 'day', 'month', 'weekday']
            field_ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]

            errors = []
            for i, (part, (min_v, max_v)) in enumerate(zip(parts[:5], field_ranges)):
                if not self._validate_field(part, min_v, max_v):
                    errors.append(f"{field_names[i]}: '{part}' 无效")

            context.set(output_var, len(errors) == 0)

            return ActionResult(
                success=len(errors) == 0,
                message=f"Cron验证 {'通过' if len(errors) == 0 else '失败'}",
                data={'valid': len(errors) == 0, 'errors': errors, 'expression': resolved_expr, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron验证失败: {str(e)}"
            )

    def _validate_field(self, field: str, min_v: int, max_v: int) -> bool:
        """Validate a single cron field."""
        if field == '*':
            return True

        for part in field.split(','):
            if '/' in part:
                step_parts = part.split('/')
                if len(step_parts) != 2:
                    return False
                range_part, step = step_parts
                try:
                    int(step)
                    if range_part != '*':
                        if '-' in range_part:
                            s, e = map(int, range_part.split('-'))
                            if s > e:
                                return False
                        else:
                            v = int(range_part)
                            if v < min_v or v > max_v:
                                return False
                except ValueError:
                    return False
            elif '-' in part:
                try:
                    s, e = map(int, part.split('-'))
                    if s < min_v or e > max_v or s > e:
                        return False
                except ValueError:
                    return False
            else:
                try:
                    v = int(part)
                    if v < min_v or v > max_v:
                        return False
                except ValueError:
                    return False

        return True

    def get_required_params(self) -> List[str]:
        return ['expression']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cron_valid'}
