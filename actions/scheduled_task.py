"""Scheduled task action module for RabAI AutoClick.

Provides scheduling and delay actions for timed automation,
including countdown, cron-style triggers, and interval loops.
"""

import os
import sys
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WaitUntilAction(BaseAction):
    """Wait until specified time.
    
    Supports absolute datetime, relative delay,
    and polling with condition check.
    """
    action_type = "wait_until"
    display_name = "等待到指定时间"
    description = "等待直到指定时间点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Wait until target time.
        
        Args:
            context: Execution context.
            params: Dict with keys: target_time, target_date,
                   timeout, save_to_var.
        
        Returns:
            ActionResult after wait completes.
        """
        target_time = params.get('target_time', None)
        target_date = params.get('target_date', None)
        timeout = params.get('timeout', 3600)
        save_to_var = params.get('save_to_var', None)

        # Parse target
        if target_time:
            if target_date:
                target_str = f"{target_date} {target_time}"
            else:
                today = datetime.now().strftime('%Y-%m-%d')
                target_str = f"{today} {target_time}"
            
            try:
                from datetime import datetime
                target_dt = datetime.strptime(target_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    target_dt = datetime.strptime(target_str, '%Y-%m-%d %H:%M')
                except ValueError as e:
                    return ActionResult(
                        success=False,
                        message=f"时间格式解析失败: {e}"
                    )
        else:
            return ActionResult(
                success=False,
                message="target_time is required"
            )

        # If target is in the past, add one day
        now = datetime.now()
        if target_dt < now:
            target_dt += timedelta(days=1)

        # Calculate wait time
        wait_seconds = (target_dt - now).total_seconds()
        
        if wait_seconds > timeout:
            result_data = {
                'waited': False,
                'timeout': True,
                'target_time': str(target_dt),
                'wait_seconds': wait_seconds
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"等待超时: 需要等待 {wait_seconds:.0f}s, 超时限制 {timeout}s"
            )

        # Wait
        start = time.time()
        time.sleep(wait_seconds)
        elapsed = time.time() - start

        result_data = {
            'waited': True,
            'target_time': str(target_dt),
            'wait_seconds': wait_seconds,
            'elapsed': elapsed
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"等待完成: 等待了 {elapsed:.1f}s",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['target_time']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'target_date': None,
            'timeout': 3600,
            'save_to_var': None
        }


class PollConditionAction(BaseAction):
    """Poll a condition until it becomes true or timeout.
    
    Supports variable-based conditions and max attempts.
    """
    action_type = "poll_condition"
    display_name = "轮询条件"
    description = "轮询检查条件直到为真或超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Poll until condition is met.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition_var, expected_value,
                   interval, max_attempts, save_to_var.
        
        Returns:
            ActionResult with poll result.
        """
        condition_var = params.get('condition_var', '')
        expected_value = params.get('expected_value', True)
        interval = params.get('interval', 1.0)
        max_attempts = params.get('max_attempts', 60)
        save_to_var = params.get('save_to_var', None)

        if not condition_var:
            return ActionResult(
                success=False,
                message="condition_var is required"
            )

        for attempt in range(1, max_attempts + 1):
            # Check if variable exists and matches
            if hasattr(context, 'variables') and condition_var in context.variables:
                current_value = context.variables[condition_var]
                if current_value == expected_value:
                    result_data = {
                        'met': True,
                        'attempts': attempt,
                        'condition_var': condition_var,
                        'value': current_value
                    }
                    if save_to_var:
                        context.variables[save_to_var] = result_data
                    return ActionResult(
                        success=True,
                        message=f"条件满足: 第 {attempt} 次尝试",
                        data=result_data
                    )

            if attempt < max_attempts:
                time.sleep(interval)

        result_data = {
            'met': False,
            'attempts': max_attempts,
            'condition_var': condition_var,
            'expected_value': expected_value
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=False,
            message=f"条件超时未满足: {condition_var} (尝试 {max_attempts} 次)"
        )

    def get_required_params(self) -> List[str]:
        return ['condition_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'expected_value': True,
            'interval': 1.0,
            'max_attempts': 60,
            'save_to_var': None
        }


class CountdownAction(BaseAction):
    """Display countdown timer.
    
    Shows progress and remaining time during wait.
    """
    action_type = "countdown"
    display_name = "倒计时"
    description = "显示倒计时等待"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run countdown.
        
        Args:
            context: Execution context.
            params: Dict with keys: duration, unit, show_progress,
                   save_to_var.
        
        Returns:
            ActionResult after countdown completes.
        """
        duration = params.get('duration', 10)
        unit = params.get('unit', 'seconds')
        show_progress = params.get('show_progress', False)
        save_to_var = params.get('save_to_var', None)

        try:
            dur = float(duration)
            if unit == 'minutes':
                dur *= 60
            elif unit == 'hours':
                dur *= 3600
            elif unit == 'milliseconds':
                dur /= 1000

            if dur <= 0 or dur > 86400:
                return ActionResult(
                    success=False,
                    message=f"Invalid duration: {duration}"
                )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid duration value: {duration}"
            )

        start = time.time()
        interval = min(dur / 20, 1.0)  # Update ~20 times or every 1s

        while True:
            remaining = dur - (time.time() - start)
            if remaining <= 0:
                break

            if show_progress:
                elapsed = dur - remaining
                pct = (elapsed / dur) * 100
                print(f"倒计时: {remaining:.1f}s ({pct:.0f}%)")

            time.sleep(min(interval, remaining))

        result_data = {
            'duration': dur,
            'elapsed': time.time() - start,
            'completed': True
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"倒计时完成: {duration} {unit}"
        )

    def get_required_params(self) -> List[str]:
        return ['duration']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'unit': 'seconds',
            'show_progress': False,
            'save_to_var': None
        }
