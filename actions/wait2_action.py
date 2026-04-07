"""Wait2 action module for RabAI AutoClick.

Provides advanced wait operations:
- WaitRandomAction: Wait random time
- WaitUntilAction: Wait until condition
- WaitForVariableAction: Wait for variable value
"""

import time
import random
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WaitRandomAction(BaseAction):
    """Wait random time."""
    action_type = "wait_random"
    display_name = "随机等待"
    description = "随机等待一段时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random wait.

        Args:
            context: Execution context.
            params: Dict with min_seconds, max_seconds.

        Returns:
            ActionResult indicating completion.
        """
        min_seconds = params.get('min_seconds', 1)
        max_seconds = params.get('max_seconds', 5)

        try:
            resolved_min = float(context.resolve_value(min_seconds))
            resolved_max = float(context.resolve_value(max_seconds))

            wait_time = random.uniform(resolved_min, resolved_max)
            time.sleep(wait_time)

            return ActionResult(
                success=True,
                message=f"随机等待完成: {wait_time:.2f} 秒",
                data={'wait_time': wait_time}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机等待失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['min_seconds', 'max_seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class WaitUntilAction(BaseAction):
    """Wait until condition."""
    action_type = "wait_until"
    display_name = "等待条件"
    description = "等待直到条件满足"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute wait until.

        Args:
            context: Execution context.
            params: Dict with condition, timeout, interval.

        Returns:
            ActionResult with result.
        """
        condition = params.get('condition', 'False')
        timeout = params.get('timeout', 30)
        interval = params.get('interval', 1)

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_condition = context.resolve_value(condition)
            resolved_timeout = float(context.resolve_value(timeout))
            resolved_interval = float(context.resolve_value(interval))

            start_time = time.time()
            result = False

            while time.time() - start_time < resolved_timeout:
                if context.safe_exec(f"return_value = {resolved_condition}"):
                    result = True
                    break
                time.sleep(resolved_interval)

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                message=f"等待条件{'满足' if result else '超时'}: {elapsed:.2f} 秒",
                data={
                    'result': result,
                    'elapsed': elapsed,
                    'timeout': resolved_timeout
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待条件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 30, 'interval': 1}


class WaitForVariableAction(BaseAction):
    """Wait for variable value."""
    action_type = "wait_for_variable"
    display_name = "等待变量"
    description = "等待变量达到期望值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute wait for variable.

        Args:
            context: Execution context.
            params: Dict with name, value, timeout, interval.

        Returns:
            ActionResult with result.
        """
        name = params.get('name', '')
        value = params.get('value', None)
        timeout = params.get('timeout', 30)
        interval = params.get('interval', 1)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_timeout = float(context.resolve_value(timeout))
            resolved_interval = float(context.resolve_value(interval))

            start_time = time.time()
            result = False

            while time.time() - start_time < resolved_timeout:
                current_value = context.get(resolved_name)
                if value is None:
                    if current_value is not None:
                        result = True
                        break
                elif current_value == context.resolve_value(value):
                    result = True
                    break
                time.sleep(resolved_interval)

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                message=f"等待变量{'达到' if result else '未达到'}: {elapsed:.2f} 秒",
                data={
                    'result': result,
                    'elapsed': elapsed,
                    'timeout': resolved_timeout
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': None, 'timeout': 30, 'interval': 1}