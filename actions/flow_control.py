"""Flow control action module for RabAI AutoClick.

Provides flow control actions:
- LoopAction: Repeat execution a specified number of times
- WhileAction: Loop while a condition is true
- ConditionAction: Conditional branching based on expression
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LoopAction(BaseAction):
    """Repeat execution a specified number of times."""
    action_type = "loop"
    display_name = "循环"
    description = "循环执行指定次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a loop action.

        Args:
            context: Execution context.
            params: Dict with count (iterations), step_id (to execute).

        Returns:
            ActionResult with iteration count.
        """
        count = params.get('count', 1)
        step_id = params.get('step_id')

        # Validate count
        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if count < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'count' must be >= 0, got {count}"
            )

        # Store loop counter in context
        loop_var = params.get('loop_var', '_loop_count')
        interval = params.get('interval', 0.0)

        # Validate interval
        valid, msg = self.validate_type(interval, (int, float), 'interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if interval < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'interval' must be >= 0, got {interval}"
            )

        try:
            executed = 0
            for i in range(count):
                context.set(loop_var, i)
                executed = i + 1
                if interval > 0 and i < count - 1:
                    time.sleep(interval)

            return ActionResult(
                success=True,
                message=f"循环完成: {executed}次",
                data={
                    'count': count,
                    'executed': executed,
                    'loop_var': loop_var
                },
                next_step_id=step_id
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"循环执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'count': 1,
            'step_id': None,
            'loop_var': '_loop_count',
            'interval': 0.0
        }


class WhileAction(BaseAction):
    """Loop while a condition is true."""
    action_type = "while_loop"
    display_name = "条件循环"
    description = "当条件满足时循环执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a while loop action.

        Args:
            context: Execution context.
            params: Dict with condition (expr), max_iterations,
                   step_id, interval, loop_var.

        Returns:
            ActionResult with iteration count.
        """
        condition = params.get('condition', '')
        max_iterations = params.get('max_iterations', 100)
        step_id = params.get('step_id')
        interval = params.get('interval', 0.0)
        loop_var = params.get('loop_var', '_loop_count')

        # Validate condition
        if not condition:
            return ActionResult(
                success=False,
                message="未指定循环条件"
            )
        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate max_iterations
        valid, msg = self.validate_type(max_iterations, int, 'max_iterations')
        if not valid:
            return ActionResult(success=False, message=msg)
        if max_iterations <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'max_iterations' must be > 0, got {max_iterations}"
            )

        # Validate interval
        valid, msg = self.validate_type(interval, (int, float), 'interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if interval < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'interval' must be >= 0, got {interval}"
            )

        try:
            executed = 0
            for i in range(max_iterations):
                try:
                    result = context.safe_exec(condition)
                    condition_met = bool(result)
                except Exception:
                    condition_met = False

                if not condition_met:
                    break

                context.set(loop_var, i)
                executed = i + 1
                if interval > 0:
                    time.sleep(interval)

            return ActionResult(
                success=True,
                message=f"条件循环完成: {executed}次",
                data={
                    'executed': executed,
                    'max_iterations': max_iterations,
                    'loop_var': loop_var
                },
                next_step_id=step_id
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条件循环执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_iterations': 100,
            'step_id': None,
            'interval': 0.0,
            'loop_var': '_loop_count'
        }


class ConditionAction(BaseAction):
    """Conditional branching based on expression evaluation."""
    action_type = "condition"
    display_name = "条件分支"
    description = "根据条件表达式的结果选择执行路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a condition action.

        Args:
            context: Execution context.
            params: Dict with condition (expr), then_step, else_step.

        Returns:
            ActionResult with next_step_id based on condition.
        """
        condition = params.get('condition', '')
        then_step = params.get('then_step')
        else_step = params.get('else_step')

        # Validate condition
        if not condition:
            return ActionResult(
                success=False,
                message="未指定条件表达式"
            )
        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result = context.safe_exec(condition)
            condition_met = bool(result)

            if condition_met:
                next_step = then_step
                branch = 'then'
            else:
                next_step = else_step
                branch = 'else'

            return ActionResult(
                success=True,
                message=f"条件判断: {branch}分支 (结果={result})",
                data={
                    'condition': condition,
                    'result': result,
                    'branch': branch,
                    'next_step': next_step
                },
                next_step_id=next_step
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条件判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'then_step': None,
            'else_step': None
        }


class BreakAction(BaseAction):
    """Break out of a loop."""
    action_type = "break"
    display_name = "跳出循环"
    description = "立即跳出当前循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a break action.

        Args:
            context: Execution context.
            params: Dict with message (optional).

        Returns:
            ActionResult indicating break was executed.
        """
        message = params.get('message', '跳出循环')

        try:
            return ActionResult(
                success=True,
                message=str(message),
                data={'action': 'break'}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"跳出循环失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message': '跳出循环'}


class ContinueAction(BaseAction):
    """Continue to next iteration of a loop."""
    action_type = "continue"
    display_name = "继续循环"
    description = "跳到下一次循环迭代"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a continue action.

        Args:
            context: Execution context.
            params: Dict with message (optional).

        Returns:
            ActionResult indicating continue was executed.
        """
        message = params.get('message', '继续循环')

        try:
            return ActionResult(
                success=True,
                message=str(message),
                data={'action': 'continue'}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"继续循环失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message': '继续循环'}