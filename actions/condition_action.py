"""Condition action module for RabAI AutoClick.

Provides conditional operations:
- ConditionIfAction: If condition
- ConditionElseAction: Else branch
- ConditionElifAction: Elif branch
- ConditionEndAction: End condition
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConditionIfAction(BaseAction):
    """If condition."""
    action_type = "condition_if"
    display_name = "条件如果"
    description = "条件如果"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute if condition.

        Args:
            context: Execution context.
            params: Dict with condition.

        Returns:
            ActionResult with evaluation result.
        """
        condition = params.get('condition', 'False')

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_condition = context.resolve_value(condition)

            result = context.safe_exec(f"return_value = {resolved_condition}")

            context.set('_condition_result', result)
            context.set('_condition_branch_taken', bool(result))

            return ActionResult(
                success=True,
                message=f"条件{'满足' if result else '不满足'}",
                data={
                    'condition': resolved_condition,
                    'result': result,
                    'branch_taken': bool(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条件判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ConditionElseAction(BaseAction):
    """Else branch."""
    action_type = "condition_else"
    display_name = "条件否则"
    description = "条件否则分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute else branch.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating else branch.
        """
        prev_result = context.get('_condition_result', True)
        context.set('_condition_branch_taken', not prev_result)

        return ActionResult(
            success=True,
            message="否则分支",
            data={'branch_taken': not prev_result}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ConditionElifAction(BaseAction):
    """Elif branch."""
    action_type = "condition_elif"
    display_name = "条件否则如果"
    description = "条件否则如果分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute elif branch.

        Args:
            context: Execution context.
            params: Dict with condition.

        Returns:
            ActionResult with evaluation result.
        """
        condition = params.get('condition', 'False')

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            prev_taken = context.get('_condition_branch_taken', False)
            if prev_taken:
                context.set('_condition_branch_taken', False)
                return ActionResult(
                    success=True,
                    message="前述条件已满足，跳过",
                    data={'branch_taken': False}
                )

            resolved_condition = context.resolve_value(condition)
            result = context.safe_exec(f"return_value = {resolved_condition}")

            context.set('_condition_result', result)
            context.set('_condition_branch_taken', bool(result))

            return ActionResult(
                success=True,
                message=f"否则如果条件{'满足' if result else '不满足'}",
                data={
                    'condition': resolved_condition,
                    'result': result,
                    'branch_taken': bool(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"否则如果判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ConditionEndAction(BaseAction):
    """End condition."""
    action_type = "condition_end"
    display_name = "结束条件"
    description = "结束条件块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end condition.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating end.
        """
        context.delete('_condition_result')
        context.delete('_condition_branch_taken')

        return ActionResult(
            success=True,
            message="条件块结束"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
