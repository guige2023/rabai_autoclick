"""Loop action module for RabAI AutoClick.

Provides loop operations:
- LoopForAction: For loop
- LoopWhileAction: While loop
- LoopBreakAction: Break loop
- LoopContinueAction: Continue loop
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LoopForAction(BaseAction):
    """For loop."""
    action_type = "loop_for"
    display_name = "For循环"
    description = "For循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute for loop.

        Args:
            context: Execution context.
            params: Dict with items, variable, output_var.

        Returns:
            ActionResult with loop results.
        """
        items = params.get('items', [])
        variable = params.get('variable', 'item')
        output_var = params.get('output_var', 'loop_results')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(variable, str, 'variable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_var = context.resolve_value(variable)

            results = []
            for i, item in enumerate(resolved_items):
                context.set(resolved_var, item)
                context.set('_loop_index', i)
                results.append(item)

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"For循环完成: {len(results)} 次迭代",
                data={
                    'count': len(results),
                    'items': results,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"For循环失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'variable']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'loop_results'}


class LoopWhileAction(BaseAction):
    """While loop."""
    action_type = "loop_while"
    display_name = "While循环"
    description = "While循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute while loop.

        Args:
            context: Execution context.
            params: Dict with condition, max_iterations, output_var.

        Returns:
            ActionResult with loop results.
        """
        condition = params.get('condition', 'True')
        max_iterations = params.get('max_iterations', 100)
        output_var = params.get('output_var', 'loop_results')

        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_condition = context.resolve_value(condition)
            resolved_max = context.resolve_value(max_iterations)

            results = []
            iterations = 0
            max_i = int(resolved_max)

            while context.safe_exec(f"return_value = {resolved_condition}"):
                results.append(iterations)
                iterations += 1
                if iterations >= max_i:
                    break

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"While循环完成: {iterations} 次迭代",
                data={
                    'count': iterations,
                    'results': results,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"While循环失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_iterations': 100, 'output_var': 'loop_results'}


class LoopBreakAction(BaseAction):
    """Break loop."""
    action_type = "loop_break"
    display_name = "跳出循环"
    description = "跳出循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute break.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating break.
        """
        context.set('_loop_break', True)

        return ActionResult(
            success=True,
            message="循环已跳出"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LoopContinueAction(BaseAction):
    """Continue loop."""
    action_type = "loop_continue"
    display_name = "继续循环"
    description = "继续下次循环"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute continue.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating continue.
        """
        context.set('_loop_continue', True)

        return ActionResult(
            success=True,
            message="继续下次循环"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}