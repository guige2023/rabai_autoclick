"""Filter2 action module for RabAI AutoClick.

Provides additional filtering operations:
- FilterEvensAction: Filter even numbers
- FilterOddsAction: Filter odd numbers
- FilterPositivesAction: Filter positive numbers
- FilterNegativesAction: Filter negative numbers
- FilterZerosAction: Filter zeros
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterEvensAction(BaseAction):
    """Filter even numbers."""
    action_type = "filter2_evens"
    display_name = "过滤偶数"
    description = "过滤出偶数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter evens.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with filtered list.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'filtered_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [x for x in resolved if int(x) % 2 == 0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤偶数: {len(result)}个",
                data={
                    'original': resolved,
                    'filtered': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤偶数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class FilterOddsAction(BaseAction):
    """Filter odd numbers."""
    action_type = "filter2_odds"
    display_name = "过滤奇数"
    description = "过滤出奇数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter odds.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with filtered list.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'filtered_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [x for x in resolved if int(x) % 2 != 0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤奇数: {len(result)}个",
                data={
                    'original': resolved,
                    'filtered': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤奇数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class FilterPositivesAction(BaseAction):
    """Filter positive numbers."""
    action_type = "filter2_positives"
    display_name = "过滤正数"
    description = "过滤出正数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter positives.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with filtered list.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'filtered_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [x for x in resolved if float(x) > 0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤正数: {len(result)}个",
                data={
                    'original': resolved,
                    'filtered': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤正数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class FilterNegativesAction(BaseAction):
    """Filter negative numbers."""
    action_type = "filter2_negatives"
    display_name = "过滤负数"
    description = "过滤出负数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter negatives.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with filtered list.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'filtered_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [x for x in resolved if float(x) < 0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤负数: {len(result)}个",
                data={
                    'original': resolved,
                    'filtered': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤负数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'filtered_list'}


class FilterZerosAction(BaseAction):
    """Filter zeros."""
    action_type = "filter2_zeros"
    display_name = "过滤零"
    description = "过滤出零值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter zeros.

        Args:
            context: Execution context.
            params: Dict with list, keep_zeros, output_var.

        Returns:
            ActionResult with filtered list.
        """
        input_list = params.get('list', [])
        keep_zeros = params.get('keep_zeros', True)
        output_var = params.get('output_var', 'filtered_list')

        try:
            resolved = context.resolve_value(input_list)
            resolved_keep = bool(context.resolve_value(keep_zeros)) if keep_zeros else True

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if resolved_keep:
                result = [x for x in resolved if float(x) == 0]
            else:
                result = [x for x in resolved if float(x) != 0]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤零: {len(result)}个",
                data={
                    'original': resolved,
                    'filtered': result,
                    'keep_zeros': resolved_keep,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤零失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keep_zeros': True, 'output_var': 'filtered_list'}