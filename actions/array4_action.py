"""Array4 action module for RabAI AutoClick.

Provides additional array operations:
- ArrayUniqueAction: Get unique elements
- ArrayReverseAction: Reverse array
- ArraySortAscAction: Sort ascending
- ArraySortDescAction: Sort descending
- ArrayFlattenAction: Flatten nested array
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArrayUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "array4_unique"
    display_name = "数组去重"
    description = "获取数组唯一元素"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unique.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with unique elements.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'unique_result')

        try:
            resolved = context.resolve_value(array)
            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"数组去重失败: 输入不是数组"
                )

            result = list(dict.fromkeys(resolved))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组去重完成: {len(result)} 个元素",
                data={
                    'original': resolved,
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组去重失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unique_result'}


class ArrayReverseAction(BaseAction):
    """Reverse array."""
    action_type = "array4_reverse"
    display_name = "数组反转"
    description = "反转数组顺序"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with reversed array.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'reversed_result')

        try:
            resolved = context.resolve_value(array)
            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"数组反转失败: 输入不是数组"
                )

            result = list(resolved)[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组反转完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组反转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_result'}


class ArraySortAscAction(BaseAction):
    """Sort ascending."""
    action_type = "array4_sort_asc"
    display_name = "数组升序"
    description = "数组升序排列"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort ascending.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with sorted array.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'sorted_result')

        try:
            resolved = context.resolve_value(array)
            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"数组升序失败: 输入不是数组"
                )

            result = sorted(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组升序完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组升序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_result'}


class ArraySortDescAction(BaseAction):
    """Sort descending."""
    action_type = "array4_sort_desc"
    display_name = "数组降序"
    description = "数组降序排列"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort descending.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with sorted array.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'sorted_result')

        try:
            resolved = context.resolve_value(array)
            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"数组降序失败: 输入不是数组"
                )

            result = sorted(resolved, reverse=True)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组降序完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组降序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_result'}


class ArrayFlattenAction(BaseAction):
    """Flatten nested array."""
    action_type = "array4_flatten"
    display_name = "数组扁平化"
    description = "将嵌套数组展平"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with flattened array.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'flattened_result')

        try:
            resolved = context.resolve_value(array)

            def _flatten(lst):
                result = []
                for item in lst:
                    if isinstance(item, (list, tuple)):
                        result.extend(_flatten(item))
                    else:
                        result.append(item)
                return result

            result = _flatten(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组扁平化完成: {len(result)} 个元素",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组扁平化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flattened_result'}