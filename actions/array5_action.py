"""Array5 action module for RabAI AutoClick.

Provides additional array operations:
- ArrayRotateAction: Rotate array
- ArrayUniqueAction: Get unique elements
- ArraySampleAction: Random sample from array
- ArrayZipAction: Zip multiple arrays
- ArrayDifferenceAction: Get array difference
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArrayRotateAction(BaseAction):
    """Rotate array."""
    action_type = "array5_rotate"
    display_name = "旋转数组"
    description = "旋转数组元素"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rotate.

        Args:
            context: Execution context.
            params: Dict with array, positions, direction, output_var.

        Returns:
            ActionResult with rotated array.
        """
        array = params.get('array', [])
        positions = params.get('positions', 1)
        direction = params.get('direction', 'left')
        output_var = params.get('output_var', 'rotated_array')

        try:
            resolved = context.resolve_value(array)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_positions = int(context.resolve_value(positions)) if positions else 1
            resolved_direction = context.resolve_value(direction) if direction else 'left'

            positions = resolved_positions % len(resolved) if resolved else 0

            if resolved_direction == 'right':
                positions = -positions

            result = resolved[positions:] + resolved[:positions]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组旋转: {positions}位",
                data={
                    'original': resolved,
                    'rotated': result,
                    'positions': positions,
                    'direction': resolved_direction,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"旋转数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array', 'positions']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'direction': 'left', 'output_var': 'rotated_array'}


class ArrayUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "array5_unique"
    display_name = "数组去重"
    description = "获取数组唯一元素"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unique.

        Args:
            context: Execution context.
            params: Dict with array, preserve_order, output_var.

        Returns:
            ActionResult with unique elements.
        """
        array = params.get('array', [])
        preserve_order = params.get('preserve_order', True)
        output_var = params.get('output_var', 'unique_array')

        try:
            resolved = context.resolve_value(array)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_preserve = bool(context.resolve_value(preserve_order)) if preserve_order else True

            if resolved_preserve:
                seen = set()
                result = []
                for item in resolved:
                    if item not in seen:
                        seen.add(item)
                        result.append(item)
            else:
                result = list(set(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组去重: {len(result)}个元素",
                data={
                    'original': resolved,
                    'unique': result,
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
        return {'preserve_order': True, 'output_var': 'unique_array'}


class ArraySampleAction(BaseAction):
    """Random sample from array."""
    action_type = "array5_sample"
    display_name = "数组抽样"
    description = "从数组随机抽样"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sample.

        Args:
            context: Execution context.
            params: Dict with array, size, output_var.

        Returns:
            ActionResult with random sample.
        """
        array = params.get('array', [])
        size = params.get('size', 1)
        output_var = params.get('output_var', 'sample_array')

        try:
            import random

            resolved = context.resolve_value(array)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_size = int(context.resolve_value(size)) if size else 1
            resolved_size = min(resolved_size, len(resolved))

            result = random.sample(resolved, resolved_size)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组抽样: {resolved_size}个元素",
                data={
                    'original': resolved,
                    'sample': result,
                    'size': resolved_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组抽样失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sample_array'}


class ArrayZipAction(BaseAction):
    """Zip multiple arrays."""
    action_type = "array5_zip"
    display_name = "数组合并"
    description = "合并多个数组"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with arrays, output_var.

        Returns:
            ActionResult with zipped arrays.
        """
        arrays = params.get('arrays', [])
        output_var = params.get('output_var', 'zipped_array')

        try:
            resolved = context.resolve_value(arrays)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(zip(*resolved))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组合并: {len(result)}组",
                data={
                    'original': resolved,
                    'zipped': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['arrays']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zipped_array'}


class ArrayDifferenceAction(BaseAction):
    """Get array difference."""
    action_type = "array5_difference"
    display_name = "数组差集"
    description = "获取数组的差集"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute difference.

        Args:
            context: Execution context.
            params: Dict with array1, array2, output_var.

        Returns:
            ActionResult with array difference.
        """
        array1 = params.get('array1', [])
        array2 = params.get('array2', [])
        output_var = params.get('output_var', 'difference_array')

        try:
            resolved1 = context.resolve_value(array1)
            resolved2 = context.resolve_value(array2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            set1 = set(resolved1)
            set2 = set(resolved2)
            result = list(set1 - set2)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组差集: {len(result)}个元素",
                data={
                    'array1': resolved1,
                    'array2': resolved2,
                    'difference': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array1', 'array2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_array'}