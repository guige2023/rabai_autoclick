"""Array2 action module for RabAI AutoClick.

Provides additional array operations:
- ArraySliceAction: Slice array
- ArrayReverseAction: Reverse array
- ArrayUniqueAction: Get unique elements
- ArrayFlattenAction: Flatten nested array
- ArrayChunkAction: Split array into chunks
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArraySliceAction(BaseAction):
    """Slice array."""
    action_type = "array_slice"
    display_name = "数组切片"
    description = "切片数组指定范围"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute array slice.

        Args:
            context: Execution context.
            params: Dict with array, start, end, step, output_var.

        Returns:
            ActionResult with sliced array.
        """
        array = params.get('array', [])
        start = params.get('start', 0)
        end = params.get('end', None)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'sliced_array')

        try:
            resolved_array = context.resolve_value(array)
            resolved_start = int(context.resolve_value(start)) if start is not None else 0
            resolved_end = int(context.resolve_value(end)) if end is not None else None
            resolved_step = int(context.resolve_value(step)) if step is not None else 1

            if not isinstance(resolved_array, (list, tuple, str)):
                return ActionResult(
                    success=False,
                    message="数组切片需要数组或字符串"
                )

            result = resolved_array[resolved_start:resolved_end:resolved_step]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组切片: {len(result)} 项",
                data={
                    'original': resolved_array,
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组切片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'step': 1, 'output_var': 'sliced_array'}


class ArrayReverseAction(BaseAction):
    """Reverse array."""
    action_type = "array_reverse"
    display_name = "反转数组"
    description = "反转数组顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute array reverse.

        Args:
            context: Execution context.
            params: Dict with array, output_var.

        Returns:
            ActionResult with reversed array.
        """
        array = params.get('array', [])
        output_var = params.get('output_var', 'reversed_array')

        try:
            resolved = context.resolve_value(array)

            if isinstance(resolved, list):
                result = list(reversed(resolved))
            elif isinstance(resolved, tuple):
                result = tuple(reversed(resolved))
            elif isinstance(resolved, str):
                result = resolved[::-1]
            else:
                return ActionResult(
                    success=False,
                    message="反转需要数组或字符串"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组反转: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_array'}


class ArrayUniqueAction(BaseAction):
    """Get unique elements."""
    action_type = "array_unique"
    display_name = "数组去重"
    description = "获取数组唯一元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute array unique.

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
            resolved_preserve = context.resolve_value(preserve_order) if preserve_order else True

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="去重需要数组"
                )

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
                message=f"数组去重: {len(result)} 项",
                data={
                    'original': resolved,
                    'count': len(result),
                    'result': result,
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


class ArrayFlattenAction(BaseAction):
    """Flatten nested array."""
    action_type = "array_flatten"
    display_name = "扁平化数组"
    description = "将嵌套数组展平"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute array flatten.

        Args:
            context: Execution context.
            params: Dict with array, depth, output_var.

        Returns:
            ActionResult with flattened array.
        """
        array = params.get('array', [])
        depth = params.get('depth', None)
        output_var = params.get('output_var', 'flattened_array')

        try:
            resolved = context.resolve_value(array)
            resolved_depth = context.resolve_value(depth) if depth is not None else None

            def flatten(lst, current_depth=0):
                result = []
                for item in lst:
                    if isinstance(item, (list, tuple)):
                        if resolved_depth is None or current_depth < resolved_depth:
                            result.extend(flatten(item, current_depth + 1))
                        else:
                            result.append(item)
                    else:
                        result.append(item)
                return result

            result = flatten(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组扁平化: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'depth': None, 'output_var': 'flattened_array'}


class ArrayChunkAction(BaseAction):
    """Split array into chunks."""
    action_type = "array_chunk"
    display_name = "数组分块"
    description = "将数组分割成多个块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute array chunk.

        Args:
            context: Execution context.
            params: Dict with array, size, output_var.

        Returns:
            ActionResult with chunks.
        """
        array = params.get('array', [])
        size = params.get('size', 1)
        output_var = params.get('output_var', 'chunked_array')

        try:
            resolved = context.resolve_value(array)
            resolved_size = int(context.resolve_value(size))

            if resolved_size <= 0:
                return ActionResult(
                    success=False,
                    message=f"块大小必须大于0: {resolved_size}"
                )

            result = [resolved[i:i + resolved_size] for i in range(0, len(resolved), resolved_size)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组分块: {len(result)} 块",
                data={
                    'original': resolved,
                    'chunk_size': resolved_size,
                    'chunks': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数组分块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chunked_array'}