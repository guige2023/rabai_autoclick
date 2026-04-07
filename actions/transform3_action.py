"""Transform3 action module for RabAI AutoClick.

Provides additional transform operations:
- TransformZipAction: Zip two lists
- TransformUnzipAction: Unzip a zipped list
- TransformEnumerateAction: Enumerate a list
- TransformChunkAction: Chunk a list
- TransformFlattenAction: Flatten nested list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformZipAction(BaseAction):
    """Zip two lists."""
    action_type = "transform3_zip"
    display_name = "合并列表"
    description = "将两个列表合并为一个元组列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with list1, list2, output_var.

        Returns:
            ActionResult with zipped list.
        """
        list1 = params.get('list1', [])
        list2 = params.get('list2', [])
        output_var = params.get('output_var', 'zipped_list')

        try:
            resolved1 = context.resolve_value(list1)
            resolved2 = context.resolve_value(list2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            result = list(zip(resolved1, resolved2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"合并列表: {len(result)} 个元组",
                data={
                    'list1': resolved1,
                    'list2': resolved2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list1', 'list2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zipped_list'}


class TransformUnzipAction(BaseAction):
    """Unzip a zipped list."""
    action_type = "transform3_unzip"
    display_name = "拆分列表"
    description = "将元组列表拆分回两个列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip.

        Args:
            context: Execution context.
            params: Dict with zipped, output_var.

        Returns:
            ActionResult with two lists.
        """
        zipped = params.get('zipped', [])
        output_var = params.get('output_var', 'unzipped_lists')

        try:
            resolved = context.resolve_value(zipped)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="zipped 必须是列表"
                )

            list1, list2 = zip(*resolved)
            result = {'list1': list(list1), 'list2': list(list2)}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"拆分列表: {len(result['list1'])} 个元素",
                data={
                    'zipped': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拆分列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zipped']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unzipped_lists'}


class TransformEnumerateAction(BaseAction):
    """Enumerate a list."""
    action_type = "transform3_enumerate"
    display_name = "枚举列表"
    description = "为列表元素添加索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enumerate.

        Args:
            context: Execution context.
            params: Dict with items, start, output_var.

        Returns:
            ActionResult with enumerated list.
        """
        items = params.get('items', [])
        start = params.get('start', 0)
        output_var = params.get('output_var', 'enumerated_list')

        try:
            resolved = context.resolve_value(items)
            resolved_start = int(context.resolve_value(start))

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(enumerate(resolved, start=resolved_start))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"枚举列表: {len(result)} 个元素",
                data={
                    'items': resolved,
                    'start': resolved_start,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"枚举列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'output_var': 'enumerated_list'}


class TransformChunkAction(BaseAction):
    """Chunk a list."""
    action_type = "transform3_chunk"
    display_name = "分割列表块"
    description = "将列表分割成指定大小的块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute chunk.

        Args:
            context: Execution context.
            params: Dict with items, size, output_var.

        Returns:
            ActionResult with chunked list.
        """
        items = params.get('items', [])
        size = params.get('size', 2)
        output_var = params.get('output_var', 'chunked_list')

        try:
            resolved = context.resolve_value(items)
            resolved_size = int(context.resolve_value(size))

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [resolved[i:i + resolved_size] for i in range(0, len(resolved), resolved_size)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分割列表块: {len(result)} 块",
                data={
                    'items': resolved,
                    'size': resolved_size,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割列表块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chunked_list'}


class TransformFlattenAction(BaseAction):
    """Flatten nested list."""
    action_type = "transform3_flatten"
    display_name = "扁平化列表"
    description = "将嵌套列表扁平化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with flattened list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'flattened_list')

        try:
            resolved = context.resolve_value(items)

            def flatten(obj):
                result = []
                for item in obj:
                    if isinstance(item, (list, tuple)):
                        result.extend(flatten(item))
                    else:
                        result.append(item)
                return result

            result = flatten(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扁平化列表: {len(result)} 个元素",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flattened_list'}
