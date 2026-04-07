"""List9 action module for RabAI AutoClick.

Provides additional list operations:
- ListIntersectionAction: Get intersection of lists
- ListDifferenceAction: Get difference of lists
- ListSymmetricDifferenceAction: Get symmetric difference
- ListFlattenAction: Flatten nested lists
- ListChunkAction: Split list into chunks
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListIntersectionAction(BaseAction):
    """Get intersection of lists."""
    action_type = "list9_intersection"
    display_name = "列表交集"
    description = "获取两个列表的交集"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersection.

        Args:
            context: Execution context.
            params: Dict with list1, list2, output_var.

        Returns:
            ActionResult with intersection.
        """
        list1 = params.get('list1', [])
        list2 = params.get('list2', [])
        output_var = params.get('output_var', 'intersection_result')

        try:
            resolved1 = context.resolve_value(list1)
            resolved2 = context.resolve_value(list2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            result = list(set(resolved1) & set(resolved2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"交集数量: {len(result)}",
                data={
                    'list1': resolved1,
                    'list2': resolved2,
                    'intersection': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list1', 'list2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_result'}


class ListDifferenceAction(BaseAction):
    """Get difference of lists."""
    action_type = "list9_difference"
    display_name = "列表差集"
    description = "获取两个列表的差集"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute difference.

        Args:
            context: Execution context.
            params: Dict with list1, list2, output_var.

        Returns:
            ActionResult with difference.
        """
        list1 = params.get('list1', [])
        list2 = params.get('list2', [])
        output_var = params.get('output_var', 'difference_result')

        try:
            resolved1 = context.resolve_value(list1)
            resolved2 = context.resolve_value(list2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            result = list(set(resolved1) - set(resolved2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"差集数量: {len(result)}",
                data={
                    'list1': resolved1,
                    'list2': resolved2,
                    'difference': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list1', 'list2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_result'}


class ListSymmetricDifferenceAction(BaseAction):
    """Get symmetric difference."""
    action_type = "list9_symmetric_difference"
    display_name = "列表对称差集"
    description = "获取两个列表的对称差集"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute symmetric difference.

        Args:
            context: Execution context.
            params: Dict with list1, list2, output_var.

        Returns:
            ActionResult with symmetric difference.
        """
        list1 = params.get('list1', [])
        list2 = params.get('list2', [])
        output_var = params.get('output_var', 'symmetric_difference_result')

        try:
            resolved1 = context.resolve_value(list1)
            resolved2 = context.resolve_value(list2)

            if not isinstance(resolved1, (list, tuple)):
                resolved1 = [resolved1]
            if not isinstance(resolved2, (list, tuple)):
                resolved2 = [resolved2]

            result = list(set(resolved1) ^ set(resolved2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"对称差集数量: {len(result)}",
                data={
                    'list1': resolved1,
                    'list2': resolved2,
                    'symmetric_difference': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表对称差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list1', 'list2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'symmetric_difference_result'}


class ListFlattenAction(BaseAction):
    """Flatten nested lists."""
    action_type = "list9_flatten"
    display_name = "列表扁平化"
    description = "将嵌套列表扁平化"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with list, depth, output_var.

        Returns:
            ActionResult with flattened list.
        """
        input_list = params.get('list', [])
        depth = params.get('depth', -1)
        output_var = params.get('output_var', 'flattened_list')

        try:
            resolved = context.resolve_value(input_list)

            def flatten(lst, d):
                if d == 0:
                    return lst
                result = []
                for item in lst:
                    if isinstance(item, (list, tuple)):
                        result.extend(flatten(item, d - 1))
                    else:
                        result.append(item)
                return result

            result = flatten(resolved, depth if depth > 0 else -1)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扁平化后数量: {len(result)}",
                data={
                    'original': resolved,
                    'flattened': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表扁平化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'depth': -1, 'output_var': 'flattened_list'}


class ListChunkAction(BaseAction):
    """Split list into chunks."""
    action_type = "list9_chunk"
    display_name = "列表分块"
    description = "将列表分割成块"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute chunk.

        Args:
            context: Execution context.
            params: Dict with list, size, output_var.

        Returns:
            ActionResult with chunks.
        """
        input_list = params.get('list', [])
        size = params.get('size', 1)
        output_var = params.get('output_var', 'chunks')

        try:
            resolved = context.resolve_value(input_list)
            resolved_size = int(context.resolve_value(size)) if size else 1

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            chunks = [resolved[i:i + resolved_size] for i in range(0, len(resolved), resolved_size)]
            context.set(output_var, chunks)

            return ActionResult(
                success=True,
                message=f"分块数量: {len(chunks)}",
                data={
                    'original': resolved,
                    'chunks': chunks,
                    'chunk_size': resolved_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表分块失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chunks'}