"""Collection11 action module for RabAI AutoClick.

Provides additional collection operations:
- CollectionZipAction: Zip collections
- CollectionUnzipAction: Unzip collection
- CollectionEnumerateAction: Enumerate collection
- CollectionReversedAction: Reverse collection
- CollectionSumAction: Sum collection
- CollectionAnyAction: Any of collection
- CollectionAllAction: All of collection
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CollectionZipAction(BaseAction):
    """Zip collections."""
    action_type = "collection11_zip"
    display_name = "合并集合"
    description = "合并多个集合"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with collections, output_var.

        Returns:
            ActionResult with zipped result.
        """
        collections = params.get('collections', [])
        output_var = params.get('output_var', 'zipped_result')

        try:
            resolved = context.resolve_value(collections)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            # Convert all to lists
            lists = [list(x) if isinstance(x, (list, tuple)) else [x] for x in resolved]

            # Find minimum length
            min_len = min(len(lst) for lst in lists) if lists else 0

            # Pad lists to same length
            padded = []
            for lst in lists:
                if len(lst) < min_len:
                    padded.append(lst + [None] * (min_len - len(lst)))
                else:
                    padded.append(lst[:min_len])

            result = [tuple(p[i] for p in padded) for i in range(min_len)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"合并集合: {len(result)}项",
                data={
                    'collections': resolved,
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collections']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zipped_result'}


class CollectionUnzipAction(BaseAction):
    """Unzip collection."""
    action_type = "collection11_unzip"
    display_name = "拆分集合"
    description = "拆分集合"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip.

        Args:
            context: Execution context.
            params: Dict with collection, output_var.

        Returns:
            ActionResult with unzipped result.
        """
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'unzipped_result')

        try:
            resolved = context.resolve_value(collection)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            if not resolved:
                return ActionResult(
                    success=False,
                    message=f"集合为空"
                )

            # Check if it's a list of tuples
            if not all(isinstance(x, (list, tuple)) for x in resolved):
                return ActionResult(
                    success=False,
                    message=f"集合格式不正确"
                )

            result = [list(x) for x in zip(*resolved)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"拆分集合: {len(result)}个子集合",
                data={
                    'collection': resolved,
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拆分集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unzipped_result'}


class CollectionEnumerateAction(BaseAction):
    """Enumerate collection."""
    action_type = "collection11_enumerate"
    display_name = "枚举集合"
    description = "枚举集合元素"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enumerate.

        Args:
            context: Execution context.
            params: Dict with collection, start, output_var.

        Returns:
            ActionResult with enumerated result.
        """
        collection = params.get('collection', [])
        start = params.get('start', 0)
        output_var = params.get('output_var', 'enumerated_result')

        try:
            resolved = context.resolve_value(collection)
            resolved_start = int(context.resolve_value(start)) if start else 0

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = [(i, x) for i, x in enumerate(resolved, start=resolved_start)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"枚举集合: {len(result)}项",
                data={
                    'collection': resolved,
                    'start': resolved_start,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"枚举集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'output_var': 'enumerated_result'}


class CollectionReversedAction(BaseAction):
    """Reverse collection."""
    action_type = "collection11_reversed"
    display_name = "反转集合"
    description = "反转集合顺序"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reversed.

        Args:
            context: Execution context.
            params: Dict with collection, output_var.

        Returns:
            ActionResult with reversed result.
        """
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'reversed_result')

        try:
            resolved = context.resolve_value(collection)

            if not isinstance(resolved, (list, tuple, str)):
                resolved = [resolved]

            result = list(resolved)[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转集合: {len(result)}项",
                data={
                    'collection': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_result'}


class CollectionSumAction(BaseAction):
    """Sum collection."""
    action_type = "collection11_sum"
    display_name = "求和集合"
    description = "求和集合元素"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with collection, start, output_var.

        Returns:
            ActionResult with sum result.
        """
        collection = params.get('collection', [])
        start = params.get('start', 0)
        output_var = params.get('output_var', 'sum_result')

        try:
            resolved = context.resolve_value(collection)
            resolved_start = float(context.resolve_value(start)) if start else 0

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = sum(resolved, start=resolved_start)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"求和集合: {result}",
                data={
                    'collection': resolved,
                    'start': resolved_start,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"求和集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'output_var': 'sum_result'}


class CollectionAnyAction(BaseAction):
    """Any of collection."""
    action_type = "collection11_any"
    display_name = "任意为真"
    description = "检查集合任意元素为真"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute any.

        Args:
            context: Execution context.
            params: Dict with collection, output_var.

        Returns:
            ActionResult with any result.
        """
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'any_result')

        try:
            resolved = context.resolve_value(collection)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = any(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"任意为真: {'是' if result else '否'}",
                data={
                    'collection': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查任意为真失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'any_result'}


class CollectionAllAction(BaseAction):
    """All of collection."""
    action_type = "collection11_all"
    display_name = "全部为真"
    description = "检查集合全部元素为真"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute all.

        Args:
            context: Execution context.
            params: Dict with collection, output_var.

        Returns:
            ActionResult with all result.
        """
        collection = params.get('collection', [])
        output_var = params.get('output_var', 'all_result')

        try:
            resolved = context.resolve_value(collection)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = all(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"全部为真: {'是' if result else '否'}",
                data={
                    'collection': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查全部为真失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['collection']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'all_result'}