"""Array action module for RabAI AutoClick.

Provides array/matrix operations:
- ArrayCreateAction: Create array
- ArrayGetAction: Get array element
- ArraySetAction: Set array element
- ArrayTransposeAction: Transpose array
- ArrayFlattenAction: Flatten array
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArrayCreateAction(BaseAction):
    """Create array."""
    action_type = "array_create"
    display_name = "创建数组"
    description = "创建多维数组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with rows, cols, fill, output_var.

        Returns:
            ActionResult indicating created.
        """
        rows = params.get('rows', 2)
        cols = params.get('cols', 2)
        fill = params.get('fill', 0)
        output_var = params.get('output_var', 'array_result')

        try:
            resolved_rows = int(context.resolve_value(rows))
            resolved_cols = int(context.resolve_value(cols))
            resolved_fill = context.resolve_value(fill)

            result = [[resolved_fill for _ in range(resolved_cols)] for _ in range(resolved_rows)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组创建: {resolved_rows}x{resolved_cols}",
                data={
                    'rows': resolved_rows,
                    'cols': resolved_cols,
                    'count': resolved_rows * resolved_cols,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'rows': 2, 'cols': 2, 'fill': 0, 'output_var': 'array_result'}


class ArrayGetAction(BaseAction):
    """Get array element."""
    action_type = "array_get"
    display_name = "获取数组元素"
    description = "获取数组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with array_var, row, col, output_var.

        Returns:
            ActionResult with element.
        """
        array_var = params.get('array_var', '')
        row = params.get('row', 0)
        col = params.get('col', 0)
        output_var = params.get('output_var', 'array_element')

        valid, msg = self.validate_type(array_var, str, 'array_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(array_var)
            resolved_row = int(context.resolve_value(row))
            resolved_col = int(context.resolve_value(col))

            arr = context.get(resolved_var)
            if not isinstance(arr, list) or len(arr) == 0 or not isinstance(arr[0], list):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是二维数组"
                )

            if resolved_row < 0 or resolved_row >= len(arr):
                return ActionResult(
                    success=False,
                    message=f"行索引 {resolved_row} 超出范围"
                )

            if resolved_col < 0 or resolved_col >= len(arr[resolved_row]):
                return ActionResult(
                    success=False,
                    message=f"列索引 {resolved_col} 超出范围"
                )

            result = arr[resolved_row][resolved_col]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取元素: {result}",
                data={
                    'row': resolved_row,
                    'col': resolved_col,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取数组元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array_var', 'row', 'col']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'array_element'}


class ArraySetAction(BaseAction):
    """Set array element."""
    action_type = "array_set"
    display_name = "设置数组元素"
    description = "设置数组元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with array_var, row, col, value.

        Returns:
            ActionResult indicating set.
        """
        array_var = params.get('array_var', '')
        row = params.get('row', 0)
        col = params.get('col', 0)
        value = params.get('value', 0)

        valid, msg = self.validate_type(array_var, str, 'array_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(array_var)
            resolved_row = int(context.resolve_value(row))
            resolved_col = int(context.resolve_value(col))
            resolved_value = context.resolve_value(value)

            arr = context.get(resolved_var)
            if not isinstance(arr, list) or len(arr) == 0 or not isinstance(arr[0], list):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是二维数组"
                )

            if resolved_row < 0 or resolved_row >= len(arr):
                return ActionResult(
                    success=False,
                    message=f"行索引 {resolved_row} 超出范围"
                )

            if resolved_col < 0 or resolved_col >= len(arr[resolved_row]):
                return ActionResult(
                    success=False,
                    message=f"列索引 {resolved_col} 超出范围"
                )

            arr[resolved_row][resolved_col] = resolved_value
            context.set(resolved_var, arr)

            return ActionResult(
                success=True,
                message=f"设置元素: {resolved_value}",
                data={
                    'row': resolved_row,
                    'col': resolved_col,
                    'value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置数组元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array_var', 'row', 'col', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ArrayTransposeAction(BaseAction):
    """Transpose array."""
    action_type = "array_transpose"
    display_name = "转置数组"
    description = "转置二维数组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute transpose.

        Args:
            context: Execution context.
            params: Dict with array_var, output_var.

        Returns:
            ActionResult with transposed array.
        """
        array_var = params.get('array_var', '')
        output_var = params.get('output_var', 'transposed_array')

        valid, msg = self.validate_type(array_var, str, 'array_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(array_var)

            arr = context.get(resolved_var)
            if not isinstance(arr, list) or len(arr) == 0 or not isinstance(arr[0], list):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是二维数组"
                )

            rows = len(arr)
            cols = len(arr[0])

            result = [[arr[r][c] for r in range(rows)] for c in range(cols)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组转置: {cols}x{rows}",
                data={
                    'original': f"{rows}x{cols}",
                    'transposed': f"{cols}x{rows}",
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转置数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transposed_array'}


class ArrayFlattenAction(BaseAction):
    """Flatten array."""
    action_type = "array_flatten"
    display_name = "扁平化数组"
    description = "将二维数组展平为一维"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with array_var, output_var.

        Returns:
            ActionResult with flattened array.
        """
        array_var = params.get('array_var', '')
        output_var = params.get('output_var', 'flattened_array')

        valid, msg = self.validate_type(array_var, str, 'array_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(array_var)

            arr = context.get(resolved_var)
            if not isinstance(arr, list) or len(arr) == 0:
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是有效数组"
                )

            result = []
            for row in arr:
                if isinstance(row, list):
                    result.extend(row)
                else:
                    result.append(row)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数组扁平化: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化数组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['array_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flattened_array'}
