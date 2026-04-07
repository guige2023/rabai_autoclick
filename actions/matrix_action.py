"""Matrix action module for RabAI AutoClick.

Provides matrix operations:
- MatrixAddAction: Add matrices
- MatrixSubtractAction: Subtract matrices
- MatrixMultiplyAction: Multiply matrices
- MatrixScaleAction: Scale matrix
- MatrixIdentityAction: Create identity matrix
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MatrixAddAction(BaseAction):
    """Add matrices."""
    action_type = "matrix_add"
    display_name = "矩阵加法"
    description = "矩阵相加"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with matrix1, matrix2, output_var.

        Returns:
            ActionResult with sum matrix.
        """
        matrix1 = params.get('matrix1', '')
        matrix2 = params.get('matrix2', '')
        output_var = params.get('output_var', 'matrix_sum')

        valid, msg = self.validate_type(matrix1, str, 'matrix1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(matrix2, str, 'matrix2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_m1 = context.resolve_value(matrix1)
            resolved_m2 = context.resolve_value(matrix2)

            m1 = context.get(resolved_m1)
            m2 = context.get(resolved_m2)

            if not isinstance(m1, list) or not isinstance(m2, list):
                return ActionResult(
                    success=False,
                    message="两个参数都必须是矩阵"
                )

            if len(m1) != len(m2) or len(m1[0]) != len(m2[0]):
                return ActionResult(
                    success=False,
                    message="矩阵维度必须相同"
                )

            rows = len(m1)
            cols = len(m1[0])
            result = [[m1[r][c] + m2[r][c] for c in range(cols)] for r in range(rows)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵加法完成: {rows}x{cols}",
                data={
                    'rows': rows,
                    'cols': cols,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵加法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix1', 'matrix2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_sum'}


class MatrixSubtractAction(BaseAction):
    """Subtract matrices."""
    action_type = "matrix_subtract"
    display_name = "矩阵减法"
    description = "矩阵相减"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract.

        Args:
            context: Execution context.
            params: Dict with matrix1, matrix2, output_var.

        Returns:
            ActionResult with difference matrix.
        """
        matrix1 = params.get('matrix1', '')
        matrix2 = params.get('matrix2', '')
        output_var = params.get('output_var', 'matrix_diff')

        valid, msg = self.validate_type(matrix1, str, 'matrix1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(matrix2, str, 'matrix2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_m1 = context.resolve_value(matrix1)
            resolved_m2 = context.resolve_value(matrix2)

            m1 = context.get(resolved_m1)
            m2 = context.get(resolved_m2)

            if not isinstance(m1, list) or not isinstance(m2, list):
                return ActionResult(
                    success=False,
                    message="两个参数都必须是矩阵"
                )

            if len(m1) != len(m2) or len(m1[0]) != len(m2[0]):
                return ActionResult(
                    success=False,
                    message="矩阵维度必须相同"
                )

            rows = len(m1)
            cols = len(m1[0])
            result = [[m1[r][c] - m2[r][c] for c in range(cols)] for r in range(rows)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵减法完成: {rows}x{cols}",
                data={
                    'rows': rows,
                    'cols': cols,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵减法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix1', 'matrix2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_diff'}


class MatrixMultiplyAction(BaseAction):
    """Multiply matrices."""
    action_type = "matrix_multiply"
    display_name = "矩阵乘法"
    description = "矩阵相乘"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute multiply.

        Args:
            context: Execution context.
            params: Dict with matrix1, matrix2, output_var.

        Returns:
            ActionResult with product matrix.
        """
        matrix1 = params.get('matrix1', '')
        matrix2 = params.get('matrix2', '')
        output_var = params.get('output_var', 'matrix_product')

        valid, msg = self.validate_type(matrix1, str, 'matrix1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(matrix2, str, 'matrix2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_m1 = context.resolve_value(matrix1)
            resolved_m2 = context.resolve_value(matrix2)

            m1 = context.get(resolved_m1)
            m2 = context.get(resolved_m2)

            if not isinstance(m1, list) or not isinstance(m2, list):
                return ActionResult(
                    success=False,
                    message="两个参数都必须是矩阵"
                )

            if len(m1[0]) != len(m2):
                return ActionResult(
                    success=False,
                    message=f"矩阵维度不匹配: {len(m1[0])} != {len(m2)}"
                )

            rows = len(m1)
            cols = len(m2[0])
            inner = len(m1[0])

            result = [[sum(m1[r][k] * m2[k][c] for k in range(inner)) for c in range(cols)] for r in range(rows)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵乘法完成: {rows}x{cols}",
                data={
                    'rows': rows,
                    'cols': cols,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵乘法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix1', 'matrix2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_product'}


class MatrixScaleAction(BaseAction):
    """Scale matrix."""
    action_type = "matrix_scale"
    display_name = "矩阵缩放"
    description = "矩阵标量乘法"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scale.

        Args:
            context: Execution context.
            params: Dict with matrix, scalar, output_var.

        Returns:
            ActionResult with scaled matrix.
        """
        matrix = params.get('matrix', '')
        scalar = params.get('scalar', 1)
        output_var = params.get('output_var', 'scaled_matrix')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_matrix = context.resolve_value(matrix)
            resolved_scalar = float(context.resolve_value(scalar))

            m = context.get(resolved_matrix)

            if not isinstance(m, list):
                return ActionResult(
                    success=False,
                    message="参数必须是矩阵"
                )

            rows = len(m)
            cols = len(m[0])
            result = [[m[r][c] * resolved_scalar for c in range(cols)] for r in range(rows)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵缩放完成: {resolved_scalar}x",
                data={
                    'scalar': resolved_scalar,
                    'rows': rows,
                    'cols': cols,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵缩放失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix', 'scalar']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'scaled_matrix'}


class MatrixIdentityAction(BaseAction):
    """Create identity matrix."""
    action_type = "matrix_identity"
    display_name = "单位矩阵"
    description = "创建单位矩阵"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute identity.

        Args:
            context: Execution context.
            params: Dict with size, output_var.

        Returns:
            ActionResult with identity matrix.
        """
        size = params.get('size', 3)
        output_var = params.get('output_var', 'identity_matrix')

        try:
            resolved_size = int(context.resolve_value(size))

            result = [[1 if r == c else 0 for c in range(resolved_size)] for r in range(resolved_size)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"单位矩阵创建: {resolved_size}x{resolved_size}",
                data={
                    'size': resolved_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建单位矩阵失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'size': 3, 'output_var': 'identity_matrix'}
