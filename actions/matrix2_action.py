"""Matrix2 action module for RabAI AutoClick.

Provides additional matrix operations:
- MatrixTransposeAction: Transpose matrix
- MatrixAddAction: Add matrices
- MatrixSubtractAction: Subtract matrices
- MatrixMultiplyAction: Multiply matrices
- MatrixDeterminantAction: Calculate determinant
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MatrixTransposeAction(BaseAction):
    """Transpose matrix."""
    action_type = "matrix2_transpose"
    display_name = "矩阵转置"
    description = "转置矩阵"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute transpose.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with transposed matrix.
        """
        input_matrix = params.get('matrix', [])
        output_var = params.get('output_var', 'transposed_matrix')

        try:
            resolved = context.resolve_value(input_matrix)

            if not isinstance(resolved, (list, tuple)) or len(resolved) == 0:
                return ActionResult(
                    success=False,
                    message="矩阵转置失败: 无效的矩阵"
                )

            rows = len(resolved)
            cols = len(resolved[0]) if isinstance(resolved[0], (list, tuple)) else 1

            if cols == 1:
                result = [[resolved[i][0]] for i in range(rows)]
            else:
                result = [[resolved[j][i] for j in range(rows)] for i in range(cols)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵转置: {len(result)}x{len(result[0]) if result else 0}",
                data={
                    'original': resolved,
                    'transposed': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵转置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transposed_matrix'}


class MatrixAddAction(BaseAction):
    """Add matrices."""
    action_type = "matrix2_add"
    display_name = "矩阵加法"
    description = "矩阵相加"
    version = "2.0"

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
            ActionResult with sum of matrices.
        """
        matrix1 = params.get('matrix1', [])
        matrix2 = params.get('matrix2', [])
        output_var = params.get('output_var', 'sum_matrix')

        try:
            resolved1 = context.resolve_value(matrix1)
            resolved2 = context.resolve_value(matrix2)

            if len(resolved1) != len(resolved2) or len(resolved1[0]) != len(resolved2[0]):
                return ActionResult(
                    success=False,
                    message="矩阵加法失败: 矩阵维度不匹配"
                )

            result = []
            for i in range(len(resolved1)):
                row = []
                for j in range(len(resolved1[0])):
                    row.append(resolved1[i][j] + resolved2[i][j])
                result.append(row)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵加法完成",
                data={
                    'matrix1': resolved1,
                    'matrix2': resolved2,
                    'sum': result,
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
        return {'output_var': 'sum_matrix'}


class MatrixSubtractAction(BaseAction):
    """Subtract matrices."""
    action_type = "matrix2_subtract"
    display_name = "矩阵减法"
    description = "矩阵相减"
    version = "2.0"

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
            ActionResult with difference of matrices.
        """
        matrix1 = params.get('matrix1', [])
        matrix2 = params.get('matrix2', [])
        output_var = params.get('output_var', 'difference_matrix')

        try:
            resolved1 = context.resolve_value(matrix1)
            resolved2 = context.resolve_value(matrix2)

            if len(resolved1) != len(resolved2) or len(resolved1[0]) != len(resolved2[0]):
                return ActionResult(
                    success=False,
                    message="矩阵减法失败: 矩阵维度不匹配"
                )

            result = []
            for i in range(len(resolved1)):
                row = []
                for j in range(len(resolved1[0])):
                    row.append(resolved1[i][j] - resolved2[i][j])
                result.append(row)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵减法完成",
                data={
                    'matrix1': resolved1,
                    'matrix2': resolved2,
                    'difference': result,
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
        return {'output_var': 'difference_matrix'}


class MatrixMultiplyAction(BaseAction):
    """Multiply matrices."""
    action_type = "matrix2_multiply"
    display_name = "矩阵乘法"
    description = "矩阵相乘"
    version = "2.0"

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
            ActionResult with product of matrices.
        """
        matrix1 = params.get('matrix1', [])
        matrix2 = params.get('matrix2', [])
        output_var = params.get('output_var', 'product_matrix')

        try:
            resolved1 = context.resolve_value(matrix1)
            resolved2 = context.resolve_value(matrix2)

            rows1 = len(resolved1)
            cols1 = len(resolved1[0]) if resolved1 else 0
            rows2 = len(resolved2)
            cols2 = len(resolved2[0]) if resolved2 else 0

            if cols1 != rows2:
                return ActionResult(
                    success=False,
                    message="矩阵乘法失败: 矩阵维度不匹配"
                )

            result = []
            for i in range(rows1):
                row = []
                for j in range(cols2):
                    val = 0
                    for k in range(cols1):
                        val += resolved1[i][k] * resolved2[k][j]
                    row.append(val)
                result.append(row)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵乘法完成: {len(result)}x{len(result[0]) if result else 0}",
                data={
                    'matrix1': resolved1,
                    'matrix2': resolved2,
                    'product': result,
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
        return {'output_var': 'product_matrix'}


class MatrixDeterminantAction(BaseAction):
    """Calculate determinant."""
    action_type = "matrix2_determinant"
    display_name = "矩阵行列式"
    description = "计算矩阵行列式"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute determinant.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with determinant.
        """
        input_matrix = params.get('matrix', [])
        output_var = params.get('output_var', 'determinant')

        try:
            resolved = context.resolve_value(input_matrix)

            n = len(resolved)
            if n != len(resolved[0]):
                return ActionResult(
                    success=False,
                    message="矩阵行列式失败: 必须是方阵"
                )

            if n == 1:
                result = resolved[0][0]
            elif n == 2:
                result = resolved[0][0] * resolved[1][1] - resolved[0][1] * resolved[1][0]
            else:
                det = 0
                for j in range(n):
                    minor = [[resolved[i][k] for k in range(n) if k != j] for i in range(1, n)]
                    sign = -1 if j % 2 == 1 else 1
                    det += sign * resolved[0][j] * self._det_helper(minor)
                result = det

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵行列式: {result}",
                data={
                    'matrix': resolved,
                    'determinant': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"矩阵行列式失败: {str(e)}"
            )

    def _det_helper(self, matrix):
        n = len(matrix)
        if n == 1:
            return matrix[0][0]
        if n == 2:
            return matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]
        det = 0
        for j in range(n):
            minor = [[matrix[i][k] for k in range(n) if k != j] for i in range(1, n)]
            sign = -1 if j % 2 == 1 else 1
            det += sign * matrix[0][j] * self._det_helper(minor)
        return det

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'determinant'}