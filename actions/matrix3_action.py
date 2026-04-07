"""Matrix operations extended module for RabAI AutoClick.

Provides advanced matrix operations:
- MatrixTransposeAction: Transpose a matrix
- MatrixDeterminantAction: Compute determinant
- MatrixInverseAction: Compute matrix inverse
- MatrixTraceAction: Compute trace (sum of diagonal)
- MatrixTranspose: Transpose matrix (alias)
- MatrixCofactorAction: Compute cofactor matrix
- MatrixAdjointAction: Compute adjugate/adjoint
- MatrixRankAction: Compute matrix rank
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MatrixTransposeAction(BaseAction):
    """Transpose a matrix."""
    action_type = "matrix_transpose"
    display_name = "矩阵转置"
    description = "转置矩阵"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transpose matrix.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with transposed matrix.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'transposed_matrix')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            rows = len(m)
            cols = len(m[0])

            # Transpose: result[j][i] = m[i][j]
            result = [[m[i][j] for i in range(rows)] for j in range(cols)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵转置完成: {cols}x{rows}",
                data={
                    'original_shape': [rows, cols],
                    'new_shape': [cols, rows],
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


class MatrixTraceAction(BaseAction):
    """Compute trace of matrix (sum of diagonal)."""
    action_type = "matrix_trace"
    display_name = "矩阵迹"
    description = "计算矩阵迹（对角线元素和）"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute trace.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with trace value.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'matrix_trace')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            rows = len(m)
            if rows != len(m[0]):
                return ActionResult(success=False, message="必须是方阵")

            result = sum(m[i][i] for i in range(rows))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵迹: {result}",
                data={
                    'trace': result,
                    'size': rows,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算矩阵迹失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_trace'}


class MatrixDeterminantAction(BaseAction):
    """Compute determinant of square matrix."""
    action_type = "matrix_determinant"
    display_name = "矩阵行列式"
    description = "计算矩阵行列式"
    version = "1.0"

    def _det_recursive(self, mat: List[List[float]]) -> float:
        """Recursive determinant calculation."""
        n = len(mat)
        if n == 1:
            return mat[0][0]
        if n == 2:
            return mat[0][0] * mat[1][1] - mat[0][1] * mat[1][0]

        det = 0.0
        for j in range(n):
            # Minor matrix (exclude row 0, column j)
            minor = [[mat[i][k] for k in range(n) if k != j] for i in range(1, n)]
            cofactor = ((-1) ** j) * self._det_recursive(minor)
            det += mat[0][j] * cofactor

        return det

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute determinant.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with determinant value.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'matrix_det')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            rows = len(m)
            if rows != len(m[0]):
                return ActionResult(success=False, message="必须是方阵")

            # Convert to float matrix
            float_m = [[float(m[i][j]) for j in range(rows)] for i in range(rows)]
            result = self._det_recursive(float_m)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"行列式: {result}",
                data={
                    'determinant': result,
                    'size': rows,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算行列式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_det'}


class MatrixCofactorAction(BaseAction):
    """Compute cofactor matrix."""
    action_type = "matrix_cofactor"
    display_name = "矩阵余子式"
    description = "计算矩阵的余子式"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute cofactor matrix.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with cofactor matrix.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'cofactor_matrix')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            n = len(m)
            if n != len(m[0]):
                return ActionResult(success=False, message="必须是方阵")

            # Compute cofactor for each element
            result = [[0.0] * n for _ in range(n)]

            for i in range(n):
                for j in range(n):
                    # Minor matrix (exclude row i, column j)
                    minor = [[m[r][c] for c in range(n) if c != j] for r in range(n) if r != i]
                    # Compute 2x2 determinant for minor
                    if n == 2:
                        minor_det = minor[0][0]
                    else:
                        minor_det = minor[0][0] * minor[1][1] - minor[0][1] * minor[1][0]
                    # Cofactor = (-1)^(i+j) * minor_det
                    result[i][j] = ((-1) ** (i + j)) * minor_det

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"余子式矩阵计算完成: {n}x{n}",
                data={
                    'cofactor_matrix': result,
                    'size': n,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算余子式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cofactor_matrix'}


class MatrixAdjointAction(BaseAction):
    """Compute adjugate (adjoint) matrix."""
    action_type = "matrix_adjoint"
    display_name = "矩阵伴随"
    description = "计算矩阵的伴随矩阵"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute adjoint matrix.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with adjoint matrix.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'adjoint_matrix')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            n = len(m)
            if n != len(m[0]):
                return ActionResult(success=False, message="必须是方阵")

            if n == 1:
                context.set(output_var, [[1.0]])
                return ActionResult(
                    success=True,
                    message="1x1矩阵的伴随矩阵为[[1]]",
                    data={'adjoint_matrix': [[1.0]], 'output_var': output_var}
                )

            # Compute cofactor matrix first
            cofactor = [[0.0] * n for _ in range(n)]

            for i in range(n):
                for j in range(n):
                    minor = [[m[r][c] for c in range(n) if c != j] for r in range(n) if r != i]
                    if n == 2:
                        minor_det = minor[0][0]
                    else:
                        minor_det = minor[0][0] * minor[1][1] - minor[0][1] * minor[1][0]
                    cofactor[i][j] = ((-1) ** (i + j)) * minor_det

            # Adjugate = transpose of cofactor
            result = [[cofactor[j][i] for j in range(n)] for i in range(n)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"伴随矩阵计算完成: {n}x{n}",
                data={
                    'adjoint_matrix': result,
                    'size': n,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算伴随矩阵失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'adjoint_matrix'}


class MatrixInverseAction(BaseAction):
    """Compute inverse of square matrix."""
    action_type = "matrix_inverse"
    display_name = "矩阵逆"
    description = "计算矩阵的逆"
    version = "1.0"

    def _det_recursive(self, mat: List[List[float]]) -> float:
        """Recursive determinant calculation."""
        n = len(mat)
        if n == 1:
            return mat[0][0]
        if n == 2:
            return mat[0][0] * mat[1][1] - mat[0][1] * mat[1][0]

        det = 0.0
        for j in range(n):
            minor = [[mat[i][k] for k in range(n) if k != j] for i in range(1, n)]
            cofactor = ((-1) ** j) * self._det_recursive(minor)
            det += mat[0][j] * cofactor

        return det

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute inverse.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with inverse matrix.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'inverse_matrix')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            n = len(m)
            if n != len(m[0]):
                return ActionResult(success=False, message="必须是方阵")

            # Convert to float
            float_m = [[float(m[i][j]) for j in range(n)] for i in range(n)]

            # Check determinant
            det = self._det_recursive(float_m)
            if abs(det) < 1e-12:
                return ActionResult(success=False, message="矩阵是奇异的，行列式为0，无法求逆")

            # For 2x2 matrix, use direct formula
            if n == 2:
                result = [
                    [float_m[1][1] / det, -float_m[0][1] / det],
                    [-float_m[1][0] / det, float_m[0][0] / det]
                ]
            else:
                # Use Gauss-Jordan elimination
                # Create augmented matrix [m | I]
                aug = [[float_m[i][j] if j < n else (1.0 if j == n + i else 0.0)
                        for j in range(2 * n)] for i in range(n)]

                # Forward elimination
                for i in range(n):
                    # Find pivot
                    pivot = aug[i][i]
                    if abs(pivot) < 1e-12:
                        return ActionResult(success=False, message=f"第{i}行 pivot接近0，无法求逆")

                    # Scale pivot row
                    for j in range(2 * n):
                        aug[i][j] /= pivot

                    # Eliminate column
                    for k in range(n):
                        if k != i:
                            factor = aug[k][i]
                            for j in range(2 * n):
                                aug[k][j] -= factor * aug[i][j]

                # Extract inverse from augmented part
                result = [[aug[i][j + n] for j in range(n)] for i in range(n)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"矩阵逆计算完成: {n}x{n}",
                data={
                    'inverse_matrix': result,
                    'determinant': det,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算矩阵逆失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inverse_matrix'}


class MatrixRankAction(BaseAction):
    """Compute rank of matrix."""
    action_type = "matrix_rank"
    display_name = "矩阵秩"
    description = "计算矩阵的秩"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute rank.

        Args:
            context: Execution context.
            params: Dict with matrix, output_var.

        Returns:
            ActionResult with rank value.
        """
        matrix = params.get('matrix', '')
        output_var = params.get('output_var', 'matrix_rank')

        valid, msg = self.validate_type(matrix, str, 'matrix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(matrix)
            m = context.get(resolved_name)

            if not isinstance(m, list) or not m:
                return ActionResult(success=False, message="矩阵不能为空")

            rows = len(m)
            cols = len(m[0])

            # Convert to float matrix for Gaussian elimination
            mat = [[float(m[i][j]) for j in range(cols)] for i in range(rows)]

            rank = 0
            row_used = [False] * rows

            for col in range(cols):
                # Find pivot row
                pivot_row = -1
                for row in range(rows):
                    if not row_used[row] and abs(mat[row][col]) > 1e-12:
                        pivot_row = row
                        break

                if pivot_row == -1:
                    continue

                # Use this row
                row_used[pivot_row] = True
                rank += 1

                # Eliminate below
                for r in range(rows):
                    if r != pivot_row and abs(mat[r][col]) > 1e-12:
                        factor = mat[r][col] / mat[pivot_row][col]
                        for c in range(cols):
                            mat[r][c] -= factor * mat[pivot_row][c]

            context.set(output_var, rank)

            return ActionResult(
                success=True,
                message=f"矩阵秩: {rank}",
                data={
                    'rank': rank,
                    'original_shape': [rows, cols],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算矩阵秩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['matrix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'matrix_rank'}
