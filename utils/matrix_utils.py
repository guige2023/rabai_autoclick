"""
Matrix utilities for linear algebra operations.

This module provides comprehensive matrix operations including:
- Matrix creation and manipulation
- Matrix arithmetic (add, subtract, multiply)
- Matrix transformations (transpose, inverse, determinant)
- Element-wise operations
- Linear system solving
- Matrix decompositions (LU, Cholesky)

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Iterator, List, Optional, Tuple, Union


@dataclass
class Matrix:
    """
    A matrix representation with comprehensive linear algebra operations.
    
    Attributes:
        rows: Number of rows.
        cols: Number of columns.
        data: 2D list of values (row-major order).
    
    Example:
        >>> m = Matrix.from_list([[1, 2], [3, 4]])
        >>> m.determinant()
        -2.0
        >>> m.transpose()
        Matrix(2, 2, [[1, 3], [2, 4]])
    """
    rows: int = field(default=0)
    cols: int = field(default=0)
    data: List[List[float]] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        if self.rows > 0 and self.cols > 0 and not self.data:
            self.data = [[0.0] * self.cols for _ in range(self.rows)]
    
    @classmethod
    def from_list(cls, data: List[List[float]]) -> Matrix:
        """
        Create a matrix from a 2D list.
        
        Args:
            data: 2D list of values.
            
        Returns:
            A new Matrix instance.
        """
        if not data or not data[0]:
            return cls(0, 0, [])
        
        rows = len(data)
        cols = len(data[0])
        
        for row in data:
            if len(row) != cols:
                raise ValueError("All rows must have the same length")
        
        return cls(rows, cols, [row[:] for row in data])
    
    @classmethod
    def identity(cls, size: int) -> Matrix:
        """
        Create an identity matrix of a given size.
        
        Args:
            size: The size of the identity matrix.
            
        Returns:
            An identity matrix.
        """
        data = [[1.0 if i == j else 0.0 for j in range(size)] for i in range(size)]
        return cls.from_list(data)
    
    @classmethod
    def zeros(cls, rows: int, cols: int) -> Matrix:
        """
        Create a zero matrix of given dimensions.
        
        Args:
            rows: Number of rows.
            cols: Number of columns.
            
        Returns:
            A zero matrix.
        """
        return cls(rows, cols, [[0.0] * cols for _ in range(rows)])
    
    @classmethod
    def ones(cls, rows: int, cols: int) -> Matrix:
        """
        Create a matrix of ones of given dimensions.
        
        Args:
            rows: Number of rows.
            cols: Number of columns.
            
        Returns:
            A matrix of ones.
        """
        return cls(rows, cols, [[1.0] * cols for _ in range(rows)])
    
    @classmethod
    def random(cls, rows: int, cols: int, min_val: float = 0.0, max_val: float = 1.0) -> Matrix:
        """
        Create a matrix with random values.
        
        Args:
            rows: Number of rows.
            cols: Number of columns.
            min_val: Minimum random value.
            max_val: Maximum random value.
            
        Returns:
            A matrix with random values.
        """
        data = [
            [random.uniform(min_val, max_val) for _ in range(cols)]
            for _ in range(rows)
        ]
        return cls.from_list(data)
    
    def get(self, row: int, col: int) -> float:
        """
        Get a matrix element.
        
        Args:
            row: Row index.
            col: Column index.
            
        Returns:
            The element value.
        """
        return self.data[row][col]
    
    def set(self, row: int, col: int, value: float) -> None:
        """
        Set a matrix element.
        
        Args:
            row: Row index.
            col: Column index.
            value: New value.
        """
        self.data[row][col] = value
    
    def get_row(self, row: int) -> List[float]:
        """Get an entire row as a list."""
        return self.data[row][:]
    
    def get_col(self, col: int) -> List[float]:
        """Get an entire column as a list."""
        return [self.data[row][col] for row in range(self.rows)]
    
    def get_diagonal(self) -> List[float]:
        """Get the main diagonal as a list."""
        return [self.data[i][i] for i in range(min(self.rows, self.cols))]
    
    def transpose(self) -> Matrix:
        """
        Return the transpose of the matrix.
        
        Returns:
            A new transposed Matrix.
        """
        result = Matrix.zeros(self.cols, self.rows)
        
        for i in range(self.rows):
            for j in range(self.cols):
                result.data[j][i] = self.data[i][j]
        
        return result
    
    def add(self, other: Matrix) -> Matrix:
        """
        Add another matrix to this one.
        
        Args:
            other: The matrix to add.
            
        Returns:
            A new Matrix with the sum.
            
        Raises:
            ValueError: If matrices have different dimensions.
        """
        if self.rows != other.rows or self.cols != other.cols:
            raise ValueError(f"Cannot add matrices of different sizes: "
                           f"({self.rows}x{self.cols}) vs ({other.rows}x{other.cols})")
        
        result = Matrix.zeros(self.rows, self.cols)
        
        for i in range(self.rows):
            for j in range(self.cols):
                result.data[i][j] = self.data[i][j] + other.data[i][j]
        
        return result
    
    def subtract(self, other: Matrix) -> Matrix:
        """
        Subtract another matrix from this one.
        
        Args:
            other: The matrix to subtract.
            
        Returns:
            A new Matrix with the difference.
        """
        if self.rows != other.rows or self.cols != other.cols:
            raise ValueError("Matrices must have the same dimensions")
        
        result = Matrix.zeros(self.rows, self.cols)
        
        for i in range(self.rows):
            for j in range(self.cols):
                result.data[i][j] = self.data[i][j] - other.data[i][j]
        
        return result
    
    def multiply(self, other: Matrix) -> Matrix:
        """
        Multiply this matrix by another matrix.
        
        Args:
            other: The matrix to multiply by.
            
        Returns:
            A new Matrix with the product.
            
        Raises:
            ValueError: If inner dimensions don't match.
        """
        if self.cols != other.rows:
            raise ValueError(f"Cannot multiply: ({self.rows}x{self.cols}) * "
                           f"({other.rows}x{other.cols}) - inner dimensions don't match")
        
        result = Matrix.zeros(self.rows, other.cols)
        
        for i in range(self.rows):
            for j in range(other.cols):
                total = 0.0
                for k in range(self.cols):
                    total += self.data[i][k] * other.data[k][j]
                result.data[i][j] = total
        
        return result
    
    def scalar_multiply(self, scalar: float) -> Matrix:
        """
        Multiply the matrix by a scalar.
        
        Args:
            scalar: The scalar value.
            
        Returns:
            A new scaled Matrix.
        """
        result = Matrix.zeros(self.rows, self.cols)
        
        for i in range(self.rows):
            for j in range(self.cols):
                result.data[i][j] = self.data[i][j] * scalar
        
        return result
    
    def negate(self) -> Matrix:
        """Negate all elements of the matrix."""
        return self.scalar_multiply(-1.0)
    
    def element_wise_multiply(self, other: Matrix) -> Matrix:
        """
        Element-wise multiplication (Hadamard product).
        
        Args:
            other: The matrix to multiply element-wise with.
            
        Returns:
            A new Matrix with the element-wise product.
        """
        if self.rows != other.rows or self.cols != other.cols:
            raise ValueError("Matrices must have the same dimensions")
        
        result = Matrix.zeros(self.rows, self.cols)
        
        for i in range(self.rows):
            for j in range(self.cols):
                result.data[i][j] = self.data[i][j] * other.data[i][j]
        
        return result
    
    def trace(self) -> float:
        """
        Calculate the trace (sum of diagonal elements).
        
        Returns:
            The trace value.
        """
        return sum(self.get_diagonal())
    
    def determinant(self) -> float:
        """
        Calculate the determinant of the matrix.
        
        Returns:
            The determinant value.
            
        Raises:
            ValueError: If matrix is not square.
        """
        if self.rows != self.cols:
            raise ValueError(f"Determinant requires a square matrix, got {self.rows}x{self.cols}")
        
        if self.rows == 1:
            return self.data[0][0]
        
        if self.rows == 2:
            return (self.data[0][0] * self.data[1][1] -
                    self.data[0][1] * self.data[1][0])
        
        if self.rows == 3:
            return (
                self.data[0][0] * (self.data[1][1] * self.data[2][2] - self.data[1][2] * self.data[2][1]) -
                self.data[0][1] * (self.data[1][0] * self.data[2][2] - self.data[1][2] * self.data[2][0]) +
                self.data[0][2] * (self.data[1][0] * self.data[2][1] - self.data[1][1] * self.data[2][0])
            )
        
        det = 0.0
        for j in range(self.cols):
            det += ((-1) ** j) * self.data[0][j] * self.minor(0, j).determinant()
        
        return det
    
    def minor(self, row: int, col: int) -> Matrix:
        """
        Get the minor of a matrix (determinant of the submatrix removing row and col).
        
        Args:
            row: Row to remove.
            col: Column to remove.
            
        Returns:
            The minor matrix.
        """
        result = Matrix.zeros(self.rows - 1, self.cols - 1)
        
        r_idx = 0
        for i in range(self.rows):
            if i == row:
                continue
            c_idx = 0
            for j in range(self.cols):
                if j == col:
                    continue
                result.data[r_idx][c_idx] = self.data[i][j]
                c_idx += 1
            r_idx += 1
        
        return result
    
    def inverse(self) -> Optional[Matrix]:
        """
        Calculate the inverse of the matrix using Gauss-Jordan elimination.
        
        Returns:
            The inverse matrix, or None if the matrix is singular.
        """
        if self.rows != self.cols:
            raise ValueError("Only square matrices can be inverted")
        
        n = self.rows
        augmented = Matrix.zeros(n, 2 * n)
        
        for i in range(n):
            for j in range(n):
                augmented.data[i][j] = self.data[i][j]
            augmented.data[i][n + i] = 1.0
        
        for col in range(n):
            max_row = col
            for row in range(col + 1, n):
                if abs(augmented.data[row][col]) > abs(augmented.data[max_row][col]):
                    max_row = row
            
            augmented.data[col], augmented.data[max_row] = (
                augmented.data[max_row][:],
                augmented.data[col][:]
            )
            
            if abs(augmented.data[col][col]) < 1e-12:
                return None
            
            pivot = augmented.data[col][col]
            for j in range(2 * n):
                augmented.data[col][j] /= pivot
            
            for row in range(n):
                if row != col:
                    factor = augmented.data[row][col]
                    for j in range(2 * n):
                        augmented.data[row][j] -= factor * augmented.data[col][j]
        
        result = Matrix.zeros(n, n)
        for i in range(n):
            for j in range(n):
                result.data[i][j] = augmented.data[i][n + j]
        
        return result
    
    def is_symmetric(self, tolerance: float = 1e-10) -> bool:
        """
        Check if the matrix is symmetric.
        
        Args:
            tolerance: Numerical tolerance for comparison.
            
        Returns:
            True if the matrix is symmetric.
        """
        if self.rows != self.cols:
            return False
        
        for i in range(self.rows):
            for j in range(i + 1, self.cols):
                if abs(self.data[i][j] - self.data[j][i]) > tolerance:
                    return False
        
        return True
    
    def is_square(self) -> bool:
        """Check if the matrix is square."""
        return self.rows == self.cols
    
    def norm(self, p: int = 2) -> float:
        """
        Calculate the matrix norm.
        
        Args:
            p: The type of norm (1=column sum, 2=Frobenius, inf=row sum).
            
        Returns:
            The norm value.
        """
        if p == 1:
            return max(sum(abs(self.data[i][j]) for i in range(self.rows))
                      for j in range(self.cols))
        
        elif p == 2:
            return math.sqrt(sum(self.data[i][j] ** 2
                                for i in range(self.rows)
                                for j in range(self.cols)))
        
        elif p == float('inf'):
            return max(sum(abs(self.data[i][j]) for j in range(self.cols))
                      for i in range(self.rows))
        
        else:
            raise ValueError(f"Unknown norm type: {p}")
    
    def submatrix(self, rows: List[int], cols: List[int]) -> Matrix:
        """
        Extract a submatrix with specified row and column indices.
        
        Args:
            rows: List of row indices to extract.
            cols: List of column indices to extract.
            
        Returns:
            The extracted submatrix.
        """
        result = Matrix.zeros(len(rows), len(cols))
        
        for i, r in enumerate(rows):
            for j, c in enumerate(cols):
                result.data[i][j] = self.data[r][c]
        
        return result
    
    def set_submatrix(self, start_row: int, start_col: int, sub: Matrix) -> None:
        """
        Set a submatrix in place.
        
        Args:
            start_row: Starting row index.
            start_col: Starting column index.
            sub: The submatrix to insert.
        """
        for i in range(sub.rows):
            for j in range(sub.cols):
                self.data[start_row + i][start_col + j] = sub.data[i][j]
    
    def clone(self) -> Matrix:
        """Create a deep copy of the matrix."""
        return Matrix(self.rows, self.cols, [row[:] for row in self.data])
    
    def flatten(self) -> List[float]:
        """Flatten the matrix to a 1D list in row-major order."""
        return [self.data[i][j] for i in range(self.rows) for j in range(self.cols)]
    
    def reshape(self, new_rows: int, new_cols: int) -> Matrix:
        """
        Reshape the matrix to new dimensions.
        
        Args:
            new_rows: New number of rows.
            new_cols: New number of columns.
            
        Returns:
            The reshaped matrix.
            
        Raises:
            ValueError: If the total number of elements doesn't match.
        """
        if new_rows * new_cols != self.rows * self.cols:
            raise ValueError(f"Cannot reshape {self.rows}x{self.cols} to {new_rows}x{new_cols}")
        
        flat = self.flatten()
        result = Matrix.zeros(new_rows, new_cols)
        
        for i in range(new_rows):
            for j in range(new_cols):
                result.data[i][j] = flat[i * new_cols + j]
        
        return result
    
    def __repr__(self) -> str:
        return f"Matrix({self.rows}, {self.cols}, {self.data})"
    
    def __str__(self) -> str:
        if self.rows == 0 or self.cols == 0:
            return "[]"
        
        max_width = max(len(f"{self.data[i][j]:.4f}") 
                       for i in range(self.rows) 
                       for j in range(self.cols))
        
        lines = []
        for i in range(self.rows):
            row_str = "[" + ", ".join(f"{v:{max_width}.4f}" for v in self.data[i]) + "]"
            lines.append(row_str)
        
        return "[\n  " + ",\n  ".join(lines) + "\n]"


def lu_decomposition(m: Matrix) -> Tuple[Matrix, Matrix, Matrix]:
    """
    Perform LU decomposition with partial pivoting.
    
    Returns:
        Tuple of (L, U, P) where PA = LU.
    """
    if m.rows != m.cols:
        raise ValueError("LU decomposition requires a square matrix")
    
    n = m.rows
    L = Matrix.identity(n)
    U = m.clone()
    P = Matrix.identity(n)
    
    for col in range(n):
        max_row = col
        for row in range(col + 1, n):
            if abs(U.data[row][col]) > abs(U.data[max_row][col]):
                max_row = row
        
        if max_row != col:
            U.data[col], U.data[max_row] = U.data[max_row][:], U.data[col][:]
            P.data[col], P.data[max_row] = P.data[max_row][:], P.data[col][:]
            if col > 0:
                L.data[col][:col], L.data[max_row][:col] = (
                    L.data[max_row][:col][:],
                    L.data[col][:col][:]
                )
        
        for row in range(col + 1, n):
            if abs(U.data[col][col]) < 1e-12:
                continue
            
            factor = U.data[row][col] / U.data[col][col]
            L.data[row][col] = factor
            
            for j in range(col, n):
                U.data[row][j] -= factor * U.data[col][j]
    
    return (L, U, P)


def solve_linear_system(A: Matrix, b: Matrix) -> Optional[Matrix]:
    """
    Solve a linear system Ax = b using LU decomposition.
    
    Args:
        A: Coefficient matrix.
        b: Right-hand side vector (as column matrix).
        
    Returns:
        Solution vector x, or None if the system is singular.
    """
    if A.rows != A.cols:
        raise ValueError("Coefficient matrix must be square")
    
    if b.cols != 1:
        raise ValueError("b must be a column vector")
    
    try:
        L, U, P = lu_decomposition(A)
        n = A.rows
        
        Pb = P.multiply(b)
        y = Matrix.zeros(n, 1)
        
        for i in range(n):
            total = 0.0
            for j in range(i):
                total += L.data[i][j] * y.data[j][0]
            y.data[i][0] = Pb.data[i][0] - total
        
        x = Matrix.zeros(n, 1)
        for i in range(n - 1, -1, -1):
            total = 0.0
            for j in range(i + 1, n):
                total += U.data[i][j] * x.data[j][0]
            
            if abs(U.data[i][i]) < 1e-12:
                return None
            
            x.data[i][0] = (y.data[i][0] - total) / U.data[i][i]
        
        return x
    
    except Exception:
        return None


def matrix_rank(m: Matrix, tolerance: float = 1e-10) -> int:
    """
    Calculate the rank of a matrix.
    
    Args:
        m: The matrix.
        tolerance: Numerical tolerance for considering a value as zero.
        
    Returns:
        The rank of the matrix.
    """
    _, U, _ = lu_decomposition(m)
    rank = 0
    
    for i in range(min(U.rows, U.cols)):
        if abs(U.data[i][i]) > tolerance:
            rank += 1
    
    return rank


def eigenvalues_power_iteration(m: Matrix, num_iterations: int = 100, tolerance: float = 1e-10) -> Tuple[float, Matrix]:
    """
    Estimate the largest eigenvalue using the power iteration method.
    
    Args:
        m: The matrix (must be symmetric for convergence).
        num_iterations: Maximum number of iterations.
        tolerance: Convergence tolerance.
        
    Returns:
        Tuple of (largest_eigenvalue, eigenvector).
    """
    if m.rows != m.cols:
        raise ValueError("Matrix must be square")
    
    n = m.rows
    v = Matrix.ones(n, 1)
    v = v.scalar_multiply(1.0 / v.norm())
    
    eigenvalue = 0.0
    
    for _ in range(num_iterations):
        m_v = m.multiply(v)
        new_eigenvalue = v.transpose().multiply(m_v).data[0][0]
        
        new_v = m_v.scalar_multiply(1.0 / m_v.norm())
        
        if abs(new_eigenvalue - eigenvalue) < tolerance:
            eigenvalue = new_eigenvalue
            v = new_v
            break
        
        eigenvalue = new_eigenvalue
        v = new_v
    
    return (eigenvalue, v)
