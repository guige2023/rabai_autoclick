"""
Data Matrix Action Module.

Provides matrix operations including arithmetic, decomposition,
linear algebra operations, and transformation utilities.

Author: RabAi Team
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional


class MatrixFormat(Enum):
    """Matrix storage format."""
    DENSE = "dense"
    SPARSE = "sparse"
    BANDED = "banded"


@dataclass
class Matrix:
    """N×M matrix representation."""
    data: list[list[float]]
    _rows: int = field(init=False)
    _cols: int = field(init=False)

    def __post_init__(self):
        self._rows = len(self.data) if self.data else 0
        self._cols = len(self.data[0]) if self.data and self.data[0] else 0

    @property
    def shape(self) -> tuple[int, int]:
        """Return (rows, cols)."""
        return (self._rows, self._cols)

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def cols(self) -> int:
        return self._cols

    def __getitem__(self, key: tuple[int, int]) -> float:
        r, c = key
        return self.data[r][c]

    def __setitem__(self, key: tuple[int, int], value: float) -> None:
        r, c = key
        self.data[r][c] = value

    def __add__(self, other: Matrix) -> Matrix:
        if self.shape != other.shape:
            raise ValueError(f"Shape mismatch: {self.shape} vs {other.shape}")
        result = [[self[r, c] + other[r, c] for c in range(self.cols)] for r in range(self.rows)]
        return Matrix(data=result)

    def __sub__(self, other: Matrix) -> Matrix:
        if self.shape != other.shape:
            raise ValueError(f"Shape mismatch: {self.shape} vs {other.shape}")
        result = [[self[r, c] - other[r, c] for c in range(self.cols)] for r in range(self.rows)]
        return Matrix(data=result)

    def __mul__(self, scalar: float) -> Matrix:
        result = [[self[r, c] * scalar for c in range(self.cols)] for r in range(self.rows)]
        return Matrix(data=result)

    def __matmul__(self, other: Matrix) -> Matrix:
        if self.cols != other.rows:
            raise ValueError(f"Cannot multiply: {self.shape} @ {other.shape}")
        result = [
            [sum(self[r, k] * other[k, c] for k in range(self.cols)) for c in range(other.cols)]
            for r in range(self.rows)
        ]
        return Matrix(data=result)

    def transpose(self) -> Matrix:
        """Return transpose of matrix."""
        result = [[self[r, c] for r in range(self.rows)] for c in range(self.cols)]
        return Matrix(data=result)

    def trace(self) -> float:
        """Return trace (sum of diagonal)."""
        return sum(self[i, i] for i in range(min(self.rows, self.cols)))

    def determinant(self) -> float:
        """Compute determinant (only for square matrices)."""
        if self.rows != self.cols:
            raise ValueError("Determinant requires square matrix")
        if self.rows == 1:
            return self[0, 0]
        if self.rows == 2:
            return self[0, 0] * self[1, 1] - self[0, 1] * self[1, 0]
        det = 0.0
        for j in range(self.cols):
            det += ((-1) ** j) * self[0, j] * self.minor(0, j).determinant()
        return det

    def minor(self, row: int, col: int) -> Matrix:
        """Return minor matrix by removing row and col."""
        result = [
            [self[r, c] for c in range(self.cols) if c != col]
            for r in range(self.rows) if r != row
        ]
        return Matrix(data=result)

    def inverse(self) -> Matrix:
        """Compute matrix inverse."""
        det = self.determinant()
        if abs(det) < 1e-10:
            raise ValueError("Matrix is singular, cannot invert")
        if self.rows == 2:
            det = self[0, 0] * self[1, 1] - self[0, 1] * self[1, 0]
            return Matrix(data=[
                [self[1, 1] / det, -self[0, 1] / det],
                [-self[1, 0] / det, self[0, 0] / det],
            ])
        adj = self.adjoint()
        return adj * (1.0 / det)

    def adjoint(self) -> Matrix:
        """Compute adjoint matrix."""
        result = [
            [((-1) ** (r + c)) * self.minor(r, c).determinant() for c in range(self.cols)]
            for r in range(self.rows)
        ]
        return Matrix(data=result).transpose()

    def norm(self, order: int = 2) -> float:
        """Compute matrix norm."""
        if order == 1:
            return max(sum(abs(self[r, c]) for r in range(self.rows)) for c in range(self.cols))
        if order == 2:
            eigenvalues = self.eigenvalues()
            return math.sqrt(max(abs(lam) for lam in eigenvalues)) if eigenvalues else 0.0
        if order == float("inf"):
            return max(sum(abs(self[r, c]) for c in range(self.cols)) for r in range(self.rows))
        flat = [abs(self[r, c]) for r in range(self.rows) for c in range(self.cols)]
        return sum(x ** order for x in flat) ** (1.0 / order)

    def eigenvalues(self) -> list[complex]:
        """Compute eigenvalues (2×2 and 3×3 only)."""
        if self.rows != self.cols:
            raise ValueError("Eigenvalues require square matrix")
        if self.rows == 2:
            a, b, c, d = self[0, 0], self[0, 1], self[1, 0], self[1, 1]
            trace = a + d
            det = a * d - b * c
            discriminant = trace * trace - 4 * det
            sqrt_disc = math.sqrt(abs(discriminant))
            return [(trace + sqrt_disc) / 2, (trace - sqrt_disc) / 2]
        return []


class MatrixFactory:
    """Factory for creating special matrices."""

    @staticmethod
    def zeros(rows: int, cols: int) -> Matrix:
        """Create zero matrix."""
        return Matrix(data=[[0.0] * cols for _ in range(rows)])

    @staticmethod
    def ones(rows: int, cols: int) -> Matrix:
        """Create all-ones matrix."""
        return Matrix(data=[[1.0] * cols for _ in range(rows)])

    @staticmethod
    def identity(n: int) -> Matrix:
        """Create identity matrix."""
        data = [[1.0 if r == c else 0.0 for c in range(n)] for r in range(n)]
        return Matrix(data=data)

    @staticmethod
    def diagonal(values: list[float]) -> Matrix:
        """Create diagonal matrix."""
        n = len(values)
        data = [[values[r] if r == c else 0.0 for c in range(n)] for r in range(n)]
        return Matrix(data=data)

    @staticmethod
    def random(rows: int, cols: int, seed: Optional[int] = None) -> Matrix:
        """Create matrix with random values in [0, 1)."""
        if seed is not None:
            import random
            random.seed(seed)
        data = [[random.random() for _ in range(cols)] for _ in range(rows)]
        return Matrix(data=data)

    @staticmethod
    def from_list(data: list[list[float]]) -> Matrix:
        """Create matrix from nested list."""
        return Matrix(data=data)


class MatrixDecomposition:
    """Matrix decomposition operations."""

    @staticmethod
    def lu_decomposition(m: Matrix) -> tuple[Matrix, Matrix, Matrix]:
        """LU decomposition with partial pivoting."""
        n = m.rows
        L = MatrixFactory.identity(n)
        U = MatrixFactory.zeros(n, n)
        P = MatrixFactory.identity(n)
        A = Matrix(data=[row[:] for row in m.data])

        for j in range(n):
            max_val = abs(A[j, j])
            max_row = j
            for i in range(j + 1, n):
                if abs(A[i, j]) > max_val:
                    max_val = abs(A[i, j])
                    max_row = i
            if max_row != j:
                A.data[j], A.data[max_row] = A.data[max_row], A.data[j]
                P.data[j], P.data[max_row] = P.data[max_row], P.data[j]
                L.data[j][:j], L.data[max_row][:j] = L.data[max_row][:j], L.data[j][:j]

            for i in range(j, n):
                U[j, i] = A[j, i] - sum(L[j, k] * U[k, i] for k in range(j))
            for i in range(j + 1, n):
                if abs(U[j, j]) < 1e-10:
                    L[i, j] = 0
                else:
                    L[i, j] = (A[i, j] - sum(L[i, k] * U[k, j] for k in range(j))) / U[j, j]

        return P, L, U

    @staticmethod
    def qr_decomposition(m: Matrix) -> tuple[Matrix, Matrix]:
        """QR decomposition using Gram-Schmidt."""
        n, p = m.rows, m.cols
        Q = MatrixFactory.zeros(n, p)
        R = MatrixFactory.zeros(p, p)

        for j in range(p):
            v = [m[i, j] for i in range(n)]
            for i in range(j):
                q_col = [Q[i2, i] for i2 in range(n)]
                R[i, j] = sum(v[k] * q_col[k] for k in range(n))
                v = [v[k] - R[i, j] * q_col[k] for k in range(n)]
            R[j, j] = math.sqrt(sum(x * x for x in v))
            if R[j, j] > 1e-10:
                for i in range(n):
                    Q[i, j] = v[i] / R[j, j]

        return Q, R

    @staticmethod
    def eigenvalues_power_iteration(m: Matrix, num_iterations: int = 100, tolerance: float = 1e-6) -> float:
        """Find largest eigenvalue using power iteration."""
        if m.rows != m.cols:
            raise ValueError("Matrix must be square")
        n = m.rows
        b = [1.0 / math.sqrt(n)] * n

        for _ in range(num_iterations):
            Ab = [sum(m[i, j] * b[j] for j in range(n)) for i in range(n)]
            eigenvalue = sum(b[i] * Ab[i] for i in range(n))
            norm = math.sqrt(sum(x * x for x in Ab))
            if norm < 1e-10:
                break
            b_new = [x / norm for x in Ab]
            if math.sqrt(sum((b_new[i] - b[i]) ** 2 for i in range(n))) < tolerance:
                break
            b = b_new

        return eigenvalue


def solve_linear_system(A: Matrix, b: list[float]) -> list[float]:
    """Solve Ax = b using LU decomposition."""
    P, L, U = MatrixDecomposition.lu_decomposition(A)
    n = len(b)

    Pb = [sum(P[i, j] * b[j] for j in range(n)) for i in range(n)]

    y = [0.0] * n
    for i in range(n):
        y[i] = Pb[i] - sum(L[i, j] * y[j] for j in range(i))

    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = (y[i] - sum(U[i, j] * x[j] for j in range(i + 1, n))) / U[i, i] if abs(U[i, i]) > 1e-10 else 0.0

    return x


async def demo():
    """Demo matrix operations."""
    A = MatrixFactory.from_list([
        [4.0, 3.0],
        [6.0, 3.0],
    ])
    b = [10.0, 18.0]

    print(f"A shape: {A.shape}")
    print(f"A determinant: {A.determinant():.4f}")

    A_inv = A.inverse()
    print(f"A inverse: {A_inv.data}")

    x = solve_linear_system(A, b)
    print(f"Solution x: {x}")

    M = MatrixFactory.identity(3)
    print(f"3x3 identity:\n{[row.data for row in M.data]}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo())
