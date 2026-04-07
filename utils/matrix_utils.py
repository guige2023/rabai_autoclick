"""
Matrix utilities for linear algebra operations.

Provides matrix multiplication, determinants, inverses,
eigenvalues, and matrix decompositions.
"""

from __future__ import annotations

import math
from typing import NamedTuple


Matrix = list[list[float]]


class LUDecomposition(NamedTuple):
    """Result of LU decomposition."""
    L: Matrix
    U: Matrix
    P: Matrix


class EigenResult(NamedTuple):
    """Result of eigenvalue computation."""
    eigenvalues: list[complex]
    eigenvectors: list[list[complex]]


def mat_mul(A: Matrix, B: Matrix) -> Matrix:
    """Matrix multiplication A @ B."""
    n, m, p = len(A), len(A[0]), len(B[0])
    result = [[0.0] * p for _ in range(n)]
    for i in range(n):
        for k in range(len(A[i])):
            if A[i][k] != 0:
                for j in range(p):
                    result[i][j] += A[i][k] * B[k][j]
    return result


def mat_add(A: Matrix, B: Matrix) -> Matrix:
    """Matrix addition."""
    return [[A[i][j] + B[i][j] for j in range(len(A[0]))] for i in range(len(A))]


def mat_sub(A: Matrix, B: Matrix) -> Matrix:
    """Matrix subtraction."""
    return [[A[i][j] - B[i][j] for j in range(len(A[0]))] for i in range(len(A))]


def mat_scalar_mul(A: Matrix, s: float) -> Matrix:
    """Scalar multiplication."""
    return [[A[i][j] * s for j in range(len(A[0]))] for i in range(len(A))]


def mat_transpose(A: Matrix) -> Matrix:
    """Matrix transpose."""
    return [[A[j][i] for j in range(len(A))] for i in range(len(A[0]))]


def mat_identity(n: int) -> Matrix:
    """Create n×n identity matrix."""
    return [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]


def mat_zeros(rows: int, cols: int) -> Matrix:
    """Create rows×cols zero matrix."""
    return [[0.0] * cols for _ in range(rows)]


def mat_determinant(A: Matrix) -> float:
    """
    Compute determinant using LU decomposition.

    Args:
        A: Square matrix

    Returns:
        Determinant value
    """
    n = len(A)
    if n == 1:
        return A[0][0]
    if n == 2:
        return A[0][0] * A[1][1] - A[0][1] * A[1][0]
    lu = lu_decomposition(A)
    det = 1.0
    for i in range(n):
        if lu.L[i][i] == 0:
            return 0.0
        det *= lu.L[i][i] * lu.U[i][i]
    return det


def lu_decomposition(A: Matrix) -> LUDecomposition:
    """
    LU decomposition with partial pivoting (Doolittle's method).

    Args:
        A: Square matrix

    Returns:
        LUDecomposition with L, U, and P matrices
    """
    n = len(A)
    L = mat_identity(n)
    U = [row[:] for row in A]
    P = mat_identity(n)

    for k in range(n):
        max_val = abs(U[k][k])
        max_row = k
        for i in range(k + 1, n):
            if abs(U[i][k]) > max_val:
                max_val = abs(U[i][k])
                max_row = i
        if max_row != k:
            U[k], U[max_row] = U[max_row], U[k]
            P[k], P[max_row] = P[max_row], P[k]
            L[k][:k], L[max_row][:k] = L[max_row][:k], L[k][:k]
        for i in range(k + 1, n):
            if U[k][k] != 0:
                L[i][k] = U[i][k] / U[k][k]
                for j in range(k, n):
                    U[i][j] -= L[i][k] * U[k][j]
    return LUDecomposition(L=L, U=U, P=P)


def mat_inverse(A: Matrix) -> Matrix | None:
    """
    Compute matrix inverse using Gauss-Jordan elimination.

    Args:
        A: Square matrix

    Returns:
        Inverse matrix, or None if singular
    """
    n = len(A)
    aug = [A[i][:] + [1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    for i in range(n):
        max_row = max(range(i, n), key=lambda r: abs(aug[r][i]))
        aug[i], aug[max_row] = aug[max_row], aug[i]
        if abs(aug[i][i]) < 1e-12:
            return None
        pivot = aug[i][i]
        for j in range(2 * n):
            aug[i][j] /= pivot
        for k in range(n):
            if k != i:
                factor = aug[k][i]
                for j in range(2 * n):
                    aug[k][j] -= factor * aug[i][j]
    return [aug[i][n:] for i in range(n)]


def mat_trace(A: Matrix) -> float:
    """Compute matrix trace (sum of diagonal elements)."""
    return sum(A[i][i] for i in range(len(A)))


def frobenius_norm(A: Matrix) -> float:
    """Compute Frobenius norm."""
    return math.sqrt(sum(A[i][j] ** 2 for i in range(len(A)) for j in range(len(A[0]))))


def power_iteration(A: Matrix, num_sim: int = 100, tol: float = 1e-8) -> tuple[float, list[float]]:
    """
    Dominant eigenvalue using power iteration.

    Args:
        A: Square matrix
        num_sim: Maximum iterations
        tol: Convergence tolerance

    Returns:
        Tuple of (dominant eigenvalue, eigenvector)
    """
    n = len(A)
    v = [1.0 / math.sqrt(n)] * n
    eigenvalue = 0.0
    for _ in range(num_sim):
        Av = [sum(A[i][j] * v[j] for j in range(n)) for i in range(n)]
        new_eig = sum(v[i] * Av[i] for i in range(n))
        norm = math.sqrt(sum(x * x for x in Av))
        if norm < 1e-12:
            break
        v = [x / norm for x in Av]
        if abs(new_eig - eigenvalue) < tol:
            eigenvalue = new_eig
            break
        eigenvalue = new_eig
    return eigenvalue, v


def char_poly_coeffs(A: Matrix) -> list[float]:
    """
    Compute characteristic polynomial coefficients using Faddeev-LeVerrier.

    Returns:
        List c_n, c_{n-1}, ..., c_0 where det(A - λI) = λ^n + c_{n-1}λ^{n-1} + ...
    """
    n = len(A)
    c = [0.0] * (n + 1)
    c[n] = 1.0
    M = [row[:] for row in A]
    for k in range(1, n + 1):
        ck_minus_1 = mat_trace(M) / k
        c[n - k] = -ck_minus_1
        M = mat_sub(A, mat_scalar_mul(mat_identity(k), ck_minus_1))
    return c
