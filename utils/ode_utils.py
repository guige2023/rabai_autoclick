"""
Ordinary Differential Equation (ODE) solvers.

Provides Euler, RK4, adaptive step size methods, and stiff ODE solvers.
"""

from __future__ import annotations

from typing import Callable


State = list[float]
Derivative = list[float]
ODEDerivativeFunc = Callable[[float, State], State]


def euler_step(
    f: ODEDerivativeFunc,
    t: float,
    y: State,
    h: float,
) -> tuple[float, State]:
    """
    Forward Euler method (first order).

    Args:
        f: dy/dt = f(t, y)
        t: Current time
        y: Current state
        h: Step size

    Returns:
        Tuple of (next_time, next_state).
    """
    dy = f(t, y)
    return t + h, [y[i] + h * dy[i] for i in range(len(y))]


def rk4_step(
    f: ODEDerivativeFunc,
    t: float,
    y: State,
    h: float,
) -> tuple[float, State]:
    """
    Classical Runge-Kutta 4th order method.

    Args:
        f: dy/dt = f(t, y)
        t: Current time
        y: Current state
        h: Step size

    Returns:
        Tuple of (next_time, next_state).
    """
    k1 = f(t, y)
    k2 = f(t + h / 2, [y[i] + h * k1[i] / 2 for i in range(len(y))])
    k3 = f(t + h / 2, [y[i] + h * k2[i] / 2 for i in range(len(y))])
    k4 = f(t + h, [y[i] + h * k3[i] for i in range(len(y))])
    return t + h, [
        y[i] + (h / 6) * (k1[i] + 2 * k2[i] + 2 * k3[i] + k4[i])
        for i in range(len(y))
    ]


def rk45_step(
    f: ODEDerivativeFunc,
    t: float,
    y: State,
    h: float,
) -> tuple[float, State, float, State]:
    """
    Dormand-Prince RK45 (RK4(5) adaptive step size).

    Returns:
        Tuple of (next_time, next_state, error_estimate, low_order_state).
    """
    c2, c3, c4, c5, c6 = 1/5, 3/10, 4/5, 8/9, 1
    a21 = 1/5
    a31, a32 = 3/40, 9/40
    a41, a42, a43 = 44/45, -56/15, 32/9
    a51, a52, a53, a54 = -12/5, 2, -64/45, 24/5
    a61, a62, a63, a64, a65 = -301/3375, 6, 600/1125, -12/5, 12/125
    b1, b2, b3, b4, b5, b6 = 15/54, 0, -135/2432, 2784/229635, 1859/4104, 225/35128
    cb1, cb2, cb3, cb4, cb5, cb6 = 25360/1771, 0, -64448/4325, 48201/199015, 4599/28386, 3680/226917

    k1 = f(t, y)
    k2 = f(t + c2 * h, [y[i] + h * a21 * k1[i] for i in range(len(y))])
    k3 = f(t + c3 * h, [y[i] + h * (a31 * k1[i] + a32 * k2[i]) for i in range(len(y))])
    k4 = f(t + c4 * h, [y[i] + h * (a41 * k1[i] + a42 * k2[i] + a43 * k3[i]) for i in range(len(y))])
    k5 = f(t + c5 * h, [y[i] + h * (a51 * k1[i] + a52 * k2[i] + a53 * k3[i] + a54 * k4[i]) for i in range(len(y))])
    k6 = f(t + c6 * h, [y[i] + h * (a61 * k1[i] + a62 * k2[i] + a63 * k3[i] + a64 * k4[i] + a65 * k5[i]) for i in range(len(y))])

    y_next = [y[i] + h * (b1 * k1[i] + b2 * k2[i] + b3 * k3[i] + b4 * k4[i] + b5 * k5[i] + b6 * k6[i]) for i in range(len(y))]
    y_low = [y[i] + h * (cb1 * k1[i] + cb2 * k2[i] + cb3 * k3[i] + cb4 * k4[i] + cb5 * k5[i] + cb6 * k6[i]) for i in range(len(y))]

    error = max(abs(y_next[i] - y_low[i]) for i in range(len(y)))
    return t + h, y_next, error, y_low


def ode_solve(
    f: ODEDerivativeFunc,
    y0: State,
    t_span: tuple[float, float],
    h: float = 0.01,
    method: str = "rk4",
    tol: float = 1e-6,
) -> tuple[list[float], list[State]]:
    """
    Solve ODE system.

    Args:
        f: dy/dt = f(t, y)
        y0: Initial state
        t_span: (start_time, end_time)
        h: Step size
        method: 'euler', 'rk4', or 'rk45'
        tol: Tolerance for adaptive methods

    Returns:
        Tuple of (time_points, states).
    """
    t_start, t_end = t_span
    times: list[float] = [t_start]
    states: list[State] = [y0]

    t = t_start
    y = list(y0)

    while t < t_end:
        remaining = t_end - t
        if h > remaining:
            h = remaining

        if method == "euler":
            t, y = euler_step(f, t, y, h)
        elif method == "rk45":
            t_new, y_new, err, _ = rk45_step(f, t, y, h)
            if err > tol and h > 1e-10:
                h *= 0.9 * (tol / err) ** 0.25
            else:
                t, y = t_new, y_new
                h *= 1.1
        else:
            t, y = rk4_step(f, t, y, h)

        times.append(t)
        states.append(list(y))

        if t >= t_end:
            break

    return times, states


def solve_linear_system(A: list[list[float]], b: list[float]) -> list[float]:
    """Solve Ax = b using Gaussian elimination."""
    n = len(A)
    # Augmented matrix
    aug = [A[i] + [b[i]] for i in range(n)]

    # Forward elimination
    for i in range(n):
        # Find pivot
        max_row = i
        for j in range(i + 1, n):
            if abs(aug[j][i]) > abs(aug[max_row][i]):
                max_row = j
        aug[i], aug[max_row] = aug[max_row], aug[i]
        if abs(aug[i][i]) < 1e-12:
            continue
        # Eliminate
        for j in range(i + 1, n):
            factor = aug[j][i] / aug[i][i]
            for k in range(i, n + 1):
                aug[j][k] -= factor * aug[i][k]

    # Back substitution
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        if abs(aug[i][i]) < 1e-12:
            x[i] = 0.0
            continue
        x[i] = aug[i][n]
        for j in range(i + 1, n):
            x[i] -= aug[i][j] * x[j]
        x[i] /= aug[i][i]
    return x


def eigenvalue_power_iteration(
    A: list[list[float]],
    num_iter: int = 100,
    tol: float = 1e-8,
) -> tuple[float, list[float]]:
    """
    Power iteration to find dominant eigenvalue and eigenvector.

    Args:
        A: Square matrix (n x n)
        num_iter: Maximum iterations
        tol: Convergence tolerance

    Returns:
        Tuple of (dominant_eigenvalue, eigenvector).
    """
    n = len(A)
    # Random initial vector
    v = [1.0 / n] * n
    eigenvalue = 0.0

    for _ in range(num_iter):
        # A*v
        Av = [sum(A[i][j] * v[j] for j in range(n)) for i in range(n)]
        new_norm = sum(x * x for x in Av) ** 0.5
        if new_norm < 1e-12:
            break
        eigenvalue = new_norm
        v = [x / new_norm for x in Av]
    return eigenvalue, v


def eigenvalue_qr_iteration(
    A: list[list[float]],
    max_iter: int = 100,
) -> list[list[float]]:
    """
    QR iteration to find all eigenvalues.

    Args:
        A: Square matrix
        max_iter: Maximum iterations

    Returns:
        Approximate eigenvalues on diagonal.
    """
    n = len(A)
    R = [row[:] for row in A]

    for _ in range(max_iter):
        Q, R_next = qr_decomposition(R)
        R = mat_mul(R_next, Q)
    return [[R[i][i]] for i in range(n)]


def qr_decomposition(A: list[list[float]]) -> tuple[list[list[float]], list[list[float]]]:
    """QR decomposition using Gram-Schmidt."""
    n = len(A)
    Q: list[list[float]] = [[0.0] * n for _ in range(n)]
    R: list[list[float]] = [[0.0] * n for _ in range(n)]

    for j in range(n):
        # Column j of A
        v = [A[i][j] for i in range(n)]
        for i in range(j):
            R[i][j] = sum(Q[k][i] * A[k][j] for k in range(n))
            v = [v[k] - R[i][j] * Q[k][i] for k in range(n)]
        R[j][j] = sum(v[k] * v[k] for k in range(n)) ** 0.5
        if R[j][j] > 1e-12:
            for i in range(n):
                Q[i][j] = v[i] / R[j][j]
        else:
            for i in range(n):
                Q[i][j] = 0.0
    return Q, R


def mat_mul(A: list[list[float]], B: list[list[float]]) -> list[list[float]]:
    """Matrix multiplication."""
    n = len(A)
    m = len(B[0])
    p = len(B)
    C = [[0.0] * m for _ in range(n)]
    for i in range(n):
        for j in range(m):
            for k in range(p):
                C[i][j] += A[i][k] * B[k][j]
    return C
