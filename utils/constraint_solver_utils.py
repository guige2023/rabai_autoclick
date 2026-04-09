"""Constraint solver utilities for layout and positioning problems.

This module provides utilities for solving constraint satisfaction problems
common in UI layout, element positioning, and resource allocation.
"""

from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import math


class ConstraintType(Enum):
    """Types of constraints."""
    EQUAL = "equal"
    LESS_THAN = "less_than"
    LESS_EQUAL = "less_equal"
    GREATER_THAN = "greater_than"
    GREATER_EQUAL = "greater_equal"
    RANGE = "range"


@dataclass
class Constraint:
    """A single constraint between variables.

    Attributes:
        left_var: Left variable name.
        right_var: Right variable name (or None for constants).
        constraint_type: Type of comparison.
        value: Constraint value (for binary constraints).
        weight: Constraint priority (higher = more important).
    """
    left_var: str
    right_var: str | float | None
    constraint_type: ConstraintType
    value: float = 0.0
    weight: float = 1.0

    def evaluate(self, vars: dict[str, float]) -> bool:
        """Evaluate this constraint given variable values.

        Args:
            vars: Dictionary of variable names to values.

        Returns:
            True if constraint is satisfied.
        """
        left_val = vars.get(self.left_var, 0.0)

        if self.right_var is None:
            right_val = self.value
        elif isinstance(self.right_var, (int, float)):
            right_val = float(self.right_var)
        else:
            right_val = vars.get(self.right_var, 0.0)

        if self.constraint_type == ConstraintType.EQUAL:
            return abs(left_val - right_val) < 0.001
        elif self.constraint_type == ConstraintType.LESS_THAN:
            return left_val < right_val
        elif self.constraint_type == ConstraintType.LESS_EQUAL:
            return left_val <= right_val + 0.001
        elif self.constraint_type == ConstraintType.GREATER_THAN:
            return left_val > right_val
        elif self.constraint_type == ConstraintType.GREATER_EQUAL:
            return left_val >= right_val - 0.001
        elif self.constraint_type == ConstraintType.RANGE:
            return self.value <= left_val <= self.value + self.value

        return True


@dataclass
class SolverResult:
    """Result of constraint solving attempt.

    Attributes:
        success: Whether solving succeeded.
        solution: Variable values if successful.
        violations: List of violated constraints.
        iterations: Number of iterations used.
    """
    success: bool
    solution: dict[str, float] = field(default_factory=dict)
    violations: list[Constraint] = field(default_factory=list)
    iterations: int = 0


class ConstraintSolver:
    """Gradient descent constraint solver.

    Minimizes constraint violations iteratively.
    """

    def __init__(
        self,
        constraints: list[Constraint] | None = None,
        learning_rate: float = 0.1,
        max_iterations: int = 100,
        tolerance: float = 0.001
    ) -> None:
        """Initialize solver.

        Args:
            constraints: Initial list of constraints.
            learning_rate: Gradient descent step size.
            max_iterations: Maximum iterations before giving up.
            tolerance: Convergence tolerance.
        """
        self.constraints = constraints or []
        self.learning_rate = learning_rate
        self.max_iterations = max_iterations
        self.tolerance = tolerance
        self._variables: set[str] = set()

    def add_constraint(self, constraint: Constraint) -> None:
        """Add a constraint to the solver.

        Args:
            constraint: Constraint to add.
        """
        self.constraints.append(constraint)
        self._variables.add(constraint.left_var)
        if isinstance(constraint.right_var, str):
            self._variables.add(constraint.right_var)

    def add_variable(self, name: str, initial_value: float = 0.0) -> None:
        """Add a variable to the solver.

        Args:
            name: Variable name.
            initial_value: Starting value.
        """
        self._variables.add(name)

    def solve(self, initial_vars: dict[str, float] | None = None) -> SolverResult:
        """Solve constraints starting from initial values.

        Args:
            initial_vars: Optional initial variable values.

        Returns:
            SolverResult with solution or violations.
        """
        vars: dict[str, float] = initial_vars or {}
        for v in self._variables:
            if v not in vars:
                vars[v] = 0.0

        for iteration in range(self.max_iterations):
            violations: list[Constraint] = []
            gradients: dict[str, float] = {v: 0.0 for v in self._variables}

            for constraint in self.constraints:
                if not constraint.evaluate(vars):
                    violations.append(constraint)

                    grad = self._compute_gradient(constraint, vars)
                    for var, g in grad.items():
                        gradients[var] += g * constraint.weight

            if not violations:
                return SolverResult(
                    success=True,
                    solution=vars.copy(),
                    violations=[],
                    iterations=iteration + 1
                )

            total_error = sum(
                self._compute_error(constraint, vars)
                for constraint in violations
            )

            if total_error < self.tolerance:
                return SolverResult(
                    success=True,
                    solution=vars.copy(),
                    violations=violations,
                    iterations=iteration + 1
                )

            for var in self._variables:
                vars[var] -= self.learning_rate * gradients[var]

        return SolverResult(
            success=False,
            solution=vars.copy(),
            violations=violations,
            iterations=self.max_iterations
        )

    def _compute_error(self, constraint: Constraint, vars: dict[str, float]) -> float:
        """Compute error for a violated constraint.

        Args:
            constraint: Constraint to compute error for.
            vars: Current variable values.

        Returns:
            Error value (higher = more violated).
        """
        left_val = vars.get(constraint.left_var, 0.0)

        if isinstance(constraint.right_var, (int, float)):
            right_val = float(constraint.right_var)
        elif constraint.right_var is None:
            right_val = constraint.value
        else:
            right_val = vars.get(constraint.right_var, 0.0)

        if constraint.constraint_type == ConstraintType.EQUAL:
            return abs(left_val - right_val)
        elif constraint.constraint_type == ConstraintType.LESS_THAN:
            return max(0.0, left_val - right_val) + 1.0
        elif constraint.constraint_type == ConstraintType.LESS_EQUAL:
            return max(0.0, left_val - right_val)
        elif constraint.constraint_type == ConstraintType.GREATER_THAN:
            return max(0.0, right_val - left_val) + 1.0
        elif constraint.constraint_type == ConstraintType.GREATER_EQUAL:
            return max(0.0, right_val - left_val)

        return 0.0

    def _compute_gradient(
        self,
        constraint: Constraint,
        vars: dict[str, float]
    ) -> dict[str, float]:
        """Compute gradient for constraint violation.

        Args:
            constraint: Constraint to compute gradient for.
            vars: Current variable values.

        Returns:
            Dictionary of variable gradients.
        """
        gradients: dict[str, float] = {v: 0.0 for v in self._variables}
        epsilon = 0.01

        base_error = self._compute_error(constraint, vars)

        for var in self._variables:
            vars_plus = vars.copy()
            vars_plus[var] = vars.get(var, 0.0) + epsilon
            error_plus = self._compute_error(constraint, vars_plus)
            gradients[var] = (error_plus - base_error) / epsilon

        return gradients


def solve_box_layout(
    container_width: float,
    container_height: float,
    elements: list[tuple[str, tuple[float, float]]]
) -> dict[str, tuple[float, float, float, float]]:
    """Solve layout constraints for a horizontal or vertical box.

    Args:
        container_width: Width of container.
        container_height: Height of container.
        elements: List of (name, size) tuples.

    Returns:
        Dictionary mapping element name to (x, y, width, height).
    """
    solver = ConstraintSolver()
    positions: dict[str, tuple[float, float, float, float]] = {}

    sorted_elements = sorted(elements, key=lambda e: e[0])

    current_x = 0.0

    for name, (elem_width, elem_height) in sorted_elements:
        x_constraint = Constraint(
            left_var=f"{name}_x",
            right_var=float(current_x),
            constraint_type=ConstraintType.EQUAL
        )
        solver.add_constraint(x_constraint)

        y_constraint = Constraint(
            left_var=f"{name}_y",
            right_var=0.0,
            constraint_type=ConstraintType.EQUAL
        )
        solver.add_constraint(y_constraint)

        w_constraint = Constraint(
            left_var=f"{name}_w",
            right_var=float(elem_width),
            constraint_type=ConstraintType.EQUAL
        )
        solver.add_constraint(w_constraint)

        h_constraint = Constraint(
            left_var=f"{name}_h",
            right_var=float(elem_height),
            constraint_type=ConstraintType.EQUAL
        )
        solver.add_constraint(h_constraint)

        current_x += elem_width

    result = solver.solve()

    for name, _ in sorted_elements:
        x = result.solution.get(f"{name}_x", 0.0)
        y = result.solution.get(f"{name}_y", 0.0)
        w = result.solution.get(f"{name}_w", 0.0)
        h = result.solution.get(f"{name}_h", 0.0)
        positions[name] = (x, y, w, h)

    return positions


def distribute_equal(
    total_space: float,
    count: int,
    gap: float = 0.0
) -> list[float]:
    """Calculate equal distribution of space among elements.

    Args:
        total_space: Total available space.
        count: Number of elements.
        gap: Gap between elements.

    Returns:
        List of positions for each element.
    """
    if count <= 0:
        return []

    if count == 1:
        return [total_space / 2]

    total_gap = gap * (count - 1)
    available = total_space - total_gap
    element_size = available / count

    positions: list[float] = []
    current = element_size / 2

    for _ in range(count):
        positions.append(current)
        current += element_size + gap

    return positions


def solve_centered(
    element_size: float,
    container_size: float
) -> float:
    """Calculate centered position for an element.

    Args:
        element_size: Size of element to center.
        container_size: Size of container.

    Returns:
        Centered position.
    """
    return (container_size - element_size) / 2


def solve_alignment(
    positions: list[float],
    alignment: str = "left"
) -> list[float]:
    """Adjust positions based on alignment mode.

    Args:
        positions: List of element positions.
        alignment: Alignment type ("left", "center", "right", "spread").

    Returns:
        Adjusted positions.
    """
    if not positions:
        return []

    if alignment == "left":
        min_pos = min(positions)
        return [p - min_pos for p in positions]

    elif alignment == "right":
        max_pos = max(positions)
        return [p - max_pos for p in positions]

    elif alignment == "center":
        center = sum(positions) / len(positions)
        return [p - center for p in positions]

    elif alignment == "spread":
        min_pos = min(positions)
        max_pos = max(positions)
        total_space = max_pos - min_pos
        if total_space == 0:
            return positions

        return [min_pos + (p - min_pos) / total_space * total_space for p in positions]

    return positions
