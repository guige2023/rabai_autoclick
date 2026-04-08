"""
Constraint solver utilities for layout constraint solving.

Provides a simple constraint satisfaction solver for
layout constraints like alignment, distribution, and sizing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class Variable:
    """A variable in the constraint system."""
    name: str
    value: float = 0.0
    min_value: float = -math.inf
    max_value: float = math.inf


@dataclass
class Constraint:
    """A constraint between variables."""
    name: str
    variables: list[str]
    evaluate: callable  # Returns error (0 = satisfied)
    priority: int = 0  # Higher = more important


class ConstraintSolver:
    """Simple constraint solver for layout calculations."""

    def __init__(self):
        self._variables: dict[str, Variable] = {}
        self._constraints: list[Constraint] = []

    def add_variable(self, name: str, initial_value: float = 0.0) -> Variable:
        """Add a variable to the solver."""
        var = Variable(name=name, value=initial_value)
        self._variables[name] = var
        return var

    def get(self, name: str) -> Optional[Variable]:
        """Get a variable by name."""
        return self._variables.get(name)

    def set_value(self, name: str, value: float) -> None:
        """Set a variable value."""
        if name in self._variables:
            self._variables[name].value = max(
                self._variables[name].min_value,
                min(self._variables[name].max_value, value),
            )

    def add_constraint(self, constraint: Constraint) -> None:
        """Add a constraint."""
        self._constraints.append(constraint)
        self._constraints.sort(key=lambda c: c.priority, reverse=True)

    def add_equal(self, name_a: str, name_b: str, offset: float = 0.0) -> Constraint:
        """Add equality constraint: A = B + offset."""
        def eval_fn(vars_dict):
            a = vars_dict.get(name_a, 0)
            b = vars_dict.get(name_b, 0)
            return abs(a - b - offset)
        c = Constraint(name=f"{name_a} == {name_b}", variables=[name_a, name_b], evaluate=eval_fn)
        self.add_constraint(c)
        return c

    def add_less_equal(self, name_a: str, name_b: str) -> Constraint:
        """Add inequality: A <= B."""
        def eval_fn(vars_dict):
            a = vars_dict.get(name_a, 0)
            b = vars_dict.get(name_b, 0)
            return max(0, a - b)
        c = Constraint(name=f"{name_a} <= {name_b}", variables=[name_a, name_b], evaluate=eval_fn)
        self.add_constraint(c)
        return c

    def add_greater_equal(self, name_a: str, name_b: str) -> Constraint:
        """Add inequality: A >= B."""
        def eval_fn(vars_dict):
            a = vars_dict.get(name_a, 0)
            b = vars_dict.get(name_b, 0)
            return max(0, b - a)
        c = Constraint(name=f"{name_a} >= {name_b}", variables=[name_a, name_b], evaluate=eval_fn)
        self.add_constraint(c)
        return c

    def add_center_constraint(self, parent: str, child: str, axis: str = "x") -> Constraint:
        """Add centering constraint: child centered in parent."""
        if axis == "x":
            def eval_fn(vars_dict):
                pw = vars_dict.get(f"{parent}_width", 0)
                cw = vars_dict.get(f"{child}_width", 0)
                px = vars_dict.get(f"{parent}_x", 0)
                cx = vars_dict.get(f"{child}_x", 0)
                expected_cx = px + pw / 2 - cw / 2
                return abs(cx - expected_cx)
            vars_list = [f"{parent}_x", f"{parent}_width", f"{child}_x", f"{child}_width"]
        else:
            def eval_fn(vars_dict):
                ph = vars_dict.get(f"{parent}_height", 0)
                ch = vars_dict.get(f"{child}_height", 0)
                py = vars_dict.get(f"{parent}_y", 0)
                cy = vars_dict.get(f"{child}_y", 0)
                expected_cy = py + ph / 2 - ch / 2
                return abs(cy - expected_cy)
            vars_list = [f"{parent}_y", f"{parent}_height", f"{child}_y", f"{child}_height"]

        c = Constraint(name=f"{child} centered on {parent} ({axis})", variables=vars_list, evaluate=eval_fn)
        self.add_constraint(c)
        return c

    def solve(self, max_iterations: int = 100, tolerance: float = 0.01) -> dict[str, float]:
        """Solve the constraint system.

        Returns:
            Dict of variable names to solved values.
        """
        vars_dict = {name: v.value for name, v in self._variables.items()}

        for iteration in range(max_iterations):
            total_error = 0.0

            for constraint in self._constraints:
                error = constraint.evaluate(vars_dict)
                total_error += error

                if error > tolerance:
                    # Adjust variables to reduce error
                    adjustment = error / len(constraint.variables) if constraint.variables else error
                    for var_name in constraint.variables:
                        if var_name in self._variables:
                            var = self._variables[var_name]
                            # Gradient descent-like adjustment
                            current = vars_dict[var_name]
                            vars_dict[var_name] = current - adjustment * 0.1

            if total_error < tolerance:
                break

        # Update variable values
        for name, var in self._variables.items():
            var.value = vars_dict.get(name, var.value)

        return vars_dict


__all__ = ["ConstraintSolver", "Variable", "Constraint"]
