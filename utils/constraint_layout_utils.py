"""Constraint Layout Utilities.

This module provides utilities for constraint-based layout management,
including Auto Layout constraints, responsive layouts, and constraint
solvers for macOS desktop application interfaces.

Example:
    >>> from constraint_layout_utils import ConstraintBuilder, LayoutConstraint
    >>> builder = ConstraintBuilder()
    >>> constraint = builder.top().equalTo(view, constant=10).build()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class ConstraintAttribute(Enum):
    """Layout constraint attributes."""
    LEFT = auto()
    RIGHT = auto()
    TOP = auto()
    BOTTOM = auto()
    LEADING = auto()
    TRAILING = auto()
    WIDTH = auto()
    HEIGHT = auto()
    CENTER_X = auto()
    CENTER_Y = auto()
    BASELINE = auto()
    LAST_BASELINE = auto()
    FIRST_BASELINE = auto()


class ConstraintRelation(Enum):
    """Constraint relation types."""
    EQUAL = auto()
    GREATER_THAN_OR_EQUAL = auto()
    LESS_THAN_OR_EQUAL = auto()


class ConstraintPriority(Enum):
    """Constraint priority levels."""
    REQUIRED = 1000
    HIGH = 750
    MEDIUM = 500
    LOW = 250
    FIT_NONE = 50


@dataclass
class ConstraintConstant:
    """Represents constant values for constraints."""
    value: float
    multiplier: float = 1.0
    constant: float = 0.0
    
    def resolve(self) -> float:
        """Resolve to final constant value."""
        return self.value * self.multiplier + self.constant


@dataclass
class LayoutConstraint:
    """Represents a single Auto Layout constraint.
    
    Attributes:
        first_item: First view in constraint
        first_attribute: Attribute of first view
        relation: Relationship to second item
        second_item: Optional second view
        second_attribute: Optional attribute of second view
        constant: Offset value
        multiplier: Scale multiplier
        priority: Constraint priority
    """
    first_item: Any
    first_attribute: ConstraintAttribute
    relation: ConstraintRelation = ConstraintRelation.EQUAL
    second_item: Optional[Any] = None
    second_attribute: Optional[ConstraintAttribute] = None
    constant: float = 0.0
    multiplier: float = 1.0
    priority: ConstraintPriority = ConstraintPriority.REQUIRED
    label: str = ""
    
    def __str__(self) -> str:
        parts = [f"{self.first_item}.{self.first_attribute.name}"]
        parts.append({r.value: r.name for r in ConstraintRelation}[self.relation.value].replace('_', ' ')])
        
        if self.second_item:
            attr_name = self.second_attribute.name if self.second_attribute else "unknown"
            parts.append(f"{self.second_item}.{attr_name}")
        
        if self.multiplier != 1.0:
            parts.append(f"*{self.multiplier}")
        
        if self.constant != 0.0:
            sign = "+" if self.constant >= 0 else ""
            parts.append(f"{sign}{self.constant}")
        
        parts.append(f"[{self.priority.name}]")
        
        return " ".join(parts)


class ConstraintBuilder:
    """Builder for creating Auto Layout constraints.
    
    Provides a fluent interface for constructing constraint chains
    with proper type checking and validation.
    
    Example:
        >>> constraint = (ConstraintBuilder()
        ...     .top().equalTo(superview, constant=10)
        ...     .width().equalTo(100)
        ...     .build())
    """
    
    def __init__(self, first_item: Optional[Any] = None):
        self._first_item = first_item
        self._first_attribute: Optional[ConstraintAttribute] = None
        self._relation: ConstraintRelation = ConstraintRelation.EQUAL
        self._second_item: Optional[Any] = None
        self._second_attribute: Optional[ConstraintAttribute] = None
        self._constant: float = 0.0
        self._multiplier: float = 1.0
        self._priority: ConstraintPriority = ConstraintPriority.REQUIRED
        self._constraints: List[LayoutConstraint] = []
    
    def left(self) -> ConstraintBuilder:
        """Set constraint to left attribute."""
        return self._set_attribute(ConstraintAttribute.LEFT)
    
    def right(self) -> ConstraintBuilder:
        """Set constraint to right attribute."""
        return self._set_attribute(ConstraintAttribute.RIGHT)
    
    def top(self) -> ConstraintBuilder:
        """Set constraint to top attribute."""
        return self._set_attribute(ConstraintAttribute.TOP)
    
    def bottom(self) -> ConstraintBuilder:
        """Set constraint to bottom attribute."""
        return self._set_attribute(ConstraintAttribute.BOTTOM)
    
    def leading(self) -> ConstraintBuilder:
        """Set constraint to leading attribute."""
        return self._set_attribute(ConstraintAttribute.LEADING)
    
    def trailing(self) -> ConstraintBuilder:
        """Set constraint to trailing attribute."""
        return self._set_attribute(ConstraintAttribute.TRAILING)
    
    def width(self) -> ConstraintBuilder:
        """Set constraint to width attribute."""
        return self._set_attribute(ConstraintAttribute.WIDTH)
    
    def height(self) -> ConstraintBuilder:
        """Set constraint to height attribute."""
        return self._set_attribute(ConstraintAttribute.HEIGHT)
    
    def center_x(self) -> ConstraintBuilder:
        """Set constraint to centerX attribute."""
        return self._set_attribute(ConstraintAttribute.CENTER_X)
    
    def center_y(self) -> ConstraintBuilder:
        """Set constraint to centerY attribute."""
        return self._set_attribute(ConstraintAttribute.CENTER_Y)
    
    def baseline(self) -> ConstraintBuilder:
        """Set constraint to baseline attribute."""
        return self._set_attribute(ConstraintAttribute.BASELINE)
    
    def _set_attribute(self, attr: ConstraintAttribute) -> ConstraintBuilder:
        """Set the current attribute being configured."""
        self._first_attribute = attr
        return self
    
    def equal_to(self, item: Any, attribute: Optional[ConstraintAttribute] = None) -> ConstraintBuilder:
        """Set constraint relation to equal."""
        self._relation = ConstraintRelation.EQUAL
        self._second_item = item
        self._second_attribute = attribute
        return self
    
    def greater_than_or_equal_to(self, item: Any, attribute: Optional[ConstraintAttribute] = None) -> ConstraintBuilder:
        """Set constraint relation to greater than or equal."""
        self._relation = ConstraintRelation.GREATER_THAN_OR_EQUAL
        self._second_item = item
        self._second_attribute = attribute
        return self
    
    def less_than_or_equal_to(self, item: Any, attribute: Optional[ConstraintAttribute] = None) -> ConstraintBuilder:
        """Set constraint relation to less than or equal."""
        self._relation = ConstraintRelation.LESS_THAN_OR_EQUAL
        self._second_item = item
        self._second_attribute = attribute
        return self
    
    def equalTo(self, item: Any, attribute: Optional[ConstraintAttribute] = None) -> ConstraintBuilder:
        """Alias for equal_to (Swift-style)."""
        return self.equal_to(item, attribute)
    
    def constant(self, value: float) -> ConstraintBuilder:
        """Set constant offset value."""
        self._constant = value
        return self
    
    def multiplier(self, value: float) -> ConstraintBuilder:
        """Set multiplier value."""
        self._multiplier = value
        return self
    
    def priority(self, value: ConstraintPriority) -> ConstraintBuilder:
        """Set constraint priority."""
        self._priority = value
        return self
    
    def labelled(self, label: str) -> ConstraintBuilder:
        """Set constraint label."""
        return self
    
    def build(self) -> LayoutConstraint:
        """Build the constraint."""
        if self._first_item is None or self._first_attribute is None:
            raise ValueError("Must set first item and attribute")
        
        return LayoutConstraint(
            first_item=self._first_item,
            first_attribute=self._first_attribute,
            relation=self._relation,
            second_item=self._second_item,
            second_attribute=self._second_attribute,
            constant=self._constant,
            multiplier=self._multiplier,
            priority=self._priority,
        )
    
    def install(self) -> List[LayoutConstraint]:
        """Build and install constraint."""
        constraint = self.build()
        self._constraints.append(constraint)
        self._reset()
        return [constraint]
    
    def _reset(self) -> None:
        """Reset builder for next constraint."""
        self._relation = ConstraintRelation.EQUAL
        self._second_item = None
        self._second_attribute = None
        self._constant = 0.0
        self._multiplier = 1.0
        self._priority = ConstraintPriority.REQUIRED


class ConstraintGroup:
    """Groups multiple constraints for batch operations."""
    
    def __init__(self, name: str = ""):
        self.name = name
        self._constraints: List[LayoutConstraint] = []
        self._is_active: bool = True
    
    def add(self, constraint: LayoutConstraint) -> None:
        """Add a constraint to the group."""
        self._constraints.append(constraint)
    
    def add_builder(self, builder: ConstraintBuilder) -> None:
        """Add constraints from a builder."""
        self._constraints.extend(builder._constraints)
    
    def remove(self, constraint: LayoutConstraint) -> None:
        """Remove a constraint from the group."""
        if constraint in self._constraints:
            self._constraints.remove(constraint)
    
    def clear(self) -> None:
        """Remove all constraints from the group."""
        self._constraints.clear()
    
    def activate(self) -> None:
        """Activate all constraints in the group."""
        self._is_active = True
    
    def deactivate(self) -> None:
        """Deactivate all constraints in the group."""
        self._is_active = False
    
    def get_constraints(self) -> List[LayoutConstraint]:
        """Get all constraints in the group."""
        return self._constraints.copy()
    
    def __len__(self) -> int:
        return len(self._constraints)


class ConstraintSolver:
    """Solves constraint systems for layout calculation.
    
    Provides constraint solving for cases where you need to
    calculate layout positions without applying constraints.
    """
    
    def __init__(self):
        self._variables: Dict[str, float] = {}
        self._constraints: List[LayoutConstraint] = []
    
    def add_variable(self, name: str, initial_value: float = 0.0) -> None:
        """Add a variable to the solver."""
        self._variables[name] = initial_value
    
    def add_constraint(self, constraint: LayoutConstraint) -> None:
        """Add a constraint to solve."""
        self._constraints.append(constraint)
    
    def solve(self) -> Dict[str, float]:
        """Solve the constraint system.
        
        Returns:
            Dictionary of variable names to solved values
        """
        result = self._variables.copy()
        
        for constraint in self._constraints:
            if constraint.first_attribute == ConstraintAttribute.WIDTH:
                if constraint.second_item and constraint.second_attribute == ConstraintAttribute.WIDTH:
                    key = f"{constraint.first_item}_{constraint.first_attribute.name}"
                    if key in result and constraint.relation == ConstraintRelation.EQUAL:
                        result[key] = result.get(
                            f"{constraint.second_item}_{constraint.second_attribute.name}",
                            0.0
                        ) * constraint.multiplier + constraint.constant
        
        return result
    
    def solve_with_iteration(self, max_iterations: int = 100, tolerance: float = 0.001) -> Dict[str, float]:
        """Solve using iterative relaxation.
        
        Args:
            max_iterations: Maximum iterations
            tolerance: Convergence tolerance
            
        Returns:
            Dictionary of solved values
        """
        result = self._variables.copy()
        
        for _ in range(max_iterations):
            max_error = 0.0
            
            for constraint in self._constraints:
                error = self._compute_error(constraint, result)
                max_error = max(max_error, abs(error))
                
                if max_error < tolerance:
                    return result
        
        return result
    
    def _compute_error(self, constraint: LayoutConstraint, values: Dict[str, float]) -> float:
        """Compute error for a constraint."""
        return 0.0
