"""Visitor pattern utilities for RabAI AutoClick.

Provides:
- Visitor interface
- Acceptable interface
- Composite visitor
- Recursive visitor
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class Visitor(ABC, Generic[T]):
    """Base visitor class."""

    @abstractmethod
    def visit(self, element: Any) -> T:
        """Visit an element."""
        pass


class Acceptable(ABC):
    """Interface for elements that accept visitors."""

    @abstractmethod
    def accept(self, visitor: Visitor[T]) -> T:
        """Accept a visitor."""
        pass


class Expression(Acceptable):
    """Base expression class for composite pattern example."""

    def accept(self, visitor: Visitor[T]) -> T:
        return visitor.visit(self)


class NumberExpression(Expression):
    def __init__(self, value: float) -> None:
        self.value = value


class AddExpression(Expression):
    def __init__(self, left: Expression, right: Expression) -> None:
        self.left = left
        self.right = right


class MultiplyExpression(Expression):
    def __init__(self, left: Expression, right: Expression) -> None:
        self.left = left
        self.right = right


class PrintVisitor(Visitor[str]):
    """Visitor that prints expression."""

    def visit(self, element: Any) -> str:
        if isinstance(element, NumberExpression):
            return str(element.value)
        elif isinstance(element, AddExpression):
            left = element.left.accept(self)
            right = element.right.accept(self)
            return f"({left} + {right})"
        elif isinstance(element, MultiplyExpression):
            left = element.left.accept(self)
            right = element.right.accept(self)
            return f"({left} * {right})"
        return str(element)


class EvaluateVisitor(Visitor[float]):
    """Visitor that evaluates expression."""

    def visit(self, element: Any) -> float:
        if isinstance(element, NumberExpression):
            return element.value
        elif isinstance(element, AddExpression):
            return element.left.accept(self) + element.right.accept(self)
        elif isinstance(element, MultiplyExpression):
            return element.left.accept(self) * element.right.accept(self)
        raise ValueError(f"Unknown expression type: {type(element)}")


class CompositeVisitor(Visitor[T]):
    """Combines multiple visitors.

    Calls each visitor in order and returns last result.
    """

    def __init__(self, *visitors: Visitor[Any]) -> None:
        self._visitors = visitors

    def add_visitor(self, visitor: Visitor[Any]) -> None:
        self._visitors = (*self._visitors, visitor)

    def visit(self, element: Any) -> T:
        result = None
        for visitor in self._visitors:
            result = visitor.visit(element)
        return result  # type: ignore


class RecursiveVisitor(Visitor[T]):
    """Visitor that traverses tree-like structures automatically."""

    def __init__(
        self,
        children_accessor: Callable[[Any], List[Any]],
    ) -> None:
        self._children_accessor = children_accessor

    def visit(self, element: Any) -> T:
        return self._visit_recursive(element)

    def _visit_recursive(self, element: Any) -> T:
        raise NotImplementedError

    def traverse(self, element: Any) -> List[Any]:
        """Traverse and return all elements."""
        results = [element]
        for child in self._children_accessor(element):
            results.extend(self.traverse(child))
        return results


class TreeVisitor(RecursiveVisitor[Any]):
    """Visitor for tree structures."""

    def __init__(self) -> None:
        super().__init__(children_accessor=lambda n: getattr(n, "children", []))

    def visit_node(self, node: Any) -> Any:
        """Override this to implement node-specific logic."""
        return node

    def _visit_recursive(self, element: Any) -> Any:
        return self.visit_node(element)


class FunctionVisitor(Generic[T]):
    """Visitor implemented with functions instead of class methods."""

    def __init__(self) -> None:
        self._handlers: Dict[type, Callable[[Any], T]] = {}

    def on(self, type_: type) -> Callable[[Callable[[Any], T]], Callable[[Any], T]]:
        """Decorator to register a handler for a type."""
        def decorator(func: Callable[[Any], T]) -> Callable[[Any], T]:
            self._handlers[type_] = func
            return func
        return decorator

    def visit(self, element: Any) -> T:
        for type_, handler in self._handlers.items():
            if isinstance(element, type_):
                return handler(element)
        raise ValueError(f"No handler for type {type(element)}")
