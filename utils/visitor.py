"""Visitor pattern utilities for RabAI AutoClick.

Provides:
- Visitor pattern implementation
- Element traversal
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, TypeVar


T = TypeVar("T")


class Element(ABC):
    """Base class for elements that can be visited."""

    @abstractmethod
    def accept(self, visitor: 'Visitor') -> Any:
        """Accept a visitor.

        Args:
            visitor: Visitor instance.

        Returns:
            Visitor result.
        """
        pass


class Visitor(ABC):
    """Base visitor class."""

    @abstractmethod
    def visit(self, element: Element) -> Any:
        """Visit an element.

        Args:
            element: Element to visit.

        Returns:
            Visitor result.
        """
        pass


class ConcreteElement(Element):
    """Concrete element implementation."""

    def __init__(
        self,
        name: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize element.

        Args:
            name: Element name.
            data: Optional element data.
        """
        self.name = name
        self.data = data or {}

    def accept(self, visitor: Visitor) -> Any:
        """Accept visitor."""
        return visitor.visit(self)


class CompositeElement(Element):
    """Element that contains child elements."""

    def __init__(
        self,
        name: str,
        children: Optional[List[Element]] = None,
    ) -> None:
        """Initialize composite.

        Args:
            name: Element name.
            children: Child elements.
        """
        self.name = name
        self.children = children or []

    def add(self, child: Element) -> 'CompositeElement':
        """Add child element.

        Args:
            child: Child to add.

        Returns:
            Self for chaining.
        """
        self.children.append(child)
        return self

    def remove(self, child: Element) -> bool:
        """Remove child element.

        Args:
            child: Child to remove.

        Returns:
            True if removed.
        """
        if child in self.children:
            self.children.remove(child)
            return True
        return False

    def accept(self, visitor: Visitor) -> Any:
        """Accept visitor."""
        return visitor.visit(self)


class TreeBuilder:
    """Build tree structures from hierarchical data."""

    def __init__(self) -> None:
        self._root: Optional[CompositeElement] = None
        self._current: Optional[CompositeElement] = None

    def root(self, name: str) -> 'TreeBuilder':
        """Set root element.

        Args:
            name: Root name.

        Returns:
            Self for chaining.
        """
        self._root = CompositeElement(name)
        self._current = self._root
        return self

    def node(self, name: str, **data: Any) -> 'TreeBuilder':
        """Add node to current element.

        Args:
            name: Node name.
            **data: Node data.

        Returns:
            Self for chaining.
        """
        if self._current:
            element = ConcreteElement(name, data)
            self._current.add(element)
        return self

    def composite(self, name: str) -> 'TreeBuilder':
        """Start a new composite element.

        Args:
            name: Composite name.

        Returns:
            Self for chaining.
        """
        if self._current:
            composite = CompositeElement(name)
            self._current.add(composite)
            self._current = composite
        return self

    def end(self) -> 'TreeBuilder':
        """End current composite and move up.

        Returns:
            Self for chaining.
        """
        if self._current and self._current.name != (self._root.name if self._root else ""):
            # Would need parent reference - simplified for demo
            pass
        return self

    def build(self) -> Optional[CompositeElement]:
        """Build tree.

        Returns:
            Root element.
        """
        return self._root


class VisitorRegistry:
    """Registry for visitors by element type."""

    def __init__(self) -> None:
        self._visitors: Dict[type, Callable] = {}

    def register(self, element_type: type, visitor: Callable) -> None:
        """Register visitor for element type.

        Args:
            element_type: Element class.
            visitor: Visitor function.
        """
        self._visitors[element_type] = visitor

    def get_visitor(self, element_type: type) -> Optional[Callable]:
        """Get visitor for element type.

        Args:
            element_type: Element class.

        Returns:
            Visitor function or None.
        """
        return self._visitors.get(element_type)


class DispatchingVisitor(Visitor):
    """Visitor that dispatches to registered handlers."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Callable] = {}

    def register(self, element_name: str, handler: Callable[[Element], Any]) -> None:
        """Register handler for element.

        Args:
            element_name: Name of element.
            handler: Handler function.
        """
        self._handlers[element_name] = handler

    def visit(self, element: Element) -> Any:
        """Visit element with dispatch."""
        handler = self._handlers.get(element.name)
        if handler:
            return handler(element)
        return None


class Traverser:
    """Traverse element trees."""

    @staticmethod
    def traverse(
        element: Element,
        pre_visit: Optional[Callable[[Element], None]] = None,
        post_visit: Optional[Callable[[Element], None]] = None,
    ) -> None:
        """Traverse tree with callbacks.

        Args:
            element: Root element.
            pre_visit: Called before visiting children.
            post_visit: Called after visiting children.
        """
        if pre_visit:
            pre_visit(element)

        if isinstance(element, CompositeElement):
            for child in element.children:
                Traverser.traverse(child, pre_visit, post_visit)

        if post_visit:
            post_visit(element)

    @staticmethod
    def flatten(element: Element) -> List[Element]:
        """Flatten tree to list.

        Args:
            element: Root element.

        Returns:
            List of all elements.
        """
        result = [element]
        if isinstance(element, CompositeElement):
            for child in element.children:
                result.extend(Traverser.flatten(child))
        return result

    @staticmethod
    def find(element: Element, predicate: Callable[[Element], bool]) -> Optional[Element]:
        """Find element by predicate.

        Args:
            element: Root to search.
            predicate: Search predicate.

        Returns:
            Found element or None.
        """
        if predicate(element):
            return element

        if isinstance(element, CompositeElement):
            for child in element.children:
                found = Traverser.find(child, predicate)
                if found:
                    return found

        return None

    @staticmethod
    def depth(element: Element) -> int:
        """Get depth of tree.

        Args:
            element: Root element.

        Returns:
            Maximum depth.
        """
        if not isinstance(element, CompositeElement) or not element.children:
            return 1

        max_child_depth = 0
        for child in element.children:
            child_depth = Traverser.depth(child)
            max_child_depth = max(max_child_depth, child_depth)

        return 1 + max_child_depth