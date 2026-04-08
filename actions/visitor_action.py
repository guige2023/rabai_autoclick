"""Visitor action module for RabAI AutoClick.

Provides visitor pattern implementation:
- Visitor: Abstract visitor interface
- Element: Accept visitor interface
- ConcreteVisitor: Specific visitor implementations
- ObjectStructure: Collection of elements
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")


class Visitable(ABC):
    """Interface for elements that accept visitors."""

    @abstractmethod
    def accept(self, visitor: "Visitor") -> Any:
        """Accept a visitor."""
        pass


class Visitor(ABC):
    """Abstract visitor interface."""

    @abstractmethod
    def visit(self, element: "Visitable") -> Any:
        """Visit an element."""
        pass


class ConcreteVisitor(Visitor):
    """Concrete visitor implementation."""

    def __init__(self, visit_fn: Callable[[Any], Any]):
        self._visit_fn = visit_fn

    def visit(self, element: Visitable) -> Any:
        """Visit an element."""
        return self._visit_fn(element)


class DataNode(Visitable):
    """A data node that can be visited."""

    def __init__(self, node_id: str, node_type: str, data: Any):
        self.node_id = node_id
        self.node_type = node_type
        self.data = data
        self.children: List["DataNode"] = []

    def add_child(self, child: "DataNode") -> None:
        """Add a child node."""
        self.children.append(child)

    def accept(self, visitor: Visitor) -> Any:
        """Accept a visitor."""
        return visitor.visit(self)


class TreeVisitor(Visitor):
    """Visitor for traversing tree structures."""

    def __init__(self):
        self._results: List[Any] = []
        self._depth: int = 0

    def visit(self, element: Visitable) -> Any:
        """Visit a data node."""
        if isinstance(element, DataNode):
            return self._visit_node(element)
        return None

    def _visit_node(self, node: DataNode) -> Any:
        """Visit a data node and its children."""
        result = {
            "node_id": node.node_id,
            "node_type": node.node_type,
            "depth": self._depth,
            "data": node.data,
        }
        self._results.append(result)

        self._depth += 1
        for child in node.children:
            child.accept(self)
        self._depth -= 1

        return result

    def get_results(self) -> List[Any]:
        """Get all visit results."""
        return self._results.copy()


class ValidationVisitor(Visitor):
    """Visitor for validating nodes."""

    def __init__(self, validator: Callable[[DataNode], bool]):
        self._validator = validator
        self._valid: List[str] = []
        self._invalid: List[str] = []

    def visit(self, element: Visitable) -> Any:
        """Visit and validate."""
        if isinstance(element, DataNode):
            return self._visit_node(element)
        return None

    def _visit_node(self, node: DataNode) -> Dict[str, Any]:
        """Validate a node."""
        is_valid = self._validator(node)
        if is_valid:
            self._valid.append(node.node_id)
        else:
            self._invalid.append(node.node_id)

        for child in node.children:
            child.accept(self)

        return {"node_id": node.node_id, "valid": is_valid}

    def get_valid_nodes(self) -> List[str]:
        """Get valid node IDs."""
        return self._valid.copy()

    def get_invalid_nodes(self) -> List[str]:
        """Get invalid node IDs."""
        return self._invalid.copy()


class TransformationVisitor(Visitor):
    """Visitor for transforming nodes."""

    def __init__(self, transformer: Callable[[DataNode], DataNode]):
        self._transformer = transformer

    def visit(self, element: Visitable) -> Any:
        """Visit and transform."""
        if isinstance(element, DataNode):
            return self._visit_node(element)
        return None

    def _visit_node(self, node: DataNode) -> DataNode:
        """Transform a node and its children."""
        new_node = self._transformer(node)
        new_children = []
        for child in node.children:
            new_children.append(self._visit_node(child))
        new_node.children = new_children
        return new_node


class ObjectStructure:
    """Collection of elements."""

    def __init__(self):
        self._elements: List[Visitable] = []

    def add(self, element: Visitable) -> None:
        """Add an element."""
        self._elements.append(element)

    def remove(self, element: Visitable) -> bool:
        """Remove an element."""
        try:
            self._elements.remove(element)
            return True
        except ValueError:
            return False

    def accept(self, visitor: Visitor) -> List[Any]:
        """Accept a visitor on all elements."""
        results = []
        for element in self._elements:
            result = element.accept(visitor)
            if result is not None:
                results.append(result)
        return results

    def get_all(self) -> List[Visitable]:
        """Get all elements."""
        return self._elements.copy()


class VisitorAction(BaseAction):
    """Visitor pattern action."""
    action_type = "visitor"
    display_name = "访问者模式"
    description = "访问者模式实现"

    def __init__(self):
        super().__init__()
        self._structure = ObjectStructure()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "traverse")

            if operation == "add":
                return self._add_node(params)
            elif operation == "traverse":
                return self._traverse(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "transform":
                return self._transform(params)
            elif operation == "query":
                return self._query(params)
            elif operation == "structure":
                return self._get_structure()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Visitor error: {str(e)}")

    def _add_node(self, params: Dict[str, Any]) -> ActionResult:
        """Add a node to structure."""
        node_id = params.get("node_id", str(uuid.uuid4()))
        node_type = params.get("node_type", "default")
        data = params.get("data", {})
        parent_id = params.get("parent_id")

        node = DataNode(node_id=node_id, node_type=node_type, data=data)

        if parent_id:
            parent = self._find_node(self._structure.get_all(), parent_id)
            if parent:
                parent.add_child(node)
            else:
                return ActionResult(success=False, message=f"Parent node not found: {parent_id}")
        else:
            self._structure.add(node)

        return ActionResult(success=True, message=f"Node added: {node_id}", data={"node_id": node_id})

    def _find_node(self, elements: List[Visitable], node_id: str) -> Optional[DataNode]:
        """Find a node by ID."""
        for elem in elements:
            if isinstance(elem, DataNode):
                if elem.node_id == node_id:
                    return elem
                found = self._find_node(elem.children, node_id)
                if found:
                    return found
        return None

    def _traverse(self, params: Dict[str, Any]) -> ActionResult:
        """Traverse the structure."""
        visitor = TreeVisitor()
        results = self._structure.accept(visitor)

        return ActionResult(
            success=True,
            message=f"Traversed {len(results)} nodes",
            data={"nodes": results, "count": len(results)},
        )

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate nodes."""
        validator_fn = params.get("validator")

        if not validator_fn:
            def default_validator(node: DataNode) -> bool:
                return bool(node.data)
            validator_fn = default_validator

        visitor = ValidationVisitor(validator=validator_fn)
        self._structure.accept(visitor)

        valid = visitor.get_valid_nodes()
        invalid = visitor.get_invalid_nodes()

        return ActionResult(
            success=len(invalid) == 0,
            message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
            data={"valid": valid, "invalid": invalid},
        )

    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        """Transform nodes."""
        transformer_fn = params.get("transformer")

        if not transformer_fn:
            return ActionResult(success=False, message="transformer is required")

        visitor = TransformationVisitor(transformer=transformer_fn)
        new_elements = self._structure.accept(visitor)

        return ActionResult(
            success=True,
            message=f"Transformed {len(new_elements)} nodes",
            data={"count": len(new_elements)},
        )

    def _query(self, params: Dict[str, Any]) -> ActionResult:
        """Query nodes."""
        node_type = params.get("node_type")
        predicate = params.get("predicate")

        elements = self._structure.get_all()
        results = []

        def search(nodes: List[Visitable]):
            for elem in nodes:
                if isinstance(elem, DataNode):
                    match = True
                    if node_type and elem.node_type != node_type:
                        match = False
                    if predicate and not predicate(elem):
                        match = False
                    if match:
                        results.append({
                            "node_id": elem.node_id,
                            "node_type": elem.node_type,
                            "data": elem.data,
                        })
                    search(elem.children)

        search(elements)

        return ActionResult(
            success=True,
            message=f"Found {len(results)} nodes",
            data={"results": results},
        )

    def _get_structure(self) -> ActionResult:
        """Get structure overview."""
        elements = self._structure.get_all()
        return ActionResult(
            success=True,
            message=f"Structure has {len(elements)} root elements",
            data={"root_count": len(elements)},
        )
