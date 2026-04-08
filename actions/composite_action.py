"""Composite Action Module.

Provides composite pattern for
grouping actions into trees.
"""

import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NodeType(Enum):
    """Node type in composite."""
    LEAF = "leaf"
    COMPOSITE = "composite"


@dataclass
class ActionNode:
    """Node in action tree."""
    node_id: str
    name: str
    node_type: NodeType
    action: Optional[callable] = None
    children: List['ActionNode'] = field(default_factory=list)


class CompositeAction:
    """Composite action implementation."""

    def __init__(self, composite_id: str, name: str):
        self.composite_id = composite_id
        self.name = name
        self.root: Optional[ActionNode] = None

    def add_node(
        self,
        parent_id: Optional[str],
        node: ActionNode
    ) -> bool:
        """Add node to tree."""
        if parent_id is None:
            if self.root is None:
                self.root = node
                return True
            return False

        return self._add_to_parent(self.root, parent_id, node)

    def _add_to_parent(
        self,
        current: ActionNode,
        parent_id: str,
        node: ActionNode
    ) -> bool:
        """Recursively find parent and add node."""
        if current.node_id == parent_id and current.node_type == NodeType.COMPOSITE:
            current.children.append(node)
            return True

        for child in current.children:
            if self._add_to_parent(child, parent_id, node):
                return True

        return False

    def execute(self, context: Optional[Dict] = None) -> List[Any]:
        """Execute all actions."""
        if not self.root:
            return []

        return self._execute_node(self.root, context or {})

    def _execute_node(self, node: ActionNode, context: Dict) -> List[Any]:
        """Execute node and children."""
        results = []

        if node.action:
            try:
                result = node.action(context)
                results.append(result)
            except Exception:
                pass

        if node.node_type == NodeType.COMPOSITE:
            for child in node.children:
                results.extend(self._execute_node(child, context))

        return results


class CompositeActionModule(BaseAction):
    """Action for composite operations."""

    def __init__(self):
        super().__init__("composite")
        self._composites: Dict[str, CompositeAction] = {}

    def execute(self, params: Dict) -> ActionResult:
        """Execute composite action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_node":
                return self._add_node(params)
            elif operation == "execute":
                return self._execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create composite."""
        composite_id = params.get("composite_id", "")
        composite = CompositeAction(composite_id, params.get("name", ""))
        self._composites[composite_id] = composite
        return ActionResult(success=True, data={"composite_id": composite_id})

    def _add_node(self, params: Dict) -> ActionResult:
        """Add node."""
        composite_id = params.get("composite_id", "")
        composite = self._composites.get(composite_id)

        if not composite:
            return ActionResult(success=False, message="Composite not found")

        def default_action(ctx):
            return {}

        node = ActionNode(
            node_id=params.get("node_id", ""),
            name=params.get("name", ""),
            node_type=NodeType(params.get("node_type", "leaf")),
            action=params.get("action") or default_action
        )

        success = composite.add_node(params.get("parent_id"), node)
        return ActionResult(success=success)

    def _execute(self, params: Dict) -> ActionResult:
        """Execute composite."""
        composite_id = params.get("composite_id", "")
        composite = self._composites.get(composite_id)

        if not composite:
            return ActionResult(success=False, message="Composite not found")

        results = composite.execute(params.get("context"))
        return ActionResult(success=True, data={"results": results})
