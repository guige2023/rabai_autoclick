"""Visitor Pattern Action Module.

Provides visitor pattern for operations
on object structures.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class VisitableElement:
    """Element that can be visited."""
    element_id: str
    name: str
    accept: Callable


@dataclass
class Visitor:
    """Visitor implementation."""
    visitor_id: str
    name: str
    visit_func: Callable


class VisitorManager:
    """Manages visitor pattern."""

    def __init__(self):
        self._elements: Dict[str, VisitableElement] = {}
        self._visitors: Dict[str, Visitor] = {}

    def register_element(
        self,
        name: str,
        properties: Optional[Dict] = None
    ) -> str:
        """Register a visitable element."""
        element_id = f"elem_{name.lower().replace(' ', '_')}"
        properties = properties or {}

        def default_accept(visitor):
            return visitor.visit_func(properties)

        element = VisitableElement(
            element_id=element_id,
            name=name,
            accept=default_accept
        )

        self._elements[element_id] = element
        return element_id

    def register_visitor(
        self,
        name: str,
        visit_func: Callable
    ) -> str:
        """Register a visitor."""
        visitor_id = f"vis_{name.lower().replace(' ', '_')}"

        visitor = Visitor(
            visitor_id=visitor_id,
            name=name,
            visit_func=visit_func
        )

        self._visitors[visitor_id] = visitor
        return visitor_id

    def accept(self, element_id: str, visitor_id: str) -> Any:
        """Element accepts visitor."""
        element = self._elements.get(element_id)
        visitor = self._visitors.get(visitor_id)

        if not element or not visitor:
            return None

        return element.accept(visitor)


class VisitorPatternAction(BaseAction):
    """Action for visitor pattern operations."""

    def __init__(self):
        super().__init__("visitor")
        self._manager = VisitorManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute visitor action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register_element":
                return self._register_element(params)
            elif operation == "register_visitor":
                return self._register_visitor(params)
            elif operation == "accept":
                return self._accept(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register_element(self, params: Dict) -> ActionResult:
        """Register element."""
        element_id = self._manager.register_element(
            name=params.get("name", ""),
            properties=params.get("properties")
        )
        return ActionResult(success=True, data={"element_id": element_id})

    def _register_visitor(self, params: Dict) -> ActionResult:
        """Register visitor."""
        def default_visit(props):
            return props

        visitor_id = self._manager.register_visitor(
            name=params.get("name", ""),
            visit_func=params.get("visit_func") or default_visit
        )
        return ActionResult(success=True, data={"visitor_id": visitor_id})

    def _accept(self, params: Dict) -> ActionResult:
        """Accept visitor."""
        result = self._manager.accept(
            params.get("element_id", ""),
            params.get("visitor_id", "")
        )
        return ActionResult(success=True, data={"result": result})
