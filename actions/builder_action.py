"""Builder Pattern Action Module.

Provides builder pattern for complex
object construction.
"""

import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BuilderStep:
    """Step in builder."""
    step_id: str
    name: str
    value: Any


class Builder:
    """Builder implementation."""
    def __init__(self, builder_id: str, name: str):
        self.builder_id = builder_id
        self.name = name
        self._steps: List[BuilderStep] = []
        self._result: Optional[Dict] = None

    def set(self, name: str, value: Any) -> 'Builder':
        """Set a property."""
        self._steps.append(BuilderStep(
            step_id=f"{len(self._steps)}",
            name=name,
            value=value
        ))
        return self

    def build(self) -> Dict:
        """Build the result."""
        self._result = {}
        for step in self._steps:
            self._result[step.name] = step.value
        return self._result


class BuilderManager:
    """Manages builder pattern."""

    def __init__(self):
        self._builders: Dict[str, Builder] = {}

    def create_builder(self, name: str) -> str:
        """Create a builder."""
        builder_id = f"build_{name.lower().replace(' ', '_')}"
        self._builders[builder_id] = Builder(builder_id, name)
        return builder_id

    def get_builder(self, builder_id: str) -> Optional[Builder]:
        """Get builder."""
        return self._builders.get(builder_id)


class BuilderPatternAction(BaseAction):
    """Action for builder pattern operations."""

    def __init__(self):
        super().__init__("builder")
        self._manager = BuilderManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute builder action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "set":
                return self._set(params)
            elif operation == "build":
                return self._build(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create builder."""
        builder_id = self._manager.create_builder(params.get("name", ""))
        return ActionResult(success=True, data={"builder_id": builder_id})

    def _set(self, params: Dict) -> ActionResult:
        """Set property."""
        builder = self._manager.get_builder(params.get("builder_id", ""))
        if not builder:
            return ActionResult(success=False, message="Builder not found")

        builder.set(params.get("name", ""), params.get("value"))
        return ActionResult(success=True)

    def _build(self, params: Dict) -> ActionResult:
        """Build result."""
        builder = self._manager.get_builder(params.get("builder_id", ""))
        if not builder:
            return ActionResult(success=False, message="Builder not found")

        result = builder.build()
        return ActionResult(success=True, data={"result": result})
