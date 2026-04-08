"""Template Method Action Module.

Provides template method pattern for
algorithm骨架 definition.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TemplateStep:
    """Step in template method."""
    step_id: str
    name: str
    func: Callable
    required: bool = True


class TemplateMethod:
    """Template method implementation."""

    def __init__(self, template_id: str, name: str):
        self.template_id = template_id
        self.name = name
        self._steps: List[TemplateStep] = []

    def add_step(self, step: TemplateStep) -> None:
        """Add step to template."""
        self._steps.append(step)

    def execute(self, context: Optional[Dict] = None) -> tuple[bool, List[Dict]]:
        """Execute template method."""
        results = []
        context = context or {}

        for step in self._steps:
            try:
                result = step.func(context)
                results.append({
                    "step_id": step.step_id,
                    "name": step.name,
                    "success": True,
                    "result": result
                })
            except Exception as e:
                if step.required:
                    results.append({
                        "step_id": step.step_id,
                        "name": step.name,
                        "success": False,
                        "error": str(e)
                    })
                    return False, results
                else:
                    results.append({
                        "step_id": step.step_id,
                        "name": step.name,
                        "success": False,
                        "error": str(e),
                        "skipped": True
                    })

        return True, results


class TemplateMethodAction(BaseAction):
    """Action for template method operations."""

    def __init__(self):
        super().__init__("template_method")
        self._templates: Dict[str, TemplateMethod] = {}

    def execute(self, params: Dict) -> ActionResult:
        """Execute template method action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_step":
                return self._add_step(params)
            elif operation == "execute":
                return self._execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create template."""
        template_id = params.get("template_id", "")
        template = TemplateMethod(template_id, params.get("name", ""))
        self._templates[template_id] = template
        return ActionResult(success=True, data={"template_id": template_id})

    def _add_step(self, params: Dict) -> ActionResult:
        """Add step to template."""
        template_id = params.get("template_id", "")
        template = self._templates.get(template_id)

        if not template:
            return ActionResult(success=False, message="Template not found")

        def default_func(ctx):
            return {}

        step = TemplateStep(
            step_id=params.get("step_id", ""),
            name=params.get("name", ""),
            func=params.get("func") or default_func,
            required=params.get("required", True)
        )

        template.add_step(step)
        return ActionResult(success=True)

    def _execute(self, params: Dict) -> ActionResult:
        """Execute template."""
        template_id = params.get("template_id", "")
        template = self._templates.get(template_id)

        if not template:
            return ActionResult(success=False, message="Template not found")

        success, results = template.execute(params.get("context"))
        return ActionResult(success=success, data={
            "success": success,
            "steps": results
        })
