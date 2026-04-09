"""Workflow template action module for RabAI AutoClick.

Provides workflow template operations:
- WorkflowTemplateBuilderAction: Build workflow templates
- WorkflowTemplateExecutorAction: Execute from templates
- WorkflowTemplateLibraryAction: Manage template library
- WorkflowTemplateVersioningAction: Version control for templates
- WorkflowTemplateValidatorAction: Validate workflow templates
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WorkflowTemplateBuilderAction(BaseAction):
    """Build workflow templates from reusable components."""
    action_type = "workflow_template_builder"
    display_name = "工作流模板构建器"
    description = "从可复用组件构建工作流模板"

    def __init__(self) -> None:
        super().__init__()
        self._templates: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "build":
                return self._build_template(params)
            elif action == "add_step":
                return self._add_step(params)
            elif action == "add_branch":
                return self._add_branch(params)
            elif action == "add_loop":
                return self._add_loop(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Template building failed: {e}")

    def _build_template(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        description = params.get("description", "")
        steps = params.get("steps", [])
        variables = params.get("variables", {})
        if not name:
            return ActionResult(success=False, message="name is required")

        template = {
            "id": str(uuid.uuid4()),
            "name": name,
            "description": description,
            "steps": steps,
            "variables": variables,
            "version": "1.0.0",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "step_count": len(steps),
        }
        self._templates[name] = template
        return ActionResult(success=True, message=f"Template '{name}' built with {len(steps)} steps", data=template)

    def _add_step(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        step = params.get("step", {})
        if template_name not in self._templates:
            return ActionResult(success=False, message=f"Template not found: {template_name}")
        self._templates[template_name]["steps"].append(step)
        return ActionResult(success=True, message="Step added", data=self._templates[template_name])

    def _add_branch(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        condition = params.get("condition", "")
        branch_steps = params.get("branch_steps", [])
        if template_name not in self._templates:
            return ActionResult(success=False, message=f"Template not found: {template_name}")
        branch = {
            "type": "branch",
            "condition": condition,
            "steps": branch_steps,
            "id": str(uuid.uuid4()),
        }
        self._templates[template_name]["steps"].append(branch)
        return ActionResult(success=True, message=f"Branch added: {condition[:50]}")

    def _add_loop(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        loop_type = params.get("loop_type", "for_each")
        loop_items = params.get("loop_items", [])
        loop_steps = params.get("loop_steps", [])
        if template_name not in self._templates:
            return ActionResult(success=False, message=f"Template not found: {template_name}")
        loop = {
            "type": "loop",
            "loop_type": loop_type,
            "items": loop_items,
            "steps": loop_steps,
            "id": str(uuid.uuid4()),
        }
        self._templates[template_name]["steps"].append(loop)
        return ActionResult(success=True, message=f"Loop added: {loop_type}")


class WorkflowTemplateExecutorAction(BaseAction):
    """Execute workflows from templates."""
    action_type = "workflow_template_executor"
    display_name = "工作流模板执行器"
    description = "从模板执行工作流"

    def __init__(self) -> None:
        super().__init__()
        self._executions: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "instantiate":
                return self._instantiate(params)
            elif action == "run":
                return self._run(params)
            elif action == "status":
                return self._get_status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Template execution failed: {e}")

    def _instantiate(self, params: Dict[str, Any]) -> ActionResult:
        template = params.get("template", {})
        variables = params.get("variables", {})
        execution_id = str(uuid.uuid4())
        self._executions[execution_id] = {
            "id": execution_id,
            "template_name": template.get("name", ""),
            "variables": variables,
            "state": "READY",
            "current_step": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
        }
        return ActionResult(success=True, message=f"Workflow instantiated: {execution_id[:8]}", data={"execution_id": execution_id})

    def _run(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id not in self._executions:
            return ActionResult(success=False, message="Execution not found")
        self._executions[execution_id]["state"] = "RUNNING"
        self._executions[execution_id]["started_at"] = datetime.now(timezone.utc).isoformat()
        return ActionResult(success=True, message=f"Workflow running: {execution_id[:8]}", data=self._executions[execution_id])

    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        execution_id = params.get("execution_id", "")
        if execution_id in self._executions:
            return ActionResult(success=True, message="Execution status", data=self._executions[execution_id])
        return ActionResult(success=False, message="Execution not found")


class WorkflowTemplateLibraryAction(BaseAction):
    """Manage workflow template library."""
    action_type = "workflow_template_library"
    display_name = "工作流模板库"
    description = "管理工作流模板库"

    def __init__(self) -> None:
        super().__init__()
        self._library: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "add":
                return self._add_template(params)
            elif action == "remove":
                return self._remove_template(params)
            elif action == "list":
                return self._list_templates(params)
            elif action == "search":
                return self._search_templates(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Template library failed: {e}")

    def _add_template(self, params: Dict[str, Any]) -> ActionResult:
        template = params.get("template", {})
        name = template.get("name", "")
        if not name:
            return ActionResult(success=False, message="template.name is required")
        self._library[name] = template
        return ActionResult(success=True, message=f"Template '{name}' added to library", data={"count": len(self._library)})

    def _remove_template(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        if name in self._library:
            del self._library[name]
            return ActionResult(success=True, message=f"Template '{name}' removed")
        return ActionResult(success=False, message=f"Template not found: {name}")

    def _list_templates(self, params: Dict[str, Any]) -> ActionResult:
        category = params.get("category", None)
        templates = list(self._library.values())
        if category:
            templates = [t for t in templates if t.get("category") == category]
        return ActionResult(success=True, message=f"{len(templates)} templates", data={"templates": templates})

    def _search_templates(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "").lower()
        results = [t for t in self._library.values() if query in t.get("name", "").lower() or query in t.get("description", "").lower()]
        return ActionResult(success=True, message=f"Found {len(results)} templates", data={"results": results})


class WorkflowTemplateVersioningAction(BaseAction):
    """Version control for workflow templates."""
    action_type = "workflow_template_versioning"
    display_name = "工作流模板版本控制"
    description = "工作流模板版本控制"

    def __init__(self) -> None:
        super().__init__()
        self._versions: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "commit":
                return self._commit_version(params)
            elif action == "list_versions":
                return self._list_versions(params)
            elif action == "checkout":
                return self._checkout(params)
            elif action == "diff":
                return self._diff(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Template versioning failed: {e}")

    def _commit_version(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        template = params.get("template", {})
        version = params.get("version", "1.0.0")
        message = params.get("message", "")
        if not template_name:
            return ActionResult(success=False, message="template_name is required")
        self._versions.setdefault(template_name, []).append({
            "version": version,
            "template": template,
            "message": message,
            "committed_at": datetime.now(timezone.utc).isoformat(),
        })
        return ActionResult(success=True, message=f"Version {version} committed for '{template_name}'", data={"version": version})

    def _list_versions(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        versions = self._versions.get(template_name, [])
        return ActionResult(success=True, message=f"{len(versions)} versions", data={"versions": versions})

    def _checkout(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        version = params.get("version", "")
        versions = self._versions.get(template_name, [])
        selected = next((v for v in versions if v["version"] == version), None)
        if not selected:
            return ActionResult(success=False, message=f"Version {version} not found")
        return ActionResult(success=True, message=f"Checked out version {version}", data=selected["template"])

    def _diff(self, params: Dict[str, Any]) -> ActionResult:
        template_name = params.get("template_name", "")
        v1 = params.get("version1", "")
        v2 = params.get("version2", "")
        versions = self._versions.get(template_name, [])
        ver1 = next((v for v in versions if v["version"] == v1), None)
        ver2 = next((v for v in versions if v["version"] == v2), None)
        if not ver1 or not ver2:
            return ActionResult(success=False, message="One or both versions not found")
        diff = {"added": [], "removed": [], "changed": []}
        return ActionResult(success=True, message=f"Diff v{v1} vs v{v2}", data=diff)


class WorkflowTemplateValidatorAction(BaseAction):
    """Validate workflow templates."""
    action_type = "workflow_template_validator"
    display_name = "工作流模板验证器"
    description = "验证工作流模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", {})
            errors: List[str] = []
            warnings: List[str] = []
            if not template.get("name"):
                errors.append("Template name is required")
            if not template.get("steps"):
                warnings.append("Template has no steps")
            for i, step in enumerate(template.get("steps", [])):
                if not step.get("type") and not step.get("action_type"):
                    errors.append(f"Step {i}: missing type/action_type")
            return ActionResult(
                success=len(errors) == 0,
                message=f"Validation: {'PASSED' if not errors else 'FAILED'}",
                data={"valid": len(errors) == 0, "errors": errors, "warnings": warnings},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Template validation failed: {e}")
