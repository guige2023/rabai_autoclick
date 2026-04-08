"""Automation builder action module for RabAI AutoClick.

Provides workflow building operations:
- WorkflowBuilderAction: Build automation workflows
- StepBuilderAction: Build individual workflow steps
- ConditionBuilderAction: Build conditional logic
- LoopBuilderAction: Build loop constructs
- BranchBuilderAction: Build branching logic
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WorkflowBuilderAction(BaseAction):
    """Build automation workflows."""
    action_type = "workflow_builder"
    display_name = "工作流构建器"
    description = "构建自动化工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow_name = params.get("name", "unnamed_workflow")
            workflow_description = params.get("description", "")
            steps = params.get("steps", [])
            config = params.get("config", {})
            version = params.get("version", "1.0.0")

            if not steps:
                return ActionResult(success=False, message="steps are required")

            workflow = {
                "id": f"wf_{workflow_name}_{int(datetime.now().timestamp())}",
                "name": workflow_name,
                "description": workflow_description,
                "version": version,
                "steps": steps,
                "config": config,
                "created_at": datetime.now().isoformat(),
                "step_count": len(steps),
                "estimated_duration": self._estimate_duration(steps)
            }

            return ActionResult(
                success=True,
                data=workflow,
                message=f"Workflow '{workflow_name}' built with {len(steps)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Workflow builder error: {str(e)}")

    def _estimate_duration(self, steps: List[Dict]) -> float:
        total = 0.0
        for step in steps:
            total += step.get("estimated_duration", 1.0)
        return total


class StepBuilderAction(BaseAction):
    """Build individual workflow steps."""
    action_type = "step_builder"
    display_name = "步骤构建器"
    description = "构建单个工作流步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            step_type = params.get("type", "")
            step_name = params.get("name", "")
            action = params.get("action", {})
            conditions = params.get("conditions", [])
            retry = params.get("retry", {})
            timeout = params.get("timeout", 30)
            on_error = params.get("on_error", "fail")

            if not step_type:
                return ActionResult(success=False, message="step type is required")

            step = {
                "id": f"step_{step_name}_{int(datetime.now().timestamp())}",
                "name": step_name,
                "type": step_type,
                "action": action,
                "conditions": conditions,
                "retry": retry,
                "timeout": timeout,
                "on_error": on_error,
                "estimated_duration": action.get("estimated_duration", 1.0)
            }

            return ActionResult(
                success=True,
                data=step,
                message=f"Step '{step_name}' built successfully"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Step builder error: {str(e)}")


class ConditionBuilderAction(BaseAction):
    """Build conditional logic for workflows."""
    action_type = "condition_builder"
    display_name = "条件构建器"
    description = "构建工作流条件逻辑"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition_type = params.get("condition_type", "if")
            expressions = params.get("expressions", [])
            operator = params.get("operator", "and")
            then_steps = params.get("then_steps", [])
            else_steps = params.get("else_steps", [])

            if not expressions:
                return ActionResult(success=False, message="expressions are required")

            condition = {
                "type": condition_type,
                "expressions": expressions,
                "operator": operator,
                "then_steps": then_steps,
                "else_steps": else_steps,
                "compiled": self._compile_condition(expressions, operator)
            }

            return ActionResult(
                success=True,
                data=condition,
                message=f"Condition '{condition_type}' built with {len(expressions)} expressions"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Condition builder error: {str(e)}")

    def _compile_condition(self, expressions: List[Dict], operator: str) -> str:
        op_symbol = " && " if operator == "and" else " || "
        compiled = op_symbol.join(f"({exp.get('field', '')} {exp.get('operator', '==')} {exp.get('value', '')})" for exp in expressions)
        return compiled


class LoopBuilderAction(BaseAction):
    """Build loop constructs for workflows."""
    action_type = "loop_builder"
    display_name = "循环构建器"
    description = "构建工作流循环结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            loop_type = params.get("loop_type", "for")
            items = params.get("items", [])
            max_iterations = params.get("max_iterations", 100)
            loop_steps = params.get("loop_steps", [])
            break_condition = params.get("break_condition", {})
            continue_condition = params.get("continue_condition", {})

            loop = {
                "type": loop_type,
                "max_iterations": max_iterations,
                "loop_steps": loop_steps,
                "break_condition": break_condition,
                "continue_condition": continue_condition,
                "estimated_iterations": len(items) if items else max_iterations
            }

            if loop_type == "for":
                loop["items"] = items
            elif loop_type == "while":
                loop["condition"] = params.get("condition", "")

            return ActionResult(
                success=True,
                data=loop,
                message=f"Loop '{loop_type}' built with {len(loop_steps)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Loop builder error: {str(e)}")


class BranchBuilderAction(BaseAction):
    """Build branching logic for workflows."""
    action_type = "branch_builder"
    display_name = "分支构建器"
    description = "构建工作流分支逻辑"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            default_branch = params.get("default_branch", None)
            branch_type = params.get("branch_type", "if_else")
            parallel = params.get("parallel", False)
            max_parallel = params.get("max_parallel", 3)

            if not branches:
                return ActionResult(success=False, message="branches are required")

            branch_config = {
                "type": branch_type,
                "branches": branches,
                "default_branch": default_branch,
                "parallel": parallel,
                "max_parallel": max_parallel if parallel else 1,
                "branch_count": len(branches)
            }

            return ActionResult(
                success=True,
                data=branch_config,
                message=f"Branch configuration built with {len(branches)} branches"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Branch builder error: {str(e)}")


class WorkflowValidatorAction(BaseAction):
    """Validate workflow configuration."""
    action_type = "workflow_validator"
    display_name = "工作流验证器"
    description = "验证工作流配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow = params.get("workflow", {})
            strict = params.get("strict", False)

            errors = []
            warnings = []

            if "name" not in workflow:
                errors.append("Workflow name is required")

            if "steps" not in workflow:
                errors.append("Workflow steps are required")
            else:
                steps = workflow["steps"]
                if not isinstance(steps, list):
                    errors.append("Workflow steps must be a list")
                elif len(steps) == 0:
                    errors.append("Workflow must have at least one step")

            step_names = [s.get("name", "") for s in workflow.get("steps", [])]
            if len(step_names) != len(set(step_names)):
                warnings.append("Duplicate step names found")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "error_count": len(errors),
                    "warning_count": len(warnings)
                },
                message=f"Workflow validation: {'PASSED' if is_valid else 'FAILED'} ({len(errors)} errors, {len(warnings)} warnings)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Workflow validator error: {str(e)}")
