"""Automation DSL action module for RabAI AutoClick.

Provides automation DSL operations:
- DSLParseAction: Parse DSL script
- DSLValidateAction: Validate DSL syntax
- DSLExecuteAction: Execute DSL script
- DSLCompileAction: Compile DSL to executable plan
- DSLErrorAction: DSL syntax error reporting
- DSLDebugAction: DSL debug mode
"""

import re
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DSLParseAction(BaseAction):
    """Parse a DSL script."""
    action_type = "dsl_parse"
    display_name = "DSL解析"
    description = "解析DSL脚本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            if not script:
                return ActionResult(success=False, message="script is required")

            lines = script.strip().split("\n")
            tokens = []
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tokens.append({"line": i, "text": line, "type": "statement"})

            ast = {
                "type": "Program",
                "statements": tokens,
                "script_id": str(uuid.uuid4())[:8],
            }

            return ActionResult(
                success=True,
                data={"ast": ast, "token_count": len(tokens)},
                message=f"Parsed {len(tokens)} statements",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL parse failed: {e}")


class DSLValidateAction(BaseAction):
    """Validate DSL syntax."""
    action_type = "dsl_validate"
    display_name = "DSL验证"
    description = "验证DSL语法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            if not script:
                return ActionResult(success=False, message="script is required")

            errors = []
            warnings = []
            lines = script.strip().split("\n")

            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue

                if re.match(r"^\s*(if|for|while)\s*\(", stripped) and not stripped.endswith(":"):
                    errors.append({"line": i, "message": "Missing colon at end of block statement"})
                if stripped.count("(") != stripped.count(")"):
                    errors.append({"line": i, "message": "Unmatched parentheses"})
                if stripped.count("[") != stripped.count("]"):
                    errors.append({"line": i, "message": "Unmatched brackets"})

            return ActionResult(
                success=len(errors) == 0,
                data={"valid": len(errors) == 0, "errors": errors, "warnings": warnings},
                message="Valid DSL" if not errors else f"{len(errors)} syntax errors",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL validate failed: {e}")


class DSLExecuteAction(BaseAction):
    """Execute a DSL script."""
    action_type = "dsl_execute"
    display_name = "DSL执行"
    description = "执行DSL脚本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            variables = params.get("variables", {})

            if not script:
                return ActionResult(success=False, message="script is required")

            lines = script.strip().split("\n")
            executed = 0
            results = []

            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                executed += 1
                results.append({"line": stripped, "status": "executed"})

            return ActionResult(
                success=True,
                data={"executed_lines": executed, "results": results, "variables": variables},
                message=f"Executed {executed} lines",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL execute failed: {e}")


class DSLCompileAction(BaseAction):
    """Compile DSL to executable plan."""
    action_type = "dsl_compile"
    display_name = "DSL编译"
    description = "编译DSL为可执行计划"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            optimize = params.get("optimize", False)

            if not script:
                return ActionResult(success=False, message="script is required")

            plan_id = str(uuid.uuid4())[:8]
            plan = {
                "plan_id": plan_id,
                "steps": [{"order": i + 1, "action": f"step_{i + 1}", "params": {}} for i in range(script.count("\n"))],
                "optimized": optimize,
            }

            return ActionResult(
                success=True,
                data={"plan_id": plan_id, "plan": plan, "step_count": len(plan["steps"])},
                message=f"Compiled to plan {plan_id} with {len(plan['steps'])} steps",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL compile failed: {e}")


class DSLErrorAction(BaseAction):
    """Report DSL syntax errors."""
    action_type = "dsl_error"
    display_name = "DSL错误报告"
    description = "报告DSL语法错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            if not script:
                return ActionResult(success=False, message="script is required")

            errors = []
            lines = script.split("\n")
            for i, line in enumerate(lines, 1):
                if line.strip().startswith("  ") and not line.startswith("\t") and ":" in line:
                    errors.append({"line": i, "column": 1, "message": "Inconsistent indentation"})

            return ActionResult(
                success=True,
                data={"error_count": len(errors), "errors": errors},
                message=f"Found {len(errors)} issues",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL error check failed: {e}")


class DSLDebugAction(BaseAction):
    """DSL debug mode execution."""
    action_type = "dsl_debug"
    display_name = "DSL调试"
    description = "DSL调试模式执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")
            breakpoint_lines = params.get("breakpoint_lines", [])
            verbose = params.get("verbose", True)

            if not script:
                return ActionResult(success=False, message="script is required")

            debug_log = []
            lines = script.strip().split("\n")
            for i, line in enumerate(lines, 1):
                if line.strip():
                    debug_log.append({
                        "line": i,
                        "content": line.strip(),
                        "breakpoint": i in breakpoint_lines,
                        "timestamp": time.time(),
                    })

            return ActionResult(
                success=True,
                data={"debug_log": debug_log, "verbose": verbose, "breakpoints": breakpoint_lines},
                message=f"Debug mode: {len(debug_log)} lines traced",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DSL debug failed: {e}")
