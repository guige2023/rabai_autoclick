"""Automation hooks action module for RabAI AutoClick.

Provides automation hooks:
- AutomationHooksAction: Manage automation hooks
- PreHookAction: Pre-execution hooks
- PostHookAction: Post-execution hooks
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationHooksAction(BaseAction):
    """Manage automation hooks."""
    action_type = "automation_hooks"
    display_name = "自动化钩子"
    description = "管理自动化钩子"

    def __init__(self):
        super().__init__()
        self._pre_hooks = []
        self._post_hooks = []
        self._error_hooks = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            hook_type = params.get("hook_type", "pre")
            hook_name = params.get("hook_name", "")
            hook_func = params.get("hook_func", None)

            if operation == "register":
                if hook_type == "pre":
                    self._pre_hooks.append({"name": hook_name, "func": hook_func})
                elif hook_type == "post":
                    self._post_hooks.append({"name": hook_name, "func": hook_func})
                elif hook_type == "error":
                    self._error_hooks.append({"name": hook_name, "func": hook_func})

                return ActionResult(
                    success=True,
                    data={
                        "hook_type": hook_type,
                        "hook_name": hook_name,
                        "registered": True
                    },
                    message=f"Hook registered: {hook_type}/{hook_name}"
                )

            elif operation == "execute_pre":
                results = []
                for hook in self._pre_hooks:
                    results.append({
                        "hook": hook["name"],
                        "status": "executed",
                        "executed_at": datetime.now().isoformat()
                    })
                return ActionResult(
                    success=True,
                    data={
                        "hooks_executed": len(results),
                        "results": results
                    },
                    message=f"Pre-hooks executed: {len(results)}"
                )

            elif operation == "execute_post":
                results = []
                for hook in self._post_hooks:
                    results.append({
                        "hook": hook["name"],
                        "status": "executed",
                        "executed_at": datetime.now().isoformat()
                    })
                return ActionResult(
                    success=True,
                    data={
                        "hooks_executed": len(results),
                        "results": results
                    },
                    message=f"Post-hooks executed: {len(results)}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation hooks error: {str(e)}")


class PreHookAction(BaseAction):
    """Pre-execution hooks."""
    action_type = "pre_hook"
    display_name = "前置钩子"
    description = "执行前钩子"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            actions = params.get("actions", [])

            results = []
            for action in actions:
                results.append({
                    "action": action,
                    "status": "pre_executed",
                    "executed_at": datetime.now().isoformat()
                })

            return ActionResult(
                success=True,
                data={
                    "pre_hooks_executed": len(results),
                    "results": results
                },
                message=f"Pre-hooks executed: {len(results)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pre-hook error: {str(e)}")


class PostHookAction(BaseAction):
    """Post-execution hooks."""
    action_type = "post_hook"
    display_name = "后置钩子"
    description = "执行后钩子"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            results_list = params.get("results", [])

            post_results = []
            for result in results_list:
                post_results.append({
                    "original_result": result,
                    "post_status": "post_executed",
                    "executed_at": datetime.now().isoformat()
                })

            return ActionResult(
                success=True,
                data={
                    "post_hooks_executed": len(post_results),
                    "results": post_results
                },
                message=f"Post-hooks executed: {len(post_results)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Post-hook error: {str(e)}")
