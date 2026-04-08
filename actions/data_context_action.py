"""Data context action module for RabAI AutoClick.

Provides data context management:
- DataContextAction: Manage data context
- ContextScopeAction: Handle context scopes
- ContextVariableAction: Manage context variables
- ContextPropagatorAction: Propagate context
- ContextCleanerAction: Clean up context
"""

import copy
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from contextvars import ContextVar

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataContextAction(BaseAction):
    """Manage data context."""
    action_type = "data_context"
    display_name = "数据上下文"
    description = "管理数据上下文"

    def __init__(self):
        super().__init__()
        self._contexts = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            context_id = params.get("context_id", "default")
            initial_data = params.get("initial_data", {})
            parent_id = params.get("parent_id", None)

            if operation == "create":
                new_context = {
                    "id": context_id,
                    "parent_id": parent_id,
                    "data": copy.deepcopy(initial_data),
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "version": 1
                }

                if parent_id and parent_id in self._contexts:
                    new_context["inherited"] = copy.deepcopy(self._contexts[parent_id]["data"])

                self._contexts[context_id] = new_context

                return ActionResult(
                    success=True,
                    data={
                        "context_id": context_id,
                        "parent_id": parent_id,
                        "created_at": new_context["created_at"],
                        "has_inherited_data": parent_id is not None
                    },
                    message=f"Context '{context_id}' created" + (f" (inherits from '{parent_id}')" if parent_id else "")
                )

            elif operation == "get":
                if context_id not in self._contexts:
                    return ActionResult(success=False, message=f"Context '{context_id}' not found")
                ctx = self._contexts[context_id]
                return ActionResult(
                    success=True,
                    data={
                        "context_id": context_id,
                        "data": ctx["data"],
                        "parent_id": ctx["parent_id"],
                        "version": ctx["version"]
                    },
                    message=f"Retrieved context '{context_id}': {len(ctx['data'])} items"
                )

            elif operation == "update":
                if context_id not in self._contexts:
                    return ActionResult(success=False, message=f"Context '{context_id}' not found")
                updates = params.get("updates", {})
                ctx = self._contexts[context_id]
                ctx["data"].update(updates)
                ctx["updated_at"] = datetime.now().isoformat()
                ctx["version"] += 1
                return ActionResult(
                    success=True,
                    data={
                        "context_id": context_id,
                        "updated_keys": list(updates.keys()),
                        "version": ctx["version"]
                    },
                    message=f"Updated context '{context_id}': {len(updates)} keys modified"
                )

            elif operation == "delete":
                if context_id in self._contexts:
                    del self._contexts[context_id]
                return ActionResult(
                    success=True,
                    data={"context_id": context_id, "deleted": True},
                    message=f"Context '{context_id}' deleted"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "contexts": list(self._contexts.keys()),
                        "count": len(self._contexts)
                    },
                    message=f"Contexts: {list(self._contexts.keys())}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data context error: {str(e)}")


class ContextScopeAction(BaseAction):
    """Handle context scopes."""
    action_type = "context_scope"
    display_name = "上下文作用域"
    description = "处理上下文作用域"

    def __init__(self):
        super().__init__()
        self._scopes = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "enter")
            scope_name = params.get("scope_name", "default")
            scope_data = params.get("scope_data", {})

            if operation == "enter":
                if scope_name not in self._scopes:
                    self._scopes[scope_name] = {
                        "stack": [],
                        "created_at": datetime.now().isoformat()
                    }
                self._scopes[scope_name]["stack"].append({
                    "data": copy.deepcopy(scope_data),
                    "entered_at": datetime.now().isoformat()
                })
                return ActionResult(
                    success=True,
                    data={
                        "scope_name": scope_name,
                        "stack_depth": len(self._scopes[scope_name]["stack"]),
                        "entered_at": self._scopes[scope_name]["stack"][-1]["entered_at"]
                    },
                    message=f"Entered scope '{scope_name}': depth={len(self._scopes[scope_name]['stack'])}"
                )

            elif operation == "exit":
                if scope_name not in self._scopes or not self._scopes[scope_name]["stack"]:
                    return ActionResult(success=False, message=f"Scope '{scope_name}' is empty")
                exited = self._scopes[scope_name]["stack"].pop()
                return ActionResult(
                    success=True,
                    data={
                        "scope_name": scope_name,
                        "stack_depth": len(self._scopes[scope_name]["stack"]),
                        "exited_data": exited["data"]
                    },
                    message=f"Exited scope '{scope_name}': depth={len(self._scopes[scope_name]['stack'])}"
                )

            elif operation == "peek":
                if scope_name not in self._scopes or not self._scopes[scope_name]["stack"]:
                    return ActionResult(success=False, message=f"Scope '{scope_name}' is empty")
                top = self._scopes[scope_name]["stack"][-1]
                return ActionResult(
                    success=True,
                    data={
                        "scope_name": scope_name,
                        "stack_depth": len(self._scopes[scope_name]["stack"]),
                        "top_data": top["data"]
                    },
                    message=f"Scope '{scope_name}' top: {top['data']}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Context scope error: {str(e)}")


class ContextVariableAction(BaseAction):
    """Manage context variables."""
    action_type = "context_variable"
    display_name = "上下文变量"
    description = "管理上下文变量"

    def __init__(self):
        super().__init__()
        self._variables = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "set")
            var_name = params.get("var_name", "")
            var_value = params.get("var_value", None)
            scope = params.get("scope", "global")

            if not var_name:
                return ActionResult(success=False, message="var_name is required")

            if operation == "set":
                key = f"{scope}:{var_name}"
                self._variables[key] = {
                    "value": var_value,
                    "scope": scope,
                    "set_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "var_name": var_name,
                        "scope": scope,
                        "value": var_value,
                        "set_at": self._variables[key]["set_at"]
                    },
                    message=f"Set variable '{var_name}' = {var_value} (scope: {scope})"
                )

            elif operation == "get":
                key = f"{scope}:{var_name}"
                if key not in self._variables:
                    return ActionResult(success=False, message=f"Variable '{var_name}' not found in scope '{scope}'")
                return ActionResult(
                    success=True,
                    data={
                        "var_name": var_name,
                        "scope": scope,
                        "value": self._variables[key]["value"]
                    },
                    message=f"Got variable '{var_name}': {self._variables[key]['value']}"
                )

            elif operation == "delete":
                key = f"{scope}:{var_name}"
                if key in self._variables:
                    del self._variables[key]
                return ActionResult(
                    success=True,
                    data={"var_name": var_name, "scope": scope, "deleted": True},
                    message=f"Deleted variable '{var_name}'"
                )

            elif operation == "list":
                if scope == "global":
                    global_vars = {k: v["value"] for k, v in self._variables.items() if v["scope"] == "global"}
                else:
                    global_vars = {k.replace(f"{scope}:", ""): v["value"] for k, v in self._variables.items() if v["scope"] == scope}
                return ActionResult(
                    success=True,
                    data={
                        "scope": scope,
                        "variables": global_vars,
                        "count": len(global_vars)
                    },
                    message=f"Listed {len(global_vars)} variables in scope '{scope}'"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Context variable error: {str(e)}")


class ContextPropagatorAction(BaseAction):
    """Propagate context through operations."""
    action_type = "context_propagator"
    display_name = "上下文传播"
    description = "传播上下文到操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            context_data = params.get("context_data", {})
            operations = params.get("operations", [])
            propagation_mode = params.get("propagation_mode", "copy")

            if not operations:
                return ActionResult(success=False, message="operations is required")

            propagated = []
            for i, op in enumerate(operations):
                if propagation_mode == "copy":
                    op_data = {**context_data, **op}
                elif propagation_mode == "merge":
                    op_data = {k: v for k, v in context_data.items()}
                    if "merge_data" in op:
                        op_data.update(op["merge_data"])
                else:
                    op_data = op

                propagated.append({
                    "operation_index": i,
                    "operation": op,
                    "context_data": op_data,
                    "propagated": True
                })

            return ActionResult(
                success=True,
                data={
                    "propagation_mode": propagation_mode,
                    "operations_count": len(operations),
                    "context_keys": list(context_data.keys()),
                    "propagated_operations": propagated
                },
                message=f"Propagated context to {len(operations)} operations (mode: {propagation_mode})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Context propagator error: {str(e)}")


class ContextCleanerAction(BaseAction):
    """Clean up context data."""
    action_type = "context_cleaner"
    display_name = "上下文清理"
    description = "清理上下文数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            context_id = params.get("context_id", None)
            clean_mode = params.get("clean_mode", "all")
            keys_to_remove = params.get("keys_to_remove", [])
            older_than_seconds = params.get("older_than_seconds", None)

            if context_id:
                cleaned_keys = keys_to_remove if keys_to_remove else []
                cleaned_count = len(cleaned_keys)

                return ActionResult(
                    success=True,
                    data={
                        "context_id": context_id,
                        "clean_mode": clean_mode,
                        "cleaned_keys": cleaned_keys,
                        "cleaned_count": cleaned_count,
                        "cleaned_at": datetime.now().isoformat()
                    },
                    message=f"Cleaned {cleaned_count} items from context '{context_id}'"
                )
            else:
                return ActionResult(
                    success=True,
                    data={
                        "clean_mode": clean_mode,
                        "cleaned_at": datetime.now().isoformat(),
                        "older_than_seconds": older_than_seconds
                    },
                    message=f"Context cleanup triggered (mode: {clean_mode})"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Context cleaner error: {str(e)}")
