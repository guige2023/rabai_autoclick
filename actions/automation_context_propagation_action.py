"""Automation context propagation action module for RabAI AutoClick.

Provides context propagation for automation workflows:
- AutomationContextPropagateAction: Propagate context through workflow steps
- AutomationContextMergeAction: Merge multiple context sources
- AutomationContextSnapshotAction: Snapshot and restore context
- AutomationContextValidatorAction: Validate context integrity
"""

import copy
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationContextPropagateAction(BaseAction):
    """Propagate context through automation workflow steps."""
    action_type = "automation_context_propagate"
    display_name = "自动化上下文传播"
    description = "在工作流步骤间传播上下文"

    def __init__(self):
        super().__init__()
        self._context_stack: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            initial_context = params.get("initial_context", {})
            propagate_errors = params.get("propagate_errors", True)
            max_depth = params.get("max_depth", 100)

            if not steps:
                return ActionResult(success=False, message="steps list is required")

            current_context = copy.deepcopy(initial_context)
            results = []
            self._context_stack.append(copy.deepcopy(current_context))

            for i, step in enumerate(steps):
                if i >= max_depth:
                    return ActionResult(success=False, message=f"Max depth {max_depth} exceeded")

                step_context = step.get("context_updates", {})
                merge_strategy = step.get("merge_strategy", "update")

                if merge_strategy == "update":
                    current_context.update(step_context)
                elif merge_strategy == "replace":
                    current_context = copy.deepcopy(step_context)
                elif merge_strategy == "deep_merge":
                    current_context = self._deep_merge(current_context, step_context)
                elif merge_strategy == "add":
                    current_context = self._add_keys(current_context, step_context)

                self._context_stack.append(copy.deepcopy(current_context))

                step_action = step.get("action")
                if callable(step_action):
                    try:
                        result = step_action(context=current_context)
                        results.append({"step": i, "success": result.get("success", True), "context": copy.deepcopy(current_context)})
                        if not result.get("success", True) and not propagate_errors:
                            break
                    except Exception as e:
                        results.append({"step": i, "success": False, "error": str(e), "context": copy.deepcopy(current_context)})
                        if not propagate_errors:
                            break
                else:
                    results.append({"step": i, "success": True, "context": copy.deepcopy(current_context)})

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=success_count == len(steps),
                message=f"Propagated through {len(steps)} steps, {success_count} succeeded",
                data={"context": current_context, "steps": len(steps), "results": results, "stack_depth": len(self._context_stack)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Context propagate error: {e}")

    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = copy.deepcopy(base)
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result

    def _add_keys(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Add only new keys."""
        result = copy.deepcopy(base)
        for key, value in update.items():
            if key not in result:
                result[key] = copy.deepcopy(value)
        return result


class AutomationContextMergeAction(BaseAction):
    """Merge multiple context sources."""
    action_type = "automation_context_merge"
    display_name = "自动化上下文合并"
    description = "合并多个上下文源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            sources = params.get("sources", [])
            merge_strategy = params.get("merge_strategy", "last_win")
            conflict_resolution = params.get("conflict_resolution", "last")
            validate_keys = params.get("validate_keys", True)

            if not sources:
                return ActionResult(success=False, message="sources list is required")

            if validate_keys:
                all_keys = set()
                for src in sources:
                    if isinstance(src, dict):
                        all_keys.update(src.keys())
                duplicates = [k for k in all_keys if sum(1 for s in sources if isinstance(s, dict) and k in s) > 1]

            if merge_strategy == "first_win":
                merged = {}
                for src in sources:
                    if isinstance(src, dict):
                        merged.update(src)
            elif merge_strategy == "last_win":
                merged = {}
                for src in sources:
                    if isinstance(src, dict):
                        merged.update(src)
            elif merge_strategy == "deep_merge":
                merged = {}
                for src in sources:
                    if isinstance(src, dict):
                        merged = self._deep_merge(merged, src)
            elif merge_strategy == "union":
                merged = {}
                for src in sources:
                    if isinstance(src, dict):
                        for k, v in src.items():
                            if k not in merged:
                                merged[k] = v
            else:
                merged = sources[-1] if sources else {}

            return ActionResult(
                success=True,
                message=f"Merged {len(sources)} sources into {len(merged)} keys",
                data={"merged": merged, "key_count": len(merged), "sources_count": len(sources)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Context merge error: {e}")

    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = copy.deepcopy(base)
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result


class AutomationContextSnapshotAction(BaseAction):
    """Snapshot and restore automation context."""
    action_type = "automation_context_snapshot"
    display_name = "自动化上下文快照"
    description = "快照和恢复自动化上下文"

    def __init__(self):
        super().__init__()
        self._snapshots: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "snapshot")
            snapshot_id = params.get("snapshot_id")
            context_data = params.get("context_data", {})
            max_snapshots = params.get("max_snapshots", 50)
            metadata = params.get("metadata", {})

            if operation == "snapshot":
                if not snapshot_id:
                    snapshot_id = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

                self._snapshots[snapshot_id] = {
                    "context": copy.deepcopy(context_data),
                    "metadata": metadata,
                    "created_at": datetime.now().isoformat(),
                }

                if len(self._snapshots) > max_snapshots:
                    oldest = min(self._snapshots.keys(), key=lambda k: self._snapshots[k]["created_at"])
                    del self._snapshots[oldest]

                return ActionResult(
                    success=True,
                    message=f"Snapshot {snapshot_id} created",
                    data={"snapshot_id": snapshot_id, "snapshot_count": len(self._snapshots)}
                )

            elif operation == "restore":
                if not snapshot_id or snapshot_id not in self._snapshots:
                    return ActionResult(success=False, message=f"Snapshot {snapshot_id} not found")

                snapshot = self._snapshots[snapshot_id]
                return ActionResult(
                    success=True,
                    message=f"Restored snapshot {snapshot_id}",
                    data={"context": snapshot["context"], "metadata": snapshot["metadata"], "created_at": snapshot["created_at"]}
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    message=f"Found {len(self._snapshots)} snapshots",
                    data={"snapshots": {k: {"created_at": v["created_at"], "metadata": v["metadata"]} for k, v in self._snapshots.items()}}
                )

            elif operation == "delete":
                if snapshot_id and snapshot_id in self._snapshots:
                    del self._snapshots[snapshot_id]
                    return ActionResult(success=True, message=f"Snapshot {snapshot_id} deleted")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Context snapshot error: {e}")


class AutomationContextValidatorAction(BaseAction):
    """Validate automation context integrity."""
    action_type = "automation_context_validator"
    display_name = "自动化上下文验证器"
    description = "验证自动化上下文的完整性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            context_data = params.get("context_data", {})
            schema = params.get("schema", {})
            required_keys = params.get("required_keys", [])
            optional_keys = params.get("optional_keys", [])
            validate_types = params.get("validate_types", True)

            errors = []
            warnings = []

            for key in required_keys:
                if key not in context_data:
                    errors.append(f"Required key missing: {key}")

            for key in context_data:
                if key not in required_keys and key not in optional_keys:
                    warnings.append(f"Unexpected key: {key}")

            if validate_types and schema:
                for key, expected_type in schema.items():
                    if key in context_data:
                        value = context_data[key]
                        if not isinstance(value, expected_type):
                            errors.append(f"Type mismatch for '{key}': expected {expected_type.__name__}, got {type(value).__name__}")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                message=f"Validation {'passed' if is_valid else 'failed'}: {len(errors)} errors, {len(warnings)} warnings",
                data={"valid": is_valid, "errors": errors, "warnings": warnings}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Context validator error: {e}")
