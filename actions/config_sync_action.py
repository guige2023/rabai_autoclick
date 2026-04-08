"""Config sync action module for RabAI AutoClick.

Provides config synchronization operations:
- ConfigSyncPushAction: Push config to target
- ConfigSyncPullAction: Pull config from source
- ConfigSyncStatusAction: Sync status
- ConfigSyncHistoryAction: Sync history
- ConfigSyncCompareAction: Compare configs
- ConfigSyncMergeAction: Merge configs
- ConfigSyncValidateAction: Validate sync config
- ConfigSyncScheduleAction: Schedule sync
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConfigSyncStore:
    """In-memory config sync history."""
    
    _history: List[Dict[str, Any]] = []
    _sync_id = 1
    
    @classmethod
    def add_sync(cls, source: str, target: str, status: str) -> Dict[str, Any]:
        sync = {
            "id": cls._sync_id,
            "source": source,
            "target": target,
            "status": status,
            "timestamp": time.time()
        }
        cls._sync_id += 1
        cls._history.append(sync)
        return sync
    
    @classmethod
    def list_history(cls, limit: int = 100) -> List[Dict[str, Any]]:
        return cls._history[-limit:]


class ConfigSyncPushAction(BaseAction):
    """Push config to target."""
    action_type = "config_sync_push"
    display_name = "推送配置"
    description = "推送配置到目标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            config = params.get("config", {})
            overwrite = params.get("overwrite", True)
            
            if not source or not target:
                return ActionResult(success=False, message="source and target required")
            
            sync = ConfigSyncStore.add_sync(source, target, "completed")
            
            return ActionResult(
                success=True,
                message=f"Pushed config from {source} to {target}",
                data={"sync": sync, "overwrite": overwrite}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync push failed: {str(e)}")


class ConfigSyncPullAction(BaseAction):
    """Pull config from source."""
    action_type = "config_sync_pull"
    display_name = "拉取配置"
    description = "从源拉取配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            merge = params.get("merge", False)
            
            if not source:
                return ActionResult(success=False, message="source required")
            
            sync = ConfigSyncStore.add_sync(source, "local", "completed")
            
            return ActionResult(
                success=True,
                message=f"Pulled config from {source}",
                data={"sync": sync, "source": source, "merged": merge}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync pull failed: {str(e)}")


class ConfigSyncStatusAction(BaseAction):
    """Get sync status."""
    action_type = "config_sync_status"
    display_name = "同步状态"
    description = "获取同步状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            history = ConfigSyncStore.list_history(10)
            
            completed = sum(1 for h in history if h["status"] == "completed")
            failed = sum(1 for h in history if h["status"] == "failed")
            
            return ActionResult(
                success=True,
                message=f"Sync status: {completed} completed, {failed} failed",
                data={
                    "recent_syncs": history,
                    "completed_count": completed,
                    "failed_count": failed
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync status failed: {str(e)}")


class ConfigSyncHistoryAction(BaseAction):
    """Get sync history."""
    action_type = "config_sync_history"
    display_name = "同步历史"
    description = "获取同步历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limit = params.get("limit", 100)
            
            history = ConfigSyncStore.list_history(limit)
            
            return ActionResult(
                success=True,
                message=f"Found {len(history)} sync history entries",
                data={"history": history, "count": len(history)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync history failed: {str(e)}")


class ConfigSyncCompareAction(BaseAction):
    """Compare configs."""
    action_type = "config_sync_compare"
    display_name = "配置对比"
    description = "对比配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config_a = params.get("config_a", {})
            config_b = params.get("config_b", {})
            
            all_keys = set(config_a.keys()) | set(config_b.keys())
            
            identical = True
            differences = []
            
            for key in all_keys:
                if key not in config_a:
                    differences.append({"key": key, "status": "only_in_b"})
                    identical = False
                elif key not in config_b:
                    differences.append({"key": key, "status": "only_in_a"})
                    identical = False
                elif config_a[key] != config_b[key]:
                    differences.append({
                        "key": key,
                        "status": "different",
                        "value_a": config_a[key],
                        "value_b": config_b[key]
                    })
                    identical = False
            
            return ActionResult(
                success=True,
                message=f"Configs {'identical' if identical else 'differ'}: {len(differences)} differences",
                data={
                    "identical": identical,
                    "differences": differences,
                    "diff_count": len(differences)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync compare failed: {str(e)}")


class ConfigSyncMergeAction(BaseAction):
    """Merge configs."""
    action_type = "config_sync_merge"
    display_name = "合并配置"
    description = "合并配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base = params.get("base", {})
            override = params.get("override", {})
            strategy = params.get("strategy", "override")
            
            if strategy == "override":
                merged = {**base, **override}
            elif strategy == "base_wins":
                merged = {**override, **base}
            else:
                merged = {**base, **override}
            
            return ActionResult(
                success=True,
                message=f"Merged configs with strategy: {strategy}",
                data={"merged": merged, "strategy": strategy}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync merge failed: {str(e)}")


class ConfigSyncValidateAction(BaseAction):
    """Validate sync config."""
    action_type = "config_sync_validate"
    display_name = "验证同步配置"
    description = "验证同步配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            
            errors = []
            warnings = []
            
            if not source:
                errors.append("source is required")
            if not target:
                errors.append("target is required")
            
            if source == target:
                warnings.append("source and target are the same")
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Sync config {'valid' if len(errors) == 0 else 'invalid'}",
                data={"errors": errors, "warnings": warnings}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync validate failed: {str(e)}")


class ConfigSyncScheduleAction(BaseAction):
    """Schedule config sync."""
    action_type = "config_sync_schedule"
    display_name = "计划同步"
    description = "计划配置同步"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            schedule = params.get("schedule", "hourly")
            enabled = params.get("enabled", True)
            
            schedules = {
                "hourly": "0 * * * *",
                "daily": "0 0 * * *",
                "weekly": "0 0 * * 0"
            }
            
            cron = schedules.get(schedule, schedules["hourly"])
            
            sync_config = {
                "source": source,
                "target": target,
                "schedule": schedule,
                "cron": cron,
                "enabled": enabled,
                "created_at": time.time()
            }
            
            return ActionResult(
                success=True,
                message=f"Scheduled config sync: {schedule}",
                data={"sync_config": sync_config}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config sync schedule failed: {str(e)}")
