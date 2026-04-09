"""API lifecycle management action module for RabAI AutoClick.

Provides API lifecycle operations:
- LifecycleStateManager: Manages API lifecycle states
- APICreationAction: Create new API versions/endpoints
- APIUpdateAction: Update existing API configurations
- APIDeprecationAction: Deprecate API versions safely
- APIGraduationAction: Graduate API from beta to stable
- LifecycleEventHook: Hooks for lifecycle state transitions
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LifecycleStage(Enum):
    """API lifecycle stages."""
    PLANNING = auto()
    ALPHA = auto()
    BETA = auto()
    STABLE = auto()
    DEPRECATED = auto()
    RETIRED = auto()
    REMOVED = auto()


class LifecycleStateManager:
    """Manages state transitions for API lifecycle."""

    VALID_TRANSITIONS: Dict[LifecycleStage, Set[LifecycleStage]] = {
        LifecycleStage.PLANNING: {LifecycleStage.ALPHA},
        LifecycleStage.ALPHA: {LifecycleStage.BETA, LifecycleStage.RETIRED},
        LifecycleStage.BETA: {LifecycleStage.STABLE, LifecycleStage.DEPRECATED, LifecycleStage.RETIRED},
        LifecycleStage.STABLE: {LifecycleStage.DEPRECATED, LifecycleStage.RETIRED},
        LifecycleStage.DEPRECATED: {LifecycleStage.RETIRED},
        LifecycleStage.RETIRED: {LifecycleStage.REMOVED},
        LifecycleStage.REMOVED: set(),
    }

    def __init__(self) -> None:
        self._states: Dict[str, LifecycleStage] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._hooks: Dict[LifecycleStage, List[Callable]] = {stage: [] for stage in LifecycleStage}

    def register_hook(self, stage: LifecycleStage, hook: Callable[[str, LifecycleStage, LifecycleStage], None]) -> None:
        """Register a callback hook for stage transitions."""
        self._hooks[stage].append(hook)

    def set_state(self, api_id: str, stage: LifecycleStage, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Set the lifecycle stage for an API."""
        current = self._states.get(api_id, LifecycleStage.PLANNING)
        if stage not in self.VALID_TRANSITIONS.get(current, set()):
            return False
        self._states[api_id] = stage
        self._history.setdefault(api_id, []).append({
            "stage": stage.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
        })
        for hook in self._hooks[stage]:
            try:
                hook(api_id, current, stage)
            except Exception:
                pass
        return True

    def get_state(self, api_id: str) -> LifecycleStage:
        """Get current lifecycle stage."""
        return self._states.get(api_id, LifecycleStage.PLANNING)

    def get_history(self, api_id: str) -> List[Dict[str, Any]]:
        """Get lifecycle history."""
        return self._history.get(api_id, [])

    def is_transition_valid(self, api_id: str, target: LifecycleStage) -> bool:
        """Check if transition is valid."""
        current = self.get_state(api_id)
        return target in self.VALID_TRANSITIONS.get(current, set())

    def get_deprecation_date(self, api_id: str) -> Optional[datetime]:
        """Get scheduled deprecation date if set."""
        history = self._history.get(api_id, [])
        for entry in reversed(history):
            if entry.get("metadata", {}).get("deprecation_date"):
                return datetime.fromisoformat(entry["metadata"]["deprecation_date"])
        return None


class APICreationAction(BaseAction):
    """Create a new API version or endpoint."""
    action_type = "api_creation"
    display_name = "API创建"
    description = "创建新的API版本或端点"

    def __init__(self) -> None:
        super().__init__()
        self.lifecycle = LifecycleStateManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_name = params.get("api_name", "")
            version = params.get("version", "v1")
            stage = params.get("stage", "PLANNING")
            spec = params.get("spec", {})
            if not api_name:
                return ActionResult(success=False, message="api_name is required")

            api_id = f"{api_name}:{version}"
            stage_enum = LifecycleStage[stage.upper()]
            self.lifecycle.set_state(api_id, stage_enum, {"created_at": datetime.now(timezone.utc).isoformat()})

            return ActionResult(
                success=True,
                message=f"API {api_id} created in {stage} stage",
                data={"api_id": api_id, "stage": stage_enum.name, "spec": spec},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API creation failed: {e}")


class APIUpdateAction(BaseAction):
    """Update an existing API configuration."""
    action_type = "api_update"
    display_name = "API更新"
    description = "更新现有API配置"

    def __init__(self) -> None:
        super().__init__()
        self.lifecycle = LifecycleStateManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_id = params.get("api_id", "")
            updates = params.get("updates", {})
            if not api_id:
                return ActionResult(success=False, message="api_id is required")

            current_stage = self.lifecycle.get_state(api_id)
            if current_stage in (LifecycleStage.RETIRED, LifecycleStage.REMOVED):
                return ActionResult(success=False, message=f"Cannot update API in {current_stage.name} stage")

            return ActionResult(
                success=True,
                message=f"API {api_id} updated",
                data={"api_id": api_id, "updates": updates, "stage": current_stage.name},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API update failed: {e}")


class APIDeprecationAction(BaseAction):
    """Deprecate an API version with migration path."""
    action_type = "api_deprecation"
    display_name = "API废弃"
    description = "安全废弃API版本并提供迁移路径"

    def __init__(self) -> None:
        super().__init__()
        self.lifecycle = LifecycleStateManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_id = params.get("api_id", "")
            sunset_date = params.get("sunset_date", "")
            migration_path = params.get("migration_path", "")
            if not api_id:
                return ActionResult(success=False, message="api_id is required")

            metadata = {
                "sunset_date": sunset_date,
                "migration_path": migration_path,
                "deprecated_at": datetime.now(timezone.utc).isoformat(),
            }
            success = self.lifecycle.set_state(api_id, LifecycleStage.DEPRECATED, metadata)
            if not success:
                return ActionResult(success=False, message="Invalid lifecycle transition")

            return ActionResult(
                success=True,
                message=f"API {api_id} deprecated, sunset date: {sunset_date}",
                data={"api_id": api_id, "sunset_date": sunset_date, "migration_path": migration_path},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API deprecation failed: {e}")


class APIGraduationAction(BaseAction):
    """Graduate API from one stage to the next."""
    action_type = "api_graduation"
    display_name = "API晋级"
    description = "API从当前阶段晋级到下一阶段"

    def __init__(self) -> None:
        super().__init__()
        self.lifecycle = LifecycleStateManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_id = params.get("api_id", "")
            target_stage = params.get("target_stage", "STABLE")
            if not api_id:
                return ActionResult(success=False, message="api_id is required")

            try:
                target = LifecycleStage[target_stage.upper()]
            except KeyError:
                return ActionResult(success=False, message=f"Invalid stage: {target_stage}")

            if not self.lifecycle.is_transition_valid(api_id, target):
                current = self.lifecycle.get_state(api_id)
                return ActionResult(success=False, message=f"Cannot transition from {current.name} to {target.name}")

            success = self.lifecycle.set_state(api_id, target, {"graduated_at": datetime.now(timezone.utc).isoformat()})
            if not success:
                return ActionResult(success=False, message="Graduation failed")

            return ActionResult(
                success=True,
                message=f"API {api_id} graduated to {target.name}",
                data={"api_id": api_id, "new_stage": target.name},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API graduation failed: {e}")


class LifecycleEventHook(BaseAction):
    """Hook system for lifecycle state transitions."""
    action_type = "lifecycle_event_hook"
    display_name = "生命周期事件钩子"
    description = "在API生命周期状态转换时触发钩子"

    def __init__(self) -> None:
        super().__init__()
        self._event_log: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            api_id = params.get("api_id", "")
            payload = params.get("payload", {})
            if not event_type:
                return ActionResult(success=False, message="event_type is required")

            event = {
                "event_type": event_type,
                "api_id": api_id,
                "payload": payload,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "id": str(uuid.uuid4()),
            }
            self._event_log.append(event)
            return ActionResult(success=True, message=f"Event {event_type} logged", data=event)
        except Exception as e:
            return ActionResult(success=False, message=f"Event hook failed: {e}")
