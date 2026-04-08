"""API Lifecycle Management Action Module.

Manages the full lifecycle of API resources including creation,
updates, deprecation, and retirement with full audit trails.
"""

from __future__ import annotations

import sys
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LifecycleStage(Enum):
    """API lifecycle stages."""
    PLANNING = "planning"
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    ARCHIVED = "archived"


class LifecycleEvent(Enum):
    """Lifecycle event types."""
    CREATED = "created"
    UPDATED = "updated"
    DEPLOYED = "deployed"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    ARCHIVED = "archived"
    RESTORED = "restored"


@dataclass
class APIResource:
    """Represents an API resource with lifecycle metadata."""
    resource_id: str
    name: str
    version: str
    stage: LifecycleStage
    owner: str
    created_at: float
    updated_at: float
    deprecated_at: Optional[float] = None
    retired_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)


@dataclass
class LifecycleTransition:
    """Records a lifecycle transition event."""
    resource_id: str
    from_stage: LifecycleStage
    to_stage: LifecycleStage
    event: LifecycleEvent
    timestamp: float
    actor: str
    reason: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LifecyclePolicy:
    """Policy rules for lifecycle transitions."""
    require_approval: bool = True
    require_testing: bool = True
    require_backup: bool = True
    notify_stakeholders: bool = True
    grace_period_days: int = 90
    migration_guide_required: bool = True


class APILifecycleAction(BaseAction):
    """
    Manages API resource lifecycles with full audit trails.

    Handles creation, staging transitions, deprecation,
    retirement, and archival of API resources with policy enforcement.

    Example:
        lifecycle = APILifecycleAction()
        result = lifecycle.execute(ctx, {
            "action": "transition",
            "resource_id": "api-123",
            "to_stage": "production",
            "actor": "admin"
        })
    """
    action_type = "api_lifecycle"
    display_name = "API生命周期管理"
    description = "管理API资源的完整生命周期，包括创建、更新、废弃和退役"

    def __init__(self) -> None:
        super().__init__()
        self._resources: Dict[str, APIResource] = {}
        self._history: List[LifecycleTransition] = []
        self._policy = LifecyclePolicy()
        self._listeners: List[Callable[[LifecycleTransition], None]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a lifecycle management action.

        Args:
            context: Execution context.
            params: Dict with keys: action (create|transition|deprecate|
                   retire|archive|get_history|list), resource_id, to_stage,
                   actor, reason, resource_data.

        Returns:
            ActionResult with operation result.
        """
        action = params.get("action", "")

        try:
            if action == "create":
                return self._create_resource(params)
            elif action == "transition":
                return self._transition_resource(params)
            elif action == "deprecate":
                return self._deprecate_resource(params)
            elif action == "retire":
                return self._retire_resource(params)
            elif action == "archive":
                return self._archive_resource(params)
            elif action == "restore":
                return self._restore_resource(params)
            elif action == "get":
                return self._get_resource(params)
            elif action == "list":
                return self._list_resources(params)
            elif action == "get_history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Lifecycle action failed: {str(e)}"
            )

    def _create_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new API resource."""
        resource_data = params.get("resource_data", {})
        name = resource_data.get("name", "")
        version = resource_data.get("version", "1.0.0")
        owner = resource_data.get("owner", "system")

        if not name:
            return ActionResult(success=False, message="Resource name is required")

        resource_id = self._generate_resource_id(name, version)

        if resource_id in self._resources:
            return ActionResult(
                success=False,
                message=f"Resource already exists: {resource_id}"
            )

        resource = APIResource(
            resource_id=resource_id,
            name=name,
            version=version,
            stage=LifecycleStage.PLANNING,
            owner=owner,
            created_at=time.time(),
            updated_at=time.time(),
            tags=resource_data.get("tags", []),
            dependencies=resource_data.get("dependencies", []),
            metadata=resource_data.get("metadata", {}),
        )

        self._resources[resource_id] = resource

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=LifecycleStage.PLANNING,
            to_stage=LifecycleStage.PLANNING,
            event=LifecycleEvent.CREATED,
            timestamp=time.time(),
            actor=owner,
            reason="Initial creation",
        ))

        return ActionResult(
            success=True,
            message=f"Resource created: {resource_id}",
            data={"resource_id": resource_id, "stage": resource.stage.value}
        )

    def _transition_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Transition a resource to a new lifecycle stage."""
        resource_id = params.get("resource_id", "")
        to_stage_str = params.get("to_stage", "")
        actor = params.get("actor", "system")
        reason = params.get("reason", "")

        if not resource_id:
            return ActionResult(success=False, message="resource_id is required")
        if not to_stage_str:
            return ActionResult(success=False, message="to_stage is required")

        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        try:
            to_stage = LifecycleStage(to_stage_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid stage: {to_stage_str}"
            )

        resource = self._resources[resource_id]
        from_stage = resource.stage

        if not self._validate_transition(from_stage, to_stage):
            return ActionResult(
                success=False,
                message=f"Invalid transition from {from_stage.value} to {to_stage.value}"
            )

        event_map = {
            (LifecycleStage.PLANNING, LifecycleStage.DEVELOPMENT): LifecycleEvent.CREATED,
            (LifecycleStage.DEVELOPMENT, LifecycleStage.TESTING): LifecycleEvent.UPDATED,
            (LifecycleStage.TESTING, LifecycleStage.STAGING): LifecycleEvent.DEPLOYED,
            (LifecycleStage.STAGING, LifecycleStage.PRODUCTION): LifecycleEvent.DEPLOYED,
        }

        event = event_map.get((from_stage, to_stage), LifecycleEvent.UPDATED)

        resource.stage = to_stage
        resource.updated_at = time.time()

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=from_stage,
            to_stage=to_stage,
            event=event,
            timestamp=time.time(),
            actor=actor,
            reason=reason,
        ))

        return ActionResult(
            success=True,
            message=f"Transitioned {resource_id}: {from_stage.value} -> {to_stage.value}",
            data={"resource_id": resource_id, "stage": to_stage.value}
        )

    def _deprecate_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Mark a resource as deprecated."""
        resource_id = params.get("resource_id", "")
        actor = params.get("actor", "system")
        reason = params.get("reason", "")

        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        resource = self._resources[resource_id]
        from_stage = resource.stage

        if resource.stage == LifecycleStage.DEPRECATED:
            return ActionResult(success=False, message="Resource is already deprecated")

        grace_period = params.get("grace_period_days", self._policy.grace_period_days)
        resource.stage = LifecycleStage.DEPRECATED
        resource.deprecated_at = time.time()
        resource.updated_at = time.time()
        resource.metadata["grace_period_days"] = grace_period

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=from_stage,
            to_stage=LifecycleStage.DEPRECATED,
            event=LifecycleEvent.DEPRECATED,
            timestamp=time.time(),
            actor=actor,
            reason=reason,
            metadata={"grace_period_days": grace_period},
        ))

        return ActionResult(
            success=True,
            message=f"Resource deprecated: {resource_id}",
            data={"resource_id": resource_id, "grace_period_days": grace_period}
        )

    def _retire_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Retire a deprecated resource."""
        resource_id = params.get("resource_id", "")
        actor = params.get("actor", "system")
        reason = params.get("reason", "")

        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        resource = self._resources[resource_id]

        if resource.stage != LifecycleStage.DEPRECATED:
            return ActionResult(
                success=False,
                message="Can only retire deprecated resources"
            )

        resource.stage = LifecycleStage.RETIRED
        resource.retired_at = time.time()
        resource.updated_at = time.time()

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=LifecycleStage.DEPRECATED,
            to_stage=LifecycleStage.RETIRED,
            event=LifecycleEvent.RETIRED,
            timestamp=time.time(),
            actor=actor,
            reason=reason,
        ))

        return ActionResult(
            success=True,
            message=f"Resource retired: {resource_id}",
            data={"resource_id": resource_id}
        )

    def _archive_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Archive a retired resource."""
        resource_id = params.get("resource_id", "")
        actor = params.get("actor", "system")
        reason = params.get("reason", "")

        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        resource = self._resources[resource_id]

        if resource.stage not in (LifecycleStage.RETIRED, LifecycleStage.DEPRECATED):
            return ActionResult(
                success=False,
                message="Can only archive retired or deprecated resources"
            )

        old_stage = resource.stage
        resource.stage = LifecycleStage.ARCHIVED
        resource.updated_at = time.time()

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=old_stage,
            to_stage=LifecycleStage.ARCHIVED,
            event=LifecycleEvent.ARCHIVED,
            timestamp=time.time(),
            actor=actor,
            reason=reason,
        ))

        return ActionResult(
            success=True,
            message=f"Resource archived: {resource_id}",
            data={"resource_id": resource_id}
        )

    def _restore_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Restore an archived or retired resource."""
        resource_id = params.get("resource_id", "")
        actor = params.get("actor", "system")
        reason = params.get("reason", "")

        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        resource = self._resources[resource_id]
        old_stage = resource.stage

        if resource.stage not in (LifecycleStage.ARCHIVED, LifecycleStage.RETIRED):
            return ActionResult(
                success=False,
                message="Can only restore archived or retired resources"
            )

        resource.stage = LifecycleStage.PLANNING
        resource.updated_at = time.time()

        self._record_transition(LifecycleTransition(
            resource_id=resource_id,
            from_stage=old_stage,
            to_stage=LifecycleStage.PLANNING,
            event=LifecycleEvent.RESTORED,
            timestamp=time.time(),
            actor=actor,
            reason=reason,
        ))

        return ActionResult(
            success=True,
            message=f"Resource restored: {resource_id}",
            data={"resource_id": resource_id, "stage": LifecycleStage.PLANNING.value}
        )

    def _get_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Get a resource by ID."""
        resource_id = params.get("resource_id", "")
        if resource_id not in self._resources:
            return ActionResult(success=False, message=f"Resource not found: {resource_id}")

        resource = self._resources[resource_id]
        return ActionResult(
            success=True,
            data=self._serialize_resource(resource)
        )

    def _list_resources(self, params: Dict[str, Any]) -> ActionResult:
        """List resources with optional filters."""
        stage_filter = params.get("stage", None)
        tag_filter = params.get("tag", None)
        owner_filter = params.get("owner", None)

        resources = list(self._resources.values())

        if stage_filter:
            try:
                stage = LifecycleStage(stage_filter)
                resources = [r for r in resources if r.stage == stage]
            except ValueError:
                pass

        if tag_filter:
            resources = [r for r in resources if tag_filter in r.tags]

        if owner_filter:
            resources = [r for r in resources if r.owner == owner_filter]

        serialized = [self._serialize_resource(r) for r in resources]
        return ActionResult(
            success=True,
            data={"resources": serialized, "count": len(serialized)}
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get lifecycle history for a resource."""
        resource_id = params.get("resource_id", "")
        history = [h for h in self._history if h.resource_id == resource_id]

        if resource_id and not history:
            return ActionResult(success=False, message=f"No history for: {resource_id}")

        serialized = [
            {
                "resource_id": h.resource_id,
                "from_stage": h.from_stage.value,
                "to_stage": h.to_stage.value,
                "event": h.event.value,
                "timestamp": h.timestamp,
                "actor": h.actor,
                "reason": h.reason,
                "metadata": h.metadata,
            }
            for h in history
        ]

        return ActionResult(
            success=True,
            data={"history": serialized, "count": len(serialized)}
        )

    def _validate_transition(self, from_stage: LifecycleStage, to_stage: LifecycleStage) -> bool:
        """Validate if a stage transition is allowed."""
        valid_transitions = {
            LifecycleStage.PLANNING: [LifecycleStage.DEVELOPMENT],
            LifecycleStage.DEVELOPMENT: [LifecycleStage.TESTING, LifecycleStage.PLANNING],
            LifecycleStage.TESTING: [LifecycleStage.DEVELOPMENT, LifecycleStage.STAGING],
            LifecycleStage.STAGING: [LifecycleStage.TESTING, LifecycleStage.PRODUCTION],
            LifecycleStage.PRODUCTION: [LifecycleStage.DEPRECATED],
            LifecycleStage.DEPRECATED: [LifecycleStage.PRODUCTION, LifecycleStage.RETIRED],
            LifecycleStage.RETIRED: [LifecycleStage.ARCHIVED, LifecycleStage.DEPRECATED],
            LifecycleStage.ARCHIVED: [LifecycleStage.PLANNING],
        }
        return to_stage in valid_transitions.get(from_stage, [])

    def _record_transition(self, transition: LifecycleTransition) -> None:
        """Record a lifecycle transition."""
        self._history.append(transition)
        for listener in self._listeners:
            try:
                listener(transition)
            except Exception:
                pass

    def _generate_resource_id(self, name: str, version: str) -> str:
        """Generate a unique resource ID."""
        raw = f"{name}:{version}:{time.time()}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    def _serialize_resource(self, resource: APIResource) -> Dict[str, Any]:
        """Serialize a resource to a dictionary."""
        return {
            "resource_id": resource.resource_id,
            "name": resource.name,
            "version": resource.version,
            "stage": resource.stage.value,
            "owner": resource.owner,
            "created_at": resource.created_at,
            "updated_at": resource.updated_at,
            "deprecated_at": resource.deprecated_at,
            "retired_at": resource.retired_at,
            "tags": resource.tags,
            "dependencies": resource.dependencies,
            "metadata": resource.metadata,
        }

    def register_listener(self, listener: Callable[[LifecycleTransition], None]) -> None:
        """Register a listener for lifecycle events."""
        self._listeners.append(listener)

    def set_policy(self, policy: LifecyclePolicy) -> None:
        """Set the lifecycle policy."""
        self._policy = policy

    def get_active_resources(self) -> List[APIResource]:
        """Get all resources that are not archived or retired."""
        return [
            r for r in self._resources.values()
            if r.stage not in (LifecycleStage.ARCHIVED, LifecycleStage.RETIRED)
        ]

    def get_resources_needing_attention(self) -> List[APIResource]:
        """Get resources that need attention (deprecated without grace period remaining)."""
        now = time.time()
        attention_needed: List[APIResource] = []

        for resource in self._resources.values():
            if resource.stage == LifecycleStage.DEPRECATED and resource.deprecated_at:
                grace_remaining = resource.metadata.get("grace_period_days", 90) * 86400
                elapsed = now - resource.deprecated_at
                if elapsed > grace_remaining * 0.8:
                    attention_needed.append(resource)

        return attention_needed
