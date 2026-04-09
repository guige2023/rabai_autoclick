"""Hook system for automation lifecycle events.

Provides a flexible hook/extension mechanism that allows automation
workflows to register callbacks at specific lifecycle points.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import uuid


class HookPhase(Enum):
    """Phases of the automation lifecycle where hooks can fire."""
    BEFORE_INIT = "before_init"
    AFTER_INIT = "after_init"
    BEFORE_EXECUTE = "before_execute"
    AFTER_EXECUTE = "after_execute"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"
    ON_TIMEOUT = "on_timeout"
    ON_RETRY = "on_retry"
    BEFORE_CLEANUP = "before_cleanup"
    AFTER_CLEANUP = "after_cleanup"
    ON_SHUTDOWN = "on_shutdown"


class HookPriority(Enum):
    """Priority levels for hook execution order."""
    FIRST = 0
    HIGH = 25
    NORMAL = 50
    LOW = 75
    LAST = 100


@dataclass
class HookRegistration:
    """A registered hook callback."""
    hook_id: str
    phase: HookPhase
    callback: Callable[..., Any]
    priority: HookPriority = HookPriority.NORMAL
    name: Optional[str] = None
    enabled: bool = True
    once: bool = False
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None
    created_at: float = field(default_factory=time.time)
    call_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class HookEvent:
    """Event object passed to hook callbacks."""
    phase: HookPhase
    workflow_id: str
    action_name: str
    context: Dict[str, Any]
    params: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "phase": self.phase.value,
            "workflow_id": self.workflow_id,
            "action_name": self.action_name,
            "timestamp": datetime.fromtimestamp(self.timestamp).isoformat(),
            "has_context": bool(self.context),
            "has_params": bool(self.params),
            "has_result": self.result is not None,
            "has_error": self.error is not None,
            "metadata": self.metadata,
        }


class HookRegistry:
    """Central registry for all hook registrations."""

    def __init__(self):
        self._hooks: Dict[HookPhase, List[HookRegistration]] = {
            phase: [] for phase in HookPhase
        }
        self._lock = threading.RLock()
        self._global_tags: Dict[str, Any] = {}

    def register(
        self,
        phase: HookPhase,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
        once: bool = False,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        tags: Optional[List[str]] = None,
    ) -> str:
        """Register a hook callback.

        Returns a unique hook_id that can be used to unregister.
        """
        hook_id = str(uuid.uuid4())[:12]

        registration = HookRegistration(
            hook_id=hook_id,
            phase=phase,
            callback=callback,
            priority=priority,
            name=name,
            once=once,
            filter_fn=filter_fn,
            tags=tags or [],
        )

        with self._lock:
            phase_hooks = self._hooks[phase]
            phase_hooks.append(registration)
            phase_hooks.sort(key=lambda h: (h.priority.value, h.created_at))

        return hook_id

    def unregister(self, hook_id: str) -> bool:
        """Unregister a hook by its ID."""
        with self._lock:
            for phase_hooks in self._hooks.values():
                for i, hook in enumerate(phase_hooks):
                    if hook.hook_id == hook_id:
                        phase_hooks.pop(i)
                        return True
        return False

    def unregister_by_tag(self, tag: str) -> int:
        """Unregister all hooks with a specific tag. Returns count removed."""
        count = 0
        with self._lock:
            for phase_hooks in self._hooks.values():
                to_remove = [h for h in phase_hooks if tag in h.tags]
                for hook in to_remove:
                    phase_hooks.remove(hook)
                    count += 1
        return count

    def get_hooks(self, phase: Optional[HookPhase] = None) -> List[HookRegistration]:
        """Get registered hooks, optionally filtered by phase."""
        with self._lock:
            if phase:
                return list(self._hooks.get(phase, []))
            all_hooks = []
            for hooks in self._hooks.values():
                all_hooks.extend(hooks)
            return all_hooks

    def disable_hook(self, hook_id: str) -> bool:
        """Disable a hook without removing it."""
        with self._lock:
            for phase_hooks in self._hooks.values():
                for hook in phase_hooks:
                    if hook.hook_id == hook_id:
                        hook.enabled = False
                        return True
        return False

    def enable_hook(self, hook_id: str) -> bool:
        """Enable a previously disabled hook."""
        with self._lock:
            for phase_hooks in self._hooks.values():
                for hook in phase_hooks:
                    if hook.hook_id == hook_id:
                        hook.enabled = True
                        return True
        return False

    def set_global_tag(self, key: str, value: Any) -> None:
        """Set a global tag available to all hooks."""
        self._global_tags[key] = value

    def get_global_tags(self) -> Dict[str, Any]:
        """Get all global tags."""
        return dict(self._global_tags)


class HookExecutor:
    """Executes hooks for a given lifecycle phase."""

    def __init__(self, registry: HookRegistry):
        self._registry = registry

    def execute(
        self,
        phase: HookPhase,
        workflow_id: str,
        action_name: str,
        context: Dict[str, Any],
        params: Dict[str, Any],
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        """Execute all hooks for a phase.

        Returns list of hook results.
        """
        event = HookEvent(
            phase=phase,
            workflow_id=workflow_id,
            action_name=action_name,
            context=context,
            params=params,
            result=result,
            error=error,
            metadata=metadata or {},
        )

        hooks = self._registry.get_hooks(phase)
        results = []

        for hook in hooks:
            if not hook.enabled:
                continue

            if hook.filter_fn and not hook.filter_fn(event.to_dict()):
                continue

            try:
                result_val = hook.callback(event)
                hook.call_count += 1
                results.append(result_val)

                if hook.once:
                    self._registry.unregister(hook.hook_id)

            except Exception as e:
                hook.error_count += 1
                hook.last_error = str(e)
                results.append(
                    HookError(hook_id=hook.hook_id, error=str(e))
                )

        return results


@dataclass
class HookError:
    """Error from a hook execution."""
    hook_id: str
    error: str


class AutomationHookAction:
    """Action providing hook capabilities for automation workflows."""

    def __init__(self, registry: Optional[HookRegistry] = None):
        self._registry = registry or HookRegistry()
        self._executor = HookExecutor(self._registry)

    def before_init(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a before-init hook."""
        return self._registry.register(
            HookPhase.BEFORE_INIT, callback, priority, name
        )

    def after_init(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register an after-init hook."""
        return self._registry.register(
            HookPhase.AFTER_INIT, callback, priority, name
        )

    def before_execute(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a before-execute hook."""
        return self._registry.register(
            HookPhase.BEFORE_EXECUTE, callback, priority, name
        )

    def after_execute(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register an after-execute hook."""
        return self._registry.register(
            HookPhase.AFTER_EXECUTE, callback, priority, name
        )

    def on_success(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a success hook."""
        return self._registry.register(
            HookPhase.ON_SUCCESS, callback, priority, name
        )

    def on_failure(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a failure hook."""
        return self._registry.register(
            HookPhase.ON_FAILURE, callback, priority, name
        )

    def on_retry(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a retry hook."""
        return self._registry.register(
            HookPhase.ON_RETRY, callback, priority, name
        )

    def on_timeout(
        self,
        callback: Callable[..., Any],
        priority: HookPriority = HookPriority.NORMAL,
        name: Optional[str] = None,
    ) -> str:
        """Register a timeout hook."""
        return self._registry.register(
            HookPhase.ON_TIMEOUT, callback, priority, name
        )

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an automation with hook lifecycle management.

        Required params:
            workflow_id: str - Unique workflow identifier
            action_name: str - Name of the action being executed
            operation: callable - The operation to execute

        Optional params:
            enable_hooks: bool - Whether to run hooks (default True)
            hook_metadata: dict - Additional metadata for hook events
        """
        workflow_id = params.get("workflow_id", str(uuid.uuid4()))
        action_name = params.get("action_name", "unnamed")
        operation = params.get("operation")
        enable_hooks = params.get("enable_hooks", True)
        hook_metadata = params.get("hook_metadata", {})

        if not callable(operation):
            raise ValueError("operation must be a callable")

        result = None
        error = None

        if enable_hooks:
            self._executor.execute(
                HookPhase.BEFORE_INIT,
                workflow_id, action_name, context, params,
            )

        try:
            init_result = None
            if enable_hooks:
                init_result = self._executor.execute(
                    HookPhase.AFTER_INIT,
                    workflow_id, action_name, context, params,
                )

            if enable_hooks:
                self._executor.execute(
                    HookPhase.BEFORE_EXECUTE,
                    workflow_id, action_name, context, params,
                )

            result = operation(context=context, params=params)

            if enable_hooks:
                self._executor.execute(
                    HookPhase.AFTER_EXECUTE,
                    workflow_id, action_name, context, params, result=result,
                )
                self._executor.execute(
                    HookPhase.ON_SUCCESS,
                    workflow_id, action_name, context, params, result=result,
                )

        except TimeoutError as e:
            error = str(e)
            if enable_hooks:
                self._executor.execute(
                    HookPhase.ON_TIMEOUT,
                    workflow_id, action_name, context, params, error=error,
                )
                self._executor.execute(
                    HookPhase.AFTER_CLEANUP,
                    workflow_id, action_name, context, params, error=error,
                )

        except Exception as e:
            error = str(e)
            if enable_hooks:
                self._executor.execute(
                    HookPhase.ON_FAILURE,
                    workflow_id, action_name, context, params, error=error,
                )
                self._executor.execute(
                    HookPhase.AFTER_CLEANUP,
                    workflow_id, action_name, context, params, error=error,
                )

        finally:
            if enable_hooks:
                self._executor.execute(
                    HookPhase.ON_SHUTDOWN,
                    workflow_id, action_name, context, params,
                    result=result, error=error,
                )

        return {
            "workflow_id": workflow_id,
            "action_name": action_name,
            "result": result,
            "error": error,
            "success": error is None,
        }

    def list_hooks(
        self,
        phase: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List registered hooks."""
        phase_enum = HookPhase(phase) if phase else None
        hooks = self._registry.get_hooks(phase_enum)
        return [
            {
                "hook_id": h.hook_id,
                "phase": h.phase.value,
                "name": h.name,
                "priority": h.priority.name,
                "enabled": h.enabled,
                "once": h.once,
                "call_count": h.call_count,
                "error_count": h.error_count,
                "last_error": h.last_error,
                "tags": h.tags,
            }
            for h in hooks
        ]

    def unregister(self, hook_id: str) -> bool:
        """Unregister a hook by ID."""
        return self._registry.unregister(hook_id)

    def fire(
        self,
        phase: str,
        workflow_id: str,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> List[Any]:
        """Manually fire hooks for a specific phase."""
        try:
            phase_enum = HookPhase(phase)
        except ValueError:
            raise ValueError(f"Invalid phase: {phase}")
        return self._executor.execute(
            phase_enum, workflow_id, "manual", context, params,
        )
