"""Automation Hooks and Lifecycle.

This module provides automation lifecycle hooks:
- Pre/post execution hooks
- Hook ordering and priority
- Conditional hook execution
- Hook error handling

Example:
    >>> from actions.automation_hooks_action import HookManager
    >>> hooks = HookManager()
    >>> hooks.register("pre_execute", my_pre_hook, priority=10)
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import IntEnum

logger = logging.getLogger(__name__)


class HookPhase(IntEnum):
    """Lifecycle phases for hooks."""
    PRE_SETUP = 1
    POST_SETUP = 2
    PRE_EXECUTE = 3
    POST_EXECUTE = 4
    PRE_CLEANUP = 5
    POST_CLEANUP = 6
    ON_ERROR = 7
    ON_SUCCESS = 8
    ON_COMPLETE = 9


@dataclass
class Hook:
    """A lifecycle hook."""
    name: str
    phase: HookPhase
    func: Callable[..., Any]
    priority: int = 100
    enabled: bool = True
    conditional: Optional[Callable[[dict], bool]] = None
    description: str = ""
    call_count: int = 0


@dataclass
class HookContext:
    """Context passed to hook functions."""
    phase: HookPhase
    timestamp: float
    task_name: str
    task_id: str
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: Optional[str] = None


class HookManager:
    """Manages automation lifecycle hooks."""

    def __init__(self) -> None:
        """Initialize the hook manager."""
        self._hooks: dict[HookPhase, list[Hook]] = {phase: [] for phase in HookPhase}
        self._lock = threading.RLock()
        self._stats = {"hooks_called": 0, "hooks_failed": 0}

    def register(
        self,
        phase: HookPhase,
        func: Callable[[HookContext], Any],
        name: str = "",
        priority: int = 100,
        conditional: Optional[Callable[[dict], bool]] = None,
        description: str = "",
    ) -> Hook:
        """Register a hook.

        Args:
            phase: Lifecycle phase.
            func: Hook function.
            name: Hook name.
            priority: Execution priority (lower = earlier).
            conditional: Optional condition function.
            description: Hook description.

        Returns:
            Created Hook.
        """
        hook_name = name or func.__name__
        hook = Hook(
            name=hook_name,
            phase=phase,
            func=func,
            priority=priority,
            conditional=conditional,
            description=description,
        )

        with self._lock:
            self._hooks[phase].append(hook)
            self._hooks[phase].sort(key=lambda h: h.priority)

        logger.info("Registered hook: %s (phase=%s, priority=%d)", hook_name, phase.name, priority)
        return hook

    def unregister(self, name: str, phase: Optional[HookPhase] = None) -> bool:
        """Unregister a hook.

        Args:
            name: Hook name.
            phase: Specific phase. None = all phases.

        Returns:
            True if unregistered.
        """
        with self._lock:
            if phase:
                hooks = [h for h in self._hooks[phase] if h.name != name]
                self._hooks[phase] = hooks
                return len(hooks) < len(hooks) + 1
            else:
                found = False
                for p in HookPhase:
                    before = len(self._hooks[p])
                    self._hooks[p] = [h for h in self._hooks[p] if h.name != name]
                    if len(self._hooks[p]) < before:
                        found = True
                return found

    def execute_phase(
        self,
        phase: HookPhase,
        context: HookContext,
    ) -> list[Any]:
        """Execute all hooks for a phase.

        Args:
            phase: Lifecycle phase.
            context: Hook context.

        Returns:
            List of hook results.
        """
        with self._lock:
            hooks = [h for h in self._hooks[phase] if h.enabled]

        results = []

        for hook in hooks:
            if hook.conditional:
                try:
                    if not hook.conditional(context.__dict__):
                        continue
                except Exception as e:
                    logger.error("Hook conditional failed for %s: %s", hook.name, e)
                    continue

            try:
                result = hook.func(context)
                hook.call_count += 1
                self._stats["hooks_called"] += 1
                results.append(result)
            except Exception as e:
                logger.error("Hook %s failed: %s", hook.name, e)
                self._stats["hooks_failed"] += 1

        return results

    def execute_pre_setup(
        self,
        task_name: str,
        task_id: str,
        **kwargs,
    ) -> list[Any]:
        """Execute pre-setup hooks."""
        ctx = HookContext(
            phase=HookPhase.PRE_SETUP,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.PRE_SETUP, ctx)

    def execute_post_setup(
        self,
        task_name: str,
        task_id: str,
        result: Any = None,
        **kwargs,
    ) -> list[Any]:
        """Execute post-setup hooks."""
        ctx = HookContext(
            phase=HookPhase.POST_SETUP,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            result=result,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.POST_SETUP, ctx)

    def execute_pre_execute(
        self,
        task_name: str,
        task_id: str,
        **kwargs,
    ) -> list[Any]:
        """Execute pre-execute hooks."""
        ctx = HookContext(
            phase=HookPhase.PRE_EXECUTE,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.PRE_EXECUTE, ctx)

    def execute_post_execute(
        self,
        task_name: str,
        task_id: str,
        result: Any = None,
        **kwargs,
    ) -> list[Any]:
        """Execute post-execute hooks."""
        ctx = HookContext(
            phase=HookPhase.POST_EXECUTE,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            result=result,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.POST_EXECUTE, ctx)

    def execute_on_error(
        self,
        task_name: str,
        task_id: str,
        error: str,
        **kwargs,
    ) -> list[Any]:
        """Execute error hooks."""
        ctx = HookContext(
            phase=HookPhase.ON_ERROR,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            error=error,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.ON_ERROR, ctx)

    def execute_on_success(
        self,
        task_name: str,
        task_id: str,
        result: Any = None,
        **kwargs,
    ) -> list[Any]:
        """Execute success hooks."""
        ctx = HookContext(
            phase=HookPhase.ON_SUCCESS,
            timestamp=time.time(),
            task_name=task_name,
            task_id=task_id,
            result=result,
            kwargs=kwargs,
        )
        return self.execute_phase(HookPhase.ON_SUCCESS, ctx)

    def list_hooks(self, phase: Optional[HookPhase] = None) -> list[Hook]:
        """List registered hooks."""
        with self._lock:
            if phase:
                return list(self._hooks[phase])
            all_hooks = []
            for hooks in self._hooks.values():
                all_hooks.extend(hooks)
            return all_hooks

    def get_stats(self) -> dict[str, int]:
        """Get hook statistics."""
        with self._lock:
            return {
                **self._stats,
                "total_hooks": sum(len(h) for h in self._hooks.values()),
            }
