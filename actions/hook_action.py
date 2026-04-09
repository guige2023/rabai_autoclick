"""
Hook Action Module.

Provides pre/post execution hooks for action lifecycle management.
Supports synchronous and asynchronous hooks with error handling.
"""

import time
import asyncio
import threading
from typing import Callable, Any, Optional, List, Dict
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps


class HookPhase(Enum):
    """Hook execution phases."""
    PRE_EXECUTE = "pre_execute"
    POST_EXECUTE = "post_execute"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"
    ON_COMPLETE = "on_complete"
    CLEANUP = "cleanup"


@dataclass
class Hook:
    """Represents a single hook."""
    name: str
    func: Callable
    phase: HookPhase
    order: int = 0
    enabled: bool = True
    timeout: Optional[float] = None
    retry_count: int = 0


@dataclass
class HookContext:
    """Context passed to hook functions."""
    action_name: str
    phase: HookPhase
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: Optional[Exception] = None
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class HookRegistry:
    """Registry for managing hooks."""

    def __init__(self):
        self._hooks: Dict[str, List[Hook]] = {phase: [] for phase in HookPhase}
        self._lock = threading.RLock()
        self._global_hooks: List[Hook] = []

    def register(
        self,
        action_name: str,
        hook: Hook,
    ) -> None:
        """Register a hook for an action."""
        with self._lock:
            if action_name == "*":
                self._global_hooks.append(hook)
            else:
                if action_name not in self._hooks:
                    self._hooks[action_name] = []
                self._hooks[action_name].append(hook)
            self._sort_hooks(action_name)

    def _sort_hooks(self, action_name: str) -> None:
        """Sort hooks by order."""
        if action_name == "*":
            self._global_hooks.sort(key=lambda h: h.order)
        else:
            self._hooks[action_name].sort(key=lambda h: h.order)

    def get_hooks(self, action_name: str, phase: HookPhase) -> List[Hook]:
        """Get all hooks for an action and phase."""
        with self._lock:
            action_hooks = self._hooks.get(action_name, [])
            global_ = [h for h in self._global_hooks if h.phase == phase and h.enabled]
            return action_hooks + global_

    def unregister(self, action_name: str, hook_name: str) -> bool:
        """Unregister a hook by name."""
        with self._lock:
            for hooks in self._hooks.values():
                for i, h in enumerate(hooks):
                    if h.name == hook_name:
                        hooks.pop(i)
                        return True
            for i, h in enumerate(self._global_hooks):
                if h.name == hook_name:
                    self._global_hooks.pop(i)
                    return True
            return False


class HookAction:
    """
    Action with hook support for lifecycle management.

    Example:
        hook_action = HookAction("my_action")
        hook_action.register_hook(
            name="log_start",
            func=lambda ctx: print(f"Starting {ctx.action_name}"),
            phase=HookPhase.PRE_EXECUTE,
        )
        result = hook_action.execute(lambda: do_work())
    """

    def __init__(self, name: str, registry: Optional[HookRegistry] = None):
        self.name = name
        self.registry = registry or HookAction._default_registry
        self._enabled = True

    @classmethod
    def create_hook(
        cls,
        name: str,
        func: Callable,
        phase: HookPhase,
        order: int = 0,
        timeout: Optional[float] = None,
        retry_count: int = 0,
    ) -> Hook:
        """Create a new hook."""
        return Hook(
            name=name,
            func=func,
            phase=phase,
            order=order,
            timeout=timeout,
            retry_count=retry_count,
        )

    def register_hook(
        self,
        name: str,
        func: Callable,
        phase: HookPhase,
        order: int = 0,
        timeout: Optional[float] = None,
    ) -> None:
        """Register a hook for this action."""
        hook = self.create_hook(name, func, phase, order, timeout)
        self.registry.register(self.name, hook)

    def _run_hook_sync(self, hook: Hook, context: HookContext) -> Any:
        """Run a single synchronous hook."""
        if hook.timeout:
            return self._run_with_timeout(hook.func, context, hook.timeout)
        return hook.func(context)

    def _run_with_timeout(
        self,
        func: Callable,
        context: HookContext,
        timeout: float,
    ) -> Any:
        """Run function with timeout."""
        result = None
        error = None

        def target():
            nonlocal result, error
            try:
                result = func(context)
            except Exception as e:
                error = e

        t = threading.Thread(target=target)
        t.daemon = True
        t.start()
        t.join(timeout)

        if t.is_alive():
            raise TimeoutError(f"Hook {hook.name} timed out after {timeout}s")
        if error:
            raise error
        return result

    async def _run_hook_async(self, hook: Hook, context: HookContext) -> Any:
        """Run a single asynchronous hook."""
        if hook.timeout:
            return await asyncio.wait_for(hook.func(context), timeout=hook.timeout)
        return await hook.func(context)

    async def _execute_with_hooks_async(
        self,
        func: Callable,
        *args,
        **kwargs,
    ) -> Any:
        """Execute function with async hooks."""
        context = HookContext(
            action_name=self.name,
            phase=HookPhase.PRE_EXECUTE,
            args=args,
            kwargs=kwargs,
        )

        pre_hooks = self.registry.get_hooks(self.name, HookPhase.PRE_EXECUTE)
        for hook in pre_hooks:
            await self._run_hook_async(hook, context)

        context.phase = HookPhase.POST_EXECUTE
        result = None
        error = None

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            context.result = result
            context.phase = HookPhase.ON_SUCCESS
        except Exception as e:
            error = e
            context.error = e
            context.phase = HookPhase.ON_FAILURE
        finally:
            context.phase = HookPhase.ON_COMPLETE

        success_hooks = self.registry.get_hooks(self.name, HookPhase.ON_SUCCESS)
        failure_hooks = self.registry.get_hooks(self.name, HookPhase.ON_FAILURE)
        complete_hooks = self.registry.get_hooks(self.name, HookPhase.ON_COMPLETE)

        for hook in success_hooks if not error else failure_hooks:
            try:
                await self._run_hook_async(hook, context)
            except Exception:
                pass

        for hook in complete_hooks:
            try:
                await self._run_hook_async(hook, context)
            except Exception:
                pass

        if error:
            raise error
        return result

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with hooks (sync version)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._execute_with_hooks_async(func, *args, **kwargs),
                    loop,
                )
                return future.result(timeout=60.0)
            return asyncio.run(self._execute_with_hooks_async(func, *args, **kwargs))
        except RuntimeError:
            return asyncio.run(self._execute_with_hooks_async(func, *args, **kwargs))

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with hooks (async version)."""
        return await self._execute_with_hooks_async(func, *args, **kwargs)

    @staticmethod
    def _default_registry() -> HookRegistry:
        """Get or create default registry."""
        if not hasattr(HookAction, "_registry"):
            HookAction._registry = HookRegistry()
        return HookAction._registry

    def disable(self) -> None:
        """Disable this action's hooks."""
        self._enabled = False

    def enable(self) -> None:
        """Enable this action's hooks."""
        self._enabled = True
