# Copyright (c) 2024. coded by claude
"""Automation Hooks Action Module.

Provides hook-based extensibility for automation workflows
with support for pre/post hooks, error hooks, and custom hook points.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class HookPoint(Enum):
    BEFORE_START = "before_start"
    AFTER_END = "after_end"
    BEFORE_ACTION = "before_action"
    AFTER_ACTION = "after_action"
    ON_ERROR = "on_error"
    ON_TIMEOUT = "on_timeout"
    ON_SUCCESS = "on_success"
    BEFORE_RETRY = "before_retry"


@dataclass
class HookContext:
    workflow_id: str
    step_id: Optional[str]
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[Exception] = None


@dataclass
class HookResult:
    success: bool
    modified_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class AutomationHooks:
    def __init__(self):
        self._hooks: Dict[HookPoint, List[Callable]] = {
            point: [] for point in HookPoint
        }
        self._global_hooks: List[Callable] = []

    def register_hook(self, point: HookPoint, hook: Callable) -> None:
        self._hooks[point].append(hook)

    def register_global_hook(self, hook: Callable) -> None:
        self._global_hooks.append(hook)

    async def execute_pre_start(self, workflow_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.BEFORE_START, workflow_id, None, context_data)

    async def execute_post_end(self, workflow_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.AFTER_END, workflow_id, None, context_data)

    async def execute_pre_action(self, workflow_id: str, step_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.BEFORE_ACTION, workflow_id, step_id, context_data)

    async def execute_post_action(self, workflow_id: str, step_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.AFTER_ACTION, workflow_id, step_id, context_data)

    async def execute_error(self, workflow_id: str, step_id: Optional[str], context_data: Dict[str, Any], error: Exception) -> HookResult:
        ctx = HookContext(
            workflow_id=workflow_id,
            step_id=step_id,
            timestamp=datetime.now(),
            data=context_data,
            error=error,
        )
        all_hooks = self._hooks[HookPoint.ON_ERROR] + self._global_hooks
        return await self._run_hooks(all_hooks, ctx)

    async def execute_timeout(self, workflow_id: str, step_id: Optional[str], context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.ON_TIMEOUT, workflow_id, step_id, context_data)

    async def execute_success(self, workflow_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.ON_SUCCESS, workflow_id, None, context_data)

    async def execute_before_retry(self, workflow_id: str, step_id: str, context_data: Dict[str, Any]) -> HookResult:
        return await self._execute_hooks(HookPoint.BEFORE_RETRY, workflow_id, step_id, context_data)

    async def _execute_hooks(self, point: HookPoint, workflow_id: str, step_id: Optional[str], context_data: Dict[str, Any]) -> HookResult:
        ctx = HookContext(
            workflow_id=workflow_id,
            step_id=step_id,
            timestamp=datetime.now(),
            data=context_data,
        )
        all_hooks = self._hooks[point] + self._global_hooks
        return await self._run_hooks(all_hooks, ctx)

    async def _run_hooks(self, hooks: List[Callable], ctx: HookContext) -> HookResult:
        modified_data = dict(ctx.data)
        for hook in hooks:
            try:
                result = hook(ctx)
                if hasattr(result, "__await__"):
                    result = await result
                if result and isinstance(result, dict):
                    modified_data.update(result)
            except Exception as e:
                logger.error(f"Hook execution failed: {e}")
                return HookResult(success=False, modified_data=None, error=str(e))
        return HookResult(success=True, modified_data=modified_data)

    def unregister_hook(self, point: HookPoint, hook: Callable) -> bool:
        if hook in self._hooks[point]:
            self._hooks[point].remove(hook)
            return True
        return False

    def clear_hooks(self, point: Optional[HookPoint] = None) -> None:
        if point:
            self._hooks[point].clear()
        else:
            for p in HookPoint:
                self._hooks[p].clear()
            self._global_hooks.clear()
