"""
Automation Hooks Action Module.

Provides hook/extension points for automation
workflows with event handling.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class HookType(Enum):
    """Hook types."""
    PRE_EXECUTE = "pre_execute"
    POST_EXECUTE = "post_execute"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"
    ON_TIMEOUT = "on_timeout"
    PRE_COMMIT = "pre_commit"
    POST_COMMIT = "post_commit"


@dataclass
class Hook:
    """Automation hook."""
    hook_id: str
    name: str
    hook_type: HookType
    handler: Callable
    enabled: bool = True
    order: int = 0
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HookContext:
    """Context passed to hooks."""
    workflow_id: Optional[str] = None
    action_id: Optional[str] = None
    action_name: Optional[str] = None
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class HookExecutor:
    """Executes hooks."""

    def __init__(self):
        self.hooks: Dict[HookType, List[Hook]] = {
            hook_type: [] for hook_type in HookType
        }

    def register(self, hook: Hook):
        """Register a hook."""
        self.hooks[hook.hook_type].append(hook)
        self.hooks[hook.hook_type].sort(key=lambda h: h.order)

    def unregister(self, hook_id: str) -> bool:
        """Unregister a hook."""
        for hooks in self.hooks.values():
            for i, hook in enumerate(hooks):
                if hook.hook_id == hook_id:
                    hooks.pop(i)
                    return True
        return False

    def get_hooks(self, hook_type: HookType) -> List[Hook]:
        """Get hooks for type."""
        return [h for h in self.hooks.get(hook_type, []) if h.enabled]

    async def execute(self, hook_type: HookType, context: HookContext) -> List[Any]:
        """Execute all hooks of a type."""
        results = []
        hooks = self.get_hooks(hook_type)

        for hook in hooks:
            try:
                result = await asyncio.wait_for(
                    hook.handler(context),
                    timeout=hook.timeout
                )
                results.append(result)
            except asyncio.TimeoutError:
                logger.warning(f"Hook {hook.name} timed out")
            except Exception as e:
                logger.error(f"Hook {hook.name} error: {e}")

        return results

    async def execute_pre_execute(self, context: HookContext) -> List[Any]:
        """Execute pre-execute hooks."""
        return await self.execute(HookType.PRE_EXECUTE, context)

    async def execute_post_execute(self, context: HookContext) -> List[Any]:
        """Execute post-execute hooks."""
        return await self.execute(HookType.POST_EXECUTE, context)

    async def execute_on_success(self, context: HookContext) -> List[Any]:
        """Execute success hooks."""
        return await self.execute(HookType.ON_SUCCESS, context)

    async def execute_on_failure(self, context: HookContext) -> List[Any]:
        """Execute failure hooks."""
        return await self.execute(HookType.ON_FAILURE, context)


class WorkflowHookManager:
    """Manages workflow-level hooks."""

    def __init__(self, executor: HookExecutor):
        self.executor = executor
        self.workflow_hooks: Dict[str, List[Hook]] = {}

    def register_workflow_hook(
        self,
        workflow_id: str,
        hook: Hook
    ):
        """Register hook for specific workflow."""
        if workflow_id not in self.workflow_hooks:
            self.workflow_hooks[workflow_id] = []
        self.workflow_hooks[workflow_id].append(hook)

    async def execute_workflow_hooks(
        self,
        workflow_id: str,
        hook_type: HookType,
        context: HookContext
    ) -> List[Any]:
        """Execute hooks for specific workflow."""
        hooks = self.workflow_hooks.get(workflow_id, [])
        hooks = [h for h in hooks if h.hook_type == hook_type and h.enabled]

        results = []
        for hook in hooks:
            try:
                result = await asyncio.wait_for(
                    hook.handler(context),
                    timeout=hook.timeout
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Workflow hook error: {e}")

        return results


def create_hook(
    name: str,
    hook_type: HookType,
    handler: Callable
) -> Hook:
    """Create a new hook."""
    import uuid
    return Hook(
        hook_id=str(uuid.uuid4()),
        name=name,
        hook_type=hook_type,
        handler=handler
    )


def main():
    """Demonstrate hooks."""
    executor = HookExecutor()

    async def pre_hook(context: HookContext):
        print(f"Pre-execute: {context.action_name}")

    async def post_hook(context: HookContext):
        print(f"Post-execute: {context.action_name}")

    executor.register(create_hook("pre", HookType.PRE_EXECUTE, pre_hook))
    executor.register(create_hook("post", HookType.POST_EXECUTE, post_hook))

    context = HookContext(
        action_name="test_action",
        input_data={"test": "data"}
    )

    asyncio.run(executor.execute_pre_execute(context))


if __name__ == "__main__":
    main()
