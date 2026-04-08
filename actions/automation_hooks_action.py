"""
Automation Hooks Action Module

Provides hook-based automation for lifecycle events, callbacks, and event handling.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import asyncio


class HookType(Enum):
    """Types of hooks."""
    PRE = "pre"
    POST = "post"
    ON_ERROR = "on_error"
    ON_SUCCESS = "on_success"
    ON_COMPLETE = "on_complete"
    ON_TIMEOUT = "on_timeout"
    ON_CANCEL = "on_cancel"


@dataclass
class Hook:
    """A hook definition."""
    hook_id: str
    hook_type: HookType
    name: str
    callback: Callable[..., Awaitable]
    filter: Optional[Callable[[dict], bool]] = None
    priority: int = 0
    timeout_seconds: float = 30.0
    async_execution: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HookContext:
    """Context passed to hook callbacks."""
    operation_name: str
    operation_id: str
    timestamp: datetime
    input_data: dict[str, Any]
    output_data: Optional[Any] = None
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HookExecutionResult:
    """Result of hook execution."""
    hook_id: str
    success: bool
    execution_time_ms: float
    error: Optional[str] = None
    output: Any = None


@dataclass
class HookRegistration:
    """Registration details for a hook."""
    hook: Hook
    registered_at: datetime = field(default_factory=datetime.now)
    execution_count: int = 0
    failure_count: int = 0


class AutomationHooksAction:
    """Main hook automation action handler."""
    
    def __init__(self):
        self._hooks: dict[str, list[HookRegistration]] = defaultdict(list)
        self._global_hooks: dict[HookType, list[Hook]] = defaultdict(list)
        self._execution_history: list[HookExecutionResult] = []
        self._max_history: int = 1000
        self._stats: dict[str, Any] = defaultdict(int)
    
    def register_hook(
        self,
        operation: str,
        hook: Hook
    ) -> "AutomationHooksAction":
        """Register a hook for an operation."""
        registration = HookRegistration(hook=hook)
        self._hooks[operation].append(registration)
        
        # Sort by priority (higher priority first)
        self._hooks[operation].sort(
            key=lambda r: r.hook.priority,
            reverse=True
        )
        
        return self
    
    def register_global_hook(
        self,
        hook_type: HookType,
        callback: Callable[..., Awaitable],
        priority: int = 0
    ) -> "AutomationHooksAction":
        """Register a global hook that runs for all operations."""
        hook = Hook(
            hook_id=f"global_{hook_type.value}_{len(self._global_hooks[hook_type])}",
            hook_type=hook_type,
            name=f"Global {hook_type.value}",
            callback=callback,
            priority=priority
        )
        
        self._global_hooks[hook_type].append(hook)
        self._global_hooks[hook_type].sort(key=lambda h: h.priority, reverse=True)
        
        return self
    
    def unregister_hook(
        self,
        operation: str,
        hook_id: str
    ) -> bool:
        """Unregister a hook."""
        if operation not in self._hooks:
            return False
        
        original_len = len(self._hooks[operation])
        self._hooks[operation] = [
            r for r in self._hooks[operation]
            if r.hook.hook_id != hook_id
        ]
        
        return len(self._hooks[operation]) < original_len
    
    async def execute_hooks(
        self,
        operation: str,
        hook_type: HookType,
        context: HookContext
    ) -> list[HookExecutionResult]:
        """
        Execute all hooks for an operation and hook type.
        
        Args:
            operation: Operation name
            hook_type: Type of hooks to execute
            context: Hook context with operation details
            
        Returns:
            List of execution results
        """
        results = []
        
        # Execute global hooks first
        for hook in self._global_hooks.get(hook_type, []):
            if hook.filter and not hook.filter(context.input_data):
                continue
            
            result = await self._execute_single_hook(hook, context)
            results.append(result)
        
        # Execute operation-specific hooks
        for registration in self._hooks.get(operation, []):
            hook = registration.hook
            
            if hook.hook_type != hook_type:
                continue
            
            if hook.filter and not hook.filter(context.input_data):
                continue
            
            result = await self._execute_single_hook(hook, context)
            registration.execution_count += 1
            
            if not result.success:
                registration.failure_count += 1
            
            results.append(result)
        
        self._stats["hooks_executed"] += len(results)
        
        return results
    
    async def _execute_single_hook(
        self,
        hook: Hook,
        context: HookContext
    ) -> HookExecutionResult:
        """Execute a single hook with timeout."""
        start_time = datetime.now()
        
        try:
            if asyncio.iscoroutinefunction(hook.callback):
                result = await asyncio.wait_for(
                    hook.callback(context),
                    timeout=hook.timeout_seconds
                )
            else:
                result = hook.callback(context)
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            execution_result = HookExecutionResult(
                hook_id=hook.hook_id,
                success=True,
                execution_time_ms=execution_time,
                output=result
            )
            
        except asyncio.TimeoutError:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            execution_result = HookExecutionResult(
                hook_id=hook.hook_id,
                success=False,
                execution_time_ms=execution_time,
                error=f"Hook execution timed out after {hook.timeout_seconds}s"
            )
            self._stats["hook_timeouts"] += 1
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            execution_result = HookExecutionResult(
                hook_id=hook.hook_id,
                success=False,
                execution_time_ms=execution_time,
                error=str(e)
            )
            self._stats["hook_errors"] += 1
        
        # Record in history
        self._execution_history.append(execution_result)
        if len(self._execution_history) > self._max_history:
            self._execution_history = self._execution_history[-self._max_history:]
        
        return execution_result
    
    async def execute_pre_hooks(
        self,
        operation: str,
        operation_id: str,
        input_data: dict[str, Any]
    ) -> list[HookExecutionResult]:
        """Execute pre-operation hooks."""
        context = HookContext(
            operation_name=operation,
            operation_id=operation_id,
            timestamp=datetime.now(),
            input_data=input_data
        )
        
        return await self.execute_hooks(operation, HookType.PRE, context)
    
    async def execute_post_hooks(
        self,
        operation: str,
        operation_id: str,
        input_data: dict[str, Any],
        output_data: Any
    ) -> list[HookExecutionResult]:
        """Execute post-operation hooks."""
        context = HookContext(
            operation_name=operation,
            operation_id=operation_id,
            timestamp=datetime.now(),
            input_data=input_data,
            output_data=output_data
        )
        
        return await self.execute_hooks(operation, HookType.POST, context)
    
    async def execute_error_hooks(
        self,
        operation: str,
        operation_id: str,
        input_data: dict[str, Any],
        error: Exception
    ) -> list[HookExecutionResult]:
        """Execute error hooks."""
        context = HookContext(
            operation_name=operation,
            operation_id=operation_id,
            timestamp=datetime.now(),
            input_data=input_data,
            error=str(error)
        )
        
        return await self.execute_hooks(operation, HookType.ON_ERROR, context)
    
    async def execute_success_hooks(
        self,
        operation: str,
        operation_id: str,
        input_data: dict[str, Any],
        output_data: Any
    ) -> list[HookExecutionResult]:
        """Execute success hooks."""
        context = HookContext(
            operation_name=operation,
            operation_id=operation_id,
            timestamp=datetime.now(),
            input_data=input_data,
            output_data=output_data
        )
        
        return await self.execute_hooks(operation, HookType.ON_SUCCESS, context)
    
    async def wrap_operation(
        self,
        operation_name: str,
        operation_id: str,
        input_data: dict[str, Any],
        operation: Callable[[], Awaitable[Any]]
    ) -> tuple[Any, list[HookExecutionResult]]:
        """
        Wrap an operation with all relevant hooks.
        
        Executes pre-hooks, runs operation, then executes post/success/error hooks.
        
        Returns:
            Tuple of (result, list of all hook execution results)
        """
        all_results = []
        
        # Execute pre-hooks
        pre_results = await self.execute_pre_hooks(
            operation_name, operation_id, input_data
        )
        all_results.extend(pre_results)
        
        # Check if any pre-hook failed critically
        if any(not r.success for r in pre_results):
            # Execute error hooks
            error_results = await self.execute_error_hooks(
                operation_name, operation_id, input_data,
                Exception("Pre-hook failed")
            )
            all_results.extend(error_results)
            raise Exception("Pre-hook execution failed")
        
        try:
            # Execute the operation
            result = await operation()
            
            # Execute success hooks
            success_results = await self.execute_success_hooks(
                operation_name, operation_id, input_data, result
            )
            all_results.extend(success_results)
            
            # Execute post-hooks
            post_results = await self.execute_post_hooks(
                operation_name, operation_id, input_data, result
            )
            all_results.extend(post_results)
            
            return result, all_results
            
        except Exception as e:
            # Execute error hooks
            error_results = await self.execute_error_hooks(
                operation_name, operation_id, input_data, e
            )
            all_results.extend(error_results)
            raise
    
    def get_hooks_for_operation(
        self,
        operation: str,
        hook_type: Optional[HookType] = None
    ) -> list[HookRegistration]:
        """Get all hooks registered for an operation."""
        registrations = self._hooks.get(operation, [])
        
        if hook_type:
            registrations = [
                r for r in registrations
                if r.hook.hook_type == hook_type
            ]
        
        return registrations
    
    def get_global_hooks(
        self,
        hook_type: Optional[HookType] = None
    ) -> list[Hook]:
        """Get all global hooks."""
        if hook_type:
            return list(self._global_hooks.get(hook_type, []))
        return [
            hook for hooks in self._global_hooks.values()
            for hook in hooks
        ]
    
    def get_stats(self) -> dict[str, Any]:
        """Get hook execution statistics."""
        return {
            "total_hooks_registered": sum(len(hooks) for hooks in self._hooks.values()),
            "global_hooks_registered": len([
                h for hooks in self._global_hooks.values() for h in hooks
            ]),
            "hooks_executed": self._stats["hooks_executed"],
            "hook_errors": self._stats["hook_errors"],
            "hook_timeouts": self._stats["hook_timeouts"],
            "execution_history_size": len(self._execution_history)
        }
    
    def get_execution_history(
        self,
        hook_id: Optional[str] = None,
        limit: int = 100
    ) -> list[HookExecutionResult]:
        """Get hook execution history."""
        history = self._execution_history
        
        if hook_id:
            history = [r for r in history if r.hook_id == hook_id]
        
        return history[-limit:]
