"""Function action module for RabAI AutoClick.

Provides function calling and composition actions including
function execution, composition chains, partial application,
and result piping.
"""

import time
import sys
import os
import threading
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FunctionCall:
    """Represents a recorded function call.
    
    Attributes:
        id: Unique call identifier.
        name: Function name.
        args: Positional arguments.
        kwargs: Keyword arguments.
        result: Return value.
        duration: Execution time in seconds.
        timestamp: When the call was made.
        error: Error message if failed.
    """
    id: str
    name: str
    args: tuple
    kwargs: Dict[str, Any]
    result: Any = None
    duration: float = 0.0
    timestamp: float = 0.0
    error: Optional[str] = None


class FunctionRegistry:
    """Thread-safe registry for named functions.
    
    Allows registration, lookup, and execution of functions by name.
    """
    
    def __init__(self):
        self._functions: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._call_history: List[FunctionCall] = []
        self._max_history = 1000
    
    def register(self, name: str, func: Callable, overwrite: bool = False) -> bool:
        """Register a function under a name.
        
        Args:
            name: Name to register under.
            func: Callable to register.
            overwrite: Whether to overwrite existing registration.
        
        Returns:
            True if registered successfully.
        """
        with self._lock:
            if name in self._functions and not overwrite:
                return False
            self._functions[name] = func
            return True
    
    def unregister(self, name: str) -> bool:
        """Unregister a function.
        
        Args:
            name: Name of function to unregister.
        
        Returns:
            True if unregistered, False if not found.
        """
        with self._lock:
            if name in self._functions:
                del self._functions[name]
                return True
            return False
    
    def get(self, name: str) -> Optional[Callable]:
        """Get a registered function.
        
        Args:
            name: Function name.
        
        Returns:
            The registered callable or None.
        """
        with self._lock:
            return self._functions.get(name)
    
    def list_functions(self) -> List[str]:
        """List all registered function names."""
        with self._lock:
            return list(self._functions.keys())
    
    def execute(
        self,
        name: str,
        args: tuple = None,
        kwargs: Dict[str, Any] = None,
        record: bool = True
    ) -> FunctionCall:
        """Execute a registered function.
        
        Args:
            name: Function name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            record: Whether to record in history.
        
        Returns:
            FunctionCall with result or error.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        
        call_id = hashlib.md5(f"{name}{time.time()}".encode()).hexdigest()[:12]
        call = FunctionCall(
            id=call_id,
            name=name,
            args=args,
            kwargs=kwargs,
            timestamp=time.time()
        )
        
        func = self.get(name)
        if func is None:
            call.error = f"Function '{name}' not found"
            return call
        
        start = time.time()
        try:
            call.result = func(*args, **kwargs)
            call.duration = time.time() - start
        except Exception as e:
            call.error = str(e)
            call.duration = time.time() - start
        
        if record:
            with self._lock:
                self._call_history.append(call)
                if len(self._call_history) > self._max_history:
                    self._call_history.pop(0)
        
        return call
    
    def get_history(self, limit: int = 50) -> List[FunctionCall]:
        """Get recent function call history."""
        with self._lock:
            return self._call_history[-limit:]


# Global registry
_global_registry = FunctionRegistry()


class FunctionRegisterAction(BaseAction):
    """Register a function in the global registry."""
    action_type = "function_register"
    display_name = "函数注册"
    description = "向注册表注册函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Register a function.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, func_body (lambda string),
                   overwrite.
        
        Returns:
            ActionResult with registration status.
        """
        name = params.get('name', '')
        func_body = params.get('func_body', '')
        overwrite = params.get('overwrite', False)
        
        if not name:
            return ActionResult(success=False, message="Function name is required")
        
        if not func_body:
            return ActionResult(success=False, message="Function body is required")
        
        try:
            func = eval(f"lambda: {func_body}")
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid function body: {str(e)}")
        
        success = _global_registry.register(name, func, overwrite=overwrite)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Function '{name}' registered",
                data={"name": name}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Function '{name}' already exists (use overwrite=True)"
            )


class FunctionExecuteAction(BaseAction):
    """Execute a registered function."""
    action_type = "function_execute"
    display_name = "函数执行"
    description = "执行注册的函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a registered function.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, args, kwargs.
        
        Returns:
            ActionResult with function result or error.
        """
        name = params.get('name', '')
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        
        if not name:
            return ActionResult(success=False, message="Function name is required")
        
        if not isinstance(args, (list, tuple)):
            args = [args]
        
        call = _global_registry.execute(name, args=tuple(args), kwargs=kwargs)
        
        if call.error:
            return ActionResult(
                success=False,
                message=f"Function execution failed: {call.error}",
                data={
                    "function": name,
                    "duration": call.duration,
                    "error": call.error
                }
            )
        else:
            return ActionResult(
                success=True,
                message=f"Function '{name}' executed in {call.duration:.4f}s",
                data={
                    "function": name,
                    "result": call.result,
                    "duration": call.duration,
                    "call_id": call.id
                }
            )


class FunctionChainAction(BaseAction):
    """Execute multiple functions in sequence, passing results."""
    action_type = "function_chain"
    display_name = "函数链"
    description = "链式执行多个函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a chain of functions.
        
        Args:
            context: Execution context.
            params: Dict with keys: functions (list of names or dicts),
                   pass_result (whether to pass previous result as first arg).
        
        Returns:
            ActionResult with final result and chain history.
        """
        functions = params.get('functions', [])
        pass_result = params.get('pass_result', True)
        
        if not functions:
            return ActionResult(success=False, message="No functions specified")
        
        results = []
        current_result = None
        
        for i, func_spec in enumerate(functions):
            if isinstance(func_spec, str):
                name = func_spec
                args = []
                kwargs = {}
            elif isinstance(func_spec, dict):
                name = func_spec.get('name', '')
                args = func_spec.get('args', [])
                kwargs = func_spec.get('kwargs', {})
            else:
                results.append({"step": i, "error": "Invalid function spec"})
                continue
            
            if not name:
                results.append({"step": i, "error": "Missing function name"})
                continue
            
            if pass_result and current_result is not None:
                if isinstance(args, list):
                    args = [current_result] + list(args)
                else:
                    args = [current_result, args]
            
            call = _global_registry.execute(name, args=tuple(args) if isinstance(args, list) else (args,), kwargs=kwargs)
            
            if call.error:
                results.append({"step": i, "function": name, "error": call.error})
                return ActionResult(
                    success=False,
                    message=f"Chain failed at step {i}: {call.error}",
                    data={"failed_step": i, "results": results}
                )
            
            current_result = call.result
            results.append({"step": i, "function": name, "result": call.result, "duration": call.duration})
        
        return ActionResult(
            success=True,
            message=f"Chain of {len(functions)} functions completed",
            data={"results": results, "final_result": current_result}
        )


class FunctionListAction(BaseAction):
    """List all registered functions."""
    action_type = "function_list"
    display_name = "函数列表"
    description = "列出所有注册的函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List registered functions.
        
        Args:
            context: Execution context.
            params: Dict with keys: include_history, limit.
        
        Returns:
            ActionResult with function list and optional history.
        """
        include_history = params.get('include_history', False)
        history_limit = params.get('history_limit', 50)
        
        functions = _global_registry.list_functions()
        
        data = {"functions": functions, "count": len(functions)}
        
        if include_history:
            history = _global_registry.get_history(limit=history_limit)
            data["history"] = [
                {
                    "id": h.id,
                    "name": h.name,
                    "duration": h.duration,
                    "timestamp": h.timestamp,
                    "error": h.error
                }
                for h in history
            ]
        
        return ActionResult(
            success=True,
            message=f"{len(functions)} functions registered",
            data=data
        )


class FunctionUnregisterAction(BaseAction):
    """Unregister a function."""
    action_type = "function_unregister"
    display_name = "函数注销"
    description = "从注册表移除函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unregister a function.
        
        Args:
            context: Execution context.
            params: Dict with keys: name.
        
        Returns:
            ActionResult with unregistration status.
        """
        name = params.get('name', '')
        
        if not name:
            return ActionResult(success=False, message="Function name is required")
        
        success = _global_registry.unregister(name)
        
        if success:
            return ActionResult(success=True, message=f"Function '{name}' unregistered")
        else:
            return ActionResult(success=False, message=f"Function '{name}' not found")
