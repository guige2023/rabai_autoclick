"""Atexit action module for RabAI AutoClick.

Provides cleanup handler utilities:
- RegisterExitHandlerAction: Register cleanup function
- UnregisterHandlerAction: Unregister exit handler
- RunExitHandlersAction: Manually run exit handlers
- GetRegisteredHandlersAction: List registered handlers
- ExitWithCodeAction: Exit with code
"""

from typing import Any, Callable, Dict, List, Optional, Union
import sys
import atexit

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


_RegisteredHandlers = []


def _make_handler(func: Callable, args: tuple, kwargs: dict) -> Callable:
    """Create a handler that calls the provided function."""
    def handler():
        try:
            func(*args, **kwargs)
        except Exception as e:
            sys.stderr.write(f"Exit handler error: {e}\n")
    return handler


class AtexitRegisterAction(BaseAction):
    """Register cleanup function."""
    action_type = "atexit_register"
    display_name = "注册退出处理"
    description = "注册程序退出时的清理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute register."""
        func_str = params.get('function', None)
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        output_var = params.get('output_var', 'register_result')

        try:
            if not func_str:
                return ActionResult(success=False, message="function is required")
            
            resolved_args = context.resolve_value(args) if isinstance(args, str) else args
            resolved_kwargs = context.resolve_value(kwargs) if isinstance(kwargs, dict) else {}
            
            func = eval(func_str, {"__builtins__": __builtins__}, {"context": context, "params": params})
            
            handler = _make_handler(func, tuple(resolved_args), resolved_kwargs)
            atexit.register(handler)
            _RegisteredHandlers.append(func)
            
            context.set_variable(output_var, {"registered": True, "handler_count": len(_RegisteredHandlers)})
            return ActionResult(success=True, message=f"registered exit handler")
        except Exception as e:
            return ActionResult(success=False, message=f"register failed: {e}")


class AtexitUnregisterAction(BaseAction):
    """Unregister exit handler."""
    action_type = "atexit_unregister"
    display_name = "取消注册"
    description = "取消注册退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute unregister."""
        func_str = params.get('function', None)
        output_var = params.get('output_var', 'unregister_result')

        try:
            if not func_str:
                return ActionResult(success=False, message="function is required")
            
            func = eval(func_str, {"__builtins__": __builtins__}, {})
            
            if func in _RegisteredHandlers:
                _RegisteredHandlers.remove(func)
                try:
                    for h in atexit._exithandlers:
                        if h[0].__name__ == '_make_handler':
                            atexit.unregister(h[0])
                except Exception:
                    pass
            
            context.set_variable(output_var, {"unregistered": True})
            return ActionResult(success=True, message=f"unregistered exit handler")
        except Exception as e:
            return ActionResult(success=False, message=f"unregister failed: {e}")


class AtexitRunHandlersAction(BaseAction):
    """Manually run exit handlers."""
    action_type = "atexit_run"
    display_name = "运行退出处理"
    description = "手动运行所有注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute run handlers."""
        output_var = params.get('output_var', 'run_handlers_result')

        try:
            count = len(_RegisteredHandlers)
            for handler in _RegisteredHandlers[:]:
                try:
                    handler()
                except Exception as e:
                    sys.stderr.write(f"Exit handler error: {e}\n")
            
            context.set_variable(output_var, {"ran": count})
            return ActionResult(success=True, message=f"ran {count} handlers")
        except Exception as e:
            return ActionResult(success=False, message=f"run handlers failed: {e}")


class AtexitListHandlersAction(BaseAction):
    """List registered handlers."""
    action_type = "atexit_list"
    display_name = "列出处理函数"
    description = "列出所有注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute list handlers."""
        output_var = params.get('output_var', 'list_handlers_result')

        try:
            handlers_info = []
            for handler in _RegisteredHandlers:
                handlers_info.append({
                    "name": getattr(handler, '__name__', str(handler)),
                    "type": type(handler).__name__
                })
            
            context.set_variable(output_var, {"handlers": handlers_info, "count": len(handlers_info)})
            return ActionResult(success=True, message=f"listed {len(handlers_info)} handlers")
        except Exception as e:
            return ActionResult(success=False, message=f"list handlers failed: {e}")


class AtexitClearHandlersAction(BaseAction):
    """Clear all handlers."""
    action_type = "atexit_clear"
    display_name = "清除处理函数"
    description = "清除所有注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clear handlers."""
        output_var = params.get('output_var', 'clear_handlers_result')

        try:
            global _RegisteredHandlers
            count = len(_RegisteredHandlers)
            _RegisteredHandlers = []
            atexit._exithandlers[:] = []
            
            context.set_variable(output_var, {"cleared": count})
            return ActionResult(success=True, message=f"cleared {count} handlers")
        except Exception as e:
            return ActionResult(success=False, message=f"clear handlers failed: {e}")


class AtexitRegisterMultipleAction(BaseAction):
    """Register multiple cleanup functions."""
    action_type = "atexit_register_multiple"
    display_name = "批量注册"
    description = "批量注册多个退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute register multiple."""
        functions_str = params.get('functions', '[]')
        output_var = params.get('output_var', 'register_multiple_result')

        try:
            resolved_funcs = context.resolve_value(functions_str) if isinstance(functions_str, str) else functions_str
            
            if not isinstance(resolved_funcs, list):
                resolved_funcs = [resolved_funcs]
            
            registered = 0
            for func_info in resolved_funcs:
                if isinstance(func_info, dict):
                    func = eval(func_info.get('function', 'pass'), {"__builtins__": __builtins__}, {"context": context, "params": params})
                    args = func_info.get('args', [])
                    kwargs = func_info.get('kwargs', {})
                else:
                    func = func_info
                    args = []
                    kwargs = {}
                
                handler = _make_handler(func, tuple(args), kwargs)
                atexit.register(handler)
                _RegisteredHandlers.append(func)
                registered += 1
            
            context.set_variable(output_var, {"registered": registered, "total": len(_RegisteredHandlers)})
            return ActionResult(success=True, message=f"registered {registered} handlers")
        except Exception as e:
            return ActionResult(success=False, message=f"register multiple failed: {e}")


class AtexitRegisterWithPriorityAction(BaseAction):
    """Register with priority."""
    action_type = "atexit_register_priority"
    display_name = "优先级注册"
    description = "带优先级注册退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute register with priority."""
        func_str = params.get('function', None)
        priority = params.get('priority', 0)
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        output_var = params.get('output_var', 'register_priority_result')

        try:
            if not func_str:
                return ActionResult(success=False, message="function is required")
            
            resolved_priority = context.resolve_value(priority) if isinstance(priority, str) else priority
            resolved_args = context.resolve_value(args) if isinstance(args, str) else args
            
            func = eval(func_str, {"__builtins__": __builtins__}, {"context": context, "params": params})
            handler = _make_handler(func, tuple(resolved_args), kwargs)
            
            _RegisteredHandlers.append((func, resolved_priority))
            _RegisteredHandlers.sort(key=lambda x: x[1] if isinstance(x, tuple) else 0)
            atexit.register(handler)
            
            context.set_variable(output_var, {"registered": True, "priority": resolved_priority})
            return ActionResult(success=True, message=f"registered with priority {resolved_priority}")
        except Exception as e:
            return ActionResult(success=False, message=f"register priority failed: {e}")
