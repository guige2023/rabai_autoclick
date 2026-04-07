"""atexit cleanup handlers for RabAI AutoClick.

Provides atexit operations:
- AtexitRegisterAction: Register cleanup function
- AtexitUnregisterAction: Unregister cleanup function
- AtexitClearAction: Clear all registered handlers
- AtexitListAction: List all registered handlers
- AtexitRunAction: Run all registered handlers manually
"""

from __future__ import annotations

import sys
import os
import atexit as _atexit
from typing import Any, Callable, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AtexitRegisterAction(BaseAction):
    """Register a function to be called at program exit."""
    action_type = "atexit_register"
    display_name = "注册退出处理"
    description = "注册程序退出时调用的函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit registration.

        Args:
            context: Execution context.
            params: Dict with func (callable), args (tuple), 
                   output_var.

        Returns:
            ActionResult with registration result.
        """
        func = params.get('func', None)
        args = params.get('args', None)
        output_var = params.get('output_var', 'atexit_result')

        if func is None:
            return ActionResult(success=False, message="func is required")

        try:
            resolved_func = context.resolve_value(func) if isinstance(func, str) else func

            if not callable(resolved_func):
                return ActionResult(success=False, message="func must be callable")

            resolved_args = ()
            if args is not None:
                resolved_args = context.resolve_value(args) if isinstance(args, str) else args
                if not isinstance(resolved_args, tuple):
                    resolved_args = (resolved_args,)

            result = _atexit.register(resolved_func, *resolved_args)

            context.set(output_var, result)
            return ActionResult(
                success=True,
                data=result,
                message=f"Registered: {resolved_func.__name__ if hasattr(resolved_func, '__name__') else str(resolved_func)}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Register error: {str(e)}")


class AtexitUnregisterAction(BaseAction):
    """Unregister a previously registered cleanup function."""
    action_type = "atexit_unregister"
    display_name = "注销退出处理"
    description = "注销之前注册的清理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit unregistration.

        Args:
            context: Execution context.
            params: Dict with func (callable), output_var.

        Returns:
            ActionResult with unregistration result.
        """
        func = params.get('func', None)
        output_var = params.get('output_var', 'atexit_unreg_result')

        if func is None:
            return ActionResult(success=False, message="func is required")

        try:
            resolved_func = context.resolve_value(func) if isinstance(func, str) else func

            if not callable(resolved_func):
                return ActionResult(success=False, message="func must be callable")

            _atexit.unregister(resolved_func)

            context.set(output_var, True)
            return ActionResult(
                success=True,
                data=True,
                message=f"Unregistered: {resolved_func.__name__ if hasattr(resolved_func, '__name__') else str(resolved_func)}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Unregister error: {str(e)}")


class AtexitClearAction(BaseAction):
    """Clear all registered atexit handlers."""
    action_type = "atexit_clear"
    display_name = "清除退出处理"
    description = "清除所有注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear result.
        """
        output_var = params.get('output_var', 'atexit_clear_result')

        try:
            _atexit.unregister(None)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="All atexit handlers cleared")

        except Exception as e:
            return ActionResult(success=False, message=f"Clear error: {str(e)}")


class AtexitListAction(BaseAction):
    """List all currently registered atexit handlers."""
    action_type = "atexit_list"
    display_name = "列出退出处理"
    description = "列出所有已注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit listing.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with list of handlers.
        """
        output_var = params.get('output_var', 'atexit_handlers')

        try:
            import gc
            handlers = []

            for obj in gc.get_objects():
                try:
                    if hasattr(obj, '__name__') and 'atexit' in str(type(obj)).lower():
                        handlers.append({
                            'type': type(obj).__name__,
                            'repr': repr(obj)
                        })
                except (ReferenceError, RuntimeError):
                    pass

            try:
                if hasattr(_atexit, '_exithandlers'):
                    exit_handlers = _atexit._exithandlers
                    handler_list = []
                    for h in exit_handlers:
                        if callable(h[0]):
                            handler_list.append({
                                'func': h[0].__name__ if hasattr(h[0], '__name__') else str(h[0]),
                                'args': h[1] if len(h) > 1 else (),
                                'kwargs': h[2] if len(h) > 2 else {}
                            })
                    handlers = handler_list
            except (AttributeError, TypeError):
                pass

            context.set(output_var, handlers)
            return ActionResult(
                success=True,
                data=handlers,
                message=f"Found {len(handlers)} registered handlers"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"List error: {str(e)}")


class AtexitRunAction(BaseAction):
    """Manually run all registered atexit handlers."""
    action_type = "atexit_run"
    display_name = "运行退出处理"
    description = "手动运行所有注册的退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit run.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with run result.
        """
        output_var = params.get('output_var', 'atexit_run_result')

        try:
            _atexit.run_exit()

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="All atexit handlers executed")

        except SystemExit:
            raise
        except Exception as e:
            return ActionResult(success=False, message=f"Run error: {str(e)}")


class AtexitDecoratorAction(BaseAction):
    """Register a function as atexit handler using decorator pattern."""
    action_type = "atexit_decorator"
    display_name = "装饰器注册退出"
    description = "使用装饰器模式注册退出处理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute atexit decorator.

        Args:
            context: Execution context.
            params: Dict with func (callable), args (tuple),
                   output_var.

        Returns:
            ActionResult with decorated function.
        """
        func = params.get('func', None)
        args = params.get('args', None)
        output_var = params.get('output_var', 'atexit_decorated')

        if func is None:
            return ActionResult(success=False, message="func is required")

        try:
            resolved_func = context.resolve_value(func) if isinstance(func, str) else func

            if not callable(resolved_func):
                return ActionResult(success=False, message="func must be callable")

            resolved_args = ()
            if args is not None:
                resolved_args = context.resolve_value(args) if isinstance(args, str) else args
                if not isinstance(resolved_args, tuple):
                    resolved_args = (resolved_args,)

            def decorated(*decorator_args, **decorator_kwargs):
                def wrapper(f):
                    _atexit.register(f, *resolved_args)
                    return f
                return wrapper(resolved_func)

            decorated_func = _atexit.register(resolved_func, *resolved_args)

            context.set(output_var, decorated_func)
            return ActionResult(
                success=True,
                data=decorated_func,
                message=f"Decorator registered: {resolved_func.__name__ if hasattr(resolved_func, '__name__') else str(resolved_func)}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Decorator error: {str(e)}")


class AtexitMultiRegisterAction(BaseAction):
    """Register multiple cleanup functions at once."""
    action_type = "atexit_multi_register"
    display_name = "批量注册退出处理"
    description = "批量注册多个清理函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute multi registration.

        Args:
            context: Execution context.
            params: Dict with handlers (list of func/args tuples),
                   output_var.

        Returns:
            ActionResult with registration results.
        """
        handlers = params.get('handlers', [])
        output_var = params.get('output_var', 'atexit_multi_result')

        if not handlers:
            return ActionResult(success=False, message="handlers list is required")

        try:
            registered = []
            errors = []

            for h in handlers:
                try:
                    if isinstance(h, dict):
                        func = h.get('func')
                        args = h.get('args', ())
                    elif isinstance(h, (list, tuple)) and len(h) >= 1:
                        func = h[0]
                        args = h[1] if len(h) > 1 else ()
                    else:
                        func = h
                        args = ()

                    resolved_func = context.resolve_value(func) if isinstance(func, str) else func
                    if not callable(resolved_func):
                        errors.append(f"Non-callable: {func}")
                        continue

                    resolved_args = context.resolve_value(args) if isinstance(args, str) else args
                    if not isinstance(resolved_args, tuple):
                        resolved_args = (resolved_args,)

                    _atexit.register(resolved_func, *resolved_args)
                    registered.append(resolved_func.__name__ if hasattr(resolved_func, '__name__') else str(resolved_func))

                except Exception as e:
                    errors.append(f"{h}: {str(e)}")

            context.set(output_var, {'registered': registered, 'errors': errors})
            return ActionResult(
                success=True,
                data={'registered': registered, 'errors': errors},
                message=f"Registered {len(registered)} handlers, {len(errors)} errors"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Multi-register error: {str(e)}")


class AtexitConditionalAction(BaseAction):
    """Register cleanup only if condition is met."""
    action_type = "atexit_conditional"
    display_name = "条件注册退出处理"
    description = "仅在条件满足时注册退出处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute conditional registration.

        Args:
            context: Execution context.
            params: Dict with func, condition, args, output_var.

        Returns:
            ActionResult with conditional result.
        """
        func = params.get('func', None)
        condition = params.get('condition', True)
        args = params.get('args', None)
        output_var = params.get('output_var', 'atexit_cond_result')

        if func is None:
            return ActionResult(success=False, message="func is required")

        try:
            resolved_condition = context.resolve_value(condition) if isinstance(condition, str) else condition

            if not resolved_condition:
                context.set(output_var, {'registered': False, 'reason': 'condition_false'})
                return ActionResult(success=True, data={'registered': False}, message="Condition not met, not registered")

            resolved_func = context.resolve_value(func) if isinstance(func, str) else func
            if not callable(resolved_func):
                return ActionResult(success=False, message="func must be callable")

            resolved_args = ()
            if args is not None:
                resolved_args = context.resolve_value(args) if isinstance(args, str) else args
                if not isinstance(resolved_args, tuple):
                    resolved_args = (resolved_args,)

            _atexit.register(resolved_func, *resolved_args)

            context.set(output_var, {'registered': True})
            return ActionResult(
                success=True,
                data={'registered': True},
                message=f"Conditionally registered: {resolved_func.__name__ if hasattr(resolved_func, '__name__') else str(resolved_func)}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Conditional error: {str(e)}")
