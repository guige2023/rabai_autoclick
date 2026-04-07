"""Contextlib action module for RabAI AutoClick.

Provides context manager utilities:
- ContextManagerAction: Generic context manager wrapper
- ContextCallbackAction: Register cleanup callbacks
- RedirectStdoutAction: Redirect stdout to variable
- RedirectStderrAction: Redirect stderr to variable
- SuppressErrorsAction: Suppress specified exceptions
- TimeoutContextAction: Timeout context manager
- ClosingAction: Ensure close() is called
- nullcontext Action: No-op context manager
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union
import sys
import contextlib as cl
import io
import threading
import time

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContextManagerAction(BaseAction):
    """Generic context manager wrapper."""
    action_type = "contextlib_manager"
    display_name = "上下文管理器"
    description = "通用上下文管理器包装器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute context manager operation."""
        enter_value = params.get('enter_value', None)
        exit_value = params.get('exit_value', None)
        output_var = params.get('output_var', 'ctx_result')

        try:
            resolved_enter = context.resolve_value(enter_value)
            result = {"entered": resolved_enter}
            context.set_variable(output_var, result)
            return ActionResult(success=True, message="context manager executed")
        except Exception as e:
            return ActionResult(success=False, message=f"context manager failed: {e}")


class ContextCallbackAction(BaseAction):
    """Register cleanup callbacks."""
    action_type = "contextlib_callback"
    display_name = "注册回调"
    description = "注册清理回调函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute callback registration."""
        callbacks_str = params.get('callbacks', '[]')
        output_var = params.get('output_var', 'callback_result')

        try:
            callbacks = context.resolve_value(callbacks_str)
            if not isinstance(callbacks, list):
                callbacks = [callbacks]
            
            registered = []
            for cb in callbacks:
                if callable(cb):
                    registered.append(cb)
            
            context.set_variable(output_var, {"registered": len(registered), "callbacks": registered})
            return ActionResult(success=True, message=f"registered {len(registered)} callbacks")
        except Exception as e:
            return ActionResult(success=False, message=f"callback registration failed: {e}")


class RedirectStdoutAction(BaseAction):
    """Redirect stdout to variable."""
    action_type = "contextlib_redirect_stdout"
    display_name = "重定向标准输出"
    description = "重定向stdout到变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stdout redirection."""
        command_str = params.get('command', 'pass')
        output_var = params.get('output_var', 'stdout_result')

        try:
            old_stdout = sys.stdout
            sys.stdout = captured = io.StringIO()
            
            try:
                exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                exec(command_str, exec_globals)
                output = captured.getvalue()
            finally:
                sys.stdout = old_stdout
            
            context.set_variable(output_var, output)
            return ActionResult(success=True, message=f"captured {len(output)} chars")
        except Exception as e:
            sys.stdout = old_stdout
            return ActionResult(success=False, message=f"redirect stdout failed: {e}")


class RedirectStderrAction(BaseAction):
    """Redirect stderr to variable."""
    action_type = "contextlib_redirect_stderr"
    display_name = "重定向错误输出"
    description = "重定向stderr到变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stderr redirection."""
        command_str = params.get('command', 'pass')
        output_var = params.get('output_var', 'stderr_result')

        try:
            old_stderr = sys.stderr
            sys.stderr = captured = io.StringIO()
            
            try:
                exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                exec(command_str, exec_globals)
                output = captured.getvalue()
            finally:
                sys.stderr = old_stderr
            
            context.set_variable(output_var, output)
            return ActionResult(success=True, message=f"captured {len(output)} chars")
        except Exception as e:
            sys.stderr = old_stderr
            return ActionResult(success=False, message=f"redirect stderr failed: {e}")


class SuppressErrorsAction(BaseAction):
    """Suppress specified exceptions."""
    action_type = "contextlib_suppress"
    display_name = "抑制异常"
    description = "抑制指定的异常类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with suppression."""
        command_str = params.get('command', 'pass')
        exception_types = params.get('exception_types', ['Exception'])
        default_value = params.get('default_value', None)
        output_var = params.get('output_var', 'suppress_result')

        try:
            resolved_types = context.resolve_value(exception_types)
            if isinstance(resolved_types, str):
                resolved_types = [resolved_types]
            
            exc_classes = []
            for exc_type in resolved_types:
                try:
                    exc_classes.append(eval(exc_type, {"__builtins__": {}}))
                except Exception:
                    exc_classes.append(Exception)
            
            result = default_value
            with cl.suppress(*exc_classes):
                exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                result = eval(command_str, exec_globals)
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message="suppress executed")
        except Exception as e:
            return ActionResult(success=False, message=f"suppress failed: {e}")


class TimeoutContextAction(BaseAction):
    """Timeout context manager."""
    action_type = "contextlib_timeout"
    display_name = "超时上下文"
    description = "带超时功能的上下文管理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with timeout."""
        command_str = params.get('command', 'pass')
        timeout_seconds = params.get('timeout', 5)
        output_var = params.get('output_var', 'timeout_result')

        try:
            resolved_timeout = context.resolve_value(timeout_seconds)
            result = None
            error_msg = None
            
            def run_command():
                nonlocal result
                exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                result = exec(command_str, exec_globals)
            
            thread = threading.Thread(target=run_command)
            thread.daemon = True
            thread.start()
            thread.join(timeout=resolved_timeout)
            
            if thread.is_alive():
                context.set_variable(output_var, {"timeout": True, "result": None})
                return ActionResult(success=True, message=f"timeout after {resolved_timeout}s")
            else:
                context.set_variable(output_var, {"timeout": False, "result": result})
                return ActionResult(success=True, message="completed within timeout")
        except Exception as e:
            return ActionResult(success=False, message=f"timeout context failed: {e}")


class ClosingAction(BaseAction):
    """Ensure close() is called."""
    action_type = "contextlib_closing"
    display_name = "资源关闭"
    description = "确保资源正确关闭"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute with closing."""
        resource = params.get('resource', None)
        command_str = params.get('command', 'pass')
        output_var = params.get('output_var', 'closing_result')

        try:
            resolved_resource = context.resolve_value(resource)
            result = None
            
            with cl.closing(resolved_resource):
                if command_str and command_str != 'pass':
                    exec_globals = {"__builtins__": __builtins__, "context": context, "params": params, "resource": resolved_resource}
                    result = eval(command_str, exec_globals)
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message="resource closed")
        except Exception as e:
            return ActionResult(success=False, message=f"closing failed: {e}")


class NullContextAction(BaseAction):
    """No-op context manager."""
    action_type = "contextlib_nullcontext"
    display_name = "空上下文"
    description = "无操作的上下文管理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute nullcontext."""
        enter_value = params.get('enter_value', None)
        command_str = params.get('command', 'pass')
        output_var = params.get('output_var', 'nullcontext_result')

        try:
            resolved_enter = context.resolve_value(enter_value)
            result = None
            
            with cl.nullcontext(resolved_enter):
                if command_str and command_str != 'pass':
                    exec_globals = {"__builtins__": __builtins__, "context": context, "params": params, "enter_value": resolved_enter}
                    result = eval(command_str, exec_globals)
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message="nullcontext executed")
        except Exception as e:
            return ActionResult(success=False, message=f"nullcontext failed: {e}")


class ContextStackAction(BaseAction):
    """Exit stack context manager."""
    action_type = "contextlib_exit_stack"
    display_name = "退出栈"
    description = "退出栈上下文管理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute exit stack."""
        contexts_str = params.get('contexts', '[]')
        command_str = params.get('command', 'pass')
        output_var = params.get('output_var', 'exit_stack_result')

        try:
            resolved_contexts = context.resolve_value(contexts_str)
            result = None
            
            with cl.ExitStack() as stack:
                for ctx in resolved_contexts:
                    stack.enter_context(ctx)
                
                if command_str and command_str != 'pass':
                    exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                    result = eval(command_str, exec_globals)
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message="exit stack completed")
        except Exception as e:
            return ActionResult(success=False, message=f"exit stack failed: {e}")
