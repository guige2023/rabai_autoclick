"""Warning control and handling for RabAI AutoClick.

Provides warning operations:
- WarningsCatchAction: Catch warnings during execution
- WarningsFilterAction: Add warning filter
- WarningsSimpleFilterAction: Simple filter for warnings
- WarningsWarnAction: Issue a warning
- WarningsShowWarningAction: Show warning to user
- WarningsCaptureAction: Capture warnings as list
- WarningsResetAction: Reset all warning filters
"""

from __future__ import annotations

import sys
import os
import warnings
import functools
from typing import Any, Callable, Dict, List, Optional, Type

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WarningsCatchAction(BaseAction):
    """Catch and handle warnings during execution."""
    action_type = "warnings_catch"
    display_name = "警告捕获"
    description = "在执行期间捕获警告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute warning catching.

        Args:
            context: Execution context.
            params: Dict with body (callable), category (warning class),
                   lineno (include line numbers), output_var.

        Returns:
            ActionResult with caught warnings list.
        """
        body = params.get('body', None)
        category = params.get('category', Warning)
        lineno = params.get('lineno', True)
        output_var = params.get('output_var', 'caught_warnings')

        try:
            resolved_category = category
            if isinstance(category, str):
                resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            caught = []

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", resolved_category)
                if body:
                    resolved_body = context.resolve_value(body) if isinstance(body, str) else body
                    if callable(resolved_body):
                        result = resolved_body()
                    else:
                        result = resolved_body
                else:
                    result = None

                for warning in w:
                    warn_dict = {
                        'message': str(warning.message),
                        'category': warning.category.__name__,
                        'filename': warning.filename,
                        'lineno': warning.lineno,
                        'line': warning.line
                    }
                    caught.append(warn_dict)

            context.set(output_var, caught)
            context.set(f"{output_var}_result", result)

            return ActionResult(
                success=True,
                data={'warnings': caught, 'result': result},
                message=f"Caught {len(caught)} warnings"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Catch warnings error: {str(e)}")


class WarningsFilterAction(BaseAction):
    """Add warning filter with action and message pattern."""
    action_type = "warnings_filter"
    display_name = "警告过滤器"
    description = "添加警告过滤器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute filter addition.

        Args:
            context: Execution context.
            params: Dict with action (error/ignore/always/default/once),
                   message_pattern, category, module, lineno, output_var.

        Returns:
            ActionResult with filter result.
        """
        action = params.get('action', 'default')
        message = params.get('message_pattern', '')
        category = params.get('category', Warning)
        module = params.get('module_pattern', '')
        lineno = params.get('lineno', 0)
        output_var = params.get('output_var', 'filter_result')

        try:
            valid_actions = ['default', 'error', 'ignore', 'always', 'once', 'module']
            if action not in valid_actions:
                return ActionResult(success=False, message=f"Invalid action. Must be one of {valid_actions}")

            resolved_category = category
            if isinstance(category, str):
                if category == 'Warning':
                    resolved_category = Warning
                else:
                    resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_module = context.resolve_value(module) if isinstance(module, str) else module
            resolved_lineno = context.resolve_value(lineno) if isinstance(lineno, str) else lineno

            warnings.filterwarnings(
                action,
                message=resolved_message or '',
                category=resolved_category,
                module=resolved_module or '',
                lineno=resolved_lineno if resolved_lineno else 0
            )

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message=f"Filter added: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class WarningsSimpleFilterAction(BaseAction):
    """Simple warning filter for a category."""
    action_type = "warnings_simple_filter"
    display_name = "简单警告过滤"
    description = "对指定类别设置简单警告过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute simple filter.

        Args:
            context: Execution context.
            params: Dict with action, category, output_var.

        Returns:
            ActionResult with filter result.
        """
        action = params.get('action', 'always')
        category = params.get('category', Warning)
        output_var = params.get('output_var', 'simple_filter_result')

        try:
            resolved_category = category
            if isinstance(category, str):
                if category == 'Warning':
                    resolved_category = Warning
                else:
                    resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            warnings.simplefilter(action, resolved_category)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message=f"Simple filter: {action} for {resolved_category.__name__}")

        except Exception as e:
            return ActionResult(success=False, message=f"Simple filter error: {str(e)}")


class WarningsWarnAction(BaseAction):
    """Issue a warning."""
    action_type = "warnings_warn"
    display_name = "发出警告"
    description = "发出警告消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute warning issuance.

        Args:
            context: Execution context.
            params: Dict with message, category, stack_level, output_var.

        Returns:
            ActionResult with warning result.
        """
        message = params.get('message', '')
        category = params.get('category', UserWarning)
        stack_level = params.get('stack_level', 1)
        output_var = params.get('output_var', 'warn_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_category = category
            if isinstance(category, str):
                if category == 'Warning':
                    resolved_category = Warning
                elif category == 'UserWarning':
                    resolved_category = UserWarning
                else:
                    resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning, "UserWarning": UserWarning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            resolved_stack = context.resolve_value(stack_level) if isinstance(stack_level, str) else stack_level

            warnings.warn(resolved_message, category=resolved_category, stack_level=resolved_stack or 1)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message=f"Warning issued: {resolved_message}")

        except Exception as e:
            return ActionResult(success=False, message=f"Warn error: {str(e)}")


class WarningsShowWarningAction(BaseAction):
    """Show warning to user via standard warning display."""
    action_type = "warnings_show"
    display_name = "显示警告"
    description = "通过标准方式显示警告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute warning display.

        Args:
            context: Execution context.
            params: Dict with message, category, filename, lineno, file (optional),
                   line (optional), output_var.

        Returns:
            ActionResult with display result.
        """
        message = params.get('message', '')
        category = params.get('category', UserWarning)
        filename = params.get('filename', '<unknown>')
        lineno = params.get('lineno', 0)
        file_handle = params.get('file', None)
        line_text = params.get('line', None)
        output_var = params.get('output_var', 'show_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_filename = context.resolve_value(filename) if isinstance(filename, str) else filename
            resolved_lineno = context.resolve_value(lineno) if isinstance(lineno, str) else lineno

            resolved_category = category
            if isinstance(category, str):
                if category == 'Warning':
                    resolved_category = Warning
                else:
                    resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            if file_handle:
                resolved_file = context.resolve_value(file_handle) if isinstance(file_handle, str) else file_handle
            else:
                resolved_file = sys.stderr

            warn_message = warnings.WarningMessage(
                message=resolved_message,
                category=resolved_category,
                filename=resolved_filename,
                lineno=resolved_lineno,
                file=resolved_file,
                line=line_text
            )

            warnings.warn_explicit(warn_message)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="Warning shown")

        except Exception as e:
            return ActionResult(success=False, message=f"Show warning error: {str(e)}")


class WarningsCaptureAction(BaseAction):
    """Capture all warnings during execution as a list."""
    action_type = "warnings_capture"
    display_name = "警告捕获列表"
    description = "执行期间捕获所有警告为列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute warning capture.

        Args:
            context: Execution context.
            params: Dict with body (callable), output_var.

        Returns:
            ActionResult with captured warnings.
        """
        body = params.get('body', None)
        output_var = params.get('output_var', 'captured_warnings')

        try:
            captured = []

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                if body:
                    resolved_body = context.resolve_value(body) if isinstance(body, str) else body
                    if callable(resolved_body):
                        result = resolved_body()
                    else:
                        result = resolved_body
                else:
                    result = None

                for warning in w:
                    captured.append({
                        'message': str(warning.message),
                        'category': warning.category.__name__,
                        'filename': warning.filename,
                        'lineno': warning.lineno
                    })

            context.set(output_var, captured)
            return ActionResult(
                success=True,
                data={'warnings': captured, 'result': result},
                message=f"Captured {len(captured)} warnings"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Capture error: {str(e)}")


class WarningsResetAction(BaseAction):
    """Reset all warning filters to default."""
    action_type = "warnings_reset"
    display_name = "重置警告过滤器"
    description = "重置所有警告过滤器为默认"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute filter reset.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with reset result.
        """
        output_var = params.get('output_var', 'reset_result')

        try:
            warnings.resetwarnings()
            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="Warning filters reset")

        except Exception as e:
            return ActionResult(success=False, message=f"Reset error: {str(e)}")


class WarningsSimpleAction(BaseAction):
    """Simple warn-once mechanism using functools."""
    action_type = "warnings_simple"
    display_name = "单次警告"
    description = "使用functools实现仅警告一次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute simple once-per-call warning.

        Args:
            context: Execution context.
            params: Dict with message, category, output_var.

        Returns:
            ActionResult with warning result.
        """
        message = params.get('message', '')
        category = params.get('category', UserWarning)
        output_var = params.get('output_var', 'simple_warn_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message

            resolved_category = category
            if isinstance(category, str):
                if category == 'Warning':
                    resolved_category = Warning
                else:
                    resolved_category = eval(category, {"__builtins__": __builtins__, "Warning": Warning})
            elif isinstance(category, type) and issubclass(category, Warning):
                resolved_category = category

            @functools.lru_cache(maxsize=None)
            def warn_once(msg, cat):
                warnings.warn(msg, category=cat)
                return True

            warn_once(str(resolved_message), resolved_category)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message=f"Warn-once: {resolved_message}")

        except Exception as e:
            return ActionResult(success=False, message=f"Simple warn error: {str(e)}")
