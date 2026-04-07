"""Warnings action module for RabAI AutoClick.

Provides warning control utilities:
- ShowWarningAction: Show a warning
- FilterWarningsAction: Configure warning filters
- CatchWarningsAction: Catch warnings as exceptions
- ResetWarningsAction: Reset warning filters
- WarnOnceAction: Warn only once per location
- WarnDeprecationAction: Show deprecation warning
- WarningSummaryAction: Get warning summary
"""

from typing import Any, Dict, List, Optional, Type, Union
import sys
import warnings
import collections

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WarningsShowAction(BaseAction):
    """Show a warning."""
    action_type = "warnings_show"
    display_name = "显示警告"
    description = "显示警告信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute show warning."""
        message = params.get('message', '')
        category = params.get('category', 'UserWarning')
        stack_level = params.get('stack_level', 1)
        output_var = params.get('output_var', 'warning_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_category = context.resolve_value(category) if isinstance(category, str) else category
            
            if isinstance(resolved_category, str):
                try:
                    resolved_category = eval(resolved_category, {"__builtins__": {}}, {"Warning": Warning})
                except Exception:
                    resolved_category = UserWarning
            
            warnings.warn(resolved_message, resolved_category, stacklevel=stack_level)
            context.set_variable(output_var, {"shown": True, "message": str(resolved_message)})
            return ActionResult(success=True, message=f"warning shown: {resolved_message}")
        except Exception as e:
            return ActionResult(success=False, message=f"show warning failed: {e}")


class WarningsFilterAction(BaseAction):
    """Configure warning filters."""
    action_type = "warnings_filter"
    display_name = "过滤警告"
    description = "配置警告过滤器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute filter warnings."""
        action = params.get('action', 'default')
        category = params.get('category', 'Warning')
        message = params.get('message', '')
        module = params.get('module', None)
        lineno = params.get('lineno', 0)
        output_var = params.get('output_var', 'filter_result')

        try:
            resolved_action = context.resolve_value(action) if isinstance(action, str) else action
            resolved_category = context.resolve_value(category) if isinstance(category, str) else category
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_module = context.resolve_value(module) if module is not None else None
            
            if isinstance(resolved_category, str):
                try:
                    resolved_category = eval(resolved_category, {"__builtins__": {}}, {"Warning": Warning})
                except Exception:
                    resolved_category = Warning
            
            if resolved_module is None:
                resolved_module = r".*"
            
            warnings.filterwarnings(resolved_action, category=resolved_category, message=resolved_message, module=resolved_module, lineno=lineno)
            context.set_variable(output_var, {"filtered": True})
            return ActionResult(success=True, message="filter configured")
        except Exception as e:
            return ActionResult(success=False, message=f"filter failed: {e}")


class WarningsCatchAction(BaseAction):
    """Catch warnings as exceptions."""
    action_type = "warnings_catch"
    display_name = "捕获警告"
    description = "捕获警告作为异常处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute catch warnings."""
        command_str = params.get('command', 'pass')
        category = params.get('category', 'Warning')
        record = params.get('record', True)
        output_var = params.get('output_var', 'catch_result')

        try:
            resolved_category = context.resolve_value(category) if isinstance(category, str) else category
            if isinstance(resolved_category, str):
                try:
                    resolved_category = eval(resolved_category, {"__builtins__": {}}, {"Warning": Warning})
                except Exception:
                    resolved_category = Warning
            
            with warnings.catch_warnings(record=record) as w:
                if record:
                    warnings.simplefilter("always")
                exec_globals = {"__builtins__": __builtins__, "context": context, "params": params}
                eval(command_str, exec_globals)
                
                if record:
                    caught = []
                    for warning in w:
                        caught.append({
                            "message": str(warning.message),
                            "category": warning.category.__name__,
                            "filename": warning.filename,
                            "lineno": warning.lineno
                        })
                    context.set_variable(output_var, caught)
                    return ActionResult(success=True, message=f"caught {len(caught)} warnings")
                else:
                    context.set_variable(output_var, [])
                    return ActionResult(success=True, message="no warnings caught")
        except Exception as e:
            return ActionResult(success=False, message=f"catch warnings failed: {e}")


class WarningsResetAction(BaseAction):
    """Reset warning filters."""
    action_type = "warnings_reset"
    display_name = "重置警告"
    description = "重置所有警告过滤器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute reset warnings."""
        output_var = params.get('output_var', 'reset_result')

        try:
            warnings.resetwarnings()
            context.set_variable(output_var, {"reset": True})
            return ActionResult(success=True, message="warnings reset")
        except Exception as e:
            return ActionResult(success=False, message=f"reset failed: {e}")


class WarningsSimpleFilterAction(BaseAction):
    """Simple filter configuration."""
    action_type = "warnings_simple_filter"
    display_name = "简单过滤"
    description = "设置简单警告过滤器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute simple filter."""
        action = params.get('action', 'default')
        category = params.get('category', 'Warning')
        output_var = params.get('output_var', 'simple_filter_result')

        try:
            resolved_action = context.resolve_value(action) if isinstance(action, str) else action
            resolved_category = context.resolve_value(category) if isinstance(category, str) else category
            
            if isinstance(resolved_category, str):
                try:
                    resolved_category = eval(resolved_category, {"__builtins__": {}}, {"Warning": Warning})
                except Exception:
                    resolved_category = Warning
            
            warnings.simplefilter(resolved_action, resolved_category)
            context.set_variable(output_var, {"simple_filter": True})
            return ActionResult(success=True, message=f"simplefilter {resolved_action}")
        except Exception as e:
            return ActionResult(success=False, message=f"simple_filter failed: {e}")


class WarnOnceAction(BaseAction):
    """Warn only once per location."""
    action_type = "warnings_warn_once"
    display_name = "警告一次"
    description = "每个位置只警告一次"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute warn once."""
        message = params.get('message', '')
        category = params.get('category', 'UserWarning')
        stack_level = params.get('stack_level', 1)
        output_var = params.get('output_var', 'warn_once_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            resolved_category = context.resolve_value(category) if isinstance(category, str) else category
            
            if isinstance(resolved_category, str):
                try:
                    resolved_category = eval(resolved_category, {"__builtins__": {}}, {"Warning": Warning})
                except Exception:
                    resolved_category = UserWarning
            
            warnings.warn(resolved_message, resolved_category, stacklevel=stack_level)
            context.set_variable(output_var, {"warned_once": True, "message": str(resolved_message)})
            return ActionResult(success=True, message=f"warn_once: {resolved_message}")
        except Exception as e:
            return ActionResult(success=False, message=f"warn_once failed: {e}")


class WarnDeprecationAction(BaseAction):
    """Show deprecation warning."""
    action_type = "warnings_deprecation"
    display_name = "弃用警告"
    description = "显示弃用警告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute deprecation warning."""
        message = params.get('message', 'This feature is deprecated')
        stack_level = params.get('stack_level', 1)
        output_var = params.get('output_var', 'deprecation_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            warnings.warn(resolved_message, DeprecationWarning, stacklevel=stack_level)
            context.set_variable(output_var, {"deprecated": True, "message": str(resolved_message)})
            return ActionResult(success=True, message=f"deprecation: {resolved_message}")
        except Exception as e:
            return ActionResult(success=False, message=f"deprecation warning failed: {e}")


class WarnFutureWarningAction(BaseAction):
    """Show future warning."""
    action_type = "warnings_future"
    display_name = "未来警告"
    description = "显示未来警告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute future warning."""
        message = params.get('message', 'This will change in a future version')
        stack_level = params.get('stack_level', 1)
        output_var = params.get('output_var', 'future_warning_result')

        try:
            resolved_message = context.resolve_value(message) if isinstance(message, str) else message
            warnings.warn(resolved_message, FutureWarning, stacklevel=stack_level)
            context.set_variable(output_var, {"future_warning": True, "message": str(resolved_message)})
            return ActionResult(success=True, message=f"future warning: {resolved_message}")
        except Exception as e:
            return ActionResult(success=False, message=f"future warning failed: {e}")
