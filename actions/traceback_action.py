"""Traceback action module for RabAI AutoClick.

Provides exception traceback handling utilities:
- FormatExceptionAction: Format exception as string
- PrintExceptionAction: Print exception with traceback
- GetTracebackAction: Get current traceback
- WalkTracebackAction: Walk traceback frames
- ExtractTracebackAction: Extract traceback info
- ExceptionCauseAction: Get exception cause
- ChainExceptionsAction: Chain exceptions
- ReraiseAction: Reraise with cause
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import sys
import traceback

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TracebackFormatExceptionAction(BaseAction):
    """Format exception as string."""
    action_type = "traceback_format_exception"
    display_name = "格式化异常"
    description = "将异常格式化为字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute format exception."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'exception_formatted')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            formatted = traceback.format_exception(type(resolved_exc), resolved_exc, resolved_exc.__traceback__)
            result = "".join(formatted)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"formatted ({len(result)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"format_exception failed: {e}")


class TracebackPrintExceptionAction(BaseAction):
    """Print exception with traceback."""
    action_type = "traceback_print_exception"
    display_name = "打印异常"
    description = "打印异常及完整traceback"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute print exception."""
        exception = params.get('exception', None)
        file_var = params.get('file_var', None)
        output_var = params.get('output_var', 'print_result')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            import io
            captured = io.StringIO()
            traceback.print_exception(type(resolved_exc), resolved_exc, resolved_exc.__traceback__, file=captured)
            result = captured.getvalue()
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"printed ({len(result)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"print_exception failed: {e}")


class TracebackGetCurrentAction(BaseAction):
    """Get current traceback."""
    action_type = "traceback_get_current"
    display_name = "获取当前Traceback"
    description = "获取当前执行位置的traceback"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get current traceback."""
        limit = params.get('limit', None)
        output_var = params.get('output_var', 'current_traceback')

        try:
            if limit is not None:
                resolved_limit = context.resolve_value(limit)
            else:
                resolved_limit = None
            
            if resolved_limit:
                formatted = traceback.format_stack(limit=resolved_limit)
            else:
                formatted = traceback.format_stack()
            result = "".join(formatted)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"got traceback ({len(result)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"get_current failed: {e}")


class TracebackWalkAction(BaseAction):
    """Walk traceback frames."""
    action_type = "traceback_walk"
    display_name = "遍历Traceback"
    description = "遍历traceback的所有帧"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute walk traceback."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'traceback_walk_result')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            frames = []
            tb = resolved_exc.__traceback__
            while tb is not None:
                frames.append({
                    "filename": tb.tb_frame.f_code.co_filename,
                    "lineno": tb.tb_lineno,
                    "name": tb.tb_frame.f_code.co_name
                })
                tb = tb.tb_next
            context.set_variable(output_var, frames)
            return ActionResult(success=True, message=f"walked {len(frames)} frames")
        except Exception as e:
            return ActionResult(success=False, message=f"walk failed: {e}")


class TracebackExtractAction(BaseAction):
    """Extract traceback info."""
    action_type = "traceback_extract"
    display_name = "提取Traceback信息"
    description = "提取traceback详细信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute extract traceback."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'traceback_extract_result')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            tb = resolved_exc.__traceback__
            extracted = traceback.extract_tb(tb)
            result = []
            for frame in extracted:
                result.append({
                    "filename": frame.filename,
                    "lineno": frame.lineno,
                    "name": frame.name,
                    "line": frame.line
                })
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"extracted {len(result)} frames")
        except Exception as e:
            return ActionResult(success=False, message=f"extract failed: {e}")


class TracebackGetCauseAction(BaseAction):
    """Get exception cause."""
    action_type = "traceback_get_cause"
    display_name = "获取异常原因"
    description = "获取异常的cause"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get cause."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'exception_cause')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            cause = resolved_exc.__cause__
            context.set_variable(output_var, {
                "cause": str(cause) if cause else None,
                "cause_type": type(cause).__name__ if cause else None
            })
            return ActionResult(success=True, message=f"cause: {type(cause).__name__ if cause else None}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_cause failed: {e}")


class TracebackGetContextAction(BaseAction):
    """Get exception context."""
    action_type = "traceback_get_context"
    display_name = "获取异常上下文"
    description = "获取异常的context"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get context."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'exception_context')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            ctx = resolved_exc.__context__
            context.set_variable(output_var, {
                "context": str(ctx) if ctx else None,
                "context_type": type(ctx).__name__ if ctx else None,
                "suppressed": resolved_exc.__suppress_context__
            })
            return ActionResult(success=True, message=f"context: {type(ctx).__name__ if ctx else None}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_context failed: {e}")


class TracebackReraiseAction(BaseAction):
    """Reraise with cause."""
    action_type = "traceback_reraise"
    display_name = "重新抛出"
    description = "使用cause重新抛出异常"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute reraise."""
        exception = params.get('exception', None)
        cause = params.get('cause', None)
        output_var = params.get('output_var', 'reraise_result')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            resolved_cause = context.resolve_value(cause) if cause is not None else None
            
            if resolved_cause:
                raise resolved_exc from resolved_cause
            else:
                raise resolved_exc
        except Exception as e:
            return ActionResult(success=True, message=f"reraised: {type(resolved_exc).__name__}")
        except BaseException as e:
            return ActionResult(success=True, message=f"reraised base: {type(e).__name__}")


class TracebackSummaryAction(BaseAction):
    """Get compact traceback summary."""
    action_type = "traceback_summary"
    display_name = "Traceback摘要"
    description = "获取精简的traceback摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute summary."""
        exception = params.get('exception', None)
        output_var = params.get('output_var', 'traceback_summary')

        try:
            if exception is None:
                return ActionResult(success=False, message="exception is required")
            
            resolved_exc = context.resolve_value(exception) if not isinstance(exception, BaseException) else exception
            summary = traceback.format_exc()
            context.set_variable(output_var, summary)
            return ActionResult(success=True, message=f"summary ({len(summary)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"summary failed: {e}")
