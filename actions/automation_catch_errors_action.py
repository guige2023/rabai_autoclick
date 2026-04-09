"""Automation catch errors action module for RabAI AutoClick.

Provides error handling for automation:
- AutomationCatchErrorsAction: Catch and handle errors
- AutomationErrorContextAction: Add context to errors
- AutomationErrorChainAction: Chain error handling
- AutomationErrorFallbackAction: Fallback on error
"""

import traceback
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationCatchErrorsAction(BaseAction):
    """Catch and handle errors in automation blocks."""
    action_type = "automation_catch_errors"
    display_name = "自动化错误捕获"
    description = "捕获并处理自动化中的错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action")
            error_handler = params.get("error_handler")
            fallback_value = params.get("fallback_value")
            capture_trace = params.get("capture_trace", True)
            reraise = params.get("reraise", False)

            if not callable(action):
                return ActionResult(success=False, message="action must be callable")

            try:
                result = action()
                return ActionResult(
                    success=True,
                    message="Action completed without errors",
                    data={"result": result, "error": None}
                )
            except Exception as e:
                error_info = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
                if capture_trace:
                    error_info["traceback"] = traceback.format_exc()

                if callable(error_handler):
                    try:
                        handler_result = error_handler(e, context)
                        return ActionResult(
                            success=True,
                            message=f"Error caught and handled: {type(e).__name__}",
                            data={"result": handler_result, "error": error_info}
                        )
                    except Exception as handler_error:
                        error_info["handler_error"] = str(handler_error)

                if fallback_value is not None:
                    return ActionResult(
                        success=True,
                        message=f"Error caught, returning fallback: {type(e).__name__}",
                        data={"result": fallback_value, "error": error_info}
                    )

                if reraise:
                    raise

                return ActionResult(
                    success=False,
                    message=f"Error caught: {type(e).__name__}: {e}",
                    data={"error": error_info}
                )
        except Exception as outer_e:
            return ActionResult(success=False, message=f"Catch errors action failed: {outer_e}")


class AutomationErrorContextAction(BaseAction):
    """Add context information to errors."""
    action_type = "automation_error_context"
    display_name = "自动化错误上下文"
    description = "为错误添加上下文信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error")
            context_data = params.get("context_data", {})
            context_key = params.get("context_key", "error_context")
            add_timestamp = params.get("add_timestamp", True)
            add_stack = params.get("add_stack", False)

            if error is None:
                return ActionResult(success=False, message="error is required")

            enriched = {
                "original_error": str(error),
                "error_type": type(error).__name__ if hasattr(error, "__class__") else "Unknown",
                "context": context_data,
            }

            if add_timestamp:
                enriched["timestamp"] = datetime.now().isoformat()

            if add_stack:
                enriched["stack_trace"] = traceback.format_exc()

            if isinstance(error, dict):
                error[context_key] = enriched
                result = error
            elif hasattr(error, context_key):
                setattr(error, context_key, enriched)
                result = error
            else:
                class EnrichedError(Exception):
                    pass
                enriched_error = EnrichedError(str(error))
                enriched_error.error_context = enriched
                result = enriched_error

            return ActionResult(
                success=True,
                message=f"Error enriched with {len(context_data)} context items",
                data={"enriched_error": result, "context_keys": list(context_data.keys())}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error context error: {e}")


class AutomationErrorChainAction(BaseAction):
    """Chain multiple error handlers."""
    action_type = "automation_error_chain"
    display_name = "自动化错误链"
    description = "链式错误处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action")
            handlers = params.get("handlers", [])
            stop_on_success = params.get("stop_on_success", True)

            if not callable(action):
                return ActionResult(success=False, message="action must be callable")
            if not handlers:
                return ActionResult(success=False, message="handlers list is required")

            try:
                result = action()
                return ActionResult(success=True, message="Action succeeded", data={"result": result})
            except Exception as e:
                last_handler_result = None

                for i, handler in enumerate(handlers):
                    if callable(handler):
                        try:
                            handler_result = handler(e, context)
                            last_handler_result = handler_result
                        except Exception as handler_error:
                            return ActionResult(
                                success=False,
                                message=f"Handler {i} raised: {handler_error}",
                                data={"original_error": str(e), "handler_error": str(handler_error)}
                            )
                    elif isinstance(handler, dict):
                        error_types = handler.get("error_types", [])
                        handler_fn = handler.get("handler")
                        fallback = handler.get("fallback")

                        if not error_types or type(e).__name__ in error_types:
                            if callable(handler_fn):
                                try:
                                    last_handler_result = handler_fn(e, context)
                                except Exception:
                                    if fallback is not None:
                                        last_handler_result = fallback
                            elif fallback is not None:
                                last_handler_result = fallback

                return ActionResult(
                    success=True,
                    message=f"Error handled by chain: {type(e).__name__}",
                    data={"original_error": str(e), "handler_result": last_handler_result}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error chain error: {e}")


class AutomationErrorFallbackAction(BaseAction):
    """Fallback actions on error."""
    action_type = "automation_error_fallback"
    display_name = "自动化错误回退"
    description = "错误时执行回退动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            primary_action = params.get("primary_action")
            fallback_action = params.get("fallback_action")
            error_action = params.get("error_action")
            fallback_value = params.get("fallback_value")
            attempts = params.get("attempts", 1)

            if not callable(primary_action):
                return ActionResult(success=False, message="primary_action must be callable")

            last_error = None
            result = None

            for attempt in range(attempts):
                try:
                    result = primary_action()
                    return ActionResult(
                        success=True,
                        message=f"Primary action succeeded on attempt {attempt + 1}",
                        data={"result": result, "attempt": attempt + 1}
                    )
                except Exception as e:
                    last_error = e
                    if attempt < attempts - 1:
                        continue

            if callable(error_action):
                try:
                    error_result = error_action(last_error, context)
                    return ActionResult(
                        success=True,
                        message=f"Error action executed: {type(last_error).__name__}",
                        data={"error": str(last_error), "error_result": error_result}
                    )
                except Exception:
                    pass

            if callable(fallback_action):
                try:
                    result = fallback_action()
                    return ActionResult(
                        success=True,
                        message=f"Fallback action executed after {attempts} attempts",
                        data={"result": result, "original_error": str(last_error)}
                    )
                except Exception as fallback_error:
                    return ActionResult(
                        success=False,
                        message=f"Fallback also failed: {fallback_error}",
                        data={"original_error": str(last_error), "fallback_error": str(fallback_error)}
                    )

            if fallback_value is not None:
                return ActionResult(
                    success=True,
                    message=f"Returning fallback value after {attempts} attempts",
                    data={"fallback_value": fallback_value, "original_error": str(last_error)}
                )

            return ActionResult(
                success=False,
                message=f"All attempts failed: {type(last_error).__name__}: {last_error}",
                data={"error": str(last_error), "attempts": attempts}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error fallback error: {e}")
