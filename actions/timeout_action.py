"""Timeout action module for RabAI AutoClick.

Provides timeout operations for various scenarios:
- TimeoutCallAction: Execute a call with timeout
- TimeoutWaitAction: Wait with timeout
- TimeoutRetryAction: Retry with timeout
- TimeoutDeadlineAction: Deadline-based timeout
"""

import signal
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeoutError(Exception):
    """Timeout error."""
    pass


def _timeout_handler(signum, frame):
    raise TimeoutError("Operation timed out")


class TimeoutCallAction(BaseAction):
    """Execute a call with a timeout."""
    action_type = "timeout_call"
    display_name = "超时调用"
    description = "带超时限制的函数调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            func_ref = params.get("func_ref", None)
            args = params.get("args", [])
            kwargs = params.get("kwargs", {})
            timeout_seconds = params.get("timeout_seconds", 30)
            default_value = params.get("default_value", None)

            if not func_ref:
                return ActionResult(success=True, message="No func_ref provided, returning default")

            result = default_value
            error = None

            def target():
                nonlocal result, error
                try:
                    result = func_ref(*args, **kwargs)
                except Exception as e:
                    error = str(e)

            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(timeout=timeout_seconds)

            if thread.is_alive():
                return ActionResult(
                    success=False,
                    message=f"Operation timed out after {timeout_seconds}s",
                    data={"timeout": timeout_seconds, "result": default_value}
                )

            if error:
                return ActionResult(success=False, message=f"Operation failed: {error}", data={"error": error})

            return ActionResult(
                success=True,
                message=f"Operation completed within {timeout_seconds}s",
                data={"result": result, "elapsed": timeout_seconds}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout call failed: {str(e)}")


class TimeoutWaitAction(BaseAction):
    """Wait with a timeout."""
    action_type = "timeout_wait"
    display_name = "超时等待"
    description = "带超时的等待操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition_ref = params.get("condition_ref", None)
            timeout_seconds = params.get("timeout_seconds", 30)
            poll_interval = params.get("poll_interval", 0.5)

            start_time = time.time()
            elapsed = 0

            if condition_ref:
                while elapsed < timeout_seconds:
                    try:
                        if condition_ref():
                            return ActionResult(
                                success=True,
                                message=f"Condition met after {elapsed:.2f}s",
                                data={"elapsed": elapsed, "timed_out": False}
                            )
                    except Exception as e:
                        return ActionResult(success=False, message=f"Condition check failed: {str(e)}")
                    time.sleep(poll_interval)
                    elapsed = time.time() - start_time

                return ActionResult(
                    success=False,
                    message=f"Condition not met after {timeout_seconds}s",
                    data={"elapsed": elapsed, "timed_out": True}
                )
            else:
                time.sleep(timeout_seconds)
                return ActionResult(
                    success=True,
                    message=f"Waited {timeout_seconds}s",
                    data={"elapsed": timeout_seconds}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout wait failed: {str(e)}")


class TimeoutRetryAction(BaseAction):
    """Retry an operation with timeout."""
    action_type = "timeout_retry"
    display_name = "超时重试"
    description = "带超时限制的重试操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            func_ref = params.get("func_ref", None)
            timeout_seconds = params.get("timeout_seconds", 60)
            max_attempts = params.get("max_attempts", 3)
            retry_interval = params.get("retry_interval", 1.0)
            backoff_multiplier = params.get("backoff_multiplier", 1.0)

            if not func_ref:
                return ActionResult(success=False, message="func_ref is required")

            start_time = time.time()
            attempts = 0
            last_error = None

            while attempts < max_attempts:
                elapsed = time.time() - start_time
                if elapsed >= timeout_seconds:
                    return ActionResult(
                        success=False,
                        message=f"Retry timeout after {attempts} attempts, {elapsed:.2f}s elapsed",
                        data={"attempts": attempts, "elapsed": elapsed, "last_error": str(last_error) if last_error else None}
                    )

                attempts += 1
                try:
                    result = func_ref()
                    return ActionResult(
                        success=True,
                        message=f"Succeeded on attempt {attempts} after {elapsed:.2f}s",
                        data={"result": result, "attempts": attempts, "elapsed": elapsed}
                    )
                except Exception as e:
                    last_error = e
                    if attempts < max_attempts:
                        sleep_time = retry_interval * (backoff_multiplier ** (attempts - 1))
                        time.sleep(sleep_time)

            return ActionResult(
                success=False,
                message=f"Failed after {attempts} attempts, {time.time() - start_time:.2f}s",
                data={"attempts": attempts, "last_error": str(last_error)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout retry failed: {str(e)}")


class TimeoutDeadlineAction(BaseAction):
    """Deadline-based timeout tracking."""
    action_type = "timeout_deadline"
    display_name = "截止时间"
    description = "基于截止时间的超时管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            deadline_seconds = params.get("deadline_seconds", 60)
            operation = params.get("operation", "check")

            deadline_id = params.get("deadline_id", str(uuid.uuid4()))
            now = datetime.utcnow()
            deadline = now + timedelta(seconds=deadline_seconds)

            if operation == "check":
                remaining = (deadline - datetime.utcnow()).total_seconds()
                return ActionResult(
                    success=remaining > 0,
                    message=f"Deadline {deadline_id}: {remaining:.2f}s remaining",
                    data={
                        "deadline_id": deadline_id,
                        "remaining_seconds": remaining,
                        "deadline": deadline.isoformat()
                    }
                )

            elif operation == "expired":
                remaining = (deadline - datetime.utcnow()).total_seconds()
                return ActionResult(
                    success=remaining <= 0,
                    message=f"Deadline {deadline_id} {'expired' if remaining <= 0 else 'active'}",
                    data={"deadline_id": deadline_id, "expired": remaining <= 0, "remaining": remaining}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout deadline failed: {str(e)}")
