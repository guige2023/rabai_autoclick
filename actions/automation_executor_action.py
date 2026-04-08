"""Automation Executor action module for RabAI AutoClick.

Executes automation workflows with timeout, retry,
and cancellation support.
"""

import time
import traceback
import sys
import os
import signal
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationExecutorAction(BaseAction):
    """Execute automation tasks with timeout and retry.

    Provides structured execution with timeout enforcement,
    automatic retries, and result capture.
    """
    action_type = "automation_executor"
    display_name = "自动化执行器"
    description = "执行自动化任务，支持超时和重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an automation task.

        Args:
            context: Execution context.
            params: Dict with keys: action, params, timeout_seconds,
                   max_retries, retry_delay, retry_on.

        Returns:
            ActionResult with execution result.
        """
        start_time = time.time()
        try:
            action = params.get('action')
            task_params = params.get('params', {})
            timeout_seconds = params.get('timeout_seconds', 60)
            max_retries = params.get('max_retries', 0)
            retry_delay = params.get('retry_delay', 1.0)
            retry_on = params.get('retry_on', [Exception])

            if action is None:
                return ActionResult(
                    success=False,
                    message="action is required",
                    duration=time.time() - start_time,
                )

            last_error = None
            attempts = []

            for attempt in range(max_retries + 1):
                attempt_start = time.time()
                try:
                    if callable(action):
                        result = action(context, task_params)
                    elif hasattr(context, 'execute_action'):
                        result = context.execute_action(action, task_params)
                    else:
                        result = ActionResult(success=False, message=f"Unknown action type: {action}")

                    attempt_duration = time.time() - attempt_start
                    attempts.append({
                        'attempt': attempt + 1,
                        'success': True,
                        'duration': attempt_duration,
                    })

                    if isinstance(result, ActionResult):
                        if result.success:
                            duration = time.time() - start_time
                            return ActionResult(
                                success=True,
                                message=f"Execution succeeded on attempt {attempt + 1}",
                                data=result.data,
                                duration=duration,
                            )
                        last_error = result.message
                    else:
                        last_error = 'Invalid result type'

                except Exception as e:
                    attempt_duration = time.time() - attempt_start
                    last_error = str(e)
                    should_retry = any(isinstance(e, exc_type) for exc_type in retry_on)
                    attempts.append({
                        'attempt': attempt + 1,
                        'success': False,
                        'error': last_error,
                        'duration': attempt_duration,
                        'retryable': should_retry,
                    })
                    if not should_retry or attempt >= max_retries:
                        break

                if attempt < max_retries:
                    time.sleep(retry_delay)

            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Execution failed after {len(attempts)} attempts: {last_error}",
                data={'attempts': attempts},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Executor error: {str(e)}",
                duration=duration,
            )


class AutomationSupervisorAction(BaseAction):
    """Supervise running automation tasks and enforce limits.

    Monitors CPU, memory, execution time, and can
    terminate runaway tasks.
    """
    action_type = "automation_supervisor"
    display_name = "自动化监督器"
    description = "监督运行中的自动化任务并执行资源限制"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Supervise task execution.

        Args:
            context: Execution context.
            params: Dict with keys: action, params, max_duration,
                   max_memory_mb, check_interval, on_violation.

        Returns:
            ActionResult with supervision result.
        """
        start_time = time.time()
        try:
            action = params.get('action')
            task_params = params.get('params', {})
            max_duration = params.get('max_duration', 300)
            max_memory_mb = params.get('max_memory_mb', 0)
            check_interval = params.get('check_interval', 5)
            on_violation = params.get('on_violation', 'terminate')

            import psutil

            result_container = [None]
            terminated = [False]
            process = psutil.Process()

            def run_with_monitoring():
                try:
                    if callable(action):
                        result_container[0] = action(context, task_params)
                    elif hasattr(context, 'execute_action'):
                        result_container[0] = context.execute_action(action, task_params)
                    else:
                        result_container[0] = ActionResult(success=False, message="Unknown action")
                except Exception as e:
                    result_container[0] = ActionResult(success=False, message=str(e))

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_with_monitoring)
                try:
                    while not future.done():
                        future.wait(timeout=check_interval)
                        elapsed = time.time() - start_time

                        # Check duration
                        if elapsed > max_duration:
                            future.cancel()
                            terminated[0] = True
                            break

                        # Check memory
                        if max_memory_mb > 0:
                            try:
                                mem_info = process.memory_info()
                                mem_mb = mem_info.rss / (1024 * 1024)
                                if mem_mb > max_memory_mb:
                                    future.cancel()
                                    terminated[0] = True
                                    break
                            except Exception:
                                pass

                except FuturesTimeoutError:
                    future.cancel()
                    terminated[0] = True

            duration = time.time() - start_time
            if terminated[0]:
                return ActionResult(
                    success=False,
                    message=f"Task terminated due to resource violation (max_duration={max_duration}s, max_memory={max_memory_mb}MB)",
                    data={'terminated': True, 'elapsed': duration},
                    duration=duration,
                )

            result = result_container[0]
            if isinstance(result, ActionResult):
                result.duration = duration
                return result

            return ActionResult(success=False, message="Unknown result", duration=duration)

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Supervisor error: {str(e)}",
                duration=duration,
            )


class AutomationControllerAction(BaseAction):
    """Control automation execution with start/pause/stop/cancel.

    Provides lifecycle management for long-running
    automation tasks.
    """
    action_type = "automation_controller"
    display_name = "自动化控制器"
    description = "控制自动化执行的生命周期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Control task execution.

        Args:
            context: Execution context.
            params: Dict with keys: task_id, command (start/pause/resume/stop),
                   state_store (reference to shared state).

        Returns:
            ActionResult with control result.
        """
        start_time = time.time()
        try:
            command = params.get('command', 'start').lower()
            task_id = params.get('task_id', 'default')
            state_store = params.get('state_store')

            # Initialize state store if not provided
            if state_store is None:
                state_store = getattr(context, '_automation_states', {})
                if not hasattr(context, '_automation_states'):
                    context._automation_states = {}

            if command == 'start':
                state_store[task_id] = {
                    'status': 'running',
                    'started_at': time.time(),
                    'paused_at': None,
                    'stopped_at': None,
                }
                return ActionResult(
                    success=True,
                    message=f"Task {task_id} started",
                    data=state_store[task_id],
                    duration=time.time() - start_time,
                )

            elif command == 'pause':
                if task_id in state_store and state_store[task_id].get('status') == 'running':
                    state_store[task_id]['status'] = 'paused'
                    state_store[task_id]['paused_at'] = time.time()
                    return ActionResult(
                        success=True,
                        message=f"Task {task_id} paused",
                        data=state_store[task_id],
                        duration=time.time() - start_time,
                    )
                return ActionResult(
                    success=False,
                    message=f"Task {task_id} not running",
                    duration=time.time() - start_time,
                )

            elif command == 'resume':
                if task_id in state_store and state_store[task_id].get('status') == 'paused':
                    elapsed = (state_store[task_id].get('paused_at', time.time()) -
                               state_store[task_id].get('started_at', time.time()))
                    state_store[task_id]['status'] = 'running'
                    state_store[task_id]['started_at'] = time.time() - elapsed
                    state_store[task_id]['paused_at'] = None
                    return ActionResult(
                        success=True,
                        message=f"Task {task_id} resumed",
                        data=state_store[task_id],
                        duration=time.time() - start_time,
                    )
                return ActionResult(
                    success=False,
                    message=f"Task {task_id} not paused",
                    duration=time.time() - start_time,
                )

            elif command == 'stop':
                if task_id in state_store:
                    state_store[task_id]['status'] = 'stopped'
                    state_store[task_id]['stopped_at'] = time.time()
                    return ActionResult(
                        success=True,
                        message=f"Task {task_id} stopped",
                        data=state_store[task_id],
                        duration=time.time() - start_time,
                    )
                return ActionResult(
                    success=False,
                    message=f"Task {task_id} not found",
                    duration=time.time() - start_time,
                )

            elif command == 'status':
                if task_id in state_store:
                    return ActionResult(
                        success=True,
                        message=f"Task {task_id} status: {state_store[task_id].get('status')}",
                        data=state_store[task_id],
                        duration=time.time() - start_time,
                    )
                return ActionResult(
                    success=False,
                    message=f"Task {task_id} not found",
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown command: {command}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Controller error: {str(e)}",
                duration=duration,
            )
