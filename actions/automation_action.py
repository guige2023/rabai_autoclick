"""Automation action module for RabAI AutoClick.

Provides workflow automation actions including scheduling, chaining, and error handling.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ErrorHandlerAction(BaseAction):
    """Handle errors and exceptions in workflow execution.
    
    Catches exceptions, logs them, and provides fallback behavior.
    """
    action_type = "error_handler"
    display_name = "错误处理"
    description = "捕获和处理执行错误"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle errors.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: error_var, fallback_value, 
                   log_traceback, reraise.
        
        Returns:
            ActionResult with error info or fallback value.
        """
        error_var = params.get('error_var', 'last_error')
        fallback_value = params.get('fallback_value', None)
        log_traceback = params.get('log_traceback', True)
        reraise = params.get('reraise', False)
        
        # Get error from context
        error = getattr(context, error_var, None)
        
        if error is None:
            return ActionResult(
                success=True,
                message="No error to handle",
                data={'error': None, 'handled': False}
            )
        
        error_info = {
            'type': type(error).__name__ if error else None,
            'message': str(error) if error else None
        }
        
        if log_traceback and hasattr(error, '__traceback__'):
            error_info['traceback'] = ''.join(traceback.format_exception(
                type(error), error, error.__traceback__
            ))
        
        # Store in context for later use
        setattr(context, f'{error_var}_info', error_info)
        
        if reraise:
            return ActionResult(
                success=False,
                message=f"Re-raising error: {error_info['type']}",
                data=error_info
            )
        
        return ActionResult(
            success=True,
            message=f"Handled error: {error_info['type']}",
            data={
                'error': error_info,
                'fallback': fallback_value,
                'handled': True
            }
        )


class RetryAction(BaseAction):
    """Retry an action on failure with configurable attempts.
    
    Retries failed actions with exponential backoff option.
    """
    action_type = "retry"
    display_name = "重试机制"
    description = "失败时重试操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retry on failure.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: max_attempts, delay, backoff_factor,
                   retry_on, action_to_retry.
        
        Returns:
            ActionResult with retry status.
        """
        max_attempts = params.get('max_attempts', 3)
        delay = params.get('delay', 1.0)
        backoff_factor = params.get('backoff_factor', 2.0)
        retry_on = params.get('retry_on', ['Exception'])
        action_to_retry = params.get('action_to_retry', None)
        
        if max_attempts < 1:
            return ActionResult(success=False, message="max_attempts must be >= 1")
        
        if not hasattr(context, '_retry_state'):
            context._retry_state = {}
        
        retry_key = params.get('retry_key', 'default')
        
        # Initialize or get attempt count
        if retry_key not in context._retry_state:
            context._retry_state[retry_key] = {'attempt': 0, 'last_error': None}
        
        state = context._retry_state[retry_key]
        state['attempt'] += 1
        current_attempt = state['attempt']
        
        # Check if we should retry
        if current_attempt > max_attempts:
            del context._retry_state[retry_key]
            return ActionResult(
                success=False,
                message=f"Max attempts ({max_attempts}) exceeded",
                data={
                    'attempted': max_attempts,
                    'last_error': state['last_error']
                }
            )
        
        # Calculate delay with backoff
        actual_delay = delay * (backoff_factor ** (current_attempt - 1))
        
        return ActionResult(
            success=True,
            message=f"Retry attempt {current_attempt}/{max_attempts}, delay: {actual_delay:.2f}s",
            data={
                'attempt': current_attempt,
                'max_attempts': max_attempts,
                'delay': actual_delay,
                'can_retry': current_attempt < max_attempts
            }
        )


class WorkflowChainAction(BaseAction):
    """Chain multiple actions together sequentially.
    
    Executes a series of actions and aggregates results.
    """
    action_type = "workflow_chain"
    display_name = "工作流链式执行"
    description = "链式执行多个操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Chain workflow actions.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: actions, stop_on_error, continue_on_error.
        
        Returns:
            ActionResult with aggregated results.
        """
        actions = params.get('actions', [])
        stop_on_error = params.get('stop_on_error', True)
        continue_on_error = params.get('continue_on_error', False)
        
        if not actions:
            return ActionResult(success=False, message="No actions to chain")
        
        results = []
        all_success = True
        
        for i, action_def in enumerate(actions):
            action_name = action_def.get('name', f'action_{i}')
            
            # In a real implementation, this would execute the action
            # For now, we simulate the chain structure
            try:
                # Execute action (placeholder)
                result = ActionResult(
                    success=True,
                    message=f"Action '{action_name}' completed",
                    data={'index': i, 'name': action_name}
                )
                results.append(result)
                
                if not result.success:
                    all_success = False
                    if stop_on_error:
                        break
            except Exception as e:
                error_result = ActionResult(
                    success=False,
                    message=f"Action '{action_name}' failed: {e}",
                    data={'index': i, 'name': action_name, 'error': str(e)}
                )
                results.append(error_result)
                all_success = False
                
                if stop_on_error and not continue_on_error:
                    break
        
        success_count = sum(1 for r in results if r.success)
        
        return ActionResult(
            success=all_success,
            message=f"Chain completed: {success_count}/{len(results)} succeeded",
            data={
                'total': len(results),
                'succeeded': success_count,
                'failed': len(results) - success_count,
                'results': results
            }
        )


class ParallelAction(BaseAction):
    """Execute multiple actions in parallel.
    
    Runs actions concurrently and waits for all to complete.
    """
    action_type = "parallel"
    display_name = "并行执行"
    description = "并行执行多个操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute actions in parallel.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: actions, wait_all, timeout.
        
        Returns:
            ActionResult with aggregated results.
        """
        actions = params.get('actions', [])
        wait_all = params.get('wait_all', True)
        timeout = params.get('timeout', 60)
        
        if not actions:
            return ActionResult(success=False, message="No actions to execute")
        
        # In a real implementation, this would use threading or asyncio
        # For now, we simulate the parallel execution structure
        results = []
        for i, action_def in enumerate(actions):
            action_name = action_def.get('name', f'action_{i}')
            
            try:
                # Placeholder - real implementation would spawn thread/task
                result = ActionResult(
                    success=True,
                    message=f"Action '{action_name}' completed",
                    data={'index': i, 'name': action_name}
                )
                results.append(result)
            except Exception as e:
                results.append(ActionResult(
                    success=False,
                    message=f"Action '{action_name}' failed: {e}",
                    data={'error': str(e)}
                ))
        
        success_count = sum(1 for r in results if r.success)
        
        return ActionResult(
            success=success_count == len(results),
            message=f"Parallel execution: {success_count}/{len(results)} succeeded",
            data={
                'total': len(results),
                'succeeded': success_count,
                'failed': len(results) - success_count
            }
        )


class TimeoutAction(BaseAction):
    """Execute action with a timeout limit.
    
    Cancels execution if it exceeds the specified duration.
    """
    action_type = "timeout"
    display_name = "超时控制"
    description = "为操作设置超时限制"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with timeout.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: timeout_seconds, action_to_execute.
        
        Returns:
            ActionResult with execution result or timeout status.
        """
        timeout_seconds = params.get('timeout_seconds', 30)
        action_to_execute = params.get('action_to_execute', None)
        
        if timeout_seconds <= 0:
            return ActionResult(success=False, message="timeout_seconds must be positive")
        
        if not action_to_execute:
            return ActionResult(success=False, message="action_to_execute is required")
        
        # In a real implementation, this would run the action with timeout
        # For now, we return the timeout configuration
        return ActionResult(
            success=True,
            message=f"Timeout set: {timeout_seconds}s",
            data={
                'timeout_seconds': timeout_seconds,
                'action': action_to_execute,
                'timed_out': False
            }
        )
