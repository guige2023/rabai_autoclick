"""Automation Recovery Action Module.

Provides error recovery and retry capabilities for automation workflows.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationRecoveryAction(BaseAction):
    """Recover from automation failures.
    
    Implements various recovery strategies for failed automation tasks.
    """
    action_type = "automation_recovery"
    display_name = "自动化恢复"
    description = "从自动化失败中恢复"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute recovery.
        
        Args:
            context: Execution context.
            params: Dict with keys: error, task, recovery_strategy.
        
        Returns:
            ActionResult with recovery result.
        """
        error = params.get('error', '')
        task = params.get('task', {})
        strategy = params.get('recovery_strategy', 'retry')
        
        if not error:
            return ActionResult(
                success=False,
                data=None,
                error="No error provided"
            )
        
        if strategy == 'retry':
            return self._retry_recovery(task, params)
        elif strategy == 'fallback':
            return self._fallback_recovery(task, params)
        elif strategy == 'checkpoint':
            return self._checkpoint_recovery(task, params)
        elif strategy == 'skip':
            return self._skip_recovery(task, params)
        elif strategy == 'manual':
            return self._manual_recovery(task, params)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown strategy: {strategy}"
            )
    
    def _retry_recovery(self, task: Dict, params: Dict) -> ActionResult:
        """Retry failed task with backoff."""
        max_retries = params.get('max_retries', 3)
        backoff_factor = params.get('backoff_factor', 2)
        current_retry = params.get('retry_count', 0)
        
        if current_retry >= max_retries:
            return ActionResult(
                success=False,
                data={
                    'strategy': 'retry',
                    'attempted': current_retry,
                    'max_retries': max_retries
                },
                error="Max retries exceeded"
            )
        
        next_retry = current_retry + 1
        delay = backoff_factor ** current_retry
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'retry',
                'retry_count': next_retry,
                'delay': delay,
                'max_retries': max_retries,
                'should_retry': True
            },
            error=None
        )
    
    def _fallback_recovery(self, task: Dict, params: Dict) -> ActionResult:
        """Use fallback task or value."""
        fallback_task = params.get('fallback_task', {})
        fallback_value = params.get('fallback_value', None)
        
        if fallback_task:
            return ActionResult(
                success=True,
                data={
                    'strategy': 'fallback',
                    'fallback_task': fallback_task,
                    'used_fallback': 'task'
                },
                error=None
            )
        elif fallback_value is not None:
            return ActionResult(
                success=True,
                data={
                    'strategy': 'fallback',
                    'fallback_value': fallback_value,
                    'used_fallback': 'value'
                },
                error=None
            )
        else:
            return ActionResult(
                success=False,
                data={'strategy': 'fallback'},
                error="No fallback available"
            )
    
    def _checkpoint_recovery(self, task: Dict, params: Dict) -> ActionResult:
        """Recover from checkpoint."""
        checkpoint_data = params.get('checkpoint_data', {})
        
        if not checkpoint_data:
            return ActionResult(
                success=False,
                data={'strategy': 'checkpoint'},
                error="No checkpoint data available"
            )
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'checkpoint',
                'checkpoint_data': checkpoint_data,
                'recovered_state': checkpoint_data
            },
            error=None
        )
    
    def _skip_recovery(self, task: Dict, params: Dict) -> ActionResult:
        """Skip failed task."""
        skip_reason = params.get('skip_reason', 'Task failed, skipped by recovery strategy')
        
        return ActionResult(
            success=True,
            data={
                'strategy': 'skip',
                'skipped': True,
                'reason': skip_reason
            },
            error=None
        )
    
    def _manual_recovery(self, task: Dict, params: Dict) -> ActionResult:
        """Request manual intervention."""
        return ActionResult(
            success=False,
            data={
                'strategy': 'manual',
                'requires_manual_intervention': True,
                'task': task
            },
            error="Manual intervention required"
        )


class AutomationRetryHandlerAction(BaseAction):
    """Handle retry logic with exponential backoff.
    
    Manages retry attempts with increasing delays.
    """
    action_type = "automation_retry_handler"
    display_name = "重试处理器"
    description = "处理指数退避重试逻辑"
    
    def __init__(self):
        super().__init__()
        self._retry_state = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute retry handling.
        
        Args:
            context: Execution context.
            params: Dict with keys: task_id, action, retry_config.
        
        Returns:
            ActionResult with retry decision.
        """
        task_id = params.get('task_id', '')
        action = params.get('action', 'should_retry')
        retry_config = params.get('retry_config', {})
        
        if action == 'should_retry':
            return self._should_retry(task_id, retry_config)
        elif action == 'record_failure':
            return self._record_failure(task_id)
        elif action == 'reset':
            return self._reset(task_id)
        elif action == 'get_state':
            return self._get_state(task_id)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _should_retry(self, task_id: str, config: Dict) -> ActionResult:
        """Decide if task should be retried."""
        max_attempts = config.get('max_attempts', 3)
        backoff_factor = config.get('backoff_factor', 2)
        
        state = self._retry_state.get(task_id, {
            'attempts': 0,
            'first_failure': None
        })
        
        if state['attempts'] >= max_attempts:
            return ActionResult(
                success=True,
                data={
                    'should_retry': False,
                    'reason': 'max_attempts_exceeded',
                    'attempts': state['attempts']
                },
                error=None
            )
        
        delay = backoff_factor ** state['attempts']
        
        return ActionResult(
            success=True,
            data={
                'should_retry': True,
                'retry_delay': delay,
                'attempt': state['attempts'] + 1,
                'max_attempts': max_attempts
            },
            error=None
        )
    
    def _record_failure(self, task_id: str) -> ActionResult:
        """Record a failure for the task."""
        if task_id not in self._retry_state:
            self._retry_state[task_id] = {
                'attempts': 0,
                'first_failure': time.time()
            }
        
        self._retry_state[task_id]['attempts'] += 1
        self._retry_state[task_id]['last_failure'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'task_id': task_id,
                'attempts': self._retry_state[task_id]['attempts']
            },
            error=None
        )
    
    def _reset(self, task_id: str) -> ActionResult:
        """Reset retry state for task."""
        if task_id in self._retry_state:
            del self._retry_state[task_id]
        
        return ActionResult(
            success=True,
            data={'task_id': task_id, 'reset': True},
            error=None
        )
    
    def _get_state(self, task_id: str) -> ActionResult:
        """Get current retry state."""
        state = self._retry_state.get(task_id, {})
        
        return ActionResult(
            success=True,
            data={
                'task_id': task_id,
                'state': state
            },
            error=None
        )


class AutomationCheckpointAction(BaseAction):
    """Manage checkpoints for recovery.
    
    Saves and restores workflow state for recovery.
    """
    action_type = "automation_checkpoint"
    display_name = "检查点管理"
    description = "管理用于恢复的工作流检查点"
    
    def __init__(self):
        super().__init__()
        self._checkpoints = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checkpoint operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id, action, checkpoint_data.
        
        Returns:
            ActionResult with checkpoint result.
        """
        workflow_id = params.get('workflow_id', '')
        action = params.get('action', 'save')
        checkpoint_data = params.get('checkpoint_data', {})
        
        if not workflow_id:
            return ActionResult(
                success=False,
                data=None,
                error="Workflow ID required"
            )
        
        if action == 'save':
            return self._save_checkpoint(workflow_id, checkpoint_data)
        elif action == 'load':
            return self._load_checkpoint(workflow_id)
        elif action == 'list':
            return self._list_checkpoints(workflow_id)
        elif action == 'delete':
            return self._delete_checkpoint(workflow_id)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _save_checkpoint(self, workflow_id: str, data: Dict) -> ActionResult:
        """Save a checkpoint."""
        timestamp = time.time()
        
        if workflow_id not in self._checkpoints:
            self._checkpoints[workflow_id] = {}
        
        self._checkpoints[workflow_id][timestamp] = {
            'data': data,
            'created_at': timestamp
        }
        
        return ActionResult(
            success=True,
            data={
                'workflow_id': workflow_id,
                'checkpoint_id': timestamp,
                'saved': True
            },
            error=None
        )
    
    def _load_checkpoint(self, workflow_id: str) -> ActionResult:
        """Load the latest checkpoint."""
        if workflow_id not in self._checkpoints or not self._checkpoints[workflow_id]:
            return ActionResult(
                success=False,
                data=None,
                error="No checkpoints found"
            )
        
        latest_timestamp = max(self._checkpoints[workflow_id].keys())
        checkpoint = self._checkpoints[workflow_id][latest_timestamp]
        
        return ActionResult(
            success=True,
            data={
                'workflow_id': workflow_id,
                'checkpoint_id': latest_timestamp,
                'checkpoint_data': checkpoint['data']
            },
            error=None
        )
    
    def _list_checkpoints(self, workflow_id: str) -> ActionResult:
        """List all checkpoints for workflow."""
        if workflow_id not in self._checkpoints:
            return ActionResult(
                success=True,
                data={'workflow_id': workflow_id, 'checkpoints': []},
                error=None
            )
        
        checkpoints = [
            {'timestamp': ts, 'created_at': cp['created_at']}
            for ts, cp in sorted(self._checkpoints[workflow_id].items())
        ]
        
        return ActionResult(
            success=True,
            data={
                'workflow_id': workflow_id,
                'checkpoints': checkpoints
            },
            error=None
        )
    
    def _delete_checkpoint(self, workflow_id: str) -> ActionResult:
        """Delete all checkpoints for workflow."""
        if workflow_id in self._checkpoints:
            del self._checkpoints[workflow_id]
        
        return ActionResult(
            success=True,
            data={'workflow_id': workflow_id, 'deleted': True},
            error=None
        )


def register_actions():
    """Register all Automation Recovery actions."""
    return [
        AutomationRecoveryAction,
        AutomationRetryHandlerAction,
        AutomationCheckpointAction,
    ]
