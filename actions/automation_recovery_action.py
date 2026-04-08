"""Automation Recovery Action.

Provides automatic recovery from automation failures with retry logic,
checkpoint/restore, state rollback, and fallback action execution.
"""

import sys
import os
import time
import pickle
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationRecoveryAction(BaseAction):
    """Recover from automation failures with various strategies.
    
    Supports retry with backoff, checkpoint/restore state,
    rollback mechanisms, and fallback action chains.
    """
    action_type = "automation_recovery"
    display_name = "自动化恢复"
    description = "自动化失败恢复，支持重试、检查点/恢复、回滚"

    def __init__(self):
        super().__init__()
        self._checkpoints: Dict[str, Any] = {}
        self._rollback_handlers: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute action with recovery capabilities.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'retry', 'checkpoint_save', 'checkpoint_restore',
                         'rollback', 'fallback', 'recover'.
                - target_action: Action to execute with recovery.
                - target_params: Parameters for target action.
                - max_retries: Max retry attempts (default: 3).
                - retry_delay: Base delay between retries (default: 1.0).
                - exponential_backoff: Use exponential backoff (default: True).
                - checkpoint_id: ID for checkpoint save/restore.
                - rollback_actions: List of rollback action definitions.
                - fallback_actions: List of fallback action definitions.
                - save_to_var: Variable name for results.
        
        Returns:
            ActionResult with recovery results.
        """
        try:
            action = params.get('action', 'recover')
            save_to_var = params.get('save_to_var', 'recovery_result')

            if action == 'retry':
                return self._retry_action(context, params, save_to_var)
            elif action == 'checkpoint_save':
                return self._save_checkpoint(context, params, save_to_var)
            elif action == 'checkpoint_restore':
                return self._restore_checkpoint(context, params, save_to_var)
            elif action == 'rollback':
                return self._rollback(context, params, save_to_var)
            elif action == 'fallback':
                return self._execute_fallback(context, params, save_to_var)
            elif action == 'recover':
                return self._recover_with_all_strategies(context, params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown recovery action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Recovery error: {e}")

    def _retry_action(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Retry action with backoff."""
        target_action = params.get('target_action')
        target_params = params.get('target_params', {})
        max_retries = params.get('max_retries', 3)
        retry_delay = params.get('retry_delay', 1.0)
        exponential_backoff = params.get('exponential_backoff', True)

        if not target_action:
            return ActionResult(success=False, message="target_action is required")

        last_error = None
        action_obj = self._get_action(target_action)

        for attempt in range(max_retries + 1):
            try:
                result = action_obj.execute(context, target_params) if action_obj else None
                
                if result and result.success:
                    return ActionResult(success=True, data={
                        'attempt': attempt + 1,
                        'result': result.data
                    }, message=f"Success on attempt {attempt + 1}")
                else:
                    last_error = result.message if result else "Unknown error"
            except Exception as e:
                last_error = str(e)

            if attempt < max_retries:
                delay = retry_delay * (2 ** attempt) if exponential_backoff else retry_delay
                time.sleep(delay)

        return ActionResult(success=False, data={
            'attempts': max_retries + 1,
            'last_error': last_error
        }, message=f"All {max_retries + 1} attempts failed")

    def _save_checkpoint(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Save current state as checkpoint."""
        checkpoint_id = params.get('checkpoint_id', 'default')
        include_variables = params.get('include_variables', True)
        include_state = params.get('include_state', True)

        checkpoint_data = {
            'timestamp': time.time(),
            'variables': context.get_all_variables() if include_variables else {},
            'state': self._serialize_state(context) if include_state else {}
        }

        self._checkpoints[checkpoint_id] = checkpoint_data

        context.set_variable(save_to_var, {
            'checkpoint_id': checkpoint_id,
            'saved': True,
            'timestamp': checkpoint_data['timestamp']
        })

        return ActionResult(success=True, data=checkpoint_data,
                           message=f"Checkpoint '{checkpoint_id}' saved")

    def _restore_checkpoint(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Restore state from checkpoint."""
        checkpoint_id = params.get('checkpoint_id', 'default')

        if checkpoint_id not in self._checkpoints:
            return ActionResult(success=False, message=f"Checkpoint '{checkpoint_id}' not found")

        checkpoint = self._checkpoints[checkpoint_id]
        
        # Restore variables
        for key, value in checkpoint.get('variables', {}).items():
            context.set_variable(key, value)

        # Restore state
        self._deserialize_state(context, checkpoint.get('state', {}))

        context.set_variable(save_to_var, {
            'checkpoint_id': checkpoint_id,
            'restored': True,
            'timestamp': checkpoint['timestamp']
        })

        return ActionResult(success=True, data=checkpoint,
                           message=f"Checkpoint '{checkpoint_id}' restored")

    def _rollback(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Execute rollback actions."""
        rollback_actions = params.get('rollback_actions', [])
        rollback_id = params.get('rollback_id', 'default')

        results = []
        for i, rb_action_def in enumerate(reversed(rollback_actions)):
            action_name = rb_action_def.get('action')
            action_params = rb_action_def.get('params', {})

            try:
                action_obj = self._get_action(action_name)
                if action_obj:
                    result = action_obj.execute(context, action_params)
                    results.append({
                        'index': i,
                        'action': action_name,
                        'success': result.success if result else False,
                        'result': result.data if result else None
                    })
            except Exception as e:
                results.append({
                    'index': i,
                    'action': action_name,
                    'success': False,
                    'error': str(e)
                })

        summary = {
            'rollback_id': rollback_id,
            'total_actions': len(rollback_actions),
            'results': results,
            'all_success': all(r.get('success', False) for r in results)
        }

        context.set_variable(save_to_var, summary)
        return ActionResult(success=summary['all_success'], data=summary,
                           message=f"Rollback: {sum(1 for r in results if r.get('success'))}/{len(results)} actions succeeded")

    def _execute_fallback(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Execute fallback action chain."""
        fallback_actions = params.get('fallback_actions', [])
        original_error = params.get('original_error', 'Unknown')

        for i, fb_action_def in enumerate(fallback_actions):
            action_name = fb_action_def.get('action')
            action_params = fb_action_def.get('params', {})

            try:
                action_obj = self._get_action(action_name)
                if action_obj:
                    result = action_obj.execute(context, action_params)
                    if result and result.success:
                        context.set_variable(save_to_var, {
                            'fallback_succeeded': True,
                            'fallback_index': i,
                            'action': action_name,
                            'result': result.data
                        })
                        return ActionResult(success=True, data=result.data,
                                          message=f"Fallback {i+1} succeeded")
            except Exception as e:
                continue

        return ActionResult(success=False, data={
            'fallback_succeeded': False,
            'original_error': original_error
        }, message="All fallbacks failed")

    def _recover_with_all_strategies(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Attempt recovery using all available strategies."""
        checkpoint_id = params.get('checkpoint_id', 'recovery')
        max_retries = params.get('max_retries', 3)
        fallback_actions = params.get('fallback_actions', [])
        rollback_actions = params.get('rollback_actions', [])

        strategies_attempted = []
        recovery_success = False
        final_result = None

        # Strategy 1: Checkpoint restore
        if checkpoint_id in self._checkpoints:
            strategies_attempted.append('checkpoint_restore')
            restore_result = self._restore_checkpoint(context, params, f'{save_to_var}_cp')
            if restore_result.success:
                recovery_success = True
                final_result = restore_result.data

        # Strategy 2: Retry
        if not recovery_success:
            strategies_attempted.append('retry')
            retry_result = self._retry_action(context, params, f'{save_to_var}_retry')
            if retry_result.success:
                recovery_success = True
                final_result = retry_result.data

        # Strategy 3: Rollback
        if not recovery_success and rollback_actions:
            strategies_attempted.append('rollback')
            rollback_result = self._rollback(context, {'rollback_actions': rollback_actions}, f'{save_to_var}_rb')
            if rollback_result.success:
                recovery_success = True
                final_result = rollback_result.data

        # Strategy 4: Fallback
        if not recovery_success and fallback_actions:
            strategies_attempted.append('fallback')
            fallback_result = self._execute_fallback(context, {'fallback_actions': fallback_actions}, f'{save_to_var}_fb')
            if fallback_result.success:
                recovery_success = True
                final_result = fallback_result.data

        summary = {
            'recovered': recovery_success,
            'strategies_attempted': strategies_attempted,
            'final_result': final_result
        }

        context.set_variable(save_to_var, summary)
        return ActionResult(success=recovery_success, data=summary,
                           message=f"Recovery: {'succeeded' if recovery_success else 'failed'}")

    def _serialize_state(self, context: Any) -> Dict:
        """Serialize context state."""
        try:
            return {
                'variables': context.get_all_variables(),
                'serialized_at': time.time()
            }
        except Exception:
            return {}

    def _deserialize_state(self, context: Any, state: Dict):
        """Deserialize and restore state to context."""
        try:
            for key, value in state.get('variables', {}).items():
                context.set_variable(key, value)
        except Exception:
            pass

    def _get_action(self, action_name: str):
        """Get action from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)
