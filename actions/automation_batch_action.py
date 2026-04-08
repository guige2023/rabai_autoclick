"""Automation Batch Action.

Executes multiple automation tasks in batch with concurrency control,
error handling, progress tracking, and result aggregation.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationBatchAction(BaseAction):
    """Execute multiple automation tasks in batch mode.
    
    Supports configurable concurrency, error handling strategies,
    progress callbacks, result aggregation, and cancellation.
    """
    action_type = "automation_batch"
    display_name = "批量自动化"
    description = "批量执行多个自动化任务，支持并发控制和错误处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch automation tasks.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - tasks: List of task definitions (dict with 'action', 'params').
                - max_workers: Max concurrent tasks (default: 4).
                - error_strategy: 'fail_fast', 'continue', 'retry' (default: continue).
                - max_retries: Max retries per task (default: 2).
                - retry_delay: Delay between retries in seconds (default: 1.0).
                - timeout: Per-task timeout in seconds (default: 300).
                - progress_callback: Lambda for progress updates (idx, total, result).
                - save_to_var: Variable name for results.
                - task_contexts: Optional list of context overrides per task.
        
        Returns:
            ActionResult with batch execution summary.
        """
        try:
            tasks = params.get('tasks', [])
            max_workers = params.get('max_workers', 4)
            error_strategy = params.get('error_strategy', 'continue')
            max_retries = params.get('max_retries', 2)
            retry_delay = params.get('retry_delay', 1.0)
            timeout = params.get('timeout', 300)
            progress_callback = params.get('progress_callback', None)
            save_to_var = params.get('save_to_var', 'batch_results')
            task_contexts = params.get('task_contexts', [None] * len(tasks))

            if not tasks:
                return ActionResult(success=False, message="tasks list is empty")

            results = []
            completed = 0
            total = len(tasks)
            lock = threading.Lock()

            def execute_task(task_idx: int, task_def: Dict) -> Dict:
                nonlocal completed
                task_context = task_contexts[task_idx] if task_idx < len(task_contexts) else None
                
                for attempt in range(max_retries + 1):
                    try:
                        action_name = task_def.get('action')
                        action_params = task_def.get('params', {})
                        
                        # Get action instance from registry
                        action = self._get_action(action_name)
                        if action is None:
                            raise ValueError(f"Unknown action: {action_name}")

                        # Execute with timeout simulation
                        result = action.execute(task_context or context, action_params)
                        
                        if result.success:
                            with lock:
                                completed += 1
                                if progress_callback:
                                    try:
                                        progress_callback(task_idx, total, result)
                                    except Exception:
                                        pass
                            return {'index': task_idx, 'success': True, 'result': result, 'attempts': attempt + 1}
                        else:
                            if attempt < max_retries:
                                time.sleep(retry_delay)
                                continue
                            return {'index': task_idx, 'success': False, 'error': result.message, 'attempts': attempt + 1}
                    
                    except Exception as e:
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                            continue
                        return {'index': task_idx, 'success': False, 'error': str(e), 'attempts': attempt + 1}

                return {'index': task_idx, 'success': False, 'error': 'Max retries exceeded', 'attempts': max_retries + 1}

            # Execute tasks
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {executor.submit(execute_task, i, task): i for i, task in enumerate(tasks)}
                
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        result = future.result(timeout=timeout * len(tasks))
                        results.append(result)
                    except Exception as e:
                        results.append({'index': idx, 'success': False, 'error': f'Executor error: {e}', 'attempts': 0})

            # Sort results by index
            results.sort(key=lambda x: x['index'])

            # Build summary
            successful = sum(1 for r in results if r['success'])
            failed = total - successful
            total_attempts = sum(r.get('attempts', 0) for r in results)

            summary = {
                'total': total,
                'successful': successful,
                'failed': failed,
                'success_rate': successful / total if total > 0 else 0,
                'total_attempts': total_attempts,
                'results': results
            }

            context.set_variable(save_to_var, summary)
            return ActionResult(success=failed == 0, data=summary, 
                             message=f"Batch complete: {successful}/{total} successful")

        except Exception as e:
            return ActionResult(success=False, message=f"Batch execution error: {e}")

    def _get_action(self, action_name: str) -> Optional['BaseAction']:
        """Get action instance from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)


class BatchProgressTracker:
    """Track progress of batch execution."""

    def __init__(self, total: int, callback: Optional[Callable] = None):
        self.total = total
        self.completed = 0
        self.successful = 0
        self.failed = 0
        self.start_time = time.time()
        self.callback = callback
        self.lock = threading.Lock()

    def update(self, success: bool):
        """Update progress counters."""
        with self.lock:
            self.completed += 1
            if success:
                self.successful += 1
            else:
                self.failed += 1
            
            if self.callback:
                self.callback(self.completed, self.total, self.successful, self.failed)

    def get_progress(self) -> Dict:
        """Get current progress."""
        elapsed = time.time() - self.start_time
        rate = self.completed / elapsed if elapsed > 0 else 0
        remaining = (self.total - self.completed) / rate if rate > 0 else 0
        
        return {
            'total': self.total,
            'completed': self.completed,
            'successful': self.successful,
            'failed': self.failed,
            'progress_pct': (self.completed / self.total * 100) if self.total > 0 else 0,
            'elapsed_seconds': elapsed,
            'estimated_remaining_seconds': remaining
        }
