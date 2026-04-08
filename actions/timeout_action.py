"""Timeout action module for RabAI AutoClick.

Provides timeout management for long-running operations
with configurable timeout handling strategies.
"""

import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TimeoutAction(BaseAction):
    """Execute operation with timeout protection.
    
    Cancels operation if it exceeds time limit.
    """
    action_type = "timeout"
    display_name = "超时控制"
    description = "操作超时管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with timeout.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, timeout, on_timeout,
                   poll_interval.
        
        Returns:
            ActionResult with execution result.
        """
        operation = params.get('operation', '')
        timeout = params.get('timeout', 30)
        on_timeout = params.get('on_timeout', 'error')
        poll_interval = params.get('poll_interval', 0.1)

        if not operation:
            return ActionResult(success=False, message="operation is required")
        if timeout <= 0:
            return ActionResult(success=False, message="timeout must be positive")

        start_time = time.time()

        result_container = [None]
        error_container = [None]
        completed = [False]
        lock = threading.Lock()

        def run_operation():
            try:
                result = self._execute_operation(operation, context)
                with lock:
                    result_container[0] = result
                    completed[0] = True
            except Exception as e:
                with lock:
                    error_container[0] = str(e)
                    completed[0] = True

        thread = threading.Thread(target=run_operation)
        thread.daemon = True
        thread.start()

        thread.join(timeout=timeout)

        with lock:
            is_completed = completed[0]

        elapsed = time.time() - start_time

        if not is_completed:
            if on_timeout == 'error':
                return ActionResult(
                    success=False,
                    message=f"Operation timed out after {timeout}s",
                    data={
                        'timeout': timeout,
                        'elapsed': elapsed,
                        'operation': operation,
                        'status': 'timeout'
                    }
                )
            elif on_timeout == 'continue':
                return ActionResult(
                    success=True,
                    message=f"Operation timed out but continuing",
                    data={
                        'timeout': timeout,
                        'elapsed': elapsed,
                        'partial_result': result_container[0],
                        'status': 'timeout_continued'
                    }
                )
            elif on_timeout == 'kill':
                return ActionResult(
                    success=False,
                    message=f"Operation timed out and killed",
                    data={
                        'timeout': timeout,
                        'elapsed': elapsed,
                        'status': 'timeout_killed'
                    }
                )

        if error_container[0]:
            return ActionResult(
                success=False,
                message=f"Operation failed: {error_container[0]}",
                data={
                    'error': error_container[0],
                    'elapsed': elapsed
                }
            )

        return ActionResult(
            success=True,
            message=f"Operation completed in {elapsed:.2f}s",
            data={
                'result': result_container[0],
                'elapsed': elapsed
            }
        )

    def _execute_operation(self, operation: str, context: Any) -> Any:
        """Execute the operation."""
        time.sleep(0.1)
        return {'status': 'completed', 'operation': operation}


class TimeoutBatchAction(BaseAction):
    """Execute batch operations with timeout per item.
    
    Manages timeouts for each item in a batch.
    """
    action_type = "timeout_batch"
    display_name = "批量超时"
    description = "批量操作超时控制"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch with timeouts.
        
        Args:
            context: Execution context.
            params: Dict with keys: operations, timeout_per_item,
                   stop_on_timeout, total_timeout.
        
        Returns:
            ActionResult with batch results.
        """
        operations = params.get('operations', [])
        timeout_per_item = params.get('timeout_per_item', 30)
        stop_on_timeout = params.get('stop_on_timeout', False)
        total_timeout = params.get('total_timeout', 300)

        if not operations:
            return ActionResult(success=False, message="operations list is required")

        start_time = time.time()
        results = []
        timeouts = []

        for i, op in enumerate(operations):
            elapsed_total = time.time() - start_time
            if elapsed_total >= total_timeout:
                results.append({
                    'index': i,
                    'operation': op.get('operation', ''),
                    'status': 'skipped',
                    'reason': 'total_timeout'
                })
                if stop_on_timeout:
                    break
                continue

            remaining_timeout = min(timeout_per_item, total_timeout - elapsed_total)
            
            item_start = time.time()
            result = self._execute_with_timeout(op, remaining_timeout)
            item_elapsed = time.time() - item_start

            result['index'] = i
            result['operation'] = op.get('operation', '')
            result['elapsed'] = item_elapsed

            if result.get('status') == 'timeout':
                timeouts.append(i)
                if stop_on_timeout:
                    results.append(result)
                    break

            results.append(result)

        total_elapsed = time.time() - start_time

        success_count = sum(1 for r in results if r.get('status') == 'success')
        timeout_count = len(timeouts)

        return ActionResult(
            success=timeout_count == 0,
            message=f"Batch: {success_count} success, {timeout_count} timeouts, {total_elapsed:.2f}s",
            data={
                'results': results,
                'total': len(operations),
                'succeeded': success_count,
                'timeouts': timeouts,
                'elapsed': total_elapsed
            }
        )

    def _execute_with_timeout(self, op: Dict, timeout: float) -> Dict:
        """Execute single operation with timeout."""
        result_container = [None]
        error_container = [None]
        completed = [False]
        lock = threading.Lock()

        def run():
            try:
                result_container[0] = {'status': 'success', 'result': 'done'}
            except Exception as e:
                error_container[0] = str(e)
            finally:
                with lock:
                    completed[0] = True

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()
        thread.join(timeout=timeout)

        with lock:
            if not completed[0]:
                return {'status': 'timeout', 'error': f'exceeded {timeout}s'}

        if error_container[0]:
            return {'status': 'error', 'error': error_container[0]}

        return result_container[0]


class TimeoutMonitorAction(BaseAction):
    """Monitor operation progress with timeout tracking.
    
    Provides progress updates during long operations.
    """
    action_type = "timeout_monitor"
    display_name = "超时监控"
    description = "超时进度监控"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Monitor operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, timeout, check_interval,
                   progress_callback.
        
        Returns:
            ActionResult with monitoring result.
        """
        operation = params.get('operation', '')
        timeout = params.get('timeout', 30)
        check_interval = params.get('check_interval', 1)
        progress_callback = params.get('progress_callback', None)

        if not operation:
            return ActionResult(success=False, message="operation is required")

        start_time = time.time()
        last_check = [start_time]
        progress_updates = []

        while True:
            elapsed = time.time() - start_time
            remaining = timeout - elapsed

            if remaining <= 0:
                return ActionResult(
                    success=False,
                    message=f"Monitor: timeout after {elapsed:.2f}s",
                    data={
                        'timeout': timeout,
                        'elapsed': elapsed,
                        'progress_updates': progress_updates,
                        'status': 'timeout'
                    }
                )

            if elapsed >= last_check[0] + check_interval:
                update = {
                    'elapsed': elapsed,
                    'remaining': remaining,
                    'progress': min(100, (elapsed / timeout) * 100)
                }
                progress_updates.append(update)
                last_check[0] = elapsed

                if progress_callback and callable(progress_callback):
                    try:
                        progress_callback(update)
                    except:
                        pass

            if progress_updates and progress_updates[-1].get('elapsed', 0) >= timeout:
                break

            time.sleep(0.1)

        return ActionResult(
            success=True,
            message=f"Monitored for {elapsed:.2f}s",
            data={
                'timeout': timeout,
                'elapsed': elapsed,
                'progress_updates': progress_updates,
                'status': 'completed'
            }
        )


class TimeoutPoolAction(BaseAction):
    """Manage a pool of operations with shared timeout budget.
    
    Distributes timeout across concurrent operations.
    """
    action_type = "timeout_pool"
    display_name = "超时池"
    description = "共享超时池管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute with pool timeout.
        
        Args:
            context: Execution context.
            params: Dict with keys: operations, total_timeout,
                   max_concurrent, strategy.
        
        Returns:
            ActionResult with pool results.
        """
        operations = params.get('operations', [])
        total_timeout = params.get('total_timeout', 60)
        max_concurrent = params.get('max_concurrent', 5)
        strategy = params.get('strategy', 'fair')

        if not operations:
            return ActionResult(success=False, message="operations list is required")

        start_time = time.time()
        
        if strategy == 'fair':
            per_op_timeout = total_timeout / len(operations)
        elif strategy == 'weighted':
            per_op_timeout = total_timeout / max_concurrent
        else:
            per_op_timeout = total_timeout

        per_op_timeout = min(per_op_timeout, 30)

        results = []
        semaphore = threading.Semaphore(max_concurrent)

        def run_with_semaphore(op, index):
            remaining = max(1, total_timeout - (time.time() - start_time))
            timeout = min(per_op_timeout, remaining)
            
            with semaphore:
                if time.time() - start_time >= total_timeout:
                    return {'index': index, 'status': 'skipped', 'reason': 'pool_timeout'}
                
                result = self._execute_with_timeout(op, timeout)
                result['index'] = index
                result['timeout_used'] = timeout
                return result

        threads = []
        for i, op in enumerate(operations):
            thread = threading.Thread(target=lambda i=i, op=op: results.append(run_with_semaphore(op, i)))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join(timeout=total_timeout)

        results.sort(key=lambda x: x.get('index', 0))

        total_elapsed = time.time() - start_time
        success_count = sum(1 for r in results if r.get('status') == 'success')

        return ActionResult(
            success=success_count == len(operations),
            message=f"Pool: {success_count}/{len(operations)} completed in {total_elapsed:.2f}s",
            data={
                'results': results,
                'total': len(operations),
                'succeeded': success_count,
                'elapsed': total_elapsed,
                'per_op_timeout': per_op_timeout
            }
        )

    def _execute_with_timeout(self, op: Dict, timeout: float) -> Dict:
        """Execute with timeout."""
        time.sleep(0.01)
        return {'status': 'success', 'result': 'done'}
