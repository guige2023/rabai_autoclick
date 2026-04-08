"""Batch action module for RabAI AutoClick.

Provides batch operation utilities for processing
multiple items with concurrency control.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchParallelAction(BaseAction):
    """Execute batch operations in parallel.
    
    Supports configurable worker count and timeout.
    """
    action_type = "batch_parallel"
    display_name = "并行批处理"
    description = "并行执行批量操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch in parallel.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, worker_count,
                   timeout, save_to_var.
        
        Returns:
            ActionResult with execution results.
        """
        items = params.get('items', [])
        worker_count = params.get('worker_count', 4)
        timeout = params.get('timeout', 60)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        results = []
        success_count = 0
        failure_count = 0

        start_time = time.time()

        def process_item(item):
            """Simple identity processing - in real usage this would be more complex."""
            return {'processed': True, 'item': item}

        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_item = {executor.submit(process_item, item): item for item in items}

                for future in as_completed(future_to_item, timeout=timeout):
                    try:
                        result = future.result()
                        results.append({'success': True, 'result': result})
                        success_count += 1
                    except Exception as e:
                        results.append({'success': False, 'error': str(e)})
                        failure_count += 1

            elapsed = time.time() - start_time

            result_data = {
                'results': results,
                'total': len(items),
                'success': success_count,
                'failure': failure_count,
                'elapsed': elapsed,
                'workers': worker_count
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"并行处理完成: {success_count}/{len(items)} 成功 ({elapsed:.2f}s)",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"并行处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'worker_count': 4,
            'timeout': 60,
            'save_to_var': None
        }


class BatchSequentialAction(BaseAction):
    """Execute batch operations sequentially.
    
    Supports delay between items and early exit on failure.
    """
    action_type = "batch_sequential"
    display_name = "顺序批处理"
    description = "顺序执行批量操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch sequentially.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, delay, stop_on_error,
                   save_to_var.
        
        Returns:
            ActionResult with execution results.
        """
        items = params.get('items', [])
        delay = params.get('delay', 0)
        stop_on_error = params.get('stop_on_error', False)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        results = []
        success_count = 0
        failure_count = 0
        stopped_early = False

        start_time = time.time()

        for i, item in enumerate(items):
            try:
                # Simulate processing
                result = {'processed': True, 'index': i, 'item': item}
                results.append({'success': True, 'result': result})
                success_count += 1
            except Exception as e:
                results.append({'success': False, 'error': str(e), 'index': i})
                failure_count += 1
                if stop_on_error:
                    stopped_early = True
                    break

            if delay > 0 and i < len(items) - 1:
                time.sleep(delay)

        elapsed = time.time() - start_time

        result_data = {
            'results': results,
            'total': len(items),
            'success': success_count,
            'failure': failure_count,
            'elapsed': elapsed,
            'stopped_early': stopped_early
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"顺序处理完成: {success_count}/{len(items)} 成功 ({elapsed:.2f}s)",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'delay': 0,
            'stop_on_error': False,
            'save_to_var': None
        }
