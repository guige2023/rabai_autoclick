"""Batch action module for RabAI AutoClick.

Provides batch processing capabilities for handling large datasets
in chunks with parallel processing support.
"""

import time
import concurrent.futures
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchProcessorAction(BaseAction):
    """Process data in configurable batch sizes.
    
    Splits large datasets into chunks and processes
    each batch through a configured action.
    """
    action_type = "batch_processor"
    display_name = "批处理"
    description = "分批处理大数据集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process data in batches.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, batch_size, action,
                   action_params, parallel, max_workers.
        
        Returns:
            ActionResult with batch processing results.
        """
        data = params.get('data', [])
        batch_size = params.get('batch_size', 100)
        action = params.get('action', '')
        action_params = params.get('action_params', {})
        parallel = params.get('parallel', False)
        max_workers = params.get('max_workers', 4)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not data:
            return ActionResult(
                success=True,
                message="No data to process",
                data={'processed': 0, 'batches': 0}
            )

        batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
        results = []
        batch_results = []

        if parallel and action:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for i, batch in enumerate(batches):
                    batch_params = {**action_params, 'batch': batch, 'batch_index': i}
                    futures.append(executor.submit(self._execute_action, action, batch_params))

                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        batch_results.append(result)
                    except Exception as e:
                        batch_results.append({'success': False, 'error': str(e)})
        else:
            for i, batch in enumerate(batches):
                batch_params = {**action_params, 'batch': batch, 'batch_index': i}
                result = self._execute_action(action, batch_params)
                batch_results.append(result)

        success_count = sum(1 for r in batch_results if r.get('success', False))
        failed_count = len(batch_results) - success_count

        return ActionResult(
            success=failed_count == 0,
            message=f"Batch processed: {success_count}/{len(batches)} batches succeeded",
            data={
                'total_records': len(data),
                'batch_count': len(batches),
                'batch_size': batch_size,
                'successful_batches': success_count,
                'failed_batches': failed_count,
                'results': batch_results,
                'parallel': parallel
            },
            duration=time.time() - start_time
        )

    def _execute_action(self, action_name: str, params: Dict) -> Dict:
        """Execute a named action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                result = action.execute(None, params)
                return {'success': result.success, 'data': result.data, 'message': result.message}
        except ImportError:
            pass
        return {'success': False, 'error': f"Action '{action_name}' not found"}


class ChunkedIteratorAction(BaseAction):
    """Iterate over data in chunks with state management.
    
    Provides chunked iteration over large datasets with
    state tracking and resumable processing.
    """
    action_type = "chunked_iterator"
    display_name = "分块迭代"
    description = "分块迭代大数据集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Iterate data in chunks.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, chunk_size, resume_from,
                   state_field, operation (get|resume|reset).
        
        Returns:
            ActionResult with chunk data.
        """
        data = params.get('data', [])
        chunk_size = params.get('chunk_size', 100)
        resume_from = params.get('resume_from', 0)
        operation = params.get('operation', 'get')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        total_chunks = (len(data) + chunk_size - 1) // chunk_size

        if operation == 'reset':
            return ActionResult(
                success=True,
                message="Iterator reset",
                data={'reset': True}
            )

        if resume_from >= total_chunks:
            return ActionResult(
                success=True,
                message=f"No more chunks (at {resume_from}/{total_chunks})",
                data={
                    'chunk': None,
                    'chunk_index': resume_from,
                    'total_chunks': total_chunks,
                    'exhausted': True
                }
            )

        chunk_index = resume_from
        start_idx = chunk_index * chunk_size
        end_idx = min(start_idx + chunk_size, len(data))
        chunk = data[start_idx:end_idx]

        return ActionResult(
            success=True,
            message=f"Chunk {chunk_index + 1}/{total_chunks} ({len(chunk)} records)",
            data={
                'chunk': chunk,
                'chunk_index': chunk_index,
                'start_index': start_idx,
                'end_index': end_idx,
                'chunk_size': len(chunk),
                'total_records': len(data),
                'total_chunks': total_chunks,
                'has_more': chunk_index + 1 < total_chunks
            },
            duration=time.time() - start_time
        )


class ParallelBatchAction(BaseAction):
    """Execute multiple batches in parallel with coordination.
    
    Runs independent batch jobs concurrently and collects
    results when all complete.
    """
    action_type = "parallel_batch"
    display_name = "并行批处理"
    description = "并行执行多个批处理任务"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parallel batches.
        
        Args:
            context: Execution context.
            params: Dict with keys: batch_configs (list of batch configs),
                   max_workers, fail_fast, collect_mode (all|first).
        
        Returns:
            ActionResult with parallel execution results.
        """
        batch_configs = params.get('batch_configs', [])
        max_workers = params.get('max_workers', 4)
        fail_fast = params.get('fail_fast', False)
        collect_mode = params.get('collect_mode', 'all')
        start_time = time.time()

        if not batch_configs:
            return ActionResult(
                success=False,
                message="No batch configurations provided"
            )

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, config in enumerate(batch_configs):
                future = executor.submit(self._process_batch, config)
                futures[future] = i

            for future in concurrent.futures.as_completed(futures):
                batch_idx = futures[future]
                try:
                    result = future.result()
                    results.append({
                        'batch_index': batch_idx,
                        'success': result.get('success', False),
                        'data': result.get('data'),
                        'message': result.get('message', '')
                    })

                    if fail_fast and not result.get('success', False):
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        return ActionResult(
                            success=False,
                            message=f"Batch {batch_idx} failed, stopping",
                            data={
                                'results': results,
                                'failed_at': batch_idx,
                                'total_batches': len(batch_configs)
                            }
                        )

                    if collect_mode == 'first' and result.get('success', False):
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        return ActionResult(
                            success=True,
                            message=f"First successful batch: {batch_idx}",
                            data={
                                'results': results,
                                'first_success': batch_idx,
                                'total_batches': len(batch_configs)
                            }
                        )
                except Exception as e:
                    results.append({
                        'batch_index': batch_idx,
                        'success': False,
                        'error': str(e)
                    })

        successful = sum(1 for r in results if r.get('success', False))
        return ActionResult(
            success=successful == len(batch_configs),
            message=f"Parallel batches: {successful}/{len(batch_configs)} succeeded",
            data={
                'results': results,
                'successful_batches': successful,
                'failed_batches': len(batch_configs) - successful,
                'total_batches': len(batch_configs)
            },
            duration=time.time() - start_time
        )

    def _process_batch(self, config: Dict) -> Dict:
        """Process a single batch configuration."""
        batch_data = config.get('data', [])
        action = config.get('action', '')
        action_params = config.get('params', {})

        batch_params = {**action_params, 'batch': batch_data}
        result = self._execute_action(action, batch_params)
        return result

    def _execute_action(self, action_name: str, params: Dict) -> Dict:
        """Execute a named action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                result = action.execute(None, params)
                return {'success': result.success, 'data': result.data, 'message': result.message}
        except ImportError:
            pass
        return {'success': False, 'error': f"Action '{action_name}' not found"}
