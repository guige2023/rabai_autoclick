"""API Batch Processing Action Module.

Provides batch API operations including bulk requests,
batch processing with retry, and batch result aggregation.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchStatus(Enum):
    """Batch operation statuses."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"


@dataclass
class BatchItem:
    """Single item in a batch operation."""
    id: str
    request: Dict
    retries: int = 0
    status: str = "pending"
    result: Any = None
    error: Optional[str] = None


class BatchProcessorAction(BaseAction):
    """Process API requests in batches with retry and error handling.
    
    Supports configurable batch sizes, parallel execution, and result aggregation.
    """
    action_type = "batch_processor"
    display_name = "批量处理器"
    description = "批量处理API请求，支持重试和错误处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch processing.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - items: List of items to process.
                - batch_size: Items per batch.
                - max_retries: Max retry attempts per item.
                - retry_delay: Delay between retries in seconds.
                - parallel: Enable parallel batch execution.
                - max_workers: Max parallel workers.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with batch processing result or error.
        """
        items = params.get('items', [])
        batch_size = params.get('batch_size', 10)
        max_retries = params.get('max_retries', 3)
        retry_delay = params.get('retry_delay', 1)
        parallel = params.get('parallel', False)
        max_workers = params.get('max_workers', 4)
        output_var = params.get('output_var', 'batch_result')

        if not items:
            return ActionResult(
                success=False,
                message="No items provided for batch processing"
            )

        try:
            start_time = time.time()

            # Convert items to BatchItem objects
            batch_items = [
                BatchItem(
                    id=item.get('id', f"item_{i}"),
                    request=item
                )
                for i, item in enumerate(items)
            ]

            # Process batches
            total_batches = (len(batch_items) + batch_size - 1) // batch_size
            completed = 0
            failed = 0
            all_results = []

            if parallel:
                # Parallel batch execution
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {}

                    for batch_idx in range(total_batches):
                        start = batch_idx * batch_size
                        end = min(start + batch_size, len(batch_items))
                        batch = batch_items[start:end]

                        future = executor.submit(
                            self._process_batch,
                            batch,
                            max_retries,
                            retry_delay,
                            context
                        )
                        futures[future] = batch_idx

                    for future in as_completed(futures):
                        batch_results = future.result()
                        all_results.extend(batch_results)
                        for item_result in batch_results:
                            if item_result.status == 'completed':
                                completed += 1
                            else:
                                failed += 1

            else:
                # Sequential batch execution
                for batch_idx in range(total_batches):
                    start = batch_idx * batch_size
                    end = min(start + batch_size, len(batch_items))
                    batch = batch_items[start:end]

                    batch_results = self._process_batch(
                        batch, max_retries, retry_delay, context
                    )
                    all_results.extend(batch_results)

                    for item_result in batch_results:
                        if item_result.status == 'completed':
                            completed += 1
                        else:
                            failed += 1

            total_duration = time.time() - start_time

            # Determine overall status
            if failed == 0:
                status = BatchStatus.COMPLETED
            elif completed > 0:
                status = BatchStatus.PARTIAL_FAILURE
            else:
                status = BatchStatus.FAILED

            result_data = {
                'status': status.value,
                'total_items': len(items),
                'completed': completed,
                'failed': failed,
                'total_batches': total_batches,
                'duration': total_duration,
                'results': [
                    {
                        'id': r.id,
                        'status': r.status,
                        'result': r.result,
                        'error': r.error,
                        'retries': r.retries
                    }
                    for r in all_results
                ]
            }

            context.variables[output_var] = result_data
            return ActionResult(
                success=status in (BatchStatus.COMPLETED, BatchStatus.PARTIAL_FAILURE),
                data=result_data,
                message=f"Batch processing {status.value}: {completed}/{len(items)} completed"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Batch processing failed: {str(e)}"
            )

    def _process_batch(
        self,
        batch: List[BatchItem],
        max_retries: int,
        retry_delay: float,
        context: Any
    ) -> List[BatchItem]:
        """Process a single batch of items."""
        results = []

        for item in batch:
            for attempt in range(max_retries + 1):
                try:
                    # Execute the request
                    result = self._execute_item(item.request, context)
                    item.status = 'completed'
                    item.result = result
                    break

                except Exception as e:
                    item.retries = attempt + 1
                    if attempt < max_retries:
                        time.sleep(retry_delay * (attempt + 1))
                    else:
                        item.status = 'failed'
                        item.error = str(e)

            results.append(item)

        return results

    def _execute_item(self, request: Dict, context: Any) -> Any:
        """Execute a single batch item request."""
        # Get handler from context
        handler_var = request.get('handler_var', 'batch_handler')
        handler = context.variables.get(handler_var)

        if handler and callable(handler):
            return handler(request)
        else:
            # Default: return the request as-is
            return request


class BulkAPIOperationAction(BaseAction):
    """Perform bulk API operations (create, update, delete).
    
    Supports bulk create, update, and delete with result tracking.
    """
    action_type = "bulk_api_operation"
    display_name = "批量API操作"
    description = "执行批量API操作（创建、更新、删除）"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute bulk API operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'create', 'update', 'delete'.
                - items: Items to operate on.
                - api_endpoint: API endpoint URL.
                - batch_size: Items per API call.
                - id_field: Field containing item ID.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with bulk operation result or error.
        """
        operation = params.get('operation', 'create')
        items = params.get('items', [])
        api_endpoint = params.get('api_endpoint', '')
        batch_size = params.get('batch_size', 50)
        id_field = params.get('id_field', 'id')
        output_var = params.get('output_var', 'bulk_result')

        if not items:
            return ActionResult(
                success=False,
                message="No items provided for bulk operation"
            )

        if not api_endpoint:
            return ActionResult(
                success=False,
                message="API endpoint is required"
            )

        try:
            start_time = time.time()

            # Process in batches
            results = []
            total_success = 0
            total_failed = 0

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_result = self._process_bulk_batch(
                    operation, batch, api_endpoint, id_field
                )
                results.extend(batch_result['items'])

                total_success += batch_result['success_count']
                total_failed += batch_result['failed_count']

            total_duration = time.time() - start_time

            result_data = {
                'operation': operation,
                'total_items': len(items),
                'success_count': total_success,
                'failed_count': total_failed,
                'batch_count': len(results),
                'duration': total_duration,
                'results': results
            }

            context.variables[output_var] = result_data
            return ActionResult(
                success=total_failed == 0,
                data=result_data,
                message=f"Bulk {operation}: {total_success}/{len(items)} succeeded"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Bulk API operation failed: {str(e)}"
            )

    def _process_bulk_batch(
        self,
        operation: str,
        items: List[Dict],
        api_endpoint: str,
        id_field: str
    ) -> Dict:
        """Process a single bulk batch."""
        results = []
        success_count = 0
        failed_count = 0

        for item in items:
            item_id = item.get(id_field, 'unknown')

            try:
                if operation == 'create':
                    # Simulate create operation
                    results.append({
                        'id': item_id,
                        'status': 'created',
                        'success': True
                    })
                elif operation == 'update':
                    results.append({
                        'id': item_id,
                        'status': 'updated',
                        'success': True
                    })
                elif operation == 'delete':
                    results.append({
                        'id': item_id,
                        'status': 'deleted',
                        'success': True
                    })
                success_count += 1

            except Exception as e:
                failed_count += 1
                results.append({
                    'id': item_id,
                    'status': 'failed',
                    'success': False,
                    'error': str(e)
                })

        return {
            'items': results,
            'success_count': success_count,
            'failed_count': failed_count
        }


class BatchResultAggregatorAction(BaseAction):
    """Aggregate and analyze batch processing results.
    
    Supports grouping, filtering, and summary generation.
    """
    action_type = "batch_result_aggregator"
    display_name = "批量结果聚合"
    description = "聚合和分析批量处理结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate batch results.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - results: List of batch results to aggregate.
                - group_by: Field to group results by.
                - filter_status: Filter by status.
                - summary_fields: Fields to include in summary.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with aggregated results or error.
        """
        results = params.get('results', [])
        group_by = params.get('group_by', None)
        filter_status = params.get('filter_status', None)
        summary_fields = params.get('summary_fields', ['status', 'id'])
        output_var = params.get('output_var', 'aggregated')

        if not isinstance(results, list):
            return ActionResult(
                success=False,
                message=f"Expected list for results, got {type(results).__name__}"
            )

        try:
            # Filter by status if specified
            filtered = results
            if filter_status:
                filtered = [r for r in filtered if r.get('status') == filter_status]

            # Group results
            grouped = {}
            if group_by:
                for result in filtered:
                    key = result.get(group_by, 'unknown')
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(result)
            else:
                grouped['all'] = filtered

            # Calculate summary
            summary = {
                'total': len(results),
                'filtered': len(filtered),
                'groups': len(grouped),
                'by_status': {}
            }

            for result in filtered:
                status = result.get('status', 'unknown')
                summary['by_status'][status] = summary['by_status'].get(status, 0) + 1

            result_data = {
                'summary': summary,
                'grouped': grouped if group_by else None,
                'filtered': filtered,
                'fields': summary_fields
            }

            context.variables[output_var] = result_data
            return ActionResult(
                success=True,
                data=result_data,
                message=f"Aggregated {len(filtered)} results into {len(grouped)} groups"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Batch result aggregation failed: {str(e)}"
            )
