"""Batch Process action module for RabAI AutoClick.

Provides batch processing operations:
- BatchSplitAction: Split data into batches
- BatchProcessAction: Process data in batches
- BatchMergeAction: Merge batch results
- BatchRetryAction: Retry failed batch items
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchSplitAction(BaseAction):
    """Split data into batches."""
    action_type = "batch_split"
    display_name = "批量分割"
    description = "将数据分割为批次"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch split."""
        data = params.get('data', [])
        batch_size = params.get('batch_size', 10)
        output_var = params.get('output_var', 'batches')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            batches = []
            for i in range(0, len(resolved_data), batch_size):
                batches.append(resolved_data[i:i + batch_size])

            result = {
                'batches': batches,
                'batch_count': len(batches),
                'batch_size': batch_size,
                'total_records': len(resolved_data),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Split into {len(batches)} batches of ~{batch_size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch split error: {e}")


class BatchProcessAction(BaseAction):
    """Process data in batches."""
    action_type = "batch_process"
    display_name = "批量处理"
    description = "批量处理数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch processing."""
        batches = params.get('batches', [])
        processor = params.get('processor', 'identity')
        max_workers = params.get('max_workers', 4)
        output_var = params.get('output_var', 'batch_results')

        if not batches:
            return ActionResult(success=False, message="batches are required")

        try:
            resolved_batches = context.resolve_value(batches) if context else batches

            results = []
            success_count = 0
            error_count = 0

            def process_batch(batch):
                processed = []
                for item in batch:
                    if processor == 'identity':
                        processed.append(item)
                    elif processor == 'uppercase' and isinstance(item, dict):
                        new_item = {}
                        for k, v in item.items():
                            new_item[k] = v.upper() if isinstance(v, str) else v
                        processed.append(new_item)
                    elif processor == 'lowercase' and isinstance(item, dict):
                        new_item = {}
                        for k, v in item.items():
                            new_item[k] = v.lower() if isinstance(v, str) else v
                        processed.append(new_item)
                    else:
                        processed.append(item)
                return processed

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(resolved_batches)}
                for future in as_completed(futures):
                    batch_index = futures[future]
                    try:
                        result = future.result()
                        results.append({'batch': batch_index, 'success': True, 'result': result})
                        success_count += 1
                    except Exception as e:
                        results.append({'batch': batch_index, 'success': False, 'error': str(e)})
                        error_count += 1

            all_results = []
            for r in results:
                if r['success']:
                    all_results.extend(r['result'])

            result = {
                'results': all_results,
                'batch_count': len(resolved_batches),
                'success_count': success_count,
                'error_count': error_count,
            }

            return ActionResult(
                success=error_count == 0,
                data={output_var: result},
                message=f"Batch process: {success_count}/{len(resolved_batches)} batches successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch process error: {e}")


class BatchMergeAction(BaseAction):
    """Merge batch results."""
    action_type = "batch_merge"
    display_name = "批量合并"
    description = "合并批次结果"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch merge."""
        batch_results = params.get('batch_results', [])
        merge_strategy = params.get('strategy', 'concat')
        output_var = params.get('output_var', 'merged_result')

        if not batch_results:
            return ActionResult(success=False, message="batch_results are required")

        try:
            resolved_results = context.resolve_value(batch_results) if context else batch_results

            if merge_strategy == 'concat':
                merged = []
                for batch in resolved_results:
                    if isinstance(batch, list):
                        merged.extend(batch)
                    else:
                        merged.append(batch)
            elif merge_strategy == 'union':
                seen = set()
                merged = []
                for batch in resolved_results:
                    if isinstance(batch, list):
                        for item in batch:
                            key = str(item) if not isinstance(item, (str, int, float)) else item
                            if key not in seen:
                                seen.add(key)
                                merged.append(item)
                    else:
                        key = str(batch) if not isinstance(batch, (str, int, float)) else batch
                        if key not in seen:
                            seen.add(key)
                            merged.append(batch)
            elif merge_strategy == 'zip':
                merged = []
                max_len = max(len(b) for b in resolved_results if isinstance(b, list))
                for i in range(max_len):
                    row = []
                    for batch in resolved_results:
                        if isinstance(batch, list) and i < len(batch):
                            row.append(batch[i])
                    merged.append(row)
            else:
                merged = list(resolved_results)

            result = {
                'merged': merged,
                'record_count': len(merged),
                'strategy': merge_strategy,
                'batch_count': len(resolved_results),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Merged {len(resolved_results)} batches into {len(merged)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch merge error: {e}")


class BatchRetryAction(BaseAction):
    """Retry failed batch items."""
    action_type = "batch_retry"
    display_name = "批量重试"
    description = "重试失败的批次"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch retry."""
        failed_items = params.get('failed_items', [])
        max_retries = params.get('max_retries', 3)
        retry_delay = params.get('retry_delay', 1)
        output_var = params.get('output_var', 'retry_result')

        if not failed_items:
            return ActionResult(success=True, data={output_var: {'retried': [], 'retry_count': 0}}, message="No failed items to retry")

        try:
            resolved_items = context.resolve_value(failed_items) if context else failed_items

            retried = []
            still_failed = []

            for item in resolved_items:
                retry_count = item.get('retry_count', 0)
                if retry_count < max_retries:
                    retried.append({
                        **item,
                        'retry_count': retry_count + 1,
                        'last_retry_at': 'now',
                    })
                else:
                    still_failed.append(item)

            result = {
                'retried': retried,
                'still_failed': still_failed,
                'retry_count': len(retried),
                'failed_count': len(still_failed),
                'max_retries': max_retries,
            }

            return ActionResult(
                success=len(still_failed) == 0,
                data={output_var: result},
                message=f"Retried {len(retried)} items, {len(still_failed)} still failing"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch retry error: {e}")
