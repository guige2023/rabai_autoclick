"""Data Fork action module for RabAI AutoClick.

Forks data streams into multiple parallel processing
branches and collects results.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataForkAction(BaseAction):
    """Fork data into multiple processing branches.

    Splits data stream and processes in parallel,
    then optionally merges results.
    """
    action_type = "data_fork"
    display_name = "数据分叉"
    description = "将数据流分叉到多个处理分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fork data processing.

        Args:
            context: Execution context.
            params: Dict with keys: data, branches (list of processors),
                   fork_mode (duplicate/partition/broadcast),
                   merge_fn, max_workers.

        Returns:
            ActionResult with fork results.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            branches = params.get('branches', [])
            fork_mode = params.get('fork_mode', 'duplicate')
            merge_fn = params.get('merge_fn')
            max_workers = params.get('max_workers', 4)

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            if not branches:
                return ActionResult(
                    success=False,
                    message="At least one branch is required",
                    duration=time.time() - start_time,
                )

            # Partition data if needed
            if fork_mode == 'partition' and isinstance(data, list):
                partitions = self._partition_data(data, len(branches))
            elif fork_mode == 'duplicate':
                partitions = [data] * len(branches)
            else:  # broadcast
                partitions = [data]

            def process_branch(branch: Dict, branch_data: Any) -> Dict:
                branch_name = branch.get('name', 'branch')
                processor = branch.get('processor')
                branch_params = branch.get('params', {})
                branch_start = time.time()

                try:
                    if callable(processor):
                        result = processor(branch_data, branch_params, context)
                    elif hasattr(context, 'execute_action'):
                        result = context.execute_action(processor, {'data': branch_data, **branch_params})
                    else:
                        result = ActionResult(success=False, message="No processor found")

                    if isinstance(result, ActionResult):
                        return {
                            'name': branch_name,
                            'success': result.success,
                            'data': result.data,
                            'message': result.message,
                            'duration': result.duration,
                        }
                    return {
                        'name': branch_name,
                        'success': True,
                        'data': result,
                        'duration': time.time() - branch_start,
                    }
                except Exception as e:
                    return {
                        'name': branch_name,
                        'success': False,
                        'error': str(e),
                        'duration': time.time() - branch_start,
                    }

            branch_results = []
            with ThreadPoolExecutor(max_workers=min(len(branches), max_workers)) as executor:
                futures = {
                    executor.submit(process_branch, branch, partitions[i] if i < len(partitions) else data): branch
                    for i, branch in enumerate(branches)
                }
                for future in as_completed(futures):
                    branch_results.append(future.result())

            # Merge results if merge_fn provided
            merged = None
            if merge_fn and callable(merge_fn):
                try:
                    results_data = [r.get('data') for r in branch_results]
                    merged = merge_fn(results_data, context)
                except Exception as e:
                    merged = {'merge_error': str(e)}

            all_success = all(r.get('success', False) for r in branch_results)
            duration = time.time() - start_time

            return ActionResult(
                success=all_success,
                message=f"Forked to {len(branches)} branches",
                data={
                    'branch_results': branch_results,
                    'merged_result': merged,
                    'all_success': all_success,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Fork error: {str(e)}",
                duration=duration,
            )

    def _partition_data(self, data: List, num_partitions: int) -> List[List]:
        """Partition list into N parts."""
        partition_size = len(data) // num_partitions
        partitions = []
        for i in range(num_partitions):
            start = i * partition_size
            end = start + partition_size if i < num_partitions - 1 else len(data)
            partitions.append(data[start:end])
        return partitions


class DataReducerAction(BaseAction):
    """Reduce forked data streams with custom reducers.

    Combines multiple data sources using reduce
    operations (sum, merge, aggregate, etc.).
    """
    action_type = "data_reducer"
    display_name = "数据归约器"
    description = "使用归约操作合并多个数据流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reduce data.

        Args:
            context: Execution context.
            params: Dict with keys: inputs (list), reduce_type,
                   reduce_fn, group_by.

        Returns:
            ActionResult with reduced result.
        """
        start_time = time.time()
        try:
            inputs = params.get('inputs', [])
            reduce_type = params.get('reduce_type', 'merge')
            reduce_fn = params.get('reduce_fn')
            group_by = params.get('group_by')

            if not inputs:
                return ActionResult(
                    success=False,
                    message="At least one input is required",
                    duration=time.time() - start_time,
                )

            if reduce_type == 'merge':
                if all(isinstance(x, dict) for x in inputs):
                    result = {}
                    for item in inputs:
                        result.update(item)
                elif all(isinstance(x, list) for x in inputs):
                    result = []
                    for item in inputs:
                        result.extend(item)
                else:
                    result = inputs

            elif reduce_type == 'sum' and all(isinstance(x, (int, float)) for x in inputs):
                result = sum(inputs)

            elif reduce_type == 'concat':
                result = ''.join(str(x) for x in inputs)

            elif reduce_type == 'group' and group_by:
                groups: Dict[Any, List] = {}
                for inp in inputs:
                    if isinstance(inp, list):
                        for item in inp:
                            if isinstance(item, dict):
                                key = item.get(group_by)
                                groups.setdefault(key, []).append(item)
                    elif isinstance(inp, dict):
                        key = inp.get(group_by)
                        groups.setdefault(key, []).append(inp)
                result = groups

            elif reduce_type == 'custom' and callable(reduce_fn):
                result = reduce_fn(inputs, context)

            else:
                result = inputs

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Reduced {len(inputs)} inputs ({reduce_type})",
                data=result,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Reduce error: {str(e)}",
                duration=duration,
            )


class DataSplitterAction(BaseAction):
    """Split data based on conditions into multiple buckets.

    Routes each item to appropriate bucket based
    on predicates.
    """
    action_type = "data_splitter"
    display_name = "数据分离器"
    description = "基于条件将数据分离到多个桶"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split data into buckets.

        Args:
            context: Execution context.
            params: Dict with keys: data, buckets (list of {name, predicate}),
                   default_bucket.

        Returns:
            ActionResult with bucket contents.
        """
        start_time = time.time()
        try:
            data = params.get('data', [])
            buckets = params.get('buckets', [])
            default_bucket = params.get('default_bucket', 'other')

            if not isinstance(data, list):
                data = [data]

            if not buckets:
                return ActionResult(
                    success=False,
                    message="At least one bucket is required",
                    duration=time.time() - start_time,
                )

            result = {bucket.get('name', f'bucket_{i}'): [] for i, bucket in enumerate(buckets)}
            result[default_bucket] = []

            for item in data:
                matched = False
                for bucket in buckets:
                    bucket_name = bucket.get('name', 'unknown')
                    predicate = bucket.get('predicate')
                    if callable(predicate):
                        try:
                            if predicate(item, context):
                                result[bucket_name].append(item)
                                matched = True
                                break
                        except Exception:
                            pass
                    elif isinstance(predicate, dict) and isinstance(item, dict):
                        if all(item.get(k) == v for k, v in predicate.items()):
                            result[bucket_name].append(item)
                            matched = True
                            break

                if not matched:
                    result[default_bucket].append(item)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Split {len(data)} items into {len(result)} buckets",
                data={
                    'buckets': result,
                    'bucket_counts': {k: len(v) for k, v in result.items()},
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Splitter error: {str(e)}",
                duration=duration,
            )
