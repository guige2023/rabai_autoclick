"""Pipeline action module for RabAI AutoClick.

Provides data pipeline orchestration and execution.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineAction(BaseAction):
    """Data pipeline orchestration.
    
    Supports defining and executing multi-step data pipelines
    with stage definitions, error handling, and retry logic.
    """
    action_type = "pipeline"
    display_name = "数据管道"
    description = "多步骤数据处理管道编排"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'run', 'create', 'add_stage'
                - stages: List of pipeline stage definitions
                - data: Input data for pipeline
                - stop_on_error: Stop pipeline on first error (default True)
                - max_retries: Max retries per stage (default 0)
        
        Returns:
            ActionResult with pipeline execution result.
        """
        command = params.get('command', 'run')
        stages = params.get('stages', [])
        data = params.get('data')
        stop_on_error = params.get('stop_on_error', True)
        max_retries = params.get('max_retries', 0)
        
        if command == 'run':
            return self._run_pipeline(stages, data, stop_on_error, max_retries)
        
        if command == 'create':
            return ActionResult(
                success=True,
                message="Pipeline template created",
                data={'stages': [], 'stage_count': 0}
            )
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _run_pipeline(self, stages: List[Dict], data: Any, stop_on_error: bool, max_retries: int) -> ActionResult:
        """Execute a pipeline of stages."""
        if not stages:
            return ActionResult(success=False, message="No stages defined in pipeline")
        
        results: List[Dict[str, Any]] = []
        current_data = data
        start_time = time.time()
        
        for i, stage in enumerate(stages):
            stage_name = stage.get('name', f'stage_{i}')
            stage_type = stage.get('type', 'transform')
            stage_config = stage.get('config', {})
            retry_count = 0
            stage_result = None
            
            while retry_count <= max_retries:
                try:
                    stage_result = self._execute_stage(stage_type, stage_config, current_data)
                    
                    if stage_result.success:
                        current_data = stage_result.data.get('output', stage_result.data)
                        results.append({
                            'stage': stage_name,
                            'type': stage_type,
                            'success': True,
                            'retries': retry_count,
                            'output_keys': list(stage_result.data.keys()) if isinstance(stage_result.data, dict) else None
                        })
                        break
                    else:
                        if retry_count < max_retries:
                            retry_count += 1
                            time.sleep(0.1 * retry_count)
                        else:
                            results.append({
                                'stage': stage_name,
                                'type': stage_type,
                                'success': False,
                                'retries': retry_count,
                                'error': stage_result.message
                            })
                            if stop_on_error:
                                return ActionResult(
                                    success=False,
                                    message=f"Pipeline failed at stage '{stage_name}': {stage_result.message}",
                                    data={
                                        'results': results,
                                        'failed_at': i,
                                        'failed_stage': stage_name,
                                        'total_time': time.time() - start_time
                                    }
                                )
                except Exception as e:
                    if retry_count < max_retries:
                        retry_count += 1
                    else:
                        results.append({
                            'stage': stage_name,
                            'type': stage_type,
                            'success': False,
                            'retries': retry_count,
                            'error': str(e)
                        })
                        if stop_on_error:
                            return ActionResult(
                                success=False,
                                message=f"Pipeline failed at stage '{stage_name}': {e}",
                                data={
                                    'results': results,
                                    'failed_at': i,
                                    'failed_stage': stage_name
                                }
                            )
                        break
        
        elapsed = time.time() - start_time
        all_success = all(r['success'] for r in results)
        
        return ActionResult(
            success=all_success,
            message=f"Pipeline {'succeeded' if all_success else 'failed'}: {sum(1 for r in results if r['success'])}/{len(results)} stages in {elapsed:.2f}s",
            data={
                'results': results,
                'stage_count': len(stages),
                'success_count': sum(1 for r in results if r['success']),
                'failure_count': sum(1 for r in results if not r['success']),
                'total_time': elapsed,
                'output': current_data
            }
        )
    
    def _execute_stage(self, stage_type: str, config: Dict, data: Any) -> ActionResult:
        """Execute a single pipeline stage."""
        if stage_type == 'filter':
            filter_key = config.get('field')
            filter_op = config.get('op', '==')
            filter_value = config.get('value')
            
            if isinstance(data, list):
                filtered = []
                for item in data:
                    val = item.get(filter_key) if isinstance(item, dict) else getattr(item, filter_key, None)
                    if self._compare(val, filter_op, filter_value):
                        filtered.append(item)
                return ActionResult(success=True, message="Filter applied", data={'output': filtered})
            return ActionResult(success=False, message="Filter requires list data")
        
        if stage_type == 'transform':
            transform_fn = config.get('fn', 'identity')
            field = config.get('field')
            
            if transform_fn == 'upper' and field:
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            item[field] = str(item[field]).upper()
                return ActionResult(success=True, message="Transform applied", data={'output': data})
            elif transform_fn == 'lower' and field:
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            item[field] = str(item[field]).lower()
                return ActionResult(success=True, message="Transform applied", data={'output': data})
            elif transform_fn == 'to_string' and field:
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            item[field] = str(item[field])
                return ActionResult(success=True, message="Transform applied", data={'output': data})
            elif transform_fn == 'to_int' and field:
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            try:
                                item[field] = int(item[field])
                            except (ValueError, TypeError):
                                pass
                return ActionResult(success=True, message="Transform applied", data={'output': data})
            
            return ActionResult(success=True, message="Transform applied", data={'output': data})
        
        if stage_type == 'sort':
            sort_key = config.get('by')
            reverse = config.get('reverse', False)
            
            if isinstance(data, list):
                sorted_data = sorted(data, key=lambda x: x.get(sort_key, '') if isinstance(x, dict) else getattr(x, sort_key, ''), reverse=reverse)
                return ActionResult(success=True, message="Sorted", data={'output': sorted_data})
            return ActionResult(success=False, message="Sort requires list data")
        
        if stage_type == 'limit':
            limit = config.get('count', 10)
            if isinstance(data, list):
                return ActionResult(success=True, message=f"Limited to {limit}", data={'output': data[:limit]})
            return ActionResult(success=False, message="Limit requires list data")
        
        if stage_type == 'group_by':
            group_key = config.get('by')
            if isinstance(data, list) and group_key:
                from collections import defaultdict
                groups: Dict[str, List] = defaultdict(list)
                for item in data:
                    key = item.get(group_key) if isinstance(item, dict) else getattr(item, group_key, None)
                    groups[str(key)].append(item)
                return ActionResult(success=True, message=f"Grouped into {len(groups)} groups", data={'output': dict(groups)})
            return ActionResult(success=False, message="group_by requires list data with 'by' field")
        
        if stage_type == 'dedupe':
            seen = set()
            if isinstance(data, list):
                deduped = []
                for item in data:
                    key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
                    if key not in seen:
                        seen.add(key)
                        deduped.append(item)
                return ActionResult(success=True, message=f"Deduped: {len(data)} -> {len(deduped)}", data={'output': deduped})
            return ActionResult(success=False, message="Dedup requires list data")
        
        return ActionResult(success=False, message=f"Unknown stage type: {stage_type}")
    
    def _compare(self, value: Any, op: str, target: Any) -> bool:
        """Compare value with target using operator."""
        if op == '==':
            return value == target
        elif op == '!=':
            return value != target
        elif op == '>':
            return value > target
        elif op == '>=':
            return value >= target
        elif op == '<':
            return value < target
        elif op == '<=':
            return value <= target
        elif op == 'in':
            return value in target
        elif op == 'not_in':
            return value not in target
        elif op == 'contains':
            return target in value
        elif op == 'starts_with':
            return str(value).startswith(target)
        elif op == 'ends_with':
            return str(value).endswith(target)
        return False
