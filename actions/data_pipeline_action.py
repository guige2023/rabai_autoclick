"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline processing with stage composition,
parallel execution, error handling, and checkpoint support.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPipelineAction(BaseAction):
    """Execute multi-stage data processing pipelines.
    
    Supports sequential and parallel stages, error handling,
    checkpoint/resume, and result aggregation.
    """
    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "多阶段数据处理管道，支持并行和串行执行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a data processing pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: stages (list of stage configs),
                   parallel (bool), stop_on_error (bool).
        
        Returns:
            ActionResult with pipeline execution results.
        """
        stages = params.get('stages', [])
        if not stages:
            return ActionResult(success=False, message="No stages defined")
        
        if not isinstance(stages, list):
            return ActionResult(success=False, message="stages must be a list")
        
        parallel = params.get('parallel', False)
        stop_on_error = params.get('stop_on_error', True)
        checkpoint_enabled = params.get('checkpoint_enabled', False)
        
        pipeline_context = {
            'data': None,
            'stage_results': [],
            'errors': [],
            'start_time': time.time(),
            'checkpoint_data': {}
        }
        
        if parallel:
            return self._execute_parallel(
                stages, pipeline_context, stop_on_error, checkpoint_enabled
            )
        else:
            return self._execute_sequential(
                stages, pipeline_context, stop_on_error, checkpoint_enabled
            )
    
    def _execute_sequential(
        self,
        stages: List[Dict[str, Any]],
        pipeline_context: Dict[str, Any],
        stop_on_error: bool,
        checkpoint_enabled: bool
    ) -> ActionResult:
        """Execute stages sequentially."""
        initial_data = pipeline_context['data']
        
        for idx, stage in enumerate(stages):
            stage_name = stage.get('name', f'stage_{idx}')
            stage_type = stage.get('type', 'transform')
            stage_config = stage.get('config', {})
            stage_input = stage.get('input', None)
            
            try:
                result = self._execute_stage(
                    stage_type, stage_config, stage_input, pipeline_context
                )
                
                pipeline_context['stage_results'].append({
                    'index': idx,
                    'name': stage_name,
                    'success': result.success,
                    'message': result.message,
                    'data': result.data
                })
                
                if result.success and result.data is not None:
                    pipeline_context['data'] = result.data
                
                if checkpoint_enabled:
                    pipeline_context['checkpoint_data'][stage_name] = {
                        'completed': True,
                        'data': result.data,
                        'timestamp': time.time()
                    }
                
                if not result.success and stop_on_error:
                    return ActionResult(
                        success=False,
                        message=f"Pipeline failed at stage '{stage_name}': {result.message}",
                        data={
                            'stages_completed': idx,
                            'failed_stage': stage_name,
                            'stage_results': pipeline_context['stage_results']
                        }
                    )
                    
            except Exception as e:
                error_msg = f"Stage '{stage_name}' exception: {e}"
                pipeline_context['errors'].append(error_msg)
                
                pipeline_context['stage_results'].append({
                    'index': idx,
                    'name': stage_name,
                    'success': False,
                    'error': str(e)
                })
                
                if stop_on_error:
                    return ActionResult(
                        success=False,
                        message=error_msg,
                        data={
                            'stages_completed': idx,
                            'failed_stage': stage_name,
                            'stage_results': pipeline_context['stage_results']
                        }
                    )
        
        elapsed = time.time() - pipeline_context['start_time']
        successful = sum(1 for r in pipeline_context['stage_results'] if r.get('success', False))
        
        return ActionResult(
            success=successful == len(stages),
            message=f"Pipeline completed: {successful}/{len(stages)} stages succeeded in {elapsed:.2f}s",
            data={
                'stages': pipeline_context['stage_results'],
                'final_data': pipeline_context['data'],
                'elapsed': elapsed
            }
        )
    
    def _execute_parallel(
        self,
        stages: List[Dict[str, Any]],
        pipeline_context: Dict[str, Any],
        stop_on_error: bool,
        checkpoint_enabled: bool
    ) -> ActionResult:
        """Execute independent stages in parallel."""
        max_workers = min(len(stages), 10)
        results_lock = threading.Lock()
        
        def execute_stage_wrapper(idx: int, stage: Dict[str, Any]) -> Dict[str, Any]:
            stage_name = stage.get('name', f'stage_{idx}')
            stage_type = stage.get('type', 'transform')
            stage_config = stage.get('config', {})
            
            try:
                result = self._execute_stage(
                    stage_type, stage_config, None, pipeline_context
                )
                return {
                    'index': idx,
                    'name': stage_name,
                    'success': result.success,
                    'message': result.message,
                    'data': result.data,
                    'error': None
                }
            except Exception as e:
                return {
                    'index': idx,
                    'name': stage_name,
                    'success': False,
                    'error': str(e)
                }
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(execute_stage_wrapper, idx, stage): idx
                    for idx, stage in enumerate(stages)
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    with results_lock:
                        pipeline_context['stage_results'].append(result)
            
            pipeline_context['stage_results'].sort(key=lambda x: x['index'])
            
            successful = sum(1 for r in pipeline_context['stage_results'] if r.get('success', False))
            elapsed = time.time() - pipeline_context['start_time']
            
            return ActionResult(
                success=successful == len(stages),
                message=f"Parallel pipeline: {successful}/{len(stages)} stages succeeded in {elapsed:.2f}s",
                data={
                    'stages': pipeline_context['stage_results'],
                    'final_data': pipeline_context['data'],
                    'elapsed': elapsed
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Parallel pipeline execution failed: {e}"
            )
    
    def _execute_stage(
        self,
        stage_type: str,
        stage_config: Dict[str, Any],
        stage_input: Any,
        pipeline_context: Dict[str, Any]
    ) -> ActionResult:
        """Execute a single pipeline stage based on type."""
        input_data = stage_input if stage_input is not None else pipeline_context['data']
        
        if stage_type == 'transform':
            return self._stage_transform(stage_config, input_data)
        elif stage_type == 'filter':
            return self._stage_filter(stage_config, input_data)
        elif stage_type == 'aggregate':
            return self._stage_aggregate(stage_config, input_data)
        elif stage_type == 'map':
            return self._stage_map(stage_config, input_data)
        elif stage_type == 'reduce':
            return self._stage_reduce(stage_config, input_data)
        elif stage_type == 'custom':
            return self._stage_custom(stage_config, input_data)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown stage type: {stage_type}"
            )
    
    def _stage_transform(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Transform stage: apply transformations to data."""
        if data is None:
            return ActionResult(success=True, message="No data to transform", data=None)
        
        transform_type = config.get('transform_type', 'passthrough')
        
        if transform_type == 'passthrough':
            return ActionResult(success=True, message="Data passed through", data=data)
        elif transform_type == 'uppercase':
            if isinstance(data, str):
                return ActionResult(success=True, message="Converted to uppercase", data=data.upper())
            return ActionResult(success=False, message="Data is not a string")
        elif transform_type == 'lowercase':
            if isinstance(data, str):
                return ActionResult(success=True, message="Converted to lowercase", data=data.lower())
            return ActionResult(success=False, message="Data is not a string")
        elif transform_type == 'json_parse':
            if isinstance(data, str):
                try:
                    parsed = json.loads(data)
                    return ActionResult(success=True, message="JSON parsed", data=parsed)
                except json.JSONDecodeError as e:
                    return ActionResult(success=False, message=f"JSON parse error: {e}")
            return ActionResult(success=False, message="Data is not a string")
        elif transform_type == 'json_dump':
            try:
                dumped = json.dumps(data)
                return ActionResult(success=True, message="JSON dumped", data=dumped)
            except (TypeError, ValueError) as e:
                return ActionResult(success=False, message=f"JSON dump error: {e}")
        else:
            return ActionResult(success=False, message=f"Unknown transform type: {transform_type}")
    
    def _stage_filter(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Filter stage: filter data based on conditions."""
        if not isinstance(data, (list, dict)):
            return ActionResult(success=False, message="Filter requires list or dict data")
        
        field = config.get('field')
        operator = config.get('operator', 'eq')
        value = config.get('value')
        
        if isinstance(data, dict):
            if field and operator == 'eq':
                filtered = {k: v for k, v in data.items() if v == value}
                return ActionResult(success=True, message=f"Filtered dict", data=filtered)
            return ActionResult(success=True, message="Dict passed through", data=data)
        else:
            if field and operator == 'eq':
                filtered = [item for item in data if isinstance(item, dict) and item.get(field) == value]
                return ActionResult(success=True, message=f"Filtered list: {len(filtered)} items", data=filtered)
            return ActionResult(success=True, message="List passed through", data=data)
    
    def _stage_aggregate(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Aggregate stage: aggregate data values."""
        if not isinstance(data, (list, dict)):
            return ActionResult(success=False, message="Aggregate requires list or dict data")
        
        agg_type = config.get('agg_type', 'count')
        field = config.get('field')
        
        if isinstance(data, list):
            if agg_type == 'count':
                return ActionResult(success=True, message=f"Count: {len(data)}", data=len(data))
            elif agg_type == 'sum' and field:
                total = sum(item.get(field, 0) for item in data if isinstance(item, dict))
                return ActionResult(success=True, message=f"Sum of {field}: {total}", data=total)
            elif agg_type == 'avg' and field:
                values = [item.get(field, 0) for item in data if isinstance(item, dict)]
                avg = sum(values) / len(values) if values else 0
                return ActionResult(success=True, message=f"Avg of {field}: {avg}", data=avg)
            elif agg_type == 'min' and field:
                values = [item.get(field) for item in data if isinstance(item, dict) and field in item]
                return ActionResult(success=True, message=f"Min of {field}: {min(values)}", data=min(values) if values else None)
            elif agg_type == 'max' and field:
                values = [item.get(field) for item in data if isinstance(item, dict) and field in item]
                return ActionResult(success=True, message=f"Max of {field}: {max(values)}", data=max(values) if values else None)
            return ActionResult(success=True, message="List passed through", data=data)
        
        return ActionResult(success=True, message="Data passed through", data=data)
    
    def _stage_map(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Map stage: apply function to each element."""
        if not isinstance(data, list):
            return ActionResult(success=False, message="Map requires list data")
        
        map_field = config.get('field')
        transform = config.get('transform', 'passthrough')
        
        if not map_field:
            return ActionResult(success=True, message="No field to map, passing through", data=data)
        
        mapped = []
        for item in data:
            if isinstance(item, dict) and map_field in item:
                value = item[map_field]
                if transform == 'uppercase' and isinstance(value, str):
                    item[map_field] = value.upper()
                elif transform == 'lowercase' and isinstance(value, str):
                    item[map_field] = value.lower()
                elif transform == 'str' and not isinstance(value, str):
                    item[map_field] = str(value)
                elif transform == 'int' and isinstance(value, str):
                    try:
                        item[map_field] = int(value)
                    except ValueError:
                        pass
                elif transform == 'float' and isinstance(value, str):
                    try:
                        item[map_field] = float(value)
                    except ValueError:
                        pass
            mapped.append(item)
        
        return ActionResult(success=True, message=f"Mapped {len(mapped)} items", data=mapped)
    
    def _stage_reduce(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Reduce stage: reduce list to single value."""
        if not isinstance(data, list):
            return ActionResult(success=False, message="Reduce requires list data")
        
        reduce_field = config.get('field')
        
        if not reduce_field:
            return ActionResult(success=False, message="reduce_field is required")
        
        values = []
        for item in data:
            if isinstance(item, dict) and reduce_field in item:
                values.append(item[reduce_field])
        
        if not values:
            return ActionResult(success=True, message="No values found", data=None)
        
        return ActionResult(success=True, message=f"Reduced to {len(values)} values", data=values)
    
    def _stage_custom(
        self,
        config: Dict[str, Any],
        data: Any
    ) -> ActionResult:
        """Custom stage: execute custom logic."""
        custom_type = config.get('custom_type', 'passthrough')
        
        if custom_type == 'passthrough':
            return ActionResult(success=True, message="Custom: pass through", data=data)
        else:
            return ActionResult(success=False, message=f"Unknown custom type: {custom_type}")
