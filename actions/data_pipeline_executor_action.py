"""Data Pipeline Executor action module for RabAI AutoClick.

Executes data processing pipelines with stages, branching,
and error handling.
"""

import time
import json
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPipelineExecutorAction(BaseAction):
    """Execute multi-stage data processing pipelines.

    Supports sequential stages, parallel branches, error
    handling per stage, and result streaming.
    """
    action_type = "data_pipeline_executor"
    display_name = "数据管道执行器"
    description = "执行多阶段数据处理管道"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a data pipeline.

        Args:
            context: Execution context.
            params: Dict with keys: stages (list), input_data,
                   stop_on_error, continue_on_error.

        Returns:
            ActionResult with pipeline execution results.
        """
        start_time = time.time()
        try:
            stages = params.get('stages', [])
            input_data = params.get('input_data', None)
            stop_on_error = params.get('stop_on_error', True)
            continue_on_error = params.get('continue_on_error', False)

            if not stages:
                return ActionResult(
                    success=False,
                    message="No pipeline stages defined",
                    duration=time.time() - start_time,
                )

            current_data = input_data
            stage_results = []
            errors = []

            for i, stage in enumerate(stages):
                stage_name = stage.get('name', f'stage_{i}')
                stage_type = stage.get('type', 'transform')
                stage_config = stage.get('config', {})
                stage_start = time.time()

                try:
                    stage_result = self._execute_stage(stage_type, stage_config, current_data, context)
                    stage_duration = time.time() - stage_start

                    if isinstance(stage_result, ActionResult):
                        if stage_result.success:
                            current_data = stage_result.data
                            stage_results.append({
                                'name': stage_name,
                                'type': stage_type,
                                'success': True,
                                'duration': stage_duration,
                                'output_size': self._estimate_size(current_data),
                            })
                        else:
                            stage_results.append({
                                'name': stage_name,
                                'type': stage_type,
                                'success': False,
                                'error': stage_result.message,
                                'duration': stage_duration,
                            })
                            errors.append(stage_result.message)
                            if stop_on_error:
                                break
                            elif not continue_on_error:
                                break
                    else:
                        current_data = stage_result
                        stage_results.append({
                            'name': stage_name,
                            'type': stage_type,
                            'success': True,
                            'duration': stage_duration,
                            'output_size': self._estimate_size(current_data),
                        })

                except Exception as e:
                    stage_duration = time.time() - stage_start
                    stage_results.append({
                        'name': stage_name,
                        'type': stage_type,
                        'success': False,
                        'error': str(e),
                        'duration': stage_duration,
                    })
                    errors.append(str(e))
                    if stop_on_error:
                        break

            duration = time.time() - start_time
            all_success = all(s.get('success', False) for s in stage_results)

            return ActionResult(
                success=all_success,
                message=f"Pipeline: {len(stage_results)} stages, {'OK' if all_success else 'errors occurred'}",
                data={
                    'stages': stage_results,
                    'output': current_data,
                    'errors': errors,
                    'total_duration': duration,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Pipeline execution failed: {str(e)}",
                duration=duration,
            )

    def _execute_stage(
        self,
        stage_type: str,
        config: Dict[str, Any],
        data: Any,
        context: Any
    ) -> ActionResult:
        """Execute a single pipeline stage."""
        if stage_type == 'transform':
            transform_fn = config.get('function')
            if callable(transform_fn):
                result = transform_fn(data, context)
                return ActionResult(success=True, data=result)
            return ActionResult(success=False, message="No transform function provided")

        elif stage_type == 'filter':
            predicate = config.get('predicate')
            if callable(predicate):
                if isinstance(data, list):
                    filtered = [item for item in data if predicate(item, context)]
                    return ActionResult(success=True, data=filtered)
                return ActionResult(success=True, data=data)
            return ActionResult(success=False, message="No predicate function provided")

        elif stage_type == 'map':
            mapper = config.get('mapper')
            if callable(mapper):
                if isinstance(data, list):
                    mapped = [mapper(item, context) for item in data]
                    return ActionResult(success=True, data=mapped)
                return ActionResult(success=True, data=mapper(data, context))
            return ActionResult(success=False, message="No mapper function provided")

        elif stage_type == 'reduce':
            reducer = config.get('reducer')
            initial = config.get('initial', None)
            if callable(reducer):
                if isinstance(data, list):
                    result = data[0] if data else initial
                    for item in data[1:]:
                        result = reducer(result, item, context)
                    return ActionResult(success=True, data=result)
                return ActionResult(success=True, data=data)
            return ActionResult(success=False, message="No reducer function provided")

        elif stage_type == 'validate':
            schema = config.get('schema', {})
            validators = config.get('validators', [])
            errors = []
            if isinstance(data, dict):
                for key, expected_type in schema.items():
                    if key not in data:
                        errors.append(f"Missing required field: {key}")
                    elif not isinstance(data[key], expected_type):
                        errors.append(f"Field {key} has wrong type")
                for validator in validators:
                    if callable(validator) and not validator(data, context):
                        errors.append(f"Validator failed: {validator.__name__}")
            return ActionResult(success=len(errors) == 0, message=', '.join(errors) if errors else 'OK', data=data)

        elif stage_type == 'aggregate':
            group_by = config.get('group_by')
            agg_fn = config.get('agg_fn', 'sum')
            if isinstance(data, list) and group_by:
                groups: Dict[Any, List] = {}
                for item in data:
                    key = item.get(group_by) if isinstance(item, dict) else getattr(item, group_by, None)
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(item)

                results = {}
                for key, items in groups.items():
                    if agg_fn == 'count':
                        results[key] = len(items)
                    elif agg_fn == 'sum' and items and isinstance(items[0], dict):
                        field = config.get('field', 'value')
                        results[key] = sum(item.get(field, 0) for item in items)
                    else:
                        results[key] = items
                return ActionResult(success=True, data=results)
            return ActionResult(success=True, data=data)

        else:
            return ActionResult(success=False, message=f"Unknown stage type: {stage_type}")

    def _estimate_size(self, data: Any) -> int:
        """Estimate data size in bytes."""
        try:
            if isinstance(data, (str, bytes)):
                return len(data)
            return len(json.dumps(data))
        except Exception:
            return 0


class DataPipelineBuilderAction(BaseAction):
    """Build data pipelines from declarative configuration.

    Parses pipeline definitions and creates optimized
    execution plans.
    """
    action_type = "data_pipeline_builder"
    display_name = "数据管道构建器"
    description = "从声明式配置构建数据管道"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build a pipeline from config.

        Args:
            context: Execution context.
            params: Dict with keys: pipeline_definition,
                   optimize (bool).

        Returns:
            ActionResult with compiled pipeline.
        """
        start_time = time.time()
        try:
            definition = params.get('pipeline_definition', {})
            optimize = params.get('optimize', True)

            stages = definition.get('stages', [])
            name = definition.get('name', 'Unnamed Pipeline')
            description = definition.get('description', '')

            # Validate stage definitions
            errors = []
            compiled_stages = []
            for i, stage in enumerate(stages):
                if 'type' not in stage:
                    errors.append(f"Stage {i} missing 'type' field")
                    continue
                compiled = self._compile_stage(stage)
                if compiled:
                    compiled_stages.append(compiled)
                else:
                    errors.append(f"Stage {i} failed to compile")

            # Optimize pipeline
            if optimize and len(compiled_stages) > 1:
                compiled_stages = self._optimize_stages(compiled_stages)

            pipeline = {
                'name': name,
                'description': description,
                'stages': compiled_stages,
                'total_stages': len(compiled_stages),
            }

            duration = time.time() - start_time
            return ActionResult(
                success=len(errors) == 0,
                message=f"Built pipeline '{name}' with {len(compiled_stages)} stages",
                data={'pipeline': pipeline, 'errors': errors},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Pipeline build failed: {str(e)}",
                duration=duration,
            )

    def _compile_stage(self, stage: Dict[str, Any]) -> Optional[Dict]:
        """Compile a stage definition."""
        stage_type = stage.get('type')
        valid_types = {'transform', 'filter', 'map', 'reduce', 'validate', 'aggregate', 'split', 'merge'}
        if stage_type not in valid_types:
            return None
        return {
            'type': stage_type,
            'name': stage.get('name', stage_type),
            'config': stage.get('config', {}),
            'optional': stage.get('optional', False),
        }

    def _optimize_stages(self, stages: List[Dict]) -> List[Dict]:
        """Optimize stage order for performance."""
        # Combine consecutive filters
        optimized = []
        for stage in stages:
            if stage['type'] == 'filter' and optimized and optimized[-1]['type'] == 'filter':
                # Merge filters
                prev_config = optimized[-1]['config']
                prev_config.setdefault('predicates', []).append(stage['config'].get('predicate'))
            else:
                optimized.append(stage)
        return optimized
