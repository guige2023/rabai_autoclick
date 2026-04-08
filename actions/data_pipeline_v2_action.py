"""Data pipeline v2 action module for RabAI AutoClick.

Provides enhanced data pipeline operations with stage management,
error handling, and monitoring.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineBuilderAction(BaseAction):
    """Build and execute multi-stage data pipelines.
    
    Chains multiple processing stages with error handling,
    stage-level monitoring, and optional rollback.
    """
    action_type = "pipeline_builder"
    display_name = "数据流水线构建器"
    description = "构建多阶段数据处理流水线"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: stages (list of stage configs),
                   data, stop_on_failure, rollback_on_failure.
        
        Returns:
            ActionResult with pipeline execution results.
        """
        stages = params.get('stages', [])
        initial_data = params.get('data')
        stop_on_failure = params.get('stop_on_failure', True)
        rollback_on_failure = params.get('rollback_on_failure', False)
        start_time = time.time()

        if not stages:
            return ActionResult(success=False, message="No pipeline stages defined")

        pipeline_data = initial_data
        stage_results = []
        rollback_stack = []

        for i, stage in enumerate(stages):
            stage_name = stage.get('name', f'stage_{i}')
            stage_action = stage.get('action', '')
            stage_params = stage.get('params', {})
            stage_rollback = stage.get('rollback')
            timeout = stage.get('timeout', 60)

            stage_start = time.time()
            try:
                if stage_params.get('_input_data') is None:
                    stage_params['_input_data'] = pipeline_data

                result = self._execute_action(stage_action, stage_params, timeout)
                stage_duration = time.time() - stage_start

                stage_results.append({
                    'stage': i,
                    'name': stage_name,
                    'action': stage_action,
                    'success': result.success,
                    'message': result.message,
                    'data': result.data,
                    'duration': stage_duration
                })

                if not result.success:
                    if rollback_on_failure:
                        self._execute_rollback(rollback_stack, context)
                    if stop_on_failure:
                        return ActionResult(
                            success=False,
                            message=f"Pipeline failed at stage '{stage_name}': {result.message}",
                            data={
                                'stage_results': stage_results,
                                'failed_stage': i,
                                'pipeline_data': pipeline_data
                            },
                            duration=time.time() - start_time
                        )
                    else:
                        pipeline_data = result.data.get('output') if result.data else None
                else:
                    if stage_rollback:
                        rollback_stack.append({'action': stage_rollback, 'data': pipeline_data})
                    pipeline_data = result.data.get('output') if result.data else result.data

            except Exception as e:
                stage_results.append({
                    'stage': i,
                    'name': stage_name,
                    'action': stage_action,
                    'success': False,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                })
                if rollback_on_failure:
                    self._execute_rollback(rollback_stack, context)
                if stop_on_failure:
                    return ActionResult(
                        success=False,
                        message=f"Pipeline failed at stage '{stage_name}': {str(e)}",
                        data={
                            'stage_results': stage_results,
                            'failed_stage': i
                        },
                        duration=time.time() - start_time
                    )

        all_success = all(r.get('success', False) for r in stage_results)
        return ActionResult(
            success=all_success,
            message=f"Pipeline completed: {sum(r.get('success', False) for r in stage_results)}/{len(stage_results)} stages succeeded",
            data={
                'stage_results': stage_results,
                'pipeline_data': pipeline_data,
                'total_stages': len(stage_results)
            },
            duration=time.time() - start_time
        )

    def _execute_action(
        self,
        action_name: str,
        params: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute a named action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                return action.execute(None, params)
        except ImportError:
            pass
        return ActionResult(success=False, message=f"Action '{action_name}' not found")

    def _execute_rollback(self, rollback_stack: List[Dict], context: Any) -> None:
        """Execute rollback actions."""
        for item in reversed(rollback_stack):
            try:
                rollback = item.get('action')
                if rollback:
                    self._execute_action(rollback, {'data': item.get('data')}, 30)
            except:
                pass


class PipelineMonitorAction(BaseAction):
    """Monitor pipeline execution progress.
    
    Tracks stage progress, timing, and throughput
    metrics for pipeline execution.
    """
    action_type = "pipeline_monitor"
    display_name = "流水线监控"
    description = "监控流水线执行进度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Monitor or report pipeline status.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (start|report|update|stop),
                   pipeline_id, stage_progress, metrics.
        
        Returns:
            ActionResult with monitoring data.
        """
        operation = params.get('operation', 'report')
        pipeline_id = params.get('pipeline_id', 'default')
        stage_progress = params.get('stage_progress', [])
        metrics = params.get('metrics', {})
        start_time = time.time()

        if not hasattr(context, '_pipeline_monitors'):
            context._pipeline_monitors = {}

        monitor = context._pipeline_monitors.get(pipeline_id, {
            'stages': [],
            'start_time': time.time(),
            'metrics': {}
        })

        if operation == 'start':
            monitor = {
                'stages': [],
                'start_time': time.time(),
                'metrics': {},
                'status': 'running'
            }
            context._pipeline_monitors[pipeline_id] = monitor
            return ActionResult(
                success=True,
                message=f"Pipeline monitor started: {pipeline_id}",
                data={'pipeline_id': pipeline_id, 'status': 'running'}
            )

        elif operation == 'update':
            stage_progress = params.get('stage_progress', [])
            monitor['stages'] = stage_progress
            monitor['metrics'].update(metrics)
            context._pipeline_monitors[pipeline_id] = monitor
            return ActionResult(
                success=True,
                message="Pipeline status updated",
                data={
                    'pipeline_id': pipeline_id,
                    'stage_count': len(stage_progress),
                    'metrics': monitor['metrics']
                }
            )

        elif operation == 'stop':
            monitor['status'] = 'completed'
            monitor['end_time'] = time.time()
            monitor['total_duration'] = monitor['end_time'] - monitor['start_time']

        elapsed = time.time() - monitor.get('start_time', time.time())
        completed_stages = sum(1 for s in monitor.get('stages', []) if s.get('status') == 'completed')

        return ActionResult(
            success=True,
            message=f"Pipeline '{pipeline_id}' report",
            data={
                'pipeline_id': pipeline_id,
                'status': monitor.get('status', 'unknown'),
                'elapsed_seconds': elapsed,
                'completed_stages': completed_stages,
                'total_stages': len(monitor.get('stages', [])),
                'metrics': monitor.get('metrics', {})
            },
            duration=time.time() - start_time
        )


class BranchingPipelineAction(BaseAction):
    """Execute branching pipeline with conditional paths.
    
    Routes data through different pipeline branches
    based on conditions and merges results.
    """
    action_type = "branching_pipeline"
    display_name = "分支流水线"
    description = "带条件分支的流水线"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute branching pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, branches (list of
                   {condition, stages}), merge_strategy (concat|union|first).
        
        Returns:
            ActionResult with branching results.
        """
        data = params.get('data', {})
        branches = params.get('branches', [])
        merge_strategy = params.get('merge_strategy', 'concat')
        start_time = time.time()

        if not branches:
            return ActionResult(success=False, message="No branches defined")

        branch_results = {}

        for i, branch in enumerate(branches):
            branch_name = branch.get('name', f'branch_{i}')
            condition = branch.get('condition', True)
            stages = branch.get('stages', [])

            condition_result = self._evaluate_condition(condition, data)
            if not condition_result:
                branch_results[branch_name] = {'skipped': True, 'reason': 'condition_not_met'}
                continue

            pipeline_builder = PipelineBuilderAction()
            result = pipeline_builder.execute(context, {
                'stages': stages,
                'data': data,
                'stop_on_failure': True
            })

            branch_results[branch_name] = {
                'skipped': False,
                'success': result.success,
                'data': result.data.get('pipeline_data') if result.data else None
            }

        merged = self._merge_results(branch_results, merge_strategy)

        return ActionResult(
            success=all(r.get('success', False) for r in branch_results.values() if not r.get('skipped')),
            message=f"Branching pipeline: {len(branch_results)} branches executed",
            data={
                'branch_results': branch_results,
                'merged_data': merged,
                'branch_count': len(branch_results)
            },
            duration=time.time() - start_time
        )

    def _evaluate_condition(self, condition: Any, data: Any) -> bool:
        """Evaluate branch condition."""
        if isinstance(condition, bool):
            return condition
        if callable(condition):
            return condition(data)
        return True

    def _merge_results(self, branch_results: Dict, strategy: str) -> Any:
        """Merge branch results based on strategy."""
        active = {k: v for k, v in branch_results.items() if not v.get('skipped')}

        if strategy == 'first':
            for v in active.values():
                if v.get('success') and v.get('data') is not None:
                    return v['data']
            return None

        if strategy == 'concat':
            results = []
            for v in active.values():
                if v.get('data') is not None:
                    data = v['data']
                    if isinstance(data, list):
                        results.extend(data)
                    else:
                        results.append(data)
            return results

        if strategy == 'union':
            union = {}
            for v in active.values():
                if isinstance(v.get('data'), dict):
                    union.update(v['data'])
            return union

        return None
