"""Data Funnel Action Module for RabAI AutoClick.

Funnel processing that routes data through multiple
transformation stages with filtering at each level.
"""

import time
import sys
import os
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFunnelAction(BaseAction):
    """Multi-stage funnel data processing.

    Routes data through sequential funnel stages, each applying
    filters and transformations. Tracks conversion rates between
    stages and identifies where data is dropped.
    """
    action_type = "data_funnel"
    display_name = "数据漏斗处理"
    description = "多阶段漏斗数据处理，追踪转化率"

    _funnels: Dict[str, Dict[str, Any]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute funnel operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'add_stage', 'process', 'stats'
                - funnel_name: str - name of the funnel
                - stage: dict (optional) - stage definition
                - data: Any (optional) - data to process through funnel
                - stage_name: str (optional) - stage identifier

        Returns:
            ActionResult with funnel operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'process')

            if operation == 'create':
                return self._create_funnel(params, start_time)
            elif operation == 'add_stage':
                return self._add_stage(params, start_time)
            elif operation == 'process':
                return self._process_funnel(params, start_time)
            elif operation == 'stats':
                return self._get_funnel_stats(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Funnel action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_funnel(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new funnel."""
        funnel_name = params.get('funnel_name', 'default')

        self._funnels[funnel_name] = {
            'name': funnel_name,
            'stages': [],
            'total_input': 0,
            'created_at': time.time(),
            'processed_count': 0
        }

        return ActionResult(
            success=True,
            message=f"Funnel created: {funnel_name}",
            data={'funnel_name': funnel_name, 'stages': 0},
            duration=time.time() - start_time
        )

    def _add_stage(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a stage to a funnel."""
        funnel_name = params.get('funnel_name', 'default')
        stage = params.get('stage', {})
        stage_name = stage.get('name', f'stage_{time.time()}')

        if funnel_name not in self._funnels:
            self._create_funnel({'funnel_name': funnel_name}, start_time)

        funnel = self._funnels[funnel_name]
        stage_def = {
            'name': stage_name,
            'filter_type': stage.get('filter_type', 'pass_all'),
            'filter_value': stage.get('filter_value'),
            'transform': stage.get('transform'),
            'processed': 0,
            'passed': 0,
            'dropped': 0
        }

        funnel['stages'].append(stage_def)

        return ActionResult(
            success=True,
            message=f"Stage added: {stage_name}",
            data={
                'funnel_name': funnel_name,
                'stage_name': stage_name,
                'stage_count': len(funnel['stages'])
            },
            duration=time.time() - start_time
        )

    def _process_funnel(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Process data through funnel stages."""
        funnel_name = params.get('funnel_name', 'default')
        data = params.get('data')

        if funnel_name not in self._funnels:
            return ActionResult(
                success=False,
                message=f"Funnel not found: {funnel_name}",
                duration=time.time() - start_time
            )

        funnel = self._funnels[funnel_name]

        if not isinstance(data, list):
            data = [data]

        funnel['total_input'] += len(data)
        current_data = list(data)
        stage_results = []

        for i, stage in enumerate(funnel['stages']):
            passed = []
            stage_name = stage['name']

            for item in current_data:
                stage['processed'] += 1
                if self._apply_filter(stage, item):
                    passed.append(item)
                    stage['passed'] += 1
                else:
                    stage['dropped'] += 1

            stage_results.append({
                'stage': stage_name,
                'input': len(current_data),
                'output': len(passed),
                'dropped': len(current_data) - len(passed)
            })

            current_data = passed

        funnel['processed_count'] += 1

        return ActionResult(
            success=True,
            message=f"Processed through {len(funnel['stages'])} stages",
            data={
                'funnel_name': funnel_name,
                'input_count': len(data),
                'output_count': len(current_data),
                'total_dropped': len(data) - len(current_data),
                'stage_results': stage_results
            },
            duration=time.time() - start_time
        )

    def _get_funnel_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get funnel statistics."""
        funnel_name = params.get('funnel_name', 'default')

        if funnel_name not in self._funnels:
            return ActionResult(
                success=False,
                message=f"Funnel not found: {funnel_name}",
                duration=time.time() - start_time
            )

        funnel = self._funnels[funnel_name]
        total_processed = sum(s['processed'] for s in funnel['stages'])
        total_passed = sum(s['passed'] for s in funnel['stages'])
        total_dropped = sum(s['dropped'] for s in funnel['stages'])

        return ActionResult(
            success=True,
            message=f"Funnel stats: {funnel_name}",
            data={
                'funnel_name': funnel_name,
                'stage_count': len(funnel['stages']),
                'total_input': funnel['total_input'],
                'processed_count': funnel['processed_count'],
                'stage_stats': [
                    {
                        'name': s['name'],
                        'processed': s['processed'],
                        'passed': s['passed'],
                        'dropped': s['dropped']
                    }
                    for s in funnel['stages']
                ]
            },
            duration=time.time() - start_time
        )

    def _apply_filter(self, stage: Dict[str, Any], item: Any) -> bool:
        """Apply a stage filter to an item."""
        filter_type = stage.get('filter_type', 'pass_all')

        if filter_type == 'pass_all':
            return True
        elif filter_type == 'pass_none':
            return False
        elif filter_type == 'equals':
            return item == stage.get('filter_value')
        elif filter_type == 'contains':
            return stage.get('filter_value', '') in str(item)
        elif filter_type == 'greater_than':
            try:
                return float(item) > float(stage.get('filter_value', 0))
            except (ValueError, TypeError):
                return False
        elif filter_type == 'regex':
            import re
            try:
                return bool(re.search(stage.get('filter_value', ''), str(item)))
            except re.error:
                return False

        return True
