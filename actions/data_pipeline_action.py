"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline construction and execution with
stage composition, error handling, and result streaming.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineStageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"
    BRANCH = "branch"


@dataclass
class PipelineStage:
    """A single stage in a data pipeline."""
    name: str
    stage_type: PipelineStageType
    func: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    on_error: str = "fail"  # fail, skip, continue
    enabled: bool = True


class DataPipelineAction(BaseAction):
    """Build and execute a multi-stage data pipeline.
    
    Each stage processes data and passes to the next.
    Supports source, transform, filter, aggregate, and sink stages.
    """
    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "构建和执行多阶段数据处理管道"

    STAGE_FUNCS = {
        'filter': '_stage_filter',
        'map': '_stage_map',
        'flatmap': '_stage_flatmap',
        'sort': '_stage_sort',
        'limit': '_stage_limit',
        'skip': '_stage_skip',
        'distinct': '_stage_distinct',
        'group': '_stage_group',
        'aggregate': '_stage_aggregate',
        'join': '_stage_join',
        'union': '_stage_union',
        'select': '_stage_select',
        'rename': '_stage_rename',
        'fillna': '_stage_fillna',
        'cast': '_stage_cast',
        'custom': '_stage_custom',
    }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a data pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - stages: list of PipelineStage dicts
                - data: list of dicts (input data, for first source)
                - data_source: str (variable name for input data)
                - save_to_var: str
                - save_intermediate: bool (save each stage result)
        
        Returns:
            ActionResult with pipeline execution result.
        """
        stages_data = params.get('stages', [])
        input_data = params.get('data', None)
        data_source = params.get('data_source', None)
        save_to_var = params.get('save_to_var', 'pipeline_result')
        save_intermediate = params.get('save_intermediate', False)

        # Parse stages
        stages = []
        for s in stages_data:
            stage_type_str = s.get('stage_type', 'transform')
            try:
                stype = PipelineStageType(stage_type_str)
            except ValueError:
                stype = PipelineStageType.TRANSFORM
            
            stages.append(PipelineStage(
                name=s.get('name', f'stage_{len(stages)}'),
                stage_type=stype,
                func=s.get('func'),
                params=s.get('params', {}),
                on_error=s.get('on_error', 'fail'),
                enabled=s.get('enabled', True),
            ))

        if not stages:
            return ActionResult(success=False, message="No stages defined")

        # Get input data
        if input_data is not None:
            data = input_data
        elif data_source and context:
            data = context.variables.get(data_source, [])
        else:
            data = []

        if not isinstance(data, list):
            data = [data]

        # Execute pipeline
        stage_results = {}
        current_data = data
        stage_times = {}

        for i, stage in enumerate(stages):
            if not stage.enabled:
                continue

            stage_name = stage.name
            start = time.time()

            try:
                current_data = self._execute_stage(stage, current_data, context)
                elapsed = int((time.time() - start) * 1000)
                stage_times[stage_name] = elapsed

                if save_intermediate:
                    stage_results[stage_name] = {
                        'data': current_data,
                        'elapsed_ms': elapsed,
                        'count': len(current_data) if isinstance(current_data, list) else 1,
                    }

                # If stage returns None or empty on error handling, stop
                if current_data is None:
                    return ActionResult(
                        success=False,
                        message=f"Stage '{stage_name}' returned None"
                    )

            except Exception as e:
                if stage.on_error == 'fail':
                    return ActionResult(
                        success=False,
                        data={'failed_stage': stage_name, 'error': str(e)},
                        message=f"Pipeline failed at '{stage_name}': {e}"
                    )
                elif stage.on_error == 'skip':
                    stage_results[stage_name] = {'skipped': True, 'error': str(e)}
                elif stage.on_error == 'continue':
                    stage_results[stage_name] = {'error': str(e), 'continued': True}

        result = {
            'stages_executed': len([s for s in stages if s.enabled]),
            'stage_times_ms': stage_times,
            'input_count': len(data),
            'output_count': len(current_data) if isinstance(current_data, list) else 1,
            'output': current_data,
        }
        
        if save_intermediate:
            result['intermediate'] = stage_results

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Pipeline: {len(data)} -> {result['output_count']} rows"
        )

    def _execute_stage(self, stage: PipelineStage, data: List, context: Any) -> Any:
        """Execute a single pipeline stage."""
        func_name = stage.func
        stage_params = stage.params

        if func_name == 'filter':
            return self._stage_filter(data, stage_params)
        elif func_name == 'map':
            return self._stage_map(data, stage_params, context)
        elif func_name == 'flatmap':
            return self._stage_flatmap(data, stage_params, context)
        elif func_name == 'sort':
            return self._stage_sort(data, stage_params)
        elif func_name == 'limit':
            return self._stage_limit(data, stage_params)
        elif func_name == 'skip':
            return self._stage_skip(data, stage_params)
        elif func_name == 'distinct':
            return self._stage_distinct(data, stage_params)
        elif func_name == 'group':
            return self._stage_group(data, stage_params)
        elif func_name == 'aggregate':
            return self._stage_aggregate(data, stage_params)
        elif func_name == 'select':
            return self._stage_select(data, stage_params)
        elif func_name == 'rename':
            return self._stage_rename(data, stage_params)
        elif func_name == 'fillna':
            return self._stage_fillna(data, stage_params)
        elif func_name == 'cast':
            return self._stage_cast(data, stage_params)
        elif func_name == 'custom':
            return self._stage_custom(data, stage_params, context)
        else:
            return data

    def _stage_filter(self, data: List, params: Dict) -> List:
        """Filter rows by condition."""
        field = params.get('field', '')
        op = params.get('operator', '==')
        value = params.get('value')

        if not field:
            return data

        result = []
        for row in data:
            val = row.get(field)
            if self._compare(val, op, value):
                result.append(row)
        return result

    def _compare(self, val: Any, op: str, cmp_val: Any) -> bool:
        """Compare values with operator."""
        try:
            if op == '==':
                return str(val) == str(cmp_val)
            elif op == '!=':
                return str(val) != str(cmp_val)
            elif op == '>':
                return float(val) > float(cmp_val)
            elif op == '<':
                return float(val) < float(cmp_val)
            elif op == '>=':
                return float(val) >= float(cmp_val)
            elif op == '<=':
                return float(val) <= float(cmp_val)
            elif op == 'in':
                return val in cmp_val if isinstance(cmp_val, list) else str(val) in str(cmp_val)
            elif op == 'not in':
                return val not in cmp_val if isinstance(cmp_val, list) else str(val) not in str(cmp_val)
            elif op == 'contains':
                return str(cmp_val) in str(val)
            elif op == 'startswith':
                return str(val).startswith(str(cmp_val))
            elif op == 'endswith':
                return str(val).endswith(str(cmp_val))
        except (ValueError, TypeError):
            pass
        return False

    def _stage_map(self, data: List, params: Dict, context: Any) -> List:
        """Transform each row with an expression."""
        output_field = params.get('output_field', '')
        expression = params.get('expression', '')
        default = params.get('default', None)

        if not output_field:
            return data

        result = []
        for row in data:
            new_row = dict(row)
            val = self._eval_expression(expression, row, context)
            new_row[output_field] = val if val is not None else default
            result.append(new_row)
        return result

    def _stage_flatmap(self, data: List, params: Dict, context: Any) -> List:
        """Map each row to multiple output rows."""
        output_field = params.get('output_field', '')
        expression = params.get('expression', '')

        result = []
        for row in data:
            val = self._eval_expression(expression, row, context)
            if isinstance(val, list):
                for item in val:
                    new_row = dict(row)
                    new_row[output_field] = item
                    result.append(new_row)
            else:
                new_row = dict(row)
                new_row[output_field] = val
                result.append(new_row)
        return result

    def _stage_sort(self, data: List, params: Dict) -> List:
        """Sort data by fields."""
        by = params.get('by', [])
        ascending = params.get('ascending', True)

        if isinstance(by, str):
            by = [by]

        if not by:
            return data

        return sorted(data, key=lambda x: tuple(x.get(k) for k in by), reverse=not ascending)

    def _stage_limit(self, data: List, params: Dict) -> List:
        """Limit to n rows."""
        n = params.get('n', 10)
        return data[:n]

    def _stage_skip(self, data: List, params: Dict) -> List:
        """Skip n rows."""
        n = params.get('n', 0)
        return data[n:]

    def _stage_distinct(self, data: List, params: Dict) -> List:
        """Remove duplicate rows."""
        subset = params.get('subset', None)
        seen = set()
        result = []
        for row in data:
            if subset:
                key = tuple(str(row.get(k, '')) for k in subset)
            else:
                key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                result.append(row)
        return result

    def _stage_group(self, data: List, params: Dict) -> List:
        """Group and aggregate."""
        by = params.get('by', [])
        agg = params.get('aggregate', {})
        
        from collections import defaultdict
        groups = defaultdict(list)
        
        for row in data:
            key = tuple(str(row.get(k, '')) for k in by)
            groups[key].append(row)
        
        result = []
        for key, rows in groups.items():
            out = {k: v for k, v in zip(by, key)}
            for out_field, (src, func) in agg.items():
                vals = [r.get(src) for r in rows if r.get(src) is not None]
                out[out_field] = self._compute_agg(vals, func)
            result.append(out)
        return result

    def _compute_agg(self, values: List, func: str) -> Any:
        """Compute aggregation."""
        if not values:
            return None
        try:
            nums = [float(v) for v in values]
        except:
            nums = values

        if func == 'sum':
            return sum(nums) if nums else None
        elif func == 'avg':
            return sum(nums) / len(nums) if nums else None
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(nums) if nums else None
        elif func == 'max':
            return max(nums) if nums else None
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        return values[0] if values else None

    def _stage_select(self, data: List, params: Dict) -> List:
        """Select specific columns."""
        columns = params.get('columns', [])
        if not columns:
            return data
        return [{k: row.get(k) for k in columns} for row in data]

    def _stage_rename(self, data: List, params: Dict) -> List:
        """Rename columns."""
        mapping = params.get('mapping', {})
        result = []
        for row in data:
            new_row = {}
            for k, v in row.items():
                new_key = mapping.get(k, k)
                new_row[new_key] = v
            result.append(new_row)
        return result

    def _stage_fillna(self, data: List, params: Dict) -> List:
        """Fill missing values."""
        value = params.get('value', '')
        fields = params.get('fields', None)
        result = []
        for row in data:
            new_row = dict(row)
            if fields:
                for f in fields:
                    if new_row.get(f) is None or new_row.get(f) == '':
                        new_row[f] = value
            else:
                for k in new_row:
                    if new_row[k] is None or new_row[k] == '':
                        new_row[k] = value
            result.append(new_row)
        return result

    def _stage_cast(self, data: List, params: Dict) -> List:
        """Cast column types."""
        mapping = params.get('mapping', {})
        result = []
        for row in data:
            new_row = dict(row)
            for col, dtype in mapping.items():
                val = new_row.get(col)
                if val is None:
                    continue
                try:
                    if dtype == 'int':
                        new_row[col] = int(float(val))
                    elif dtype == 'float':
                        new_row[col] = float(val)
                    elif dtype == 'str':
                        new_row[col] = str(val)
                    elif dtype == 'bool':
                        new_row[col] = bool(val)
                except (ValueError, TypeError):
                    pass
            result.append(new_row)
        return result

    def _stage_aggregate(self, data: List, params: Dict) -> List:
        """Aggregate entire dataset."""
        agg = params.get('aggregate', {})
        result = {}
        for field, func in agg.items():
            values = [row.get(field) for row in data if row.get(field) is not None]
            result[field] = self._compute_agg(values, func)
        return [result]

    def _stage_custom(self, data: List, params: Dict, context: Any) -> List:
        """Custom transformation via expression."""
        output_field = params.get('output_field', 'result')
        expression = params.get('expression', '')
        
        result = []
        for row in data:
            val = self._eval_expression(expression, row, context)
            new_row = dict(row)
            new_row[output_field] = val
            result.append(new_row)
        return result

    def _eval_expression(self, expr: str, row: Dict, context: Any) -> Any:
        """Evaluate an expression against a row."""
        # Simple expression evaluation
        # Supports: field references ($field), arithmetic, string ops
        import math
        
        if not expr:
            return None

        # Replace field references
        expr_copy = expr
        for field, value in row.items():
            expr_copy = expr_copy.replace(f'${field}', repr(value))

        try:
            # Only allow safe operations
            allowed = {'math': math, '__builtins__': {}}
            return eval(expr_copy, allowed, {})
        except:
            return expr_copy
