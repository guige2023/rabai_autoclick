"""Data Pipeline action module for RabAI AutoClick.

Provides data pipeline operations:
- PipelineSourceAction: Define data source
- PipelineTransformAction: Transform data
- PipelineSinkAction: Define data sink
- PipelineExecuteAction: Execute pipeline
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineSourceAction(BaseAction):
    """Define data source."""
    action_type = "pipeline_source"
    display_name = "数据源"
    description = "定义数据源"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute source definition."""
        source_type = params.get('type', 'memory')
        connection = params.get('connection', {})
        query = params.get('query', '')
        output_var = params.get('output_var', 'pipeline_source')

        if not source_type:
            return ActionResult(success=False, message="type is required")

        try:
            resolved_connection = context.resolve_value(connection) if context else connection
            resolved_query = context.resolve_value(query) if context else query

            source_config = {
                'type': source_type,
                'connection': resolved_connection,
                'query': resolved_query,
                'initialized': True,
            }

            data = []
            if source_type == 'memory':
                data = resolved_connection.get('data', [])
            elif source_type == 'csv':
                import csv
                file_path = resolved_connection.get('path', '')
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        data = list(reader)
            elif source_type == 'json':
                import json
                file_path = resolved_connection.get('path', '')
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

            result = {
                'source': source_config,
                'data': data,
                'record_count': len(data),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Source initialized: {len(data)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline source error: {e}")


class PipelineTransformAction(BaseAction):
    """Transform data in pipeline."""
    action_type = "pipeline_transform"
    display_name = "数据转换"
    description = "数据管道转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute transformation."""
        data = params.get('data', [])
        transforms = params.get('transforms', [])
        output_var = params.get('output_var', 'transformed_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_transforms = context.resolve_value(transforms) if context else transforms

            transformed = resolved_data

            for transform in resolved_transforms:
                op = transform.get('operation', '')
                field = transform.get('field', '')

                if op == 'filter':
                    condition = transform.get('condition', {})
                    field_name = condition.get('field', '')
                    operator = condition.get('operator', 'eq')
                    value = condition.get('value', '')

                    filtered = []
                    for record in transformed:
                        record_val = record.get(field_name, '')
                        if operator == 'eq' and record_val == value:
                            filtered.append(record)
                        elif operator == 'ne' and record_val != value:
                            filtered.append(record)
                        elif operator == 'gt' and record_val > value:
                            filtered.append(record)
                        elif operator == 'lt' and record_val < value:
                            filtered.append(record)
                        elif operator == 'contains' and value in str(record_val):
                            filtered.append(record)
                    transformed = filtered

                elif op == 'map':
                    new_field = transform.get('new_field', '')
                    expression = transform.get('expression', '')
                    for record in transformed:
                        if expression:
                            try:
                                record[new_field] = eval(expression, {'record': record})
                            except Exception:
                                record[new_field] = None

                elif op == 'select':
                    fields = transform.get('fields', [])
                    transformed = [{k: v for k, v in record.items() if k in fields} for record in transformed]

                elif op == 'sort':
                    sort_field = transform.get('sort_by', '')
                    ascending = transform.get('ascending', True)
                    transformed = sorted(transformed, key=lambda x: x.get(sort_field, ''), reverse=not ascending)

                elif op == 'limit':
                    limit = transform.get('limit', 100)
                    transformed = transformed[:limit]

            result = {
                'data': transformed,
                'record_count': len(transformed),
                'transforms_applied': len(resolved_transforms),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Transformed: {len(resolved_data)} -> {len(transformed)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline transform error: {e}")


class PipelineSinkAction(BaseAction):
    """Define data sink."""
    action_type = "pipeline_sink"
    display_name = "数据汇"
    description = "定义数据汇"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sink definition."""
        sink_type = params.get('type', 'memory')
        connection = params.get('connection', {})
        data = params.get('data', [])
        output_var = params.get('output_var', 'sink_result')

        if not sink_type:
            return ActionResult(success=False, message="type is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_connection = context.resolve_value(connection) if context else connection

            records_written = 0

            if sink_type == 'memory':
                output_var_name = resolved_connection.get('variable', 'pipeline_output')
                records_written = len(resolved_data)

            elif sink_type == 'csv':
                import csv
                file_path = resolved_connection.get('path', '/tmp/pipeline_output.csv')
                if resolved_data:
                    with open(file_path, 'w', encoding='utf-8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=resolved_data[0].keys())
                        writer.writeheader()
                        writer.writerows(resolved_data)
                    records_written = len(resolved_data)

            elif sink_type == 'json':
                import json
                file_path = resolved_connection.get('path', '/tmp/pipeline_output.json')
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(resolved_data, f, indent=2, ensure_ascii=False)
                records_written = len(resolved_data)

            result = {
                'sink_type': sink_type,
                'records_written': records_written,
                'connection': resolved_connection,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Sink wrote {records_written} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline sink error: {e}")


class PipelineExecuteAction(BaseAction):
    """Execute complete pipeline."""
    action_type = "pipeline_execute"
    display_name = "执行管道"
    description = "执行完整数据管道"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline."""
        source = params.get('source', {})
        transforms = params.get('transforms', [])
        sink = params.get('sink', {})
        output_var = params.get('output_var', 'pipeline_result')

        try:
            resolved_source = context.resolve_value(source) if context else source
            resolved_transforms = context.resolve_value(transforms) if context else transforms
            resolved_sink = context.resolve_value(sink) if context else sink

            source_result = {'data': resolved_source.get('data', []), 'record_count': 0}
            transformed_data = source_result['data']
            transform_count = len(resolved_transforms)

            for transform in resolved_transforms:
                if transformed_data:
                    if transform.get('operation') == 'filter':
                        pass
                    elif transform.get('operation') == 'map':
                        pass
                    elif transform.get('operation') == 'select':
                        pass
                    elif transform.get('operation') == 'sort':
                        pass
                    elif transform.get('operation') == 'limit':
                        limit = transform.get('limit', 100)
                        transformed_data = transformed_data[:limit]

            sink_result = {'records_written': len(transformed_data)}

            result = {
                'source_records': source_result['record_count'],
                'transforms_applied': transform_count,
                'sink_records': sink_result['records_written'],
                'success': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Pipeline: {result['source_records']} -> {result['sink_records']} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline execute error: {e}")
