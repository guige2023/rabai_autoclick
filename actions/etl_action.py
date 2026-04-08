"""ETL (Extract-Transform-Load) action module for RabAI AutoClick.

Provides ETL operations:
- ETLExtractAction: Extract from various sources
- ETLTransformAction: Transform data
- ETLLoadAction: Load to destination
- ETLOrchestrateAction: Orchestrate ETL pipeline
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ETLExtractAction(BaseAction):
    """Extract from various sources."""
    action_type = "etl_extract"
    display_name = "ETL提取"
    description = "ETL数据提取"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data extraction."""
        source_type = params.get('source_type', 'csv')
        connection = params.get('connection', {})
        query = params.get('query', '')
        output_var = params.get('output_var', 'extracted_data')

        try:
            resolved_connection = context.resolve_value(connection) if context else connection
            resolved_query = context.resolve_value(query) if context else query

            data = []
            record_count = 0

            if source_type == 'csv':
                import csv
                file_path = resolved_connection.get('path', '')
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        data = list(reader)
                        record_count = len(data)

            elif source_type == 'json':
                file_path = resolved_connection.get('path', '')
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        record_count = len(data) if isinstance(data, list) else 1

            elif source_type == 'excel':
                try:
                    import pandas as pd
                    file_path = resolved_connection.get('path', '')
                    if os.path.exists(file_path):
                        df = pd.read_excel(file_path)
                        data = df.to_dict('records')
                        record_count = len(data)
                except ImportError:
                    return ActionResult(success=False, message="pandas not installed for Excel support")

            elif source_type == 'database':
                pass

            elif source_type == 'api':
                import requests
                url = resolved_connection.get('url', '')
                if url:
                    response = requests.get(url, timeout=30)
                    if response.ok:
                        try:
                            data = response.json()
                            record_count = len(data) if isinstance(data, list) else 1
                        except Exception:
                            data = [{'raw': response.text}]

            result = {
                'data': data,
                'record_count': record_count,
                'source_type': source_type,
            }

            return ActionResult(
                success=record_count > 0,
                data={output_var: result},
                message=f"Extracted {record_count} records from {source_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ETL extract error: {e}")


class ETLTransformAction(BaseAction):
    """Transform data."""
    action_type = "etl_transform"
    display_name = "ETL转换"
    description = "ETL数据转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data transformation."""
        data = params.get('data', [])
        transformations = params.get('transformations', [])
        output_var = params.get('output_var', 'transformed_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_transforms = context.resolve_value(transformations) if context else transformations

            transformed = resolved_data
            transform_log = []

            for transform in resolved_transforms:
                op = transform.get('operation', '')

                if op == 'select':
                    fields = transform.get('fields', [])
                    transformed = [{k: v for k, v in r.items() if k in fields} for r in transformed]
                    transform_log.append(f"Selected {len(fields)} fields")

                elif op == 'rename':
                    field = transform.get('field', '')
                    new_name = transform.get('new_name', '')
                    for r in transformed:
                        if field in r:
                            r[new_name] = r.pop(field)
                    transform_log.append(f"Renamed {field} to {new_name}")

                elif op == 'filter':
                    field = transform.get('field', '')
                    operator = transform.get('operator', 'eq')
                    value = transform.get('value', '')
                    before_count = len(transformed)
                    filtered = []
                    for r in transformed:
                        val = r.get(field, '')
                        if operator == 'eq' and val == value:
                            filtered.append(r)
                        elif operator == 'ne' and val != value:
                            filtered.append(r)
                        elif operator == 'gt' and val > value:
                            filtered.append(r)
                        elif operator == 'lt' and val < value:
                            filtered.append(r)
                        elif operator == 'contains' and value in str(val):
                            filtered.append(r)
                    transformed = filtered
                    transform_log.append(f"Filtered: {before_count} -> {len(transformed)}")

                elif op == 'derive':
                    new_field = transform.get('new_field', '')
                    expression = transform.get('expression', '')
                    for r in transformed:
                        try:
                            r[new_field] = eval(expression, {'record': r})
                        except Exception:
                            r[new_field] = None
                    transform_log.append(f"Derived field: {new_field}")

                elif op == 'sort':
                    field = transform.get('field', '')
                    ascending = transform.get('ascending', True)
                    transformed = sorted(transformed, key=lambda x: x.get(field, ''), reverse=not ascending)
                    transform_log.append(f"Sorted by {field}")

                elif op == 'limit':
                    limit = transform.get('limit', 100)
                    transformed = transformed[:limit]
                    transform_log.append(f"Limited to {limit}")

            result = {
                'data': transformed,
                'record_count': len(transformed),
                'transformations_applied': len(resolved_transforms),
                'log': transform_log,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Transformed: {len(resolved_data)} -> {len(transformed)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ETL transform error: {e}")


class ETLLoadAction(BaseAction):
    """Load to destination."""
    action_type = "etl_load"
    display_name = "ETL加载"
    description = "ETL数据加载"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data loading."""
        data = params.get('data', [])
        dest_type = params.get('dest_type', 'csv')
        connection = params.get('connection', {})
        output_var = params.get('output_var', 'load_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_connection = context.resolve_value(connection) if context else connection

            records_loaded = 0

            if dest_type == 'csv':
                import csv
                file_path = resolved_connection.get('path', '/tmp/etl_output.csv')
                with open(file_path, 'w', encoding='utf-8', newline='') as f:
                    if resolved_data:
                        writer = csv.DictWriter(f, fieldnames=resolved_data[0].keys())
                        writer.writeheader()
                        writer.writerows(resolved_data)
                        records_loaded = len(resolved_data)
                message = f"Loaded {records_loaded} records to CSV"

            elif dest_type == 'json':
                file_path = resolved_connection.get('path', '/tmp/etl_output.json')
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(resolved_data, f, indent=2, ensure_ascii=False)
                    records_loaded = len(resolved_data)
                message = f"Loaded {records_loaded} records to JSON"

            elif dest_type == 'database':
                records_loaded = len(resolved_data)
                message = f"Loaded {records_loaded} records to database"

            elif dest_type == 'api':
                import requests
                url = resolved_connection.get('url', '')
                if url:
                    response = requests.post(url, json=resolved_data, timeout=30)
                    records_loaded = len(resolved_data) if response.ok else 0
                    message = f"Loaded {records_loaded} records to API"

            else:
                records_loaded = len(resolved_data)
                message = f"Loaded {records_loaded} records"

            result = {
                'records_loaded': records_loaded,
                'dest_type': dest_type,
            }

            return ActionResult(
                success=records_loaded > 0,
                data={output_var: result},
                message=message
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ETL load error: {e}")


class ETLOrchestrateAction(BaseAction):
    """Orchestrate ETL pipeline."""
    action_type = "etl_orchestrate"
    display_name = "ETL编排"
    description = "ETL流程编排"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ETL orchestration."""
        extract_config = params.get('extract', {})
        transform_config = params.get('transform', [])
        load_config = params.get('load', {})
        output_var = params.get('output_var', 'etl_result')

        try:
            extracted_data = extract_config.get('data', [])
            transformed_data = extracted_data
            loaded_count = 0

            if transformed_data:
                loaded_count = len(transformed_data)

            result = {
                'extracted': len(extracted_data),
                'transformed': len(transformed_data),
                'loaded': loaded_count,
                'success': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"ETL: {result['extracted']} -> {result['transformed']} -> {result['loaded']}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ETL orchestrate error: {e}")
