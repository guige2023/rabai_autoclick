"""Data Export action module for RabAI AutoClick.

Provides data export operations:
- ExportCSVAction: Export to CSV
- ExportJSONAction: Export to JSON
- ExportExcelAction: Export to Excel
- ExportSQLAction: Export to SQL
"""

from __future__ import annotations

import sys
import os
import json
import csv
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExportCSVAction(BaseAction):
    """Export data to CSV."""
    action_type = "export_csv"
    display_name = "导出CSV"
    description = "导出数据到CSV"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV export."""
        data = params.get('data', [])
        file_path = params.get('file_path', '/tmp/export.csv')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'export_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if not resolved_data:
                return ActionResult(success=False, message="data is empty")

            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=resolved_data[0].keys(), delimiter=delimiter)
                writer.writeheader()
                writer.writerows(resolved_data)

            result = {
                'file_path': file_path,
                'record_count': len(resolved_data),
                'format': 'csv',
                'size_bytes': os.path.getsize(file_path),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported {len(resolved_data)} records to {file_path}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV export error: {e}")


class ExportJSONAction(BaseAction):
    """Export data to JSON."""
    action_type = "export_json"
    display_name = "导出JSON"
    description = "导出数据到JSON"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON export."""
        data = params.get('data', {})
        file_path = params.get('file_path', '/tmp/export.json')
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'export_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(resolved_data, f, indent=indent, ensure_ascii=False)

            result = {
                'file_path': file_path,
                'format': 'json',
                'size_bytes': os.path.getsize(file_path),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported JSON to {file_path} ({os.path.getsize(file_path)} bytes)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON export error: {e}")


class ExportExcelAction(BaseAction):
    """Export data to Excel."""
    action_type = "export_excel"
    display_name = "导出Excel"
    description = "导出数据到Excel"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Excel export."""
        data = params.get('data', [])
        file_path = params.get('file_path', '/tmp/export.xlsx')
        sheet_name = params.get('sheet_name', 'Sheet1')
        output_var = params.get('output_var', 'export_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            import pandas as pd

            resolved_data = context.resolve_value(data) if context else data

            df = pd.DataFrame(resolved_data)
            df.to_excel(file_path, sheet_name=sheet_name, index=False)

            result = {
                'file_path': file_path,
                'record_count': len(resolved_data),
                'format': 'excel',
                'size_bytes': os.path.getsize(file_path),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported {len(resolved_data)} records to Excel: {file_path}"
            )
        except ImportError:
            return ActionResult(success=False, message="pandas/openpyxl not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel export error: {e}")


class ExportSQLAction(BaseAction):
    """Export data to SQL."""
    action_type = "export_sql"
    display_name = "导出SQL"
    description = "导出数据到SQL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL export."""
        data = params.get('data', [])
        table_name = params.get('table_name', 'export_table')
        file_path = params.get('file_path', '/tmp/export.sql')
        output_var = params.get('output_var', 'export_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if not resolved_data:
                return ActionResult(success=False, message="data is empty")

            columns = list(resolved_data[0].keys())
            sql_lines = []

            for record in resolved_data:
                values = []
                for col in columns:
                    val = record.get(col, '')
                    if val is None:
                        values.append('NULL')
                    elif isinstance(val, str):
                        values.append(f"'{val.replace('\'', '\'\'')}'")
                    else:
                        values.append(str(val))
                sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                sql_lines.append(sql)

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sql_lines))

            result = {
                'file_path': file_path,
                'record_count': len(resolved_data),
                'table_name': table_name,
                'format': 'sql',
                'size_bytes': os.path.getsize(file_path),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported {len(resolved_data)} records to SQL: {file_path}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SQL export error: {e}")
