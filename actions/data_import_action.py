"""Data Import action module for RabAI AutoClick.

Provides data import operations:
- ImportCSVAction: Import from CSV
- ImportJSONAction: Import from JSON
- ImportExcelAction: Import from Excel
- ImportSQLAction: Import from SQL
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


class ImportCSVAction(BaseAction):
    """Import data from CSV."""
    action_type = "import_csv"
    display_name = "导入CSV"
    description = "从CSV导入数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV import."""
        file_path = params.get('file_path', '')
        delimiter = params.get('delimiter', ',')
        has_header = params.get('has_header', True)
        limit = params.get('limit', 0)
        output_var = params.get('output_var', 'import_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"File not found: {resolved_path}")

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=delimiter) if has_header else csv.reader(f, delimiter=delimiter)
                if has_header:
                    data = list(reader)
                else:
                    headers = f.readline().strip().split(delimiter)
                    data = [{f'col{i}': v for i, v in enumerate(row)} for row in reader]

            if limit > 0:
                data = data[:limit]

            result = {
                'file_path': resolved_path,
                'record_count': len(data),
                'format': 'csv',
                'has_header': has_header,
                'columns': list(data[0].keys()) if data else [],
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Imported {len(data)} records from CSV"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV import error: {e}")


class ImportJSONAction(BaseAction):
    """Import data from JSON."""
    action_type = "import_json"
    display_name: "导入JSON"
    description = "从JSON导入数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON import."""
        file_path = params.get('file_path', '')
        path = params.get('path', '')
        output_var = params.get('output_var', 'import_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"File not found: {resolved_path}")

            with open(resolved_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if path:
                for key in path.split('.'):
                    if key.isdigit():
                        data = data[int(key)]
                    elif isinstance(data, dict) and key in data:
                        data = data[key]
                    else:
                        data = None
                        break

            if not isinstance(data, list):
                data = [data]

            result = {
                'file_path': resolved_path,
                'record_count': len(data),
                'format': 'json',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Imported {len(data)} records from JSON"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON import error: {e}")


class ImportExcelAction(BaseAction):
    """Import data from Excel."""
    action_type = "import_excel"
    display_name = "导入Excel"
    description = "从Excel导入数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Excel import."""
        file_path = params.get('file_path', '')
        sheet_name = params.get('sheet_name', 0)
        limit = params.get('limit', 0)
        output_var = params.get('output_var', 'import_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import pandas as pd

            resolved_path = context.resolve_value(file_path) if context else file_path

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"File not found: {resolved_path}")

            df = pd.read_excel(resolved_path, sheet_name=sheet_name)

            if limit > 0:
                df = df.head(limit)

            data = df.to_dict('records')

            result = {
                'file_path': resolved_path,
                'record_count': len(data),
                'format': 'excel',
                'sheet_name': sheet_name,
                'columns': list(df.columns),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Imported {len(data)} records from Excel"
            )
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel import error: {e}")


class ImportSQLAction(BaseAction):
    """Import data from SQL."""
    action_type = "import_sql"
    display_name = "导入SQL"
    description = "从SQL导入数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL import."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'import_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"File not found: {resolved_path}")

            with open(resolved_path, 'r', encoding='utf-8') as f:
                content = f.read()

            statements = [s.strip() for s in content.split(';') if s.strip() and s.strip().upper().startswith('INSERT')]

            result = {
                'file_path': resolved_path,
                'statement_count': len(statements),
                'format': 'sql',
                'statements': statements[:10],
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Imported {len(statements)} SQL INSERT statements"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SQL import error: {e}")
