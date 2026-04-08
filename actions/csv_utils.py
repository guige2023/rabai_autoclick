"""CSV utilities action module for RabAI AutoClick.

Provides CSV file operations including reading, writing,
filtering, and data transformation.
"""

import os
import sys
import csv
import json
from typing import Any, Dict, List, Optional, TextIO
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CSVReadAction(BaseAction):
    """Read CSV file into list of dicts.
    
    Supports custom delimiter, encoding, and header detection.
    """
    action_type = "csv_read"
    display_name = "读取CSV"
    description = "读取CSV文件为列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Read CSV file.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, delimiter, encoding,
                   has_header, save_to_var.
        
        Returns:
            ActionResult with CSV data.
        """
        path = params.get('path', '')
        delimiter = params.get('delimiter', ',')
        encoding = params.get('encoding', 'utf-8')
        has_header = params.get('has_header', True)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="CSV path is required")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"CSV file not found: {path}")

        try:
            with open(path, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.reader(f, delimiter=delimiter)
                rows = list(reader)

            if not rows:
                result_data = {'rows': [], 'count': 0}
                if save_to_var:
                    context.variables[save_to_var] = result_data
                return ActionResult(success=True, message="空CSV文件", data=result_data)

            if has_header and len(rows) > 1:
                headers = rows[0]
                data = []
                for row in rows[1:]:
                    if len(row) == len(headers):
                        data.append(dict(zip(headers, row)))
                    elif len(row) < len(headers):
                        d = dict(zip(headers[:len(row)], row))
                        data.append(d)
                    else:
                        d = dict(zip(headers, row[:len(headers)]))
                        data.append(d)
            else:
                data = rows

            result_data = {
                'rows': data,
                'count': len(data),
                'headers': rows[0] if has_header else None,
                'has_header': has_header
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"CSV读取成功: {len(data)} 行",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'delimiter': ',',
            'encoding': 'utf-8',
            'has_header': True,
            'save_to_var': None
        }


class CSVWriteAction(BaseAction):
    """Write data to CSV file.
    
    Supports list of dicts or list of lists.
    Auto-generates headers from dict keys.
    """
    action_type = "csv_write"
    display_name = "写入CSV"
    description = "写入数据到CSV文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Write CSV file.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, data, headers, delimiter,
                   encoding, save_to_var.
        
        Returns:
            ActionResult with write result.
        """
        path = params.get('path', '')
        data = params.get('data', [])
        headers = params.get('headers', None)
        delimiter = params.get('delimiter', ',')
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="CSV path is required")

        if not data:
            return ActionResult(success=False, message="Data is empty")

        try:
            os.makedirs(os.path.dirname(path) or '.', exist_ok=True)

            with open(path, 'w', encoding=encoding, newline='') as f:
                # Determine headers
                if headers:
                    header_list = headers if isinstance(headers, list) else list(headers)
                elif isinstance(data[0], dict):
                    header_list = list(data[0].keys())
                else:
                    header_list = None

                writer = csv.writer(f, delimiter=delimiter)

                # Write header
                if header_list:
                    writer.writerow(header_list)

                # Write data
                rows_written = 0
                for row in data:
                    if isinstance(row, dict):
                        writer.writerow([row.get(h, '') for h in header_list])
                    elif isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    else:
                        writer.writerow([row])
                    rows_written += 1

            result_data = {
                'written': True,
                'path': path,
                'rows_written': rows_written,
                'headers': header_list
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"CSV写入成功: {rows_written} 行 -> {path}",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'headers': None,
            'delimiter': ',',
            'encoding': 'utf-8',
            'save_to_var': None
        }


class CSVFilterAction(BaseAction):
    """Filter CSV rows by column values.
    
    Supports comparison operators and multiple filter conditions.
    """
    action_type = "csv_filter"
    display_name = "过滤CSV"
    description = "按条件过滤CSV行"

    OPERATORS = ['==', '!=', '>', '<', '>=', '<=', 'contains', 'startswith', 'endswith']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter CSV rows.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, column, operator,
                   value, save_to_var.
        
        Returns:
            ActionResult with filtered rows.
        """
        data = params.get('data', [])
        column = params.get('column', '')
        operator = params.get('operator', '==')
        value = params.get('value', '')
        save_to_var = params.get('save_to_var', None)

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Data must be list, got {type(data).__name__}"
            )

        if not column:
            return ActionResult(success=False, message="Column is required")

        if operator not in self.OPERATORS:
            return ActionResult(
                success=False,
                message=f"Invalid operator: {operator}"
            )

        filtered = []

        for row in data:
            if not isinstance(row, dict):
                continue

            row_value = row.get(column, '')
            match = False

            if operator == '==':
                match = str(row_value) == str(value)
            elif operator == '!=':
                match = str(row_value) != str(value)
            elif operator == '>':
                try:
                    match = float(row_value) > float(value)
                except (ValueError, TypeError):
                    match = str(row_value) > str(value)
            elif operator == '<':
                try:
                    match = float(row_value) < float(value)
                except (ValueError, TypeError):
                    match = str(row_value) < str(value)
            elif operator == '>=':
                try:
                    match = float(row_value) >= float(value)
                except (ValueError, TypeError):
                    match = str(row_value) >= str(value)
            elif operator == '<=':
                try:
                    match = float(row_value) <= float(value)
                except (ValueError, TypeError):
                    match = str(row_value) <= str(value)
            elif operator == 'contains':
                match = str(value) in str(row_value)
            elif operator == 'startswith':
                match = str(row_value).startswith(str(value))
            elif operator == 'endswith':
                match = str(row_value).endswith(str(value))

            if match:
                filtered.append(row)

        result_data = {
            'filtered': filtered,
            'count': len(filtered),
            'original_count': len(data)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"过滤完成: {len(data)} -> {len(filtered)} 行",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['data', 'column', 'operator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value': '',
            'save_to_var': None
        }
