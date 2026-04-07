"""Csv2 action module for RabAI AutoClick.

Provides additional CSV operations:
- CsvReadAction: Read CSV file
- CsvWriteAction: Write CSV file
- CsvAppendAction: Append to CSV file
- CsvToJsonAction: Convert CSV to JSON
- CsvFilterAction: Filter CSV rows
"""

import csv
import json
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvReadAction(BaseAction):
    """Read CSV file."""
    action_type = "csv2_read"
    display_name = "CSV读取"
    description = "读取CSV文件内容"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV read.

        Args:
            context: Execution context.
            params: Dict with file_path, delimiter, output_var.

        Returns:
            ActionResult with CSV data.
        """
        file_path = params.get('file_path', '')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'csv_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_delimiter = context.resolve_value(delimiter)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=resolved_delimiter)
                rows = list(reader)

            context.set(output_var, rows)

            return ActionResult(
                success=True,
                message=f"CSV读取完成: {len(rows)} 行",
                data={
                    'file_path': resolved_path,
                    'rows': rows,
                    'row_count': len(rows),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'csv_data'}


class CsvWriteAction(BaseAction):
    """Write CSV file."""
    action_type = "csv2_write"
    display_name = "CSV写入"
    description = "写入CSV文件内容"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, delimiter, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'write_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)
            resolved_delimiter = context.resolve_value(delimiter)

            with open(resolved_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delimiter)
                for row in resolved_data:
                    writer.writerow(row)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"CSV写入完成: {resolved_path}",
                data={
                    'file_path': resolved_path,
                    'row_count': len(resolved_data),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'write_status'}


class CsvAppendAction(BaseAction):
    """Append to CSV file."""
    action_type = "csv2_append"
    display_name = "CSV追加"
    description = "追加到CSV文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV append.

        Args:
            context: Execution context.
            params: Dict with file_path, row, delimiter, output_var.

        Returns:
            ActionResult with append status.
        """
        file_path = params.get('file_path', '')
        row = params.get('row', [])
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'append_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_row = context.resolve_value(row)
            resolved_delimiter = context.resolve_value(delimiter)

            import os
            file_exists = os.path.exists(resolved_path)

            with open(resolved_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delimiter)
                writer.writerow(resolved_row)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"CSV追加完成",
                data={
                    'file_path': resolved_path,
                    'row': resolved_row,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'row']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'append_status'}


class CsvToJsonAction(BaseAction):
    """Convert CSV to JSON."""
    action_type = "csv2_to_json"
    display_name = "CSV转JSON"
    description = "将CSV转换为JSON"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV to JSON.

        Args:
            context: Execution context.
            params: Dict with file_path, delimiter, output_var.

        Returns:
            ActionResult with JSON string.
        """
        file_path = params.get('file_path', '')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'json_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_delimiter = context.resolve_value(delimiter)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=resolved_delimiter)
                rows = list(reader)

            if len(rows) < 2:
                return ActionResult(
                    success=False,
                    message="CSV转JSON失败: 数据行不足"
                )

            headers = rows[0]
            data = []
            for row in rows[1:]:
                if len(row) == len(headers):
                    data.append(dict(zip(headers, row)))

            result = json.dumps(data, ensure_ascii=False, indent=2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"CSV转JSON完成: {len(data)} 条记录",
                data={
                    'file_path': resolved_path,
                    'result': result,
                    'record_count': len(data),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV转JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'json_data'}


class CsvFilterAction(BaseAction):
    """Filter CSV rows."""
    action_type = "csv2_filter"
    display_name = "CSV筛选"
    description = "根据条件筛选CSV行"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV filter.

        Args:
            context: Execution context.
            params: Dict with file_path, column, value, delimiter, output_var.

        Returns:
            ActionResult with filtered rows.
        """
        file_path = params.get('file_path', '')
        column = params.get('column', '')
        value = params.get('value', '')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'filtered_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_column = context.resolve_value(column)
            resolved_value = context.resolve_value(value)
            resolved_delimiter = context.resolve_value(delimiter)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=resolved_delimiter)
                rows = list(reader)

            if len(rows) < 2:
                return ActionResult(
                    success=False,
                    message="CSV筛选失败: 数据行不足"
                )

            headers = rows[0]
            if resolved_column not in headers:
                return ActionResult(
                    success=False,
                    message=f"CSV筛选失败: 列 '{resolved_column}' 不存在"
                )

            col_index = headers.index(resolved_column)
            filtered = [rows[0]]
            for row in rows[1:]:
                if len(row) > col_index and row[col_index] == resolved_value:
                    filtered.append(row)

            context.set(output_var, filtered)

            return ActionResult(
                success=True,
                message=f"CSV筛选完成: {len(filtered)-1} 条匹配",
                data={
                    'file_path': resolved_path,
                    'column': resolved_column,
                    'value': resolved_value,
                    'filtered_rows': filtered,
                    'match_count': len(filtered) - 1,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV筛选失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'column', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'filtered_data'}