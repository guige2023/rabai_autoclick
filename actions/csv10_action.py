"""CSV10 action module for RabAI AutoClick.

Provides additional CSV operations:
- CSVParseAction: Parse CSV string
- CSVToStringAction: Convert to CSV string
- CSVReadAction: Read CSV from file
- CSVWriteAction: Write CSV to file
- CSVGetRowAction: Get CSV row
- CSVGetColumnAction: Get CSV column
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CSVParseAction(BaseAction):
    """Parse CSV string."""
    action_type = "csv10_parse"
    display_name = "解析CSV"
    description = "解析CSV字符串"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV parse.

        Args:
            context: Execution context.
            params: Dict with csv_str, delimiter, output_var.

        Returns:
            ActionResult with parsed CSV.
        """
        csv_str = params.get('csv_str', '')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'parsed_csv')

        try:
            import csv
            from io import StringIO

            resolved = context.resolve_value(csv_str)
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else ','

            reader = csv.reader(StringIO(resolved), delimiter=resolved_delimiter)
            result = list(reader)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析CSV: {len(result)}行",
                data={
                    'rows': len(result),
                    'cols': len(result[0]) if result else 0,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析CSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['csv_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'parsed_csv'}


class CSVToStringAction(BaseAction):
    """Convert to CSV string."""
    action_type = "csv10_tostring"
    display_name = "转换为CSV"
    description = "将数据转换为CSV字符串"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV to string.

        Args:
            context: Execution context.
            params: Dict with data, headers, delimiter, output_var.

        Returns:
            ActionResult with CSV string.
        """
        data = params.get('data', [])
        headers = params.get('headers', None)
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'csv_string')

        try:
            import csv
            from io import StringIO

            resolved_data = context.resolve_value(data)
            resolved_headers = context.resolve_value(headers) if headers else None
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else ','

            if not isinstance(resolved_data, list):
                resolved_data = [resolved_data]

            output = StringIO()
            writer = csv.writer(output, delimiter=resolved_delimiter)

            if resolved_headers:
                writer.writerow(resolved_headers)

            for row in resolved_data:
                if isinstance(row, (list, tuple)):
                    writer.writerow(row)
                else:
                    writer.writerow([row])

            result = output.getvalue()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为CSV: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为CSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'delimiter': ',', 'output_var': 'csv_string'}


class CSVReadAction(BaseAction):
    """Read CSV from file."""
    action_type = "csv10_read"
    display_name = "读取CSV文件"
    description = "从文件读取CSV"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV read from file.

        Args:
            context: Execution context.
            params: Dict with path, delimiter, output_var.

        Returns:
            ActionResult with read CSV.
        """
        path = params.get('path', '')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'csv_data')

        try:
            import csv

            resolved_path = context.resolve_value(path)
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else ','

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=resolved_delimiter)
                result = list(reader)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"读取CSV: {len(result)}行",
                data={
                    'path': resolved_path,
                    'rows': len(result),
                    'cols': len(result[0]) if result else 0,
                    'result': result,
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"文件未找到: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取CSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'output_var': 'csv_data'}


class CSVWriteAction(BaseAction):
    """Write CSV to file."""
    action_type = "csv10_write"
    display_name = "写入CSV文件"
    description = "写入CSV到文件"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV write to file.

        Args:
            context: Execution context.
            params: Dict with path, data, headers, delimiter, output_var.

        Returns:
            ActionResult with write status.
        """
        path = params.get('path', '')
        data = params.get('data', [])
        headers = params.get('headers', None)
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'write_status')

        try:
            import csv

            resolved_path = context.resolve_value(path)
            resolved_data = context.resolve_value(data)
            resolved_headers = context.resolve_value(headers) if headers else None
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else ','

            if not isinstance(resolved_data, list):
                resolved_data = [resolved_data]

            with open(resolved_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delimiter)

                if resolved_headers:
                    writer.writerow(resolved_headers)

                for row in resolved_data:
                    if isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    else:
                        writer.writerow([row])

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"写入CSV: {resolved_path}",
                data={
                    'path': resolved_path,
                    'rows': len(resolved_data),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入CSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'delimiter': ',', 'output_var': 'write_status'}


class CSVGetRowAction(BaseAction):
    """Get CSV row."""
    action_type = "csv10_getrow"
    display_name = "获取CSV行"
    description = "获取CSV行"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV get row.

        Args:
            context: Execution context.
            params: Dict with data, index, output_var.

        Returns:
            ActionResult with row.
        """
        data = params.get('data', [])
        index = params.get('index', 0)
        output_var = params.get('output_var', 'csv_row')

        try:
            resolved_data = context.resolve_value(data)
            resolved_index = int(context.resolve_value(index)) if index else 0

            if not isinstance(resolved_data, list):
                resolved_data = [resolved_data]

            if resolved_index >= len(resolved_data):
                return ActionResult(
                    success=False,
                    message=f"索引超出范围: {resolved_index}"
                )

            result = resolved_data[resolved_index]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取CSV行: {resolved_index}",
                data={
                    'index': resolved_index,
                    'row': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CSV行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'csv_row'}


class CSVGetColumnAction(BaseAction):
    """Get CSV column."""
    action_type = "csv10_getcol"
    display_name = "获取CSV列"
    description = "获取CSV列"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV get column.

        Args:
            context: Execution context.
            params: Dict with data, index, output_var.

        Returns:
            ActionResult with column.
        """
        data = params.get('data', [])
        index = params.get('index', 0)
        output_var = params.get('output_var', 'csv_column')

        try:
            resolved_data = context.resolve_value(data)
            resolved_index = int(context.resolve_value(index)) if index else 0

            if not isinstance(resolved_data, list):
                resolved_data = [resolved_data]

            result = [row[resolved_index] if isinstance(row, (list, tuple)) and len(row) > resolved_index else None for row in resolved_data]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取CSV列: {len(result)}项",
                data={
                    'index': resolved_index,
                    'column': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CSV列失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'csv_column'}