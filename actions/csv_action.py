"""CSV action module for RabAI AutoClick.

Provides CSV operations:
- CsvReadAction: Read CSV file
- CsvWriteAction: Write to CSV file
- CsvAppendAction: Append to CSV file
- CsvToListAction: Convert CSV to list of dicts
- ListToCsvAction: Convert list to CSV string
"""

import csv
import io
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvReadAction(BaseAction):
    """Read CSV file."""
    action_type = "csv_read"
    display_name = "读取CSV"
    description = "读取CSV文件内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV read.

        Args:
            context: Execution context.
            params: Dict with file_path, has_header, output_var.

        Returns:
            ActionResult with CSV data.
        """
        file_path = params.get('file_path', '')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)

            if has_header and rows:
                headers = rows[0]
                data = []
                for row in rows[1:]:
                    record = dict(zip(headers, row))
                    data.append(record)
            else:
                data = rows

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"CSV读取成功: {len(data)} 行",
                data={
                    'rows': len(data),
                    'data': data,
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
        return {'has_header': True, 'output_var': 'csv_data'}


class CsvWriteAction(BaseAction):
    """Write to CSV file."""
    action_type = "csv_write"
    display_name = "写入CSV"
    description = "将数据写入CSV文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, headers, output_var.

        Returns:
            ActionResult with write result.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'csv_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            if not resolved_data:
                return ActionResult(
                    success=False,
                    message="数据为空"
                )

            rows = []
            if headers:
                resolved_headers = context.resolve_value(headers)
                rows.append(resolved_headers)

            if isinstance(resolved_data, list):
                if resolved_data and isinstance(resolved_data[0], dict):
                    if not headers:
                        rows.append(list(resolved_data[0].keys()))
                    for item in resolved_data:
                        rows.append(list(item.values()))
                else:
                    for item in resolved_data:
                        if isinstance(item, (list, tuple)):
                            rows.append(item)
                        else:
                            rows.append([item])
            else:
                rows.append([resolved_data])

            with open(resolved_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            context.set(output_var, len(rows))

            return ActionResult(
                success=True,
                message=f"CSV写入成功: {len(rows)} 行",
                data={
                    'rows': len(rows),
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
        return {'headers': None, 'output_var': 'csv_result'}


class CsvAppendAction(BaseAction):
    """Append to CSV file."""
    action_type = "csv_append"
    display_name = "追加CSV"
    description = "追加数据到CSV文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV append.

        Args:
            context: Execution context.
            params: Dict with file_path, data, output_var.

        Returns:
            ActionResult with append result.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        output_var = params.get('output_var', 'csv_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            if not resolved_data:
                return ActionResult(
                    success=False,
                    message="数据为空"
                )

            rows = []
            if isinstance(resolved_data, list):
                for item in resolved_data:
                    if isinstance(item, (list, tuple)):
                        rows.append(item)
                    elif isinstance(item, dict):
                        rows.append(list(item.values()))
                    else:
                        rows.append([item])
            else:
                rows.append([resolved_data])

            with open(resolved_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            context.set(output_var, len(rows))

            return ActionResult(
                success=True,
                message=f"CSV追加成功: {len(rows)} 行",
                data={
                    'rows': len(rows),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'csv_result'}


class CsvToListAction(BaseAction):
    """Convert CSV string to list."""
    action_type = "csv_to_list"
    display_name = "CSV转列表"
    description = "将CSV字符串转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CSV to list.

        Args:
            context: Execution context.
            params: Dict with csv_string, has_header, output_var.

        Returns:
            ActionResult with list data.
        """
        csv_string = params.get('csv_string', '')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_list')

        valid, msg = self.validate_type(csv_string, str, 'csv_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(csv_string)

            reader = csv.reader(io.StringIO(resolved))
            rows = list(reader)

            if has_header and rows:
                headers = rows[0]
                data = []
                for row in rows[1:]:
                    record = dict(zip(headers, row))
                    data.append(record)
            else:
                data = rows

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"CSV转换成功: {len(data)} 行",
                data={
                    'rows': len(data),
                    'data': data,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['csv_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'has_header': True, 'output_var': 'csv_list'}


class ListToCsvAction(BaseAction):
    """Convert list to CSV string."""
    action_type = "list_to_csv"
    display_name = "列表转CSV"
    description = "将列表转换为CSV字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list to CSV.

        Args:
            context: Execution context.
            params: Dict with data, headers, output_var.

        Returns:
            ActionResult with CSV string.
        """
        data = params.get('data', [])
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'csv_string')

        try:
            resolved_data = context.resolve_value(data)

            if not resolved_data:
                return ActionResult(
                    success=False,
                    message="数据为空"
                )

            output = io.StringIO()
            writer = csv.writer(output)

            if headers:
                resolved_headers = context.resolve_value(headers)
                writer.writerow(resolved_headers)
            elif resolved_data and isinstance(resolved_data[0], dict):
                writer.writerow(list(resolved_data[0].keys()))

            if isinstance(resolved_data, list):
                for item in resolved_data:
                    if isinstance(item, dict):
                        writer.writerow(list(item.values()))
                    elif isinstance(item, (list, tuple)):
                        writer.writerow(item)
                    else:
                        writer.writerow([item])

            result = output.getvalue()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转换CSV成功: {len(result)} 字符",
                data={
                    'csv': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转换CSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': None, 'output_var': 'csv_string'}