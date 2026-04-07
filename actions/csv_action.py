"""CSV action module for RabAI AutoClick.

Provides CSV processing operations:
- CsvReadAction: Read CSV file
- CsvWriteAction: Write CSV file
- CsvAppendAction: Append row to CSV
- CsvFilterAction: Filter CSV rows
- CsvSortAction: Sort CSV
- CsvJoinAction: Join two CSV files
- CsvStatsAction: Get CSV statistics
"""

import csv
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvReadAction(BaseAction):
    """Read CSV file."""
    action_type = "csv_read"
    display_name = "读取CSV"
    description = "读取CSV文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read.

        Args:
            context: Execution context.
            params: Dict with file_path, delimiter, has_header, output_var.

        Returns:
            ActionResult with CSV data.
        """
        file_path = params.get('file_path', '')
        delimiter = params.get('delimiter', ',')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_delim = context.resolve_value(delimiter)
            resolved_header = context.resolve_value(has_header)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            with open(resolved_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f, delimiter=resolved_delim)
                rows = list(reader)

            if resolved_header and rows:
                header = rows[0]
                data = rows[1:]
                result = {'header': header, 'rows': data, 'has_header': True}
            else:
                result = {'header': None, 'rows': rows, 'has_header': False}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"CSV已读取: {len(rows)} 行",
                data={'rows': len(rows), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'has_header': True, 'output_var': 'csv_data'}


class CsvWriteAction(BaseAction):
    """Write CSV file."""
    action_type = "csv_write"
    display_name = "写入CSV"
    description = "将数据写入CSV文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, header, delimiter.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        header = params.get('header', [])
        delimiter = params.get('delimiter', ',')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(data, list, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)
            resolved_header = context.resolve_value(header) if header else []
            resolved_delim = context.resolve_value(delimiter)

            with open(resolved_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delim)

                if resolved_header:
                    writer.writerow(resolved_header)

                for row in resolved_data:
                    if isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    elif isinstance(row, dict):
                        writer.writerow([row.get(h, '') for h in resolved_header]) if resolved_header else writer.writerow(row.values())
                    else:
                        writer.writerow([row])

            return ActionResult(
                success=True,
                message=f"CSV已写入: {resolved_path}",
                data={'path': resolved_path, 'rows': len(resolved_data)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'header': [], 'delimiter': ','}


class CsvAppendAction(BaseAction):
    """Append row to CSV."""
    action_type = "csv_append"
    display_name = "追加CSV行"
    description = "向CSV文件追加行"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append.

        Args:
            context: Execution context.
            params: Dict with file_path, row, delimiter.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        row = params.get('row', [])
        delimiter = params.get('delimiter', ',')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(row, list, 'row')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_row = context.resolve_value(row)
            resolved_delim = context.resolve_value(delimiter)

            file_exists = os.path.exists(resolved_path)

            with open(resolved_path, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delim)
                writer.writerow(resolved_row)

            return ActionResult(
                success=True,
                message=f"已追加行到: {resolved_path}",
                data={'path': resolved_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'row']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ','}


class CsvFilterAction(BaseAction):
    """Filter CSV rows."""
    action_type = "csv_filter"
    display_name = "筛选CSV"
    description = "筛选CSV行"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with file_path, column, operator, value, has_header, output_var.

        Returns:
            ActionResult with filtered rows.
        """
        file_path = params.get('file_path', '')
        column = params.get('column', 0)
        operator = params.get('operator', 'equals')
        value = params.get('value', '')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_filtered')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_col = context.resolve_value(column)
            resolved_op = context.resolve_value(operator)
            resolved_val = context.resolve_value(value)
            resolved_header = context.resolve_value(has_header)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            with open(resolved_path, 'r', encoding='utf-8-sig') as f:
                reader = list(csv.reader(f))

            header = reader[0] if resolved_header else []
            data_rows = reader[1:] if resolved_header else reader

            col_idx = resolved_col
            if isinstance(resolved_col, str) and header:
                for i, h in enumerate(header):
                    if str(h).lower() == resolved_col.lower():
                        col_idx = i
                        break

            filtered = []
            for row in data_rows:
                if col_idx < len(row):
                    cell_val = row[col_idx]
                    if self._match(cell_val, resolved_op, resolved_val):
                        filtered.append(row)

            context.set(output_var, {'header': header, 'rows': filtered, 'has_header': resolved_header})

            return ActionResult(
                success=True,
                message=f"筛选结果: {len(filtered)} 行",
                data={'count': len(filtered), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV筛选失败: {str(e)}"
            )

    def _match(self, cell_val: str, op: str, target: str) -> bool:
        if op == 'equals':
            return cell_val == target
        elif op == 'contains':
            return target in cell_val
        elif op == 'starts_with':
            return cell_val.startswith(target)
        elif op == 'ends_with':
            return cell_val.endswith(target)
        elif op == 'greater':
            try:
                return float(cell_val) > float(target)
            except ValueError:
                return cell_val > target
        elif op == 'less':
            try:
                return float(cell_val) < float(target)
            except ValueError:
                return cell_val < target
        elif op == 'not_equals':
            return cell_val != target
        return False

    def get_required_params(self) -> List[str]:
        return ['file_path', 'column', 'operator', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'has_header': True, 'output_var': 'csv_filtered'}


class CsvSortAction(BaseAction):
    """Sort CSV."""
    action_type = "csv_sort"
    display_name = "排序CSV"
    description = "对CSV排序"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with file_path, column, order, has_header, output_var.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        column = params.get('column', 0)
        order = params.get('order', 'asc')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_sorted')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_col = context.resolve_value(column)
            resolved_order = context.resolve_value(order)
            resolved_header = context.resolve_value(has_header)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            with open(resolved_path, 'r', encoding='utf-8-sig') as f:
                reader = list(csv.reader(f))

            header = reader[0] if resolved_header else []
            data_rows = reader[1:] if resolved_header else reader

            col_idx = resolved_col
            reverse = resolved_order == 'desc'

            sorted_rows = sorted(
                data_rows,
                key=lambda r: self._sort_key(r[col_idx] if col_idx < len(r) else ''),
                reverse=reverse
            )

            if resolved_header:
                sorted_rows = [header] + sorted_rows

            context.set(output_var, sorted_rows)

            return ActionResult(
                success=True,
                message=f"CSV已排序: {len(sorted_rows)} 行",
                data={'rows': len(sorted_rows), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV排序失败: {str(e)}"
            )

    def _sort_key(self, val: Any) -> Any:
        if val is None or val == '':
            return ''
        try:
            return float(val)
        except (ValueError, TypeError):
            return str(val).lower()

    def get_required_params(self) -> List[str]:
        return ['file_path', 'column']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'order': 'asc', 'has_header': True, 'output_var': 'csv_sorted'}


class CsvJoinAction(BaseAction):
    """Join two CSV files."""
    action_type = "csv_join"
    display_name = "合并CSV"
    description = "合并两个CSV文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join.

        Args:
            context: Execution context.
            params: Dict with file1, file2, key1, key2, output_file, join_type.

        Returns:
            ActionResult indicating success.
        """
        file1 = params.get('file1', '')
        file2 = params.get('file2', '')
        key1 = params.get('key1', 0)
        key2 = params.get('key2', 0)
        output_file = params.get('output_file', '/tmp/joined.csv')
        join_type = params.get('join_type', 'inner')

        valid, msg = self.validate_type(file1, str, 'file1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(file2, str, 'file2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_file1 = context.resolve_value(file1)
            resolved_file2 = context.resolve_value(file2)
            resolved_key1 = context.resolve_value(key1)
            resolved_key2 = context.resolve_value(key2)
            resolved_output = context.resolve_value(output_file)
            resolved_type = context.resolve_value(join_type)

            if not os.path.exists(resolved_file1) or not os.path.exists(resolved_file2):
                return ActionResult(
                    success=False,
                    message="CSV文件不存在"
                )

            with open(resolved_file1, 'r', encoding='utf-8-sig') as f:
                csv1 = list(csv.reader(f))
            with open(resolved_file2, 'r', encoding='utf-8-sig') as f:
                csv2 = list(csv.reader(f))

            header1 = csv1[0]
            header2 = csv2[0]
            data1 = csv1[1:]
            data2 = csv2[1:]

            index2 = {row[resolved_key2]: row for row in data2 if resolved_key2 < len(row)}

            result = []
            for row1 in data1:
                key_val = row1[resolved_key1] if resolved_key1 < len(row1) else ''
                if key_val in index2:
                    joined = row1 + index2[key_val]
                    result.append(joined)
                elif resolved_type == 'left':
                    result.append(row1 + [''] * len(header2))

            if resolved_type == 'outer':
                matched_keys = {row[resolved_key1] for row in data1 if resolved_key1 < len(row1)}
                for row2 in data2:
                    if resolved_key2 < len(row2) and row2[resolved_key2] not in matched_keys:
                        result.append([''] * len(header1) + row2)

            output = [header1 + header2] + result

            with open(resolved_output, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(output)

            return ActionResult(
                success=True,
                message=f"CSV已合并: {len(result)} 行",
                data={'path': resolved_output, 'rows': len(result)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file1', 'file2', 'key1', 'key2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_file': '/tmp/joined.csv', 'join_type': 'inner'}
