"""CSV action module for RabAI AutoClick.

Provides CSV file operations including reading, writing, filtering,
sorting, merging, and format conversion.
"""

import csv
import io
import sys
import os
from typing import Any, Dict, List, Optional, Union, TextIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CsvReadAction(BaseAction):
    """Read CSV file or string content.
    
    Parses CSV data from files or strings with customizable
    delimiter, quoting, and header handling.
    """
    action_type = "csv_read"
    display_name = "读取CSV"
    description = "读取CSV文件或字符串内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read CSV data.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - source: File path or CSV string content (required)
                - source_type: 'file' or 'string' (default auto-detect)
                - delimiter: Field delimiter (default ',')
                - quotechar: Quote character (default '"')
                - has_header: Whether CSV has header row (default True)
                - encoding: File encoding (default utf-8)
                - skip_rows: Number of rows to skip at start (default 0)
                - max_rows: Maximum rows to read (0 = all, default 0)
        
        Returns:
            ActionResult with parsed CSV data and headers.
        """
        source = params.get('source', '')
        if not source:
            return ActionResult(success=False, message="source is required")
        
        source_type = params.get('source_type', 'auto')
        delimiter = params.get('delimiter', ',')
        quotechar = params.get('quotechar', '"')
        has_header = params.get('has_header', True)
        encoding = params.get('encoding', 'utf-8')
        skip_rows = params.get('skip_rows', 0)
        max_rows = params.get('max_rows', 0)
        
        # Auto-detect source type
        if source_type == 'auto':
            source_type = 'file' if os.path.exists(source) else 'string'
        
        try:
            if source_type == 'file':
                with open(source, 'r', encoding=encoding, newline='') as f:
                    content = f.read()
            else:
                content = source
            
            # Apply skip_rows
            if skip_rows > 0:
                lines = content.split('\n')
                content = '\n'.join(lines[skip_rows:])
            
            # Parse CSV
            reader = csv.reader(io.StringIO(content), delimiter=delimiter, quotechar=quotechar)
            rows = list(reader)
            
            if len(rows) == 0:
                return ActionResult(success=True, message="Empty CSV", data={'headers': [], 'rows': [], 'count': 0})
            
            # Apply max_rows
            if max_rows > 0:
                rows = rows[:max_rows]
            
            # Split header and data
            if has_header and len(rows) > 0:
                headers = rows[0]
                data_rows = rows[1:]
            else:
                headers = [f"col_{i}" for i in range(len(rows[0]))]
                data_rows = rows
            
            # Build dict rows if has header
            dict_rows = []
            if has_header:
                for row in data_rows:
                    dict_rows.append({headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))})
            
            return ActionResult(
                success=True,
                message=f"Read {len(data_rows)} rows, {len(headers)} columns",
                data={
                    'headers': headers,
                    'rows': data_rows,
                    'dict_rows': dict_rows,
                    'count': len(data_rows)
                }
            )
            
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {source}")
        except UnicodeDecodeError as e:
            return ActionResult(success=False, message=f"Encoding error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"CSV read error: {e}")


class CsvWriteAction(BaseAction):
    """Write data to CSV file or string.
    
    Exports data to CSV format with customizable delimiter,
    quoting, and line endings.
    """
    action_type = "csv_write"
    display_name = "写入CSV"
    description = "将数据写入CSV文件或字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Write CSV data.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - data: List of dicts or list of lists (required)
                - output: File path or 'return' for string (default 'return')
                - headers: List of column names (auto from dict keys if None)
                - delimiter: Field delimiter (default ',')
                - quotechar: Quote character (default '"')
                - quoting: 'minimal', 'all', 'nonnumeric' (default 'minimal')
                - include_header: Include header row (default True)
                - encoding: File encoding (default utf-8)
                - lineterminator: Line terminator (default \r\n)
        
        Returns:
            ActionResult with output path or CSV string.
        """
        data = params.get('data', [])
        if not data:
            return ActionResult(success=False, message="data is required")
        
        output = params.get('output', 'return')
        headers = params.get('headers', None)
        delimiter = params.get('delimiter', ',')
        quotechar = params.get('quotechar', '"')
        quoting = params.get('quoting', 'minimal')
        include_header = params.get('include_header', True)
        encoding = params.get('encoding', 'utf-8')
        lineterminator = params.get('lineterminator', '\r\n')
        
        quoting_map = {
            'minimal': csv.QUOTE_MINIMAL,
            'all': csv.QUOTE_ALL,
            'nonnumeric': csv.QUOTE_NONNUMERIC
        }
        csv_quoting = quoting_map.get(quoting, csv.QUOTE_MINIMAL)
        
        try:
            # Extract headers from first dict if not provided
            if headers is None:
                if isinstance(data[0], dict):
                    headers = list(data[0].keys())
                else:
                    headers = [f"col_{i}" for i in range(len(data[0]))]
            
            # Convert all rows to lists
            rows: List[List[Any]] = []
            if include_header:
                rows.append(headers)
            
            for item in data:
                if isinstance(item, dict):
                    rows.append([item.get(h, '') for h in headers])
                elif isinstance(item, (list, tuple)):
                    rows.append(list(item))
                else:
                    rows.append([item])
            
            output_buffer = io.StringIO(newline='')
            writer = csv.writer(
                output_buffer,
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=csv_quoting,
                lineterminator=lineterminator
            )
            writer.writerows(rows)
            result = output_buffer.getvalue()
            
            if output == 'return':
                return ActionResult(
                    success=True,
                    message=f"Generated CSV with {len(data)} rows",
                    data={'csv': result, 'row_count': len(data)}
                )
            else:
                with open(output, 'w', encoding=encoding, newline='') as f:
                    f.write(result)
                return ActionResult(
                    success=True,
                    message=f"Wrote {len(data)} rows to {output}",
                    data={'path': output, 'row_count': len(data)}
                )
                
        except Exception as e:
            return ActionResult(success=False, message=f"CSV write error: {e}")


class CsvMergeAction(BaseAction):
    """Merge multiple CSV files or data sources.
    
    Combines CSV files with matching or different headers,
    supports deduplication and sorting.
    """
    action_type = "csv_merge"
    display_name = "合并CSV"
    description = "合并多个CSV文件或数据源"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge CSV files.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - sources: List of file paths or CSV strings (required)
                - mode: 'concat' (all columns) or 'inner' (common columns) (default 'concat')
                - sort_by: Column name to sort by (optional)
                - sort_reverse: Sort descending (default False)
                - deduplicate: Remove duplicate rows (default False)
                - delimiter: Field delimiter (default ',')
                - headers: Headers for string sources (optional)
        
        Returns:
            ActionResult with merged CSV data.
        """
        sources = params.get('sources', [])
        if not sources or len(sources) < 2:
            return ActionResult(success=False, message="sources must have at least 2 items")
        
        mode = params.get('mode', 'concat')
        sort_by = params.get('sort_by', None)
        sort_reverse = params.get('sort_reverse', False)
        deduplicate = params.get('deduplicate', False)
        delimiter = params.get('delimiter', ',')
        
        all_rows: List[Dict[str, str]] = []
        all_headers: set = set()
        
        for source in sources:
            try:
                if os.path.exists(source):
                    with open(source, 'r', encoding='utf-8', newline='') as f:
                        reader = csv.DictReader(f, delimiter=delimiter)
                        rows = list(reader)
                        if rows:
                            all_headers.update(rows[0].keys())
                            all_rows.extend(rows)
                else:
                    reader = csv.DictReader(io.StringIO(source), delimiter=delimiter)
                    rows = list(reader)
                    if rows:
                        all_headers.update(rows[0].keys())
                        all_rows.extend(rows)
            except Exception as e:
                return ActionResult(success=False, message=f"Error reading source: {e}")
        
        if len(all_rows) == 0:
            return ActionResult(success=True, message="No data to merge", data={'rows': [], 'count': 0})
        
        # Determine columns based on mode
        if mode == 'inner':
            common_headers = None
            for row in all_rows:
                if common_headers is None:
                    common_headers = set(row.keys())
                else:
                    common_headers &= set(row.keys())
            headers = sorted(common_headers) if common_headers else []
        else:
            headers = sorted(all_headers)
        
        # Filter rows to selected columns
        filtered_rows = [{h: row.get(h, '') for h in headers} for row in all_rows]
        
        # Deduplicate
        if deduplicate:
            seen = set()
            unique_rows = []
            for row in filtered_rows:
                key = tuple(sorted(row.items()))
                if key not in seen:
                    seen.add(key)
                    unique_rows.append(row)
            filtered_rows = unique_rows
        
        # Sort
        if sort_by and sort_by in headers:
            filtered_rows.sort(key=lambda x: x.get(sort_by, ''), reverse=sort_reverse)
        
        return ActionResult(
            success=True,
            message=f"Merged {len(all_rows)} rows, {len(headers)} columns",
            data={
                'headers': headers,
                'rows': filtered_rows,
                'count': len(filtered_rows)
            }
        )


class CsvFilterAction(BaseAction):
    """Filter CSV rows based on column conditions.
    
    Supports equality, contains, greater/less than, and regex
    matching on specific columns.
    """
    action_type = "csv_filter"
    display_name = "筛选CSV"
    description = "根据条件筛选CSV行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter CSV rows.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - data: List of dicts from csv_read output (required)
                - column: Column name to filter on (required)
                - operator: 'eq', 'ne', 'gt', 'lt', 'ge', 'le', 'contains', 'startswith', 'regex' (default 'eq')
                - value: Value to compare against (required for non-regex ops)
                - case_sensitive: Case sensitive string matching (default False)
        
        Returns:
            ActionResult with filtered rows.
        """
        data = params.get('data', [])
        column = params.get('column', '')
        operator = params.get('operator', 'eq')
        value = params.get('value', '')
        case_sensitive = params.get('case_sensitive', False)
        
        if not data:
            return ActionResult(success=True, message="Empty data", data={'rows': [], 'count': 0})
        
        if not column:
            return ActionResult(success=False, message="column is required")
        
        import re
        op_funcs = {
            'eq': lambda a, b: a == b,
            'ne': lambda a, b: a != b,
            'gt': lambda a, b: str(a) > str(b),
            'lt': lambda a, b: str(a) < str(b),
            'ge': lambda a, b: str(a) >= str(b),
            'le': lambda a, b: str(a) <= str(b),
        }
        
        def matches(row: Dict[str, Any]) -> bool:
            cell = str(row.get(column, ''))
            if not case_sensitive:
                cell = cell.lower()
                cmp_val = str(value).lower()
            else:
                cmp_val = str(value)
            
            if operator == 'contains':
                return cmp_val in cell
            elif operator == 'startswith':
                return cell.startswith(cmp_val)
            elif operator == 'regex':
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    return bool(re.search(value, cell, flags))
                except re.error:
                    return False
            elif operator in op_funcs:
                return op_funcs[operator](cell, cmp_val)
            return False
        
        filtered = [row for row in data if column in row and matches(row)]
        
        return ActionResult(
            success=True,
            message=f"Filtered {len(data)} rows to {len(filtered)} matches",
            data={'rows': filtered, 'count': len(filtered)}
        )
