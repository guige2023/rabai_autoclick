"""Data CSV action module for RabAI AutoClick.

Provides CSV parsing, transformation, and generation capabilities.
Handles various delimiters, quoting rules, and dialect configurations.
"""

import csv
import io
import json
from typing import Any, Dict, List, Optional, Union, TextIO

from core.base_action import BaseAction, ActionResult


class CsvParserAction(BaseAction):
    """Parse CSV data into structured records.
    
    Supports custom delimiters, quote characters, and escape handling.
    Automatically detects CSV dialects and handles malformed input.
    """
    action_type = "csv_parser"
    display_name = "CSV解析"
    description = "将CSV数据解析为结构化记录"
    VALID_DIALECTS = ["excel", "unix", "excel-tab", "custom"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse CSV data into records.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, delimiter, quotechar, escapechar,
                   has_header, dialect, skip_rows.
        
        Returns:
            ActionResult with parsed records and metadata.
        """
        data = params.get("data", "")
        delimiter = params.get("delimiter", ",")
        quotechar = params.get("quotechar", '"')
        escapechar = params.get("escapechar")
        has_header = params.get("has_header", True)
        dialect_name = params.get("dialect", "custom")
        skip_rows = params.get("skip_rows", 0)
        
        if not data:
            return ActionResult(success=False, message="CSV data is required")
        
        if isinstance(data, list):
            data = "\n".join(str(row) for row in data)
        
        try:
            for _ in range(skip_rows):
                lines = data.split("\n")
                data = "\n".join(lines[1:])
            
            if dialect_name != "custom":
                reader = csv.reader(io.StringIO(data), dialect=dialect_name)
            else:
                reader = csv.reader(
                    io.StringIO(data),
                    delimiter=delimiter,
                    quotechar=quotechar,
                    escapechar=escapechar
                )
            
            rows = list(reader)
            
            if not rows:
                return ActionResult(success=False, message="No data found in CSV")
            
            if has_header and rows:
                headers = [str(h).strip() for h in rows[0]]
                records = []
                for row in rows[1:]:
                    record = {}
                    for i, header in enumerate(headers):
                        if i < len(row):
                            record[header] = row[i].strip()
                    records.append(record)
                
                return ActionResult(
                    success=True,
                    message=f"Parsed {len(records)} records with headers",
                    data={
                        "headers": headers,
                        "records": records,
                        "row_count": len(records),
                        "column_count": len(headers)
                    }
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Parsed {len(rows)} rows without headers",
                    data={
                        "rows": rows,
                        "row_count": len(rows),
                        "column_count": len(rows[0]) if rows else 0
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV parsing failed: {e}")


class CsvGeneratorAction(BaseAction):
    """Generate CSV data from structured records.
    
    Creates CSV output with configurable formatting,
    headers, and quoting behavior.
    """
    action_type = "csv_generator"
    display_name = "CSV生成"
    description = "从结构化记录生成CSV数据"
    VALID_MODES = ["from_records", "from_rows"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate CSV from records or rows.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, headers, delimiter,
                   quotechar, include_header, line_terminator.
        
        Returns:
            ActionResult with generated CSV string.
        """
        mode = params.get("mode", "from_records")
        data = params.get("data", [])
        headers = params.get("headers")
        delimiter = params.get("delimiter", ",")
        quotechar = params.get("quotechar", '"')
        include_header = params.get("include_header", True)
        
        if not data:
            return ActionResult(success=False, message="Data is required")
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            output = io.StringIO()
            writer = csv.writer(
                output,
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=csv.QUOTE_MINIMAL
            )
            
            if mode == "from_records":
                if headers:
                    if include_header:
                        writer.writerow(headers)
                    
                    for record in data:
                        row = []
                        for h in headers:
                            val = record.get(h, "") if isinstance(record, dict) else record[h] if isinstance(record, (list, tuple)) else ""
                            row.append(str(val) if val is not None else "")
                        writer.writerow(row)
                else:
                    inferred_headers = set()
                    for record in data:
                        if isinstance(record, dict):
                            inferred_headers.update(record.keys())
                    
                    sorted_headers = sorted(inferred_headers)
                    if include_header:
                        writer.writerow(sorted_headers)
                    
                    for record in data:
                        row = [str(record.get(h, "")) if isinstance(record, dict) else str(record) for h in sorted_headers]
                        writer.writerow(row)
            else:
                if include_header and headers:
                    writer.writerow(headers)
                
                for row in data:
                    writer.writerow([str(cell) if cell is not None else "" for cell in row])
            
            csv_output = output.getvalue()
            
            return ActionResult(
                success=True,
                message=f"Generated CSV: {len(csv_output)} bytes",
                data={
                    "csv": csv_output,
                    "size": len(csv_output),
                    "rows": len(data),
                    "columns": len(headers) if headers else 0
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV generation failed: {e}")


class CsvTransformAction(BaseAction):
    """Transform CSV data with filtering, sorting, and aggregation.
    
    Supports column selection, row filtering, sorting, and
    computing derived columns from existing data.
    """
    action_type = "csv_transform"
    display_name = "CSV转换"
    description = "CSV数据过滤、排序和聚合转换"
    VALID_SORT_ORDERS = ["asc", "desc"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform CSV records.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, select_columns, filter_expr,
                   sort_by, sort_order, compute_column.
        
        Returns:
            ActionResult with transformed records.
        """
        records = params.get("records", [])
        select_columns = params.get("select_columns")
        filter_expr = params.get("filter_expr")
        sort_by = params.get("sort_by")
        sort_order = params.get("sort_order", "asc")
        compute_column = params.get("compute_column")
        
        if not records:
            return ActionResult(success=False, message="No records to transform")
        
        if not isinstance(records[0], dict):
            return ActionResult(success=False, message="Records must be dictionaries")
        
        try:
            result = list(records)
            
            if select_columns:
                result = []
                for record in records:
                    filtered = {k: record.get(k) for k in select_columns if k in record}
                    result.append(filtered)
            
            if filter_expr:
                filtered = []
                for record in result:
                    try:
                        eval_globals = {"record": record}
                        if eval(filter_expr, eval_globals):
                            filtered.append(record)
                    except Exception:
                        pass
                result = filtered
            
            if sort_by:
                reverse = sort_order == "desc"
                result = sorted(
                    result,
                    key=lambda r: r.get(sort_by, ""),
                    reverse=reverse
                )
            
            if compute_column:
                col_name = compute_column.get("name")
                col_expr = compute_column.get("expression")
                if col_name and col_expr:
                    for record in result:
                        try:
                            eval_globals = {"record": record}
                            record[col_name] = eval(col_expr, eval_globals)
                        except Exception:
                            record[col_name] = None
            
            return ActionResult(
                success=True,
                message=f"Transformed {len(result)} records",
                data={
                    "records": result,
                    "row_count": len(result)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV transform failed: {e}")
