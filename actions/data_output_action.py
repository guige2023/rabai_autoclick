"""Data output formatting action module for RabAI AutoClick.

Provides data output formatting operations:
- CsvOutputAction: Format data as CSV
- JsonOutputAction: Format data as JSON
- TableOutputAction: Format data as table
- XmlOutputAction: Format data as XML
"""

import csv
import io
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvOutputAction(BaseAction):
    """Format data as CSV."""
    action_type = "csv_output"
    display_name = "CSV输出"
    description = "将数据格式化为CSV"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            columns = params.get("columns", None)
            delimiter = params.get("delimiter", ",")
            include_header = params.get("include_header", True)
            output_file = params.get("output_file", None)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            if columns:
                fieldnames = columns
            else:
                if isinstance(data[0], dict):
                    fieldnames = list(data[0].keys())
                else:
                    fieldnames = ["value"]

            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)

            if include_header:
                writer.writeheader()

            for row in data:
                if isinstance(row, dict):
                    writer.writerow({k: row.get(k, "") for k in fieldnames})
                else:
                    writer.writerow({fieldnames[0]: row})

            csv_content = output.getvalue()

            if output_file:
                with open(output_file, "w", newline="") as f:
                    f.write(csv_content)
                return ActionResult(
                    success=True,
                    message=f"CSV written to {output_file}",
                    data={"file": output_file, "rows": len(data), "columns": len(fieldnames)},
                )

            return ActionResult(
                success=True,
                message=f"CSV output: {len(data)} rows",
                data={"csv": csv_content, "rows": len(data), "columns": len(fieldnames)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CsvOutput error: {e}")


class JsonOutputAction(BaseAction):
    """Format data as JSON."""
    action_type = "json_output"
    display_name = "JSON输出"
    description = "将数据格式化为JSON"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            indent = params.get("indent", 2)
            ensure_ascii = params.get("ensure_ascii", False)
            sort_keys = params.get("sort_keys", False)
            output_file = params.get("output_file", None)

            if not isinstance(data, list):
                data = [data]

            if indent is None:
                json_content = json.dumps(data, ensure_ascii=ensure_ascii, sort_keys=sort_keys)
            else:
                json_content = json.dumps(data, ensure_ascii=ensure_ascii, indent=indent, sort_keys=sort_keys)

            if output_file:
                with open(output_file, "w") as f:
                    f.write(json_content)
                return ActionResult(
                    success=True,
                    message=f"JSON written to {output_file}",
                    data={"file": output_file, "items": len(data)},
                )

            return ActionResult(
                success=True,
                message=f"JSON output: {len(data)} items",
                data={"json": json_content, "items": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JsonOutput error: {e}")


class TableOutputAction(BaseAction):
    """Format data as a text table."""
    action_type = "table_output"
    display_name = "表格输出"
    description = "将数据格式化为表格"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            columns = params.get("columns", None)
            max_col_width = params.get("max_col_width", 30)
            truncate = params.get("truncate", True)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            if columns:
                headers = columns
            else:
                if isinstance(data[0], dict):
                    headers = list(data[0].keys())
                else:
                    headers = ["value"]

            def format_value(v: Any, max_w: int) -> str:
                s = str(v) if v is not None else ""
                if truncate and len(s) > max_w:
                    return s[: max_w - 3] + "..."
                return s

            col_widths = {h: len(h) for h in headers}
            rows_formatted = []
            for row in data:
                if isinstance(row, dict):
                    formatted_row = [format_value(row.get(h), max_col_width) for h in headers]
                else:
                    formatted_row = [format_value(row if i == 0 else "", max_col_width) for i in range(len(headers))]
                rows_formatted.append(formatted_row)
                for i, cell in enumerate(formatted_row):
                    col_widths[headers[i]] = max(col_widths[headers[i]], len(cell))

            separator = "+" + "+".join("-" * (col_widths[h] + 2) for h in headers) + "+"
            header_line = "|" + "|".join(f" {headers[i]:<{col_widths[headers[i]]}} " for i in range(len(headers))) + "|"

            table_lines = [separator, header_line, separator]
            for row in rows_formatted:
                table_lines.append("|" + "|".join(f" {row[i]:<{col_widths[headers[i]]}} " for i in range(len(headers))) + "|")
            table_lines.append(separator)

            table_output = "\n".join(table_lines)

            return ActionResult(
                success=True,
                message=f"Table output: {len(data)} rows, {len(headers)} columns",
                data={"table": table_output, "rows": len(data), "columns": len(headers)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TableOutput error: {e}")


class XmlOutputAction(BaseAction):
    """Format data as XML."""
    action_type = "xml_output"
    display_name = "XML输出"
    description = "将数据格式化为XML"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            root_name = params.get("root_name", "root")
            item_name = params.get("item_name", "item")
            output_file = params.get("output_file", None)

            if not isinstance(data, list):
                data = [data]

            root = ET.Element(root_name)

            for item in data:
                item_elem = ET.SubElement(root, item_name)
                if isinstance(item, dict):
                    self._dict_to_xml(item, item_elem)
                else:
                    item_elem.text = str(item)

            xml_content = ET.tostring(root, encoding="unicode")

            if output_file:
                tree = ET.ElementTree(root)
                with open(output_file, "wb") as f:
                    tree.write(f, encoding="utf-8", xml_declaration=True)
                return ActionResult(
                    success=True,
                    message=f"XML written to {output_file}",
                    data={"file": output_file, "items": len(data)},
                )

            return ActionResult(
                success=True,
                message=f"XML output: {len(data)} items",
                data={"xml": xml_content, "items": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XmlOutput error: {e}")

    def _dict_to_xml(self, d: Dict, parent: ET.Element):
        for key, value in d.items():
            child = ET.SubElement(parent, str(key))
            if isinstance(value, dict):
                self._dict_to_xml(value, child)
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    item = ET.SubElement(child, "item")
                    if isinstance(v, dict):
                        self._dict_to_xml(v, item)
                    else:
                        item.text = str(v)
            else:
                child.text = str(value) if value is not None else ""
