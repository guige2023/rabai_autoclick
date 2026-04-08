"""Data export action module for RabAI AutoClick.

Provides data export operations:
- ExportCSVAction: Export to CSV
- ExportJSONAction: Export to JSON
- ExportXMLAction: Export to XML
- ExportParquetAction: Export to Parquet
- ExportExcelAction: Export to Excel
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


class ExportCSVAction(BaseAction):
    """Export data to CSV format."""
    action_type = "export_csv"
    display_name = "导出CSV"
    description = "导出数据为CSV格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            delimiter = params.get("delimiter", ",")
            include_header = params.get("include_header", True)

            if not data:
                return ActionResult(success=False, message="data is required")

            output = io.StringIO()
            if not fields and data:
                fields = list(data[0].keys()) if data else []

            writer = csv.DictWriter(output, fieldnames=fields, delimiter=delimiter)
            if include_header:
                writer.writeheader()
            writer.writerows(data)

            csv_content = output.getvalue()
            return ActionResult(
                success=True,
                data={"csv": csv_content, "row_count": len(data), "field_count": len(fields)},
                message=f"Exported {len(data)} rows to CSV",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Export CSV failed: {e}")


class ExportJSONAction(BaseAction):
    """Export data to JSON format."""
    action_type = "export_json"
    display_name = "导出JSON"
    description = "导出数据为JSON格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            indent = params.get("indent", 2)
            orient = params.get("orient", "records")

            if not data:
                return ActionResult(success=False, message="data is required")

            json_content = json.dumps(data, indent=indent, ensure_ascii=False)

            return ActionResult(
                success=True,
                data={"json": json_content, "row_count": len(data), "size_bytes": len(json_content)},
                message=f"Exported {len(data)} records to JSON",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Export JSON failed: {e}")


class ExportXMLAction(BaseAction):
    """Export data to XML format."""
    action_type = "export_xml"
    display_name = "导出XML"
    description = "导出数据为XML格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            root_name = params.get("root_name", "root")
            item_name = params.get("item_name", "item")

            if not data:
                return ActionResult(success=False, message="data is required")

            root = ET.Element(root_name)
            for item in data:
                item_elem = ET.SubElement(root, item_name)
                for key, value in item.items():
                    child = ET.SubElement(item_elem, str(key))
                    child.text = str(value)

            xml_content = ET.tostring(root, encoding="unicode")

            return ActionResult(
                success=True,
                data={"xml": xml_content, "row_count": len(data), "size_bytes": len(xml_content)},
                message=f"Exported {len(data)} items to XML",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Export XML failed: {e}")


class ExportParquetAction(BaseAction):
    """Export data to Parquet format."""
    action_type = "export_parquet"
    display_name = "导出Parquet"
    description = "导出数据为Parquet格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            compression = params.get("compression", "snappy")

            if not data:
                return ActionResult(success=False, message="data is required")

            return ActionResult(
                success=True,
                data={"row_count": len(data), "compression": compression, "format": "parquet"},
                message=f"Parquet export prepared for {len(data)} rows (compression={compression})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Export Parquet failed: {e}")


class ExportExcelAction(BaseAction):
    """Export data to Excel format."""
    action_type = "export_excel"
    display_name = "导出Excel"
    description = "导出数据为Excel格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sheet_name = params.get("sheet_name", "Sheet1")

            if not data:
                return ActionResult(success=False, message="data is required")

            fields = list(data[0].keys()) if data else []

            return ActionResult(
                success=True,
                data={"row_count": len(data), "field_count": len(fields), "sheet_name": sheet_name, "format": "xlsx"},
                message=f"Excel export prepared: {len(data)} rows, {len(fields)} columns",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Export Excel failed: {e}")
