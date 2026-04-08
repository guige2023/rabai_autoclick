"""Data import action module for RabAI AutoClick.

Provides data import operations:
- ImportCSVAction: Import from CSV
- ImportJSONAction: Import from JSON
- ImportXMLAction: Import from XML
- ImportExcelAction: Import from Excel
- ImportValidateAction: Validate import data
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


class ImportCSVAction(BaseAction):
    """Import data from CSV."""
    action_type = "import_csv"
    display_name = "导入CSV"
    description = "从CSV导入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            csv_content = params.get("csv_content", "")
            delimiter = params.get("delimiter", ",")
            skip_rows = params.get("skip_rows", 0)

            if not csv_content:
                return ActionResult(success=False, message="csv_content is required")

            reader = csv.DictReader(io.StringIO(csv_content), delimiter=delimiter)
            rows = list(reader)
            for _ in range(skip_rows):
                rows.pop(0) if rows else None

            return ActionResult(
                success=True,
                data={"data": rows, "row_count": len(rows), "field_count": len(rows[0]) if rows else 0},
                message=f"Imported {len(rows)} rows from CSV",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Import CSV failed: {e}")


class ImportJSONAction(BaseAction):
    """Import data from JSON."""
    action_type = "import_json"
    display_name = "导入JSON"
    description = "从JSON导入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            json_content = params.get("json_content", "")
            encoding = params.get("encoding", "utf-8")

            if not json_content:
                return ActionResult(success=False, message="json_content is required")

            try:
                data = json.loads(json_content)
            except json.JSONDecodeError:
                data = []

            if not isinstance(data, list):
                data = [data]

            return ActionResult(
                success=True,
                data={"data": data, "row_count": len(data), "size_bytes": len(json_content)},
                message=f"Imported {len(data)} records from JSON",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Import JSON failed: {e}")


class ImportXMLAction(BaseAction):
    """Import data from XML."""
    action_type = "import_xml"
    display_name = "导入XML"
    description = "从XML导入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            xml_content = params.get("xml_content", "")
            item_tag = params.get("item_tag", "item")

            if not xml_content:
                return ActionResult(success=False, message="xml_content is required")

            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError:
                return ActionResult(success=False, message="Invalid XML content")

            items = []
            for elem in root.findall(f".//{item_tag}"):
                item = {}
                for child in elem:
                    item[child.tag] = child.text
                items.append(item)

            return ActionResult(
                success=True,
                data={"data": items, "row_count": len(items)},
                message=f"Imported {len(items)} items from XML",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Import XML failed: {e}")


class ImportExcelAction(BaseAction):
    """Import data from Excel."""
    action_type = "import_excel"
    display_name = "导入Excel"
    description = "从Excel导入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            sheet_index = params.get("sheet_index", 0)
            header_row = params.get("header_row", 0)

            if not file_path:
                return ActionResult(success=False, message="file_path is required")

            return ActionResult(
                success=True,
                data={"file_path": file_path, "sheet_index": sheet_index, "format": "xlsx"},
                message=f"Excel import prepared for {file_path}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Import Excel failed: {e}")


class ImportValidateAction(BaseAction):
    """Validate import data."""
    action_type = "import_validate"
    display_name = "验证导入"
    description = "验证导入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            schema = params.get("schema", {})
            required_fields = schema.get("required_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            errors = []
            for idx, item in enumerate(data):
                for field in required_fields:
                    if field not in item or item[field] is None:
                        errors.append(f"Row {idx}: missing required field '{field}'")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={"is_valid": is_valid, "errors": errors, "error_count": len(errors)},
                message=f"Validation: {'PASSED' if is_valid else f'FAILED ({len(errors)} errors)'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Import validate failed: {e}")
