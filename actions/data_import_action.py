"""Data import action module for RabAI AutoClick.

Provides data import operations from various formats including
JSON, CSV, XML, Excel, and database imports.
"""

import time
import json
import csv
import io
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JsonImporterAction(BaseAction):
    """Import data from JSON format.
    
    Imports records from JSON files or strings with
    support for nested structures and various JSON forms.
    """
    action_type = "json_importer"
    display_name = "JSON导入"
    description = "从JSON格式导入数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import data from JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: source (file path or JSON string),
                   data_path (dot notation to array in JSON),
                   normalize, flatten_separator.
        
        Returns:
            ActionResult with imported data.
        """
        source = params.get('source', '')
        data_path = params.get('data_path', '')
        normalize = params.get('normalize', False)
        flatten_sep = params.get('flatten_separator', '_')
        start_time = time.time()

        if not source:
            return ActionResult(success=False, message="source is required")

        try:
            if os.path.exists(source):
                with open(source, 'r', encoding='utf-8') as f:
                    raw = f.read()
            else:
                raw = source

            data = json.loads(raw)

            if data_path:
                for key in data_path.split('.'):
                    if key.isdigit():
                        data = data[int(key)]
                    else:
                        data = data.get(key, [])

            if not isinstance(data, list):
                data = [data]

            if normalize:
                data = self._normalize_records(data, flatten_sep)

            return ActionResult(
                success=True,
                message=f"Imported {len(data)} records from JSON",
                data={
                    'data': data,
                    'count': len(data),
                    'source': source if os.path.exists(source) else '<string>'
                },
                duration=time.time() - start_time
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"JSON parse error: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Import failed: {str(e)}"
            )

    def _normalize_records(self, data: List[Dict], sep: str) -> List[Dict]:
        """Flatten nested records."""
        normalized = []
        for record in data:
            if isinstance(record, dict):
                flat = self._flatten_dict(record, sep)
                normalized.append(flat)
            else:
                normalized.append(record)
        return normalized

    def _flatten_dict(self, d: Dict, sep: str, parent_key: str = '') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, sep, new_key).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)


class CsvImporterAction(BaseAction):
    """Import data from CSV format.
    
    Imports records from CSV files or strings with
    configurable delimiter, quoting, and header handling.
    """
    action_type = "csv_importer"
    display_name = "CSV导入"
    description = "从CSV格式导入数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import data from CSV.
        
        Args:
            context: Execution context.
            params: Dict with keys: source (file path or CSV string),
                   delimiter, skip_rows, has_header, columns.
        
        Returns:
            ActionResult with imported data.
        """
        source = params.get('source', '')
        delimiter = params.get('delimiter', ',')
        skip_rows = params.get('skip_rows', 0)
        has_header = params.get('has_header', True)
        columns = params.get('columns', [])
        start_time = time.time()

        if not source:
            return ActionResult(success=False, message="source is required")

        try:
            if os.path.exists(source):
                with open(source, 'r', encoding='utf-8') as f:
                    raw = f.read()
            else:
                raw = source

            for _ in range(skip_rows):
                raw = raw[raw.index('\n') + 1:]

            reader = csv.reader(io.StringIO(raw), delimiter=delimiter)
            rows = list(reader)

            if has_header and rows:
                header = rows[0]
                data_rows = rows[1:]
            elif columns:
                header = columns
                data_rows = rows
            else:
                header = [f"col_{i}" for i in range(len(rows[0]))]
                data_rows = rows

            data = []
            for row in data_rows:
                record = {header[i]: row[i] if i < len(row) else '' for i in range(len(header))}
                data.append(record)

            return ActionResult(
                success=True,
                message=f"Imported {len(data)} records from CSV",
                data={
                    'data': data,
                    'count': len(data),
                    'columns': header
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV import failed: {str(e)}"
            )


class XmlImporterAction(BaseAction):
    """Import data from XML format.
    
    Imports records from XML files or strings with
    support for repeated elements and attributes.
    """
    action_type = "xml_importer"
    display_name = "XML导入"
    description = "从XML格式导入数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import data from XML.
        
        Args:
            context: Execution context.
            params: Dict with keys: source (file path or XML string),
                   row_tag, include_attrs.
        
        Returns:
            ActionResult with imported data.
        """
        import xml.etree.ElementTree as ET

        source = params.get('source', '')
        row_tag = params.get('row_tag', 'row')
        include_attrs = params.get('include_attrs', False)
        start_time = time.time()

        if not source:
            return ActionResult(success=False, message="source is required")

        try:
            if os.path.exists(source):
                tree = ET.parse(source)
                root = tree.getroot()
            else:
                root = ET.fromstring(source)

            data = []
            for element in root.iter(row_tag):
                record = {}
                if include_attrs:
                    record.update(element.attrib)
                for child in element:
                    tag = child.tag
                    text = child.text.strip() if child.text else ''
                    if child.attrib:
                        record[f"{tag}_attr"] = dict(child.attrib)
                    record[tag] = text
                if not record and element.text and element.text.strip():
                    record['_value'] = element.text.strip()
                data.append(record)

            return ActionResult(
                success=True,
                message=f"Imported {len(data)} records from XML",
                data={
                    'data': data,
                    'count': len(data)
                },
                duration=time.time() - start_time
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML parse error: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML import failed: {str(e)}"
            )


class ExcelImporterAction(BaseAction):
    """Import data from Excel format.
    
    Imports records from Excel files (.xlsx, .xls) with
    support for sheet selection and header detection.
    """
    action_type = "excel_importer"
    display_name = "Excel导入"
    description = "从Excel格式导入数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import data from Excel.
        
        Args:
            context: Execution context.
            params: Dict with keys: source (file path), sheet_name,
                   sheet_index, has_header, skip_rows.
        
        Returns:
            ActionResult with imported data.
        """
        source = params.get('source', '')
        sheet_name = params.get('sheet_name', None)
        sheet_index = params.get('sheet_index', 0)
        has_header = params.get('has_header', True)
        skip_rows = params.get('skip_rows', 0)
        start_time = time.time()

        if not source or not os.path.exists(source):
            return ActionResult(success=False, message="source file not found")

        try:
            import pandas as pd
            df = pd.read_excel(source, sheet_name=sheet_name or sheet_index, header=0 if has_header else None, skiprows=skip_rows)

            if has_header:
                df.columns = [str(c) for c in df.columns]
            else:
                df.columns = [f"col_{i}" for i in range(len(df.columns))]

            data = df.to_dict('records')

            return ActionResult(
                success=True,
                message=f"Imported {len(data)} records from Excel (sheet: {sheet_name or sheet_index})",
                data={
                    'data': data,
                    'count': len(data),
                    'columns': list(df.columns),
                    'sheet': sheet_name or sheet_index
                },
                duration=time.time() - start_time
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="pandas and openpyxl required for Excel import"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Excel import failed: {str(e)}"
            )
