"""Data Convert Action.

Converts data between various formats (JSON, CSV, XML, YAML, Excel, etc.)
with schema validation and transformation capabilities.
"""

import sys
import os
import json
import csv
from typing import Any, Dict, List, Optional
from io import StringIO, BytesIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataConvertAction(BaseAction):
    """Convert data between various formats.
    
    Supports JSON, CSV, XML, YAML, TSV, and Excel formats
    with configurable parsing options and validation.
    """
    action_type = "data_convert"
    display_name = "数据转换"
    description = "数据格式转换，支持JSON/CSV/XML/YAML/Excel等"

    SUPPORTED_FORMATS = ['json', 'csv', 'tsv', 'xml', 'yaml', 'xlsx', 'dict', 'list']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Convert data between formats.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'convert', 'validate', 'preview'.
                - data: Input data (string, dict, list, or variable name).
                - from_format: Source format (auto-detected if not specified).
                - to_format: Target format for conversion.
                - source_file: Source file path.
                - destination_file: Output file path.
                - encoding: File encoding (default: utf-8).
                - csv_delimiter: CSV delimiter (default: comma).
                - csv_headers: Use first row as headers (default: True).
                - flatten: Flatten nested structures (default: False).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with converted data.
        """
        try:
            action = params.get('action', 'convert')
            save_to_var = params.get('save_to_var', 'converted_data')

            if action == 'convert':
                return self._convert(context, params, save_to_var)
            elif action == 'validate':
                return self._validate(context, params, save_to_var)
            elif action == 'preview':
                return self._preview(context, params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Conversion error: {e}")

    def _convert(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Convert data between formats."""
        data = params.get('data')
        from_format = params.get('from_format', 'auto').lower()
        to_format = params.get('to_format', 'json').lower()
        source_file = params.get('source_file')
        destination_file = params.get('destination_file')
        encoding = params.get('encoding', 'utf-8')
        csv_delimiter = params.get('csv_delimiter', ',')
        csv_headers = params.get('csv_headers', True)
        flatten = params.get('flatten', False)

        # Get input data
        if data is None and source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"Source file not found: {source_file}")
            with open(source_file, 'r', encoding=encoding) as f:
                data = f.read()
            if from_format == 'auto':
                from_format = self._detect_format_from_file(source_file)
        elif data is None:
            data = context.get_variable(params.get('use_var', 'input_data'))
            if data is None:
                return ActionResult(success=False, message="No data provided")

        # Auto-detect source format
        if from_format == 'auto':
            if isinstance(data, (dict, list)):
                from_format = 'dict'
            elif isinstance(data, str):
                from_format = self._detect_format(data)
            else:
                from_format = 'json'

        # Parse to intermediate format (dict/list)
        if from_format == 'json' and isinstance(data, str):
            parsed = json.loads(data)
        elif from_format == 'csv':
            parsed = self._parse_csv(data, csv_delimiter, csv_headers)
        elif from_format == 'tsv':
            parsed = self._parse_csv(data, '\t', csv_headers)
        elif from_format == 'xml':
            parsed = self._parse_xml(data)
        elif from_format == 'yaml':
            parsed = self._parse_yaml(data)
        elif from_format in ('dict', 'json'):
            parsed = data if isinstance(data, (dict, list)) else json.loads(data)
        else:
            return ActionResult(success=False, message=f"Unsupported source format: {from_format}")

        # Flatten if requested
        if flatten and isinstance(parsed, list):
            parsed = self._flatten_list(parsed)

        # Convert to target format
        if to_format == 'json':
            output = json.dumps(parsed, ensure_ascii=False, indent=2)
        elif to_format == 'csv':
            output = self._to_csv(parsed, csv_delimiter, csv_headers)
        elif to_format == 'tsv':
            output = self._to_csv(parsed, '\t', csv_headers)
        elif to_format == 'xml':
            output = self._to_xml(parsed)
        elif to_format == 'yaml':
            output = self._to_yaml(parsed)
        elif to_format == 'dict':
            output = parsed if isinstance(parsed, dict) else {'data': parsed}
        elif to_format == 'list':
            output = parsed if isinstance(parsed, list) else [parsed]
        else:
            return ActionResult(success=False, message=f"Unsupported target format: {to_format}")

        # Save or return
        if destination_file:
            mode = 'wb' if isinstance(output, bytes) else 'w'
            with open(destination_file, mode, encoding=encoding if mode == 'w' else None) as f:
                f.write(output)
            result = {'destination': destination_file, 'format': to_format}
        else:
            result = {'data': output, 'format': to_format}

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, message=f"Converted {from_format} -> {to_format}")

    def _validate(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Validate data format."""
        data = params.get('data')
        format_type = params.get('format', 'auto').lower()
        source_file = params.get('source_file')
        encoding = params.get('encoding', 'utf-8')

        if data is None and source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"Source file not found: {source_file}")
            with open(source_file, 'r', encoding=encoding) as f:
                data = f.read()
            if format_type == 'auto':
                format_type = self._detect_format_from_file(source_file)
        elif data is None:
            data = context.get_variable(params.get('use_var', 'input_data'))

        if data is None:
            return ActionResult(success=False, message="No data provided")

        if format_type == 'auto':
            format_type = self._detect_format(data)

        valid = True
        error = None

        try:
            if format_type == 'json':
                json.loads(data)
            elif format_type == 'csv':
                list(csv.reader(StringIO(data)))
            elif format_type == 'xml':
                self._parse_xml(data)
            elif format_type == 'yaml':
                self._parse_yaml(data)
        except Exception as e:
            valid = False
            error = str(e)

        result = {'valid': valid, 'format': format_type, 'error': error}
        context.set_variable(save_to_var, result)
        return ActionResult(success=valid, data=result, message=f"Validation: {'PASS' if valid else 'FAIL'}")

    def _preview(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Preview data with format detection."""
        data = params.get('data')
        source_file = params.get('source_file')
        max_lines = params.get('max_lines', 5)
        encoding = params.get('encoding', 'utf-8')

        if data is None and source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"Source file not found: {source_file}")
            with open(source_file, 'r', encoding=encoding) as f:
                lines = f.readlines()[:max_lines]
                data = ''.join(lines)
        elif data is None:
            data = context.get_variable(params.get('use_var', 'input_data'))

        if data is None:
            return ActionResult(success=False, message="No data provided")

        format_type = self._detect_format(data)

        result = {
            'detected_format': format_type,
            'preview': str(data)[:500],
            'length': len(str(data))
        }
        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, message=f"Format: {format_type}")

    def _detect_format(self, data: str) -> str:
        """Auto-detect data format."""
        data = data.strip()
        
        if data.startswith('{') or data.startswith('['):
            try:
                json.loads(data)
                return 'json'
            except Exception:
                pass
        
        if data.startswith('<?xml') or data.startswith('<'):
            return 'xml'
        
        if ':' in data and '\n' in data:
            try:
                self._parse_yaml(data)
                return 'yaml'
            except Exception:
                pass
        
        if ',' in data or '\t' in data:
            first_line = data.split('\n')[0]
            if first_line.count(',') > first_line.count(':'):
                return 'csv'
            elif '\t' in first_line:
                return 'tsv'
        
        return 'json'

    def _detect_format_from_file(self, path: str) -> str:
        """Detect format from file extension."""
        if path.endswith('.json'):
            return 'json'
        elif path.endswith('.csv'):
            return 'csv'
        elif path.endswith('.tsv'):
            return 'tsv'
        elif path.endswith('.xml'):
            return 'xml'
        elif path.endswith(('.yaml', '.yml')):
            return 'yaml'
        return 'auto'

    def _parse_csv(self, data: str, delimiter: str, headers: bool) -> List[Dict]:
        """Parse CSV data to list of dicts."""
        reader = csv.reader(StringIO(data), delimiter=delimiter)
        rows = list(reader)
        
        if not rows:
            return []
        
        if headers and len(rows) > 1:
            header = rows[0]
            return [dict(zip(header, row)) for row in rows[1:]]
        else:
            return [list(row) for row in rows]

    def _to_csv(self, data: List, delimiter: str, headers: bool) -> str:
        """Convert list of dicts to CSV."""
        if not data:
            return ''
        
        output = StringIO()
        
        if headers and isinstance(data[0], dict):
            writer = csv.DictWriter(output, fieldnames=list(data[0].keys()), delimiter=delimiter)
            writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(output, delimiter=delimiter)
            writer.writerows(data)
        
        return output.getvalue()

    def _parse_xml(self, data: str) -> Any:
        """Parse XML to dict."""
        import xml.etree.ElementTree as ET
        root = ET.fromstring(data)
        return self._xml_to_dict(root)

    def _xml_to_dict(self, element) -> Dict:
        """Convert XML element to dict."""
        result = {}
        for child in element:
            child_data = self._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        if element.text and element.text.strip():
            result['_text'] = element.text.strip()
        return {element.tag: result}

    def _to_xml(self, data: Any, root_name: str = 'root') -> str:
        """Convert dict to XML."""
        import xml.etree.ElementTree as ET
        
        def dict_to_xml(tag, d):
            elem = ET.Element(tag)
            if isinstance(d, dict):
                for k, v in d.items():
                    if k.startswith('_'):
                        elem.text = str(v)
                    else:
                        child = dict_to_xml(k, v)
                        elem.append(child)
            elif isinstance(d, list):
                for item in d:
                    child = dict_to_xml(tag.rstrip('s'), item)
                    elem.append(child)
            else:
                elem.text = str(d)
            return elem
        
        root = dict_to_xml(root_name, data)
        return ET.tostring(root, encoding='unicode')

    def _parse_yaml(self, data: str) -> Any:
        """Parse YAML to dict."""
        try:
            import yaml
            return yaml.safe_load(data)
        except ImportError:
            # Simple fallback for basic YAML
            result = {}
            for line in data.split('\n'):
                if ':' in line:
                    key, val = line.split(':', 1)
                    result[key.strip()] = val.strip().strip('"').strip("'")
            return result

    def _to_yaml(self, data: Any) -> str:
        """Convert dict to YAML."""
        try:
            import yaml
            return yaml.dump(data, allow_unicode=True, default_flow_style=False)
        except ImportError:
            return json.dumps(data, ensure_ascii=False)

    def _flatten_list(self, data: List[Dict]) -> List[Dict]:
        """Flatten list of dicts to single-level dicts."""
        result = []
        for item in data:
            if isinstance(item, dict):
                flat = {}
                for k, v in item.items():
                    if isinstance(v, (dict, list)):
                        flat[k] = json.dumps(v)
                    else:
                        flat[k] = v
                result.append(flat)
            else:
                result.append(item)
        return result
