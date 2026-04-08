"""Data Formatter action module for RabAI AutoClick.

Formats data between various formats (CSV, JSON, XML, YAML, etc.)
with schema validation and transformation.
"""

import json
import csv
import io
import sys
import os
import base64
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFormatterAction(BaseAction):
    """Format data between different serialization formats.

    Converts between JSON, CSV, XML, YAML, and other formats
    with configurable options.
    """
    action_type = "data_formatter"
    display_name = "数据格式化器"
    description = "在不同数据格式之间转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format data.

        Args:
            context: Execution context.
            params: Dict with keys: data, input_format, output_format,
                   options (format-specific options).

        Returns:
            ActionResult with formatted data.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            input_format = params.get('input_format', 'auto')
            output_format = params.get('output_format', 'json')
            options = params.get('options', {})

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            # Auto-detect input format
            if input_format == 'auto':
                input_format = self._detect_format(data)

            # Parse input to normalized dict/list
            parsed = self._parse(data, input_format)

            # Format output
            output = self._format(parsed, output_format, options)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Formatted {input_format} -> {output_format}",
                data={'format': output_format, 'data': output, 'size': len(str(output))},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Formatting failed: {str(e)}",
                duration=duration,
            )

    def _detect_format(self, data: Any) -> str:
        """Auto-detect data format."""
        if isinstance(data, (dict, list)):
            return 'json'
        if isinstance(data, str):
            data = data.strip()
            if data.startswith('{') or data.startswith('['):
                return 'json'
            if data.startswith('<') and data.endswith('>'):
                return 'xml'
            if data.startswith('---') or '\n  ' in data:
                return 'yaml'
            if ',' in data and '\n' in data:
                return 'csv'
        return 'json'

    def _parse(self, data: Any, fmt: str) -> Any:
        """Parse data from format."""
        if fmt == 'json':
            if isinstance(data, (dict, list)):
                return data
            return json.loads(data)
        elif fmt == 'csv':
            if isinstance(data, str):
                reader = csv.DictReader(io.StringIO(data))
                return list(reader)
            return data
        elif fmt == 'xml':
            import xml.etree.ElementTree as ET
            root = ET.fromstring(data)
            return self._xml_to_dict(root)
        elif fmt == 'yaml':
            try:
                import yaml
                return yaml.safe_load(data)
            except ImportError:
                return json.loads(data)
        return data

    def _format(self, data: Any, fmt: str, options: Dict) -> str:
        """Format data to output format."""
        indent = options.get('indent', 2)
        if fmt == 'json':
            return json.dumps(data, indent=indent, ensure_ascii=False)
        elif fmt == 'csv':
            return self._to_csv(data, options)
        elif fmt == 'xml':
            return self._to_xml(data, options)
        elif fmt == 'yaml':
            try:
                import yaml
                return yaml.dump(data, indent=indent, allow_unicode=True)
            except ImportError:
                return json.dumps(data, indent=indent)
        elif fmt == 'base64':
            return base64.b64encode(str(data).encode('utf-8')).decode('ascii')
        return str(data)

    def _to_csv(self, data: Any, options: Dict) -> str:
        """Convert data to CSV."""
        if not isinstance(data, list):
            data = [data]
        if not data:
            return ''
        keys = options.get('fields') or (list(data[0].keys()) if isinstance(data[0], dict) else [])
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys, extrasaction='ignore')
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, '') for k in keys})
        return output.getvalue()

    def _to_xml(self, data: Any, options: Dict) -> str:
        """Convert data to XML."""
        root_name = options.get('root_name', 'root')
        item_name = options.get('item_name', 'item')
        import xml.etree.ElementTree as ET
        if isinstance(data, dict):
            root = ET.Element(root_name)
            self._dict_to_xml(data, root)
        elif isinstance(data, list):
            root = ET.Element(root_name)
            for item in data:
                item_elem = ET.SubElement(root, item_name)
                if isinstance(item, dict):
                    self._dict_to_xml(item, item_elem)
                else:
                    item_elem.text = str(item)
        else:
            root = ET.Element(root_name)
            root.text = str(data)
        return ET.tostring(root, encoding='unicode')

    def _dict_to_xml(self, d: Dict, parent: Any) -> None:
        """Convert dict to XML element."""
        import xml.etree.ElementTree as ET
        for key, value in d.items():
            child = ET.SubElement(parent, str(key))
            if isinstance(value, dict):
                self._dict_to_xml(value, child)
            elif isinstance(value, list):
                for item in value:
                    item_elem = ET.SubElement(child, 'item')
                    if isinstance(item, dict):
                        self._dict_to_xml(item, item_elem)
                    else:
                        item_elem.text = str(item)
            else:
                child.text = str(value) if value is not None else ''

    def _xml_to_dict(self, elem: Any) -> Dict:
        """Convert XML element to dict."""
        result = {}
        for child in elem:
            value = self._xml_to_dict(child) if len(child) else child.text
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(value)
            else:
                result[child.tag] = value
        return result


class DataNormalizerAction(BaseAction):
    """Normalize data to standard schemas.

    Transforms data to conform to a target schema,
    handling missing fields and type coercion.
    """
    action_type = "data_normalizer"
    display_name = "数据标准化器"
    description = "将数据标准化为目标模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize data.

        Args:
            context: Execution context.
            params: Dict with keys: data, schema, strict_mode.

        Returns:
            ActionResult with normalized data.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            schema = params.get('schema', {})
            strict_mode = params.get('strict_mode', False)

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            errors = []
            normalized = {}

            for field_name, field_spec in schema.items():
                field_type = field_spec.get('type', 'string')
                required = field_spec.get('required', False)
                default = field_spec.get('default')
                transform = field_spec.get('transform')

                value = data.get(field_name, default)

                if value is None:
                    if required:
                        errors.append(f"Missing required field: {field_name}")
                    normalized[field_name] = default
                    continue

                # Type coercion
                try:
                    normalized[field_name] = self._coerce(value, field_type)
                except Exception as e:
                    if strict_mode:
                        errors.append(f"Field {field_name}: {str(e)}")
                    normalized[field_name] = default

                # Apply transform
                if transform and callable(transform):
                    try:
                        normalized[field_name] = transform(normalized[field_name])
                    except Exception as e:
                        errors.append(f"Transform failed for {field_name}: {str(e)}")

            # Add fields not in schema
            for key, value in data.items():
                if key not in schema:
                    normalized[key] = value

            duration = time.time() - start_time
            return ActionResult(
                success=len(errors) == 0 or not strict_mode,
                message=f"Normalized: {len(normalized)} fields" + (f", {len(errors)} errors" if errors else ""),
                data={'normalized': normalized, 'errors': errors},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Normalization failed: {str(e)}",
                duration=duration,
            )

    def _coerce(self, value: Any, target_type: str) -> Any:
        """Coerce value to target type."""
        if target_type == 'string':
            return str(value)
        elif target_type == 'integer':
            return int(float(value))
        elif target_type == 'float':
            return float(value)
        elif target_type == 'boolean':
            if isinstance(value, bool):
                return value
            return str(value).lower() in ('true', '1', 'yes', 'on')
        elif target_type == 'array':
            if isinstance(value, list):
                return value
            return [value]
        elif target_type == 'object':
            if isinstance(value, dict):
                return value
            return {'value': value}
        return value
