"""Data converter action module for RabAI AutoClick.

Provides data format conversion between JSON, CSV, XML, YAML,
and other structured data formats.
"""

import sys
import os
import json
import csv
import io
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataConverterAction(BaseAction):
    """Data converter action for format transformation.
    
    Supports conversion between JSON, CSV, XML, YAML, and other
    structured data formats.
    """
    action_type = "data_converter"
    display_name = "数据格式转换"
    description = "JSON/CSV/XML/YAML格式互转"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute conversion operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: convert|format
                data: Input data
                input_format: Source format (auto-detected if not specified)
                output_format: Target format
                options: Format-specific options.
        
        Returns:
            ActionResult with converted data.
        """
        data = params.get('data')
        input_format = params.get('input_format', 'auto')
        output_format = params.get('output_format', 'json')
        options = params.get('options', {})
        
        if data is None:
            return ActionResult(success=False, message="No data provided")
        
        if input_format == 'auto':
            input_format = self._detect_format(data)
        
        parsed = self._parse(data, input_format)
        
        if output_format == 'json':
            return self._to_json(parsed, options)
        elif output_format == 'csv':
            return self._to_csv(parsed, options)
        elif output_format == 'xml':
            return self._to_xml(parsed, options)
        elif output_format == 'yaml':
            return self._to_yaml(parsed, options)
        else:
            return ActionResult(success=False, message=f"Unknown output format: {output_format}")
    
    def _detect_format(self, data: Any) -> str:
        """Auto-detect data format."""
        if isinstance(data, str):
            data_stripped = data.strip()
            if data_stripped.startswith('{') or data_stripped.startswith('['):
                return 'json'
            if data_stripped.startswith('<'):
                return 'xml'
            if ':' in data_stripped and '\n' in data_stripped:
                return 'yaml'
            if ',' in data_stripped and '\n' in data_stripped:
                return 'csv'
        elif isinstance(data, (dict, list)):
            return 'json'
        
        return 'json'
    
    def _parse(self, data: Any, format: str) -> Any:
        """Parse data from specified format."""
        if isinstance(data, (dict, list)):
            return data
        
        if format == 'json':
            try:
                return json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {e}")
        
        elif format == 'csv':
            return self._parse_csv(data)
        
        elif format == 'xml':
            return self._parse_xml(data)
        
        elif format == 'yaml':
            return self._parse_yaml(data)
        
        return data
    
    def _parse_csv(self, data: str) -> List[Dict]:
        """Parse CSV string to list of dicts."""
        reader = csv.DictReader(io.StringIO(data))
        return list(reader)
    
    def _parse_xml(self, data: str) -> Dict:
        """Parse XML string to dict (simplified)."""
        import re
        
        result = {}
        
        tag_pattern = re.compile(r'<(\w+)(?:\s+[^>]*)?>([^<]*)</\1>')
        matches = tag_pattern.findall(data)
        
        for tag, content in matches:
            if tag not in result:
                result[tag] = content.strip()
            else:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(content.strip())
        
        return {'root': result}
    
    def _parse_yaml(self, data: str) -> Dict:
        """Parse YAML string to dict."""
        try:
            import yaml
            return yaml.safe_load(data)
        except ImportError:
            lines = data.strip().split('\n')
            result = {}
            for line in lines:
                if ':' in line:
                    key, value = line.split(':', 1)
                    result[key.strip()] = value.strip().strip('"\'')
            return result
        except Exception:
            return {'data': data}
    
    def _to_json(self, data: Any, options: Dict) -> ActionResult:
        """Convert data to JSON."""
        indent = options.get('indent', 2)
        sort_keys = options.get('sort_keys', False)
        
        json_str = json.dumps(data, indent=indent, sort_keys=sort_keys, ensure_ascii=False)
        
        return ActionResult(
            success=True,
            message=f"Converted to JSON ({len(json_str)} bytes)",
            data={
                'data': json_str,
                'format': 'json',
                'size_bytes': len(json_str)
            }
        )
    
    def _to_csv(self, data: Any, options: Dict) -> ActionResult:
        """Convert data to CSV."""
        if not isinstance(data, list):
            return ActionResult(success=False, message="CSV export requires list of records")
        
        if not data:
            return ActionResult(success=True, message="Empty CSV", data={'data': '', 'format': 'csv'})
        
        fieldnames = list(data[0].keys()) if isinstance(data[0], dict) else []
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)
            else:
                writer.writerow({fieldnames[0] if fieldnames else 'value': row})
        
        csv_str = output.getvalue()
        
        return ActionResult(
            success=True,
            message=f"Converted to CSV ({len(data)} rows)",
            data={
                'data': csv_str,
                'format': 'csv',
                'rows': len(data),
                'columns': len(fieldnames)
            }
        )
    
    def _to_xml(self, data: Any, options: Dict) -> ActionResult:
        """Convert data to XML."""
        root_name = options.get('root', 'root')
        row_name = options.get('row', 'row')
        
        xml_parts = [f'<{root_name}>']
        
        if isinstance(data, list):
            for item in data:
                xml_parts.append(f'  <{row_name}>')
                if isinstance(item, dict):
                    for key, value in item.items():
                        safe_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        xml_parts.append(f'    <{key}>{safe_value}</{key}>')
                else:
                    xml_parts.append(f'    <value>{item}</value>')
                xml_parts.append(f'  </{row_name}>')
        elif isinstance(data, dict):
            for key, value in data.items():
                safe_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                xml_parts.append(f'  <{key}>{safe_value}</{key}>')
        
        xml_parts.append(f'</{root_name}>')
        xml_str = '\n'.join(xml_parts)
        
        return ActionResult(
            success=True,
            message=f"Converted to XML",
            data={
                'data': xml_str,
                'format': 'xml',
                'size_bytes': len(xml_str)
            }
        )
    
    def _to_yaml(self, data: Any, options: Dict) -> ActionResult:
        """Convert data to YAML."""
        try:
            import yaml
            yaml_str = yaml.dump(data, allow_unicode=True, sort_keys=False)
        except ImportError:
            yaml_str = json.dumps(data, indent=2)
        
        return ActionResult(
            success=True,
            message=f"Converted to YAML",
            data={
                'data': yaml_str,
                'format': 'yaml',
                'size_bytes': len(yaml_str)
            }
        )
