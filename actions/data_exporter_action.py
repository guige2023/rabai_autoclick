"""Data exporter action module for RabAI AutoClick.

Provides data export to various formats,
batch export, and streaming export capabilities.
"""

import json
import csv
import io
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
import base64
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataExporterAction(BaseAction):
    """Export data to various formats with batch support.
    
    Supports JSON, CSV, XML, Excel, and custom format exports.
    Provides batch processing for large datasets.
    """
    action_type = "data_exporter"
    display_name = "数据导出"
    description = "数据导出为多种格式"
    DEFAULT_BATCH_SIZE = 1000
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute export operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, format, options.
        
        Returns:
            ActionResult with exported data.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records to export")
        
        format_type = params.get('format', 'json').lower()
        
        if format_type == 'json':
            return self._export_json(records, params)
        elif format_type == 'csv':
            return self._export_csv(records, params)
        elif format_type == 'xml':
            return self._export_xml(records, params)
        elif format_type == 'tsv':
            return self._export_tsv(records, params)
        elif format_type == 'base64':
            return self._export_base64(records, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported format: {format_type}"
            )
    
    def _export_json(
        self,
        records: List[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export records as JSON."""
        pretty = params.get('pretty', False)
        indent = params.get('indent', 2) if pretty else None
        sort_keys = params.get('sort_keys', False)
        
        try:
            if isinstance(records, list):
                json_str = json.dumps(records, indent=indent, sort_keys=sort_keys, default=str)
            else:
                json_str = json.dumps(records, indent=indent, sort_keys=sort_keys, default=str)
            
            return ActionResult(
                success=True,
                message=f"Exported {len(records)} records as JSON",
                data={
                    'format': 'json',
                    'data': json_str,
                    'size': len(json_str)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON export failed: {e}"
            )
    
    def _export_csv(
        self,
        records: List[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export records as CSV."""
        if not records:
            return ActionResult(success=False, message="No records to export")
        
        delimiter = params.get('delimiter', ',')
        include_header = params.get('include_header', True)
        quoting = params.get('quoting', 'minimal')
        
        try:
            output = io.StringIO()
            
            all_keys = set()
            for record in records:
                if isinstance(record, dict):
                    all_keys.update(record.keys())
            
            fieldnames = sorted(list(all_keys))
            
            if quoting == 'minimal':
                qu = csv.QUOTE_MINIMAL
            elif quoting == 'all':
                qu = csv.QUOTE_ALL
            elif quoting == 'non_numeric':
                qu = csv.QUOTE_NONNUMERIC
            else:
                qu = csv.QUOTE_ALL
            
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter=delimiter,
                quoting=qu
            )
            
            if include_header:
                writer.writeheader()
            
            for record in records:
                if isinstance(record, dict):
                    writer.writerow(record)
                else:
                    writer.writerow({})
            
            csv_str = output.getvalue()
            
            return ActionResult(
                success=True,
                message=f"Exported {len(records)} records as CSV",
                data={
                    'format': 'csv',
                    'data': csv_str,
                    'size': len(csv_str),
                    'record_count': len(records)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV export failed: {e}"
            )
    
    def _export_xml(
        self,
        records: List[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export records as XML."""
        root_element = params.get('root_element', 'root')
        record_element = params.get('record_element', 'record')
        
        try:
            lines = ['<?xml version="1.0" encoding="UTF-8"?>']
            lines.append(f'<{root_element}>')
            
            for record in records:
                lines.append(f'  <{record_element}>')
                
                if isinstance(record, dict):
                    for key, value in record.items():
                        safe_key = str(key).replace(' ', '_')
                        escaped_value = self._xml_escape(str(value))
                        lines.append(f'    <{safe_key}>{escaped_value}</{safe_key}>')
                
                lines.append(f'  </{record_element}>')
            
            lines.append(f'</{root_element}>')
            
            xml_str = '\n'.join(lines)
            
            return ActionResult(
                success=True,
                message=f"Exported {len(records)} records as XML",
                data={
                    'format': 'xml',
                    'data': xml_str,
                    'size': len(xml_str)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML export failed: {e}"
            )
    
    def _export_tsv(
        self,
        records: List[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export records as TSV."""
        if not records:
            return ActionResult(success=False, message="No records to export")
        
        include_header = params.get('include_header', True)
        
        try:
            all_keys = set()
            for record in records:
                if isinstance(record, dict):
                    all_keys.update(record.keys())
            
            fieldnames = sorted(list(all_keys))
            
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter='\t'
            )
            
            if include_header:
                writer.writeheader()
            
            for record in records:
                if isinstance(record, dict):
                    writer.writerow(record)
            
            tsv_str = output.getvalue()
            
            return ActionResult(
                success=True,
                message=f"Exported {len(records)} records as TSV",
                data={
                    'format': 'tsv',
                    'data': tsv_str,
                    'size': len(tsv_str)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"TSV export failed: {e}"
            )
    
    def _export_base64(
        self,
        records: List[Dict[str, Any]],
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export records as base64-encoded JSON."""
        inner_format = params.get('inner_format', 'json')
        
        if inner_format == 'json':
            inner = json.dumps(records, default=str)
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported inner format: {inner_format}"
            )
        
        encoded = base64.b64encode(inner.encode('utf-8')).decode('utf-8')
        
        return ActionResult(
            success=True,
            message=f"Exported {len(records)} records as base64",
            data={
                'format': 'base64',
                'data': encoded,
                'size': len(encoded)
            }
        )
    
    def _xml_escape(self, value: str) -> str:
        """Escape special XML characters."""
        return (
            value.replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&apos;')
        )
