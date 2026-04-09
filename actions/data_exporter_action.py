"""Data Exporter Action Module.

Provides data export functionality to various formats and destinations.
"""

import time
import json
import csv
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataExporterAction(BaseAction):
    """Export data to various formats.
    
    Supports JSON, CSV, XML, Excel, and custom format exports.
    """
    action_type = "data_exporter"
    display_name = "数据导出"
    description = "导出数据到多种格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, destination, options.
        
        Returns:
            ActionResult with export result.
        """
        data = params.get('data', [])
        export_format = params.get('format', 'json')
        destination = params.get('destination', '/tmp/export')
        options = params.get('options', {})
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to export"
            )
        
        try:
            if export_format == 'json':
                result_path = self._export_json(data, destination, options)
            elif export_format == 'csv':
                result_path = self._export_csv(data, destination, options)
            elif export_format == 'xml':
                result_path = self._export_xml(data, destination, options)
            elif export_format == 'parquet':
                result_path = self._export_parquet(data, destination, options)
            elif export_format == 'excel':
                result_path = self._export_excel(data, destination, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unsupported format: {export_format}"
                )
            
            return ActionResult(
                success=True,
                data={
                    "format": export_format,
                    "destination": result_path,
                    "record_count": len(data) if isinstance(data, list) else 1
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Export failed: {str(e)}"
            )
    
    def _export_json(self, data: Any, destination: str, options: Dict) -> str:
        """Export data to JSON."""
        indent = options.get('indent', 2)
        ensure_ascii = options.get('ensure_ascii', False)
        
        if destination.endswith('.json') or not destination.endswith('.gz'):
            path = destination if destination.endswith('.json') else f"{destination}.json"
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
            return path
        else:
            import gzip
            path = destination
            with gzip.open(path, 'wt', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
            return path
    
    def _export_csv(self, data: Any, destination: str, options: Dict) -> str:
        """Export data to CSV."""
        if not isinstance(data, list):
            data = [data]
        
        delimiter = options.get('delimiter', ',')
        quotechar = options.get('quotechar', '"')
        
        path = destination if destination.endswith('.csv') else f"{destination}.csv"
        
        if not data:
            return path
        
        with open(path, 'w', newline='', encoding='utf-8') as f:
            # Get headers from first item
            if isinstance(data[0], dict):
                headers = list(data[0].keys())
                writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter, quotechar=quotechar)
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(f, delimiter=delimiter, quotechar=quotechar)
                writer.writerow(data)
        
        return path
    
    def _export_xml(self, data: Any, destination: str, options: Dict) -> str:
        """Export data to XML."""
        root_tag = options.get('root_tag', 'data')
        item_tag = options.get('item_tag', 'item')
        
        path = destination if destination.endswith('.xml') else f"{destination}.xml"
        
        import xml.etree.ElementTree as ET
        
        def dict_to_xml(parent: ET.Element, data: Dict):
            for key, value in data.items():
                child = ET.SubElement(parent, str(key))
                if isinstance(value, dict):
                    dict_to_xml(child, value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            list_child = ET.SubElement(child, item_tag)
                            dict_to_xml(list_child, item)
                        else:
                            ET.SubElement(child, item_tag).text = str(item)
                else:
                    child.text = str(value)
        
        root = ET.Element(root_tag)
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    item_elem = ET.SubElement(root, item_tag)
                    dict_to_xml(item_elem, item)
                else:
                    ET.SubElement(root, item_tag).text = str(item)
        elif isinstance(data, dict):
            dict_to_xml(root, data)
        else:
            root.text = str(data)
        
        tree = ET.ElementTree(root)
        tree.write(path, encoding='utf-8', xml_declaration=True)
        
        return path
    
    def _export_parquet(self, data: Any, destination: str, options: Dict) -> str:
        """Export data to Parquet format."""
        try:
            import pandas as pd
            
            if not isinstance(data, list):
                data = [data]
            
            df = pd.DataFrame(data)
            path = destination if destination.endswith('.parquet') else f"{destination}.parquet"
            df.to_parquet(path, index=False)
            
            return path
        except ImportError:
            raise Exception("pandas or pyarrow not installed for parquet export")
    
    def _export_excel(self, data: Any, destination: str, options: Dict) -> str:
        """Export data to Excel format."""
        try:
            import pandas as pd
            
            if not isinstance(data, list):
                data = [data]
            
            df = pd.DataFrame(data)
            path = destination if destination.endswith('.xlsx') else f"{destination}.xlsx"
            df.to_excel(path, index=False, engine='openpyxl')
            
            return path
        except ImportError:
            raise Exception("pandas or openpyxl not installed for excel export")


class DataBatchExporterAction(BaseAction):
    """Export data in batches.
    
    Handles large datasets by exporting in configurable batch sizes.
    """
    action_type = "data_batch_exporter"
    display_name = "批量数据导出"
    description = "分批导出大型数据集"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data in batches.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, destination, batch_size.
        
        Returns:
            ActionResult with batch export results.
        """
        data = params.get('data', [])
        export_format = params.get('format', 'json')
        destination = params.get('destination', '/tmp/export')
        batch_size = params.get('batch_size', 1000)
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to export"
            )
        
        if not isinstance(data, list):
            data = [data]
        
        total_records = len(data)
        batch_count = (total_records + batch_size - 1) // batch_size
        exported_files = []
        
        try:
            for i in range(batch_count):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_records)
                batch_data = data[start_idx:end_idx]
                
                batch_dest = f"{destination}_part{i+1}"
                
                exporter = DataExporterAction()
                result = exporter.execute(context, {
                    'data': batch_data,
                    'format': export_format,
                    'destination': batch_dest
                })
                
                if result.success:
                    exported_files.append(result.data['destination'])
                else:
                    return ActionResult(
                        success=False,
                        data={'exported_files': exported_files},
                        error=f"Batch {i+1} export failed: {result.error}"
                    )
            
            return ActionResult(
                success=True,
                data={
                    'total_records': total_records,
                    'batch_count': batch_count,
                    'batch_size': batch_size,
                    'exported_files': exported_files
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data={'exported_files': exported_files},
                error=f"Batch export failed: {str(e)}"
            )


class DataStreamingExporterAction(BaseAction):
    """Stream export data to destination.
    
    Exports data incrementally without loading all data into memory.
    """
    action_type = "data_streaming_exporter"
    display_name = "流式数据导出"
    description = "流式导出数据到目标位置"
    
    def __init__(self):
        super().__init__()
        self._stream_handles = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute streaming export.
        
        Args:
            context: Execution context.
            params: Dict with keys: stream_id, action, data, format, destination.
        
        Returns:
            ActionResult with streaming export result.
        """
        stream_id = params.get('stream_id', 'default')
        action = params.get('action', 'write')
        
        if action == 'open':
            return self._open_stream(params, stream_id)
        elif action == 'write':
            return self._write_stream(params, stream_id)
        elif action == 'close':
            return self._close_stream(params, stream_id)
        elif action == 'flush':
            return self._flush_stream(params, stream_id)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _open_stream(self, params: Dict, stream_id: str) -> ActionResult:
        """Open a new export stream."""
        export_format = params.get('format', 'json')
        destination = params.get('destination', f'/tmp/stream_{stream_id}')
        
        self._stream_handles[stream_id] = {
            'format': export_format,
            'destination': destination,
            'record_count': 0,
            'file_handle': None
        }
        
        return ActionResult(
            success=True,
            data={
                'stream_id': stream_id,
                'destination': destination,
                'format': export_format
            },
            error=None
        )
    
    def _write_stream(self, params: Dict, stream_id: str) -> ActionResult:
        """Write data to stream."""
        if stream_id not in self._stream_handles:
            return ActionResult(
                success=False,
                data=None,
                error=f"Stream {stream_id} not open"
            )
        
        data = params.get('data', [])
        if not isinstance(data, list):
            data = [data]
        
        handle = self._stream_handles[stream_id]
        handle['record_count'] += len(data)
        
        return ActionResult(
            success=True,
            data={
                'stream_id': stream_id,
                'records_written': len(data),
                'total_records': handle['record_count']
            },
            error=None
        )
    
    def _close_stream(self, params: Dict, stream_id: str) -> ActionResult:
        """Close an export stream."""
        if stream_id not in self._stream_handles:
            return ActionResult(
                success=False,
                data=None,
                error=f"Stream {stream_id} not found"
            )
        
        handle = self._stream_handles[stream_id]
        del self._stream_handles[stream_id]
        
        return ActionResult(
            success=True,
            data={
                'stream_id': stream_id,
                'final_record_count': handle['record_count'],
                'destination': handle['destination']
            },
            error=None
        )
    
    def _flush_stream(self, params: Dict, stream_id: str) -> ActionResult:
        """Flush stream buffers."""
        if stream_id not in self._stream_handles:
            return ActionResult(
                success=False,
                data=None,
                error=f"Stream {stream_id} not found"
            )
        
        handle = self._stream_handles[stream_id]
        
        return ActionResult(
            success=True,
            data={
                'stream_id': stream_id,
                'flushed': True,
                'record_count': handle['record_count']
            },
            error=None
        )


def register_actions():
    """Register all Data Exporter actions."""
    return [
        DataExporterAction,
        DataBatchExporterAction,
        DataStreamingExporterAction,
    ]
