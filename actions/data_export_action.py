"""Data export action module for RabAI AutoClick.

Provides data export operations to various formats including
JSON, CSV, XML, Excel, and database exports.
"""

import time
import json
import csv
import io
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JsonExporterAction(BaseAction):
    """Export data to JSON format.
    
    Exports records to JSON with configurable formatting,
    indentation, and array vs objects structure.
    """
    action_type = "json_exporter"
    display_name = "JSON导出"
    description = "导出数据为JSON格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data to JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, output_path, pretty,
                   indent, orient (records|index|columns|split).
        
        Returns:
            ActionResult with export result.
        """
        data = params.get('data', [])
        output_path = params.get('output_path', '')
        pretty = params.get('pretty', True)
        indent = params.get('indent', 2)
        orient = params.get('orient', 'records')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if orient == 'records':
            output_data = data
        elif orient == 'index':
            output_data = {str(i): row for i, row in enumerate(data)}
        elif orient == 'columns':
            if data:
                keys = list(data[0].keys()) if isinstance(data[0], dict) else []
                output_data = {k: [row.get(k) if isinstance(row, dict) else None for row in data] for k in keys}
            else:
                output_data = {}
        else:
            output_data = data

        json_str = json.dumps(output_data, indent=indent if pretty else None, ensure_ascii=False)

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(json_str)
                return ActionResult(
                    success=True,
                    message=f"Exported {len(data)} records to {output_path}",
                    data={
                        'path': output_path,
                        'record_count': len(data),
                        'bytes': len(json_str)
                    },
                    duration=time.time() - start_time
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to write file: {str(e)}"
                )

        return ActionResult(
            success=True,
            message=f"Exported {len(data)} records as JSON",
            data={
                'json': json_str,
                'record_count': len(data),
                'bytes': len(json_str)
            },
            duration=time.time() - start_time
        )


class CsvExporterAction(BaseAction):
    """Export data to CSV format.
    
    Exports records to CSV with configurable delimiter,
    quoting, and header options.
    """
    action_type = "csv_exporter"
    display_name = "CSV导出"
    description = "导出数据为CSV格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data to CSV.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, output_path, delimiter,
                   quote_char, include_header, columns.
        
        Returns:
            ActionResult with export result.
        """
        data = params.get('data', [])
        output_path = params.get('output_path', '')
        delimiter = params.get('delimiter', ',')
        quote_char = params.get('quote_char', '"')
        include_header = params.get('include_header', True)
        columns = params.get('columns', [])
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not columns and data and isinstance(data[0], dict):
            columns = list(data[0].keys())

        output = io.StringIO()
        writer = csv.writer(output, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_MINIMAL)

        if include_header:
            writer.writerow(columns)

        for row in data:
            if isinstance(row, dict):
                writer.writerow([row.get(c, '') for c in columns])
            else:
                writer.writerow([row])

        csv_str = output.getvalue()

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8', newline='') as f:
                    f.write(csv_str)
                return ActionResult(
                    success=True,
                    message=f"Exported {len(data)} records to {output_path}",
                    data={
                        'path': output_path,
                        'record_count': len(data),
                        'bytes': len(csv_str)
                    },
                    duration=time.time() - start_time
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to write file: {str(e)}"
                )

        return ActionResult(
            success=True,
            message=f"Exported {len(data)} records as CSV",
            data={
                'csv': csv_str,
                'record_count': len(data),
                'bytes': len(csv_str)
            },
            duration=time.time() - start_time
        )


class XmlExporterAction(BaseAction):
    """Export data to XML format.
    
    Exports records to XML with configurable root element,
    row element name, and attribute mapping.
    """
    action_type = "xml_exporter"
    display_name = "XML导出"
    description = "导出数据为XML格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data to XML.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, output_path, root_element,
                   row_element, pretty.
        
        Returns:
            ActionResult with export result.
        """
        import xml.etree.ElementTree as ET

        data = params.get('data', [])
        output_path = params.get('output_path', '')
        root_element = params.get('root_element', 'data')
        row_element = params.get('row_element', 'row')
        pretty = params.get('pretty', True)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        root = ET.Element(root_element)
        for row in data:
            if isinstance(row, dict):
                row_el = ET.SubElement(root, row_element)
                for key, value in row.items():
                    child = ET.SubElement(row_el, str(key))
                    child.text = str(value) if value is not None else ''
            else:
                row_el = ET.SubElement(root, row_element)
                row_el.text = str(row)

        xml_str = ET.tostring(root, encoding='unicode')

        if pretty:
            import xml.dom.minidom
            dom = xml.dom.minidom.parseString(xml_str)
            xml_str = dom.toprettyxml(indent='  ')

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(xml_str)
                return ActionResult(
                    success=True,
                    message=f"Exported {len(data)} records to {output_path}",
                    data={
                        'path': output_path,
                        'record_count': len(data),
                        'bytes': len(xml_str)
                    },
                    duration=time.time() - start_time
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to write file: {str(e)}"
                )

        return ActionResult(
            success=True,
            message=f"Exported {len(data)} records as XML",
            data={
                'xml': xml_str,
                'record_count': len(data),
                'bytes': len(xml_str)
            },
            duration=time.time() - start_time
        )


class ParquetExporterAction(BaseAction):
    """Export data to Parquet format.
    
    Exports records to Parquet columnar format with
    compression options.
    """
    action_type = "parquet_exporter"
    display_name = "Parquet导出"
    description = "导出数据为Parquet格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Export data to Parquet.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, output_path, compression
                   (snappy|gzip|none), engine.
        
        Returns:
            ActionResult with export result.
        """
        import tempfile

        data = params.get('data', [])
        output_path = params.get('output_path', '')
        compression = params.get('compression', 'snappy')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not output_path:
            output_path = tempfile.mktemp(suffix='.parquet')

        try:
            import pandas as pd
            import pyarrow.parquet as pq

            df = pd.DataFrame(data)
            df.to_parquet(output_path, compression=compression, engine='pyarrow')

            import os
            file_size = os.path.getsize(output_path)

            return ActionResult(
                success=True,
                message=f"Exported {len(data)} records to {output_path}",
                data={
                    'path': output_path,
                    'record_count': len(data),
                    'bytes': file_size,
                    'compression': compression
                },
                duration=time.time() - start_time
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="pandas and pyarrow required for Parquet export"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Parquet export failed: {str(e)}"
            )
