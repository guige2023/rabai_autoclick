"""ORC action module for RabAI AutoClick.

Provides actions for reading, writing, and manipulating ORC format data.
Supports schema evolution and data type handling.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

try:
    import pyorc
    HAS_ORC = True
except ImportError:
    HAS_ORC = False


class OrcReadAction(BaseAction):
    """Read data from ORC format files.
    
    Parses ORC files with schema support.
    """
    action_type = "orc_read"
    display_name = "读取ORC"
    description = "从ORC文件读取数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read ORC file.
        
        Args:
            context: Execution context.
            params: Dict with keys: file_path, columns, limit,
                   stripe_index.
        
        Returns:
            ActionResult with parsed records.
        """
        if not HAS_ORC:
            return ActionResult(
                success=False,
                message="pyorc library not installed. Run: pip install pyorc"
            )

        file_path = params.get('file_path', '')
        columns = params.get('columns', None)
        limit = params.get('limit', 0)
        stripe_index = params.get('stripe_index', None)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            records = []
            count = 0

            with open(file_path, 'rb') as f:
                reader = pyorc.Reader(f)
                schema = reader.schema
                
                if stripe_index is not None:
                    reader = pyorc.Reader(f, stripe=str(stripe_index)

                for row in reader:
                    if columns:
                        if isinstance(row, tuple):
                            row = {name: row[i] for i, name in enumerate(schema.names) if name in columns}
                        elif isinstance(row, dict):
                            row = {k: v for k, v in row.items() if k in columns}
                    
                    records.append(row)
                    count += 1
                    
                    if limit > 0 and count >= limit:
                        break

            return ActionResult(
                success=True,
                message=f"Read {len(records)} records",
                data={
                    'records': records,
                    'count': len(records),
                    'schema': schema.names,
                    'file_path': file_path
                }
            )

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {file_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to read ORC: {str(e)}")


class OrcWriteAction(BaseAction):
    """Write data to ORC format files.
    
    Serializes records with schema support.
    """
    action_type = "orc_write"
    display_name = "写入ORC"
    description = "写入数据到ORC文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Write ORC file.
        
        Args:
            context: Execution context.
            params: Dict with keys: file_path, records, schema,
                   compression, batch_size.
        
        Returns:
            ActionResult with write status.
        """
        if not HAS_ORC:
            return ActionResult(
                success=False,
                message="pyorc library not installed. Run: pip install pyorc"
            )

        file_path = params.get('file_path', '')
        records = params.get('records', [])
        schema = params.get('schema', None)
        compression = params.get('compression', 'zlib')
        batch_size = params.get('batch_size', 1024)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not records:
            return ActionResult(success=False, message="records list is required")

        try:
            os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

            if schema:
                if isinstance(schema, str):
                    schema_obj = pyorc.parse_schema(schema)
                else:
                    schema_obj = pyorc.parse_schema(json.dumps(schema))
            else:
                if records:
                    schema_obj = self._infer_schema(records[0])
                else:
                    return ActionResult(success=False, message="schema or records required")

            with open(file_path, 'wb') as f:
                writer = pyorc.Writer(f, schema_obj, compression=compression)
                
                with writer:
                    for record in records:
                        writer.write(record)
                
                if writer:
                    writer.close()

            file_size = os.path.getsize(file_path)

            return ActionResult(
                success=True,
                message=f"Wrote {len(records)} records",
                data={
                    'file_path': file_path,
                    'record_count': len(records),
                    'file_size': file_size,
                    'compression': compression
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Failed to write ORC: {str(e)}")

    def _infer_schema(self, record: Dict) -> 'Schema':
        """Infer ORC schema from record."""
        struct_fields = []
        for key, value in record.items():
            if isinstance(value, bool):
                orc_type = 'boolean'
            elif isinstance(value, int):
                orc_type = 'bigint'
            elif isinstance(value, float):
                orc_type = 'double'
            elif isinstance(value, str):
                orc_type = 'string'
            elif isinstance(value, bytes):
                orc_type = 'binary'
            elif isinstance(value, list):
                orc_type = 'array<string>'
            elif isinstance(value, dict):
                orc_type = 'map<string,string>'
            else:
                orc_type = 'string'
            struct_fields.append(f"{key}:{orc_type}")
        
        return pyorc.parse_schema(f"struct<{','.join(struct_fields)}>")


class OrcSchemaAction(BaseAction):
    """Work with ORC schemas.
    
    Handles schema parsing, validation, and conversion.
    """
    action_type = "orc_schema"
    display_name = "ORC Schema"
    description = "ORC Schema处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process ORC schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: schema_string, operation,
                   file_path.
        
        Returns:
            ActionResult with schema info.
        """
        if not HAS_ORC:
            return ActionResult(
                success=False,
                message="pyorc library not installed. Run: pip install pyorc"
            )

        schema_string = params.get('schema_string', '')
        operation = params.get('operation', 'parse')
        file_path = params.get('file_path', '')

        try:
            if operation == 'parse':
                if file_path:
                    with open(file_path, 'rb') as f:
                        reader = pyorc.Reader(f)
                        schema = reader.schema
                        return ActionResult(
                            success=True,
                            message=f"Schema: {schema}",
                            data={
                                'schema': str(schema),
                                'names': schema.names,
                                'types': [str(schema[name]) for name in schema.names]
                            }
                        )
                elif schema_string:
                    schema = pyorc.parse_schema(schema_string)
                    return ActionResult(
                        success=True,
                        message=f"Schema parsed: {schema}",
                        data={
                            'schema': str(schema),
                            'names': schema.names
                        }
                    )
                return ActionResult(success=False, message="schema_string or file_path required")

            elif operation == 'to_json':
                if file_path:
                    with open(file_path, 'rb') as f:
                        reader = pyorc.Reader(f)
                        schema = reader.schema
                        return ActionResult(
                            success=True,
                            message="Schema converted to JSON",
                            data={'json': json.dumps({'names': schema.names})}
                        )
                return ActionResult(success=False, message="file_path required")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Schema error: {str(e)}")


class OrcStatsAction(BaseAction):
    """Get ORC file statistics.
    
    Returns file metadata and statistics.
    """
    action_type = "orc_stats"
    display_name = "ORC统计"
    description = "ORC文件统计"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get ORC stats.
        
        Args:
            context: Execution context.
            params: Dict with keys: file_path.
        
        Returns:
            ActionResult with file statistics.
        """
        if not HAS_ORC:
            return ActionResult(
                success=False,
                message="pyorc library not installed. Run: pip install pyorc"
            )

        file_path = params.get('file_path', '')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            with open(file_path, 'rb') as f:
                reader = pyorc.Reader(f)
                schema = reader.schema
                
                file_size = os.path.getsize(file_path)
                
                stats = {
                    'file_path': file_path,
                    'file_size': file_size,
                    'file_size_mb': round(file_size / 1024 / 1024, 2),
                    'schema': str(schema),
                    'column_names': schema.names,
                    'compression': str(reader.compression),
                    'writer_version': str(reader.writer_version),
                }

                try:
                    stats['row_count'] = reader.row_count
                except:
                    pass

                try:
                    stats['stripe_count'] = reader.stripe_count
                except:
                    pass

                return ActionResult(
                    success=True,
                    message=f"Stats for {file_path}",
                    data=stats
                )

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {file_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Stats failed: {str(e)}")


class OrcConvertAction(BaseAction):
    """Convert data to/from ORC format.
    
    Handles format conversion for ORC data.
    """
    action_type = "orc_convert"
    display_name = "ORC转换"
    description = "ORC格式转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert to/from ORC.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, schema.
                   format: 'from_json', 'to_json', 'from_csv'.
        
        Returns:
            ActionResult with converted data.
        """
        if not HAS_ORC:
            return ActionResult(
                success=False,
                message="pyorc library not installed. Run: pip install pyorc"
            )

        data = params.get('data', [])
        format_type = params.get('format', 'from_json')
        schema = params.get('schema', None)

        try:
            if format_type == 'from_json':
                if isinstance(data, str):
                    data = json.loads(data)
                
                if not isinstance(data, list):
                    data = [data]
                
                return ActionResult(
                    success=True,
                    message=f"Converted JSON to ORC-ready records",
                    data={'records': data, 'count': len(data), 'schema': schema}
                )

            elif format_type == 'to_json':
                if isinstance(data, list) and data:
                    return ActionResult(
                        success=True,
                        message=f"Converted {len(data)} ORC records to JSON",
                        data={'json': json.dumps(data, ensure_ascii=False)}
                    )
                return ActionResult(success=False, message="No data to convert")

            elif format_type == 'from_csv':
                if isinstance(data, str):
                    import csv
                    import io
                    reader = csv.DictReader(io.StringIO(data))
                    data = list(reader)
                
                return ActionResult(
                    success=True,
                    message=f"Converted CSV to {len(data)} records",
                    data={'records': data, 'count': len(data)}
                )

            return ActionResult(success=False, message=f"Unknown format: {format_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Conversion failed: {str(e)}")
