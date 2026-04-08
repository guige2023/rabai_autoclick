"""Avro action module for RabAI AutoClick.

Provides actions for reading, writing, and manipulating Avro format data.
Supports schema validation and data serialization.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

try:
    import avro
    from avro.schema import parse
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter
    HAS_AVRO = True
except ImportError:
    HAS_AVRO = False


class AvroReadAction(BaseAction):
    """Read data from Avro format files.
    
    Parses Avro files with schema support.
    """
    action_type = "avro_read"
    display_name = "读取Avro"
    description = "从Avro文件读取数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read Avro file.
        
        Args:
            context: Execution context.
            params: Dict with keys: file_path, schema_path, limit,
                   as_json.
        
        Returns:
            ActionResult with parsed records.
        """
        if not HAS_AVRO:
            return ActionResult(
                success=False,
                message="avro library not installed. Run: pip install avro"
            )

        file_path = params.get('file_path', '')
        schema_path = params.get('schema_path', '')
        limit = params.get('limit', 0)
        as_json = params.get('as_json', True)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            records = []
            count = 0

            with open(file_path, 'rb') as f:
                reader = DataFileReader(f, DatumReader())
                
                if as_json:
                    for record in reader:
                        records.append(record)
                        count += 1
                        if limit > 0 and count >= limit:
                            break
                else:
                    schema = reader.GetSchema()
                    records = {'schema': str(schema), 'record_count': reader.GetBlockCount()}
                
                reader.close()

            return ActionResult(
                success=True,
                message=f"Read {len(records)} records",
                data={
                    'records': records,
                    'count': len(records),
                    'file_path': file_path
                }
            )

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {file_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to read Avro: {str(e)}")


class AvroWriteAction(BaseAction):
    """Write data to Avro format files.
    
    Serializes records with schema support.
    """
    action_type = "avro_write"
    display_name = "写入Avro"
    description = "写入数据到Avro文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Write Avro file.
        
        Args:
            context: Execution context.
            params: Dict with keys: file_path, records, schema_json,
                   codec.
        
        Returns:
            ActionResult with write status.
        """
        if not HAS_AVRO:
            return ActionResult(
                success=False,
                message="avro library not installed. Run: pip install avro"
            )

        file_path = params.get('file_path', '')
        records = params.get('records', [])
        schema_json = params.get('schema_json', '')
        codec = params.get('codec', 'null')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not records:
            return ActionResult(success=False, message="records list is required")

        try:
            if schema_json:
                if isinstance(schema_json, str):
                    schema = parse(schema_json)
                else:
                    schema = parse(json.dumps(schema_json))
            else:
                if records:
                    schema = self._infer_schema(records[0])
                else:
                    return ActionResult(success=False, message="schema_json or records required")

            os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

            with open(file_path, 'wb') as f:
                writer = DataFileWriter(f, DatumWriter(), schema, codec=codec)
                
                for record in records:
                    writer.append(record)
                
                writer.close()

            file_size = os.path.getsize(file_path)

            return ActionResult(
                success=True,
                message=f"Wrote {len(records)} records",
                data={
                    'file_path': file_path,
                    'record_count': len(records),
                    'file_size': file_size,
                    'codec': codec
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Failed to write Avro: {str(e)}")

    def _infer_schema(self, record: Dict) -> 'Schema':
        """Infer Avro schema from record."""
        from avro.schema import Record, Field, String, Int, Long, Float, Double, Boolean, Array, Map
        
        fields = []
        for key, value in record.items():
            if isinstance(value, str):
                fields.append(Field(name=key, type=String()))
            elif isinstance(value, bool):
                fields.append(Field(name=key, type=Boolean()))
            elif isinstance(value, int):
                fields.append(Field(name=key, type=Long()))
            elif isinstance(value, float):
                fields.append(Field(name=key, type=Double()))
            elif isinstance(value, list):
                fields.append(Field(name=key, type=Array(String())))
            elif isinstance(value, dict):
                fields.append(Field(name=key, type=Map(String())))
            else:
                fields.append(Field(name=key, type=String()))
        
        return Record(name='GeneratedRecord', fields=fields)


class AvroSchemaAction(BaseAction):
    """Validate and manipulate Avro schemas.
    
    Handles schema parsing, validation, and compatibility checking.
    """
    action_type = "avro_schema"
    display_name = "Avro Schema"
    description = "Avro Schema验证和操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: schema_json, operation,
                   compare_schema.
        
        Returns:
            ActionResult with validation result.
        """
        if not HAS_AVRO:
            return ActionResult(
                success=False,
                message="avro library not installed. Run: pip install avro"
            )

        schema_json = params.get('schema_json', '')
        operation = params.get('operation', 'validate')
        compare_schema = params.get('compare_schema', '')

        if not schema_json:
            return ActionResult(success=False, message="schema_json is required")

        try:
            if isinstance(schema_json, str):
                schema = parse(schema_json)
            else:
                schema = parse(json.dumps(schema_json))

            if operation == 'validate':
                return ActionResult(
                    success=True,
                    message="Schema is valid",
                    data={'schema': str(schema), 'name': schema.name}
                )

            elif operation == 'to_json':
                return ActionResult(
                    success=True,
                    message="Schema converted to JSON",
                    data={'json': schema.to_json()}
                )

            elif operation == 'compare' and compare_schema:
                if isinstance(compare_schema, str):
                    compare = parse(compare_schema)
                else:
                    compare = parse(json.dumps(compare_schema))
                
                compatible = self._check_compatibility(schema, compare)
                
                return ActionResult(
                    success=compatible,
                    message="Schemas are compatible" if compatible else "Schemas are incompatible",
                    data={'compatible': compatible}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Schema error: {str(e)}")

    def _check_compatibility(self, schema1, schema2) -> bool:
        """Check schema compatibility."""
        return str(schema1) == str(schema2)


class AvroConvertAction(BaseAction):
    """Convert data to/from Avro format.
    
    Handles format conversion for Avro data.
    """
    action_type = "avro_convert"
    display_name = "Avro转换"
    description = "Avro格式转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert data to Avro.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, schema_json.
                   format: 'from_json', 'to_json', 'from_csv', 'to_csv'.
        
        Returns:
            ActionResult with converted data.
        """
        if not HAS_AVRO:
            return ActionResult(
                success=False,
                message="avro library not installed. Run: pip install avro"
            )

        data = params.get('data', [])
        format_type = params.get('format', 'from_json')
        schema_json = params.get('schema_json', '')

        try:
            if format_type == 'from_json':
                if isinstance(data, str):
                    data = json.loads(data)
                
                if not isinstance(data, list):
                    data = [data]
                
                return ActionResult(
                    success=True,
                    message=f"Converted JSON to Avro-ready records",
                    data={'records': data, 'count': len(data)}
                )

            elif format_type == 'to_json':
                return ActionResult(
                    success=True,
                    message=f"Converted Avro records to JSON",
                    data={'json': json.dumps(data, ensure_ascii=False)}
                )

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

            elif format_type == 'to_csv':
                import csv
                import io
                
                if isinstance(data, list) and data:
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                    
                    return ActionResult(
                        success=True,
                        message=f"Converted {len(data)} records to CSV",
                        data={'csv': output.getvalue()}
                    )
                
                return ActionResult(success=False, message="No data to convert")

            return ActionResult(success=False, message=f"Unknown format: {format_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Conversion failed: {str(e)}")
