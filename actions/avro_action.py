"""Avro action module for RabAI AutoClick.

Provides Apache Avro serialization operations:
- AvroEncoder: Encode data to Avro binary format
- AvroDecoder: Decode Avro binary data
- AvroSchemaLoader: Load and validate Avro schemas
- AvroSchemaRegistry: Schema registry integration
- AvroFileOps: Read/write Avro files with schema
"""

from __future__ import annotations

import json
import io
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import avro
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False


@dataclass
class AvroSchema:
    """Avro schema container."""
    name: str
    namespace: str = ""
    doc: str = ""
    schema_json: Dict[str, Any] = field(default_factory=dict)
    fields: List[Dict[str, Any]] = field(default_factory=list)


class AvroEncoderAction(BaseAction):
    """Encode data to Avro binary format."""
    action_type = "avro_encoder"
    display_name = "Avro编码"
    description = "将数据编码为Avro二进制格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not AVRO_AVAILABLE:
            return ActionResult(success=False, message="avro not installed: pip install avro")

        try:
            data = params.get("data", None)
            schema = params.get("schema", None)
            schema_json = params.get("schema_json", None)
            as_binary = params.get("as_binary", True)

            if data is None:
                return ActionResult(success=False, message="data is required")
            if not schema and not schema_json:
                return ActionResult(success=False, message="schema or schema_json is required")

            if isinstance(schema, str):
                if os.path.exists(schema):
                    with open(schema, "r") as f:
                        schema_obj = avro.schema.parse(f.read())
                else:
                    schema_obj = avro.schema.parse(schema)
            elif isinstance(schema_json, dict):
                schema_obj = avro.schema.parse(json.dumps(schema_json))
            else:
                return ActionResult(success=False, message="Invalid schema format")

            writer = DatumWriter(schema_obj)
            output = io.BytesIO()
            encoder = BinaryEncoder(output)
            writer.write(data, encoder)
            binary_data = output.getvalue()

            if as_binary:
                return ActionResult(
                    success=True,
                    message=f"Encoded {len(data)} records",
                    data={"binary": binary_data.hex(), "size": len(binary_data)}
                )

            return ActionResult(
                success=True,
                message=f"Encoded data: {len(binary_data)} bytes",
                data={"binary": binary_data, "hex": binary_data.hex()}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Encoding error: {str(e)}")


class AvroDecoderAction(BaseAction):
    """Decode Avro binary data."""
    action_type = "avro_decoder"
    display_name = "Avro解码"
    description = "解码Avro二进制数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not AVRO_AVAILABLE:
            return ActionResult(success=False, message="avro not installed: pip install avro")

        try:
            binary_data = params.get("binary_data", None)
            hex_data = params.get("hex_data", None)
            schema = params.get("schema", None)
            schema_json = params.get("schema_json", None)

            if not binary_data and not hex_data:
                return ActionResult(success=False, message="binary_data or hex_data is required")
            if not schema and not schema_json:
                return ActionResult(success=False, message="schema or schema_json is required")

            if hex_data:
                binary_data = bytes.fromhex(hex_data)

            if isinstance(schema, str):
                if os.path.exists(schema):
                    with open(schema, "r") as f:
                        schema_obj = avro.schema.parse(f.read())
                else:
                    schema_obj = avro.schema.parse(schema)
            elif isinstance(schema_json, dict):
                schema_obj = avro.schema.parse(json.dumps(schema_json))
            else:
                return ActionResult(success=False, message="Invalid schema format")

            reader = DatumReader(schema_obj)
            input_stream = io.BytesIO(binary_data)
            decoder = BinaryDecoder(input_stream)

            records = []
            input_stream.seek(0)
            try:
                while input_stream.tell() < len(binary_data):
                    record = reader.read(decoder)
                    records.append(record)
            except Exception:
                pass

            return ActionResult(
                success=True,
                message=f"Decoded {len(records)} records",
                data={"records": records, "count": len(records)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Decoding error: {str(e)}")


class AvroSchemaLoaderAction(BaseAction):
    """Load and validate Avro schemas."""
    action_type = "avro_schema_loader"
    display_name = "Avro Schema加载"
    description = "加载并验证Avro Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not AVRO_AVAILABLE:
            return ActionResult(success=False, message="avro not installed: pip install avro")

        try:
            schema_source = params.get("schema_source", None)
            schema_json = params.get("schema_json", None)
            validate = params.get("validate", True)

            if not schema_source and not schema_json:
                return ActionResult(success=False, message="schema_source or schema_json is required")

            if schema_source:
                if os.path.exists(schema_source):
                    with open(schema_source, "r") as f:
                        schema_str = f.read()
                else:
                    schema_str = schema_source
            else:
                schema_str = json.dumps(schema_json)

            try:
                schema_obj = avro.schema.parse(schema_str)
            except Exception as e:
                return ActionResult(success=False, message=f"Invalid Avro schema: {str(e)}")

            schema_info = {
                "name": str(schema_obj.name) if schema_obj.name else "",
                "namespace": schema_obj.namespace or "",
                "type": str(schema_obj.type),
                "json": json.loads(str(schema_obj)),
                "fields": [],
            }

            if hasattr(schema_obj, "fields"):
                for field in schema_obj.fields:
                    schema_info["fields"].append({
                        "name": field.name,
                        "type": str(field.type),
                        "default": field.default if hasattr(field, "default") else None,
                    })

            if validate:
                if not schema_info["name"]:
                    return ActionResult(success=False, message="Schema validation failed: missing name")

            return ActionResult(
                success=True,
                message=f"Schema loaded: {schema_info['name']}",
                data=schema_info
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class AvroFileWriterAction(BaseAction):
    """Write Avro records to files."""
    action_type = "avro_file_writer"
    display_name = "Avro文件写入"
    description = "将记录写入Avro文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not AVRO_AVAILABLE:
            return ActionResult(success=False, message="avro not installed: pip install avro")

        try:
            file_path = params.get("file_path", "")
            records = params.get("records", [])
            schema_source = params.get("schema_source", None)
            schema_json = params.get("schema_json", None)
            overwrite = params.get("overwrite", True)

            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            if not records:
                return ActionResult(success=False, message="records list is required")
            if not schema_source and not schema_json:
                return ActionResult(success=False, message="schema_source or schema_json is required")

            if not overwrite and os.path.exists(file_path):
                return ActionResult(success=False, message=f"File exists: {file_path}")

            if schema_source:
                if os.path.exists(schema_source):
                    with open(schema_source, "r") as f:
                        schema_str = f.read()
                else:
                    schema_str = schema_source
            else:
                schema_str = json.dumps(schema_json)

            schema_obj = avro.schema.parse(schema_str)
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

            writer = DataFileWriter(open(file_path, "wb"), DatumWriter(), schema_obj)
            try:
                for record in records:
                    writer.append(record)
            finally:
                writer.close()

            file_size = os.path.getsize(file_path)
            return ActionResult(
                success=True,
                message=f"Wrote {len(records)} records to {file_path}",
                data={"file_path": file_path, "records": len(records), "file_size": file_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class AvroFileReaderAction(BaseAction):
    """Read Avro records from files."""
    action_type = "avro_file_reader"
    display_name = "Avro文件读取"
    description = "从Avro文件读取记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not AVRO_AVAILABLE:
            return ActionResult(success=False, message="avro not installed: pip install avro")

        try:
            file_path = params.get("file_path", "")
            limit = params.get("limit", None)
            skip = params.get("skip", 0)

            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")

            reader = DataFileReader(open(file_path, "rb"), DatumReader())
            schema = reader.GetWriter()

            records = []
            for i, record in enumerate(reader):
                if i < skip:
                    continue
                records.append(record)
                if limit and i >= skip + limit:
                    break

            reader.close()
            return ActionResult(
                success=True,
                message=f"Read {len(records)} records from {file_path}",
                data={"records": records, "count": len(records), "schema": str(schema)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
