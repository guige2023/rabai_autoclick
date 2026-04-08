"""
Avro serialization action for compact, fast binary data format.

This module provides actions for reading and writing Apache Avro files,
supporting schema evolution, compression, and both binary and JSON encoding.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import io
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

try:
    import avro
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False


class AvroEncoding(Enum):
    """Avro encoding types."""
    BINARY = "binary"
    JSON = "json"


class CompressionType(Enum):
    """Supported compression codecs for Avro files."""
    NULL = "null"
    DEFLATE = "deflate"
    SNAPPY = "snappy"
    ZSTANDARD = "zstandard"


@dataclass
class AvroSchema:
    """Represents an Avro schema definition."""
    name: str
    namespace: Optional[str] = None
    doc: Optional[str] = None
    type: str = "record"
    fields: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to Avro-compatible dictionary."""
        schema: Dict[str, Any] = {
            "type": self.type,
            "name": self.name,
        }
        if self.namespace:
            schema["namespace"] = self.namespace
        if self.doc:
            schema["doc"] = self.doc
        if self.fields:
            schema["fields"] = self.fields
        return schema

    def to_json(self) -> str:
        """Serialize schema to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AvroSchema:
        """Create schema from dictionary."""
        return cls(
            name=data.get("name", ""),
            namespace=data.get("namespace"),
            doc=data.get("doc"),
            type=data.get("type", "record"),
            fields=data.get("fields", []),
        )

    @classmethod
    def from_json(cls, json_str: str) -> AvroSchema:
        """Create schema from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class AvroWriteOptions:
    """Options for writing Avro files."""
    schema: Union[str, Dict[str, Any], AvroSchema]
    encoding: AvroEncoding = AvroEncoding.BINARY
    compression: CompressionType = CompressionType.DEFLATE
    codec_args: Optional[Dict[str, Any]] = None
    sync_interval: int = 16 * 1024
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if isinstance(self.schema, str):
            self.schema = json.loads(self.schema)
        if isinstance(self.schema, AvroSchema):
            self.schema = self.schema.to_dict()


@dataclass
class AvroReadOptions:
    """Options for reading Avro files."""
    schema: Optional[Union[str, Dict[str, Any], AvroSchema]] = None
    raw_decoder: bool = False
    seeker: Optional[Any] = None

    def __post_init__(self):
        if self.schema:
            if isinstance(self.schema, str):
                self.schema = json.loads(self.schema)
            elif isinstance(self.schema, AvroSchema):
                self.schema = self.schema.to_dict()


class AvroSerializer:
    """Serialize and deserialize data to/from Avro format."""

    def __init__(self):
        """Initialize the Avro serializer."""
        if not AVRO_AVAILABLE:
            raise ImportError(
                "avro is required for Avro serialization. "
                "Install with: pip install avro"
            )

    def create_schema_from_records(
        self,
        records: List[Dict[str, Any]],
        name: str = "GeneratedSchema",
        namespace: Optional[str] = None,
    ) -> AvroSchema:
        """
        Infer an Avro schema from sample records.

        Args:
            records: List of sample records to infer schema from.
            name: Schema name.
            namespace: Optional namespace.

        Returns:
            AvroSchema object.
        """
        if not records:
            raise ValueError("Cannot infer schema from empty records")

        sample = records[0]
        fields = []

        for key, value in sample.items():
            avro_type = self._infer_avro_type(value)
            fields.append({
                "name": key,
                "type": avro_type,
            })

        return AvroSchema(
            name=name,
            namespace=namespace,
            fields=fields,
        )

    def _infer_avro_type(self, value: Any) -> Any:
        """Infer Avro type from a Python value."""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                return "int"
            elif -9223372036854775808 <= value <= 9223372036854775807:
                return "long"
            else:
                return "long"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, bytes):
            return "bytes"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            if value:
                item_type = self._infer_avro_type(value[0])
                return {"type": "array", "items": item_type}
            return {"type": "array", "items": "string"}
        elif isinstance(value, dict):
            return self._dict_to_record(value)
        else:
            return "string"

    def _dict_to_record(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a dictionary to an Avro record type."""
        fields = []
        for key, value in d.items():
            fields.append({
                "name": key,
                "type": self._infer_avro_type(value),
            })
        return {"type": "record", "name": "Record", "fields": fields}

    def serialize_to_bytes(
        self,
        records: List[Dict[str, Any]],
        schema: Union[str, Dict[str, Any], AvroSchema],
    ) -> bytes:
        """
        Serialize records to Avro binary format (bytes).

        Args:
            records: List of records to serialize.
            schema: Avro schema for the records.

        Returns:
            Bytes containing the Avro-encoded data.
        """
        if isinstance(schema, AvroSchema):
            schema = schema.to_dict()

        schema_obj = avro.schema.parse(json.dumps(schema) if isinstance(schema, dict) else schema)

        writer = DatumWriter(schema_obj)
        output = io.BytesIO()
        encoder = BinaryEncoder(output)
        
        for record in records:
            writer.write(record, encoder)

        return output.getvalue()

    def deserialize_from_bytes(
        self,
        data: bytes,
        schema: Optional[Union[str, Dict[str, Any], AvroSchema]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deserialize Avro binary data to records.

        Args:
            data: Bytes containing Avro-encoded data.
            schema: Optional schema (reads from data if not provided).

        Returns:
            List of deserialized records.
        """
        if isinstance(schema, AvroSchema):
            schema = schema.to_dict()

        input_stream = io.BytesIO(data)

        if schema:
            schema_obj = avro.schema.parse(json.dumps(schema) if isinstance(schema, dict) else schema)
            reader = DatumReader(schema_obj)
            decoder = BinaryDecoder(input_stream)
            records = []
            while input_stream.tell() < len(data):
                try:
                    records.append(reader.read(decoder))
                except Exception:
                    break
            return records
        else:
            reader = DatumReader()
            decoder = BinaryDecoder(input_stream)
            records = []
            while input_stream.tell() < len(data):
                try:
                    records.append(reader.read(decoder))
                except Exception:
                    break
            return records

    def write_file(
        self,
        records: List[Dict[str, Any]],
        output_path: Union[str, Path],
        options: AvroWriteOptions,
    ) -> Dict[str, Any]:
        """
        Write records to an Avro file.

        Args:
            records: List of records to write.
            output_path: Path to the output Avro file.
            options: Write options including schema.

        Returns:
            Dictionary with metadata about the written file.
        """
        output_path = Path(output_path)

        if not records:
            raise ValueError("Cannot write empty records")

        codec_map = {
            CompressionType.NULL: "null",
            CompressionType.DEFLATE: "deflate",
            CompressionType.SNAPPY: "snappy",
            CompressionType.ZSTANDARD: "zstandard",
        }

        try:
            schema_obj = avro.schema.parse(
                json.dumps(options.schema) if isinstance(options.schema, dict) else options.schema
            )

            with open(output_path, "wb") as f:
                writer = DataFileWriter(
                    f,
                    DatumWriter(),
                    schema_obj,
                    codec=codec_map.get(options.compression, "deflate"),
                )

                for record in records:
                    writer.append(record)

                writer.close()

            file_size = output_path.stat().st_size

            return {
                "output_path": str(output_path),
                "file_size_bytes": file_size,
                "num_records": len(records),
                "compression": options.compression.value,
                "encoding": options.encoding.value,
                "schema": json.dumps(options.schema) if isinstance(options.schema, dict) else options.schema,
            }

        except Exception as e:
            raise IOError(f"Failed to write Avro file: {e}") from e

    def read_file(
        self,
        input_path: Union[str, Path],
        options: Optional[AvroReadOptions] = None,
    ) -> List[Dict[str, Any]]:
        """
        Read records from an Avro file.

        Args:
            input_path: Path to the Avro file.
            options: Read options.

        Returns:
            List of records from the file.
        """
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Avro file not found: {input_path}")

        try:
            with open(input_path, "rb") as f:
                reader = DataFileReader(f, DatumReader())
                records = list(reader)
                reader.close()
                return records

        except Exception as e:
            raise IOError(f"Failed to read Avro file: {e}") from e

    def get_schema_from_file(
        self,
        input_path: Union[str, Path],
    ) -> Dict[str, Any]:
        """
        Extract the schema from an Avro file.

        Args:
            input_path: Path to the Avro file.

        Returns:
            Dictionary containing the schema.
        """
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Avro file not found: {input_path}")

        try:
            with open(input_path, "rb") as f:
                reader = DataFileReader(f, DatumReader())
                schema = reader.GetWriterSchema()
                reader.close()

                if hasattr(schema, "to_json"):
                    return json.loads(schema.to_json())
                return schema

        except Exception as e:
            raise IOError(f"Failed to read Avro schema: {e}") from e

    def evolve_schema(
        self,
        records: List[Dict[str, Any]],
        old_schema: Union[str, Dict[str, Any]],
        new_schema: Union[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Transform records from old schema to new schema.

        Handles schema evolution by mapping fields from old to new schema.

        Args:
            records: Records using the old schema.
            old_schema: Original schema.
            new_schema: Target schema.

        Returns:
            Records conforming to the new schema.
        """
        if isinstance(old_schema, AvroSchema):
            old_schema = old_schema.to_dict()
        if isinstance(new_schema, AvroSchema):
            new_schema = new_schema.to_dict()

        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        evolved = []
        for record in records:
            evolved_record = {}
            for field_name, field_def in new_fields.items():
                if field_name in record:
                    evolved_record[field_name] = record[field_name]
                elif "default" in field_def:
                    evolved_record[field_name] = field_def["default"]
            evolved.append(evolved_record)

        return evolved


def avro_serialize_action(
    input_data: List[Dict[str, Any]],
    output_path: str,
    schema: str,
    compression: str = "deflate",
) -> Dict[str, Any]:
    """
    Action function to serialize data to Avro format.

    Args:
        input_data: List of records to serialize.
        output_path: Path for the output Avro file.
        schema: Avro schema as JSON string.
        compression: Compression type (null, deflate, snappy, zstandard).

    Returns:
        Dictionary with operation results and metadata.
    """
    compression_map = {
        "null": CompressionType.NULL,
        "deflate": CompressionType.DEFLATE,
        "snappy": CompressionType.SNAPPY,
        "zstandard": CompressionType.ZSTANDARD,
    }

    if compression.lower() not in compression_map:
        raise ValueError(f"Unsupported compression: {compression}")

    options = AvroWriteOptions(
        schema=schema,
        compression=compression_map[compression.lower()],
    )

    serializer = AvroSerializer()
    return serializer.write_file(input_data, output_path, options)


def avro_deserialize_action(
    input_path: str,
) -> List[Dict[str, Any]]:
    """
    Action function to deserialize Avro data.

    Args:
        input_path: Path to the Avro file.

    Returns:
        List of deserialized records.
    """
    serializer = AvroSerializer()
    return serializer.read_file(input_path)


# Convenience instances
serialize_avro = AvroSerializer()
deserialize_avro = AvroSerializer()
