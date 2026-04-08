"""
Data Serialization Action Module

Provides data serialization, deserialization, and format conversion.
"""
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import base64
import pickle
import gzip

T = TypeVar('T')


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    JSON_LINES = "jsonl"
    BSON = "bson"
    MSGPACK = "msgpack"
    PROTOBUF = "protobuf"
    PICKLE = "pickle"
    XML = "xml"
    YAML = "yaml"
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"


@dataclass
class SerializationConfig:
    """Configuration for serialization."""
    format: SerializationFormat
    compression: Optional[str] = None  # gzip, zlib, lz4
    encoding: str = "utf-8"
    indent: Optional[int] = 2
    date_format: str = "iso"  # iso, epoch, custom
    strict: bool = True
    custom_encoders: dict[type, Callable] = field(default_factory=dict)


@dataclass
class SerializationResult:
    """Result of serialization operation."""
    success: bool
    data: Any
    bytes_written: int = 0
    duration_ms: float = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class DeserializationResult:
    """Result of deserialization operation."""
    success: bool
    data: Any
    format: SerializationFormat
    duration_ms: float = 0
    errors: list[str] = field(default_factory=list)


class DataSerializationAction:
    """Main data serialization action handler."""
    
    def __init__(self, default_config: Optional[SerializationConfig] = None):
        self.default_config = default_config or SerializationConfig(
            format=SerializationFormat.JSON
        )
        self._serializers = {
            SerializationFormat.JSON: self._serialize_json,
            SerializationFormat.JSON_LINES: self._serialize_jsonl,
            SerializationFormat.PICKLE: self._serialize_pickle,
            SerializationFormat.CSV: self._serialize_csv,
            SerializationFormat.YAML: self._serialize_yaml,
        }
        self._deserializers = {
            SerializationFormat.JSON: self._deserialize_json,
            SerializationFormat.JSON_LINES: self._deserialize_jsonl,
            SerializationFormat.PICKLE: self._deserialize_pickle,
            SerializationFormat.CSV: self._deserialize_csv,
            SerializationFormat.YAML: self._deserialize_yaml,
        }
        self._stats = {"serialize": 0, "deserialize": 0, "errors": 0}
    
    async def serialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
        config: Optional[SerializationConfig] = None
    ) -> SerializationResult:
        """
        Serialize data to specified format.
        
        Args:
            data: Data to serialize
            format: Target format (uses default if not specified)
            config: Serialization configuration
            
        Returns:
            SerializationResult with serialized data
        """
        cfg = config or self.default_config
        fmt = format or cfg.format
        start_time = datetime.now()
        errors = []
        
        try:
            serializer = self._serializers.get(fmt)
            if not serializer:
                return SerializationResult(
                    success=False,
                    data=None,
                    errors=[f"No serializer for format {fmt.value}"]
                )
            
            result = await serializer(data, cfg)
            self._stats["serialize"] += 1
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            # Apply compression if configured
            if cfg.compression:
                result = await self._compress(result, cfg.compression)
            
            return SerializationResult(
                success=True,
                data=result,
                bytes_written=len(str(result)) if isinstance(result, str) else len(result),
                duration_ms=duration_ms
            )
            
        except Exception as e:
            self._stats["errors"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return SerializationResult(
                success=False,
                data=None,
                errors=[str(e)],
                duration_ms=duration_ms
            )
    
    async def deserialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
        target_type: Optional[type] = None,
        config: Optional[SerializationConfig] = None
    ) -> DeserializationResult:
        """
        Deserialize data from specified format.
        
        Args:
            data: Data to deserialize
            format: Source format (detected from data if not specified)
            target_type: Optional target type to cast to
            config: Deserialization configuration
            
        Returns:
            DeserializationResult with deserialized data
        """
        cfg = config or self.default_config
        fmt = format or self._detect_format(data) or cfg.format
        start_time = datetime.now()
        errors = []
        
        try:
            # Decompress if needed
            if cfg.compression:
                data = await self._decompress(data, cfg.compression)
            
            deserializer = self._deserializers.get(fmt)
            if not deserializer:
                return DeserializationResult(
                    success=False,
                    data=None,
                    format=fmt,
                    errors=[f"No deserializer for format {fmt.value}"]
                )
            
            result = await deserializer(data, cfg)
            self._stats["deserialize"] += 1
            
            # Apply target type if specified
            if target_type and result is not None:
                try:
                    result = target_type(result)
                except Exception as e:
                    errors.append(f"Type conversion failed: {e}")
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return DeserializationResult(
                success=len(errors) == 0,
                data=result,
                format=fmt,
                duration_ms=duration_ms,
                errors=errors
            )
            
        except Exception as e:
            self._stats["errors"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return DeserializationResult(
                success=False,
                data=None,
                format=fmt,
                duration_ms=duration_ms,
                errors=[str(e)]
            )
    
    def _detect_format(self, data: Any) -> Optional[SerializationFormat]:
        """Detect serialization format from data."""
        if isinstance(data, bytes):
            # Check for magic bytes
            if data[:2] == b'\x80\x04':  # Pickle
                return SerializationFormat.PICKLE
            if data[:2] == b'PK':  # ZIP/Parquet
                return SerializationFormat.PARQUET
            if data[:1] == b'{':  # JSON
                return SerializationFormat.JSON
        elif isinstance(data, str):
            if data.startswith("{"):
                return SerializationFormat.JSON
            if data.startswith("["):
                try:
                    json.loads(data)
                    return SerializationFormat.JSON_LINES
                except:
                    pass
            if "---" in data or data.startswith("- "):
                return SerializationFormat.YAML
        
        return None
    
    async def _serialize_json(self, data: Any, config: SerializationConfig) -> str:
        """Serialize to JSON."""
        return json.dumps(
            data,
            indent=config.indent,
            default=self._json_default,
            ensure_ascii=False
        )
    
    async def _serialize_jsonl(self, data: Any, config: SerializationConfig) -> str:
        """Serialize to JSON Lines format."""
        if isinstance(data, list):
            return "\n".join(json.dumps(item, default=self._json_default) for item in data)
        return json.dumps(data, default=self._json_default)
    
    async def _serialize_pickle(self, data: Any, config: SerializationConfig) -> bytes:
        """Serialize to Pickle format."""
        return pickle.dumps(data)
    
    async def _serialize_csv(self, data: Any, config: SerializationConfig) -> str:
        """Serialize to CSV format."""
        import csv
        from io import StringIO
        
        if isinstance(data, list) and data:
            output = StringIO()
            if isinstance(data[0], dict):
                writer = csv.DictWriter(output, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(output)
                writer.writerows(data)
            return output.getvalue()
        return ""
    
    async def _serialize_yaml(self, data: Any, config: SerializationConfig) -> str:
        """Serialize to YAML format."""
        try:
            import yaml
            return yaml.dump(data, default_flow_style=False, allow_unicode=True)
        except ImportError:
            # Fallback to JSON
            return await self._serialize_json(data, config)
    
    async def _deserialize_json(self, data: Any, config: SerializationConfig) -> Any:
        """Deserialize from JSON."""
        if isinstance(data, bytes):
            data = data.decode(config.encoding)
        return json.loads(data, object_hook=self._json_object_hook)
    
    async def _deserialize_jsonl(self, data: Any, config: SerializationConfig) -> list:
        """Deserialize from JSON Lines format."""
        if isinstance(data, bytes):
            data = data.decode(config.encoding)
        
        lines = data.strip().split("\n")
        return [json.loads(line) for line in lines if line.strip()]
    
    async def _deserialize_pickle(self, data: Any, config: SerializationConfig) -> Any:
        """Deserialize from Pickle format."""
        return pickle.loads(data)
    
    async def _deserialize_csv(self, data: Any, config: SerializationConfig) -> list:
        """Deserialize from CSV format."""
        import csv
        from io import StringIO
        
        if isinstance(data, bytes):
            data = data.decode(config.encoding)
        
        reader = csv.DictReader(StringIO(data))
        return list(reader)
    
    async def _deserialize_yaml(self, data: Any, config: SerializationConfig) -> Any:
        """Deserialize from YAML format."""
        try:
            import yaml
            if isinstance(data, bytes):
                data = data.decode(config.encoding)
            return yaml.safe_load(data)
        except ImportError:
            raise Exception("YAML library not installed")
    
    def _json_default(self, obj: Any) -> Any:
        """Custom JSON encoder for non-serializable types."""
        if isinstance(obj, datetime):
            if self.default_config.date_format == "iso":
                return obj.isoformat()
            elif self.default_config.date_format == "epoch":
                return obj.timestamp()
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return str(obj)
    
    def _json_object_hook(self, obj: dict) -> dict:
        """Hook for processing JSON objects during deserialization."""
        # Convert date strings back to datetime
        date_fields = ["created_at", "updated_at", "timestamp", "date"]
        for field in date_fields:
            if field in obj and isinstance(obj[field], str):
                try:
                    obj[field] = datetime.fromisoformat(obj[field])
                except:
                    pass
        return obj
    
    async def _compress(self, data: Any, algorithm: str) -> bytes:
        """Compress serialized data."""
        if isinstance(data, str):
            data = data.encode(self.default_config.encoding)
        
        if algorithm == "gzip":
            return gzip.compress(data)
        return data
    
    async def _decompress(self, data: bytes, algorithm: str) -> bytes:
        """Decompress data."""
        if algorithm == "gzip":
            return gzip.decompress(data)
        return data
    
    async def convert_format(
        self,
        data: Any,
        from_format: SerializationFormat,
        to_format: SerializationFormat,
        config: Optional[SerializationConfig] = None
    ) -> SerializationResult:
        """Convert data between formats."""
        # First deserialize
        deserialized = await self.deserialize(data, from_format, config=config)
        if not deserialized.success:
            return SerializationResult(
                success=False,
                data=None,
                errors=deserialized.errors
            )
        
        # Then serialize
        return await self.serialize(
            deserialized.data,
            format=to_format,
            config=config
        )
    
    async def serialize_to_base64(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
        compress: bool = False
    ) -> str:
        """Serialize data and encode to base64."""
        cfg = SerializationConfig(
            format=format or SerializationFormat.JSON,
            compression="gzip" if compress else None
        )
        
        result = await self.serialize(data, config=cfg)
        if not result.success:
            raise Exception(result.errors[0])
        
        serialized = result.data
        if isinstance(serialized, str):
            serialized = serialized.encode(cfg.encoding)
        
        return base64.b64encode(serialized).decode(cfg.encoding)
    
    async def deserialize_from_base64(
        self,
        encoded: str,
        format: Optional[SerializationFormat] = None,
        compressed: bool = False
    ) -> Any:
        """Decode from base64 and deserialize."""
        cfg = SerializationConfig(
            format=format or SerializationFormat.JSON,
            compression="gzip" if compressed else None
        )
        
        data = base64.b64decode(encoded.encode())
        
        result = await self.deserialize(data, config=cfg)
        if not result.success:
            raise Exception(result.errors[0])
        
        return result.data
    
    def get_stats(self) -> dict[str, Any]:
        """Get serialization statistics."""
        return dict(self._stats)
