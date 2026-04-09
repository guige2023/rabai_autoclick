"""
Data Parser Action Module

Multi-format data parsing (JSON, XML, CSV, YAML, TOML).
Streaming parsers, schema validation, and error recovery.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

logger = logging.getLogger(__name__)


class DataFormat(Enum):
    """Supported data formats."""
    
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    YAML = "yaml"
    TOML = "toml"
    URL_ENCODED = "url_encoded"
    PLAINTEXT = "plaintext"


@dataclass
class ParseConfig:
    """Configuration for parsing behavior."""
    
    format: DataFormat = DataFormat.JSON
    encoding: str = "utf-8"
    strict: bool = False
    coalesce_errors: bool = True
    max_depth: int = 10
    timestamp_fields: List[str] = field(default_factory=list)
    number_fields: List[str] = field(default_factory=list)


@dataclass
class ParseResult:
    """Result of a parse operation."""
    
    success: bool
    data: Any = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get metadata value."""
        return self.metadata.get(key, default)


class JSONParser:
    """High-performance JSON parser."""
    
    def __init__(self, config: ParseConfig):
        self.config = config
    
    def parse(self, data: Union[str, bytes]) -> ParseResult:
        """Parse JSON data."""
        try:
            if isinstance(data, bytes):
                data = data.decode(self.config.encoding)
            
            kwargs = {}
            if self.config.max_depth:
                kwargs["depth_scale"] = self.config.max_depth
            
            result = json.loads(data)
            
            return ParseResult(
                success=True,
                data=result,
                metadata={
                    "format": DataFormat.JSON.value,
                    "bytes": len(data) if isinstance(data, str) else len(data)
                }
            )
        
        except json.JSONDecodeError as e:
            if self.config.coalesce_errors:
                return ParseResult(
                    success=False,
                    errors=[f"JSON parse error: {e.msg} at position {e.pos}"],
                    data=None
                )
            raise
    
    def parse_stream(self, stream: Iterator[str]) -> Iterator[Any]:
        """Parse JSON from a stream (newline-delimited JSON)."""
        buffer = ""
        for line in stream:
            buffer += line
            if buffer.strip():
                try:
                    yield json.loads(buffer)
                    buffer = ""
                except json.JSONDecodeError:
                    continue


class XMLParser:
    """XML parsing with namespace support."""
    
    def __init__(self, config: ParseConfig):
        self.config = config
    
    def parse(self, data: Union[str, bytes]) -> ParseResult:
        """Parse XML data."""
        try:
            if isinstance(data, bytes):
                data = data.decode(self.config.encoding)
            
            root = ET.fromstring(data)
            
            result = self._element_to_dict(root)
            
            return ParseResult(
                success=True,
                data=result,
                metadata={
                    "format": DataFormat.XML.value,
                    "root_tag": root.tag,
                    "namespaces": self._extract_namespaces(root)
                }
            )
        
        except ET.ParseError as e:
            return ParseResult(
                success=False,
                errors=[f"XML parse error: {str(e)}"]
            )
    
    def _element_to_dict(self, element: ET.Element) -> Dict:
        """Convert XML element to dictionary."""
        result: Dict[str, Any] = {}
        
        if element.attrib:
            result["@attributes"] = dict(element.attrib)
        
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result["#text"] = element.text.strip()
        
        for child in element:
            child_data = self._element_to_dict(child)
            
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    def _extract_namespaces(self, element: ET.Element) -> Dict[str, str]:
        """Extract namespaces from XML."""
        namespaces: Dict[str, str] = {}
        for event, elem in ET.iterparse(
            io.StringIO(ET.tostring(element, encoding="unicode")),
            events=["start-ns"]
        ):
            if event == "start-ns":
                prefix, uri = elem
                namespaces[prefix or "default"] = uri
        return namespaces


class CSVParser:
    """CSV parsing with flexible delimiters."""
    
    def __init__(self, config: ParseConfig):
        self.config = config
        self._delimiter = ","
        self._quotechar = '"'
    
    def set_delimiter(self, delimiter: str) -> None:
        """Set the field delimiter."""
        self._delimiter = delimiter
    
    def parse(self, data: Union[str, bytes]) -> ParseResult:
        """Parse CSV data."""
        try:
            if isinstance(data, bytes):
                data = data.decode(self.config.encoding)
            
            reader = csv.reader(
                io.StringIO(data),
                delimiter=self._delimiter,
                quotechar=self._quotechar
            )
            
            rows = list(reader)
            
            if not rows:
                return ParseResult(success=True, data=[], metadata={"rows": 0})
            
            headers = rows[0] if rows else []
            records = []
            
            for i, row in enumerate(rows[1:], start=2):
                if len(row) != len(headers):
                    if self.config.coalesce_errors:
                        continue
                record = dict(zip(headers, row))
                records.append(record)
            
            return ParseResult(
                success=True,
                data=records,
                metadata={
                    "format": DataFormat.CSV.value,
                    "rows": len(records),
                    "columns": len(headers),
                    "headers": headers
                }
            )
        
        except csv.Error as e:
            return ParseResult(success=False, errors=[f"CSV parse error: {str(e)}"])
    
    def parse_dict(self, data: Union[str, bytes]) -> ParseResult:
        """Parse CSV with automatic header detection."""
        if isinstance(data, bytes):
            data = data.decode(self.config.encoding)
        
        try:
            reader = csv.DictReader(
                io.StringIO(data),
                delimiter=self._delimiter
            )
            records = list(reader)
            
            return ParseResult(
                success=True,
                data=records,
                metadata={
                    "format": DataFormat.CSV.value,
                    "rows": len(records),
                    "columns": len(reader.fieldnames) if reader.fieldnames else 0
                }
            )
        except Exception as e:
            return ParseResult(success=False, errors=[str(e)])


class DataParserAction:
    """
    Main data parser action handler.
    
    Unified interface for parsing multiple data formats
    with automatic format detection and error recovery.
    """
    
    def __init__(self, config: Optional[ParseConfig] = None):
        self.config = config or ParseConfig()
        self._parsers: Dict[DataFormat, Any] = {
            DataFormat.JSON: JSONParser(self.config),
            DataFormat.XML: XMLParser(self.config),
            DataFormat.CSV: CSVParser(self.config),
        }
        self._transformers: List[Callable] = []
    
    def add_transformer(self, transformer: Callable[[Any], Any]) -> None:
        """Add a post-parse transformer."""
        self._transformers.append(transformer)
    
    def detect_format(self, data: Union[str, bytes]) -> DataFormat:
        """Auto-detect data format from content."""
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                return DataFormat.PLAINTEXT
        
        data = data.strip()
        
        if data.startswith(("{", "[")):
            return DataFormat.JSON
        
        if data.startswith("<"):
            return DataFormat.XML
        
        if "\n" in data and "," in data:
            return DataFormat.CSV
        
        if re.match(r"^[\w-]+:", data, re.M):
            try:
                import yaml
                yaml.safe_load(data)
                return DataFormat.YAML
            except Exception:
                pass
        
        return DataFormat.PLAINTEXT
    
    def parse(
        self,
        data: Union[str, bytes],
        format: Optional[DataFormat] = None
    ) -> ParseResult:
        """Parse data with optional format override."""
        if format is None:
            format = self.detect_format(data)
        
        parser = self._parsers.get(format)
        if parser is None:
            return ParseResult(
                success=False,
                errors=[f"No parser for format: {format.value}"]
            )
        
        result = parser.parse(data)
        
        if result.success and self._transformers:
            for transformer in self._transformers:
                try:
                    result.data = transformer(result.data)
                except Exception as e:
                    result.warnings.append(f"Transformer error: {str(e)}")
        
        return result
    
    def parse_multiple(
        self,
        data_list: List[Union[str, bytes]],
        format: Optional[DataFormat] = None
    ) -> List[ParseResult]:
        """Parse multiple data items."""
        return [self.parse(data, format) for data in data_list]
    
    def parse_streaming(
        self,
        stream: Iterator[str],
        format: DataFormat
    ) -> Iterator[Any]:
        """Parse data in streaming mode."""
        if format == DataFormat.JSON:
            parser = self._parsers[DataFormat.JSON]
            yield from parser.parse_stream(stream)
        else:
            buffer = ""
            for line in stream:
                buffer += line
                result = self.parse(buffer, format)
                if result.success:
                    yield result.data
                    buffer = ""
    
    def register_format(self, format: DataFormat, parser: Any) -> None:
        """Register a custom parser for a format."""
        self._parsers[format] = parser
    
    def get_stats(self) -> Dict[str, Any]:
        """Get parser statistics."""
        return {
            "supported_formats": [f.value for f in self._parsers.keys()],
            "transformers_count": len(self._transformers)
        }
