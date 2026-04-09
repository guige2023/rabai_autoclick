"""
API Response Parser Action Module

Parses and transforms API responses into structured data.
Supports JSON, XML, HTML, and custom parsing strategies.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ParseStrategy(Enum):
    """Response parsing strategy."""
    JSON = "json"
    XML = "xml"
    HTML = "html"
    TEXT = "text"
    CUSTOM = "custom"


@dataclass
class ParseResult:
    """Result of a parsing operation."""
    success: bool
    data: Any = None
    strategy: ParseStrategy
    duration_ms: float = 0.0
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FieldMapping:
    """Mapping for a field."""
    source_path: str
    target_field: str
    transform_fn: Optional[Callable] = None
    default_value: Any = None


class JsonPathParser:
    """JSON path parser for extracting data from JSON."""
    
    @staticmethod
    def extract(data: Any, path: str) -> Any:
        """Extract data using JSON path."""
        parts = path.replace("[", ".").replace("]", "").split(".")
        current = data
        
        for part in parts:
            if not part:
                continue
            
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    current = None
            else:
                return None
            
            if current is None:
                return None
        
        return current


class ApiResponseParserAction:
    """
    Parser for API responses.
    
    Example:
        parser = ApiResponseParserAction()
        
        result = parser.parse(
            response_text='{"user": {"name": "John"}}',
            strategy=ParseStrategy.JSON,
            field_mappings=[
                FieldMapping("$.user.name", "user_name")
            ]
        )
    """
    
    def __init__(self):
        self._stats = {
            "total_parses": 0,
            "successful_parses": 0,
            "failed_parses": 0,
            "bytes_processed": 0
        }
    
    def parse(
        self,
        response: Any,
        strategy: ParseStrategy = ParseStrategy.JSON,
        custom_parser: Optional[Callable] = None
    ) -> ParseResult:
        """
        Parse an API response.
        
        Args:
            response: Response to parse
            strategy: Parsing strategy
            custom_parser: Optional custom parser function
            
        Returns:
            ParseResult with parsed data
        """
        start_time = datetime.now()
        self._stats["total_parses"] += 1
        
        if isinstance(response, bytes):
            response = response.decode('utf-8')
        
        self._stats["bytes_processed"] += len(str(response))
        
        try:
            if custom_parser:
                data = custom_parser(response)
            elif strategy == ParseStrategy.JSON:
                data = self._parse_json(response)
            elif strategy == ParseStrategy.XML:
                data = self._parse_xml(response)
            elif strategy == ParseStrategy.HTML:
                data = self._parse_html(response)
            elif strategy == ParseStrategy.TEXT:
                data = self._parse_text(response)
            else:
                data = response
            
            self._stats["successful_parses"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return ParseResult(
                success=True,
                data=data,
                strategy=strategy,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["failed_parses"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return ParseResult(
                success=False,
                data=None,
                strategy=strategy,
                duration_ms=duration_ms,
                error=str(e)
            )
    
    def _parse_json(self, response: str) -> Any:
        """Parse JSON response."""
        if not response or response == "":
            return {}
        return json.loads(response)
    
    def _parse_xml(self, response: str) -> dict:
        """Parse XML response to dict."""
        if not response or response == "":
            return {}
        
        root = ET.fromstring(response)
        return self._xml_to_dict(root)
    
    def _xml_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dict."""
        result = {}
        
        if element.attrib:
            result["@attributes"] = element.attrib
        
        if element.text and element.text.strip():
            return element.text.strip()
        
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    def _parse_html(self, response: str) -> list[dict]:
        """Parse HTML response, extracting text and links."""
        result = {
            "title": "",
            "links": [],
            "text": ""
        }
        
        title_match = re.search(r'<title>(.*?)</title>', response, re.IGNORECASE | re.DOTALL)
        if title_match:
            result["title"] = title_match.group(1).strip()
        
        links = re.findall(r'href=["\']([^"\']+)["\']', response)
        result["links"] = links
        
        text = re.sub(r'<[^>]+>', ' ', response)
        text = re.sub(r'\s+', ' ', text).strip()
        result["text"] = text
        
        return result
    
    def _parse_text(self, response: str) -> str:
        """Parse text response."""
        return response.strip() if response else ""
    
    def extract_fields(
        self,
        data: Any,
        mappings: list[FieldMapping]
    ) -> dict[str, Any]:
        """
        Extract and transform fields from parsed data.
        
        Args:
            data: Parsed data
            mappings: List of field mappings
            
        Returns:
            Dictionary with extracted and transformed fields
        """
        result = {}
        
        for mapping in mappings:
            value = JsonPathParser.extract(data, mapping.source_path)
            
            if value is None:
                value = mapping.default_value
            elif mapping.transform_fn:
                try:
                    value = mapping.transform_fn(value)
                except Exception:
                    value = mapping.default_value or value
            
            result[mapping.target_field] = value
        
        return result
    
    def parse_and_extract(
        self,
        response: Any,
        strategy: ParseStrategy = ParseStrategy.JSON,
        mappings: Optional[list[FieldMapping]] = None
    ) -> ParseResult:
        """Parse response and extract fields."""
        parse_result = self.parse(response, strategy)
        
        if not parse_result.success:
            return parse_result
        
        if mappings:
            extracted = self.extract_fields(parse_result.data, mappings)
            parse_result.data = extracted
            parse_result.metadata["extracted_fields"] = list(extracted.keys())
        
        return parse_result
    
    def parse_batch(
        self,
        responses: list[Any],
        strategy: ParseStrategy = ParseStrategy.JSON
    ) -> list[ParseResult]:
        """Parse multiple responses."""
        return [self.parse(r, strategy) for r in responses]
    
    def get_stats(self) -> dict[str, Any]:
        """Get parsing statistics."""
        return {
            **self._stats,
            "success_rate": (
                self._stats["successful_parses"] / self._stats["total_parses"]
                if self._stats["total_parses"] > 0 else 0
            )
        }
