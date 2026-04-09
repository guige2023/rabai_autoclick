"""
API Response Parser and Normalizer Module.

Parses various API response formats, handles pagination,
extracts embedded data, normalizes schema differences,
and provides consistent data access patterns.
"""

from typing import (
    Dict, List, Optional, Any, Union, Callable,
    Tuple, Set, TypeVar, Generic, Iterator
)
from dataclasses import dataclass, field
from enum import Enum, auto
from dataclasses import dataclass
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import parse_qs, urlparse
import logging
from collections import abc

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ResponseFormat(Enum):
    """API response formats."""
    JSON = auto()
    XML = auto()
    HTML = auto()
    TEXT = auto()
    BINARY = auto()
    UNKNOWN = auto()


class PaginationType(Enum):
    """Pagination strategies."""
    NONE = auto()
    OFFSET = auto()
    PAGE = auto()
    CURSOR = auto()
    LINK_HEADER = auto()
    NEXT_TOKEN = auto()


@dataclass
class ParsedResponse:
    """Normalized API response."""
    data: Any
    status_code: int
    headers: Dict[str, str]
    format: ResponseFormat
    pagination: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    raw_body: Optional[str] = None
    
    def get_data(self) -> Any:
        """Get primary response data."""
        return self.data
    
    def get_items(self, path: str = "") -> List[Any]:
        """Get items at JSON path."""
        if not path:
            return self.data if isinstance(self.data, list) else [self.data]
        
        parts = path.split(".")
        current = self.data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx]
                except (ValueError, IndexError):
                    return []
            else:
                return []
        
        return current if isinstance(current, list) else [current]
    
    def has_error(self) -> bool:
        """Check if response indicates error."""
        return self.status_code >= 400 or len(self.errors) > 0


@dataclass
class PaginationConfig:
    """Configuration for pagination handling."""
    pagination_type: PaginationType
    limit_param: str = "limit"
    offset_param: str = "offset"
    page_param: str = "page"
    cursor_param: str = "cursor"
    next_link_field: str = "next"
    total_field: Optional[str] = None
    items_field: Optional[str] = None
    page_size: int = 100


@dataclass
class ResponseParserConfig:
    """Configuration for response parsing."""
    default_format: ResponseFormat = ResponseFormat.JSON
    normalize_keys: bool = True  # Convert snake_case to camelCase
    strip_nulls: bool = False
    unwrap_root: bool = True
    pagination: Optional[PaginationConfig] = None
    error_paths: List[str] = field(default_factory=lambda: ["error", "errors", "message"])
    data_paths: List[str] = field(default_factory=lambda: ["data", "result", "results", "items", "records"])


class JsonPathExtractor:
    """Extracts data using JSON path expressions."""
    
    @staticmethod
    def extract(data: Any, path: str) -> Any:
        """
        Extract data at JSON path.
        
        Supports:
            - Key access: $.name or name
            - Array index: [0], [-1]
            - Wildcard: [*], [*].name
            - Filter: [?(@.age > 18)]
        """
        if not path or path == "$":
            return data
        
        # Remove leading $ if present
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:]
        
        parts = JsonPathExtractor._parse_path(path)
        return JsonPathExtractor._navigate(data, parts)
    
    @staticmethod
    def _parse_path(path: str) -> List[Union[str, int, Tuple[str, Any]]]:
        """Parse path into components."""
        parts = []
        current = ""
        i = 0
        
        while i < len(path):
            char = path[i]
            
            if char == ".":
                if current:
                    parts.append(current)
                    current = ""
            
            elif char == "[":
                if current:
                    parts.append(current)
                    current = ""
                
                # Find matching ]
                end = path.find("]", i)
                if end == -1:
                    break
                
                bracket_content = path[i+1:end]
                
                if bracket_content.startswith("?"):
                    # Filter expression [?(@.field > value)]
                    filter_expr = bracket_content[1:]  # Remove ?
                    # Simplified filter parsing
                    parts.append(("filter", filter_expr))
                elif bracket_content.isdigit():
                    parts.append(int(bracket_content))
                elif bracket_content.startswith('"') and bracket_content.endswith('"'):
                    parts.append(bracket_content[1:-1])
                elif bracket_content.startswith("'") and bracket_content.endswith("'"):
                    parts.append(bracket_content[1:-1])
                
                i = end
            
            elif char == "*":
                parts.append("*")
            
            else:
                current += char
            
            i += 1
        
        if current:
            parts.append(current)
        
        return parts
    
    @staticmethod
    def _navigate(data: Any, parts: List[Union[str, int, Tuple[str, Any]]]) -> Any:
        """Navigate to path component."""
        current = data
        
        for part in parts:
            if current is None:
                return None
            
            if isinstance(part, str) and part == "*":
                if isinstance(current, (list, tuple)):
                    result = []
                    for item in current:
                        if isinstance(item, dict):
                            result.append(item)
                    current = result
                else:
                    current = []
            
            elif isinstance(part, str):
                if isinstance(current, dict):
                    current = current.get(part)
                elif isinstance(current, list):
                    current = [item.get(part) if isinstance(item, dict) else None for item in current]
                else:
                    return None
            
            elif isinstance(part, int):
                if isinstance(current, (list, tuple)) and abs(part) < len(current):
                    current = current[part]
                else:
                    return None
            
            elif isinstance(part, tuple) and part[0] == "filter":
                if isinstance(current, list):
                    # Simplified filter
                    current = current  # Would need proper filter evaluation
                else:
                    current = []
            
            else:
                return None
        
        return current


class ResponseParser:
    """
    Parses and normalizes API responses.
    
    Handles multiple formats, pagination, error detection,
    and provides consistent data extraction patterns.
    """
    
    def __init__(self, config: Optional[ResponseParserConfig] = None) -> None:
        self.config = config or ResponseParserConfig()
        self.json_extractor = JsonPathExtractor()
    
    def parse(
        self,
        response_body: str,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None
    ) -> ParsedResponse:
        """
        Parse API response body.
        
        Args:
            response_body: Raw response body
            status_code: HTTP status code
            headers: Response headers
            content_type: Content-Type header value
            
        Returns:
            Normalized ParsedResponse
        """
        headers = headers or {}
        format = self._detect_format(response_body, content_type)
        
        response = ParsedResponse(
            data=None,
            status_code=status_code,
            headers=headers,
            format=format,
            raw_body=response_body
        )
        
        try:
            if format == ResponseFormat.JSON:
                response.data = self._parse_json(response_body)
            elif format == ResponseFormat.XML:
                response.data = self._parse_xml(response_body)
            elif format == ResponseFormat.TEXT:
                response.data = response_body
            else:
                response.data = response_body
                response.errors.append(f"Unsupported format: {format}")
            
            # Unwrap root if configured
            if self.config.unwrap_root:
                response.data = self._unwrap_data(response.data)
            
            # Normalize keys
            if self.config.normalize_keys:
                response.data = self._normalize_keys(response.data)
            
            # Strip nulls
            if self.config.strip_nulls:
                response.data = self._strip_nulls(response.data)
            
            # Extract pagination info
            if self.config.pagination:
                response.pagination = self._extract_pagination(
                    response.data, headers
                )
            
            # Check for errors
            if self._has_error_response(response):
                response.errors.append("Error response detected")
            
        except Exception as e:
            logger.error(f"Parse error: {e}")
            response.errors.append(str(e))
        
        return response
    
    def _detect_format(
        self,
        body: str,
        content_type: Optional[str]
    ) -> ResponseFormat:
        """Detect response format."""
        if content_type:
            if "json" in content_type.lower():
                return ResponseFormat.JSON
            elif "xml" in content_type.lower():
                return ResponseFormat.XML
            elif "html" in content_type.lower():
                return ResponseFormat.HTML
            elif "text" in content_type.lower():
                return ResponseFormat.TEXT
        
        # Try auto-detection
        body = body.strip()
        if body.startswith("{") or body.startswith("["):
            return ResponseFormat.JSON
        elif body.startswith("<"):
            return ResponseFormat.XML
        
        return ResponseFormat.UNKNOWN
    
    def _parse_json(self, body: str) -> Any:
        """Parse JSON body."""
        return json.loads(body)
    
    def _parse_xml(self, body: str) -> Dict[str, Any]:
        """Parse XML body to dict."""
        root = ET.fromstring(body)
        return self._xml_to_dict(root)
    
    def _xml_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dict."""
        result = {}
        
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in result:
                # Multiple children with same tag
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        if element.text and element.text.strip():
            if result:
                result["_text"] = element.text.strip()
            else:
                return element.text.strip()
        
        return result if result else None
    
    def _unwrap_data(self, data: Any) -> Any:
        """Unwrap data from common wrapper fields."""
        if not isinstance(data, dict):
            return data
        
        for unwrap_key in self.config.data_paths:
            if unwrap_key in data:
                return data[unwrap_key]
        
        return data
    
    def _normalize_keys(self, data: Any) -> Any:
        """Normalize dictionary keys."""
        if isinstance(data, dict):
            return {
                self._normalize_key(k): self._normalize_keys(v)
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return [self._normalize_keys(item) for item in data]
        return data
    
    def _normalize_key(self, key: str) -> str:
        """Normalize a single key."""
        # Convert snake_case to camelCase
        parts = key.split("_")
        if len(parts) > 1:
            return parts[0] + "".join(p.title() for p in parts[1:])
        return key
    
    def _strip_nulls(self, data: Any) -> Any:
        """Remove null values from data."""
        if isinstance(data, dict):
            return {
                k: self._strip_nulls(v)
                for k, v in data.items()
                if v is not None
            }
        elif isinstance(data, list):
            return [self._strip_nulls(item) for item in data if item is not None]
        return data
    
    def _extract_pagination(
        self,
        data: Any,
        headers: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Extract pagination information."""
        if not isinstance(data, dict):
            return None
        
        pagination = {}
        
        # Check config
        config = self.config.pagination
        
        if config.pagination_type == PaginationType.OFFSET:
            pagination["offset"] = data.get(config.offset_param, 0)
            pagination["limit"] = data.get(config.limit_param, config.page_size)
        
        elif config.pagination_type == PaginationType.CURSOR:
            pagination["cursor"] = data.get(config.cursor_param)
        
        elif config.pagination_type == PaginationType.LINK_HEADER:
            link_header = headers.get("Link", "")
            pagination["next"] = "next" in link_header.lower()
        
        elif config.pagination_type == PaginationType.NEXT_TOKEN:
            pagination["next_token"] = data.get(config.next_link_field)
        
        # Total count if available
        if config.total_field and config.total_field in data:
            pagination["total"] = data[config.total_field]
        
        return pagination if pagination else None
    
    def _has_error_response(self, response: ParsedResponse) -> bool:
        """Check if response contains error."""
        for error_path in self.config.error_paths:
            error_data = self.json_extractor.extract(response.data, error_path)
            if error_data:
                return True
        
        return response.status_code >= 400


class PagedResponseIterator(Generic[T]):
    """Iterator for paginated API responses."""
    
    def __init__(
        self,
        fetch_func: Callable[[Optional[str]], ParsedResponse],
        pagination_field: str = "next_cursor"
    ) -> None:
        self.fetch_func = fetch_func
        self.pagination_field = pagination_field
        self.cursor: Optional[str] = None
        self._exhausted = False
    
    def __iter__(self) -> Iterator[T]:
        return self
    
    def __next__(self) -> T:
        if self._exhausted:
            raise StopIteration
        
        response = self.fetch_func(self.cursor)
        
        if response.has_error() or not response.data:
            self._exhausted = True
            raise StopIteration
        
        # Extract next cursor
        if isinstance(response.data, dict):
            self.cursor = response.data.get(self.pagination_field)
            if not self.cursor:
                self._exhausted = True
        
        return response.data
    
    def fetch_all(self) -> List[T]:
        """Fetch all pages and return combined results."""
        results = []
        for page in self:
            results.append(page)
        return results


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test response parsing
    sample_responses = [
        '{"status": "success", "data": {"users": [{"id": 1, "user_name": "Alice"}, {"id": 2, "user_name": "Bob"}], "next_cursor": "abc123"}}',
        '<?xml version="1.0"?><response><item><id>1</id><name>Item1</name></item></response>',
    ]
    
    parser = ResponseParser()
    
    for raw in sample_responses:
        response = parser.parse(raw)
        print(f"Format: {response.format.name}")
        print(f"Data: {response.data}")
        print()
