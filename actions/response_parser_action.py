"""Response parser action module for RabAI AutoClick.

Provides response parsing with automatic format detection, 
error handling, and data extraction.
"""

import sys
import os
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ResponseFormat(Enum):
    """Response data format."""
    JSON = "json"
    XML = "xml"
    HTML = "html"
    TEXT = "text"
    BINARY = "binary"


@dataclass
class ParsedResponse:
    """Parsed response with metadata."""
    data: Any
    format: ResponseFormat
    status_code: int
    headers: Dict[str, str]
    raw_body: Optional[str] = None
    error: Optional[str] = None


class ResponseParserAction(BaseAction):
    """Parse HTTP responses with automatic format detection.
    
    Supports JSON, XML, HTML parsing with error handling
    and field extraction.
    """
    action_type = "response_parser"
    display_name = "响应解析"
    description = "自动解析HTTP响应，支持多种格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute response parsing.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'parse', 'extract', 'detect'
                - response: Response dict with body, status_code, headers
                - format: Force specific format ('json', 'xml', 'html', 'text')
                - extract_path: JSONPath or XPath for data extraction
                - default: Default value if extraction fails
        
        Returns:
            ActionResult with parsed response.
        """
        operation = params.get('operation', 'parse').lower()
        
        if operation == 'parse':
            return self._parse(params)
        elif operation == 'extract':
            return self._extract(params)
        elif operation == 'detect':
            return self._detect(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _parse(self, params: Dict[str, Any]) -> ActionResult:
        """Parse response body."""
        response = params.get('response', {})
        format_spec = params.get('format')
        extract_path = params.get('extract_path')
        default = params.get('default', None)
        
        body = response.get('body', '')
        status_code = response.get('status_code', 200)
        headers = response.get('headers', {})
        
        if not body:
            return ActionResult(
                success=True,
                message="Empty response",
                data={'data': None, 'format': 'none'}
            )
        
        # Detect or use specified format
        if format_spec:
            fmt = ResponseFormat(format_spec.lower())
        else:
            fmt = self._detect_format(body, headers)
        
        # Parse based on format
        parsed = None
        error = None
        
        try:
            if fmt == ResponseFormat.JSON:
                if isinstance(body, str):
                    parsed = json.loads(body)
                else:
                    parsed = body
            
            elif fmt == ResponseFormat.XML:
                if isinstance(body, str):
                    parsed = self._parse_xml(body)
                else:
                    parsed = body
            
            elif fmt == ResponseFormat.HTML:
                parsed = self._parse_html(str(body))
            
            elif fmt == ResponseFormat.TEXT:
                parsed = str(body)
            
            else:
                parsed = body
        
        except Exception as e:
            error = str(e)
            # Try to return raw text on parse failure
            if fmt == ResponseFormat.JSON:
                parsed = str(body)
                fmt = ResponseFormat.TEXT
        
        # Extract specific path if requested
        if extract_path and parsed:
            parsed = self._extract_path(parsed, extract_path, default)
        
        return ActionResult(
            success=error is None,
            message=f"Parsed {fmt.value} response",
            data={
                'data': parsed,
                'format': fmt.value,
                'status_code': status_code,
                'error': error
            }
        )
    
    def _extract(self, params: Dict[str, Any]) -> ActionResult:
        """Extract data from parsed response."""
        data = params.get('data')
        path = params.get('path')
        default = params.get('default', None)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if not path:
            return ActionResult(success=False, message="path is required")
        
        result = self._extract_path(data, path, default)
        
        return ActionResult(
            success=True,
            message=f"Extracted from path {path}",
            data={'value': result, 'path': path}
        )
    
    def _detect(self, params: Dict[str, Any]) -> ActionResult:
        """Detect response format."""
        response = params.get('response', {})
        body = response.get('body', '')
        headers = response.get('headers', {})
        
        fmt = self._detect_format(body, headers)
        
        return ActionResult(
            success=True,
            message=f"Detected format: {fmt.value}",
            data={'format': fmt.value}
        )
    
    def _detect_format(
        self,
        body: Any,
        headers: Dict[str, str]
    ) -> ResponseFormat:
        """Detect response format from body and headers."""
        content_type = headers.get('Content-Type', '').lower()
        
        if 'json' in content_type:
            return ResponseFormat.JSON
        elif 'xml' in content_type:
            return ResponseFormat.XML
        elif 'html' in content_type:
            return ResponseFormat.HTML
        elif 'text' in content_type:
            return ResponseFormat.TEXT
        
        # Try auto-detection from body
        if isinstance(body, str):
            body_stripped = body.strip()
            
            if body_stripped.startswith('{') or body_stripped.startswith('['):
                return ResponseFormat.JSON
            
            if body_stripped.startswith('<'):
                if '<html' in body_stripped.lower():
                    return ResponseFormat.HTML
                return ResponseFormat.XML
        
        return ResponseFormat.TEXT
    
    def _parse_xml(self, xml_str: str) -> Dict:
        """Parse XML to dict."""
        try:
            root = ET.fromstring(xml_str)
            return self._xml_to_dict(root)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML: {e}")
    
    def _xml_to_dict(self, element: ET.Element) -> Dict:
        """Convert XML element to dict."""
        result = {}
        
        # Attributes
        if element.attrib:
            result['@attributes'] = element.attrib
        
        # Text content
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result['#text'] = element.text.strip()
        
        # Children
        for child in element:
            child_data = self._xml_to_dict(child)
            tag = child.tag
            
            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_data)
            else:
                result[tag] = child_data
        
        return result
    
    def _parse_html(self, html: str) -> Dict:
        """Basic HTML parsing (returns text content)."""
        # Simple extraction - in real impl would use BeautifulSoup
        import re
        
        # Remove script and style tags
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove tags but keep text
        text = re.sub(r'<[^>]+>', ' ', html)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return {'text': text}
    
    def _extract_path(
        self,
        data: Any,
        path: str,
        default: Any = None
    ) -> Any:
        """Extract data using dot notation path."""
        if not path:
            return data
        
        parts = path.split('.')
        current = data
        
        for part in parts:
            if current is None:
                return default
            
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, (list, tuple)) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return default
        
        return current if current is not None else default


class ErrorParserAction(BaseAction):
    """Parse and normalize API error responses."""
    action_type = "error_parser"
    display_name = "错误解析"
    description = "解析API错误响应并标准化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse error response.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - response: Response dict with body and status_code
                - error_map: Dict of status_code -> error type mapping
        
        Returns:
            ActionResult with normalized error.
        """
        response = params.get('response', {})
        error_map = params.get('error_map', {})
        
        status_code = response.get('status_code', 0)
        body = response.get('body', '')
        
        # Parse body for error details
        error_message = None
        error_code = None
        error_details = None
        
        try:
            if isinstance(body, str):
                parsed = json.loads(body)
            else:
                parsed = body
            
            if isinstance(parsed, dict):
                # Common error field names
                error_message = (
                    parsed.get('error') or 
                    parsed.get('message') or 
                    parsed.get('error_message') or
                    str(parsed)
                )
                error_code = (
                    parsed.get('code') or 
                    parsed.get('error_code') or
                    parsed.get('type')
                )
                error_details = parsed.get('details') or parsed.get('error_details')
        
        except (json.JSONDecodeError, TypeError):
            error_message = str(body) if body else f"HTTP {status_code}"
        
        # Map to known error type
        error_type = error_map.get(status_code, error_map.get(error_code, 'UnknownError'))
        
        return ActionResult(
            success=False,
            message=error_message,
            data={
                'error_type': error_type,
                'error_message': error_message,
                'error_code': error_code,
                'status_code': status_code,
                'details': error_details
            }
        )


class PaginationParserAction(BaseAction):
    """Parse pagination metadata from responses."""
    action_type = "pagination_parser"
    display_name = "分页解析"
    description = "从响应中解析分页元数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse pagination info from response.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - response: Response dict
                - pagination_fields: Dict mapping response fields to pagination info
                  Example: {'total': 'total_count', 'next': 'next_cursor', 'page': 'page_num'}
        
        Returns:
            ActionResult with pagination metadata.
        """
        response = params.get('response', {})
        field_map = params.get('pagination_fields', {})
        
        body = response.get('body', {})
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                body = {}
        
        # Extract pagination fields
        pagination = {}
        for target_field, source_field in field_map.items():
            value = body.get(source_field)
            if value is not None:
                pagination[target_field] = value
        
        # Also check response-level fields
        for key in ('total', 'total_count', 'page', 'page_num', 'per_page', 'next_cursor', 'has_more'):
            if key not in pagination and key in response:
                pagination[key] = response[key]
        
        return ActionResult(
            success=True,
            message=f"Parsed pagination metadata",
            data={'pagination': pagination}
        )
