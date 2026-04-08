"""Parser action module for RabAI AutoClick.

Provides parsing actions for various data formats including
CSV, JSON Lines, TSV, and custom delimited formats.
"""

import csv
import io
import json
import sys
import os
import time
from typing import Any, Dict, List, Optional, Union, TextIO
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ParseResult:
    """Result of a parsing operation.
    
    Attributes:
        success: Whether parsing succeeded.
        data: Parsed data.
        errors: Any parsing errors.
        line_count: Number of lines processed.
        duration: Parsing time.
    """
    success: bool
    data: Any = None
    errors: List[str] = field(default_factory=list)
    line_count: int = 0
    duration: float = 0.0


class DelimitedParser:
    """Parser for delimited text formats (CSV, TSV, etc.)."""
    
    def __init__(self, delimiter: str = ',', quote_char: str = '"'):
        """Initialize parser.
        
        Args:
            delimiter: Field delimiter character.
            quote_char: Quote character.
        """
        self.delimiter = delimiter
        self.quote_char = quote_char
    
    def parse_string(self, text: str, has_header: bool = True) -> ParseResult:
        """Parse delimited text from string.
        
        Args:
            text: Delimited text.
            has_header: Whether first row is header.
        
        Returns:
            ParseResult with parsed data.
        """
        start_time = time.time()
        errors = []
        rows = []
        line_count = 0
        
        try:
            reader = csv.reader(io.StringIO(text), delimiter=self.delimiter, quotechar=self.quote_char)
            
            for row in reader:
                line_count += 1
                rows.append(row)
            
            data = rows
            
            if has_header and rows:
                headers = rows[0]
                data = []
                for row in rows[1:]:
                    if len(row) == len(headers):
                        data.append(dict(zip(headers, row)))
                    else:
                        errors.append(f"Line {line_count}: column count mismatch")
            
            return ParseResult(
                success=True,
                data=data,
                errors=errors,
                line_count=line_count,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ParseResult(
                success=False,
                data=None,
                errors=[str(e)],
                line_count=line_count,
                duration=time.time() - start_time
            )
    
    def to_string(self, data: List[Dict], headers: List[str] = None) -> str:
        """Convert data to delimited string.
        
        Args:
            data: List of dictionaries.
            headers: Column order (derived from first dict if None).
        
        Returns:
            Delimited string.
        """
        if not data:
            return ""
        
        if headers is None:
            headers = list(data[0].keys())
        
        output = io.StringIO()
        writer = csv.writer(output, delimiter=self.delimiter, quotechar=self.quote_char)
        
        writer.writerow(headers)
        
        for row in data:
            writer.writerow([row.get(h, '') for h in headers])
        
        return output.getvalue()


class JSONLinesParser:
    """Parser for JSON Lines (newline-delimited JSON) format."""
    
    def parse_string(self, text: str) -> ParseResult:
        """Parse JSON Lines text.
        
        Args:
            text: JSON Lines text.
        
        Returns:
            ParseResult with parsed data.
        """
        start_time = time.time()
        errors = []
        data = []
        line_count = 0
        
        for line_num, line in enumerate(text.strip().split('\n'), 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                data.append(json.loads(line))
                line_count += 1
            except json.JSONDecodeError as e:
                errors.append(f"Line {line_num}: {str(e)}")
        
        return ParseResult(
            success=len(errors) == 0,
            data=data,
            errors=errors,
            line_count=line_count,
            duration=time.time() - start_time
        )
    
    def to_string(self, data: List[Dict]) -> str:
        """Convert data to JSON Lines string.
        
        Args:
            data: List of dictionaries.
        
        Returns:
            JSON Lines string.
        """
        return '\n'.join(json.dumps(item, ensure_ascii=False) for item in data)


class KeyValueParser:
    """Parser for key=value format (like env files)."""
    
    def parse_string(self, text: str, delimiter: str = '=') -> ParseResult:
        """Parse key=value text.
        
        Args:
            text: Key=value text.
            delimiter: Key-value delimiter.
        
        Returns:
            ParseResult with parsed data.
        """
        start_time = time.time()
        errors = []
        data = {}
        line_count = 0
        
        for line_num, line in enumerate(text.strip().split('\n'), 1):
            line = line.strip()
            
            if not line or line.startswith('#'):
                continue
            
            if delimiter not in line:
                errors.append(f"Line {line_num}: missing delimiter")
                continue
            
            parts = line.split(delimiter, 1)
            key = parts[0].strip()
            value = parts[1].strip()
            
            data[key] = value
            line_count += 1
        
        return ParseResult(
            success=len(errors) == 0,
            data=data,
            errors=errors,
            line_count=line_count,
            duration=time.time() - start_time
        )
    
    def to_string(self, data: Dict[str, Any], delimiter: str = '=') -> str:
        """Convert dict to key=value string.
        
        Args:
            data: Dictionary to convert.
            delimiter: Key-value delimiter.
        
        Returns:
            Key=value string.
        """
        lines = [f"{k}{delimiter}{v}" for k, v in data.items()]
        return '\n'.join(lines)


class ParseCSVAction(BaseAction):
    """Parse CSV text."""
    action_type = "parse_csv"
    display_name = "解析CSV"
    description = "解析CSV格式数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse CSV.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, delimiter, has_header.
        
        Returns:
            ActionResult with parsed data.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', ',')
        has_header = params.get('has_header', True)
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        try:
            parser = DelimitedParser(delimiter=delimiter)
            result = parser.parse_string(text, has_header=has_header)
            
            return ActionResult(
                success=result.success,
                message=f"Parsed {result.line_count} lines",
                data={
                    "data": result.data,
                    "line_count": result.line_count,
                    "errors": result.errors,
                    "duration_ms": round(result.duration * 1000, 2)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV parse failed: {str(e)}")


class ParseJSONLinesAction(BaseAction):
    """Parse JSON Lines text."""
    action_type = "parse_jsonlines"
    display_name = "解析JSONL"
    description = "解析JSON Lines格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse JSON Lines.
        
        Args:
            context: Execution context.
            params: Dict with keys: text.
        
        Returns:
            ActionResult with parsed data.
        """
        text = params.get('text', '')
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        try:
            parser = JSONLinesParser()
            result = parser.parse_string(text)
            
            return ActionResult(
                success=result.success,
                message=f"Parsed {result.line_count} JSON objects",
                data={
                    "data": result.data,
                    "line_count": result.line_count,
                    "errors": result.errors,
                    "duration_ms": round(result.duration * 1000, 2)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON Lines parse failed: {str(e)}")


class ParseKeyValueAction(BaseAction):
    """Parse key=value text."""
    action_type = "parse_keyvalue"
    display_name = "解析键值"
    description = "解析键值格式数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse key=value.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, delimiter.
        
        Returns:
            ActionResult with parsed data.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', '=')
        
        if not text:
            return ActionResult(success=False, message="text is required")
        
        try:
            parser = KeyValueParser()
            result = parser.parse_string(text, delimiter=delimiter)
            
            return ActionResult(
                success=result.success,
                message=f"Parsed {result.line_count} key-value pairs",
                data={
                    "data": result.data,
                    "line_count": result.line_count,
                    "errors": result.errors,
                    "duration_ms": round(result.duration * 1000, 2)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Key-value parse failed: {str(e)}")


class ToCSVAction(BaseAction):
    """Convert data to CSV string."""
    action_type = "to_csv"
    display_name = "转CSV"
    description = "将数据转换为CSV"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert to CSV.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, delimiter, headers.
        
        Returns:
            ActionResult with CSV string.
        """
        data = params.get('data', [])
        delimiter = params.get('delimiter', ',')
        headers = params.get('headers', None)
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            parser = DelimitedParser(delimiter=delimiter)
            csv_text = parser.to_string(data, headers=headers)
            
            return ActionResult(
                success=True,
                message=f"Converted {len(data)} rows to CSV",
                data={
                    "csv": csv_text,
                    "row_count": len(data)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV conversion failed: {str(e)}")


class ToJSONLinesAction(BaseAction):
    """Convert data to JSON Lines string."""
    action_type = "to_jsonlines"
    display_name = "转JSONL"
    description = "将数据转换为JSON Lines"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert to JSON Lines.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with JSON Lines string.
        """
        data = params.get('data', [])
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not isinstance(data, list):
            data = [data]
        
        try:
            parser = JSONLinesParser()
            jsonl_text = parser.to_string(data)
            
            return ActionResult(
                success=True,
                message=f"Converted {len(data)} objects to JSON Lines",
                data={
                    "jsonl": jsonl_text,
                    "object_count": len(data)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON Lines conversion failed: {str(e)}")
