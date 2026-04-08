"""Data importer action module for RabAI AutoClick.

Provides data import from various formats,
validation, and transformation on import.
"""

import json
import csv
import io
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
import base64
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataImporterAction(BaseAction):
    """Import data from various formats with validation.
    
    Supports JSON, CSV, TSV, XML, and base64 formats.
    Provides schema validation and data transformation.
    """
    action_type = "data_importer"
    display_name = "数据导入"
    description = "从多种格式导入数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute import operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, validate, transform.
        
        Returns:
            ActionResult with imported records.
        """
        data = params.get('data')
        if not data:
            return ActionResult(success=False, message="No data to import")
        
        format_type = params.get('format', 'auto').lower()
        validate = params.get('validate', False)
        
        if format_type == 'auto':
            format_type = self._detect_format(data)
        
        if format_type == 'json':
            return self._import_json(data, validate, params)
        elif format_type == 'csv':
            return self._import_csv(data, validate, params)
        elif format_type == 'tsv':
            return self._import_tsv(data, validate, params)
        elif format_type == 'base64':
            return self._import_base64(data, validate, params)
        elif format_type == 'xml':
            return self._import_xml(data, validate, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported format: {format_type}"
            )
    
    def _detect_format(self, data: str) -> str:
        """Auto-detect data format."""
        data = data.strip()
        
        if data.startswith('{') or data.startswith('['):
            return 'json'
        elif data.startswith('<?xml'):
            return 'xml'
        elif ',' in data and '\t' not in data:
            return 'csv'
        elif '\t' in data:
            return 'tsv'
        else:
            try:
                base64.b64decode(data)
                return 'base64'
            except Exception:
                return 'json'
    
    def _import_json(
        self,
        data: str,
        validate: bool,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import JSON data."""
        try:
            if isinstance(data, str):
                parsed = json.loads(data)
            else:
                parsed = data
            
            if isinstance(parsed, dict):
                records = [parsed]
            elif isinstance(parsed, list):
                records = parsed
            else:
                records = [{'value': parsed}]
            
            validated = []
            errors = []
            
            for idx, record in enumerate(records):
                if validate:
                    is_valid, err_msg = self._validate_record(record, params)
                    if is_valid:
                        validated.append(record)
                    else:
                        errors.append({'index': idx, 'error': err_msg})
                else:
                    validated.append(record)
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Imported {len(validated)} records from JSON",
                data={
                    'format': 'json',
                    'records': validated,
                    'count': len(validated),
                    'errors': errors
                }
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"JSON parse error: {e}"
            )
    
    def _import_csv(
        self,
        data: str,
        validate: bool,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import CSV data."""
        try:
            if isinstance(data, str):
                csv_data = io.StringIO(data)
            else:
                csv_data = io.StringIO(data.decode('utf-8'))
            
            reader = csv.DictReader(csv_data)
            records = list(reader)
            
            validated = []
            errors = []
            
            for idx, record in enumerate(records):
                if validate:
                    is_valid, err_msg = self._validate_record(record, params)
                    if is_valid:
                        validated.append(record)
                    else:
                        errors.append({'index': idx, 'error': err_msg})
                else:
                    validated.append(record)
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Imported {len(validated)} records from CSV",
                data={
                    'format': 'csv',
                    'records': validated,
                    'count': len(validated),
                    'errors': errors
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CSV import error: {e}"
            )
    
    def _import_tsv(
        self,
        data: str,
        validate: bool,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import TSV data."""
        try:
            if isinstance(data, str):
                tsv_data = io.StringIO(data)
            else:
                tsv_data = io.StringIO(data.decode('utf-8'))
            
            reader = csv.DictReader(tsv_data, delimiter='\t')
            records = list(reader)
            
            validated = []
            errors = []
            
            for idx, record in enumerate(records):
                if validate:
                    is_valid, err_msg = self._validate_record(record, params)
                    if is_valid:
                        validated.append(record)
                    else:
                        errors.append({'index': idx, 'error': err_msg})
                else:
                    validated.append(record)
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Imported {len(validated)} records from TSV",
                data={
                    'format': 'tsv',
                    'records': validated,
                    'count': len(validated),
                    'errors': errors
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"TSV import error: {e}"
            )
    
    def _import_base64(
        self,
        data: str,
        validate: bool,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import base64-encoded data."""
        try:
            decoded = base64.b64decode(data).decode('utf-8')
            return self._import_json(decoded, validate, params)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64 decode error: {e}"
            )
    
    def _import_xml(
        self,
        data: str,
        validate: bool,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Import XML data (simplified)."""
        import re
        
        try:
            record_pattern = re.compile(r'<record>(.*?)</record>', re.DOTALL)
            field_pattern = re.compile(r'<(\w+)>(.*?)</\1>', re.DOTALL)
            
            records = []
            
            for match in record_pattern.finditer(data):
                record_text = match.group(1)
                record = {}
                
                for field_match in field_pattern.finditer(record_text):
                    key = field_match.group(1)
                    value = field_match.group(2)
                    record[key] = value
                
                if record:
                    records.append(record)
            
            validated = []
            errors = []
            
            for idx, record in enumerate(records):
                if validate:
                    is_valid, err_msg = self._validate_record(record, params)
                    if is_valid:
                        validated.append(record)
                    else:
                        errors.append({'index': idx, 'error': err_msg})
                else:
                    validated.append(record)
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Imported {len(validated)} records from XML",
                data={
                    'format': 'xml',
                    'records': validated,
                    'count': len(validated),
                    'errors': errors
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML import error: {e}"
            )
    
    def _validate_record(
        self,
        record: Dict[str, Any],
        params: Dict[str, Any]
    ) -> tuple:
        """Validate a single record."""
        required_fields = params.get('required_fields', [])
        
        for field in required_fields:
            if field not in record or record[field] == '':
                return False, f"Missing required field: {field}"
        
        return True, None
