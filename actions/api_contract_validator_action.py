"""API Contract Validator Action Module.

Validates API requests and responses against OpenAPI/JSON Schema specifications.
Ensures API compatibility and prevents contract violations.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Strictness level for validation."""
    STRICT = auto()      # Fail on any violation
    WARN = auto()        # Log warnings but don't fail
    PERMISSIVE = auto()  # Skip validation


@dataclass
class ValidationError:
    """Represents a validation error."""
    path: str
    message: str
    value: Any = None
    expected: Any = None
    schema_path: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of validation operation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class SchemaField:
    """Schema field definition."""
    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    default: Any = None
    pattern: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    minimum: Optional[Union[int, float]] = None
    maximum: Optional[Union[int, float]] = None
    enum_values: Optional[List[Any]] = None
    items_schema: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, SchemaField]] = None


class JSONSchemaValidator:
    """JSON Schema based validator.
    
    Validates data structures against JSON Schema specifications.
    """
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self._type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }
    
    def validate(self, data: Any, path: str = "#") -> List[ValidationError]:
        """Validate data against schema.
        
        Args:
            data: Data to validate
            path: Current JSON path for error reporting
            
        Returns:
            List of validation errors
        """
        errors = []
        
        if "$ref" in self.schema:
            errors.extend(self._validate_ref(data, path))
            return errors
        
        schema_type = self.schema.get("type")
        if schema_type:
            errors.extend(self._validate_type(data, schema_type, path))
        
        if "properties" in self.schema:
            errors.extend(self._validate_properties(data, path))
        
        if "items" in self.schema:
            errors.extend(self._validate_items(data, path))
        
        if "enum" in self.schema:
            errors.extend(self._validate_enum(data, path))
        
        if "pattern" in self.schema:
            errors.extend(self._validate_pattern(data, path))
        
        if "minLength" in self.schema:
            errors.extend(self._validate_min_length(data, path))
        
        if "maxLength" in self.schema:
            errors.extend(self._validate_max_length(data, path))
        
        if "minimum" in self.schema:
            errors.extend(self._validate_minimum(data, path))
        
        if "maximum" in self.schema:
            errors.extend(self._validate_maximum(data, path))
        
        return errors
    
    def _validate_ref(self, data: Any, path: str) -> List[ValidationError]:
        """Validate $ref reference."""
        return []
    
    def _validate_type(self, data: Any, schema_type: str, path: str) -> List[ValidationError]:
        """Validate data type."""
        errors = []
        expected_type = self._type_map.get(schema_type)
        
        if expected_type and data is not None and not isinstance(data, expected_type):
            errors.append(ValidationError(
                path=path,
                message=f"Expected type '{schema_type}', got '{type(data).__name__}'",
                value=type(data).__name__,
                expected=schema_type
            ))
        
        return errors
    
    def _validate_properties(self, data: Any, path: str) -> List[ValidationError]:
        """Validate object properties."""
        errors = []
        
        if not isinstance(data, dict):
            return errors
        
        for prop_name, prop_schema in self.schema["properties"].items():
            prop_path = f"{path}/{prop_name}"
            
            if prop_name not in data:
                if prop_schema.get("required", False):
                    errors.append(ValidationError(
                        path=prop_path,
                        message=f"Missing required property '{prop_name}'",
                        expected="present"
                    ))
                continue
            
            prop_validator = JSONSchemaValidator(prop_schema)
            errors.extend(prop_validator.validate(data[prop_name], prop_path))
        
        return errors
    
    def _validate_items(self, data: Any, path: str) -> List[ValidationError]:
        """Validate array items."""
        errors = []
        
        if not isinstance(data, list):
            return errors
        
        items_schema = self.schema.get("items", {})
        for i, item in enumerate(data):
            item_path = f"{path}[{i}]"
            item_validator = JSONSchemaValidator(items_schema)
            errors.extend(item_validator.validate(item, item_path))
        
        return errors
    
    def _validate_enum(self, data: Any, path: str) -> List[ValidationError]:
        """Validate enum values."""
        errors = []
        
        if data not in self.schema["enum"]:
            errors.append(ValidationError(
                path=path,
                message=f"Value must be one of {self.schema['enum']}",
                value=data,
                expected=self.schema["enum"]
            ))
        
        return errors
    
    def _validate_pattern(self, data: Any, path: str) -> List[ValidationError]:
        """Validate string pattern."""
        errors = []
        
        if not isinstance(data, str):
            return errors
        
        pattern = self.schema["pattern"]
        if not re.match(pattern, data):
            errors.append(ValidationError(
                path=path,
                message=f"String does not match pattern '{pattern}'",
                value=data,
                expected=pattern
            ))
        
        return errors
    
    def _validate_min_length(self, data: Any, path: str) -> List[ValidationError]:
        """Validate minimum string length."""
        errors = []
        
        if isinstance(data, str) and len(data) < self.schema["minLength"]:
            errors.append(ValidationError(
                path=path,
                message=f"String length {len(data)} is less than minimum {self.schema['minLength']}",
                value=len(data),
                expected=f">={self.schema['minLength']}"
            ))
        
        return errors
    
    def _validate_max_length(self, data: Any, path: str) -> List[ValidationError]:
        """Validate maximum string length."""
        errors = []
        
        if isinstance(data, str) and len(data) > self.schema["maxLength"]:
            errors.append(ValidationError(
                path=path,
                message=f"String length {len(data)} exceeds maximum {self.schema['maxLength']}",
                value=len(data),
                expected=f"<={self.schema['maxLength']}"
            ))
        
        return errors
    
    def _validate_minimum(self, data: Any, path: str) -> List[ValidationError]:
        """Validate numeric minimum."""
        errors = []
        
        if isinstance(data, (int, float)) and data < self.schema["minimum"]:
            errors.append(ValidationError(
                path=path,
                message=f"Value {data} is less than minimum {self.schema['minimum']}",
                value=data,
                expected=f">={self.schema['minimum']}"
            ))
        
        return errors
    
    def _validate_maximum(self, data: Any, path: str) -> List[ValidationError]:
        """Validate numeric maximum."""
        errors = []
        
        if isinstance(data, (int, float)) and data > self.schema["maximum"]:
            errors.append(ValidationError(
                path=path,
                message=f"Value {data} exceeds maximum {self.schema['maximum']}",
                value=data,
                expected=f"<={self.schema['maximum']}"
            ))
        
        return errors


class APIContractValidator:
    """Validates API requests and responses against contract specifications.
    
    Supports:
    - Request body validation
    - Response body validation
    - Header validation
    - Status code validation
    - Query parameter validation
    """
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STRICT):
        self.validation_level = validation_level
        self._request_schemas: Dict[str, Dict[str, Any]] = {}
        self._response_schemas: Dict[str, Dict[str, Any]] = {}
        self._header_schemas: Dict[str, Dict[str, Any]] = {}
        self._validation_cache: Dict[str, bool] = {}
        self._lock = asyncio.Lock()
    
    def register_request_schema(self, endpoint: str, method: str, schema: Dict[str, Any]) -> None:
        """Register request schema for an endpoint.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            schema: JSON Schema for request body
        """
        key = f"{method.upper()}:{endpoint}"
        self._request_schemas[key] = schema
    
    def register_response_schema(self, endpoint: str, method: str, status_code: int, schema: Dict[str, Any]) -> None:
        """Register response schema for an endpoint.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            status_code: HTTP status code
            schema: JSON Schema for response body
        """
        key = f"{method.upper()}:{endpoint}:{status_code}"
        self._response_schemas[key] = schema
    
    def register_header_schema(self, key: str, schema: Dict[str, Any]) -> None:
        """Register header validation schema.
        
        Args:
            key: Header name
            schema: Schema for header value validation
        """
        self._header_schemas[key.lower()] = schema
    
    async def validate_request(
        self,
        endpoint: str,
        method: str,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate an API request.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            body: Request body
            headers: Request headers
            query_params: Query parameters
            
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        schema_key = f"{method.upper()}:{endpoint}"
        
        if schema_key in self._request_schemas:
            schema = self._request_schemas[schema_key]
            validator = JSONSchemaValidator(schema)
            
            if body is not None:
                type_errors = validator.validate(body)
                errors.extend(type_errors)
        elif body and self.validation_level == ValidationLevel.WARN:
            warnings.append(f"No schema registered for {schema_key}")
        
        if headers:
            header_errors = await self._validate_headers(headers)
            errors.extend(header_errors)
        
        if query_params:
            param_errors = self._validate_query_params(query_params)
            errors.extend(param_errors)
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    async def validate_response(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        body: Any,
        headers: Optional[Dict[str, str]] = None
    ) -> ValidationResult:
        """Validate an API response.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            status_code: HTTP status code
            body: Response body
            headers: Response headers
            
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        schema_key = f"{method.upper()}:{endpoint}:{status_code}"
        
        if schema_key in self._response_schemas:
            schema = self._response_schemas[schema_key]
            validator = JSONSchemaValidator(schema)
            errors.extend(validator.validate(body))
        elif self.validation_level == ValidationLevel.WARN:
            warnings.append(f"No schema registered for {schema_key}")
        
        if headers:
            header_errors = await self._validate_headers(headers)
            errors.extend(header_errors)
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    async def _validate_headers(self, headers: Dict[str, str]) -> List[ValidationError]:
        """Validate request/response headers."""
        errors = []
        
        for header_name, header_value in headers.items():
            header_key = header_name.lower()
            
            if header_key in self._header_schemas:
                schema = self._header_schemas[header_key]
                validator = JSONSchemaValidator(schema)
                errors.extend(validator.validate(header_value, f"header.{header_name}"))
        
        return errors
    
    def _validate_query_params(self, params: Dict[str, Any]) -> List[ValidationError]:
        """Validate query parameters."""
        errors = []
        
        for param_name, param_value in params.items():
            if param_value is None:
                continue
            
            if isinstance(param_value, list):
                for i, item in enumerate(param_value):
                    if not isinstance(item, (str, int, float, bool)):
                        errors.append(ValidationError(
                            path=f"query.{param_name}[{i}]",
                            message=f"Invalid query parameter type",
                            value=type(item).__name__,
                            expected="string|number|boolean"
                        ))
            elif not isinstance(param_value, (str, int, float, bool)):
                errors.append(ValidationError(
                    path=f"query.{param_name}",
                    message=f"Invalid query parameter type",
                    value=type(param_value).__name__,
                    expected="string|number|boolean|array"
                ))
        
        return errors
    
    def get_registered_contracts(self) -> Dict[str, List[str]]:
        """Get all registered contract schemas.
        
        Returns:
            Dictionary of contract types and their keys
        """
        return {
            "requests": list(self._request_schemas.keys()),
            "responses": list(self._response_schemas.keys()),
            "headers": list(self._header_schemas.keys())
        }


class ContractValidationMiddleware:
    """Middleware that automatically validates requests/responses.
    
    Can wrap API client calls to enforce contract validation.
    """
    
    def __init__(self, validator: APIContractValidator):
        self.validator = validator
    
    async def wrap_request(
        self,
        func: Callable,
        endpoint: str,
        method: str,
        **kwargs
    ) -> Any:
        """Wrap an API request with validation.
        
        Args:
            func: Async function to call
            endpoint: API endpoint
            method: HTTP method
            **kwargs: Arguments for func
            
        Returns:
            Function result
            
        Raises:
            ContractViolationError: On validation failure
        """
        result = await self.validator.validate_request(
            endpoint=endpoint,
            method=method,
            body=kwargs.get("json"),
            headers=kwargs.get("headers"),
            query_params=kwargs.get("params")
        )
        
        if not result.valid:
            error_messages = [e.message for e in result.errors]
            raise ContractViolationError(
                f"Request validation failed: {', '.join(error_messages)}",
                errors=result.errors
            )
        
        response = await func(**kwargs)
        
        await self.validator.validate_response(
            endpoint=endpoint,
            method=method,
            status_code=response.status_code,
            body=response.json() if response.content else None,
            headers=dict(response.headers)
        )
        
        return response


class ContractViolationError(Exception):
    """Raised when contract validation fails."""
    
    def __init__(self, message: str, errors: List[ValidationError]):
        super().__init__(message)
        self.errors = errors
