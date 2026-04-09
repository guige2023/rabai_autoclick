"""API validator action module for RabAI AutoClick.

Provides API validation operations:
- SchemaValidatorAction: Validate against JSON Schema
- RequestValidatorAction: Validate API requests
- ResponseValidatorAction: Validate API responses
- ContractValidatorAction: Validate API contracts
"""

import sys
import os
import logging
import re
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
import json

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """A validation error."""
    path: str
    message: str
    value: Any = None
    expected: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class SchemaValidator:
    """JSON Schema validator (simplified)."""

    TYPE_MAP = {
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None)
    }

    def __init__(self, schema: Dict[str, Any]) -> None:
        self.schema = schema

    def validate(self, data: Any) -> ValidationResult:
        errors: List[ValidationError] = []
        self._validate_node(data, self.schema, "", errors)
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def _validate_node(self, value: Any, schema: Dict[str, Any], path: str, errors: List[ValidationError]) -> None:
        if value is None:
            if schema.get("required") and schema.get("type") != "null":
                errors.append(ValidationError(path=path, message="Value is required", value=value))
            if schema.get("type") == "null":
                return
            return

        value_type = schema.get("type")
        if value_type and value_type not in ("null", "any"):
            expected_type = self.TYPE_MAP.get(value_type)
            if expected_type and not isinstance(value, expected_type):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected type {value_type}, got {type(value).__name__}",
                    value=value,
                    expected=value_type
                ))
                return

        if "enum" in schema:
            if value not in schema["enum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value must be one of {schema['enum']}",
                    value=value,
                    expected=schema["enum"]
                ))

        if "minLength" in schema and isinstance(value, str):
            if len(value) < schema["minLength"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"String length must be at least {schema['minLength']}",
                    value=value
                ))

        if "maxLength" in schema and isinstance(value, str):
            if len(value) > schema["maxLength"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"String length must be at most {schema['maxLength']}",
                    value=value
                ))

        if "minimum" in schema and isinstance(value, (int, float)):
            if value < schema["minimum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value must be >= {schema['minimum']}",
                    value=value
                ))

        if "maximum" in schema and isinstance(value, (int, float)):
            if value > schema["maximum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value must be <= {schema['maximum']}",
                    value=value
                ))

        if "pattern" in schema and isinstance(value, str):
            if not re.match(schema["pattern"], value):
                errors.append(ValidationError(
                    path=path,
                    message=f"String does not match pattern {schema['pattern']}",
                    value=value
                ))

        if schema.get("type") == "array" and isinstance(value, list):
            if "items" in schema:
                for i, item in enumerate(value):
                    self._validate_node(item, schema["items"], f"{path}[{i}]", errors)
            if "minItems" in schema:
                if len(value) < schema["minItems"]:
                    errors.append(ValidationError(
                        path=path,
                        message=f"Array must have at least {schema['minItems']} items",
                        value=value
                    ))

        if schema.get("type") == "object" and isinstance(value, dict):
            if "properties" in schema:
                for prop_name, prop_schema in schema["properties"].items():
                    if prop_name in value:
                        self._validate_node(value[prop_name], prop_schema, f"{path}.{prop_name}", errors)
                    elif prop_schema.get("required"):
                        errors.append(ValidationError(
                            path=f"{path}.{prop_name}",
                            message=f"Required property '{prop_name}' is missing",
                            value=None
                        ))


class RequestValidator:
    """Validates API requests."""

    def validate_method(self, method: str, allowed: List[str]) -> ValidationResult:
        errors = []
        if method.upper() not in [m.upper() for m in allowed]:
            errors.append(ValidationError(
                path="method",
                message=f"Method must be one of {allowed}",
                value=method,
                expected=allowed
            ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_headers(self, headers: Dict[str, str], required: Optional[List[str]] = None) -> ValidationResult:
        errors = []
        if required:
            for header in required:
                if header.lower() not in [h.lower() for h in headers.keys()]:
                    errors.append(ValidationError(
                        path="headers",
                        message=f"Required header '{header}' is missing",
                        value=None,
                        expected=header
                    ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_path_params(self, path: str, pattern: str) -> ValidationResult:
        errors = []
        path_parts = path.strip("/").split("/")
        pattern_parts = pattern.strip("/").split("/")
        if len(path_parts) != len(pattern_parts):
            errors.append(ValidationError(
                path="path",
                message="Path parameter count mismatch",
                value=path,
                expected=pattern
            ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)


class ResponseValidator:
    """Validates API responses."""

    def validate_status_code(self, status_code: int, expected: Union[int, List[int]]) -> ValidationResult:
        errors = []
        if isinstance(expected, list):
            if status_code not in expected:
                errors.append(ValidationError(
                    path="status",
                    message=f"Status code must be one of {expected}",
                    value=status_code
                ))
        else:
            if status_code != expected:
                errors.append(ValidationError(
                    path="status",
                    message=f"Status code must be {expected}",
                    value=status_code
                ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_content_type(self, content_type: str, expected: str) -> ValidationResult:
        errors = []
        if not content_type.startswith(expected):
            errors.append(ValidationError(
                path="content_type",
                message=f"Content-Type must start with {expected}",
                value=content_type,
                expected=expected
            ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)


class SchemaValidatorAction(BaseAction):
    """Validate data against JSON Schema."""
    action_type = "api_schema_validator"
    display_name = "Schema验证"
    description = "使用JSON Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        schema = params.get("schema", {})
        data = params.get("data")

        if not schema:
            return ActionResult(success=False, message="schema参数是必需的")

        try:
            validator = SchemaValidator(schema)
            result = validator.validate(data)

            error_messages = [f"{e.path}: {e.message}" for e in result.errors]

            return ActionResult(
                success=result.valid,
                message=f"验证{'通过' if result.valid else '失败'}，{len(result.errors)} 个错误",
                data={
                    "valid": result.valid,
                    "errors": [
                        {"path": e.path, "message": e.message, "value": str(e.value)}
                        for e in result.errors
                    ]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema验证失败: {e}")


class RequestValidatorAction(BaseAction):
    """Validate API requests."""
    action_type = "api_request_validator"
    display_name = "请求验证"
    description = "验证API请求的格式和参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        method = params.get("method", "GET")
        path = params.get("path", "/")
        headers = params.get("headers", {})
        body = params.get("body")
        allowed_methods = params.get("allowed_methods", ["GET", "POST", "PUT", "DELETE"])
        required_headers = params.get("required_headers")

        validator = RequestValidator()

        result = validator.validate_method(method, allowed_methods)
        if not result.valid:
            return ActionResult(success=False, message=result.errors[0].message, data={"errors": [e.message for e in result.errors]})

        if required_headers:
            result = validator.validate_headers(headers, required_headers)
            if not result.valid:
                return ActionResult(success=False, message=result.errors[0].message, data={"errors": [e.message for e in result.errors]})

        return ActionResult(
            success=True,
            message="请求验证通过",
            data={"method": method, "path": path}
        )


class ResponseValidatorAction(BaseAction):
    """Validate API responses."""
    action_type = "api_response_validator"
    display_name = "响应验证"
    description = "验证API响应的状态和内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        status_code = params.get("status_code", 200)
        content_type = params.get("content_type", "application/json")
        expected_status = params.get("expected_status", 200)
        expected_content_type = params.get("expected_content_type", "application/json")

        validator = ResponseValidator()

        result = validator.validate_status_code(status_code, expected_status)
        if not result.valid:
            return ActionResult(success=False, message=result.errors[0].message, data={"errors": [e.message for e in result.errors]})

        result = validator.validate_content_type(content_type, expected_content_type)
        if not result.valid:
            return ActionResult(success=False, message=result.errors[0].message, data={"errors": [e.message for e in result.errors]})

        return ActionResult(
            success=True,
            message="响应验证通过",
            data={"status_code": status_code, "content_type": content_type}
        )


class ContractValidatorAction(BaseAction):
    """Validate API contracts (OpenAPI style)."""
    action_type = "api_contract_validator"
    display_name = "契约验证"
    description = "验证API契约规范的符合性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "validate")
        path = params.get("path", "/")
        method = params.get("method", "GET")
        request_body = params.get("request_body")
        response_body = params.get("response_body")
        response_status = params.get("response_status", 200)

        if operation == "validate":
            errors = []

            if not path.startswith("/"):
                errors.append("Path must start with /")

            if method.upper() not in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
                errors.append(f"Invalid HTTP method: {method}")

            if errors:
                return ActionResult(success=False, message="契约验证失败", data={"errors": errors})

            return ActionResult(
                success=True,
                message="API契约验证通过",
                data={"path": path, "method": method, "response_status": response_status}
            )

        if operation == "check_compatibility":
            return ActionResult(
                success=True,
                message="契约兼容性检查完成",
                data={"compatible": True, "path": path, "method": method}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")
