"""API validator action module for RabAI AutoClick.

Provides API request/response validation:
- RequestValidatorAction: Validate API request structure
- ResponseValidatorAction: Validate API response structure
- APIKeyValidatorAction: Validate API keys
- EndpointValidatorAction: Validate API endpoints
- RateLimitValidatorAction: Validate rate limit compliance
"""

import re
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RequestValidatorAction(BaseAction):
    """Validate API request structure."""
    action_type = "api_request_validator"
    display_name = "请求验证器"
    description = "验证API请求结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            request = params.get("request", {})
            required_headers = params.get("required_headers", [])
            required_fields = params.get("required_fields", [])
            allowed_methods = params.get("allowed_methods", ["GET", "POST", "PUT", "DELETE"])
            max_body_size = params.get("max_body_size", 1024 * 1024)

            errors = []
            warnings = []

            method = request.get("method", "GET")
            if method not in allowed_methods:
                errors.append(f"Method '{method}' not allowed. Allowed: {allowed_methods}")

            headers = request.get("headers", {})
            for required_header in required_headers:
                if required_header not in headers:
                    errors.append(f"Required header missing: {required_header}")

            body = request.get("body")
            if body:
                if isinstance(body, dict):
                    body_size = len(str(body))
                else:
                    body_size = len(str(body))
                if body_size > max_body_size:
                    errors.append(f"Body size {body_size} exceeds max {max_body_size}")

            url = request.get("url", "")
            if not url:
                errors.append("URL is required")
            elif not re.match(r"^https?://", url):
                errors.append(f"Invalid URL scheme: {url}")

            if required_fields:
                body_data = body if isinstance(body, dict) else {}
                for field in required_fields:
                    if field not in body_data:
                        errors.append(f"Required field missing in body: {field}")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "method": method,
                    "url": url,
                    "has_body": body is not None
                },
                message=f"Request validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Request validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["request"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"required_headers": [], "required_fields": [], "allowed_methods": ["GET", "POST", "PUT", "DELETE"], "max_body_size": 1048576}


class ResponseValidatorAction(BaseAction):
    """Validate API response structure."""
    action_type = "api_response_validator"
    display_name = "响应验证器"
    description = "验证API响应结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            expected_status_codes = params.get("expected_status_codes", [200])
            required_fields = params.get("required_fields", [])
            validate_schema = params.get("validate_schema", False)
            schema = params.get("schema", {})

            errors = []
            warnings = []

            status_code = response.get("status_code", 0)
            if status_code not in expected_status_codes:
                errors.append(f"Unexpected status code: {status_code}. Expected: {expected_status_codes}")

            headers = response.get("headers", {})
            content_type = headers.get("Content-Type", "")
            if not content_type:
                warnings.append("No Content-Type header")

            body = response.get("body")
            if body is None:
                if params.get("allow_empty_body", False):
                    warnings.append("Empty response body")
                else:
                    errors.append("Response body is empty")

            if required_fields and body:
                body_data = body if isinstance(body, dict) else {}
                if not isinstance(body_data, dict):
                    errors.append("Body is not a dictionary, cannot validate fields")
                else:
                    for field in required_fields:
                        if field not in body_data:
                            errors.append(f"Required field missing in response: {field}")

            if validate_schema and body and isinstance(body, dict):
                schema_validator = SchemaValidatorAction()
                schema_result = schema_validator.execute(context, {"data": body, "schema": schema})
                if not schema_result.success:
                    errors.extend([f"Schema: {e}" for e in schema_result.data.get("errors", [])])

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "status_code": status_code,
                    "has_body": body is not None
                },
                message=f"Response validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["response"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"expected_status_codes": [200], "required_fields": [], "validate_schema": False, "schema": {}, "allow_empty_body": False}


class SchemaValidatorAction(BaseAction):
    """Helper schema validator for response validation."""
    action_type = "api_schema_validator_helper"
    display_name = "Schema验证器"
    description = "验证Schema结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            errors = []

            required_fields = schema.get("required", [])
            for field in required_fields:
                if field not in data:
                    errors.append(f"Required field missing: {field}")

            field_schemas = schema.get("fields", {})
            for field, field_schema in field_schemas.items():
                if field not in data:
                    continue
                expected_type = field_schema.get("type")
                if expected_type:
                    type_map = {"string": str, "integer": int, "number": (int, float), "boolean": bool, "array": list, "object": dict}
                    expected = type_map.get(expected_type)
                    if expected and not isinstance(data[field], expected):
                        errors.append(f"Field '{field}': expected {expected_type}")

            return ActionResult(success=len(errors) == 0, data={"errors": errors}, message=f"Schema: {len(errors)} errors")

        except Exception as e:
            return ActionResult(success=False, message=f"Schema validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data", "schema"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class APIKeyValidatorAction(BaseAction):
    """Validate API keys."""
    action_type = "api_key_validator"
    display_name = "API密钥验证器"
    description = "验证API密钥格式和有效性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_key = params.get("api_key", "")
            key_format = params.get("key_format", "any")
            prefix = params.get("prefix")
            min_length = params.get("min_length", 16)
            max_length = params.get("max_length", 128)

            errors = []
            warnings = []

            if not api_key:
                errors.append("API key is empty")
                return ActionResult(success=False, data={"is_valid": False, "errors": errors}, message="API key validation failed")

            if len(api_key) < min_length:
                errors.append(f"API key too short: {len(api_key)} < {min_length}")
            if len(api_key) > max_length:
                errors.append(f"API key too long: {len(api_key)} > {max_length}")

            if prefix and not api_key.startswith(prefix):
                errors.append(f"API key does not start with expected prefix: {prefix}")

            if key_format == "uuid":
                uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                if not re.match(uuid_pattern, api_key, re.IGNORECASE):
                    errors.append("API key is not a valid UUID format")
            elif key_format == "bearer":
                if not re.match(r"^[A-Za-z0-9_\-\.]+$", api_key):
                    errors.append("API key contains invalid characters for bearer format")
            elif key_format == "hex":
                if not re.match(r"^[0-9a-fA-F]+$", api_key):
                    errors.append("API key is not valid hex format")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "key_length": len(api_key),
                    "key_format": key_format,
                    "prefix_match": api_key.startswith(prefix) if prefix else True
                },
                message=f"API key validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API key validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["api_key"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"key_format": "any", "prefix": None, "min_length": 16, "max_length": 128}


class EndpointValidatorAction(BaseAction):
    """Validate API endpoints."""
    action_type = "api_endpoint_validator"
    display_name = "端点验证器"
    description = "验证API端点格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            allowed_schemes = params.get("allowed_schemes", ["https"])
            allowed_domains = params.get("allowed_domains", [])
            require_trailing_slash = params.get("require_trailing_slash", False)

            errors = []
            warnings = []

            if not endpoint:
                errors.append("Endpoint is empty")
                return ActionResult(success=False, data={"is_valid": False, "errors": errors}, message="Endpoint validation failed")

            scheme_match = re.match(r"^(https?)://", endpoint)
            if not scheme_match:
                errors.append("Endpoint missing scheme (http:// or https://)")
            else:
                scheme = scheme_match.group(1)
                if scheme not in allowed_schemes:
                    errors.append(f"Scheme '{scheme}' not allowed. Allowed: {allowed_schemes}")

            domain_match = re.match(r"^https?://([^/]+)", endpoint)
            if domain_match:
                domain = domain_match.group(1)
                if allowed_domains and domain not in allowed_domains:
                    errors.append(f"Domain '{domain}' not in allowed list: {allowed_domains}")
            else:
                errors.append("Could not extract domain from endpoint")

            if require_trailing_slash and not endpoint.endswith("/"):
                warnings.append("Endpoint does not end with trailing slash")

            path = re.sub(r"^https?://[^/]+", "", endpoint)
            if ".." in path:
                errors.append("Path contains '..' which may indicate path traversal")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "endpoint": endpoint,
                    "scheme": scheme_match.group(1) if scheme_match else None,
                    "domain": domain_match.group(1) if domain_match else None
                },
                message=f"Endpoint validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Endpoint validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["endpoint"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"allowed_schemes": ["https"], "allowed_domains": [], "require_trailing_slash": False}


class RateLimitValidatorAction(BaseAction):
    """Validate rate limit compliance."""
    action_type = "api_rate_limit_validator"
    display_name = "限流验证器"
    description = "验证限流合规性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            current_requests = params.get("current_requests", 0)
            limit = params.get("limit", 100)
            window_seconds = params.get("window_seconds", 60)
            action = params.get("action", "check")

            remaining = max(0, limit - current_requests)
            is_compliant = current_requests < limit
            utilization_pct = (current_requests / limit * 100) if limit > 0 else 0

            if action == "check":
                return ActionResult(
                    success=is_compliant,
                    data={
                        "is_compliant": is_compliant,
                        "current_requests": current_requests,
                        "limit": limit,
                        "remaining": remaining,
                        "utilization_pct": utilization_pct,
                        "window_seconds": window_seconds
                    },
                    message=f"Rate limit: {current_requests}/{limit} ({utilization_pct:.1f}%)"
                )

            elif action == "reserve":
                if not is_compliant:
                    return ActionResult(
                        success=False,
                        data={"reserved": False, "reason": "Rate limit exceeded"},
                        message="Cannot reserve: rate limit exceeded"
                    )
                return ActionResult(
                    success=True,
                    data={
                        "reserved": True,
                        "new_count": current_requests + 1,
                        "remaining": remaining - 1
                    },
                    message=f"Reserved: {remaining - 1} requests remaining"
                )

            elif action == "reset":
                return ActionResult(
                    success=True,
                    data={
                        "reset": True,
                        "current_requests": 0,
                        "remaining": limit
                    },
                    message="Rate limit counter reset"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["current_requests"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"limit": 100, "window_seconds": 60, "action": "check"}
