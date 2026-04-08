"""API Schema action module for RabAI AutoClick.

Validates and parses API schemas (OpenAPI, JSON Schema)
and generates type-safe code.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import urlopen

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiSchemaValidatorAction(BaseAction):
    """Validate API requests/responses against JSON Schema.

    Validates data structure, types, required fields,
    and format constraints.
    """
    action_type = "api_schema_validator"
    display_name = "API Schema验证器"
    description = "根据JSON Schema验证API数据"

    JSON_SCHEMA_TYPES = ['string', 'number', 'integer', 'boolean', 'array', 'object', 'null']

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate data against schema.

        Args:
            context: Execution context.
            params: Dict with keys: data, schema, strict (bool).

        Returns:
            ActionResult with validation result.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            schema = params.get('schema', {})
            strict = params.get('strict', False)

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            errors = []
            self._validate(data, schema, '', errors, strict)

            duration = time.time() - start_time
            return ActionResult(
                success=len(errors) == 0,
                message=f"Validation: {'PASS' if not errors else f'{len(errors)} errors'}",
                data={'valid': len(errors) == 0, 'errors': errors},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Schema validation error: {str(e)}",
                duration=duration,
            )

    def _validate(
        self,
        data: Any,
        schema: Dict,
        path: str,
        errors: List[str],
        strict: bool
    ) -> None:
        """Recursively validate data against schema."""
        if not schema:
            return

        # Type check
        expected_type = schema.get('type')
        if expected_type:
            type_map = {
                'string': str, 'number': (int, float),
                'integer': int, 'boolean': bool,
                'array': list, 'object': dict,
            }
            expected_python_type = type_map.get(expected_type)
            if expected_python_type and not isinstance(data, expected_python_type):
                if not (expected_type == 'number' and isinstance(data, int)):
                    errors.append(f"{path}: expected {expected_type}, got {type(data).__name__}")
                return

        # Enum check
        enum = schema.get('enum')
        if enum and data not in enum:
            errors.append(f"{path}: value not in enum {enum}")

        # Pattern check
        pattern = schema.get('pattern')
        if pattern == 'string' and isinstance(data, str):
            import re
            if not re.match(pattern, data):
                errors.append(f"{path}: does not match pattern {pattern}")

        # Min/max for numbers
        if isinstance(data, (int, float)):
            minimum = schema.get('minimum')
            maximum = schema.get('maximum')
            if minimum is not None and data < minimum:
                errors.append(f"{path}: {data} < minimum {minimum}")
            if maximum is not None and data > maximum:
                errors.append(f"{path}: {data} > maximum {maximum}")

        # Min/max length for strings
        if isinstance(data, str):
            min_length = schema.get('minLength', 0)
            max_length = schema.get('maxLength')
            if len(data) < min_length:
                errors.append(f"{path}: length {len(data)} < minLength {min_length}")
            if max_length and len(data) > max_length:
                errors.append(f"{path}: length {len(data)} > maxLength {max_length}")

        # Array items
        if isinstance(data, list) and 'items' in schema:
            for i, item in enumerate(data):
                self._validate(item, schema['items'], f"{path}[{i}]", errors, strict)

        # Object properties
        if isinstance(data, dict) and 'properties' in schema:
            required = schema.get('required', [])
            for req_field in required:
                if req_field not in data:
                    errors.append(f"{path}.{req_field}: required field missing")

            props = schema['properties']
            for key, value in data.items():
                if key in props:
                    self._validate(value, props[key], f"{path}.{key}", errors, strict)
                elif strict:
                    errors.append(f"{path}.{key}: unexpected property")


class ApiSchemaGeneratorAction(BaseAction):
    """Generate JSON Schema from sample data or code.

    Infers schema from data samples and generates
    type definitions.
    """
    action_type = "api_schema_generator"
    display_name = "API Schema生成器"
    description = "从数据样本生成JSON Schema"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate schema.

        Args:
            context: Execution context.
            params: Dict with keys: samples (list), schema_version,
                   include_required.

        Returns:
            ActionResult with generated schema.
        """
        start_time = time.time()
        try:
            samples = params.get('samples', [])
            schema_version = params.get('schema_version', 'draft-07')
            include_required = params.get('include_required', True)

            if not samples:
                return ActionResult(
                    success=False,
                    message="At least one sample is required",
                    duration=time.time() - start_time,
                )

            schema = {
                '$schema': f'http://json-schema.org/draft-07/schema#',
                'type': 'object',
                'properties': {},
            }

            # Merge all sample keys
            all_keys = set()
            for sample in samples:
                if isinstance(sample, dict):
                    all_keys.update(sample.keys())

            required = []
            for key in sorted(all_keys):
                types_in_samples = set()
                for sample in samples:
                    if isinstance(sample, dict) and key in sample:
                        types_in_samples.add(type(sample[key]).__name__)

                prop_schema = {'type': 'string'}
                if len(types_in_samples) == 1:
                    t = types_in_samples.pop()
                    if t == 'dict':
                        prop_schema['type'] = 'object'
                    elif t == 'list':
                        prop_schema['type'] = 'array'
                    elif t == 'int':
                        prop_schema['type'] = 'integer'
                    elif t == 'float':
                        prop_schema['type'] = 'number'
                    elif t == 'bool':
                        prop_schema['type'] = 'boolean'
                    elif t == 'NoneType':
                        prop_schema['type'] = 'null'

                schema['properties'][key] = prop_schema
                if include_required and all(isinstance(s, dict) and key in s for s in samples):
                    required.append(key)

            if required:
                schema['required'] = required

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Generated schema with {len(schema['properties'])} properties",
                data={'schema': schema},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Schema generation error: {str(e)}",
                duration=duration,
            )


class ApiOpenApiParserAction(BaseAction):
    """Parse OpenAPI specs and extract endpoints, schemas.

    Loads OpenAPI 3.x specifications and extracts
    structured information.
    """
    action_type = "api_openapi_parser"
    display_name = "OpenAPI解析器"
    description = "解析OpenAPI规范并提取端点信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse OpenAPI spec.

        Args:
            context: Execution context.
            params: Dict with keys: spec_url, spec_content, extract.

        Returns:
            ActionResult with parsed OpenAPI data.
        """
        start_time = time.time()
        try:
            spec_url = params.get('spec_url', '')
            spec_content = params.get('spec_content', '')
            extract = params.get('extract', 'endpoints')

            if spec_url:
                from urllib.request import urlopen
                with urlopen(spec_url, timeout=30) as resp:
                    spec = json.loads(resp.read())
            elif spec_content:
                spec = json.loads(spec_content) if isinstance(spec_content, str) else spec_content
            else:
                return ActionResult(
                    success=False,
                    message="spec_url or spec_content is required",
                    duration=time.time() - start_time,
                )

            info = spec.get('info', {})
            servers = spec.get('servers', [])
            paths = spec.get('paths', {})
            components = spec.get('components', {})
            schemas = components.get('schemas', {})

            if extract == 'endpoints':
                endpoints = []
                for path, methods in paths.items():
                    for method, details in methods.items():
                        if method.upper() in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
                            endpoints.append({
                                'path': path,
                                'method': method.upper(),
                                'summary': details.get('summary', ''),
                                'operation_id': details.get('operationId', ''),
                                'parameters': details.get('parameters', []),
                                'request_body': details.get('requestBody'),
                                'responses': list(details.get('responses', {}).keys()),
                            })
                return ActionResult(
                    success=True,
                    message=f"Extracted {len(endpoints)} endpoints",
                    data={'endpoints': endpoints, 'base_url': servers[0].get('url') if servers else ''},
                    duration=time.time() - start_time,
                )

            elif extract == 'schemas':
                return ActionResult(
                    success=True,
                    message=f"Extracted {len(schemas)} schemas",
                    data={'schemas': schemas},
                    duration=time.time() - start_time,
                )

            elif extract == 'full':
                return ActionResult(
                    success=True,
                    message=f"Parsed OpenAPI spec: {info.get('title')}",
                    data={
                        'info': info,
                        'servers': servers,
                        'endpoints_count': sum(len(m) for m in paths.values()),
                        'schemas_count': len(schemas),
                    },
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown extract type: {extract}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"OpenAPI parse error: {str(e)}",
                duration=duration,
            )
