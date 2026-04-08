"""OpenAPI/Swagger action module for RabAI AutoClick.

Provides OpenAPI operations:
- OpenAPIParserAction: Parse OpenAPI spec
- OpenAPIValidatorAction: Validate requests against spec
- OpenAPIGeneratorAction: Generate client code
- OpenAPIMockerAction: Mock server from spec
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OpenAPIParserAction(BaseAction):
    """Parse OpenAPI specification."""
    action_type = "openapi_parser"
    display_name = "OpenAPI解析"
    description = "解析OpenAPI规范"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OpenAPI parse."""
        spec = params.get('spec', '')
        spec_file = params.get('spec_file', None)
        output_var = params.get('output_var', 'openapi_spec')

        if not spec and not spec_file:
            return ActionResult(success=False, message="spec or spec_file is required")

        try:
            import json
            resolved = context.resolve_value(spec) if context else spec
            resolved_file = context.resolve_value(spec_file) if context else spec_file

            if resolved_file:
                with open(resolved_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = json.loads(resolved)

            info = data.get('info', {})
            paths = data.get('paths', {})
            components = data.get('components', {})

            result = {
                'title': info.get('title', ''),
                'version': info.get('version', ''),
                'description': info.get('description', ''),
                'path_count': len(paths),
                'paths': list(paths.keys()),
                'schemas': list(components.get('schemas', {}).keys()),
            }

            return ActionResult(success=True, data={output_var: result}, message=f"Parsed {len(paths)} paths")
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI parse error: {e}")


class OpenAPIValidatorAction(BaseAction):
    """Validate request against OpenAPI spec."""
    action_type = "openapi_validator"
    display_name = "OpenAPI验证"
    description = "验证请求是否符合OpenAPI规范"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OpenAPI validation."""
        spec = params.get('spec', {})
        method = params.get('method', 'get').lower()
        path = params.get('path', '')
        body = params.get('body', None)
        output_var = params.get('output_var', 'validation_result')

        if not spec or not path:
            return ActionResult(success=False, message="spec, method, and path are required")

        try:
            import json
            resolved = context.resolve_value(spec) if context else spec

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            paths = resolved.get('paths', {})
            path_template = paths.get(path, {})
            operation = path_template.get(method, {}) if path_template else {}

            errors = []
            required_fields = operation.get('requestBody', {}).get('required', False)
            if required_fields and not body:
                errors.append("Request body is required")

            params_spec = operation.get('parameters', [])
            for p in params_spec:
                if p.get('required') and p.get('name') not in (body or {}):
                    errors.append(f"Missing required parameter: {p['name']}")

            result = {
                'valid': len(errors) == 0,
                'errors': errors,
                'method': method.upper(),
                'path': path,
            }

            return ActionResult(success=True, data={output_var: result}, message="Valid" if not errors else f"{len(errors)} errors")
        except Exception as e:
            return ActionResult(success=False, message=f"Validation error: {e}")


class OpenAPIGeneratorAction(BaseAction):
    """Generate client code from OpenAPI spec."""
    action_type = "openapi_generator"
    display_name = "OpenAPI代码生成"
    description = "从OpenAPI规范生成客户端代码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute client code generation."""
        spec = params.get('spec', {})
        language = params.get('language', 'python')
        output_var = params.get('output_var', 'generated_code')

        if not spec:
            return ActionResult(success=False, message="spec is required")

        try:
            import json
            resolved = context.resolve_value(spec) if context else spec

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            title = resolved.get('info', {}).get('title', 'API')
            version = resolved.get('info', {}).get('version', '1.0')
            paths = resolved.get('paths', {})

            if language == 'python':
                code = f'''"""Generated client for {title}."""
import requests

class {title.replace(' ', '')}Client:
    def __init__(self, base_url: str, token: str = None):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        if token:
            self.session.headers['Authorization'] = f'Bearer {{token}}'
'''
                for path, methods in paths.items():
                    for method, op in methods.items():
                        if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                            op_id = op.get('operationId', f'{method}_{path.replace("/", "_")}')
                            code += f'''
    def {op_id}(self{", " if op.get('parameters') else ""}{', '.join([p['name'] for p in op.get('parameters', [])]) if op.get('parameters') else ''}):
        """{op.get('summary', '')}"""
        return self.session.{method}(
            f'{{self.base_url}}{path}'{", ".join([f"{p['name']}={p['name']}" for p in op.get('parameters', [])]) if op.get('parameters') else ''}
        )
'''
            else:
                code = f"// Generated client for {title} v{version}\n// Language: {language}"

            return ActionResult(success=True, data={output_var: code}, message=f"Generated {language} client")
        except Exception as e:
            return ActionResult(success=False, message=f"Code generation error: {e}")


class OpenAPIMockerAction(BaseAction):
    """Mock server responses from OpenAPI spec."""
    action_type = "openapi_mocker"
    display_name = "OpenAPI模拟"
    description = "从OpenAPI规范模拟服务器响应"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute mock response generation."""
        spec = params.get('spec', {})
        method = params.get('method', 'get').lower()
        path = params.get('path', '')
        output_var = params.get('output_var', 'mock_response')

        if not spec or not path:
            return ActionResult(success=False, message="spec, method, and path are required")

        try:
            import json
            resolved = context.resolve_value(spec) if context else spec

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            paths = resolved.get('paths', {})
            path_config = paths.get(path, {})
            operation = path_config.get(method, {}) if path_config else {}

            responses = operation.get('responses', {})
            mock_response = {}
            for code, resp in responses.items():
                content = resp.get('content', {})
                if 'application/json' in content:
                    schema = content['application/json'].get('schema', {})
                    mock_response = {
                        'status_code': int(code) if code.isdigit() else 200,
                        'body': self._generate_mock(schema),
                        'description': resp.get('description', ''),
                    }
                    break

            result = {
                'method': method.upper(),
                'path': path,
                'mock': mock_response,
            }

            return ActionResult(success=True, data={output_var: result}, message=f"Mocked response for {method.upper()} {path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Mock generation error: {e}")

    def _generate_mock(self, schema: Dict) -> Any:
        """Generate mock data from schema."""
        schema_type = schema.get('type', 'string')
        if schema_type == 'integer':
            return 0
        elif schema_type == 'number':
            return 0.0
        elif schema_type == 'boolean':
            return True
        elif schema_type == 'array':
            return [self._generate_mock(schema.get('items', {}))]
        elif schema_type == 'object':
            return {k: self._generate_mock(v) for k, v in schema.get('properties', {}).items()}
        return ''
