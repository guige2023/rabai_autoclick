"""OpenAPI/Swagger action module for RabAI AutoClick.

Provides OpenAPI spec parsing, client generation, and API exploration.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OpenAPIAction(BaseAction):
    """OpenAPI/Swagger spec parser and client generator.
    
    Supports loading specs from files or URLs, endpoint listing,
    parameter extraction, and basic client code generation.
    """
    action_type = "openapi"
    display_name = "OpenAPI客户端"
    description = "OpenAPI规范解析与客户端生成"
    
    def __init__(self) -> None:
        super().__init__()
        self._spec: Optional[Dict[str, Any]] = None
    
    def _load_spec(self, spec_input: Union[str, Dict]) -> Optional[Dict[str, Any]]:
        """Load OpenAPI spec from file path, URL, or dict."""
        if isinstance(spec_input, dict):
            return spec_input
        if isinstance(spec_input, str):
            if spec_input.startswith('http://') or spec_input.startswith('https://'):
                try:
                    from urllib.request import urlopen
                    with urlopen(spec_input, timeout=10) as resp:
                        return json.loads(resp.read().decode('utf-8'))
                except Exception:
                    return None
            try:
                with open(spec_input, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return None
        return None
    
    def _resolve_ref(self, spec: Dict[str, Any], ref: str) -> Any:
        """Resolve JSON Pointer $ref."""
        if not ref.startswith('#/'):
            return None
        parts = ref[2:].split('/')
        current = spec
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OpenAPI operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'parse', 'list_endpoints', 'get_endpoint', 'generate_client'
                - spec: OpenAPI spec (dict, file path, or URL)
                - path: API path (for get_endpoint)
                - method: HTTP method (for get_endpoint)
                - language: Target language for client generation
        
        Returns:
            ActionResult with parsed spec or generated code.
        """
        command = params.get('command', 'parse')
        spec_input = params.get('spec')
        
        if command == 'parse':
            spec = self._load_spec(spec_input)
            if spec is None:
                return ActionResult(success=False, message="Failed to load OpenAPI spec")
            self._spec = spec
            info = spec.get('info', {})
            return ActionResult(
                success=True,
                message=f"Parsed OpenAPI spec: {info.get('title', 'Unknown')} v{info.get('version', '?')}",
                data={
                    'title': info.get('title'),
                    'version': info.get('version'),
                    'description': info.get('description'),
                    'paths': list(spec.get('paths', {}).keys()),
                    'schemas': list(spec.get('components', {}).get('schemas', {}).keys())
                }
            )
        
        if command == 'list_endpoints':
            if self._spec is None:
                spec = self._load_spec(spec_input)
                if spec is None:
                    return ActionResult(success=False, message="No spec loaded. Run 'parse' first or provide spec.")
                self._spec = spec
            paths = self._spec.get('paths', {})
            endpoints = []
            for path, path_item in paths.items():
                for method in ['get', 'post', 'put', 'patch', 'delete', 'head', 'options']:
                    if method in path_item:
                        op = path_item[method]
                        endpoints.append({
                            'path': path,
                            'method': method.upper(),
                            'operation_id': op.get('operationId'),
                            'summary': op.get('summary'),
                            'tags': op.get('tags', [])
                        })
            return ActionResult(
                success=True,
                message=f"Found {len(endpoints)} endpoints",
                data={'endpoints': endpoints}
            )
        
        if command == 'get_endpoint':
            if self._spec is None:
                spec = self._load_spec(spec_input)
                if spec is None:
                    return ActionResult(success=False, message="No spec loaded. Run 'parse' first or provide spec.")
                self._spec = spec
            path = params.get('path', '')
            method = params.get('method', 'get').lower()
            paths = self._spec.get('paths', {})
            if path not in paths:
                return ActionResult(success=False, message=f"Path {path} not found in spec")
            path_item = paths[path]
            if method not in path_item:
                return ActionResult(success=False, message=f"Method {method.upper()} not supported for {path}")
            op = path_item[method]
            params_list = []
            for param in op.get('parameters', []):
                if '$ref' in param:
                    param = self._resolve_ref(self._spec, param['$ref'])
                params_list.append({
                    'name': param.get('name'),
                    'in': param.get('in'),
                    'required': param.get('required', False),
                    'schema': param.get('schema', {})
                })
            responses = op.get('responses', {})
            return ActionResult(
                success=True,
                message=f"Endpoint {method.upper()} {path}",
                data={
                    'operation_id': op.get('operationId'),
                    'summary': op.get('summary'),
                    'description': op.get('description'),
                    'parameters': params_list,
                    'request_body': op.get('requestBody'),
                    'responses': responses,
                    'tags': op.get('tags', [])
                }
            )
        
        if command == 'generate_client':
            if self._spec is None:
                spec = self._load_spec(spec_input)
                if spec is None:
                    return ActionResult(success=False, message="No spec loaded. Run 'parse' first or provide spec.")
                self._spec = spec
            language = params.get('language', 'python')
            info = self._spec.get('info', {})
            title = info.get('title', 'APIClient').replace(' ', '')
            
            if language == 'python':
                code = self._generate_python_client(title)
            elif language == 'typescript':
                code = self._generate_typescript_client(title)
            elif language == 'javascript':
                code = self._generate_javascript_client(title)
            else:
                return ActionResult(success=False, message=f"Unsupported language: {language}")
            
            return ActionResult(
                success=True,
                message=f"Generated {language} client for {title}",
                data={'code': code, 'language': language, 'title': title}
            )
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _generate_python_client(self, title: str) -> str:
        """Generate Python client code."""
        paths = self._spec.get('paths', {})
        lines = [
            f'\"\"\"Auto-generated {title} API client.\"\"\"',
            'import requests',
            '',
            '',
            f'class {title}Client:',
            f'    """{self._spec.get("info", {{}}).get("description", title)}"""',
            '',
            '    def __init__(self, base_url: str, api_key: str = None) -> None:',
            '        self.base_url = base_url.rstrip("/")',
            '        self.api_key = api_key',
            '        self.session = requests.Session()',
            '        if api_key:',
            '            self.session.headers.update({"Authorization": f"Bearer {{api_key}}"}})',
            '',
        ]
        for path, path_item in paths.items():
            for method in ['get', 'post', 'put', 'patch', 'delete']:
                if method in path_item:
                    op = path_item[method]
                    op_id = op.get('operationId', f'{method}_{path.replace("/", "_")}')
                    safe_id = op_id.replace('.', '_').replace('-', '_')
                    summary = op.get('summary', '')
                    param_names = []
                    for param in op.get('parameters', []):
                        if '$ref' in param:
                            param = self._resolve_ref(self._spec, param['$ref'])
                        pname = param.get('name', 'param')
                        param_names.append(pname)
                    params_str = ', '.join(param_names)
                    lines.append(f'    def {safe_id}(self, {params_str}) -> dict:')
                    lines.append(f'        """{summary}"""')
                    lines.append(f'        url = f"{{self.base_url}}{path}"')
                    lines.append(f'        return self.session.{method}(url)')
                    lines.append('')
        return '\n'.join(lines)
    
    def _generate_typescript_client(self, title: str) -> str:
        """Generate TypeScript client code."""
        paths = self._spec.get('paths', {})
        lines = [
            f'// Auto-generated {title} API client',
            '',
            f'export class {title}Client {{',
            '  constructor(private baseUrl: string, private apiKey?: string) {{}}',
            '',
        ]
        for path, path_item in paths.items():
            for method in ['get', 'post', 'put', 'patch', 'delete']:
                if method in path_item:
                    op = path_item[method]
                    op_id = op.get('operationId', f'{method}_{path.replace("/", "_")}')
                    safe_id = op_id.replace('.', '_').replace('-', '_')
                    summary = op.get('summary', '')
                    param_names = []
                    for param in op.get('parameters', []):
                        if '$ref' in param:
                            param = self._resolve_ref(self._spec, param['$ref'])
                        pname = param.get('name', 'param')
                        param_names.append(pname)
                    params_str = ', '.join(param_names) if param_names else ''
                    lines.append(f'  async {safe_id}({params_str}): Promise<any> {{')
                    lines.append(f'    const url = `${{this.baseUrl}}{path}`;')
                    lines.append(f'    const response = await fetch(url, {{ method: "{method.upper()}" }});')
                    lines.append('    return response.json();')
                    lines.append('  }')
                    lines.append('')
        lines.append('}')
        return '\n'.join(lines)
    
    def _generate_javascript_client(self, title: str) -> str:
        """Generate JavaScript client code."""
        paths = self._spec.get('paths', {})
        lines = [
            f'// Auto-generated {title} API client',
            '',
            f'class {title}Client {{',
            '  constructor(baseUrl, apiKey) {',
            '    this.baseUrl = baseUrl;',
            '    this.apiKey = apiKey;',
            '  }',
            '',
        ]
        for path, path_item in paths.items():
            for method in ['get', 'post', 'put', 'patch', 'delete']:
                if method in path_item:
                    op = path_item[method]
                    op_id = op.get('operationId', f'{method}_{path.replace("/", "_")}')
                    safe_id = op_id.replace('.', '_').replace('-', '_')
                    lines.append(f'  async {safe_id}(params) {{')
                    lines.append(f'    const url = `${this.baseUrl}{path}`;')
                    lines.append(f'    const response = await fetch(url, {{ method: "{method.upper()}" }});')
                    lines.append('    return response.json();')
                    lines.append('  }')
                    lines.append('')
        lines.append('}')
        lines.append(f'module.exports = {{ {title}Client }};')
        return '\n'.join(lines)
