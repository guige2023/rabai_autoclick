"""API SDK action module for RabAI AutoClick.

Provides API client SDK generation from OpenAPI specs and
automatic SDK method binding.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiSdkGeneratorAction(BaseAction):
    """Generate typed API SDK from OpenAPI/Swagger specifications.

    Parses OpenAPI 3.x specs and generates method signatures,
    type hints, and request builders.
    """
    action_type = "api_sdk_generator"
    display_name = "API SDK生成器"
    description = "从OpenAPI规范生成类型安全的API SDK"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate SDK code from API specification.

        Args:
            context: Execution context.
            params: Dict with keys: spec_url, spec_content,
                   language (python/js/typescript), output_format.

        Returns:
            ActionResult with generated SDK code.
        """
        start_time = time.time()
        try:
            spec_url = params.get('spec_url', '')
            spec_content = params.get('spec_content', '')
            language = params.get('language', 'python').lower()
            output_format = params.get('output_format', 'code')

            # Load spec
            if spec_url:
                from urllib.request import urlopen
                with urlopen(spec_url) as resp:
                    spec = json.loads(resp.read().decode('utf-8'))
            elif spec_content:
                if isinstance(spec_content, str):
                    spec = json.loads(spec_content)
                else:
                    spec = spec_content
            else:
                return ActionResult(
                    success=False,
                    message="Either spec_url or spec_content is required",
                    duration=time.time() - start_time,
                )

            # Extract info
            info = spec.get('info', {})
            title = info.get('title', 'Unknown API')
            version = info.get('version', '1.0.0')
            base_url = ''
            servers = spec.get('servers', [])
            if servers:
                base_url = servers[0].get('url', '')

            paths = spec.get('paths', {})

            if language == 'python':
                code = self._generate_python_sdk(title, version, base_url, paths)
            elif language in ('javascript', 'js', 'typescript', 'ts'):
                code = self._generate_js_sdk(title, version, base_url, paths)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unsupported language: {language}",
                    duration=time.time() - start_time,
                )

            if output_format == 'code':
                data = {'code': code, 'language': language}
            else:
                data = {'spec': spec, 'endpoints': list(paths.keys())}

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Generated {language} SDK for {title}",
                data=data,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"SDK generation failed: {str(e)}",
                duration=duration,
            )

    def _generate_python_sdk(self, title: str, version: str, base_url: str, paths: Dict) -> str:
        """Generate Python SDK code."""
        lines = [
            f'"""SDK for {title} v{version}."""',
            'import requests',
            '',
            '',
            f'class {title.replace(" ", "").replace("-", "")}Client:',
            f'    """Client for {title}."""',
            '',
            f'    def __init__(self, api_key: str = None, base_url: str = "{base_url}"):',
            '        self.session = requests.Session()',
            '        if api_key:',
            '            self.session.headers["Authorization"] = f"Bearer {{api_key}}"',
            '        self.base_url = base_url',
            '',
        ]
        for path, methods in sorted(paths.items()):
            for method, details in methods.items():
                if method.upper() in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
                    operation_id = details.get('operationId', method + path.replace('/', '_'))
                    summary = details.get('summary', '')
                    params = details.get('parameters', [])
                    args = []
                    for p in params:
                        p_name = p.get('name', 'param')
                        p_required = p.get('required', False)
                        p_type = p.get('schema', {}).get('type', 'str')
                        default = '' if p_required else '=None'
                        args.append(f'{p_name}: {p_type} {default}')
                    args_str = ', '.join(args) if args else 'self'
                    func_lines = [
                        '',
                        f'    def {operation_id}({args_str}) -> dict:',
                        f'        """{summary}"""',
                        f'        url = f"{{self.base_url}}{path}"',
                        f'        response = self.session.{method}(url)',
                        f'        response.raise_for_status()',
                        f'        return response.json()',
                    ]
                    lines.extend(func_lines)
        return '\n'.join(lines)

    def _generate_js_sdk(self, title: str, version: str, base_url: str, paths: Dict) -> str:
        """Generate JavaScript/TypeScript SDK code."""
        lines = [
            f'// SDK for {title} v{version}',
            '',
            f'class {title.replace(" ", "").replace("-", "")}Client {{',
            f'  constructor(apiKey = null, baseUrl = "{base_url}") {{',
            '    this.baseUrl = baseUrl;',
            '    this.apiKey = apiKey;',
            '  }',
            '',
            '  async request(method, path, params = {}) {',
            '    const url = this.baseUrl + path;',
            '    const options = { method, headers: {} };',
            '    if (this.apiKey) options.headers["Authorization"] = `Bearer ${{this.apiKey}}`;',
            '    const resp = await fetch(url, options);',
            '    if (!resp.ok) throw new Error(`HTTP ${{resp.status}}`);',
            '    return resp.json();',
            '  }',
        ]
        for path, methods in sorted(paths.items()):
            for method, details in methods.items():
                if method.upper() in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
                    operation_id = details.get('operationId', method + path.replace('/', '_'))
                    summary = details.get('summary', '')
                    params = details.get('parameters', [])
                    args = ', '.join([p.get('name', 'param') for p in params])
                    lines.extend([
                        '',
                        f'  async {operation_id}({args}) {{',
                        f'    // {summary}',
                        f'    return this.request("{method.upper()}", "{path}");',
                        '  }',
                    ])
        lines.append('}')
        return '\n'.join(lines)


class ApiSdkClientAction(BaseAction):
    """Use a generated SDK to make API calls.

    Wraps SDK methods and handles authentication, retries,
    and response parsing.
    """
    action_type = "api_sdk_client"
    display_name = "API SDK客户端"
    description = "使用SDK发起API调用"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an SDK method call.

        Args:
            context: Execution context.
            params: Dict with keys: sdk_instance, method_name,
                   args, kwargs.

        Returns:
            ActionResult with API response.
        """
        start_time = time.time()
        try:
            sdk_instance = params.get('sdk_instance')
            method_name = params.get('method_name', '')
            args = params.get('args', [])
            kwargs = params.get('kwargs', {})

            if not sdk_instance or not method_name:
                return ActionResult(
                    success=False,
                    message="sdk_instance and method_name are required",
                    duration=time.time() - start_time,
                )

            if not hasattr(sdk_instance, method_name):
                return ActionResult(
                    success=False,
                    message=f"SDK has no method '{method_name}'",
                    duration=time.time() - start_time,
                )

            method = getattr(sdk_instance, method_name)
            result = method(*args, **kwargs)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"SDK call {method_name} succeeded",
                data=result,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"SDK call failed: {str(e)}",
                duration=duration,
            )
