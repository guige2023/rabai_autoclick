"""API Documentation action module for RabAI AutoClick.

Provides API documentation operations:
- DocGenerateAction: Generate API documentation
- DocOpenAPIExportAction: Export to OpenAPI format
- DocMarkdownAction: Generate Markdown docs
- DocPostmanAction: Export to Postman collection
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DocGenerateAction(BaseAction):
    """Generate API documentation."""
    action_type = "doc_generate"
    display_name = "文档生成"
    description = "生成API文档"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute doc generation."""
        api_spec = params.get('api_spec', {})
        template = params.get('template', 'default')
        output_var = params.get('output_var', 'documentation')

        if not api_spec:
            return ActionResult(success=False, message="api_spec is required")

        try:
            resolved_spec = context.resolve_value(api_spec) if context else api_spec

            title = resolved_spec.get('info', {}).get('title', 'API Documentation')
            version = resolved_spec.get('info', {}).get('version', '1.0')
            description = resolved_spec.get('info', {}).get('description', '')
            paths = resolved_spec.get('paths', {})

            doc = {
                'title': title,
                'version': version,
                'description': description,
                'endpoints': [],
            }

            for path, methods in paths.items():
                for method, details in methods.items():
                    if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                        endpoint = {
                            'method': method.upper(),
                            'path': path,
                            'summary': details.get('summary', ''),
                            'description': details.get('description', ''),
                            'parameters': details.get('parameters', []),
                            'responses': list(details.get('responses', {}).keys()),
                        }
                        doc['endpoints'].append(endpoint)

            return ActionResult(
                success=True,
                data={output_var: doc},
                message=f"Generated docs for {len(doc['endpoints'])} endpoints"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Doc generate error: {e}")


class DocOpenAPIExportAction(BaseAction):
    """Export to OpenAPI format."""
    action_type = "doc_openapi_export"
    display_name = "导出OpenAPI"
    description = "导出为OpenAPI格式"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute OpenAPI export."""
        api_spec = params.get('api_spec', {})
        output_file = params.get('output_file', '/tmp/openapi.json')
        output_var = params.get('output_var', 'openapi_json')

        if not api_spec:
            return ActionResult(success=False, message="api_spec is required")

        try:
            resolved_spec = context.resolve_value(api_spec) if context else api_spec

            openapi_spec = {
                'openapi': '3.0.0',
                'info': {
                    'title': resolved_spec.get('info', {}).get('title', 'API'),
                    'version': resolved_spec.get('info', {}).get('version', '1.0'),
                    'description': resolved_spec.get('info', {}).get('description', ''),
                },
                'paths': resolved_spec.get('paths', {}),
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(openapi_spec, f, indent=2)

            result = {
                'file': output_file,
                'size': os.path.getsize(output_file),
                'format': 'openapi',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported to {output_file}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI export error: {e}")


class DocMarkdownAction(BaseAction):
    """Generate Markdown documentation."""
    action_type = "doc_markdown"
    display_name = "生成Markdown文档"
    description = "生成Markdown格式文档"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Markdown generation."""
        api_spec = params.get('api_spec', {})
        output_var = params.get('output_var', 'markdown_doc')

        if not api_spec:
            return ActionResult(success=False, message="api_spec is required")

        try:
            resolved_spec = context.resolve_value(api_spec) if context else api_spec

            title = resolved_spec.get('info', {}).get('title', 'API Documentation')
            version = resolved_spec.get('info', {}).get('version', '1.0')
            description = resolved_spec.get('info', {}).get('description', '')
            paths = resolved_spec.get('paths', {})

            md = f"# {title}\n\n"
            md += f"**Version:** {version}\n\n"
            if description:
                md += f"{description}\n\n"

            md += "## Endpoints\n\n"

            for path, methods in paths.items():
                for method, details in methods.items():
                    if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                        md += f"### `{method.upper()} {path}`\n\n"
                        if details.get('summary'):
                            md += f"**Summary:** {details['summary']}\n\n"
                        if details.get('description'):
                            md += f"{details['description']}\n\n"
                        if details.get('parameters'):
                            md += "**Parameters:**\n\n"
                            for param in details['parameters']:
                                md += f"- `{param.get('name', '')}` ({param.get('in', 'query')}): {param.get('description', '')}\n"
                            md += "\n"
                        if details.get('requestBody'):
                            md += f"**Request Body:** {details['requestBody'].get('description', '')}\n\n"
                        if details.get('responses'):
                            md += "**Responses:**\n\n"
                            for code, resp in details['responses'].items():
                                md += f"- `{code}`: {resp.get('description', '')}\n"
                            md += "\n"

            return ActionResult(
                success=True,
                data={output_var: {'markdown': md, 'length': len(md)}},
                message=f"Generated Markdown ({len(md)} chars)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Markdown generate error: {e}")


class DocPostmanAction(BaseAction):
    """Export to Postman collection."""
    action_type = "doc_postman"
    display_name = "导出Postman"
    description = "导出为Postman集合"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Postman export."""
        api_spec = params.get('api_spec', {})
        output_file = params.get('output_file', '/tmp/postman_collection.json')
        base_url = params.get('base_url', 'https://api.example.com')
        output_var = params.get('output_var', 'postman_result')

        if not api_spec:
            return ActionResult(success=False, message="api_spec is required")

        try:
            resolved_spec = context.resolve_value(api_spec) if context else api_spec
            resolved_base_url = context.resolve_value(base_url) if context else base_url

            title = resolved_spec.get('info', {}).get('title', 'API')
            version = resolved_spec.get('info', {}).get('version', '1.0')
            paths = resolved_spec.get('paths', {})

            collection = {
                'info': {
                    'name': title,
                    'schema': 'https://schema.getpostman.com/json/collection/v2.1.0/collection.json',
                },
                'item': [],
            }

            for path, methods in paths.items():
                folder_items = []
                for method, details in methods.items():
                    if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                        item = {
                            'name': f"{method.upper()} {path}",
                            'request': {
                                'method': method.upper(),
                                'header': [],
                                'url': {
                                    'raw': f"{resolved_base_url}{path}",
                                    'host': [resolved_base_url.replace('https://', '').replace('http://', '')],
                                    'path': path.strip('/').split('/'),
                                },
                            },
                        }
                        if details.get('requestBody'):
                            item['request']['body'] = {'mode': 'raw'}
                        folder_items.append(item)

                if folder_items:
                    collection['item'].append({
                        'name': path,
                        'item': folder_items,
                    })

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(collection, f, indent=2)

            result = {
                'file': output_file,
                'size': os.path.getsize(output_file),
                'format': 'postman',
                'item_count': len(collection['item']),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Exported Postman collection to {output_file}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Postman export error: {e}")
