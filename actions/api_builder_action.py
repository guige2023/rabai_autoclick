"""API Builder action module for RabAI AutoClick.

Provides dynamic API request builder with parameter substitution,
template support, and URL construction from schemas.
"""

import json
import time
import sys
import os
import re
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiBuilderAction(BaseAction):
    """Build HTTP API requests dynamically from templates and schemas.

    Supports parameter substitution, path variable expansion, query
    string construction, and header templating.
    """
    action_type = "api_builder"
    display_name = "API构建器"
    description = "动态构建HTTP API请求，支持模板和参数替换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build an API request from template.

        Args:
            context: Execution context.
            params: Dict with keys: base_url, path_template, method,
                   path_params, query_params, headers_template,
                   body_template, timeout.

        Returns:
            ActionResult with built request details.
        """
        start_time = time.time()
        try:
            base_url = params.get('base_url', '')
            path_template = params.get('path_template', '/')
            method = params.get('method', 'GET').upper()
            path_params = params.get('path_params', {})
            query_params = params.get('query_params', {})
            headers_template = params.get('headers_template', {})
            body_template = params.get('body_template', None)
            timeout = params.get('timeout', 30)

            # Build path with parameter substitution
            path = path_template
            for key, value in path_params.items():
                placeholder = '{' + key + '}'
                if placeholder in path:
                    path = path.replace(placeholder, str(value))
                else:
                    pattern = r'\{' + re.escape(key) + r'\}'
                    path = re.sub(pattern, str(value), path)

            # Construct full URL
            if base_url:
                url = urljoin(base_url.rstrip('/') + '/', path.lstrip('/'))
            else:
                url = path

            # Build query string
            flat_query = {}
            for key, value in query_params.items():
                if isinstance(value, list):
                    for item in value:
                        flat_query[key] = item
                else:
                    flat_query[key] = value

            if flat_query:
                url = url + '?' + urlencode(flat_query)

            # Substitute headers
            headers = {}
            for key, value in headers_template.items():
                if isinstance(value, str) and '{' in value:
                    try:
                        value = value.format(**path_params)
                    except (KeyError, ValueError):
                        pass
                headers[key] = value

            # Handle body
            body = None
            if body_template and method in ['POST', 'PUT', 'PATCH']:
                if isinstance(body_template, dict):
                    # Substitute variables in body
                    body_str = json.dumps(body_template)
                    for key, value in path_params.items():
                        body_str = body_str.replace('{' + key + '}', str(value))
                    body = body_str.encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')
                elif isinstance(body_template, str):
                    body = body_template.encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Built {method} request for {url}",
                data={
                    'url': url,
                    'method': method,
                    'headers': headers,
                    'body': body,
                    'timeout': timeout,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Failed to build API request: {str(e)}",
                duration=duration,
            )


class ApiTemplateAction(BaseAction):
    """Manage and apply API request templates.

    Stores templates, applies variable substitution, and manages
    template versioning.
    """
    action_type = "api_template"
    display_name = "API模板管理"
    description = "管理和应用API请求模板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply or manage an API template.

        Args:
            context: Execution context.
            params: Dict with keys: action (get/apply/save/delete),
                   template_name, template_data, variables.

        Returns:
            ActionResult with template or applied result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'get')
            template_name = params.get('template_name', 'default')
            template_data = params.get('template_data', {})
            variables = params.get('variables', {})

            # In-memory template store (in production, use a database)
            template_store = getattr(context, '_api_templates', {})
            if not hasattr(context, '_api_templates'):
                context._api_templates = {}

            if action == 'save':
                context._api_templates[template_name] = template_data
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Saved template '{template_name}'",
                    duration=duration,
                )

            elif action == 'delete':
                if template_name in context._api_templates:
                    del context._api_templates[template_name]
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Deleted template '{template_name}'",
                    duration=duration,
                )

            elif action == 'apply':
                if template_name not in context._api_templates:
                    return ActionResult(
                        success=False,
                        message=f"Template '{template_name}' not found",
                        duration=time.time() - start_time,
                    )
                template = context._api_templates[template_name]
                # Apply variable substitution
                applied = self._substitute(template, variables)
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Applied template '{template_name}'",
                    data=applied,
                    duration=duration,
                )

            else:  # get
                if template_name in context._api_templates:
                    data = context._api_templates[template_name]
                else:
                    data = {}
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Retrieved template '{template_name}'",
                    data=data,
                    duration=duration,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Template action failed: {str(e)}",
                duration=duration,
            )

    def _substitute(self, template: Any, variables: Dict[str, Any]) -> Any:
        """Recursively substitute variables in template."""
        if isinstance(template, dict):
            return {k: self._substitute(v, variables) for k, v in template.items()}
        elif isinstance(template, list):
            return [self._substitute(item, variables) for item in template]
        elif isinstance(template, str):
            try:
                return template.format(**variables)
            except (KeyError, ValueError):
                return template
        return template
